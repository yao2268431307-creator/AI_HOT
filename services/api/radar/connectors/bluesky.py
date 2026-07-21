from __future__ import annotations

import asyncio
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Callable
from urllib.parse import quote, urlencode, urlsplit

from websockets.asyncio.client import connect as WebSocketConnect
from websockets.exceptions import WebSocketException

from ..contracts import Observation
from ..normalize import content_fingerprint, language_hint, normalize_url, sanitize_external_text
from .base import (
    BaseConnector,
    ConnectorError,
    ensure_safe_public_url,
    ensure_safe_public_websocket_url,
    ensure_safe_public_websocket_url_resolved,
    utcnow,
)


DEFAULT_JETSTREAM_ENDPOINT = "wss://jetstream2.us-west.bsky.network/subscribe"
DEFAULT_JETSTREAM_FALLBACK_ENDPOINTS = (
    "wss://jetstream1.us-west.bsky.network/subscribe",
    "wss://jetstream2.us-east.bsky.network/subscribe",
    "wss://jetstream1.us-east.bsky.network/subscribe",
)
DEFAULT_APPVIEW_ENDPOINT = "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"
DEFAULT_AI_KEYWORDS = (
    "ai",
    "artificial intelligence",
    "machine learning",
    "large language model",
    "llm",
    "gpt",
    "openai",
    "anthropic",
    "claude",
    "gemini",
    "qwen",
    "deepseek",
    "mistral",
    "llama",
    "人工智能",
    "大模型",
    "模型",
    "智能体",
    "多模态",
)
JETSTREAM_CURSOR_VERSION = "jetstream-unix-microseconds-v1"
JETSTREAM_COLLECTION = "app.bsky.feed.post"
MAX_APPVIEW_URIS = 25
MAX_STORED_EXCERPT_CHARACTERS = 500


SocketFactory = Callable[..., AbstractAsyncContextManager[Any]]


class NoRedirectWebSocketConnect(WebSocketConnect):
    """Disable WebSocket redirects so DNS validation cannot be bypassed."""

    def process_redirect(self, exc: Exception) -> Exception | str:
        destination = super().process_redirect(exc)
        if isinstance(destination, str):
            return ConnectorError("websocket redirects are disabled")
        return destination


def _bounded_integer(value: int, *, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not minimum <= value <= maximum:
        raise ConnectorError(f"{name} must be from {minimum} to {maximum}")
    return value


def _parse_datetime(value: object, fallback: datetime, collected_at: datetime) -> datetime:
    if not isinstance(value, str):
        return fallback
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return fallback if parsed > collected_at + timedelta(minutes=5) else parsed


class BlueskyJetstreamConnector(BaseConnector):
    """Use legacy Jetstream for discovery and AppView for bounded verification.

    Jetstream events aren't self-authenticating. AppView verifies the current
    URI/CID/author snapshot, but Jetstream discovery does not yet provide a
    durable update/delete supersession index. Therefore every Bluesky item is
    discovery-only in V1 and cannot contribute metrics, scores or alerts.
    """

    id = "bluesky"
    access_class = "public_no_billing"
    rights_policy_id = "bluesky-appview-jetstream-experimental-v1"
    platform = "Bluesky"
    signal_family = "discussion"
    expected_requests_per_collect = 2

    def __init__(
        self,
        *args: object,
        endpoint: str = DEFAULT_JETSTREAM_ENDPOINT,
        fallback_endpoints: tuple[str, ...] = DEFAULT_JETSTREAM_FALLBACK_ENDPOINTS,
        appview_endpoint: str = DEFAULT_APPVIEW_ENDPOINT,
        keywords: tuple[str, ...] = DEFAULT_AI_KEYWORDS,
        max_messages: int = 500,
        idle_timeout_seconds: float = 3.0,
        max_message_size_bytes: int = 262_144,
        replay_overlap_seconds: int = 5,
        connect_factory: SocketFactory | None = None,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        ensure_safe_public_websocket_url(endpoint)
        endpoints = tuple(dict.fromkeys((endpoint, *fallback_endpoints)))
        for candidate_endpoint in endpoints:
            ensure_safe_public_websocket_url(candidate_endpoint)
            if urlsplit(candidate_endpoint).scheme != "wss":
                raise ConnectorError("Bluesky Jetstream must use wss://")
        ensure_safe_public_url(appview_endpoint)
        if urlsplit(endpoint).scheme != "wss":
            raise ConnectorError("Bluesky Jetstream must use wss://")
        if urlsplit(appview_endpoint).scheme != "https":
            raise ConnectorError("Bluesky AppView must use https://")
        if not keywords or len(keywords) > 100:
            raise ConnectorError("Bluesky discovery requires from 1 to 100 AI keywords")
        normalized_keywords = tuple(
            value for value in (sanitize_external_text(str(item)).casefold() for item in keywords)
            if value
        )
        if not normalized_keywords or any(len(value) > 80 for value in normalized_keywords):
            raise ConnectorError("Bluesky keywords must be non-empty and at most 80 characters")
        self.endpoint = endpoint
        self.endpoints = endpoints
        self.appview_endpoint = appview_endpoint
        self.keywords = tuple(dict.fromkeys(normalized_keywords))
        self.max_messages = _bounded_integer(max_messages, name="max_messages", minimum=1, maximum=5_000)
        if not 0.1 <= idle_timeout_seconds <= 30:
            raise ConnectorError("idle_timeout_seconds must be from 0.1 to 30")
        self.idle_timeout_seconds = float(idle_timeout_seconds)
        self.max_message_size_bytes = _bounded_integer(
            max_message_size_bytes,
            name="max_message_size_bytes",
            minimum=1_024,
            maximum=1_048_576,
        )
        self.replay_overlap_us = _bounded_integer(
            replay_overlap_seconds,
            name="replay_overlap_seconds",
            minimum=0,
            maximum=300,
        ) * 1_000_000
        self._connect_factory = connect_factory or NoRedirectWebSocketConnect
        self._validate_dns = connect_factory is None
        self._cursor_us = 0
        self._highest_time_us = 0

    def restore_checkpoint(self, payload: dict[str, object]) -> None:
        super().restore_checkpoint(payload)
        raw_cursor = payload.get("cursorTimeUs", 0)
        if isinstance(raw_cursor, bool):
            raise ConnectorError("Bluesky checkpoint cursorTimeUs must be a non-negative integer")
        try:
            cursor = int(raw_cursor)
        except (TypeError, ValueError) as exc:
            raise ConnectorError("Bluesky checkpoint cursorTimeUs must be a non-negative integer") from exc
        if cursor < 0 or payload.get("cursorVersion", JETSTREAM_CURSOR_VERSION) != JETSTREAM_CURSOR_VERSION:
            raise ConnectorError("Bluesky checkpoint version or cursor is unsupported")
        self._cursor_us = cursor
        self._highest_time_us = cursor

    def next_checkpoint(self, observations: list[Observation]) -> dict[str, object]:
        del observations
        return {
            **self.checkpoint,
            "cursorVersion": JETSTREAM_CURSOR_VERSION,
            "cursorTimeUs": max(self._cursor_us, self._highest_time_us),
            "replayOverlapUs": self.replay_overlap_us,
            "completedAt": utcnow().isoformat(),
        }

    def subscription_url(self, *, replay_overlap: bool = True, endpoint: str | None = None) -> str:
        params: list[tuple[str, str]] = [
            ("wantedCollections", JETSTREAM_COLLECTION),
            ("maxMessageSizeBytes", str(self.max_message_size_bytes)),
        ]
        if self._cursor_us:
            cursor = self._cursor_us - self.replay_overlap_us if replay_overlap else self._cursor_us
            params.append(("cursor", str(max(0, cursor))))
        active_endpoint = endpoint or self.endpoint
        separator = "&" if "?" in active_endpoint else "?"
        return f"{active_endpoint}{separator}{urlencode(params)}"

    def _matches_keywords(self, text: str) -> bool:
        normalized = sanitize_external_text(text).casefold()
        for keyword in self.keywords:
            if keyword.isascii() and keyword.isalnum() and len(keyword) <= 3:
                if re.search(rf"(?<![a-z0-9]){re.escape(keyword)}(?![a-z0-9])", normalized):
                    return True
            elif keyword in normalized:
                return True
        return False

    def _candidate(self, event: dict[str, object]) -> dict[str, object] | None:
        if event.get("kind") != "commit":
            return None
        did = event.get("did")
        commit = event.get("commit")
        if not isinstance(did, str) or not did.startswith("did:") or not isinstance(commit, dict):
            return None
        if commit.get("operation") not in {"create", "update"} or commit.get("collection") != JETSTREAM_COLLECTION:
            return None
        rkey = commit.get("rkey")
        record = commit.get("record")
        cid = commit.get("cid")
        if not isinstance(rkey, str) or not rkey or not isinstance(record, dict) or not isinstance(cid, str):
            return None
        text = record.get("text")
        if not isinstance(text, str) or not text.strip() or not self._matches_keywords(text):
            return None
        uri = f"at://{did}/{JETSTREAM_COLLECTION}/{rkey}"
        return {"event": event, "did": did, "rkey": rkey, "cid": cid, "uri": uri, "record": record}

    async def _read_candidates(self) -> list[dict[str, object]]:
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            active_endpoint = self.endpoints[attempt % len(self.endpoints)]
            try:
                if self._validate_dns:
                    await ensure_safe_public_websocket_url_resolved(active_endpoint)
                # First reconnect with a small overlap. If that entire bounded
                # read contains only already-checkpointed events, reconnect once
                # at the exact cursor so a dense replay window cannot livelock.
                for replay_overlap in (True, False):
                    if not replay_overlap and (not self._cursor_us or not self.replay_overlap_us):
                        break
                    received = 0
                    valid_events = 0
                    novel_events = 0
                    reached_limit = False
                    attempt_highest = self._cursor_us
                    candidates: list[dict[str, object]] = []
                    candidate_keys: set[tuple[str, str]] = set()
                    self._request_count += 1
                    async with self._connect_factory(
                        self.subscription_url(replay_overlap=replay_overlap, endpoint=active_endpoint),
                        open_timeout=10,
                        close_timeout=5,
                        ping_interval=20,
                        ping_timeout=20,
                        max_size=self.max_message_size_bytes,
                        compression=None,
                        proxy=None,
                        user_agent_header="signal-ai-radar/0.3",
                    ) as socket:
                        for index in range(self.max_messages):
                            try:
                                message = await asyncio.wait_for(socket.recv(), timeout=self.idle_timeout_seconds)
                            except TimeoutError:
                                break
                            received += 1
                            reached_limit = index + 1 == self.max_messages
                            if isinstance(message, bytes):
                                if len(message) > self.max_message_size_bytes:
                                    raise ConnectorError("Jetstream message exceeds configured maximum")
                                message = message.decode("utf-8")
                            if not isinstance(message, str) or len(message.encode("utf-8")) > self.max_message_size_bytes:
                                raise ConnectorError("Jetstream message is invalid or oversized")
                            try:
                                event = json.loads(message)
                            except json.JSONDecodeError:
                                continue
                            if not isinstance(event, dict) or type(event.get("time_us")) is not int:
                                continue
                            time_us = int(event["time_us"])
                            collected = utcnow()
                            if time_us < 0 or time_us > int((collected + timedelta(minutes=5)).timestamp() * 1_000_000):
                                raise ConnectorError("Jetstream event cursor is negative or implausibly in the future")
                            valid_events += 1
                            if time_us <= self._cursor_us:
                                continue
                            novel_events += 1
                            attempt_highest = max(attempt_highest, time_us)
                            candidate = self._candidate(event)
                            if candidate is not None:
                                key = (str(candidate["uri"]), str(candidate["cid"]))
                                if key not in candidate_keys:
                                    candidates.append(candidate)
                                    candidate_keys.add(key)
                                if len(candidates) >= MAX_APPVIEW_URIS:
                                    # Do not read past the last candidate this
                                    # AppView batch can verify. Its time_us is
                                    # the only safe checkpoint for this pass.
                                    reached_limit = False
                                    break
                    if received and not valid_events:
                        raise ConnectorError("Jetstream returned messages without a recognized event envelope")
                    if replay_overlap and reached_limit and not novel_events and self._cursor_us and self.replay_overlap_us:
                        continue
                    self._highest_time_us = max(self._highest_time_us, attempt_highest)
                    return candidates
                raise ConnectorError("Jetstream replay window exceeded the bounded read without cursor progress")
            except (OSError, TimeoutError, UnicodeDecodeError, WebSocketException, ConnectorError) as exc:
                last_error = exc
                if attempt + 1 < self.max_attempts:
                    await asyncio.sleep(min(2 ** attempt, 4))
        raise ConnectorError(f"bluesky failed after {self.max_attempts} attempts: {last_error}")

    async def _verified_posts(self, candidates: list[dict[str, object]]) -> dict[tuple[str, str], dict[str, object]]:
        if not candidates:
            return {}
        try:
            response = await self.get(
                self.appview_endpoint,
                params=[("uris", str(candidate["uri"])) for candidate in candidates],
            )
            payload = response.json()
        except ValueError as exc:
            raise ConnectorError("Bluesky AppView returned invalid JSON; checkpoint is not safe") from exc
        posts = payload.get("posts") if isinstance(payload, dict) else None
        if not isinstance(posts, list):
            raise ConnectorError("Bluesky AppView response is missing a posts list; checkpoint is not safe")
        verified: dict[tuple[str, str], dict[str, object]] = {}
        expected = {
            (str(candidate["uri"]), str(candidate["cid"])): candidate for candidate in candidates
        }
        for post in posts:
            if not isinstance(post, dict) or not isinstance(post.get("uri"), str):
                continue
            key = (str(post["uri"]), str(post.get("cid", "")))
            candidate = expected.get(key)
            author = post.get("author")
            record = post.get("record")
            if (
                candidate is None
                or not isinstance(author, dict)
                or author.get("did") != candidate["did"]
                or not isinstance(record, dict)
                or not isinstance(record.get("text"), str)
                or not self._matches_keywords(str(record["text"]))
            ):
                continue
            verified[key] = post
        return verified

    async def collect(self) -> list[Observation]:
        candidates = await self._read_candidates()
        verified = await self._verified_posts(candidates)
        collected = utcnow()
        observations: list[Observation] = []
        for candidate in candidates:
            uri = str(candidate["uri"])
            post = verified.get((uri, str(candidate["cid"])))
            # Exact AppView identity is recorded in the envelope for analyst
            # inspection, but remains discovery-only until update/delete
            # supersession and retraction-triggered rescoring are implemented.
            provenance = "unverified_discovery"
            record = post["record"] if post else candidate["record"]
            assert isinstance(record, dict)
            text = sanitize_external_text(str(record.get("text", "")))[:MAX_STORED_EXCERPT_CHARACTERS]
            if not text:
                continue
            event = candidate["event"]
            assert isinstance(event, dict)
            event_time = datetime.fromtimestamp(int(event["time_us"]) / 1_000_000, tz=timezone.utc)
            published = _parse_datetime(record.get("createdAt"), event_time, collected)
            did = str(candidate["did"])
            rkey = str(candidate["rkey"])
            url = normalize_url(
                f"https://bsky.app/profile/{quote(did, safe=':')}/post/{quote(rkey, safe='')}",
            )
            raw_ref = self.item_raw_ref(
                "bluesky/discovery", f"{uri}:{candidate['cid']}", collected,
            )
            envelope = {
                "jetstream": event,
                "appView": post,
                "identityVerified": post is not None,
                "provenanceLevel": provenance,
            }
            await self.archive(raw_ref, json.dumps(envelope, ensure_ascii=False).encode())
            relation = "unknown" if record.get("reply") else "quote" if record.get("embed") else "original"
            observations.append(Observation(
                id=f"bluesky:{hashlib.sha256(uri.encode()).hexdigest()[:32]}",
                platform=self.platform,
                externalId=uri,
                sourceId=f"bluesky:{did}",
                accountId=f"bluesky:{did}",
                publishedAt=published,
                collectedAt=collected,
                language=language_hint(text),
                title=text[:160],
                text=text,
                url=url,
                metrics={},
                rawEvidenceRef=raw_ref,
                rightsPolicyId=self.rights_policy_id,
                relation=relation,
                contentFingerprint=content_fingerprint(text[:160], text, url),
                signalFamily="discussion",
                provenanceLevel=provenance,
            ))
        return observations
