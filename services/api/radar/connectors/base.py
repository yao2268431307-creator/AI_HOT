from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import random
import socket
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from urllib.parse import urljoin, urlsplit

import httpx

from ..contracts import Observation
from ..evidence_store import RawEvidenceStore


class ConnectorError(RuntimeError):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def ensure_safe_public_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ConnectorError("only absolute http(s) URLs are allowed")
    host = parts.hostname.lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise ConnectorError("local network destinations are blocked")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
        raise ConnectorError("private network destinations are blocked")


def ensure_safe_public_websocket_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme not in {"ws", "wss"} or not parts.hostname or parts.username or parts.password:
        raise ConnectorError("only credential-free absolute ws(s) URLs are allowed")
    host = parts.hostname.lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise ConnectorError("local network destinations are blocked")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if address.is_private or address.is_loopback or address.is_link_local or address.is_reserved:
        raise ConnectorError("private network destinations are blocked")


def _resolve_public_host(host: str) -> None:
    try:
        addresses = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ConnectorError(f"cannot resolve upstream host: {host}") from exc
    for address in addresses:
        ip = ipaddress.ip_address(address[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ConnectorError("upstream DNS resolved to a private network destination")


async def ensure_safe_public_url_resolved(url: str) -> None:
    ensure_safe_public_url(url)
    host = urlsplit(url).hostname
    if host:
        await asyncio.to_thread(_resolve_public_host, host)


async def ensure_safe_public_websocket_url_resolved(url: str) -> None:
    ensure_safe_public_websocket_url(url)
    host = urlsplit(url).hostname
    if host:
        await asyncio.to_thread(_resolve_public_host, host)


class BaseConnector(ABC):
    id: str
    platform: str
    signal_family: str
    metered: bool = False
    expected_requests_per_collect: int = 1
    rights_policy_id: str = "metadata-and-excerpt"

    def __init__(self, client: httpx.AsyncClient | None = None, *, max_attempts: int = 3, evidence_store: RawEvidenceStore | None = None) -> None:
        self._owned_client = client is None
        self.client = client or httpx.AsyncClient(timeout=15, follow_redirects=True, headers={"User-Agent": "signal-ai-radar/0.3"})
        self.max_attempts = max_attempts
        self.evidence_store = evidence_store
        self.evidence_bucket = os.getenv("RAW_EVIDENCE_BUCKET", "raw")
        cost_key = f"CONNECTOR_COST_RMB_{self.id.upper().replace('-', '_')}"
        configured_cost = os.getenv(cost_key)
        self.estimated_cost_per_request_rmb: float | None = float(configured_cost) if configured_cost not in {None, ""} else (0.0 if not self.metered else None)
        self._request_count = 0
        self.checkpoint: dict[str, object] = {}

    def restore_checkpoint(self, payload: dict[str, object]) -> None:
        """Allow provider-specific connectors to resume from durable state."""
        self.checkpoint = payload.copy()

    def next_checkpoint(self, observations: list[Observation]) -> dict[str, object]:
        if not observations:
            return self.checkpoint.copy()
        latest = max(observations, key=lambda item: (item.published_at, item.external_id))
        return {
            **self.checkpoint,
            "latestPublishedAt": latest.published_at.isoformat(),
            "latestExternalId": latest.external_id,
            "completedAt": utcnow().isoformat(),
        }

    def drain_request_count(self) -> int:
        count = self._request_count
        self._request_count = 0
        return count

    def raw_ref(self, key: str) -> str:
        return f"r2://{self.evidence_bucket}/{key.lstrip('/')}"

    def item_raw_ref(self, namespace: str, external_id: str, collected_at: datetime, extension: str = "json") -> str:
        """Return a per-item revision key so one source can be physically erased.

        A search response may contain records owned by many independent sources;
        storing the whole response as one object makes selective deletion
        impossible. The external id is hashed to keep object keys predictable
        without leaking arbitrary path characters.
        """
        digest = hashlib.sha256(external_id.encode("utf-8")).hexdigest()[:24]
        revision = int(collected_at.timestamp() * 1_000_000)
        return self.raw_ref(f"{namespace}/items/{digest}/{revision}.{extension}")

    async def archive(self, reference: str, body: bytes, content_type: str = "application/json") -> None:
        if self.evidence_store:
            await self.evidence_store.put(reference, body, content_type)

    async def close(self) -> None:
        if self._owned_client:
            await self.client.aclose()

    async def get(self, url: str, **kwargs: object) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(self.max_attempts):
            try:
                current_url = url
                for _ in range(6):
                    ensure_safe_public_url(current_url)
                    if self._owned_client:
                        host = urlsplit(current_url).hostname
                        if host:
                            await asyncio.to_thread(_resolve_public_host, host)
                    self._request_count += 1
                    response = await self.client.get(current_url, follow_redirects=False, **kwargs)
                    if response.status_code not in {301, 302, 303, 307, 308}:
                        break
                    location = response.headers.get("location")
                    if not location:
                        raise ConnectorError("redirect response has no location")
                    current_url = urljoin(current_url, location)
                else:
                    raise ConnectorError("too many upstream redirects")
                if response.status_code == 429 or response.status_code >= 500:
                    raise ConnectorError(f"upstream returned {response.status_code}")
                response.raise_for_status()
                return response
            except (httpx.HTTPError, ConnectorError) as exc:
                last_error = exc
                if attempt + 1 < self.max_attempts:
                    await asyncio.sleep(min(2 ** attempt + random.random() * 0.15, 4.0))
        raise ConnectorError(f"{self.id} failed after {self.max_attempts} attempts: {last_error}")

    @abstractmethod
    async def collect(self) -> list[Observation]:
        raise NotImplementedError
