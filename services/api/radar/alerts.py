from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import RLock

import httpx

from .connectors.base import ensure_safe_public_url_resolved


def sign_payload(payload: dict[str, object], secret: str, *, timestamp: int | None = None, key_id: str = "primary") -> tuple[bytes, str]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    issued_at = int(time.time()) if timestamp is None else timestamp
    signed = f"{issued_at}.".encode("ascii") + body
    signature = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return body, f"t={issued_at},kid={key_id},v1={signature}"


def verify_payload(body: bytes, signature: str, secrets: dict[str, str], *, now: int | None = None, replay_window_seconds: int = 300) -> bool:
    try:
        values = dict(part.split("=", 1) for part in signature.split(","))
        issued_at = int(values["t"])
        key_id = values["kid"]
        supplied = values["v1"]
        secret = secrets[key_id]
    except (KeyError, TypeError, ValueError):
        return False
    current = int(time.time()) if now is None else now
    if abs(current - issued_at) > replay_window_seconds:
        return False
    expected = hmac.new(secret.encode("utf-8"), f"{issued_at}.".encode("ascii") + body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(supplied, expected)


@dataclass(slots=True)
class WebhookResult:
    status_code: int
    delivered: bool


@dataclass(frozen=True, slots=True)
class AlertCandidate:
    workspace_id: str
    event_id: str
    domain: str
    lifecycle_state: str
    evidence_strength: str
    new_evidence_count: int
    state_upgraded: bool
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class AlertDecision:
    allowed: bool
    reason: str


class AlertPolicyEngine:
    """Workspace/domain budgets and repeat suppression for strong alerts.

    State is intentionally injectable. A production delivery worker can rebuild
    it from its durable delivery log after restart; tests use the in-memory log.
    """

    def __init__(self, *, workspace_daily_limit: int = 10, domain_daily_limit: int = 3, cooldown: timedelta = timedelta(hours=4)) -> None:
        self.workspace_daily_limit = workspace_daily_limit
        self.domain_daily_limit = domain_daily_limit
        self.cooldown = cooldown
        self._delivered: list[AlertCandidate] = []
        self._lock = RLock()

    def evaluate(self, candidate: AlertCandidate) -> AlertDecision:
        now = candidate.occurred_at.astimezone(timezone.utc)
        day = now.date()
        with self._lock:
            if candidate.lifecycle_state not in {"accelerating", "established"}:
                return AlertDecision(False, "lifecycle state is not eligible for a strong alert")
            if candidate.evidence_strength == "low":
                return AlertDecision(False, "low evidence stays in the review queue")
            if candidate.new_evidence_count <= 0 and not candidate.state_upgraded:
                return AlertDecision(False, "no new evidence or lifecycle upgrade")
            workspace_today = [item for item in self._delivered if item.workspace_id == candidate.workspace_id and item.occurred_at.astimezone(timezone.utc).date() == day]
            if len(workspace_today) >= self.workspace_daily_limit:
                return AlertDecision(False, "workspace daily alert budget exhausted")
            if sum(item.domain == candidate.domain for item in workspace_today) >= self.domain_daily_limit:
                return AlertDecision(False, "domain daily alert budget exhausted")
            # Daily budgets reset at UTC midnight, but the per-event cooldown
            # intentionally does not. Otherwise a 23:59 alert could be repeated
            # one minute later merely because the calendar date changed.
            previous = [item for item in self._delivered if item.workspace_id == candidate.workspace_id and item.event_id == candidate.event_id]
            if previous and not candidate.state_upgraded and now - max(item.occurred_at.astimezone(timezone.utc) for item in previous) < self.cooldown:
                return AlertDecision(False, "event is inside the repeat cooldown")
            return AlertDecision(True, "eligible strong alert")

    def record_delivery(self, candidate: AlertCandidate) -> None:
        with self._lock:
            self._delivered.append(candidate)

    def decide_and_record(self, candidate: AlertCandidate) -> AlertDecision:
        # Keep evaluation and reservation under one lock so parallel delivery
        # workers cannot overshoot the daily budget.
        with self._lock:
            decision = self.evaluate(candidate)
            if decision.allowed:
                self._delivered.append(candidate)
            return decision

    def release_reservation(self, candidate: AlertCandidate) -> None:
        with self._lock:
            for index in range(len(self._delivered) - 1, -1, -1):
                if self._delivered[index] == candidate:
                    del self._delivered[index]
                    break


async def deliver_webhook(url: str, payload: dict[str, object], secret: str, client: httpx.AsyncClient | None = None, *, idempotency_key: str | None = None, key_id: str = "primary", max_body_bytes: int = 256_000) -> WebhookResult:
    await ensure_safe_public_url_resolved(url)
    timestamp = int(time.time())
    body, signature = sign_payload(payload, secret, timestamp=timestamp, key_id=key_id)
    if len(body) > max_body_bytes:
        raise ValueError("webhook body exceeds configured limit")
    idempotency_key = idempotency_key or hashlib.sha256(body).hexdigest()
    owned = client is None
    http = client or httpx.AsyncClient(timeout=10, follow_redirects=False)
    try:
        response = await http.post(url, content=body, headers={
            "Content-Type": "application/json", "X-Signal-Signature": signature,
            "X-Signal-Timestamp": str(timestamp), "X-Signal-Idempotency-Key": idempotency_key,
            "X-Signal-Key-Id": key_id,
        })
        return WebhookResult(response.status_code, 200 <= response.status_code < 300)
    finally:
        if owned:
            await http.aclose()


async def deliver_budgeted_webhook(
    policy: AlertPolicyEngine,
    candidate: AlertCandidate,
    url: str,
    payload: dict[str, object],
    secret: str,
    client: httpx.AsyncClient | None = None,
    *,
    idempotency_key: str | None = None,
    key_id: str = "primary",
) -> tuple[AlertDecision, WebhookResult | None]:
    """Apply alert-fatigue policy before network I/O and record successes only."""
    decision = policy.decide_and_record(candidate)
    if not decision.allowed:
        return decision, None
    result = await deliver_webhook(url, payload, secret, client, idempotency_key=idempotency_key, key_id=key_id)
    if not result.delivered:
        policy.release_reservation(candidate)
    return decision, result
