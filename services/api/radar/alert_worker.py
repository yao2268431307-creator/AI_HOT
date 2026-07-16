from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone

import httpx
from redis.asyncio import Redis

from .alerts import AlertCandidate, AlertDecision, AlertPolicyEngine, deliver_webhook
from .contracts import RadarEvent
from .deletion import SourceDeletionConsumer
from .evidence_store import LocalEvidenceStore, S3EvidenceStore
from .storage import InMemoryRepository, PostgresRepository


STATE_RANK = {
    "insufficient_data": 0, "noise": 0, "dormant": 1, "detected": 2,
    "emerging": 3, "cooling": 3, "accelerating": 4, "established": 5,
}


@dataclass(frozen=True, slots=True)
class DispatchSummary:
    evaluated: int
    delivered: int
    skipped: int


class AlertDispatcher:
    """Match committed score events to workspace rules and durable budgets."""

    def __init__(
        self,
        repository: InMemoryRepository | PostgresRepository,
        *,
        signing_secret: str,
        signing_key_id: str = "primary",
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.repository = repository
        self.signing_secret = signing_secret
        self.signing_key_id = signing_key_id
        self.client = client

    @staticmethod
    def _candidate(event: RadarEvent, workspace_id: str, previous: dict[str, object] | None, now: datetime) -> AlertCandidate:
        previous_count = int(previous["evidenceCount"]) if previous else 0
        previous_state = str(previous["lifecycleState"]) if previous else "insufficient_data"
        evidence_count = event.evidence_count or len(event.evidence)
        return AlertCandidate(
            workspace_id=workspace_id, event_id=event.id, domain=event.event_type.value,
            lifecycle_state=event.state.value, evidence_strength=event.evidence_strength.value,
            new_evidence_count=max(0, evidence_count - previous_count),
            state_upgraded=STATE_RANK[event.state.value] > STATE_RANK.get(previous_state, 0),
            occurred_at=now,
        )

    async def dispatch_event(self, event_id: str, workspace_ids: list[str], now: datetime | None = None) -> DispatchSummary:
        event = self.repository.get_event(event_id)
        if event is None:
            return DispatchSummary(0, 0, 0)
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        day_start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
        cooldown_start = now - timedelta(hours=4)
        evaluated = delivered = skipped = 0
        for workspace_id in workspace_ids:
            daily_history = self.repository.list_alert_deliveries(workspace_id, day_start)
            cooldown_history = self.repository.list_alert_deliveries(workspace_id, cooldown_start)
            history_by_key = {
                (str(item["ruleId"]), str(item["eventId"]), item["deliveredAt"]): item
                for item in daily_history + cooldown_history
            }
            history = list(history_by_key.values())
            policy = AlertPolicyEngine()
            for item in history:
                policy.record_delivery(AlertCandidate(
                    workspace_id, str(item["eventId"]), str(item["domain"]), str(item["lifecycleState"]),
                    str(item["evidenceStrength"]), int(item["evidenceCount"]), False, item["deliveredAt"],
                ))
            # Evidence deltas compare against the last successful delivery, not
            # merely today's or the cooldown-window history.
            previous = self.repository.last_alert_delivery(workspace_id, event.id)
            for rule_id, rule in self.repository.list_alert_rules(workspace_id):
                if rule.event_types and event.event_type not in rule.event_types:
                    continue
                if event.attention < rule.minimum_attention or event.evidence_score < rule.minimum_evidence_strength:
                    continue
                evaluated += 1
                candidate = self._candidate(event, workspace_id, previous, now)
                # Strong alerts must be independently auditable from the message.
                # Low-evidence events stay in the review queue even when a
                # workspace rule was configured too permissively.
                if event.evidence_strength.value == "low" or len(event.evidence) < 3:
                    skipped += 1
                    continue
                decision: AlertDecision = policy.evaluate(candidate)
                if not decision.allowed:
                    skipped += 1
                    continue
                idempotency_key = f"{workspace_id}:{rule_id}:{event.id}:{event.cluster_version}:{event.score_version}:{event.updated_at.isoformat()}"
                channel = "webhook" if rule.webhook_url else "in_app"
                delivery = {
                    "ruleId": rule_id, "workspaceId": workspace_id, "eventId": event.id,
                    "domain": event.event_type.value, "lifecycleState": event.state.value,
                    "evidenceStrength": event.evidence_strength.value, "evidenceCount": event.evidence_count or len(event.evidence),
                    "channel": channel, "deliveredAt": now, "idempotencyKey": idempotency_key,
                }
                if not self.repository.reserve_alert_delivery(delivery, allow_cooldown_bypass=candidate.state_upgraded):
                    skipped += 1
                    continue
                if rule.webhook_url:
                    if not self.signing_secret:
                        self.repository.release_alert_reservation(idempotency_key, workspace_id)
                        raise RuntimeError("WEBHOOK_SIGNING_SECRET is required for webhook rules")
                    payload = {
                        "eventId": event.id, "title": event.title, "lifecycleState": event.state.value,
                        "structureLabels": [label.value for label in event.labels],
                        "attention": event.attention, "behavior": event.behavior,
                        "evidenceStrength": event.evidence_strength.value, "coverageNote": event.coverage_note,
                        "evidence": [item.model_dump(mode="json", by_alias=True) for item in event.evidence[:3]],
                        "scoreVersion": event.score_version, "clusterVersion": event.cluster_version,
                    }
                    try:
                        result = await deliver_webhook(
                            str(rule.webhook_url), payload, self.signing_secret, self.client,
                            idempotency_key=idempotency_key, key_id=self.signing_key_id,
                        )
                    except Exception:
                        self.repository.release_alert_reservation(idempotency_key, workspace_id)
                        raise
                    if not result.delivered:
                        self.repository.release_alert_reservation(idempotency_key, workspace_id)
                        raise RuntimeError(f"webhook delivery failed for rule {rule_id}")
                self.repository.confirm_alert_delivery(idempotency_key, workspace_id)
                policy.record_delivery(candidate)
                previous = delivery
                delivered += 1
        return DispatchSummary(evaluated, delivered, skipped)


class RedisAlertWorker:
    def __init__(self, redis: Redis, dispatcher: AlertDispatcher, workspace_ids: list[str], *, deletion_consumer: SourceDeletionConsumer | None = None, stream: str = "radar:events", group: str = "radar-alerts", consumer: str = "alert-1", max_deliveries: int = 5) -> None:
        self.redis = redis
        self.dispatcher = dispatcher
        self.workspace_ids = workspace_ids
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.max_deliveries = max_deliveries
        self.deletion_consumer = deletion_consumer

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run_once(self, *, block_ms: int = 5000, count: int = 50) -> int:
        await self.ensure_group()
        claimed = await self.redis.xautoclaim(self.stream, self.group, self.consumer, min_idle_time=60_000, start_id="0-0", count=count)
        claimed_messages = claimed[1] if len(claimed) > 1 else []
        batches = await self.redis.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=count, block=block_ms)
        handled = 0
        messages = list(claimed_messages)
        for _, new_messages in batches:
            messages.extend(new_messages)
        for message_id, fields in messages:
            try:
                if fields.get("kind") == "score.created":
                    await self.dispatcher.dispatch_event(fields["aggregate_id"], self.workspace_ids)
                elif fields.get("kind") == "source.erased" and self.deletion_consumer:
                    await self.deletion_consumer.handle(json.loads(fields.get("payload", "{}")))
                await self.redis.xack(self.stream, self.group, message_id)
                handled += 1
            except Exception as exc:
                pending = await self.redis.xpending_range(self.stream, self.group, min=message_id, max=message_id, count=1)
                deliveries = int(pending[0].get("times_delivered", 1)) if pending else 1
                if deliveries >= self.max_deliveries:
                    await self.redis.xadd(f"{self.stream}:dlq", {
                        "original_id": message_id, "kind": fields.get("kind", "unknown"),
                        "aggregate_id": fields.get("aggregate_id", "unknown"), "error": str(exc)[:1000],
                        "payload": fields.get("payload", "{}"),
                    })
                    await self.redis.xack(self.stream, self.group, message_id)
                    handled += 1
        return handled


async def run() -> None:
    dsn = os.environ["DATABASE_URL"]
    workspace_ids = [value.strip() for value in os.getenv("RADAR_WORKSPACE_IDS", "").split(",") if value.strip()]
    if not workspace_ids:
        raise RuntimeError("RADAR_WORKSPACE_IDS must list workspaces served by the alert worker")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    if os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        objects = S3EvidenceStore(os.environ["R2_ENDPOINT_URL"], os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"])
    else:
        objects = LocalEvidenceStore(os.getenv("RAW_EVIDENCE_LOCAL_DIR", ".data/evidence"))

    class RedisCacheInvalidator:
        async def invalidate(self, tags: list[str]) -> None:
            await redis.publish("radar:cache:invalidate", json.dumps(tags, ensure_ascii=False))
            if tags:
                await redis.delete(*(f"cache:{tag}" for tag in tags))

    worker = RedisAlertWorker(
        redis,
        AlertDispatcher(
            PostgresRepository(dsn), signing_secret=os.getenv("WEBHOOK_SIGNING_SECRET", ""),
            signing_key_id=os.getenv("WEBHOOK_SIGNING_KEY_ID", "primary"),
        ),
        workspace_ids,
        deletion_consumer=SourceDeletionConsumer(objects, RedisCacheInvalidator()),
        consumer=os.getenv("ALERT_CONSUMER_NAME", "alert-1"),
    )
    try:
        while True:
            await worker.run_once()
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
