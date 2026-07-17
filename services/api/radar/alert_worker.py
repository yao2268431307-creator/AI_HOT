from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib
import json
import os
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone

import httpx
from redis.asyncio import Redis

from .alerts import AlertCandidate, AlertDecision, AlertPolicyEngine, deliver_webhook
from .contracts import AlertRuleRequest, Evidence, RadarEvent
from .deletion import SourceDeletionConsumer
from .evidence_store import LocalEvidenceStore, S3EvidenceStore
from .outbox import register_stream_participant, release_stream_participant, renew_stream_participant
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
    def _verified_evidence(event: RadarEvent) -> list[Evidence]:
        return [item for item in event.evidence if item.provenance_level != "unverified_discovery"]

    @classmethod
    def _verified_evidence_count(cls, event: RadarEvent) -> int:
        return max(event.evidence_count, len(cls._verified_evidence(event)))

    @staticmethod
    def _candidate(event: RadarEvent, workspace_id: str, previous: dict[str, object] | None, now: datetime) -> AlertCandidate:
        previous_count = int(previous["evidenceCount"]) if previous else 0
        previous_state = str(previous["lifecycleState"]) if previous else "insufficient_data"
        evidence_count = AlertDispatcher._verified_evidence_count(event)
        return AlertCandidate(
            workspace_id=workspace_id, event_id=event.id, domain=event.event_type.value,
            lifecycle_state=event.state.value, evidence_strength=event.evidence_strength.value,
            new_evidence_count=max(0, evidence_count - previous_count),
            state_upgraded=STATE_RANK[event.state.value] > STATE_RANK.get(previous_state, 0),
            occurred_at=now,
        )

    async def _deliver_webhook_if_configured(
        self,
        event: RadarEvent,
        rule: AlertRuleRequest,
        idempotency_key: str,
    ) -> None:
        if not rule.webhook_url:
            return
        if not self.signing_secret:
            raise RuntimeError("WEBHOOK_SIGNING_SECRET is required for webhook rules")
        payload = {
            "eventId": event.id, "title": event.title, "lifecycleState": event.state.value,
            "structureLabels": [label.value for label in event.labels],
            "attention": event.attention, "behavior": event.behavior,
            "evidenceStrength": event.evidence_strength.value, "coverageNote": event.coverage_note,
            "evidence": [
                item.model_dump(mode="json", by_alias=True)
                for item in self._verified_evidence(event)[:3]
            ],
            "scoreVersion": event.score_version, "clusterVersion": event.cluster_version,
        }
        result = await deliver_webhook(
            str(rule.webhook_url), payload, self.signing_secret, self.client,
            idempotency_key=idempotency_key, key_id=self.signing_key_id,
        )
        if not result.delivered:
            raise RuntimeError("webhook delivery failed")

    async def dispatch_event(
        self,
        event_id: str,
        workspace_ids: list[str],
        now: datetime | None = None,
        *,
        delivery_key: str | None = None,
    ) -> DispatchSummary:
        now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        message_token = hashlib.sha256(delivery_key.encode()).hexdigest() if delivery_key else None
        evaluated = delivered = skipped = 0

        # Recover durable in-app reservations using the immutable stream
        # identity before consulting mutable/current event state. Webhooks need
        # the event and rule below so they can be retried with the same key.
        if message_token:
            for workspace_id in workspace_ids:
                for reservation in self.repository.list_alert_reservations_for_message(
                    workspace_id, event_id, message_token,
                ):
                    if reservation.get("channel") == "in_app":
                        self.repository.confirm_alert_delivery(str(reservation["idempotencyKey"]), workspace_id)
                        delivered += 1

        event = self.repository.get_event(event_id)
        if event is None:
            if message_token:
                for workspace_id in workspace_ids:
                    for reservation in self.repository.list_alert_reservations_for_message(
                        workspace_id, event_id, message_token,
                    ):
                        self.repository.abort_alert_reservation(
                            str(reservation["idempotencyKey"]),
                            "event snapshot unavailable during reserved webhook recovery",
                            workspace_id,
                        )
                        skipped += 1
            return DispatchSummary(evaluated, delivered, skipped)

        if message_token is None:
            legacy_identity = f"{event.id}:{event.cluster_version}:{event.score_version}:{event.updated_at.isoformat()}"
            message_token = hashlib.sha256(legacy_identity.encode()).hexdigest()
        day_start = datetime.combine(now.date(), time.min, tzinfo=timezone.utc)
        cooldown_start = now - timedelta(hours=4)
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
                if item.get("status", "delivered") == "aborted":
                    continue
                policy.record_delivery(AlertCandidate(
                    workspace_id, str(item["eventId"]), str(item["domain"]), str(item["lifecycleState"]),
                    str(item["evidenceStrength"]), int(item["evidenceCount"]), False, item["deliveredAt"],
                ))
            # Evidence deltas compare against the last successful delivery, not
            # merely today's or the cooldown-window history.
            previous = self.repository.last_alert_delivery(workspace_id, event.id)
            for rule_id, rule in self.repository.list_alert_rules(workspace_id):
                idempotency_key = f"{workspace_id}:{rule_id}:{event.id}:{message_token}"
                existing = self.repository.get_alert_delivery_by_idempotency(workspace_id, idempotency_key)
                if existing:
                    evaluated += 1
                    if existing.get("status") in {"delivered", "aborted"}:
                        skipped += 1
                        continue
                    if existing.get("status") != "reserved":
                        raise RuntimeError(f"unsupported alert delivery state {existing.get('status')}")
                    candidate = self._candidate(event, workspace_id, previous, now)
                    # The receiver may already have accepted the first attempt;
                    # retry with the same idempotency key before durable confirm.
                    await self._deliver_webhook_if_configured(event, rule, idempotency_key)
                    self.repository.confirm_alert_delivery(idempotency_key, workspace_id)
                    policy.record_delivery(candidate)
                    previous = {**existing, "status": "delivered"}
                    delivered += 1
                    continue
                if rule.event_types and event.event_type not in rule.event_types:
                    continue
                if event.attention < rule.minimum_attention or event.evidence_score < rule.minimum_evidence_strength:
                    continue
                evaluated += 1
                candidate = self._candidate(event, workspace_id, previous, now)
                # Strong alerts must be independently auditable from the message.
                # Low-evidence events stay in the review queue even when a
                # workspace rule was configured too permissively.
                verified_evidence = self._verified_evidence(event)
                if event.evidence_strength.value == "low" or len(verified_evidence) < 3:
                    skipped += 1
                    continue
                decision: AlertDecision = policy.evaluate(candidate)
                if not decision.allowed:
                    skipped += 1
                    continue
                channel = "webhook" if rule.webhook_url else "in_app"
                delivery = {
                    "ruleId": rule_id, "workspaceId": workspace_id, "eventId": event.id,
                    "domain": event.event_type.value, "lifecycleState": event.state.value,
                    "evidenceStrength": event.evidence_strength.value,
                    "evidenceCount": self._verified_evidence_count(event),
                    "channel": channel, "deliveredAt": now, "idempotencyKey": idempotency_key,
                }
                if not self.repository.reserve_alert_delivery(delivery, allow_cooldown_bypass=candidate.state_upgraded):
                    skipped += 1
                    continue
                try:
                    await self._deliver_webhook_if_configured(event, rule, idempotency_key)
                except Exception:
                    self.repository.release_alert_reservation(idempotency_key, workspace_id)
                    raise
                self.repository.confirm_alert_delivery(idempotency_key, workspace_id)
                policy.record_delivery(candidate)
                previous = delivery
                delivered += 1
        if delivery_key:
            for workspace_id in workspace_ids:
                for reservation in self.repository.list_alert_reservations_for_message(
                    workspace_id, event_id, message_token,
                ):
                    self.repository.abort_alert_reservation(
                        str(reservation["idempotencyKey"]),
                        "alert rule unavailable during reserved webhook recovery",
                        workspace_id,
                    )
                    skipped += 1
        return DispatchSummary(evaluated, delivered, skipped)

    def abort_message_reservations(
        self,
        event_id: str,
        workspace_ids: list[str],
        delivery_key: str,
        reason: str,
    ) -> int:
        message_token = hashlib.sha256(delivery_key.encode()).hexdigest()
        aborted = 0
        for workspace_id in workspace_ids:
            for reservation in self.repository.list_alert_reservations_for_message(
                workspace_id, event_id, message_token,
            ):
                self.repository.abort_alert_reservation(
                    str(reservation["idempotencyKey"]), reason, workspace_id,
                )
                aborted += 1
        return aborted


class RedisAlertWorker:
    def __init__(self, redis: Redis, dispatcher: AlertDispatcher, workspace_ids: list[str], *, deletion_consumer: SourceDeletionConsumer | None = None, stream: str = "radar:events", group: str = "radar-alerts", consumer: str = "alert-1", max_deliveries: int = 5, claim_min_idle_ms: int = 60_000) -> None:
        if max_deliveries < 1:
            raise ValueError("max_deliveries must be at least 1")
        if claim_min_idle_ms < 0:
            raise ValueError("claim_min_idle_ms cannot be negative")
        self.redis = redis
        self.dispatcher = dispatcher
        self.workspace_ids = workspace_ids
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.max_deliveries = max_deliveries
        self.claim_min_idle_ms = claim_min_idle_ms
        self.deletion_consumer = deletion_consumer
        self.claim_cursor = "0-0"

    async def _maintain_participant_lease(self, token: str, lost: asyncio.Event) -> None:
        while not lost.is_set():
            try:
                await asyncio.wait_for(lost.wait(), timeout=30)
                return
            except TimeoutError:
                pass
            try:
                await renew_stream_participant(self.redis, self.stream, token)
            except Exception:
                lost.set()
                return

    @staticmethod
    def _score_delivery_key(fields: dict[str, str]) -> str:
        if fields.get("outbox_id"):
            return str(fields["outbox_id"])
        payload = json.loads(fields.get("payload", "{}"))
        if not isinstance(payload, dict):
            raise RuntimeError("score.created payload must be a JSON object")
        delivery_key = payload.get("cycleId") or payload.get("inputDigest")
        if not delivery_key:
            raise RuntimeError("score.created message has no stable delivery identity")
        return str(delivery_key)

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def run_once(self, *, block_ms: int = 5000, count: int = 50) -> int:
        participant_token = await register_stream_participant(
            self.redis, self.stream, f"consumer:{self.group}:{self.consumer}:{secrets.token_hex(4)}",
        )
        participant_lost = asyncio.Event()
        lease_task = asyncio.create_task(self._maintain_participant_lease(participant_token, participant_lost))
        try:
            await self.ensure_group()
            await renew_stream_participant(self.redis, self.stream, participant_token)
            claimed = await self.redis.xautoclaim(
                self.stream, self.group, self.consumer,
                min_idle_time=self.claim_min_idle_ms, start_id=self.claim_cursor, count=count,
            )
            self.claim_cursor = str(claimed[0]) if claimed else "0-0"
            claimed_messages = claimed[1] if len(claimed) > 1 else []
            deleted_pending_ids = claimed[2] if len(claimed) > 2 else []
            if deleted_pending_ids:
                for deleted_id in deleted_pending_ids:
                    await self.redis.xadd(f"{self.stream}:dlq", {
                        "original_id": deleted_id, "outbox_id": "unknown",
                        "kind": "pending.deleted", "aggregate_id": "unknown",
                        "error": "pending message was deleted outside the consumer before ACK",
                        "payload": "{}",
                    })
                raise RuntimeError(
                    f"{len(deleted_pending_ids)} pending stream message(s) were deleted before ACK",
                )
            batches = await self.redis.xreadgroup(self.group, self.consumer, {self.stream: ">"}, count=count, block=block_ms)
            handled = 0
            messages = list(claimed_messages)
            for _, new_messages in batches:
                messages.extend(new_messages)
            for message_id, fields in messages:
                await renew_stream_participant(self.redis, self.stream, participant_token)
                try:
                    if fields.get("kind") == "score.created":
                        delivery_key = self._score_delivery_key(fields)
                        await self.dispatcher.dispatch_event(
                            fields["aggregate_id"], self.workspace_ids, delivery_key=delivery_key,
                        )
                    elif fields.get("kind") == "source.erased" and self.deletion_consumer:
                        await self.deletion_consumer.handle(json.loads(fields.get("payload", "{}")))
                    if participant_lost.is_set():
                        raise RuntimeError("stream participant lease was lost before ACK")
                    await self.redis.xack(self.stream, self.group, message_id)
                    handled += 1
                except Exception as exc:
                    if participant_lost.is_set():
                        raise RuntimeError("stream participant lease was lost; message remains pending") from exc
                    pending = await self.redis.xpending_range(self.stream, self.group, min=message_id, max=message_id, count=1)
                    deliveries = int(pending[0].get("times_delivered", 1)) if pending else 1
                    if deliveries >= self.max_deliveries:
                        delivery_key: str | None = None
                        if fields.get("kind") == "score.created":
                            try:
                                delivery_key = self._score_delivery_key(fields)
                            except (RuntimeError, ValueError, TypeError, json.JSONDecodeError):
                                pass
                        abort_reservations = getattr(self.dispatcher, "abort_message_reservations", None)
                        if fields.get("kind") == "score.created" and delivery_key and abort_reservations:
                            abort_reservations(
                                fields.get("aggregate_id", "unknown"), self.workspace_ids, str(delivery_key),
                                f"delivery moved to DLQ after {deliveries} attempts: {str(exc)[:500]}",
                            )
                        await self.redis.xadd(f"{self.stream}:dlq", {
                            "original_id": message_id, "outbox_id": fields.get("outbox_id", "unknown"),
                            "kind": fields.get("kind", "unknown"),
                            "aggregate_id": fields.get("aggregate_id", "unknown"), "error": str(exc)[:1000],
                            "payload": fields.get("payload", "{}"),
                        })
                        await self.redis.xack(self.stream, self.group, message_id)
                        handled += 1
            return handled
        finally:
            participant_lost.set()
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_task
            active_exception = sys.exc_info()[0] is not None
            try:
                await release_stream_participant(self.redis, self.stream, participant_token)
            except Exception:
                if not active_exception:
                    raise


async def probe_alert_dependencies(
    redis: Redis, evidence_store: LocalEvidenceStore | S3EvidenceStore, bucket: str,
) -> tuple[bool, bool, list[str]]:
    """Re-probe the alert worker's Redis and delete-only R2 capabilities."""
    failures: list[str] = []
    try:
        redis_verified = bool(await redis.ping())
    except Exception as exc:  # provider clients expose several transport errors
        redis_verified = False
        failures.append(f"redis:{type(exc).__name__}")
    try:
        await evidence_store.probe_delete(bucket)
        r2_delete_verified = True
    except Exception as exc:  # preserve only the error class in heartbeat/logs
        r2_delete_verified = False
        failures.append(f"r2-delete:{type(exc).__name__}")
    return redis_verified, r2_delete_verified, failures


async def run() -> None:
    dsn = os.environ["DATABASE_URL"]
    repository = PostgresRepository(dsn)
    production = os.getenv("DEMO_MODE", "true").lower() == "false"
    workspace_ids = [value.strip() for value in os.getenv("RADAR_WORKSPACE_IDS", "").split(",") if value.strip()]
    if not workspace_ids:
        raise RuntimeError("RADAR_WORKSPACE_IDS must list workspaces served by the alert worker")
    redis = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    bucket = os.getenv("RAW_EVIDENCE_BUCKET", "")
    if production and not bucket:
        raise RuntimeError("production alert worker requires RAW_EVIDENCE_BUCKET")
    bucket = bucket or "raw-evidence"
    if os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        objects = S3EvidenceStore(os.environ["R2_ENDPOINT_URL"], os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"])
    else:
        if production:
            raise RuntimeError("production alert worker requires R2 credentials and cannot use local evidence")
        objects = LocalEvidenceStore(os.getenv("RAW_EVIDENCE_LOCAL_DIR", ".data/evidence"))

    class RedisCacheInvalidator:
        async def invalidate(self, tags: list[str]) -> None:
            await redis.publish("radar:cache:invalidate", json.dumps(tags, ensure_ascii=False))
            if tags:
                await redis.delete(*(f"cache:{tag}" for tag in tags))

    worker = RedisAlertWorker(
        redis,
        AlertDispatcher(
            repository, signing_secret=os.getenv("WEBHOOK_SIGNING_SECRET", ""),
            signing_key_id=os.getenv("WEBHOOK_SIGNING_KEY_ID", "primary"),
        ),
        workspace_ids,
        deletion_consumer=SourceDeletionConsumer(objects, RedisCacheInvalidator()),
        consumer=os.getenv("ALERT_CONSUMER_NAME", "alert-1"),
        max_deliveries=int(os.getenv("ALERT_MAX_DELIVERIES", "5")),
        claim_min_idle_ms=int(os.getenv("ALERT_CLAIM_MIN_IDLE_MS", "60000")),
    )
    try:
        while True:
            dependency_probe_at = datetime.now(timezone.utc)
            redis_verified, r2_delete_verified, dependency_failures = await probe_alert_dependencies(
                redis, objects, bucket,
            )
            dependency_details = {
                "workspaceCount": len(workspace_ids),
                "dependencyProbeAt": dependency_probe_at.isoformat(),
                "redisVerified": redis_verified,
                "r2DeleteVerified": r2_delete_verified,
                "dependencyProbeFailures": dependency_failures,
            }
            if not redis_verified or not r2_delete_verified:
                repository.heartbeat_runtime_component(
                    "alert-consumer", os.getenv("RADAR_INSTANCE_ID", "local-alert-worker"),
                    dependency_details,
                )
                print(json.dumps({
                    "level": "error", "event": "alert_dependency_probe_failed",
                    **dependency_details,
                }, ensure_ascii=False))
                raise RuntimeError("alert dependency probe failed before stream consumption")
            await worker.run_once()
            repository.heartbeat_runtime_component(
                "alert-consumer", os.getenv("RADAR_INSTANCE_ID", "local-alert-worker"),
                dependency_details,
            )
    finally:
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(run())
