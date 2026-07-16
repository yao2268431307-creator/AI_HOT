from __future__ import annotations

import os
import uuid
from datetime import timedelta

from botocore.exceptions import ClientError
import pytest
from redis.asyncio import Redis

from radar.alert_worker import AlertDispatcher, RedisAlertWorker
from radar.contracts import AlertRuleRequest
from radar.evidence_store import S3EvidenceStore
from radar.fixtures import seed_repository
from radar.storage import InMemoryRepository


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("REDIS_INTEGRATION_URL"), reason="set REDIS_INTEGRATION_URL for Redis integration")
async def test_redis_stream_group_round_trip() -> None:
    stream = f"integration:radar:{uuid.uuid4().hex}"
    group = "integration-consumers"
    redis = Redis.from_url(os.environ["REDIS_INTEGRATION_URL"], decode_responses=True)
    try:
        assert await redis.ping()
        message_id = await redis.xadd(stream, {"kind": "integration", "aggregate_id": "event-1"})
        await redis.xgroup_create(stream, group, id="0-0")
        batches = await redis.xreadgroup(group, "consumer-1", {stream: ">"}, count=1, block=1000)
        assert len(batches) == 1
        returned_stream, messages = batches[0]
        assert returned_stream == stream
        assert messages == [(message_id, {"kind": "integration", "aggregate_id": "event-1"})]
        assert await redis.xack(stream, group, message_id) == 1
        assert (await redis.xpending(stream, group))["pending"] == 0
    finally:
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("REDIS_INTEGRATION_URL"), reason="set REDIS_INTEGRATION_URL for Redis integration")
async def test_real_redis_worker_retries_then_moves_poison_message_to_dlq() -> None:
    stream = f"integration:radar:{uuid.uuid4().hex}"
    group = "integration-alerts"
    dlq = f"{stream}:dlq"
    trimmed_stream = f"{stream}:trimmed"
    trimmed_group = f"{group}:trimmed"
    fallback_stream = f"{stream}:fallback"
    fallback_group = f"{group}:fallback"
    redis = Redis.from_url(os.environ["REDIS_INTEGRATION_URL"], decode_responses=True)

    class FailingDispatcher:
        async def dispatch_event(self, event_id: str, workspace_ids: list[str], *, delivery_key: str) -> None:
            raise RuntimeError(f"poison event {event_id}")

    try:
        outbox_id = str(uuid.uuid4())
        message_id = await redis.xadd(stream, {
            "outbox_id": outbox_id, "kind": "score.created",
            "aggregate_id": "poison-event", "payload": "{}",
        })
        worker = RedisAlertWorker(
            redis, FailingDispatcher(), [], stream=stream, group=group,
            consumer="integration-consumer", max_deliveries=2, claim_min_idle_ms=0,
        )
        assert await worker.run_once(block_ms=10, count=1) == 0
        assert (await redis.xpending(stream, group))["pending"] == 1
        assert await worker.run_once(block_ms=10, count=1) == 1
        assert (await redis.xpending(stream, group))["pending"] == 0
        dlq_rows = await redis.xrange(dlq)
        assert len(dlq_rows) == 1
        assert dlq_rows[0][1]["original_id"] == message_id
        assert dlq_rows[0][1]["outbox_id"] == outbox_id
        assert dlq_rows[0][1]["aggregate_id"] == "poison-event"
        assert "poison event" in dlq_rows[0][1]["error"]

        # A persisted XAUTOCLAIM cursor must move beyond a poison prefix even
        # while those earlier entries remain pending.
        cursor_ids = [await redis.xadd(stream, {
            "outbox_id": str(uuid.uuid4()), "kind": "score.created",
            "aggregate_id": f"cursor-poison-{index}", "payload": "{}",
        }) for index in range(5)]
        cursor_worker = RedisAlertWorker(
            redis, FailingDispatcher(), [], stream=stream, group=group,
            consumer="cursor-consumer", max_deliveries=99, claim_min_idle_ms=0,
        )
        assert await cursor_worker.run_once(block_ms=10, count=5) == 0
        assert await cursor_worker.run_once(block_ms=10, count=2) == 0
        assert cursor_worker.claim_cursor != "0-0"
        assert await cursor_worker.run_once(block_ms=10, count=2) == 0
        fourth = await redis.xpending_range(stream, group, min=cursor_ids[3], max=cursor_ids[3], count=1)
        assert fourth[0]["times_delivered"] == 2

        trimmed_id = await redis.xadd(trimmed_stream, {
            "outbox_id": str(uuid.uuid4()), "kind": "score.created",
            "aggregate_id": "trimmed-pending", "payload": "{}",
        })
        await redis.xgroup_create(trimmed_stream, trimmed_group, id="0-0")
        await redis.xreadgroup(trimmed_group, "original-consumer", {trimmed_stream: ">"}, count=1)
        assert (await redis.xpending(trimmed_stream, trimmed_group))["pending"] == 1
        assert await redis.xdel(trimmed_stream, trimmed_id) == 1
        trimmed_worker = RedisAlertWorker(
            redis, FailingDispatcher(), [], stream=trimmed_stream, group=trimmed_group,
            consumer="recovery-consumer", max_deliveries=3, claim_min_idle_ms=0,
        )
        with pytest.raises(RuntimeError, match="deleted before ACK"):
            await trimmed_worker.run_once(block_ms=10, count=10)
        trimmed_dlq = await redis.xrange(f"{trimmed_stream}:dlq")
        assert trimmed_dlq[0][1]["original_id"] == trimmed_id
        assert trimmed_dlq[0][1]["kind"] == "pending.deleted"

        class RecordingFailingDispatcher(FailingDispatcher):
            aborted_keys: list[str] = []

            def abort_message_reservations(
                self, event_id: str, workspace_ids: list[str], delivery_key: str, reason: str,
            ) -> int:
                self.aborted_keys.append(delivery_key)
                return 0

        fallback_dispatcher = RecordingFailingDispatcher()
        await redis.xadd(fallback_stream, {
            "kind": "score.created", "aggregate_id": "fallback-poison",
            "payload": '{"cycleId":"stable-fallback-cycle"}',
        })
        fallback_worker = RedisAlertWorker(
            redis, fallback_dispatcher, [], stream=fallback_stream, group=fallback_group,
            consumer="fallback-consumer", max_deliveries=1, claim_min_idle_ms=0,
        )
        assert await fallback_worker.run_once(block_ms=10, count=1) == 1
        assert fallback_dispatcher.aborted_keys == ["stable-fallback-cycle"]
        await redis.xadd(fallback_stream, {
            "kind": "score.created", "aggregate_id": "malformed-poison", "payload": "[]",
        })
        assert await fallback_worker.run_once(block_ms=10, count=1) == 1
        assert len(await redis.xrange(f"{fallback_stream}:dlq")) == 2
    finally:
        await redis.delete(
            stream, dlq, trimmed_stream, f"{trimmed_stream}:dlq",
            fallback_stream, f"{fallback_stream}:dlq",
        )
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not os.getenv("REDIS_INTEGRATION_URL"), reason="set REDIS_INTEGRATION_URL for Redis integration")
async def test_real_redis_worker_acks_only_after_alert_confirmation_is_durable() -> None:
    stream = f"integration:radar:{uuid.uuid4().hex}"
    group = "integration-alert-confirmation"
    redis = Redis.from_url(os.environ["REDIS_INTEGRATION_URL"], decode_responses=True)

    class FlakyConfirmationRepository(InMemoryRepository):
        confirmation_attempts = 0

        def confirm_alert_delivery(self, idempotency_key: str, workspace_id: str | None = None) -> bool:
            self.confirmation_attempts += 1
            if self.confirmation_attempts == 1:
                raise RuntimeError("injected durable confirmation failure")
            return super().confirm_alert_delivery(idempotency_key, workspace_id)

    repository = seed_repository(FlakyConfirmationRepository())
    repository.add_alert(
        AlertRuleRequest(name="strong", minimumAttention=70, minimumEvidenceStrength=65),
        "workspace-a", "analyst-a",
    )
    try:
        await redis.xadd(stream, {
            "outbox_id": str(uuid.uuid4()), "kind": "score.created",
            "aggregate_id": "evt-open-model", "payload": "{}",
        })
        worker = RedisAlertWorker(
            redis, AlertDispatcher(repository, signing_secret="test-secret"), ["workspace-a"],
            stream=stream, group=group, consumer="confirmation-consumer",
            max_deliveries=5, claim_min_idle_ms=0,
        )
        assert await worker.run_once(block_ms=10, count=1) == 0
        assert (await redis.xpending(stream, group))["pending"] == 1
        assert repository.alert_deliveries[0]["status"] == "reserved"
        original = repository.get_event("evt-open-model")
        assert original is not None
        repository.upsert_event(original.model_copy(update={
            "score_version": "score-after-confirmation-failure",
            "updated_at": original.updated_at + timedelta(minutes=1),
        }))
        assert await worker.run_once(block_ms=10, count=1) == 1
        assert (await redis.xpending(stream, group))["pending"] == 0
        assert len(repository.alert_deliveries) == 1
        assert repository.alert_deliveries[0]["status"] == "delivered"
    finally:
        await redis.delete(stream, f"{stream}:dlq")
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(
    not all(os.getenv(name) for name in ("S3_INTEGRATION_ENDPOINT", "S3_INTEGRATION_ACCESS_KEY", "S3_INTEGRATION_SECRET_KEY")),
    reason="set S3_INTEGRATION_ENDPOINT/access/secret for S3-compatible integration",
)
async def test_s3_compatible_raw_evidence_put_read_and_delete() -> None:
    endpoint = os.environ["S3_INTEGRATION_ENDPOINT"]
    access_key = os.environ["S3_INTEGRATION_ACCESS_KEY"]
    secret_key = os.environ["S3_INTEGRATION_SECRET_KEY"]
    bucket = f"integration-{uuid.uuid4().hex}"
    key = "raw/source/item.json"
    reference = f"r2://{bucket}/{key}"
    store = S3EvidenceStore(endpoint, access_key, secret_key)
    store.client.create_bucket(Bucket=bucket)
    try:
        await store.put(reference, b'{"evidence":true}', "application/json")
        response = store.client.get_object(Bucket=bucket, Key=key)
        assert response["Body"].read() == b'{"evidence":true}'
        assert response["ContentType"] == "application/json"
        await store.delete_many([reference])
        with pytest.raises(ClientError) as error:
            store.client.head_object(Bucket=bucket, Key=key)
        assert error.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    finally:
        remaining = store.client.list_objects_v2(Bucket=bucket).get("Contents", [])
        if remaining:
            store.client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": item["Key"]} for item in remaining]},
            )
        store.client.delete_bucket(Bucket=bucket)
