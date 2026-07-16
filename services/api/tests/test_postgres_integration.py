from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis

from radar import stream_retention as retention_module
from radar.alert_worker import AlertDispatcher, RedisAlertWorker
from radar.contracts import AlertRuleRequest, Observation, StoredScore
from radar.fixtures import demo_events
from radar.main import create_app
from radar.outbox import (
    RedisOutboxPublisher,
    STREAM_FENCE_PROTOCOL_VERSION,
    STREAM_OUTBOX_KINDS,
    acquire_stream_exclusive,
    outbox_recovery_keys,
    register_stream_participant,
    release_stream_exclusive,
    release_stream_participant,
    xadd_as_stream_exclusive,
    xadd_as_stream_participant,
)
from radar.storage import InMemoryRepository, PostgresRepository
from radar.stream_retention import (
    inspect_consumer_groups,
    maintain_stream_retention,
    trim_stream_at_verified_watermarks,
)
from tools.replay_outbox_to_redis import execute_replay, main as replay_main


APP_DSN = os.getenv("POSTGRES_INTEGRATION_DSN")
ADMIN_DSN = os.getenv("POSTGRES_INTEGRATION_ADMIN_DSN")
REDIS_URL = os.getenv("REDIS_INTEGRATION_URL")
pytestmark = pytest.mark.skipif(
    not APP_DSN or not ADMIN_DSN,
    reason="set POSTGRES_INTEGRATION_DSN and POSTGRES_INTEGRATION_ADMIN_DSN for destructive local integration tests",
)


def _cleanup_source(prefix: str) -> None:
    assert ADMIN_DSN is not None
    with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
        cursor.execute("SET session_replication_role='replica'")
        cursor.execute("SELECT id FROM observations WHERE id LIKE %s", (f"{prefix}%",))
        observation_ids = [row[0] for row in cursor.fetchall()]
        if observation_ids:
            cursor.execute("DELETE FROM metric_snapshots WHERE subject_id=ANY(%s)", (observation_ids,))
            cursor.execute("DELETE FROM observation_processing_history WHERE observation_id=ANY(%s)", (observation_ids,))
            cursor.execute("DELETE FROM content_ingest_history WHERE observation_id=ANY(%s)", (observation_ids,))
            cursor.execute("DELETE FROM observations WHERE id=ANY(%s)", (observation_ids,))
        cursor.execute("DELETE FROM outbox WHERE aggregate_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM sources WHERE id LIKE %s", (f"{prefix}%",))
        cursor.execute("SET session_replication_role='origin'")
        connection.commit()


def _cleanup_event(prefix: str, workspace_id: str) -> None:
    assert ADMIN_DSN is not None
    with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
        cursor.execute("SET session_replication_role='replica'")
        cursor.execute("DELETE FROM alert_deliveries WHERE workspace_id=%s", (workspace_id,))
        cursor.execute("DELETE FROM alert_rules WHERE workspace_id=%s", (workspace_id,))
        cursor.execute("DELETE FROM review_queue_entries WHERE event_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM lead_threshold_crossings WHERE event_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM outbox WHERE aggregate_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM events WHERE id LIKE %s", (f"{prefix}%",))
        cursor.execute("SET session_replication_role='origin'")
        connection.commit()


def test_postgres_runtime_attestation_and_production_health(monkeypatch: pytest.MonkeyPatch) -> None:
    assert APP_DSN is not None
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("RADAR_INSTANCE_ID", "postgres-integration-rc24")
    monkeypatch.setenv("RADAR_API_KEYS", json.dumps({
        "integration-owner": {
            "subject": "integration-owner", "role": "OWNER", "workspaceId": "system-governance",
        },
    }))
    repository = PostgresRepository(APP_DSN)
    attestation = repository.runtime_attestation()
    expected = {
        "storageBackend": "postgresql",
        "rlsVerified": True,
        "migrationVersion": "001_init_rc2.6",
        "auditTriggersVerified": True,
        "migrationMarkerReadOnly": True,
        "instanceId": "postgres-integration-rc24",
        "databaseUser": "radar_app",
        "databaseRoleSuperuser": False,
        "databaseRoleBypassRls": False,
    }
    for key, value in expected.items():
        assert attestation[key] == value
    assert float(attestation["databaseClockSkewSeconds"]) <= 5
    with TestClient(create_app(repository)) as client:
        health = client.get("/health")
    assert health.status_code == 200
    assert health.json()["productionReady"] is True


def test_postgres_rls_append_only_trigger_and_read_only_marker() -> None:
    assert APP_DSN is not None
    rule_id = uuid.uuid4()
    with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT set_config('app.workspace_id','workspace-a',false)")
        cursor.execute(
            "INSERT INTO alert_rules (id,workspace_id,actor_id,payload) VALUES (%s,'workspace-a','actor-a','{}')",
            (rule_id,),
        )
        connection.commit()
    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT set_config('app.workspace_id','workspace-b',false)")
            cursor.execute("SELECT id FROM alert_rules WHERE id=%s", (rule_id,))
            assert cursor.fetchone() is None
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO source_promotion_facts
                (source_id,from_status,to_status,score,policy_version,policy_digest)
                VALUES ('integration-trigger','candidate','active',60,'integration-policy','sha256:integration')"""
            )
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                cursor.execute(
                    "UPDATE source_promotion_facts SET score=61 WHERE source_id='integration-trigger'",
                )
            connection.rollback()
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cursor.execute(
                    "UPDATE schema_attestations SET value='tampered' WHERE key='migration_version'",
                )
            connection.rollback()
        repository = PostgresRepository(APP_DSN)
        reservation_key = f"integration-abort-{uuid.uuid4()}"
        assert repository.reserve_alert_delivery({
            "ruleId": str(rule_id), "workspaceId": "workspace-a", "eventId": "integration-aborted-event",
            "domain": "model_release", "lifecycleState": "accelerating", "evidenceStrength": "high",
            "evidenceCount": 3, "channel": "webhook", "deliveredAt": datetime.now(timezone.utc),
            "idempotencyKey": reservation_key,
        })
        assert repository.abort_alert_reservation(reservation_key, "integration terminal reason", "workspace-a")
        aborted = repository.get_alert_delivery_by_idempotency("workspace-a", reservation_key)
        assert aborted is not None
        assert aborted["status"] == "aborted"
        assert aborted["terminalReason"] == "integration terminal reason"
    finally:
        assert ADMIN_DSN is not None
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SET session_replication_role='replica'")
            cursor.execute("DELETE FROM alert_deliveries WHERE rule_id=%s", (rule_id,))
            cursor.execute("SET session_replication_role='origin'")
            connection.commit()
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT set_config('app.workspace_id','workspace-a',false)")
            cursor.execute("DELETE FROM alert_rules WHERE id=%s", (rule_id,))
            connection.commit()


def test_postgres_concurrent_duplicate_content_counts_one_valid_source_observation() -> None:
    assert APP_DSN is not None
    prefix = f"integration-{uuid.uuid4().hex[:12]}"
    source_id = f"{prefix}-source"
    fingerprint = f"{prefix}-same-fingerprint"
    collected_at = datetime.now(timezone.utc)

    def write(index: int) -> bool:
        item_id = f"{prefix}-observation-{index}"
        observation = Observation(
            id=item_id, platform="RSS", externalId=item_id, sourceId=source_id,
            accountId=f"{source_id}:account", entityId=f"{source_id}:entity",
            publishedAt=collected_at, collectedAt=collected_at, language="en",
            title="Concurrent duplicate", text="The same canonical source content",
            url=f"https://example.com/{prefix}?copy={index}", metrics={},
            rawEvidenceRef=f"r2://raw/{item_id}.json", relation="original",
            contentFingerprint=fingerprint, signalFamily="official",
        )
        return PostgresRepository(APP_DSN).save_observation_with_outbox(observation)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(write, (1, 2)))
        assert results == [True, True]
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT valid_observations,discovery_reasons FROM sources WHERE id=%s",
                (source_id,),
            )
            valid_observations, discovery_reasons = cursor.fetchone()
            cursor.execute(
                "SELECT count(*) FROM observations WHERE source_id=%s AND content_fingerprint=%s",
                (source_id, fingerprint),
            )
            persisted_observations = cursor.fetchone()[0]
        assert persisted_observations == 2
        assert valid_observations == 1
        assert "duplicate_content_observation" in discovery_reasons
    finally:
        _cleanup_source(prefix)


@pytest.mark.asyncio
@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_INTEGRATION_URL for outbox integration")
async def test_outbox_failure_retry_consumer_ack_and_redis_loss_replay() -> None:
    assert APP_DSN is not None
    assert REDIS_URL is not None
    prefix = f"integration-outbox-{uuid.uuid4().hex}"
    aggregate_id = f"{prefix}-event"
    workspace_id = f"{prefix}-workspace"
    stream = f"integration:outbox:{uuid.uuid4().hex}"
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    outbox_id: uuid.UUID | None = None
    later_outbox_id: uuid.UUID | None = None
    started_at = datetime.now(timezone.utc)

    class FailAfterXadd(RedisOutboxPublisher):
        def __init__(self, dsn: str, redis: Redis, stream: str, target_id: str) -> None:
            super().__init__(dsn, redis, stream)
            self.target_id = target_id
            self.injected = False

        def _before_mark_published(self, outbox_id: str) -> None:
            if outbox_id == self.target_id and not self.injected:
                self.injected = True
                raise RuntimeError(f"injected PostgreSQL mark failure for {outbox_id}")

    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL")
            if cursor.fetchone()[0] != 0:
                pytest.skip("requires an isolated integration database with an empty pending outbox")

        now = datetime.now(timezone.utc)
        event = demo_events()[0].model_copy(update={
            "id": aggregate_id,
            "first_seen": now - timedelta(hours=2),
            "updated_at": now,
        })
        repository = PostgresRepository(APP_DSN)
        repository.upsert_event(event)
        repository.add_alert(
            AlertRuleRequest(name="integration strong event", minimumAttention=70, minimumEvidenceStrength=65),
            workspace_id, "integration-analyst",
        )
        repository.save_score(StoredScore(
            event_id=aggregate_id,
            score_version=event.score_version,
            threshold_version="integration-thresholds",
            input_from=now - timedelta(minutes=15),
            input_to=now,
            input_digest=f"sha256:{uuid.uuid4().hex}",
            drivers=["integration outbox event"],
            payload={"state": event.state.value},
            created_at=now,
        ))
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT id FROM outbox WHERE aggregate_id=%s", (aggregate_id,))
            outbox_id = cursor.fetchone()[0]
            cursor.execute(
                """INSERT INTO outbox (kind,aggregate_id,payload)
                VALUES ('feedback.created',%s,%s::jsonb) RETURNING id""",
                (f"{prefix}-later", json.dumps({"later": True})),
            )
            later_outbox_id = cursor.fetchone()[0]
            connection.commit()

        failed = await FailAfterXadd(APP_DSN, redis, stream, str(outbox_id)).publish_batch(limit=2)
        assert failed.published == 1
        assert failed.failed == 1
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT published_at,attempts,last_error FROM outbox WHERE id=%s", (outbox_id,))
            published_at, attempts, last_error = cursor.fetchone()
            cursor.execute("SELECT published_at,attempts,last_error FROM outbox WHERE id=%s", (later_outbox_id,))
            later_published_at, later_attempts, later_error = cursor.fetchone()
        assert published_at is None
        assert attempts == 1
        assert "injected PostgreSQL mark failure" in last_error
        assert later_published_at is not None
        assert later_attempts == 1
        assert later_error is None

        publisher = RedisOutboxPublisher(APP_DSN, redis, stream)
        retried = await publisher.publish_batch(limit=1)
        assert retried.published == 1
        assert retried.failed == 0
        rows = await redis.xrange(stream)
        assert len(rows) == 3
        assert [fields["outbox_id"] for _, fields in rows].count(str(outbox_id)) == 2
        assert [fields["outbox_id"] for _, fields in rows].count(str(later_outbox_id)) == 1
        assert {fields["kind"] for _, fields in rows} == {"score.created", "feedback.created"}

        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT published_at,attempts,last_error FROM outbox WHERE id=%s", (outbox_id,))
            published_at, attempts, last_error = cursor.fetchone()
        assert published_at is not None
        assert attempts == 2
        assert last_error is None

        group = f"{prefix}-group"
        worker = RedisAlertWorker(
            redis, AlertDispatcher(repository, signing_secret="integration-secret"), [workspace_id],
            stream=stream, group=group, consumer=f"{prefix}-consumer", claim_min_idle_ms=0,
        )
        assert await worker.run_once(block_ms=10, count=10) == 3
        assert (await redis.xpending(stream, group))["pending"] == 0
        deliveries = repository.list_alert_deliveries(workspace_id, started_at - timedelta(minutes=1))
        assert len(deliveries) == 1
        assert deliveries[0]["status"] == "delivered"
        assert deliveries[0]["eventId"] == aggregate_id

        # Simulate complete Redis stream loss. Rebuild every registered Stream
        # kind; the worker ACKs unrelated kinds, and the stable score outbox id
        # must not duplicate the already committed business delivery.
        await redis.delete(stream)
        replay_token = await acquire_stream_exclusive(redis, stream, "integration-replay")
        try:
            replay = await publisher.replay_batch(
                since=started_at - timedelta(minutes=1),
                until=datetime.now(timezone.utc) + timedelta(minutes=1),
                limit=10,
                exclusive_token=replay_token,
            )
        finally:
            await release_stream_exclusive(redis, stream, replay_token)
        assert replay.replayed == 2
        assert replay.complete is True
        replayed_rows = await redis.xrange(stream)
        assert len(replayed_rows) == 2
        assert {row[1]["outbox_id"] for row in replayed_rows} == {
            str(outbox_id), str(later_outbox_id),
        }
        assert all(row[1]["replay"] == "true" for row in replayed_rows)
        replay_group = f"{prefix}-replay-group"
        replay_worker = RedisAlertWorker(
            redis, AlertDispatcher(repository, signing_secret="integration-secret"), [workspace_id],
            stream=stream, group=replay_group, consumer=f"{prefix}-replay-consumer", claim_min_idle_ms=0,
        )
        assert await replay_worker.run_once(block_ms=10, count=10) == 2
        assert (await redis.xpending(stream, replay_group))["pending"] == 0
        assert len(repository.list_alert_deliveries(workspace_id, started_at - timedelta(minutes=1))) == 1
    finally:
        await redis.delete(stream)
        await redis.aclose()
        _cleanup_event(prefix, workspace_id)


@pytest.mark.asyncio
@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_INTEGRATION_URL for replay integration")
async def test_resumable_replay_command_refuses_unrelated_or_completed_stream(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert APP_DSN is not None
    assert REDIS_URL is not None
    prefix = f"integration-replay-{uuid.uuid4().hex}"
    aggregate_id = f"{prefix}-event"
    stream = f"{prefix}:stream"
    unrelated_stream = f"{prefix}:unrelated"
    resumed_stream = f"{prefix}:resumed"
    lost_stream = f"{prefix}:lost"
    tampered_stream = f"{prefix}:tampered"
    active_stream = f"{prefix}:active"
    locked_stream = f"{prefix}:locked"
    stolen_stream = f"{prefix}:stolen"
    missing_prefix_stream = f"{prefix}:missing-prefix"
    legacy_completed_stream = f"{prefix}:legacy-completed"
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    outbox_id: uuid.UUID | None = None
    prefix_outbox_ids: list[uuid.UUID] = []
    now = datetime.now(timezone.utc)
    stream_keys = {
        value: outbox_recovery_keys(value)
        for value in (
            stream, unrelated_stream, resumed_stream, lost_stream, tampered_stream,
            active_stream, locked_stream, stolen_stream, missing_prefix_stream,
            legacy_completed_stream,
        )
    }
    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO outbox (kind,aggregate_id,payload,published_at)
                VALUES ('score.created',%s,%s::jsonb,now()) RETURNING id,created_at""",
                (aggregate_id, json.dumps({"integration": True})),
            )
            outbox_id, outbox_created_at = cursor.fetchone()
            connection.commit()
        result = await execute_replay(
            APP_DSN, REDIS_URL, stream,
            now - timedelta(minutes=1), now + timedelta(minutes=1), 1,
        )
        assert result["replayed"] == 1
        assert result["status"] == "completed"
        rows = await redis.xrange(stream)
        assert len(rows) == 1
        assert rows[0][1]["outbox_id"] == str(outbox_id)
        state_key, lock_key, publishers_key = stream_keys[stream]
        state = await redis.hgetall(state_key)
        assert state["status"] == "completed"
        assert state["afterId"] == str(outbox_id)

        with pytest.raises(RuntimeError, match="already marked completed"):
            await execute_replay(
                APP_DSN, REDIS_URL, stream,
                now - timedelta(minutes=1), now + timedelta(minutes=1), 1,
            )
        await redis.delete(stream)
        rebuilt_again = await execute_replay(
            APP_DSN, REDIS_URL, stream,
            now - timedelta(minutes=1), now + timedelta(minutes=1), 1,
        )
        assert rebuilt_again["replayed"] == 1
        assert await redis.xlen(stream) == 1
        await redis.xadd(unrelated_stream, {"kind": "unrelated"})
        with pytest.raises(RuntimeError, match="without a matching replay checkpoint"):
            await execute_replay(
                APP_DSN, REDIS_URL, unrelated_stream,
                now - timedelta(minutes=1), now + timedelta(minutes=1), 1,
            )
        since = now - timedelta(minutes=1)
        until = now + timedelta(minutes=1)
        legacy_state_key, _, _ = stream_keys[legacy_completed_stream]
        await redis.hset(legacy_state_key, mapping={
            "since": since.isoformat(), "until": until.isoformat(),
            "stream": legacy_completed_stream, "kinds": ",".join(STREAM_OUTBOX_KINDS),
            "protocolVersion": "stream-fence-legacy",
            "status": "completed", "replayed": "1", "completedAt": now.isoformat(),
        })
        legacy_state_before = await redis.hgetall(legacy_state_key)
        with pytest.raises(RuntimeError, match="kind/protocol set"):
            await execute_replay(
                APP_DSN, REDIS_URL, legacy_completed_stream, since, until, 1,
            )
        assert await redis.hgetall(legacy_state_key) == legacy_state_before
        assert not await redis.exists(legacy_completed_stream)

        resumed_message_id = await redis.xadd(resumed_stream, {"outbox_id": str(outbox_id), "kind": "score.created"})
        resumed_state_key, resumed_lock_key, resumed_publishers_key = stream_keys[resumed_stream]
        await redis.hset(resumed_state_key, mapping={
            "since": since.isoformat(), "until": until.isoformat(),
            "stream": resumed_stream, "kinds": ",".join(STREAM_OUTBOX_KINDS),
            "protocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
            "status": "running", "replayed": "1", "startedAt": now.isoformat(),
            "afterCreatedAt": outbox_created_at.isoformat(), "afterId": str(outbox_id),
            "afterStreamId": resumed_message_id,
        })
        resumed = await execute_replay(APP_DSN, REDIS_URL, resumed_stream, since, until, 1)
        assert resumed["status"] == "completed"
        assert resumed["replayed"] == 1
        assert await redis.xlen(resumed_stream) == 1

        lost_state_key, lost_lock_key, lost_publishers_key = stream_keys[lost_stream]
        await redis.hset(lost_state_key, mapping={
            "since": since.isoformat(), "until": until.isoformat(),
            "stream": lost_stream, "kinds": ",".join(STREAM_OUTBOX_KINDS),
            "protocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
            "status": "running", "replayed": "1", "startedAt": now.isoformat(),
            "afterCreatedAt": outbox_created_at.isoformat(), "afterId": str(outbox_id),
            "afterStreamId": "1-0",
        })
        with pytest.raises(RuntimeError, match="stream disappeared"):
            await execute_replay(APP_DSN, REDIS_URL, lost_stream, since, until, 1)

        tampered_message_id = await redis.xadd(tampered_stream, {"outbox_id": "wrong-row"})
        tampered_state_key, tampered_lock_key, tampered_publishers_key = stream_keys[tampered_stream]
        await redis.hset(tampered_state_key, mapping={
            "since": since.isoformat(), "until": until.isoformat(),
            "stream": tampered_stream, "kinds": ",".join(STREAM_OUTBOX_KINDS),
            "protocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
            "status": "running", "replayed": "1", "startedAt": now.isoformat(),
            "afterCreatedAt": outbox_created_at.isoformat(), "afterId": str(outbox_id),
            "afterStreamId": tampered_message_id,
        })
        with pytest.raises(RuntimeError, match="checkpointed outbox row"):
            await execute_replay(APP_DSN, REDIS_URL, tampered_stream, since, until, 1)

        active_token = await register_stream_participant(redis, active_stream, "integration-test")
        with pytest.raises(RuntimeError, match="active stream processor"):
            await execute_replay(APP_DSN, REDIS_URL, active_stream, since, until, 1)
        await release_stream_participant(redis, active_stream, active_token)

        locked_state_key, locked_lock_key, locked_publishers_key = stream_keys[locked_stream]
        await redis.set(locked_lock_key, "another-operator", ex=60)
        with pytest.raises(RuntimeError, match="another replay or active stream processor"):
            await execute_replay(APP_DSN, REDIS_URL, locked_stream, since, until, 1)
        with pytest.raises(RuntimeError, match="outbox replay is active"):
            await RedisOutboxPublisher(APP_DSN, redis, locked_stream).publish_batch(limit=1)
        with pytest.raises(RuntimeError, match="outbox replay is active"):
            await RedisAlertWorker(
                redis, AlertDispatcher(InMemoryRepository(), signing_secret="test"), [],
                stream=locked_stream, group=f"{prefix}:group", claim_min_idle_ms=0,
            ).run_once(block_ms=1, count=1)

        monkeypatch.setenv("DATABASE_URL", APP_DSN)
        monkeypatch.setenv("REDIS_URL", REDIS_URL)
        monkeypatch.setattr(sys, "argv", [
            "replay_outbox_to_redis.py",
            "--since", since.isoformat(),
            "--until", (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
            "--stream", f"{prefix}:future",
            "--execute", "--confirm-stream", f"{prefix}:future",
        ])
        with pytest.raises(SystemExit, match="no later than the PostgreSQL clock"):
            replay_main()

        prefix_since = datetime(2001, 1, 1, tzinfo=timezone.utc) + timedelta(
            seconds=int(uuid.uuid4().hex[:6], 16),
        )
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            for index in range(2):
                created_at = prefix_since + timedelta(seconds=index + 1)
                cursor.execute(
                    """INSERT INTO outbox (kind,aggregate_id,payload,published_at,created_at)
                    VALUES ('score.created',%s,%s::jsonb,%s,%s) RETURNING id,created_at""",
                    (
                        f"{prefix}-missing-prefix-{index}", json.dumps({"index": index}),
                        created_at, created_at,
                    ),
                )
                inserted_id, inserted_at = cursor.fetchone()
                prefix_outbox_ids.append(inserted_id)
                if index == 1:
                    last_prefix_created_at = inserted_at
            connection.commit()
        last_prefix_id = prefix_outbox_ids[-1]
        last_prefix_stream_id = await redis.xadd(
            missing_prefix_stream, {"outbox_id": str(last_prefix_id), "kind": "score.created"},
        )
        missing_state_key, _, _ = stream_keys[missing_prefix_stream]
        prefix_until = prefix_since + timedelta(seconds=3)
        await redis.hset(missing_state_key, mapping={
            "since": prefix_since.isoformat(), "until": prefix_until.isoformat(),
            "stream": missing_prefix_stream, "kinds": ",".join(STREAM_OUTBOX_KINDS),
            "protocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
            "status": "running", "replayed": "2", "startedAt": now.isoformat(),
            "afterCreatedAt": last_prefix_created_at.isoformat(), "afterId": str(last_prefix_id),
            "afterStreamId": last_prefix_stream_id,
        })
        with pytest.raises(RuntimeError, match="prefix integrity mismatch"):
            await execute_replay(
                APP_DSN, REDIS_URL, missing_prefix_stream, prefix_since, prefix_until, 1,
            )
        assert await redis.xlen(missing_prefix_stream) == 1
        assert (await redis.hgetall(missing_state_key))["status"] == "running"

        original_replay_batch = RedisOutboxPublisher.replay_batch

        async def replay_then_steal_lock(self: RedisOutboxPublisher, **kwargs: object):
            replay_result = await original_replay_batch(self, **kwargs)
            _, stolen_lock_key, _ = stream_keys[stolen_stream]
            await self.redis.set(stolen_lock_key, "replacement-owner", ex=60)
            return replay_result

        monkeypatch.setattr(RedisOutboxPublisher, "replay_batch", replay_then_steal_lock)
        with pytest.raises(RuntimeError, match="lost before checkpoint commit"):
            await execute_replay(APP_DSN, REDIS_URL, stolen_stream, since, until, 1)
        stolen_state = await redis.hgetall(stream_keys[stolen_stream][0])
        assert stolen_state["replayed"] == "0"
        assert stolen_state["afterId"] == ""
    finally:
        if outbox_id is not None or prefix_outbox_ids:
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM outbox WHERE id=ANY(%s)",
                    ([value for value in [outbox_id, *prefix_outbox_ids] if value is not None],),
                )
                connection.commit()
        cleanup_keys = [key for value in stream_keys for key in (value, *stream_keys[value])]
        await redis.delete(*cleanup_keys)
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_INTEGRATION_URL for outbox integration")
async def test_stream_retention_trims_only_recoverable_rows_behind_every_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert APP_DSN is not None
    assert REDIS_URL is not None
    prefix = f"integration-retention-{uuid.uuid4().hex}"
    stream = f"{prefix}:stream"
    unsafe_stream = f"{prefix}:unsafe"
    time_stream = f"{prefix}:time-boundary"
    fence_stream = f"{prefix}:fence"
    first_group = f"{prefix}:alerts"
    second_group = f"{prefix}:deletions"
    racing_group = f"{prefix}:late-group"
    unsafe_group = f"{prefix}:unsafe-group"
    time_group = f"{prefix}:time-group"
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    outbox_ids: list[uuid.UUID] = []
    stream_keys = [stream, unsafe_stream, time_stream, fence_stream]
    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            for index in range(5):
                cursor.execute(
                    """INSERT INTO outbox (kind,aggregate_id,payload,published_at)
                    VALUES ('score.created',%s,%s::jsonb,now()) RETURNING id""",
                    (f"{prefix}-{index}", json.dumps({"index": index})),
                )
                outbox_ids.append(cursor.fetchone()[0])
            connection.commit()

        stream_ids: list[str] = []
        for index, outbox_id in enumerate(outbox_ids):
            stream_ids.append(await redis.xadd(
                stream,
                {"outbox_id": str(outbox_id), "kind": "score.created", "index": str(index)},
            ))
        await redis.xgroup_create(stream, first_group, id="0-0")
        await redis.xgroup_create(stream, second_group, id="0-0")

        first_delivery = await redis.xreadgroup(
            first_group, f"{prefix}:consumer-a", {stream: ">"}, count=5,
        )
        assert len(first_delivery[0][1]) == 5
        assert await redis.xack(stream, first_group, stream_ids[0]) == 1

        second_delivery = await redis.xreadgroup(
            second_group, f"{prefix}:consumer-b", {stream: ">"}, count=4,
        )
        assert [entry[0] for entry in second_delivery[0][1]] == stream_ids[:4]
        assert await redis.xack(stream, second_group, *stream_ids[:4]) == 4

        participant = await register_stream_participant(redis, stream, "publisher")
        try:
            with pytest.raises(RuntimeError, match="active replay or stream participant"):
                await maintain_stream_retention(
                    APP_DSN,
                    redis,
                    stream,
                    required_groups={first_group, second_group},
                    retention_hours=0,
                    capacity_limit=4,
                )
        finally:
            await release_stream_participant(redis, stream, participant)

        stale_token = await register_stream_participant(redis, fence_stream, "stale-publisher")
        _, fence_lock_key, fence_participants_key = outbox_recovery_keys(fence_stream)
        await redis.zadd(fence_participants_key, {stale_token: 0})
        exclusive_fence = await acquire_stream_exclusive(redis, fence_stream, "recovery")
        with pytest.raises(RuntimeError, match="participant fencing rejected XADD"):
            await xadd_as_stream_participant(
                redis, fence_stream, stale_token, {"outbox_id": str(uuid.uuid4())},
            )
        assert await redis.xlen(fence_stream) == 0
        await xadd_as_stream_exclusive(
            redis, fence_stream, exclusive_fence, {"outbox_id": str(uuid.uuid4())},
        )
        assert await redis.xlen(fence_stream) == 1
        await redis.set(fence_lock_key, "replacement-owner", ex=60)
        with pytest.raises(RuntimeError, match="exclusive stream fencing rejected XADD"):
            await xadd_as_stream_exclusive(
                redis, fence_stream, exclusive_fence, {"outbox_id": str(uuid.uuid4())},
            )
        assert await redis.xlen(fence_stream) == 1

        dry_run = await maintain_stream_retention(
            APP_DSN,
            redis,
            stream,
            required_groups={first_group, second_group},
            retention_hours=0,
            capacity_limit=4,
        )
        assert dry_run["mode"] == "dry-run"
        assert dry_run["safeTrimMinId"] == stream_ids[1]
        assert dry_run["candidateEntries"] == 1
        assert dry_run["uniqueOutboxIds"] == 1
        assert dry_run["missingPublishedOutboxIds"] == 0
        assert dry_run["estimatedAfterLength"] == 4
        assert dry_run["capacityStatusAfterSafeTrim"] == "within_limit"
        assert dry_run["recoverableKinds"] == list(STREAM_OUTBOX_KINDS)
        assert dry_run["fenceProtocolVersion"] == STREAM_FENCE_PROTOCOL_VERSION

        groups_before_stale_trim = await inspect_consumer_groups(
            redis,
            stream,
            {first_group, second_group},
        )
        stale_trim_token = await acquire_stream_exclusive(redis, stream, "stale-retention")
        _, stream_lock_key, _ = outbox_recovery_keys(stream)
        await redis.set(stream_lock_key, "replacement-retention-owner", ex=60)
        before_stale_trim_length = await redis.xlen(stream)
        with pytest.raises(RuntimeError, match="exclusive maintenance lease changed"):
            await trim_stream_at_verified_watermarks(
                redis,
                stream,
                stale_trim_token,
                str(dry_run["safeTrimMinId"]),
                groups_before_stale_trim,
            )
        assert await redis.xlen(stream) == before_stale_trim_length
        await redis.delete(stream_lock_key)

        with pytest.raises(RuntimeError, match="execute requires the safeTrimMinId"):
            await maintain_stream_retention(
                APP_DSN,
                redis,
                stream,
                required_groups={first_group, second_group},
                retention_hours=0,
                capacity_limit=4,
                execute=True,
            )
        assert await redis.xlen(stream) == 5

        expected_groups = await inspect_consumer_groups(
            redis, stream, {first_group, second_group},
        )
        await redis.xgroup_create(stream, racing_group, id="0-0")
        with pytest.raises(RuntimeError, match="safety boundary moved behind"):
            await maintain_stream_retention(
                APP_DSN,
                redis,
                stream,
                required_groups={first_group, second_group},
                retention_hours=0,
                capacity_limit=4,
                execute=True,
                confirmed_trim_min_id=str(dry_run["safeTrimMinId"]),
            )
        race_token = await acquire_stream_exclusive(redis, stream, "retention-race")
        try:
            with pytest.raises(RuntimeError, match="consumer group membership changed"):
                await trim_stream_at_verified_watermarks(
                    redis,
                    stream,
                    race_token,
                    str(dry_run["safeTrimMinId"]),
                    expected_groups,
                )
        finally:
            await release_stream_exclusive(redis, stream, race_token)
        assert await redis.xlen(stream) == 5
        assert await redis.xgroup_destroy(stream, racing_group) is True

        original_atomic_trim = retention_module.trim_stream_at_verified_watermarks

        def concurrent_outbox_delete_is_blocked() -> bool:
            assert ADMIN_DSN is not None
            try:
                with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
                    cursor.execute("SET LOCAL lock_timeout='100ms'")
                    cursor.execute("DELETE FROM outbox WHERE id=%s", (outbox_ids[0],))
                    connection.commit()
            except psycopg.errors.LockNotAvailable:
                return True
            return False

        async def atomic_trim_with_lock_assertion(*args: object, **kwargs: object) -> int:
            assert await asyncio.to_thread(concurrent_outbox_delete_is_blocked)
            return await original_atomic_trim(*args, **kwargs)

        monkeypatch.setattr(
            retention_module,
            "trim_stream_at_verified_watermarks",
            atomic_trim_with_lock_assertion,
        )

        executed = await maintain_stream_retention(
            APP_DSN,
            redis,
            stream,
            required_groups={first_group, second_group},
            retention_hours=0,
            capacity_limit=4,
            execute=True,
            confirmed_trim_min_id=str(dry_run["safeTrimMinId"]),
        )
        assert executed["trimmedEntries"] == 1
        assert executed["afterLength"] == 4
        assert (await redis.xrange(stream, count=1))[0][0] == stream_ids[1]
        pending = await redis.xpending_range(stream, first_group, "-", "+", 10)
        assert [item["message_id"] for item in pending] == stream_ids[1:]
        remaining_for_second = await redis.xreadgroup(
            second_group, f"{prefix}:consumer-b", {stream: ">"}, count=10,
        )
        assert [entry[0] for entry in remaining_for_second[0][1]] == stream_ids[4:]
        monkeypatch.setattr(
            retention_module,
            "trim_stream_at_verified_watermarks",
            original_atomic_trim,
        )

        redis_time = await redis.time()
        now_milliseconds = int(redis_time[0]) * 1_000 + int(redis_time[1]) // 1_000
        explicit_time_ids = [
            f"{now_milliseconds - 7_200_000}-0",
            f"{now_milliseconds - 7_199_000}-0",
            f"{now_milliseconds - 1_000}-0",
        ]
        for index, explicit_id in enumerate(explicit_time_ids):
            await redis.xadd(
                time_stream,
                {"outbox_id": str(outbox_ids[index]), "kind": "score.created"},
                id=explicit_id,
            )
        await redis.xgroup_create(time_stream, time_group, id="0-0")
        time_delivery = await redis.xreadgroup(
            time_group, f"{prefix}:time-consumer", {time_stream: ">"}, count=3,
        )
        assert len(time_delivery[0][1]) == 3
        assert await redis.xack(time_stream, time_group, *explicit_time_ids) == 3
        time_dry_run = await maintain_stream_retention(
            APP_DSN,
            redis,
            time_stream,
            required_groups={time_group},
            retention_hours=1,
            capacity_limit=1,
        )
        confirmed_time_boundary = str(time_dry_run["safeTrimMinId"])
        assert stream_ids[0] != confirmed_time_boundary
        assert explicit_time_ids[1] < confirmed_time_boundary < explicit_time_ids[2]
        assert time_dry_run["candidateEntries"] == 2
        await asyncio.sleep(.02)
        time_executed = await maintain_stream_retention(
            APP_DSN,
            redis,
            time_stream,
            required_groups={time_group},
            retention_hours=1,
            capacity_limit=1,
            execute=True,
            confirmed_trim_min_id=confirmed_time_boundary,
        )
        assert time_executed["safeTrimMinId"] == confirmed_time_boundary
        assert time_executed["calculatedSafeTrimMinId"] != confirmed_time_boundary
        assert time_executed["trimmedEntries"] == 2
        assert await redis.xlen(time_stream) == 1

        wrong_kind_id = uuid.uuid4()
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO outbox (id,kind,aggregate_id,payload,published_at)
                VALUES (%s,'integration.retention',%s,'{}'::jsonb,now())""",
                (wrong_kind_id, f"{prefix}-wrong-kind"),
            )
            connection.commit()
        outbox_ids.append(wrong_kind_id)
        unsafe_ids = [str(wrong_kind_id), str(uuid.uuid4())]
        unsafe_stream_ids = [
            await redis.xadd(unsafe_stream, {"outbox_id": value}) for value in unsafe_ids
        ]
        await redis.xgroup_create(unsafe_stream, unsafe_group, id="0-0")
        unsafe_delivery = await redis.xreadgroup(
            unsafe_group, f"{prefix}:unsafe-consumer", {unsafe_stream: ">"}, count=2,
        )
        assert len(unsafe_delivery[0][1]) == 2
        assert await redis.xack(unsafe_stream, unsafe_group, *unsafe_stream_ids) == 2
        unsafe_dry_run = await maintain_stream_retention(
            APP_DSN,
            redis,
            unsafe_stream,
            required_groups={unsafe_group},
            retention_hours=0,
            capacity_limit=1,
        )
        assert unsafe_dry_run["candidateEntries"] == 1
        assert unsafe_dry_run["missingPublishedOutboxIds"] == 1
        with pytest.raises(RuntimeError, match="not recoverable by the supported Outbox replay kinds"):
            await maintain_stream_retention(
                APP_DSN,
                redis,
                unsafe_stream,
                required_groups={unsafe_group},
                retention_hours=0,
                capacity_limit=1,
                execute=True,
                confirmed_trim_min_id=str(unsafe_dry_run["safeTrimMinId"]),
            )
        assert await redis.xlen(unsafe_stream) == 2
    finally:
        if outbox_ids:
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute("DELETE FROM outbox WHERE id=ANY(%s::uuid[])", (outbox_ids,))
                connection.commit()
        cleanup_keys: list[str] = []
        for value in stream_keys:
            cleanup_keys.extend((value, *outbox_recovery_keys(value)))
        await redis.delete(*cleanup_keys)
        await redis.aclose()


@pytest.mark.asyncio
@pytest.mark.skipif(not REDIS_URL, reason="set REDIS_INTEGRATION_URL for outbox integration")
async def test_concurrent_outbox_publishers_do_not_duplicate_normal_delivery() -> None:
    assert APP_DSN is not None
    assert REDIS_URL is not None
    prefix = f"integration-publishers-{uuid.uuid4().hex}"
    stream = f"{prefix}:stream"
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    outbox_ids: list[uuid.UUID] = []
    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL")
            if cursor.fetchone()[0] != 0:
                pytest.skip("requires an isolated integration database with an empty pending outbox")
            for index in range(10):
                cursor.execute(
                    """INSERT INTO outbox (kind,aggregate_id,payload)
                    VALUES ('feedback.created',%s,%s::jsonb) RETURNING id""",
                    (f"{prefix}-{index}", json.dumps({"index": index})),
                )
                outbox_ids.append(cursor.fetchone()[0])
            connection.commit()
        first, second = await asyncio.gather(
            RedisOutboxPublisher(APP_DSN, redis, stream).publish_batch(limit=10),
            RedisOutboxPublisher(APP_DSN, redis, stream).publish_batch(limit=10),
        )
        assert first.failed + second.failed == 0
        assert first.published + second.published == 10
        rows = await redis.xrange(stream)
        assert len(rows) == 10
        assert {fields["outbox_id"] for _, fields in rows} == {str(value) for value in outbox_ids}
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT count(*) FILTER (WHERE published_at IS NOT NULL),min(attempts),max(attempts)
                FROM outbox WHERE id=ANY(%s)""",
                (outbox_ids,),
            )
            published, minimum_attempts, maximum_attempts = cursor.fetchone()
        assert published == 10
        assert minimum_attempts == maximum_attempts == 1

        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO outbox (kind,aggregate_id,payload)
                VALUES ('integration.unregistered',%s,'{}'::jsonb) RETURNING id""",
                (f"{prefix}-unknown",),
            )
            unknown_id = cursor.fetchone()[0]
            outbox_ids.append(unknown_id)
            connection.commit()
        unsupported = await RedisOutboxPublisher(APP_DSN, redis, stream).publish_batch(limit=1)
        assert unsupported.published == 0
        assert unsupported.failed == 1
        assert await redis.xlen(stream) == 10
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT published_at,attempts,last_error FROM outbox WHERE id=%s",
                (unknown_id,),
            )
            unknown_published_at, unknown_attempts, unknown_error = cursor.fetchone()
        assert unknown_published_at is None
        assert unknown_attempts == 1
        assert "not registered for stream publication" in unknown_error
    finally:
        if outbox_ids:
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute("DELETE FROM outbox WHERE id=ANY(%s)", (outbox_ids,))
                connection.commit()
        await redis.delete(stream)
        await redis.aclose()
