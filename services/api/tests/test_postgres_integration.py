from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis

from radar.contracts import Observation
from radar.main import create_app
from radar.outbox import RedisOutboxPublisher
from radar.storage import PostgresRepository


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
        "migrationVersion": "001_init_rc2.4",
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
    finally:
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
async def test_postgres_outbox_publishes_to_real_redis_stream() -> None:
    assert APP_DSN is not None
    assert REDIS_URL is not None
    aggregate_id = f"integration-outbox-{uuid.uuid4().hex}"
    stream = f"integration:outbox:{uuid.uuid4().hex}"
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    outbox_id: uuid.UUID | None = None
    try:
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL")
            if cursor.fetchone()[0] != 0:
                pytest.skip("requires an isolated integration database with an empty pending outbox")
            cursor.execute(
                """INSERT INTO outbox (kind,aggregate_id,payload)
                VALUES ('integration.created',%s,%s::jsonb) RETURNING id""",
                (aggregate_id, json.dumps({"aggregateId": aggregate_id})),
            )
            outbox_id = cursor.fetchone()[0]
            connection.commit()

        result = await RedisOutboxPublisher(APP_DSN, redis, stream).publish_batch(limit=1)
        assert result.published == 1
        assert result.failed == 0
        rows = await redis.xrange(stream)
        assert len(rows) == 1
        _, fields = rows[0]
        assert fields["outbox_id"] == str(outbox_id)
        assert fields["kind"] == "integration.created"
        assert fields["aggregate_id"] == aggregate_id
        assert json.loads(fields["payload"]) == {"aggregateId": aggregate_id}

        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SELECT published_at,attempts,last_error FROM outbox WHERE id=%s", (outbox_id,))
            published_at, attempts, last_error = cursor.fetchone()
        assert published_at is not None
        assert attempts == 1
        assert last_error is None
    finally:
        if outbox_id is not None:
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute("DELETE FROM outbox WHERE id=%s", (outbox_id,))
                connection.commit()
        await redis.delete(stream)
        await redis.aclose()
