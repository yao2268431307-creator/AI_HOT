from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import threading
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

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
from radar.processor import EventProcessor
from radar.scoring import replay_score_payload
from radar.storage import ConcurrentScoreConflict, InMemoryRepository, PostgresRepository
from radar.stream_retention import (
    inspect_consumer_groups,
    maintain_stream_retention,
    trim_stream_at_verified_watermarks,
)
from tools.replay_outbox_to_redis import execute_replay, main as replay_main
from tools.production_capacity_probe import dataset_counts


APP_DSN = os.getenv("POSTGRES_INTEGRATION_DSN")
ADMIN_DSN = os.getenv("POSTGRES_INTEGRATION_ADMIN_DSN")
REDIS_URL = os.getenv("REDIS_INTEGRATION_URL")
DELETION_DSN = os.getenv("POSTGRES_INTEGRATION_DELETION_DSN")
pytestmark = pytest.mark.skipif(
    not APP_DSN or not ADMIN_DSN or not DELETION_DSN,
    reason="set PostgreSQL app/admin/deletion integration DSNs for destructive local integration tests",
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
        cursor.execute("DELETE FROM raw_evidence_deletions WHERE reference LIKE %s", (f"%{prefix}%",))
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
        cursor.execute("DELETE FROM baseline_samples WHERE source_event_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM outbox WHERE aggregate_id LIKE %s", (f"{prefix}%",))
        cursor.execute("DELETE FROM events WHERE id LIKE %s", (f"{prefix}%",))
        cursor.execute("SET session_replication_role='origin'")
        connection.commit()


def test_postgres_runtime_attestation_and_production_health(monkeypatch: pytest.MonkeyPatch) -> None:
    assert APP_DSN is not None
    assert ADMIN_DSN is not None
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("RADAR_AUTH_MODE", "jwt")
    public_pem = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    monkeypatch.setenv("RADAR_JWT_PUBLIC_KEYS", json.dumps({"integration-key": public_pem}))
    monkeypatch.setenv("RADAR_JWT_ISSUER", "https://identity.integration")
    monkeypatch.setenv("RADAR_JWT_AUDIENCE", "signal-ai-integration")
    monkeypatch.setenv("RADAR_INSTANCE_ID", "postgres-integration-rc24")
    # The API consumes fresh runtime proofs; service isolation means it must
    # not hold the Scheduler/Alert Redis or R2 credentials itself.
    for name in (
        "REDIS_URL", "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
        "RAW_EVIDENCE_BUCKET",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("RADAR_RELEASE_IMAGE_DIGESTS", json.dumps({
        "api": "sha256:" + "a" * 64, "web": "sha256:" + "b" * 64,
    }))
    monkeypatch.setenv("RADAR_ACTUAL_IMAGE_DIGESTS", json.dumps({
        "api": "sha256:" + "a" * 64, "web": "sha256:" + "b" * 64,
    }))
    now = datetime.now(timezone.utc)
    with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
        for component in (
            "scheduler", "collector-worker", "retention-worker", "outbox-publisher", "alert-consumer",
        ):
            details = {
                **({
                    "redisVerified": True, "r2ReadWriteVerified": True,
                    "dependencyProbeAt": now.isoformat(),
                } if component == "collector-worker" else {}),
                **({"redisVerified": True} if component == "outbox-publisher" else {}),
                **({
                    "redisVerified": True, "r2DeleteVerified": True,
                    "dependencyProbeAt": now.isoformat(),
                } if component == "alert-consumer" else {}),
            }
            cursor.execute(
                """INSERT INTO runtime_component_heartbeats (component_id,instance_id,last_seen_at,details)
                VALUES (%s,'postgres-integration-rc24',%s,%s::jsonb)
                ON CONFLICT (component_id) DO UPDATE SET instance_id=excluded.instance_id,
                  last_seen_at=excluded.last_seen_at,details=excluded.details""",
                (component, now, json.dumps(details)),
            )
        cursor.execute(
            """INSERT INTO disaster_recovery_attestations
            (performed_at,backup_reference,restored_instance_id,verification_digest,
             measured_rpo_seconds,measured_rto_seconds,status,operator_subject)
            VALUES (%s,'integration-backup','integration-restore',%s,300,900,'passed','integration-operator')""",
            (now, "sha256:" + "c" * 64),
        )
        connection.commit()
    repository = PostgresRepository(APP_DSN, DELETION_DSN)
    attestation = repository.runtime_attestation()
    expected = {
        "storageBackend": "postgresql",
        "rlsVerified": True,
        "migrationVersion": "001_init_rc3.1",
        "auditTriggersVerified": True,
        "migrationMarkerReadOnly": True,
        "instanceId": "postgres-integration-rc24",
        "databaseUser": "radar_app",
        "databaseRoleSuperuser": False,
        "databaseRoleBypassRls": False,
        "databaseRoleLeastPrivilege": True,
        "deletionRole": "radar_deletion_worker",
        "deletionRoleReady": True,
    }
    for key, value in expected.items():
        assert attestation[key] == value
    assert float(attestation["databaseClockSkewSeconds"]) <= 5
    with TestClient(create_app(repository)) as client:
        health = client.get("/health")
        ready = client.get("/health/ready")
    assert health.status_code == 200
    assert "databaseUser" not in health.json()
    assert ready.status_code == 200
    assert ready.json()["productionReady"] is True


def test_postgres_workspace_membership_and_jti_revocation() -> None:
    assert APP_DSN is not None
    assert ADMIN_DSN is not None
    suffix = uuid.uuid4().hex
    workspace_id = f"workspace-auth-{suffix}"
    subject = f"subject-{suffix}"
    jti = f"jti-{suffix}"
    try:
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO workspace_memberships (workspace_id,subject,role,status) VALUES (%s,%s,'ANALYST','active')",
                (workspace_id, subject),
            )
            cursor.execute(
                "INSERT INTO jwt_revocations (jti,expires_at,reason) VALUES (%s,clock_timestamp()+interval '5 minutes','integration')",
                (jti,),
            )
            connection.commit()
        repository = PostgresRepository(APP_DSN)
        assert repository.resolve_workspace_membership(subject, workspace_id) == "ANALYST"
        assert repository.resolve_workspace_membership(subject, f"other-{workspace_id}") is None
        assert repository.is_token_revoked(jti) is True
        assert repository.is_token_revoked(f"other-{jti}") is False
    finally:
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("DELETE FROM jwt_revocations WHERE jti=%s", (jti,))
            cursor.execute(
                "DELETE FROM workspace_memberships WHERE workspace_id=%s AND subject=%s",
                (workspace_id, subject),
            )
            connection.commit()


def test_postgres_event_compare_and_swap_prevents_stale_overwrite_and_uses_vector_knn() -> None:
    assert APP_DSN is not None
    prefix = f"it-cas-{uuid.uuid4().hex[:10]}"
    workspace_id = f"workspace-{prefix}"
    repository = PostgresRepository(APP_DSN)
    first_id = f"{prefix}-near"
    second_id = f"{prefix}-far"
    try:
        template = demo_events()[0]
        first = template.model_copy(update={"id": first_id, "title": f"Near {prefix}"})
        second = template.model_copy(update={"id": second_id, "title": f"Far {prefix}"})
        repository.upsert_event(first)
        repository.upsert_event(second)
        fresh = repository.get_event(first_id)
        stale = repository.get_event(first_id)
        assert fresh is not None and stale is not None
        repository.upsert_event(fresh.model_copy(update={"driver": "winner"}))
        with pytest.raises(ConcurrentScoreConflict):
            repository.upsert_event(stale.model_copy(update={"driver": "stale-loser"}))
        assert repository.get_event(first_id).driver == "winner"  # type: ignore[union-attr]

        near_vector = [1.0, *([0.0] * 1023)]
        far_vector = [0.0, 1.0, *([0.0] * 1022)]
        repository.save_event_embedding(first_id, "integration-vector:1024", first.title, near_vector)
        repository.save_event_embedding(second_id, "integration-vector:1024", second.title, far_vector)
        nearest = repository.nearest_event_embeddings(
            near_vector,
            {first_id: first.title, second_id: second.title},
            "integration-vector:1024",
            limit=1,
        )
        assert list(nearest) == [first_id]

        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT has_function_privilege(current_user,'erase_source_score_history(text)','EXECUTE')"
            )
            assert cursor.fetchone()[0] is False
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute("DELETE FROM events WHERE id=%s", (first_id,))
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO score_history_erasure_audit
                    (source_id,event_ids,reason,erased_score_runs,erased_baseline_samples,actor)
                    VALUES ('forged',%s,'source_erasure',0,0,'forged')""",
                    ([first_id],),
                )
    finally:
        _cleanup_event(prefix, workspace_id)


@pytest.mark.asyncio
async def test_postgres_source_erasure_uses_isolated_role_and_hides_empty_tombstone() -> None:
    assert APP_DSN is not None and ADMIN_DSN is not None and DELETION_DSN is not None
    prefix = f"it-erasure-{uuid.uuid4().hex[:10]}"
    workspace_id = f"workspace-{prefix}"
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    first = Observation(
        id=f"{prefix}-a-observation", platform="RSS", externalId=f"{prefix}-a",
        sourceId=f"{prefix}-source-a", publishedAt=now, collectedAt=now,
        language="en", title=f"Shared release {prefix}", text=f"Shared release {prefix}",
        url=f"https://example.com/{prefix}/a", metrics={"mentions": 10},
        rawEvidenceRef=f"r2://raw/{prefix}-a.json", contentFingerprint=f"{prefix}-a-fp",
        signalFamily="discussion", relation="original",
    )
    second = first.model_copy(update={
        "id": f"{prefix}-b-observation", "external_id": f"{prefix}-b",
        "source_id": f"{prefix}-source-b", "url": f"https://example.com/{prefix}/b",
        "raw_evidence_ref": f"r2://raw/{prefix}-b.json",
        "content_fingerprint": f"{prefix}-b-fp", "signal_family": "official",
    })
    repository = PostgresRepository(APP_DSN, DELETION_DSN)
    event_id: str | None = None
    try:
        assert repository.save_observation_with_outbox(first)
        event_id = (await EventProcessor(repository).process(first)).id
        assert repository.save_observation_with_outbox(second)
        await EventProcessor(repository).process(second)
        assert repository.list_score_runs(event_id)

        assert repository.purge_source(first.source_id) == 1
        retained = repository.get_event(event_id)
        assert retained is not None and retained.state.value == "insufficient_data"
        assert repository.list_score_runs(event_id) == []
        assert all(item.source != first.source_id for item in retained.evidence)

        assert repository.purge_source(second.source_id) == 1
        tombstone = repository.get_event(event_id)
        assert tombstone is not None and tombstone.superseded_by
        assert tombstone.title == "已删除事件"
        assert all(item.id != event_id for item in repository.list_events())
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                """SELECT source_id,actor FROM score_history_erasure_audit
                WHERE source_id LIKE %s ORDER BY created_at""",
                (f"{prefix}%",),
            )
            rows = cursor.fetchall()
        assert rows == [
            (first.source_id, "radar_deletion_worker"),
            (second.source_id, "radar_deletion_worker"),
        ]
    finally:
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SET session_replication_role='replica'")
            cursor.execute(
                "DELETE FROM score_history_erasure_audit WHERE source_id LIKE %s",
                (f"{prefix}%",),
            )
            cursor.execute("SET session_replication_role='origin'")
            connection.commit()
        _cleanup_source(prefix)
        _cleanup_event(prefix, workspace_id)


def test_postgres_atomic_budget_reservations_cannot_jointly_exceed_scope() -> None:
    assert APP_DSN is not None
    assert ADMIN_DSN is not None
    connector_id = f"it-budget-{uuid.uuid4().hex[:10]}"
    family = f"family-{connector_id}"

    def reserve() -> str | None:
        return PostgresRepository(APP_DSN).reserve_connector_budget(
            connector_id, family, 60, 1_000_000_000, 100, 100, {connector_id}, 0,
        )

    reservation_id: str | None = None
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            reservations = list(pool.map(lambda _: reserve(), range(2)))
        assert sum(value is not None for value in reservations) == 1
        reservation_id = next(value for value in reservations if value is not None)
        repository = PostgresRepository(APP_DSN)
        assert repository.monthly_connector_spend(
            connector_ids={connector_id},
        ) == 60
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE connector_budget_reservations SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_token=%s",
                (reservation_id,),
            )
            connection.commit()
        assert repository.reconcile_expired_connector_budget_reservations() == 1
        assert repository.connector_budget_reconciliation_count() == 1
        assert repository.monthly_connector_spend(connector_ids={connector_id}) == 60
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT has_function_privilege(current_user,'resolve_connector_budget_reservation(uuid,text,numeric,text)','EXECUTE')"
            )
            assert cursor.fetchone()[0] is False
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT resolve_connector_budget_reservation(%s,'released',0,'integration verified no provider request')",
                (reservation_id,),
            )
            assert cursor.fetchone()[0] == "released"
            connection.commit()
        assert repository.connector_budget_reconciliation_count() == 0
        assert repository.monthly_connector_spend(connector_ids={connector_id}) == 0
    finally:
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute("SET session_replication_role='replica'")
            cursor.execute(
                "DELETE FROM connector_budget_reconciliation_audit WHERE connector_id=%s",
                (connector_id,),
            )
            cursor.execute(
                "DELETE FROM connector_budget_reservations WHERE connector_id=%s",
                (connector_id,),
            )
            cursor.execute("SET session_replication_role='origin'")
            connection.commit()


def test_production_capacity_counts_only_non_superseded_events() -> None:
    assert APP_DSN is not None
    assert ADMIN_DSN is not None
    prefix = f"it-capacity-{uuid.uuid4().hex[:10]}"
    workspace_id = f"workspace-{prefix}"
    repository = PostgresRepository(APP_DSN)
    try:
        before = dataset_counts(APP_DSN)
        template = demo_events()[0]
        active_id = f"{prefix}-active"
        superseded_id = f"{prefix}-superseded"
        repository.upsert_event(template.model_copy(update={"id": active_id}))
        repository.upsert_event(template.model_copy(update={"id": superseded_id}))
        with psycopg.connect(ADMIN_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "UPDATE events SET superseded_by=%s WHERE id=%s",
                ([active_id], superseded_id),
            )
            connection.commit()
        after = dataset_counts(APP_DSN)
        assert after["activeEvents"] == before["activeEvents"] + 1
    finally:
        _cleanup_event(prefix, workspace_id)


@pytest.mark.asyncio
async def test_postgres_score_revisions_are_append_only_replayable_and_bind_availability() -> None:
    assert APP_DSN is not None
    prefix = f"it-score-{uuid.uuid4().hex[:10]}"
    current = datetime.now(timezone.utc)
    now = current.replace(minute=(current.minute // 15) * 15 + 5, second=0, microsecond=0)
    first = Observation(
        id=f"{prefix}-observation", platform="RSS", externalId=prefix,
        sourceId=f"{prefix}-source", publishedAt=now - timedelta(minutes=2),
        availableAt=now - timedelta(minutes=1), availabilityBasis="provider_timestamp",
        collectedAt=now, language="en", title=f"Unique replay release {prefix}",
        text=f"Official unique replay release {prefix}", url=f"https://example.com/{prefix}",
        metrics={"mentions": 10}, rawEvidenceRef=f"r2://raw/{prefix}-1.json",
        contentFingerprint=f"{prefix}-fingerprint", signalFamily="discussion", relation="original",
    )
    second = first.model_copy(update={
        "collected_at": now + timedelta(minutes=1), "metrics": {"mentions": 40},
        "raw_evidence_ref": f"r2://raw/{prefix}-2.json",
    })
    repository = PostgresRepository(APP_DSN)
    event_id: str | None = None
    try:
        assert repository.save_observation_with_outbox(first)
        event = await EventProcessor(repository).process(first)
        event_id = event.id
        assert repository.save_observation_with_outbox(second)
        event = await EventProcessor(repository).process(second)
        runs = repository.list_score_runs(event.id)
        assert len(runs) == 2
        assert [run.scoring_revision for run in runs] == [1, 2]
        assert len({run.input_digest for run in runs}) == 2
        expected = runs[-1].payload["_replay"]["expected"]
        replayed = replay_score_payload(runs[-1].payload)
        assert replayed.state.value == expected["state"]
        assert [label.value for label in replayed.labels] == expected["labels"]
        assert runs[-1].baseline_digest.startswith("sha256:")
        assert runs[-1].feature_registry_digest.startswith("sha256:")
        priority = repository.review_priority_context({event.id: first.collected_at})[event.id]
        assert len(priority["scoreRuns"]) == 2
        embedding = [1.0, *([0.0] * 1023)]
        repository.save_event_embedding(event.id, "integration-bge:1024", event.title, embedding)
        loaded_embeddings = repository.load_event_embeddings(
            {event.id: event.title}, "integration-bge:1024", 1024,
        )
        assert loaded_embeddings[event.id][:2] == [1.0, 0.0]
        assert repository.load_event_embeddings(
            {event.id: event.title + " changed"}, "integration-bge:1024", 1024,
        ) == {}
        loaded = repository.get_latest_observation(first.id)
        assert loaded is not None
        assert loaded.available_at == first.available_at
        assert loaded.availability_basis == "provider_timestamp"
    finally:
        if event_id:
            _cleanup_event(event_id, "integration-score-workspace")
        _cleanup_source(prefix)


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


def test_postgres_provenance_upgrade_is_persisted_and_reloaded() -> None:
    assert APP_DSN is not None
    prefix = f"it-provenance-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    candidate = Observation(
        id=f"{prefix}-observation", platform="Bluesky", externalId=f"at://{prefix}",
        sourceId=f"{prefix}-source", publishedAt=now, collectedAt=now, language="en",
        title="AI model", text="AI model", url=f"https://example.com/{prefix}", metrics={},
        rawEvidenceRef=f"r2://raw/{prefix}-candidate.json", relation="original",
        contentFingerprint=prefix, signalFamily="discussion", provenanceLevel="unverified_discovery",
    )
    verified = candidate.model_copy(update={
        "provenance_level": "provider_verified", "metrics": {"comments": 3},
        "title": "Verified security advisory", "text": "Verified CVE security advisory",
        "url": f"https://example.com/{prefix}/verified",
        "content_fingerprint": f"{prefix}-verified-fingerprint",
        "collected_at": now + timedelta(minutes=1),
        "raw_evidence_ref": f"r2://raw/{prefix}-verified.json",
    })
    repository = PostgresRepository(APP_DSN)
    try:
        assert repository.save_observation_with_outbox(candidate) is True
        assert repository.save_observation_with_outbox(verified) is True
        loaded = repository.get_latest_observation(candidate.id)
        assert loaded is not None
        assert loaded.provenance_level == "provider_verified"
        assert loaded.raw_evidence_ref == verified.raw_evidence_ref
        assert loaded.title == verified.title
        assert loaded.text == verified.text
        assert loaded.url == verified.url
        assert loaded.content_fingerprint == verified.content_fingerprint
        assert loaded.metrics == {"comments": 3.0}
        profiles = repository.list_source_profiles()
        profile = next(item for item in profiles if item["id"] == candidate.source_id)
        assert profile["validObservations"] == 1
        assert "provider_verified_upgrade" in profile["discoveryReasons"]
        ignored_ref = f"r2://raw/{prefix}-ignored-unverified.json"
        ignored = verified.model_copy(update={
            "provenance_level": "unverified_discovery", "metrics": {"score": 1_000_000_000},
            "raw_evidence_ref": ignored_ref,
        })
        assert repository.save_observation_with_outbox(ignored) is False
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM raw_evidence_deletions WHERE reference=%s",
                (candidate.raw_evidence_ref,),
            )
            assert cursor.fetchone() == ("pending",)
            cursor.execute(
                "SELECT status FROM raw_evidence_deletions WHERE reference=%s",
                (ignored_ref,),
            )
            assert cursor.fetchone() == ("pending",)
    finally:
        _cleanup_source(prefix)


def test_postgres_concurrent_same_id_provenance_upgrade_is_exactly_once() -> None:
    assert APP_DSN is not None
    prefix = f"it-provenance-race-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    candidate = Observation(
        id=f"{prefix}-observation", platform="Generic", externalId=f"external:{prefix}",
        sourceId=f"{prefix}-source", publishedAt=now, collectedAt=now, language="en",
        title="AI candidate", text="AI candidate", url=f"https://example.com/{prefix}", metrics={},
        rawEvidenceRef=f"r2://raw/{prefix}-candidate.json", relation="original",
        contentFingerprint=f"{prefix}-candidate", signalFamily="discussion",
        provenanceLevel="unverified_discovery",
    )
    verified = candidate.model_copy(update={
        "provenance_level": "provider_verified", "title": "Verified AI release",
        "text": "Verified AI release", "content_fingerprint": f"{prefix}-verified",
        "collected_at": now + timedelta(minutes=1), "metrics": {"comments": 4},
        "raw_evidence_ref": f"r2://raw/{prefix}-verified.json",
    })
    repository = PostgresRepository(APP_DSN)
    barrier = threading.Barrier(2)

    def upgrade() -> bool:
        barrier.wait(timeout=10)
        return PostgresRepository(APP_DSN).save_observation_with_outbox(verified)

    try:
        assert repository.save_observation_with_outbox(candidate) is True
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: upgrade(), range(2)))
        assert sorted(results) == [False, True]
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT valid_observations FROM sources WHERE id=%s",
                (candidate.source_id,),
            )
            assert cursor.fetchone() == (1,)
            cursor.execute(
                "SELECT revision FROM observation_processing WHERE observation_id=%s",
                (candidate.id,),
            )
            assert cursor.fetchone() == (2,)
            cursor.execute(
                "SELECT count(*) FROM content_ingest_history WHERE observation_id=%s",
                (candidate.id,),
            )
            assert cursor.fetchone() == (2,)
            cursor.execute(
                "SELECT count(*) FROM raw_evidence_deletions WHERE reference=%s AND status='pending'",
                (candidate.raw_evidence_ref,),
            )
            assert cursor.fetchone() == (1,)
    finally:
        _cleanup_source(prefix)


def test_postgres_unverified_observations_never_advance_source_validity() -> None:
    assert APP_DSN is not None
    prefix = f"it-unverified-{uuid.uuid4().hex[:8]}"
    now = datetime.now(timezone.utc)
    repository = PostgresRepository(APP_DSN)
    try:
        for index in range(2):
            item = Observation(
                id=f"{prefix}-observation-{index}", platform="Bluesky",
                externalId=f"at://{prefix}/{index}", sourceId=f"{prefix}-source",
                publishedAt=now, collectedAt=now + timedelta(seconds=index), language="en",
                title=f"AI discovery {index}", text=f"AI discovery candidate {index}",
                url=f"https://example.com/{prefix}/{index}", metrics={},
                rawEvidenceRef=f"r2://raw/{prefix}-{index}.json", relation="original",
                contentFingerprint=f"{prefix}-fingerprint-{index}", signalFamily="discussion",
                provenanceLevel="unverified_discovery",
            )
            if index == 0:
                item = item.model_copy(update={"metrics": {"score": 1_000_000_000}})
            assert repository.save_observation_with_outbox(item) is True
        profile = next(item for item in repository.list_source_profiles() if item["id"] == f"{prefix}-source")
        assert profile["validObservations"] == 0
        with psycopg.connect(APP_DSN) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT count(*) FROM metric_snapshots WHERE subject_id LIKE %s",
                (f"{prefix}%",),
            )
            assert cursor.fetchone()[0] == 0
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
