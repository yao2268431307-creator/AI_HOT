"""Read-only integrity verification for an isolated PostgreSQL PITR restore."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import psycopg


RLS_TABLES = {
    "feedback", "alert_rules", "alert_deliveries", "watchlists",
    "product_interactions", "cluster_edit_requests", "metric_incidents",
    "workspace_memberships",
}
OUTBOX_KINDS = {
    "observation.created", "metric_snapshots.created", "score.created",
    "feedback.created", "cluster.edit.requested", "event.rescore.requested",
    "source.erased",
}


def report_digest(payload: dict[str, object]) -> str:
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(material).hexdigest()


def verify(
    dsn: str,
    *,
    minimum_sources: int,
    minimum_observations: int,
    minimum_events: int,
    restored_instance_id: str,
    backup_reference: str,
) -> dict[str, object]:
    if not restored_instance_id or not backup_reference:
        raise ValueError("restored instance ID and backup reference are required")
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SELECT current_database(),clock_timestamp(),current_user")
        database_name, database_time, database_user = cursor.fetchone()
        cursor.execute("SELECT value FROM schema_attestations WHERE key='migration_version'")
        migration_row = cursor.fetchone()
        cursor.execute("SELECT extname FROM pg_extension WHERE extname IN ('vector','pgcrypto') ORDER BY extname")
        extensions = [str(row[0]) for row in cursor.fetchall()]
        cursor.execute(
            """SELECT roles.rolname,roles.rolsuper,roles.rolcreatedb,roles.rolcreaterole,
            roles.rolinherit,roles.rolreplication,roles.rolbypassrls,
            (SELECT count(*) FROM pg_auth_members membership
             WHERE membership.roleid=roles.oid OR membership.member=roles.oid)
            FROM pg_roles roles
            WHERE roles.rolname IN ('radar_app','radar_deletion_worker') ORDER BY roles.rolname"""
        )
        roles = {
            str(row[0]): {
                "superuser": bool(row[1]), "createDb": bool(row[2]),
                "createRole": bool(row[3]), "inherit": bool(row[4]),
                "replication": bool(row[5]), "bypassRls": bool(row[6]),
                "memberships": int(row[7]),
            }
            for row in cursor.fetchall()
        }
        cursor.execute(
            """SELECT relation.relname,relation.relrowsecurity,relation.relforcerowsecurity
            FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
            WHERE namespace.nspname='public' AND relation.relname=ANY(%s)""",
            (list(RLS_TABLES),),
        )
        rls = {str(row[0]): bool(row[1] and row[2]) for row in cursor.fetchall()}
        cursor.execute(
            """SELECT
              (SELECT count(*) FROM sources),
              (SELECT count(*) FROM observations),
              (SELECT count(*) FROM events),
              (SELECT count(*) FROM score_runs),
              (SELECT count(*) FROM outbox)"""
        )
        counts_row = cursor.fetchone()
        counts = {
            "sources": int(counts_row[0]), "observations": int(counts_row[1]),
            "events": int(counts_row[2]), "scoreRuns": int(counts_row[3]),
            "outbox": int(counts_row[4]),
        }
        cursor.execute(
            """SELECT
              count(*) FILTER (WHERE raw_evidence_ref !~ '^r2://[^/]+/.+'),
              count(*) FILTER (WHERE content_fingerprint IS NULL OR content_fingerprint='')
            FROM observations"""
        )
        malformed_refs, missing_fingerprints = cursor.fetchone()
        cursor.execute(
            """SELECT input_digest,created_at,
               (SELECT count(*) FROM score_runs WHERE input_digest !~ '^sha256:[0-9a-f]{64}$')
            FROM score_runs ORDER BY created_at DESC,id DESC LIMIT 1"""
        )
        score_row = cursor.fetchone()
        malformed_score_digests = int(score_row[2]) if score_row else 0
        cursor.execute(
            """SELECT
              count(*) FILTER (WHERE published_at IS NULL),
              count(*) FILTER (WHERE published_at IS NOT NULL),
              min(created_at),max(created_at),
              count(*) FILTER (
                WHERE kind <> ALL(%s)
                   OR jsonb_typeof(payload) <> 'object'
                   OR published_at < created_at
                   OR attempts < 0
              )
            FROM outbox""",
            (list(OUTBOX_KINDS),),
        )
        outbox_row = cursor.fetchone()

    role_ready = (
        set(roles) == {"radar_app", "radar_deletion_worker"}
        and all(not any(values.values()) for values in roles.values())
    )
    gates = {
        "databaseName": database_name == "ai_hot",
        "migration": bool(migration_row and migration_row[0] == "001_init_rc3.1"),
        "extensions": extensions == ["pgcrypto", "vector"],
        "leastPrivilegeRoles": role_ready,
        "forcedRls": set(rls) == RLS_TABLES and all(rls.values()),
        "sourceFloor": counts["sources"] >= minimum_sources,
        "observationFloor": counts["observations"] >= minimum_observations,
        "eventFloor": counts["events"] >= minimum_events,
        "scoreRunPresence": counts["observations"] == 0 or counts["scoreRuns"] > 0,
        "outboxPresence": counts["observations"] == 0 or counts["outbox"] > 0,
        "rawReferences": int(malformed_refs) == 0,
        "contentFingerprints": int(missing_fingerprints) == 0,
        "scoreDigests": malformed_score_digests == 0,
        "outboxShape": int(outbox_row[4]) == 0,
    }
    payload: dict[str, object] = {
        "schemaVersion": "restored-database-verification-v1",
        "verifiedAt": datetime.now(timezone.utc).isoformat(),
        "restoredInstanceId": restored_instance_id,
        "backupReference": backup_reference,
        "database": {
            "name": str(database_name), "clock": database_time.isoformat(),
            "verificationUser": str(database_user), "migrationVersion": migration_row[0] if migration_row else None,
            "extensions": extensions, "roles": roles, "forcedRls": rls,
        },
        "counts": counts,
        "integrity": {
            "malformedRawEvidenceReferences": int(malformed_refs),
            "missingContentFingerprints": int(missing_fingerprints),
            "malformedScoreInputDigests": malformed_score_digests,
            "latestScoreInputDigest": score_row[0] if score_row else None,
            "latestScoreCreatedAt": score_row[1].isoformat() if score_row else None,
            "unpublishedOutbox": int(outbox_row[0]), "publishedOutbox": int(outbox_row[1]),
            "outboxFirstCreatedAt": outbox_row[2].isoformat() if outbox_row[2] else None,
            "outboxLastCreatedAt": outbox_row[3].isoformat() if outbox_row[3] else None,
            "malformedOutboxRows": int(outbox_row[4]),
        },
        "thresholds": {
            "minimumSources": minimum_sources,
            "minimumObservations": minimum_observations,
            "minimumEvents": minimum_events,
        },
        "gates": gates,
        "qualifies": all(gates.values()),
    }
    payload["verificationDigest"] = report_digest(payload)
    return payload


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Verify an isolated AI Hot Radar PostgreSQL restore")
    connection = result.add_mutually_exclusive_group(required=True)
    connection.add_argument("--dsn")
    connection.add_argument("--dsn-file", type=Path)
    result.add_argument("--minimum-sources", type=int, default=0)
    result.add_argument("--minimum-observations", type=int, default=0)
    result.add_argument("--minimum-events", type=int, default=0)
    result.add_argument("--restored-instance-id", required=True)
    result.add_argument("--backup-reference", required=True)
    result.add_argument("--output", type=Path, required=True)
    return result


def main() -> int:
    args = parser().parse_args()
    dsn = args.dsn if args.dsn is not None else args.dsn_file.read_text(encoding="utf-8").strip()
    if not dsn:
        raise ValueError("restore DSN is empty")
    payload = verify(
        dsn,
        minimum_sources=args.minimum_sources,
        minimum_observations=args.minimum_observations,
        minimum_events=args.minimum_events,
        restored_instance_id=args.restored_instance_id,
        backup_reference=args.backup_reference,
    )
    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if payload["qualifies"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
