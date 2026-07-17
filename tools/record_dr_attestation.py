"""Append a verified disaster-recovery exercise to the production database."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re

import psycopg

try:
    from tools.verify_restored_database import report_digest
except ModuleNotFoundError:  # direct ``python tools/record_dr_attestation.py`` execution
    from verify_restored_database import report_digest


def validate_report(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schemaVersion") != "restored-database-verification-v1":
        raise ValueError("unsupported restore verification report")
    claimed = payload.pop("verificationDigest", None)
    actual = report_digest(payload)
    payload["verificationDigest"] = claimed
    if claimed != actual or not re.fullmatch(r"sha256:[0-9a-f]{64}", str(claimed)):
        raise ValueError("restore verification report digest mismatch")
    return payload


def record(
    *, dsn: str, report_path: Path, performed_at: str, backup_reference: str,
    restored_instance_id: str, measured_rpo_seconds: int, measured_rto_seconds: int,
    operator_subject: str, notes: str, status: str,
) -> str:
    report = validate_report(report_path)
    if report.get("restoredInstanceId") != restored_instance_id:
        raise ValueError("restore report is bound to a different instance")
    if report.get("backupReference") != backup_reference:
        raise ValueError("restore report is bound to a different backup")
    if measured_rpo_seconds < 0 or measured_rto_seconds < 0:
        raise ValueError("RPO and RTO measurements cannot be negative")
    if status == "passed" and report.get("qualifies") is not True:
        raise ValueError("a failed restore verification cannot be recorded as passed")
    if status == "passed" and (measured_rpo_seconds > 3600 or measured_rto_seconds > 14400):
        raise ValueError("a recovery outside the RPO/RTO target cannot be recorded as passed")
    performed = datetime.fromisoformat(performed_at.replace("Z", "+00:00"))
    if performed.tzinfo is None:
        raise ValueError("performed-at must include a timezone")
    if not backup_reference or not restored_instance_id or not operator_subject:
        raise ValueError("backup, restored instance and operator identities are required")
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """INSERT INTO disaster_recovery_attestations
            (performed_at,backup_reference,restored_instance_id,verification_digest,
             measured_rpo_seconds,measured_rto_seconds,status,operator_subject,notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
            (
                performed, backup_reference, restored_instance_id, report["verificationDigest"],
                measured_rpo_seconds, measured_rto_seconds, status, operator_subject, notes,
            ),
        )
        row = cursor.fetchone()
        connection.commit()
    return str(row[0])


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description="Record a reviewed PITR exercise attestation")
    connection = result.add_mutually_exclusive_group(required=True)
    connection.add_argument("--dsn", help="DBA DSN; prefer --dsn-file to avoid process-list exposure")
    connection.add_argument("--dsn-file", type=Path, help="root-owned file containing the DBA DSN")
    result.add_argument("--report", type=Path, required=True)
    result.add_argument("--performed-at", required=True)
    result.add_argument("--backup-reference", required=True)
    result.add_argument("--restored-instance-id", required=True)
    result.add_argument("--confirm-restored-instance", required=True)
    result.add_argument("--measured-rpo-seconds", type=int, required=True)
    result.add_argument("--measured-rto-seconds", type=int, required=True)
    result.add_argument("--status", choices=("passed", "failed"), required=True)
    result.add_argument("--operator-subject", required=True)
    result.add_argument("--notes", default="")
    return result


def main() -> int:
    args = parser().parse_args()
    if args.confirm_restored_instance != args.restored_instance_id:
        raise SystemExit("confirmation does not match restored instance ID")
    dsn = args.dsn if args.dsn is not None else args.dsn_file.read_text(encoding="utf-8").strip()
    if not dsn:
        raise SystemExit("DBA DSN is empty")
    record_id = record(
        dsn=dsn, report_path=args.report, performed_at=args.performed_at,
        backup_reference=args.backup_reference, restored_instance_id=args.restored_instance_id,
        measured_rpo_seconds=args.measured_rpo_seconds,
        measured_rto_seconds=args.measured_rto_seconds, operator_subject=args.operator_subject,
        notes=args.notes, status=args.status,
    )
    print(json.dumps({"recorded": True, "attestationId": record_id}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
