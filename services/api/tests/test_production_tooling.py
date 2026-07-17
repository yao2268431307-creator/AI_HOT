from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from radar.auth import jwt_configuration_ready
from tools.acceptance_monitor import load_keyring
from tools import generate_acceptance_keyring as keygen
from tools.production_capacity_probe import run as run_capacity_probe
from tools.production_preflight import FORMAL_CONNECTORS, run as run_preflight
from tools.provision_production_database import ensure_database
from tools.record_dr_attestation import record as record_dr_attestation, validate_report
from tools.render_verified_release import (
    file_sha256,
    render_release_env,
    validate_checksum,
    validate_manifest,
    verify_supply_chain,
)
from tools.run_scheduled_acceptance import (
    already_collected,
    fetch_client_credentials_token,
    parser as acceptance_parser,
    scheduled_slot,
)
from tools.verify_restored_database import report_digest


ROOT = Path(__file__).resolve().parents[3]
DIGESTS = {"api": "sha256:" + "a" * 64, "web": "sha256:" + "b" * 64}


def write_private(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)


def create_bundle(tmp_path: Path) -> Path:
    release_digest_json = json.dumps(DIGESTS, separators=(",", ":"))
    jwt_keys = json.dumps({"idp-key-202607": "-----BEGIN PUBLIC KEY-----\nkey\n-----END PUBLIC KEY-----"})
    database = "postgresql://radar_app:long-random-secret@db.acme.test/ai_hot?sslmode=verify-full"
    write_private(
        tmp_path / "api.env",
        "\n".join(
            (
                "DEMO_MODE=false",
                "AUTH_REQUIRED=true",
                "RADAR_AUTH_MODE=jwt",
                "CORS_ORIGINS=https://radar.acme.test",
                f"DATABASE_URL={database}",
                "DELETION_DATABASE_URL=postgresql://radar_deletion_worker:other-long-secret@db.acme.test/ai_hot?sslmode=verify-full",
                f"RADAR_JWT_PUBLIC_KEYS={jwt_keys}",
                "RADAR_JWT_ISSUER=https://id.acme.test",
                "RADAR_JWT_AUDIENCE=signal-ai-radar",
                "RADAR_INSTANCE_ID=prod-api-01",
                "RADAR_SYSTEM_WORKSPACE_ID=system-governance",
                "PRODUCT_METRIC_POLICY_FILE=/app/config/product_metric_policy.json",
                "SCORE_LEDGER_ED25519_PRIVATE_KEY=ZmFrZS1ub24tcGxhY2Vob2xkZXIta2V5LW1hdGVyaWFs",
                "SCORE_LEDGER_ED25519_KEY_ID=score-ledger-202607",
                "EXTERNAL_DATA_BUDGET_RMB=2000",
                'CONNECTOR_BUDGETS_RMB_JSON={"github":300,"huggingface":200,"openalex":300}',
                'SIGNAL_FAMILY_BUDGETS_RMB_JSON={"behavior":500,"research":300}',
                f"RADAR_RELEASE_IMAGE_DIGESTS={release_digest_json}",
            )
        )
        + "\n",
    )
    write_private(
        tmp_path / "scheduler.env",
        "\n".join(
            (
                "DEMO_MODE=false",
                "AUTH_REQUIRED=true",
                "RADAR_AUTH_MODE=jwt",
                f"DATABASE_URL={database}",
                "REDIS_URL=rediss://redis.acme.test/0",
                "R2_ENDPOINT_URL=https://objects.acme.test",
                "R2_ACCESS_KEY_ID=scheduler-access-key",
                "R2_SECRET_ACCESS_KEY=scheduler-secret-key",
                "R2_SESSION_TOKEN=scheduler-session-token",
                "RAW_EVIDENCE_BUCKET=ai-hot-raw",
                "RADAR_INSTANCE_ID=prod-scheduler-01",
                f"RADAR_JWT_PUBLIC_KEYS={jwt_keys}",
                "RADAR_JWT_ISSUER=https://id.acme.test",
                "RADAR_JWT_AUDIENCE=signal-ai-radar",
                "SOURCE_IDENTITIES_FILE=/run/config/source_identities.json",
                "RSS_FEEDS_FILE=/run/config/feeds.json",
                "EXTERNAL_DATA_BUDGET_RMB=2000",
                'CONNECTOR_BUDGETS_RMB_JSON={"github":300,"huggingface":200,"openalex":300}',
                'SIGNAL_FAMILY_BUDGETS_RMB_JSON={"behavior":500,"research":300}',
                "CONNECTOR_COST_RMB_GITHUB=0",
                "CONNECTOR_COST_RMB_HUGGINGFACE=0",
                "CONNECTOR_COST_RMB_OPENALEX=0.01",
                "GITHUB_TOKEN=approved-github-token",
                "OPENALEX_API_KEY=approved-openalex-key",
                "OPENALEX_MAILTO=radar-ops@acme.test",
                f"RADAR_RELEASE_IMAGE_DIGESTS={release_digest_json}",
            )
        )
        + "\n",
    )
    write_private(
        tmp_path / "alert.env",
        "\n".join(
            (
                "DEMO_MODE=false",
                f"DATABASE_URL={database}",
                "REDIS_URL=rediss://redis.acme.test/0",
                "R2_ENDPOINT_URL=https://objects.acme.test",
                "R2_ACCESS_KEY_ID=alert-access-key",
                "R2_SECRET_ACCESS_KEY=alert-secret-key",
                "R2_SESSION_TOKEN=alert-session-token",
                "RAW_EVIDENCE_BUCKET=ai-hot-raw",
                "WEBHOOK_SIGNING_SECRET=webhook-signing-material",
                "RADAR_WORKSPACE_IDS=workspace-prod",
                "RADAR_INSTANCE_ID=prod-alert-01",
            )
        )
        + "\n",
    )
    (tmp_path / "source-identities.json").write_text(
        json.dumps([{"sourceId": "lab-feed", "accountId": "rss:lab", "entityId": "org:lab"}]),
        encoding="utf-8",
    )
    (tmp_path / "rss-feeds.json").write_text(
        json.dumps([{
            "sourceId": "lab-feed",
            "url": "https://feeds.acme.test/ai.xml",
            "signalFamily": "official",
            "approvalReference": "approved-test-lab-feed",
            "approvalEvidenceDigest": "sha256:" + "c" * 64,
        }]),
        encoding="utf-8",
    )
    release = tmp_path / "release.env"
    write_private(
        release,
        "\n".join(
            (
                "RADAR_API_IMAGE_REPOSITORY=ghcr.io/acme/ai-hot-radar-api",
                f"RADAR_API_IMAGE_DIGEST={DIGESTS['api']}",
                "RADAR_WEB_IMAGE_REPOSITORY=ghcr.io/acme/ai-hot-radar-web",
                f"RADAR_WEB_IMAGE_DIGEST={DIGESTS['web']}",
                "RADAR_PUBLIC_API_URL=https://api.acme.test",
                "RADAR_PUBLIC_WEB_URL=https://radar.acme.test",
                "RADAR_RELEASE_REPOSITORY=acme/ai-hot",
                "RADAR_RELEASE_COMMIT=" + "e" * 40,
                "RADAR_RELEASE_MANIFEST_DIGEST=sha256:" + "f" * 64,
                "RADAR_API_ENV_FILE=api.env",
                "RADAR_SCHEDULER_ENV_FILE=scheduler.env",
                "RADAR_ALERT_ENV_FILE=alert.env",
                "RADAR_SOURCE_IDENTITIES_FILE=source-identities.json",
                "RADAR_RSS_FEEDS_FILE=rss-feeds.json",
            )
        )
        + "\n",
    )
    return release


def preflight_args(tmp_path: Path, release: Path, phase: str) -> argparse.Namespace:
    return argparse.Namespace(
        release_env=release,
        phase=phase,
        connector_registry=tmp_path / "connector-registry.json",
        rights_policies=tmp_path / "rights-policies.json",
        acceptance_keyring=tmp_path / "acceptance-keyring.json",
        product_policy=tmp_path / "product-policy.json",
        output=None,
    )


def copy_governance_files(tmp_path: Path) -> None:
    mapping = {
        "connector-registry.json": "connector_registry.json",
        "rights-policies.json": "rights_policies.json",
        "acceptance-keyring.json": "acceptance_monitor_public_keys.json",
        "product-policy.json": "product_metric_policy.json",
    }
    for target, source in mapping.items():
        (tmp_path / target).write_bytes((ROOT / "config" / source).read_bytes())


def approve_formal_connectors(tmp_path: Path) -> None:
    registry_path = tmp_path / "connector-registry.json"
    rights_path = tmp_path / "rights-policies.json"
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    rights = json.loads(rights_path.read_text(encoding="utf-8"))
    for row in registry["connectors"]:
        if row["id"] in FORMAL_CONNECTORS:
            row["rightsStatus"] = "active"
            policy = rights["policies"][row["rightsPolicyId"]]
            policy["legalApproval"] = f"approved-test-{row['id']}"
            policy["approvalEvidenceDigest"] = "sha256:" + "d" * 64
            policy["approvedAt"] = "2026-07-17T12:00:00+08:00"
            policy["approvalReviewers"] = {
                "dataRights": "reviewer-data-rights",
                "security": "reviewer-security",
                "product": "reviewer-product",
            }
    registry_path.write_text(json.dumps(registry), encoding="utf-8")
    rights_path.write_text(json.dumps(rights), encoding="utf-8")


def test_bootstrap_preflight_accepts_rendered_bundle_and_resolves_relative_paths(tmp_path: Path) -> None:
    release = create_bundle(tmp_path)
    copy_governance_files(tmp_path)
    report = run_preflight(preflight_args(tmp_path, release, "bootstrap"))
    assert report["qualifies"] is True
    assert report["errors"] == []
    assert any("formal collection remains blocked" in warning for warning in report["warnings"])


def test_formal_preflight_requires_approvals_and_bound_populated_keyring(tmp_path: Path) -> None:
    release = create_bundle(tmp_path)
    copy_governance_files(tmp_path)
    blocked = run_preflight(preflight_args(tmp_path, release, "formal"))
    assert blocked["qualifies"] is False
    assert any("rights.rss.status" in error for error in blocked["errors"])
    assert any("acceptanceKeyring.populated" in error for error in blocked["errors"])

    approve_formal_connectors(tmp_path)
    keygen.generate(
        keyring_path=tmp_path / "acceptance-keyring.json",
        product_policy_path=tmp_path / "product-policy.json",
        private_dir=tmp_path / "private-keys",
        keyring_version="acceptance-keys-test-v2",
        product_policy_version="product-metrics-test-v2",
        frozen_at="2026-07-17T12:00:00+08:00",
    )
    ready = run_preflight(preflight_args(tmp_path, release, "formal"))
    assert ready["qualifies"] is True
    assert ready["errors"] == []


def test_key_generation_creates_separated_loadable_identities(tmp_path: Path) -> None:
    copy_governance_files(tmp_path)
    result = keygen.generate(
        keyring_path=tmp_path / "acceptance-keyring.json",
        product_policy_path=tmp_path / "product-policy.json",
        private_dir=tmp_path / "private-keys",
        keyring_version="acceptance-keys-test-v3",
        product_policy_version="product-metrics-test-v3",
        frozen_at="2026-07-17T12:00:00+08:00",
    )
    scheduler, reviewers, baseline, ledger, identity = load_keyring(tmp_path / "acceptance-keyring.json")
    assert (len(scheduler), len(reviewers), len(baseline), len(ledger)) == (1, 2, 1, 1)
    assert len(set(scheduler) | set(reviewers) | set(baseline) | set(ledger)) == 5
    assert identity["keyringDigest"] == result["keyringDigest"]
    assert len(list((tmp_path / "private-keys").glob("*.private-key-base64"))) == 5


def test_public_pair_replacement_rolls_back_first_file_on_second_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    keyring = tmp_path / "keyring.json"
    policy = tmp_path / "policy.json"
    keyring.write_bytes(b"old-keyring")
    policy.write_bytes(b"old-policy")
    real_atomic_write = keygen.atomic_write
    failed = False

    def fail_policy_once(path: Path, body: bytes, mode: int = 0o600) -> None:
        nonlocal failed
        if path == policy and not failed:
            failed = True
            raise OSError("simulated policy replacement failure")
        real_atomic_write(path, body, mode)

    monkeypatch.setattr(keygen, "atomic_write", fail_policy_once)
    with pytest.raises(OSError, match="simulated"):
        keygen.replace_public_pair(keyring, b"new-keyring", policy, b"new-policy")
    assert keyring.read_bytes() == b"old-keyring"
    assert policy.read_bytes() == b"old-policy"


def test_restore_report_digest_is_verified_before_attestation(tmp_path: Path) -> None:
    payload: dict[str, object] = {
        "schemaVersion": "restored-database-verification-v1",
        "qualifies": True,
        "gates": {"migration": True},
    }
    payload["verificationDigest"] = report_digest(payload)
    report = tmp_path / "restore.json"
    report.write_text(json.dumps(payload), encoding="utf-8")
    assert validate_report(report)["qualifies"] is True

    payload["qualifies"] = False
    report.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="digest mismatch"):
        validate_report(report)


def test_restore_attestation_cannot_rebind_report_to_another_instance(tmp_path: Path) -> None:
    payload: dict[str, object] = {
        "schemaVersion": "restored-database-verification-v1",
        "qualifies": True,
        "restoredInstanceId": "restore-a",
        "backupReference": "backup-a",
    }
    payload["verificationDigest"] = report_digest(payload)
    report = tmp_path / "restore.json"
    report.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="different instance"):
        record_dr_attestation(
            dsn="postgresql://unused",
            report_path=report,
            performed_at="2026-07-17T12:00:00+08:00",
            backup_reference="backup-a",
            restored_instance_id="restore-b",
            measured_rpo_seconds=60,
            measured_rto_seconds=120,
            operator_subject="dba-subject",
            notes="",
            status="passed",
        )


def test_acceptance_schedule_is_utc_quarter_hour_and_idempotent(tmp_path: Path) -> None:
    slot = scheduled_slot(datetime(2026, 7, 17, 12, 29, 59, tzinfo=timezone.utc))
    assert slot.isoformat() == "2026-07-17T12:15:00+00:00"
    output = tmp_path / "samples.jsonl"
    output.write_text(json.dumps({"scheduledAt": slot.isoformat()}) + "\n", encoding="utf-8")
    assert already_collected(output, slot) is True
    assert already_collected(output, slot.replace(minute=30)) is False
    with pytest.raises(ValueError, match="HTTPS"):
        fetch_client_credentials_token(
            endpoint="http://identity.acme.test/token",
            client_id="monitor",
            client_secret="secret",
            scope="",
            audience=None,
            auth_method="client_secret_basic",
            timeout=1,
        )
    with pytest.raises(ValueError, match="HTTPS"):
        fetch_client_credentials_token(
            endpoint="https://identity.acme.test/token?secret=forbidden",
            client_id="monitor",
            client_secret="secret",
            scope="",
            audience=None,
            auth_method="client_secret_basic",
            timeout=1,
        )


def test_capacity_probe_rejects_non_https_target_before_sending_owner_token() -> None:
    with pytest.raises(ValueError, match="credential-free HTTPS"):
        run_capacity_probe(
            dsn="postgresql://unused",
            base_url="http://radar.acme.test",
            token="owner-token",
            requests=20,
            timeout=1,
            minimum_sources=10_000,
            minimum_observations=5_000_000,
            minimum_events=2_000,
            maximum_cycle_seconds=300,
            maximum_p95_ms=500,
        )


def test_acceptance_parser_treats_blank_optional_artifacts_as_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    required = {
        "RADAR_API_URL": "https://api.acme.test",
        "ACCEPTANCE_OUTPUT": str(tmp_path / "samples.jsonl"),
        "OIDC_TOKEN_ENDPOINT": "https://id.acme.test/token",
        "OIDC_CLIENT_ID": "monitor-client",
        "OIDC_CLIENT_SECRET_FILE": str(tmp_path / "client-secret"),
        "ACCEPTANCE_SIGNING_KEY_ID": "scheduler-test-key",
        "ACCEPTANCE_SIGNING_PRIVATE_KEY_FILE": str(tmp_path / "signing-key"),
        "ACCEPTANCE_MANUAL_PREREGISTRATION": "",
        "ACCEPTANCE_MANUAL_SNAPSHOT": "",
    }
    for key, value in required.items():
        monkeypatch.setenv(key, value)
    args = acceptance_parser().parse_args([])
    assert args.manual_preregistration is None
    assert args.manual_snapshot is None


class StubCursor:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def __enter__(self) -> StubCursor:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def execute(self, _query: str) -> None:
        return None

    def fetchone(self) -> tuple[object, ...]:
        return self.row


class StubConnection:
    def __init__(self, row: tuple[object, ...]) -> None:
        self.row = row

    def cursor(self) -> StubCursor:
        return StubCursor(self.row)


def test_database_provisioning_accepts_managed_owner_without_true_superuser() -> None:
    ensure_database(StubConnection(("ai_hot", "provider_admin", False, True, True)))  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="CREATEROLE owner"):
        ensure_database(StubConnection(("ai_hot", "plain_user", False, True, False)))  # type: ignore[arg-type]


def test_production_image_preserves_policy_loader_layout() -> None:
    dockerfile = (ROOT / "services" / "api" / "Dockerfile").read_text(encoding="utf-8")
    assert "PYTHONPATH=/app/services/api" in dockerfile
    assert "COPY services/api/radar /app/services/api/radar" in dockerfile
    assert "COPY config /app/config" in dockerfile


def test_signed_release_manifest_is_validated_and_rendered(tmp_path: Path) -> None:
    commit = "e" * 40
    payload = {
        "schemaVersion": "ai-hot-release-manifest-v1",
        "commit": commit,
        "repository": "acme/ai-hot",
        "api": {"repository": "ghcr.io/acme/ai-hot-radar-api", "digest": DIGESTS["api"]},
        "web": {"repository": "ghcr.io/acme/ai-hot-radar-web", "digest": DIGESTS["web"]},
        "baseImages": {
            "python": "python:3.12-slim@sha256:" + "1" * 64,
            "node": "node:22-slim@sha256:" + "2" * 64,
        },
    }
    manifest_path = tmp_path / "release-manifest.json"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    checksum_path = tmp_path / "release-manifest.json.sha256"
    checksum_path.write_text(
        f"{file_sha256(manifest_path)}  release-manifest.json\n",
        encoding="utf-8",
    )
    validate_checksum(manifest_path, checksum_path)
    manifest = validate_manifest(
        manifest_path,
        expected_repository="acme/ai-hot",
        expected_commit=commit,
    )
    rendered = render_release_env(
        manifest,
        public_api_url="https://api.acme.test",
        public_web_url="https://radar.acme.test",
        config_dir=Path("/etc/ai-hot"),
        manifest_digest="sha256:" + file_sha256(manifest_path),
    )
    assert f"RADAR_API_IMAGE_DIGEST={DIGESTS['api']}" in rendered
    assert "RADAR_API_ENV_FILE=/etc/ai-hot/api.env" in rendered
    assert "RADAR_PUBLIC_WEB_URL=https://radar.acme.test" in rendered

    commands: list[list[str]] = []

    def runner(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    verify_supply_chain(
        manifest_path,
        tmp_path / "release-manifest.sigstore.json",
        manifest,
        cosign="cosign",
        verify_github_attestations=True,
        gh="gh",
        runner=runner,
    )
    assert [command[:2] for command in commands] == [
        ["cosign", "verify-blob"],
        ["cosign", "verify"],
        ["gh", "attestation"],
        ["cosign", "verify"],
        ["gh", "attestation"],
    ]

    checksum_path.write_text("0" * 64 + "  release-manifest.json\n", encoding="utf-8")
    with pytest.raises(ValueError, match="checksum mismatch"):
        validate_checksum(manifest_path, checksum_path)


def test_production_jwt_readiness_rejects_weak_rsa_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    weak_key = rsa.generate_private_key(public_exponent=65537, key_size=1024).public_key()
    pem = weak_key.public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    monkeypatch.setenv("RADAR_JWT_ISSUER", "https://id.acme.test")
    monkeypatch.setenv("RADAR_JWT_AUDIENCE", "signal-ai-radar")
    monkeypatch.setenv("RADAR_JWT_PUBLIC_KEYS", json.dumps({"weak-key": pem}))
    assert jwt_configuration_ready() is False
