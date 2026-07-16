from __future__ import annotations

import argparse
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import random
from statistics import median
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo


MODES = {
    "canary": {"minimumSpanHours": 0, "requiresProductEvidence": False},
    "soak72h": {"minimumSpanHours": 72, "requiresProductEvidence": False},
    "shadow7d": {"minimumSpanHours": 168, "requiresProductEvidence": True},
}
DEFAULT_CONNECTORS = ["rss", "hackernews", "github", "huggingface", "arxiv", "openalex"]
REQUIRED_RESPONSE_KEYS = {"health", "coverage", "connectorRuns", "pipelineSla", "dataQuality", "betaMetrics", "rankingLedger"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def signature_material(payload: dict[str, Any], excluded: set[str]) -> bytes:
    canonical = {name: value for name, value in payload.items() if name not in excluded}
    return json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def seal_sample(sample: dict[str, Any], private_key: bytes, key_id: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if len(private_key) != 32 or len(key_id) < 8:
        raise ValueError("Ed25519 collection requires a 32-byte private key and stable key ID")
    sample["collectorKeyId"] = key_id
    signature = Ed25519PrivateKey.from_private_bytes(private_key).sign(
        signature_material(sample, {"sampleHash", "collectorSignature"}),
    )
    sample["collectorSignature"] = base64.b64encode(signature).decode()


def verify_signature(payload: dict[str, Any], signature_value: object, public_key: bytes, excluded: set[str]) -> bool:
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        signature = base64.b64decode(str(signature_value), validate=True)
        Ed25519PublicKey.from_public_bytes(public_key).verify(signature, signature_material(payload, excluded))
    except (InvalidSignature, TypeError, ValueError):
        return False
    return True


def load_keyring(path: Path) -> tuple[dict[str, bytes], dict[str, bytes], dict[str, bytes], dict[str, bytes], dict[str, str]]:
    raw = path.read_bytes()
    payload = json.loads(raw)
    if payload.get("schemaVersion") != "acceptance-ed25519-keyring-v1":
        raise ValueError("unsupported acceptance keyring")

    def decode(rows: object) -> dict[str, bytes]:
        if not isinstance(rows, list):
            raise ValueError("keyring entries must be arrays")
        result: dict[str, bytes] = {}
        seen_ids: set[str] = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"keyId", "publicKeyBase64", "status"}:
                raise ValueError("invalid keyring entry")
            key_id = str(row.get("keyId"))
            if key_id in seen_ids:
                raise ValueError("duplicate key ID in acceptance keyring")
            seen_ids.add(key_id)
            if row.get("status") != "active":
                continue
            key = base64.b64decode(str(row.get("publicKeyBase64")), validate=True)
            if len(key_id) < 8 or len(key) != 32:
                raise ValueError("invalid Ed25519 public key entry")
            result[key_id] = key
        return result

    scheduler_keys = decode(payload.get("schedulerKeys"))
    reviewer_keys = decode(payload.get("reviewerKeys"))
    baseline_keys = decode(payload.get("baselineKeys"))
    ledger_keys = decode(payload.get("ledgerKeys"))
    if len(set(scheduler_keys.values())) != len(scheduler_keys):
        raise ValueError("scheduler key material must be unique")
    if len(set(reviewer_keys.values())) != len(reviewer_keys):
        raise ValueError("reviewer identities must use distinct public keys")
    if len(set(baseline_keys.values())) != len(baseline_keys):
        raise ValueError("baseline collector key material must be unique")
    if len(set(ledger_keys.values())) != len(ledger_keys):
        raise ValueError("score ledger key material must be unique")
    role_material = [set(value.values()) for value in (scheduler_keys, reviewer_keys, baseline_keys, ledger_keys)]
    if any(role_material[left] & role_material[right] for left in range(4) for right in range(left + 1, 4)):
        raise ValueError("scheduler, reviewer, baseline, and score-ledger key material must be separated")
    identity = {
        "keyringVersion": str(payload.get("keyringVersion") or ""),
        "keyringDigest": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "keyringFrozenAt": str(payload.get("frozenAt") or ""),
    }
    if len(identity["keyringVersion"]) < 8:
        raise ValueError("keyring version is missing")
    parse_time(identity["keyringFrozenAt"])
    return scheduler_keys, reviewer_keys, baseline_keys, ledger_keys, identity


def artifact_digest(payload: dict[str, Any]) -> str:
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(material).hexdigest()


def seal_preregistration(payload: dict[str, Any], private_key: bytes, key_id: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if len(private_key) != 32 or len(key_id) < 8:
        raise ValueError("Ed25519 preregistration requires a 32-byte private key and stable key ID")
    payload["schedulerKeyId"] = key_id
    payload["schedulerSignature"] = base64.b64encode(
        Ed25519PrivateKey.from_private_bytes(private_key).sign(
            signature_material(payload, {"schedulerSignature"}),
        )
    ).decode()


def snapshot_commitment(snapshot: dict[str, Any]) -> dict[str, Any]:
    candidates = snapshot.get("candidates") if isinstance(snapshot.get("candidates"), list) else []
    return {
        "schemaVersion": "manual-daily-snapshot-commitment-v1",
        "snapshotId": snapshot.get("snapshotId"),
        "scheduledAt": snapshot.get("scheduledAt"),
        "preregistrationDigest": snapshot.get("preregistrationDigest"),
        "rankingRuleVersion": snapshot.get("rankingRuleVersion"),
        "rankingResponseDigest": snapshot.get("rankingResponseDigest"),
        "candidates": [
            {
                "rank": candidate.get("rank"), "eventId": candidate.get("eventId"),
                "score": candidate.get("score"), "scoreRunId": candidate.get("scoreRunId"),
            }
            for candidate in candidates if isinstance(candidate, dict)
        ],
        "schedulerKeyId": snapshot.get("schedulerKeyId"),
    }


def seal_snapshot(snapshot: dict[str, Any], private_key: bytes, key_id: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if len(private_key) != 32 or len(key_id) < 8:
        raise ValueError("Ed25519 snapshot commitment requires a 32-byte private key and stable key ID")
    snapshot["schedulerKeyId"] = key_id
    snapshot["schedulerSignature"] = base64.b64encode(
        Ed25519PrivateKey.from_private_bytes(private_key).sign(
            signature_material(snapshot_commitment(snapshot), set()),
        )
    ).decode()


def snapshot_artifact_digest(snapshot: dict[str, Any]) -> str:
    return artifact_digest({
        **snapshot_commitment(snapshot),
        "schedulerSignature": snapshot.get("schedulerSignature"),
    })


def lead_manifest_commitment(manifest: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": manifest.get("schemaVersion"),
        "preregistrationDigest": manifest.get("preregistrationDigest"),
        "generatedAt": manifest.get("generatedAt"),
        "candidates": manifest.get("candidates"),
        "schedulerKeyId": manifest.get("schedulerKeyId"),
    }


def seal_lead_manifest(manifest: dict[str, Any], private_key: bytes, key_id: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if len(private_key) != 32 or len(key_id) < 8:
        raise ValueError("Ed25519 lead manifest requires a 32-byte private key and stable key ID")
    manifest["schedulerKeyId"] = key_id
    manifest["schedulerSignature"] = base64.b64encode(
        Ed25519PrivateKey.from_private_bytes(private_key).sign(
            signature_material(lead_manifest_commitment(manifest), set()),
        )
    ).decode()


def baseline_artifact_commitment(artifact: dict[str, Any]) -> dict[str, Any]:
    return {
        "schemaVersion": artifact.get("schemaVersion"),
        "generatedAt": artifact.get("generatedAt"),
        "eventLogs": artifact.get("eventLogs"),
        "collectorKeyId": artifact.get("collectorKeyId"),
    }


def seal_baseline_artifact(artifact: dict[str, Any], private_key: bytes, key_id: str) -> None:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    if len(private_key) != 32 or len(key_id) < 8:
        raise ValueError("Ed25519 baseline artifact requires a 32-byte private key and stable key ID")
    artifact["collectorKeyId"] = key_id
    artifact["collectorSignature"] = base64.b64encode(
        Ed25519PrivateKey.from_private_bytes(private_key).sign(
            signature_material(baseline_artifact_commitment(artifact), set()),
        )
    ).decode()


def validated_api_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
        raise ValueError("API URL must be a credential-free absolute HTTP(S) URL")
    host = parts.hostname.lower()
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    loopback = host == "localhost" or (address is not None and address.is_loopback)
    if parts.scheme != "https" and not loopback:
        raise ValueError("non-loopback monitoring endpoints require HTTPS")
    return value.rstrip("/")


def fetch_json(api_url: str, path: str, api_key: str | None, timeout: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    request = Request(f"{api_url.rstrip('/')}{path}", headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310 - operator-supplied internal endpoint
            payload = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path}: expected a JSON object")
    return payload


def collect_sample(
    api_url: str,
    api_key: str | None = None,
    *,
    timeout: float = 10,
    evidence_window_hours: int = 72,
    manual_preregistration: dict[str, Any] | None = None,
    manual_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    api_url = validated_api_url(api_url)
    if not 24 <= evidence_window_hours <= 2160:
        raise ValueError("evidence window must be between 24 and 2160 hours")
    endpoints = {
        "health": "/health",
        "coverage": "/api/v1/coverage",
        "connectorRuns": "/api/v1/operations/connector-runs?hours=2",
        "pipelineSla": f"/api/v1/operations/pipeline-sla?hours={evidence_window_hours}",
        "dataQuality": f"/api/v1/operations/data-quality?hours={evidence_window_hours}",
        "betaMetrics": f"/api/v1/metrics/beta?hours={evidence_window_hours}",
        "rankingLedger": "/api/v1/metrics/ranking-ledger",
    }
    sample: dict[str, Any] = {
        "schemaVersion": "acceptance-monitor-sample-v1",
        "observedAt": utcnow().isoformat(),
        "apiUrl": api_url,
        "evidenceWindowHours": evidence_window_hours,
        "responses": {},
        "errors": {},
    }
    if manual_preregistration is not None:
        sample["manualPreregistrationDigest"] = artifact_digest(manual_preregistration)
    if manual_snapshot is not None:
        sample["manualSnapshotCommitmentDigest"] = snapshot_artifact_digest(manual_snapshot)
    signing_key = os.getenv("ACCEPTANCE_MONITOR_ED25519_PRIVATE_KEY")
    if signing_key:
        scheduled_at = os.getenv("ACCEPTANCE_MONITOR_SCHEDULED_AT")
        run_id = os.getenv("ACCEPTANCE_MONITOR_RUN_ID")
        if not scheduled_at or not run_id:
            raise ValueError("signed collection requires scheduler-provided SCHEDULED_AT and RUN_ID")
        scheduled = parse_time(scheduled_at)
        if abs((parse_time(sample["observedAt"]) - scheduled).total_seconds()) > 300:
            raise ValueError("scheduler timestamp must be within five minutes of collection")
        sample["scheduledAt"] = scheduled.isoformat()
        sample["collectorRunId"] = run_id
    for name, path in endpoints.items():
        try:
            sample["responses"][name] = fetch_json(api_url, path, api_key, timeout)
        except RuntimeError as exc:
            sample["errors"][name] = str(exc)
    sample["sampleHealthy"] = not sample["errors"]
    return sample


def append_sample(path: Path, sample: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    previous_hash: str | None = None
    if path.exists():
        existing_lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if existing_lines:
            previous = json.loads(existing_lines[-1])
            previous_hash = previous.get("sampleHash")
            if not isinstance(previous_hash, str):
                raise ValueError("existing evidence file does not end in a hashed sample")
            if parse_time(str(sample.get("observedAt"))) <= parse_time(str(previous.get("observedAt"))):
                raise ValueError("new evidence samples must have a strictly increasing observedAt")
    sample["previousHash"] = previous_hash
    signing_key = os.getenv("ACCEPTANCE_MONITOR_ED25519_PRIVATE_KEY")
    if signing_key:
        key_id = os.getenv("ACCEPTANCE_MONITOR_ED25519_KEY_ID", "")
        seal_sample(sample, base64.b64decode(signing_key, validate=True), key_id)
    material = json.dumps(sample, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    sample["sampleHash"] = f"sha256:{hashlib.sha256(material).hexdigest()}"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(sample, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def load_samples(path: Path) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    previous_hash: str | None = None
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict) or not isinstance(payload.get("observedAt"), str):
            raise ValueError(f"invalid sample at line {line_number}")
        if payload.get("previousHash") != previous_hash:
            raise ValueError(f"broken evidence hash chain at line {line_number}")
        claimed_hash = payload.pop("sampleHash", None)
        material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        actual_hash = f"sha256:{hashlib.sha256(material).hexdigest()}"
        if claimed_hash != actual_hash:
            raise ValueError(f"sample hash mismatch at line {line_number}")
        payload["sampleHash"] = claimed_hash
        previous_hash = claimed_hash
        samples.append(payload)
    return samples


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    fraction = position - lower
    return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def _sample_is_healthy(sample: dict[str, Any]) -> bool:
    responses = sample.get("responses")
    errors = sample.get("errors")
    if not isinstance(responses, dict) or not isinstance(errors, dict) or errors:
        return False
    health = responses.get("health")
    return (
        REQUIRED_RESPONSE_KEYS.issubset(responses)
        and isinstance(health, dict)
        and health.get("status") == "ok"
    )


def _validated_ranking_ledger(
    artifact: object,
    observed_at: datetime,
    ledger_public_keys: dict[str, bytes],
    expected_policy_version: str | None = None,
    expected_threshold_version: str | None = None,
) -> list[dict[str, Any]] | None:
    if not isinstance(artifact, dict) or set(artifact) != {
        "schemaVersion", "productMetricPolicyVersion", "thresholdVersion", "selectionRuleVersion",
        "generatedAt", "rows", "leadCrossings", "ledgerKeyId", "ledgerSignature", "ledgerDigest",
    }:
        return None
    try:
        generated_at = parse_time(str(artifact["generatedAt"]))
        rows = artifact["rows"]
        lead_crossings = artifact["leadCrossings"]
        key_id = artifact["ledgerKeyId"]
        material = signature_material(artifact, {"ledgerSignature", "ledgerDigest"})
        expected_digest = "sha256:" + hashlib.sha256(material).hexdigest()
        if (
            artifact.get("schemaVersion") != "signed-score-ledger-v1"
            or abs((generated_at - observed_at).total_seconds()) > 300
            or not isinstance(key_id, str) or key_id not in ledger_public_keys
            or artifact.get("ledgerDigest") != expected_digest
            or not verify_signature(artifact, artifact.get("ledgerSignature"), ledger_public_keys[key_id], {"ledgerSignature", "ledgerDigest"})
            or (expected_policy_version is not None and artifact.get("productMetricPolicyVersion") != expected_policy_version)
            or (expected_threshold_version is not None and artifact.get("thresholdVersion") != expected_threshold_version)
            or artifact.get("selectionRuleVersion") != "daily-top5-score-v2"
            or not isinstance(rows, list) or not isinstance(lead_crossings, list)
        ):
            return None
        event_ids: set[str] = set()
        previous_score: float | None = None
        for rank, row in enumerate(rows, 1):
            if not isinstance(row, dict) or set(row) != {
                "eventId", "score", "scoreRunId", "scoreRunAt", "firstDetectedAt", "lifecycleState",
                "coverage", "eligibleTop5", "rank",
            }:
                return None
            event_id = row.get("eventId")
            score = row.get("score")
            if (
                row.get("rank") != rank or not isinstance(event_id, str) or len(event_id) < 3 or event_id in event_ids
                or not _is_number(score) or (previous_score is not None and float(score) > previous_score)
                or not isinstance(row.get("eligibleTop5"), bool)
                or not _is_number(row.get("coverage"))
            ):
                return None
            if row["eligibleTop5"]:
                if (
                    not isinstance(row.get("scoreRunId"), str) or len(str(row["scoreRunId"])) < 8
                    or parse_time(str(row.get("scoreRunAt"))) > generated_at
                    or parse_time(str(row.get("firstDetectedAt"))) > generated_at
                ):
                    return None
            event_ids.add(event_id)
            previous_score = float(score)
        crossing_events: set[str] = set()
        for crossing in lead_crossings:
            if not isinstance(crossing, dict) or set(crossing) != {
                "eventId", "crossedAt", "scoreRunId", "thresholdVersion", "policyVersion",
            }:
                return None
            event_id = crossing.get("eventId")
            if (
                not isinstance(event_id, str) or len(event_id) < 3 or event_id in crossing_events
                or not isinstance(crossing.get("scoreRunId"), str) or len(str(crossing["scoreRunId"])) < 8
                or crossing.get("thresholdVersion") != artifact.get("thresholdVersion")
                or crossing.get("policyVersion") != artifact.get("productMetricPolicyVersion")
                or parse_time(str(crossing.get("crossedAt"))) > generated_at
            ):
                return None
            crossing_events.add(event_id)
        return rows
    except (KeyError, TypeError, ValueError):
        return None


def _manual_evaluation(
    payload: dict[str, Any] | None,
    expected_policy_version: str | None,
    expected_policy_digest: str | None,
    manual_policy: dict[str, Any],
    scheduler_public_keys: dict[str, bytes],
    reviewer_public_keys: dict[str, bytes],
    baseline_public_keys: dict[str, bytes],
    ledger_public_keys: dict[str, bytes],
    *,
    expected_preregistration_digest: str | None,
    snapshot_commitment_times: dict[str, datetime],
    evidence_started_at: datetime,
    evidence_ended_at: datetime,
    as_of: datetime,
    ranking_ledgers: list[tuple[datetime, dict[str, Any], list[dict[str, Any]]]],
) -> tuple[bool, dict[str, Any]]:
    if not payload:
        return False, {"status": "missing", "reason": "shadow7d requires an independently reviewed manual evaluation file"}
    top_level_keys = {
        "schemaVersion", "productMetricPolicyVersion", "productMetricPolicyDigest",
        "precisionCandidateEventSetDigest", "precisionCandidateEventIds", "eligibleLeadEventIds", "eligibleLeadEventSetDigest",
        "snapshotScheduleId", "preregisteredAt", "evaluatedAt",
        "reviewerIds", "reviewerSignatures", "baselineArtifact", "baselineArtifactDigest", "precisionAt5", "discoveryLead",
        "preregistrationArtifact", "preregistrationDigest",
        "leadCandidateManifest", "leadCandidateManifestDigest",
    }
    preregistration_keys = {
        "schemaVersion", "productMetricPolicyVersion", "productMetricPolicyDigest", "selectionRuleVersion",
        "inclusionRules", "exclusionRules", "snapshotScheduleId", "snapshotTimezone", "snapshotLocalTime",
        "leadEligibilityRuleVersion", "thresholdVersion", "baselineDefinitionId",
        "bootstrapSeed", "bootstrapIterations", "bootstrapUnit", "preregisteredAt",
        "schedulerKeyId", "schedulerSignature",
    }
    precision_keys = {
        "doubleReviewed", "snapshots", "candidates", "value", "ciLow", "ciHigh", "intervalMethod",
        "confidenceLevel", "bootstrapIterations", "bootstrapSeed", "rawSnapshots",
    }
    lead_keys = {"independentBaseline", "sample", "medianMinutes", "rawEvents"}
    precision_value = payload.get("precisionAt5")
    lead_value = payload.get("discoveryLead")
    precision = precision_value if isinstance(precision_value, dict) else {}
    lead = lead_value if isinstance(lead_value, dict) else {}
    reviewers = payload.get("reviewerIds") or []
    reviewer_signatures = payload.get("reviewerSignatures")
    precision_candidate_event_ids = payload.get("precisionCandidateEventIds")
    eligible_lead_event_ids = payload.get("eligibleLeadEventIds")
    preregistration_value = payload.get("preregistrationArtifact")
    preregistration = preregistration_value if isinstance(preregistration_value, dict) else {}
    lead_manifest_value = payload.get("leadCandidateManifest")
    lead_manifest = lead_manifest_value if isinstance(lead_manifest_value, dict) else {}
    baseline_artifact_value = payload.get("baselineArtifact")
    baseline_artifact = baseline_artifact_value if isinstance(baseline_artifact_value, dict) else {}
    try:
        preregistered_at = parse_time(str(payload["preregisteredAt"]))
        evaluated_at = parse_time(str(payload["evaluatedAt"]))
        lead_manifest_generated_at = parse_time(str(lead_manifest.get("generatedAt")))
        lead_manifest_candidates = lead_manifest.get("candidates")
        lead_manifest_candidate_map = {
            str(row.get("eventId")): row
            for row in lead_manifest_candidates
            if isinstance(row, dict)
        } if isinstance(lead_manifest_candidates, list) else {}
        lead_manifest_ok = (
            set(lead_manifest) == {
                "schemaVersion", "preregistrationDigest", "generatedAt", "candidates",
                "schedulerKeyId", "schedulerSignature",
            }
            and lead_manifest.get("schemaVersion") == "manual-lead-candidate-manifest-v1"
            and lead_manifest.get("preregistrationDigest") == payload.get("preregistrationDigest")
            and payload.get("leadCandidateManifestDigest") == artifact_digest(lead_manifest)
            and isinstance(lead_manifest_candidates, list)
            and len(lead_manifest_candidates) >= int(manual_policy["minimumDiscoveryLeadEvents"])
            and len(lead_manifest_candidate_map) == len(lead_manifest_candidates)
            and set(lead_manifest_candidate_map) == set(eligible_lead_event_ids)
            and evidence_ended_at <= lead_manifest_generated_at <= evaluated_at
            and isinstance(lead_manifest.get("schedulerKeyId"), str)
            and lead_manifest["schedulerKeyId"] in scheduler_public_keys
            and verify_signature(
                lead_manifest_commitment(lead_manifest), lead_manifest.get("schedulerSignature"),
                scheduler_public_keys[str(lead_manifest["schedulerKeyId"])], set(),
            )
            and all(
                set(row) == {"eventId", "radarDetectedAt", "scoreRunId", "thresholdVersion", "eligibilityReason"}
                and isinstance(row.get("eligibilityReason"), str) and len(str(row["eligibilityReason"])) >= 8
                and isinstance(row.get("scoreRunId"), str) and len(str(row["scoreRunId"])) >= 8
                and row.get("thresholdVersion") == manual_policy.get("requiredThresholdVersion")
                and evidence_started_at <= parse_time(str(row.get("radarDetectedAt"))) <= evidence_ended_at
                for row in lead_manifest_candidates
            )
        )
        baseline_generated_at = parse_time(str(baseline_artifact.get("generatedAt")))
        baseline_logs = baseline_artifact.get("eventLogs")
        baseline_log_map = {
            str(row.get("eventId")): row
            for row in baseline_logs
            if isinstance(row, dict)
        } if isinstance(baseline_logs, list) else {}
        baseline_ok = (
            set(baseline_artifact) == {
                "schemaVersion", "generatedAt", "eventLogs", "collectorKeyId", "collectorSignature",
            }
            and baseline_artifact.get("schemaVersion") == "manual-independent-baseline-v1"
            and payload.get("baselineArtifactDigest") == artifact_digest(baseline_artifact)
            and isinstance(baseline_logs, list)
            and len(baseline_logs) >= int(manual_policy["minimumDiscoveryLeadEvents"])
            and len(baseline_log_map) == len(baseline_logs)
            and set(baseline_log_map) == set(eligible_lead_event_ids)
            and evidence_ended_at <= baseline_generated_at <= evaluated_at
            and isinstance(baseline_artifact.get("collectorKeyId"), str)
            and baseline_artifact["collectorKeyId"] in baseline_public_keys
            and verify_signature(
                baseline_artifact_commitment(baseline_artifact), baseline_artifact.get("collectorSignature"),
                baseline_public_keys[str(baseline_artifact["collectorKeyId"])], set(),
            )
            and all(
                set(row) == {"logRef", "eventId", "firstDetectedAt"}
                and isinstance(row.get("logRef"), str) and len(str(row["logRef"])) >= 8
                and parse_time(str(row.get("firstDetectedAt"))) <= baseline_generated_at
                for row in baseline_logs
            )
        )
        identity_ok = (
            set(payload) == top_level_keys
            and payload.get("schemaVersion") == manual_policy["manualSchemaVersion"]
            and isinstance(expected_policy_version, str)
            and payload.get("productMetricPolicyVersion") == expected_policy_version
            and isinstance(expected_policy_digest, str)
            and payload.get("productMetricPolicyDigest") == expected_policy_digest
            and isinstance(precision_candidate_event_ids, list) and len(precision_candidate_event_ids) >= 1
            and all(isinstance(value, str) and len(value) >= 3 for value in precision_candidate_event_ids)
            and len(set(precision_candidate_event_ids)) == len(precision_candidate_event_ids)
            and payload.get("precisionCandidateEventSetDigest") == "sha256:" + hashlib.sha256(
                json.dumps(sorted(precision_candidate_event_ids), ensure_ascii=False, separators=(",", ":")).encode()
            ).hexdigest()
            and isinstance(eligible_lead_event_ids, list) and len(eligible_lead_event_ids) >= 30
            and all(isinstance(value, str) and len(value) >= 3 for value in eligible_lead_event_ids)
            and len(set(eligible_lead_event_ids)) == len(eligible_lead_event_ids)
            and payload.get("eligibleLeadEventSetDigest") == "sha256:" + hashlib.sha256(
                json.dumps(sorted(eligible_lead_event_ids), ensure_ascii=False, separators=(",", ":")).encode()
            ).hexdigest()
            and isinstance(payload.get("snapshotScheduleId"), str) and len(payload["snapshotScheduleId"]) >= 8
            and set(preregistration) == preregistration_keys
            and preregistration.get("schemaVersion") == manual_policy.get("preregistrationSchemaVersion")
            and manual_policy.get("preregistrationSchemaDigest") == "sha256:" + hashlib.sha256(
                (Path(__file__).resolve().parents[1] / "config" / "manual_product_preregistration.schema.json").read_bytes()
            ).hexdigest()
            and payload.get("preregistrationDigest") == artifact_digest(preregistration)
            and payload.get("preregistrationDigest") == expected_preregistration_digest
            and lead_manifest_ok
            and baseline_ok
            and all(
                preregistration.get(key) == payload.get(key)
                for key in (
                    "productMetricPolicyVersion", "productMetricPolicyDigest", "snapshotScheduleId",
                    "preregisteredAt",
                )
            )
            and isinstance(preregistration.get("selectionRuleVersion"), str)
            and isinstance(preregistration.get("inclusionRules"), list) and preregistration["inclusionRules"]
            and isinstance(preregistration.get("exclusionRules"), list)
            and preregistration.get("snapshotTimezone") == manual_policy.get("snapshotTimezone")
            and preregistration.get("snapshotLocalTime") in {
                manual_policy.get("snapshotLocalTime"), str(manual_policy.get("snapshotLocalTime"))[:5]
            }
            and preregistration.get("leadEligibilityRuleVersion") == manual_policy.get("leadEligibilityRuleVersion")
            and preregistration.get("thresholdVersion") == manual_policy.get("requiredThresholdVersion")
            and isinstance(preregistration.get("baselineDefinitionId"), str)
            and preregistration.get("bootstrapUnit") == "daily_snapshot"
            and preregistration.get("bootstrapSeed") == precision.get("bootstrapSeed")
            and preregistration.get("bootstrapIterations") == precision.get("bootstrapIterations")
            and isinstance(preregistration.get("schedulerKeyId"), str)
            and preregistration["schedulerKeyId"] in scheduler_public_keys
            and verify_signature(
                preregistration,
                preregistration.get("schedulerSignature"),
                scheduler_public_keys[str(preregistration["schedulerKeyId"])],
                {"schedulerSignature"},
            )
            and isinstance(reviewers, list)
            and all(isinstance(value, str) and len(value) >= 3 for value in reviewers)
            and len(reviewers) >= 2 and len(set(reviewers)) == len(reviewers)
            and isinstance(reviewer_signatures, dict) and set(reviewer_signatures) == set(reviewers)
            and all(reviewer in reviewer_public_keys for reviewer in reviewers)
            and all(
                verify_signature(payload, reviewer_signatures[reviewer], reviewer_public_keys[reviewer], {"reviewerSignatures"})
                for reviewer in reviewers
            )
            and isinstance(payload.get("baselineArtifactDigest"), str)
            and str(payload["baselineArtifactDigest"]).startswith("sha256:")
            and manual_policy.get("manualSchemaDigest") == "sha256:" + hashlib.sha256(
                (Path(__file__).resolve().parents[1] / "config" / "manual_product_evaluation.schema.json").read_bytes()
            ).hexdigest()
            and preregistered_at <= evidence_started_at
            and evidence_ended_at <= evaluated_at <= as_of + timedelta(minutes=5)
        )
        snapshots = precision.get("snapshots")
        candidates = precision.get("candidates")
        precision_value_number = precision.get("value")
        ci_low = precision.get("ciLow")
        ci_high = precision.get("ciHigh")
        raw_snapshots = precision.get("rawSnapshots")
        raw_snapshot_ok = isinstance(raw_snapshots, list) and len(raw_snapshots) == snapshots
        snapshot_ids: set[str] = set()
        snapshot_times: set[datetime] = set()
        snapshot_precision: list[float] = []
        snapshot_local_dates: set[object] = set()
        snapshot_event_ids: set[str] = set()
        schedule_zone = ZoneInfo(str(manual_policy["snapshotTimezone"]))
        target_time = datetime.combine(
            evidence_started_at.astimezone(schedule_zone).date(),
            datetime.fromisoformat(f"2000-01-01T{manual_policy['snapshotLocalTime']}").time(),
            tzinfo=schedule_zone,
        )
        expected_snapshot_local_dates: set[object] = set()
        candidate_day = evidence_started_at.astimezone(schedule_zone).date()
        while True:
            candidate_local = datetime.combine(candidate_day, target_time.timetz(), tzinfo=schedule_zone)
            candidate_utc = candidate_local.astimezone(timezone.utc)
            if candidate_utc >= evidence_ended_at:
                break
            if candidate_utc >= evidence_started_at:
                expected_snapshot_local_dates.add(candidate_day)
            candidate_day += timedelta(days=1)
        tolerance_seconds = int(manual_policy["snapshotTimeToleranceMinutes"]) * 60
        if raw_snapshot_ok:
            for raw_snapshot in raw_snapshots:
                if not isinstance(raw_snapshot, dict) or set(raw_snapshot) != {
                    "snapshotId", "scheduledAt", "preregistrationDigest", "schedulerKeyId",
                    "schedulerSignature", "rankingRuleVersion", "rankingResponseDigest", "candidates",
                }:
                    raw_snapshot_ok = False
                    break
                snapshot_id = raw_snapshot.get("snapshotId")
                scheduled_at = parse_time(str(raw_snapshot.get("scheduledAt")))
                candidates_rows = raw_snapshot.get("candidates")
                snapshot_digest = snapshot_artifact_digest(raw_snapshot)
                committed_at = snapshot_commitment_times.get(snapshot_digest)
                matching_ledgers = [
                    (artifact, rows) for ledger_at, artifact, rows in ranking_ledgers
                    if abs((ledger_at - scheduled_at).total_seconds()) <= 300
                ]
                ledger_artifact, ledger_rows = matching_ledgers[0] if len(matching_ledgers) == 1 else ({}, [])
                expected_candidates = [row for row in ledger_rows if row.get("eligibleTop5")][:int(manual_policy["candidatesPerSnapshot"])]
                expected_ranking_digest = ledger_artifact.get("ledgerDigest")
                if (
                    not isinstance(snapshot_id, str) or len(snapshot_id) < 8 or snapshot_id in snapshot_ids
                    or scheduled_at in snapshot_times or not evidence_started_at <= scheduled_at <= evidence_ended_at
                    or not isinstance(candidates_rows, list) or len(candidates_rows) != int(manual_policy["candidatesPerSnapshot"])
                    or raw_snapshot.get("preregistrationDigest") != payload.get("preregistrationDigest")
                    or raw_snapshot.get("rankingRuleVersion") != preregistration.get("selectionRuleVersion")
                    or raw_snapshot.get("rankingResponseDigest") != expected_ranking_digest
                    or len(expected_candidates) != int(manual_policy["candidatesPerSnapshot"])
                    or committed_at is None or abs((committed_at - scheduled_at).total_seconds()) > 300
                    or not isinstance(raw_snapshot.get("schedulerKeyId"), str)
                    or raw_snapshot["schedulerKeyId"] not in scheduler_public_keys
                    or not verify_signature(
                        snapshot_commitment(raw_snapshot), raw_snapshot.get("schedulerSignature"),
                        scheduler_public_keys[str(raw_snapshot["schedulerKeyId"])], set(),
                    )
                ):
                    raw_snapshot_ok = False
                    break
                snapshot_ids.add(snapshot_id)
                snapshot_times.add(scheduled_at)
                local_time = scheduled_at.astimezone(schedule_zone)
                expected_local = target_time.replace(year=local_time.year, month=local_time.month, day=local_time.day)
                if local_time.date() in snapshot_local_dates or abs((local_time - expected_local).total_seconds()) > tolerance_seconds:
                    raw_snapshot_ok = False
                    break
                snapshot_local_dates.add(local_time.date())
                relevant = 0
                event_ids: set[str] = set()
                previous_score: float | None = None
                for rank, candidate in enumerate(candidates_rows, 1):
                    if not isinstance(candidate, dict) or set(candidate) != {
                        "rank", "eventId", "score", "scoreRunId", "labels",
                    }:
                        raw_snapshot_ok = False
                        break
                    event_id = candidate.get("eventId")
                    score = candidate.get("score")
                    labels = candidate.get("labels")
                    if (
                        candidate.get("rank") != rank or not isinstance(event_id, str) or len(event_id) < 3
                        or event_id in event_ids or event_id not in set(precision_candidate_event_ids)
                        or not _is_number(score)
                        or (previous_score is not None and float(score) > previous_score)
                        or not isinstance(candidate.get("scoreRunId"), str) or len(str(candidate["scoreRunId"])) < 8
                        or not isinstance(labels, list) or len(labels) != len(reviewers)
                        or candidate.get("eventId") != expected_candidates[rank - 1].get("eventId")
                        or float(score) != float(expected_candidates[rank - 1].get("score"))
                        or candidate.get("scoreRunId") != expected_candidates[rank - 1].get("scoreRunId")
                    ):
                        raw_snapshot_ok = False
                        break
                    event_ids.add(event_id)
                    previous_score = float(score)
                    snapshot_event_ids.add(event_id)
                    label_map = {
                        label.get("reviewerId"): label.get("relevant")
                        for label in labels if isinstance(label, dict) and set(label) == {"reviewerId", "relevant"}
                    }
                    if set(label_map) != set(reviewers) or any(not isinstance(value, bool) for value in label_map.values()):
                        raw_snapshot_ok = False
                        break
                    # Formal evidence requires resolved agreement; unresolved
                    # disagreement is insufficient, never silently adjudicated.
                    if len(set(label_map.values())) != 1:
                        raw_snapshot_ok = False
                        break
                    relevant += int(all(label_map.values()))
                if not raw_snapshot_ok:
                    break
                snapshot_precision.append(relevant / len(candidates_rows))
        raw_snapshot_set_complete = (
            snapshot_event_ids == set(precision_candidate_event_ids)
            if isinstance(precision_candidate_event_ids, list) else False
        )
        raw_snapshot_schedule_complete = snapshot_local_dates == expected_snapshot_local_dates
        expected_lead_rows: dict[str, dict[str, Any]] = {}
        for ledger_at, artifact, _rows in ranking_ledgers:
            if not evidence_started_at <= ledger_at <= evidence_ended_at:
                continue
            for crossing in artifact.get("leadCrossings", []):
                crossed_at = parse_time(str(crossing.get("crossedAt")))
                if evidence_started_at <= crossed_at <= evidence_ended_at:
                    expected_lead_rows.setdefault(str(crossing["eventId"]), crossing)
        lead_manifest_ok = (
            lead_manifest_ok
            and set(expected_lead_rows) == set(eligible_lead_event_ids)
            and all(
                lead_manifest_candidate_map[event_id].get("radarDetectedAt") == row.get("crossedAt")
                and lead_manifest_candidate_map[event_id].get("scoreRunId") == row.get("scoreRunId")
                and lead_manifest_candidate_map[event_id].get("thresholdVersion") == manual_policy.get("requiredThresholdVersion")
                for event_id, row in expected_lead_rows.items()
            )
        )
        recomputed_precision = sum(snapshot_precision) / len(snapshot_precision) if snapshot_precision else None
        recomputed_ci_low = recomputed_ci_high = None
        bootstrap_iterations = precision.get("bootstrapIterations")
        bootstrap_iterations_ok = (
            isinstance(bootstrap_iterations, int) and not isinstance(bootstrap_iterations, bool)
            and int(manual_policy["minimumBootstrapIterations"]) <= bootstrap_iterations <= 100_000
        )
        if raw_snapshot_ok and snapshot_precision and bootstrap_iterations_ok:
            rng = random.Random(precision.get("bootstrapSeed"))
            bootstrap_values = [
                sum(rng.choice(snapshot_precision) for _ in snapshot_precision) / len(snapshot_precision)
                for _ in range(bootstrap_iterations)
            ]
            alpha = 1 - float(manual_policy["confidenceLevel"])
            recomputed_ci_low = _percentile(bootstrap_values, alpha / 2)
            recomputed_ci_high = _percentile(bootstrap_values, 1 - alpha / 2)
        precision_ok = (
            set(precision) == precision_keys
            and precision.get("doubleReviewed") is True
            and isinstance(snapshots, int) and not isinstance(snapshots, bool)
            and snapshots >= int(manual_policy["minimumSnapshots"])
            and isinstance(candidates, int) and not isinstance(candidates, bool)
            and candidates == snapshots * int(manual_policy["candidatesPerSnapshot"])
            and _is_number(precision_value_number) and _is_number(ci_low) and _is_number(ci_high)
            and precision_value_number >= float(manual_policy["precisionAt5Target"])
            and float(manual_policy["precisionAt5MinimumCiLow"]) <= ci_low <= precision_value_number <= ci_high <= 1
            and precision.get("intervalMethod") == manual_policy["intervalMethod"]
            and precision.get("confidenceLevel") == manual_policy["confidenceLevel"]
            and bootstrap_iterations_ok
            and isinstance(precision.get("bootstrapSeed"), int)
            and not isinstance(precision.get("bootstrapSeed"), bool)
            and raw_snapshot_ok
            and raw_snapshot_set_complete
            and raw_snapshot_schedule_complete
            and recomputed_precision is not None
            and abs(float(precision_value_number) - recomputed_precision) <= 1e-6
            and recomputed_ci_low is not None and abs(float(ci_low) - recomputed_ci_low) <= 1e-6
            and recomputed_ci_high is not None and abs(float(ci_high) - recomputed_ci_high) <= 1e-6
        )
        raw_events = lead.get("rawEvents")
        raw_lead_ok = isinstance(raw_events, list) and len(raw_events) == lead.get("sample")
        lead_event_ids: set[str] = set()
        recomputed_leads: list[float] = []
        if raw_lead_ok:
            for raw_event in raw_events:
                if not isinstance(raw_event, dict) or set(raw_event) != {"eventId", "radarDetectedAt", "baselineDetectedAt", "scoreRunId", "thresholdVersion", "baselineLogRef"}:
                    raw_lead_ok = False
                    break
                event_id = raw_event.get("eventId")
                if (
                    not isinstance(event_id, str) or len(event_id) < 3 or event_id in lead_event_ids
                    or event_id not in set(eligible_lead_event_ids)
                    or not all(isinstance(raw_event.get(key), str) and len(str(raw_event[key])) >= 8 for key in ("scoreRunId", "thresholdVersion", "baselineLogRef"))
                ):
                    raw_lead_ok = False
                    break
                lead_event_ids.add(event_id)
                radar_at = parse_time(str(raw_event.get("radarDetectedAt")))
                baseline_at = parse_time(str(raw_event.get("baselineDetectedAt")))
                manifest_candidate = lead_manifest_candidate_map.get(str(event_id))
                baseline_log = baseline_log_map.get(str(event_id))
                if (
                    not evidence_started_at <= radar_at <= evidence_ended_at
                    or baseline_at > evaluated_at or baseline_at > as_of + timedelta(minutes=5)
                    or raw_event.get("thresholdVersion") != manual_policy.get("requiredThresholdVersion")
                    or manifest_candidate is None
                    or manifest_candidate.get("radarDetectedAt") != raw_event.get("radarDetectedAt")
                    or manifest_candidate.get("scoreRunId") != raw_event.get("scoreRunId")
                    or manifest_candidate.get("thresholdVersion") != raw_event.get("thresholdVersion")
                    or baseline_log is None
                    or baseline_log.get("logRef") != raw_event.get("baselineLogRef")
                    or baseline_log.get("firstDetectedAt") != raw_event.get("baselineDetectedAt")
                ):
                    raw_lead_ok = False
                    break
                recomputed_leads.append((baseline_at - radar_at).total_seconds() / 60)
        recomputed_median_lead = median(recomputed_leads) if recomputed_leads else None
        raw_lead_set_complete = lead_event_ids == set(eligible_lead_event_ids) if isinstance(eligible_lead_event_ids, list) else False
        lead_ok = (
            set(lead) == lead_keys
            and lead.get("independentBaseline") is True
            and isinstance(lead.get("sample"), int) and not isinstance(lead.get("sample"), bool)
            and lead["sample"] >= int(manual_policy["minimumDiscoveryLeadEvents"])
            and _is_number(lead.get("medianMinutes"))
            and lead["medianMinutes"] >= float(manual_policy["minimumMedianDiscoveryLeadMinutes"])
            and raw_lead_ok and raw_lead_set_complete and recomputed_median_lead is not None
            and abs(float(lead["medianMinutes"]) - recomputed_median_lead) <= 1e-6
        )
    except (KeyError, TypeError, ValueError):
        identity_ok = precision_ok = lead_ok = False
    passes = identity_ok and precision_ok and lead_ok
    manual_digest = "sha256:" + hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return passes, {
        "status": "eligible" if passes else "insufficient_or_failed",
        "identityAndPolicyBinding": identity_ok,
        "manualEvaluationDigest": manual_digest,
        "precisionAt5": precision,
        "discoveryLead": lead,
        "recomputed": {
            "precisionAt5": recomputed_precision if 'recomputed_precision' in locals() else None,
            "ciLow": recomputed_ci_low if 'recomputed_ci_low' in locals() else None,
            "ciHigh": recomputed_ci_high if 'recomputed_ci_high' in locals() else None,
            "medianDiscoveryLeadMinutes": recomputed_median_lead if 'recomputed_median_lead' in locals() else None,
        },
    }


def evaluate_samples(
    samples: list[dict[str, Any]],
    *,
    mode: str,
    cadence_seconds: int = 900,
    required_connectors: list[str] | None = None,
    manual_evaluation: dict[str, Any] | None = None,
    scheduler_public_keys: dict[str, bytes] | None = None,
    reviewer_public_keys: dict[str, bytes] | None = None,
    baseline_public_keys: dict[str, bytes] | None = None,
    ledger_public_keys: dict[str, bytes] | None = None,
    keyring_identity: dict[str, str] | None = None,
) -> dict[str, Any]:
    if mode not in MODES:
        raise ValueError(f"unsupported mode: {mode}")
    if mode != "canary" and cadence_seconds != 900:
        raise ValueError("formal soak/shadow cadence is frozen at 900 seconds")
    required = list(dict.fromkeys(required_connectors or DEFAULT_CONNECTORS))
    if len(required) < 4:
        raise ValueError("formal acceptance requires at least four distinct connector signal families")
    if not samples:
        return {
            "schemaVersion": "acceptance-monitor-report-v1", "mode": mode,
            "acceptanceEligible": False, "passes": None, "status": "insufficient", "reason": "no samples",
        }
    original_timestamps = [parse_time(item["observedAt"]) for item in samples]
    chronology_ok = all(right > left for left, right in zip(original_timestamps, original_timestamps[1:]))
    current = utcnow()
    future_samples = sum(timestamp > current + timedelta(minutes=5) for timestamp in original_timestamps)
    samples = [item for _, item in sorted(zip(original_timestamps, samples, strict=True), key=lambda pair: pair[0])]
    timestamps = sorted(original_timestamps)
    time_integrity_ok = chronology_ok and future_samples == 0
    collector_run_ids = [item.get("collectorRunId") for item in samples]
    scheduler_times: list[datetime] = []
    scheduler_fields_ok = True
    for item, observed_at in zip(samples, timestamps, strict=True):
        try:
            scheduled_at = parse_time(str(item["scheduledAt"]))
            scheduler_times.append(scheduled_at)
            if abs((observed_at - scheduled_at).total_seconds()) > 300:
                scheduler_fields_ok = False
        except (KeyError, TypeError, ValueError):
            scheduler_fields_ok = False
    trusted_scheduler_keys = scheduler_public_keys or {}
    trusted_ledger_keys = ledger_public_keys or {}
    collector_signatures_ok = (
        scheduler_fields_ok
        and len(collector_run_ids) == len(set(collector_run_ids))
        and all(isinstance(value, str) and len(value) >= 8 for value in collector_run_ids)
        and all(
            isinstance(item.get("collectorKeyId"), str)
            and item["collectorKeyId"] in trusted_scheduler_keys
            and verify_signature(
                item, item.get("collectorSignature"), trusted_scheduler_keys[str(item["collectorKeyId"])],
                {"sampleHash", "collectorSignature"},
            )
            for item in samples
        )
    )
    span_seconds = max(0.0, (timestamps[-1] - timestamps[0]).total_seconds())
    gaps = [(right - left).total_seconds() for left, right in zip(timestamps, timestamps[1:])]
    expected_samples = max(1, int(span_seconds // cadence_seconds) + 1)
    healthy_samples = sum(_sample_is_healthy(item) for item in samples)
    runtime_attestations = [
        item.get("responses", {}).get("health") for item in samples
        if _sample_is_healthy(item) and isinstance(item.get("responses", {}).get("health"), dict)
    ]
    runtime_identities = {
        (row.get("storageBackend"), row.get("migrationVersion"), row.get("instanceId"))
        for row in runtime_attestations
    }
    runtime_attestation_ok = (
        len(runtime_attestations) == len(samples)
        and len(runtime_identities) == 1
        and all(
            row.get("storageBackend") == "postgresql"
            and row.get("rlsVerified") is True
            and row.get("authRequired") is True
            and row.get("productionReady") is True
            and row.get("migrationVersion") == "001_init_rc2.6"
            and row.get("auditTriggersVerified") is True
            and row.get("migrationMarkerReadOnly") is True
            and row.get("databaseUser") == "radar_app"
            and row.get("databaseRoleSuperuser") is False
            and row.get("databaseRoleBypassRls") is False
            and isinstance(row.get("instanceId"), str) and len(str(row["instanceId"])) >= 8
            and _is_number(row.get("databaseClockSkewSeconds"))
            and float(row["databaseClockSkewSeconds"]) <= 5
            for row in runtime_attestations
        )
    )
    healthy_buckets = {
        min(expected_samples - 1, max(0, int(round((timestamp - timestamps[0]).total_seconds() / cadence_seconds))))
        for timestamp, item in zip(timestamps, samples, strict=True)
        if _sample_is_healthy(item)
    }
    sample_coverage = len(healthy_buckets) / expected_samples
    minimum_span = MODES[mode]["minimumSpanHours"] * 3600
    evidence_window_ok = all(
        int(item.get("evidenceWindowHours", 0)) >= MODES[mode]["minimumSpanHours"]
        for item in samples
    ) if mode != "canary" else True
    cadence_ok = (max(gaps, default=0) <= cadence_seconds * 2) and sample_coverage >= .95
    span_ok = span_seconds >= minimum_span

    latest_coverage = next((
        item.get("responses", {}).get("coverage") for item in reversed(samples)
        if isinstance(item.get("responses", {}).get("coverage"), dict)
    ), {})
    rights = {
        str(row.get("id")): row.get("rightsStatus")
        for row in latest_coverage.get("connectors", [])
        if isinstance(row, dict)
    }
    rights_active_buckets: dict[str, set[int]] = {connector_id: set() for connector_id in required}
    rights_explicitly_inactive: dict[str, int] = {connector_id: 0 for connector_id in required}
    connector_families: dict[str, set[str]] = {connector_id: set() for connector_id in required}
    for timestamp, item in zip(timestamps, samples, strict=True):
        coverage = item.get("responses", {}).get("coverage") if isinstance(item.get("responses"), dict) else None
        rows = coverage.get("connectors", []) if isinstance(coverage, dict) else []
        statuses = {
            str(row.get("id")): row.get("rightsStatus") for row in rows if isinstance(row, dict)
        }
        families = {
            str(row.get("id")): str(row.get("family")) for row in rows
            if isinstance(row, dict) and isinstance(row.get("family"), str) and row.get("family")
        }
        bucket = min(expected_samples - 1, max(0, int(round((timestamp - timestamps[0]).total_seconds() / cadence_seconds))))
        for connector_id in required:
            status = statuses.get(connector_id)
            if status == "active":
                rights_active_buckets[connector_id].add(bucket)
            elif status is not None:
                rights_explicitly_inactive[connector_id] += 1
            if connector_id in families:
                connector_families[connector_id].add(families[connector_id])
    unique_runs: dict[tuple[str, str], dict[str, Any]] = {}
    for sample in samples:
        payload = sample.get("responses", {}).get("connectorRuns") or {}
        for row in payload.get("items", []):
            if isinstance(row, dict):
                unique_runs[(str(row.get("connectorId")), str(row.get("startedAt")))] = row
    connector_checks: dict[str, Any] = {}
    for connector_id in required:
        rows = sorted(
            (
                row for (row_connector, _), row in unique_runs.items()
                if row_connector == connector_id and timestamps[0] <= parse_time(str(row["startedAt"])) <= timestamps[-1]
            ),
            key=lambda row: parse_time(str(row["startedAt"])),
        )
        total_items = sum(int(row.get("inserted", 0)) + int(row.get("duplicates", 0)) for row in rows)
        duplicates = sum(int(row.get("duplicates", 0)) for row in rows)
        healthy_rate = sum(row.get("status") == "healthy" for row in rows) / len(rows) if rows else None
        input_dedup_hit_rate = duplicates / total_items if total_items else None
        run_times = [parse_time(str(row["startedAt"])) for row in rows]
        run_gaps = [(right - left).total_seconds() for left, right in zip(run_times, run_times[1:])]
        edge_gaps = (
            [max(0.0, (run_times[0] - timestamps[0]).total_seconds()), max(0.0, (timestamps[-1] - run_times[-1]).total_seconds())]
            if run_times else []
        )
        all_run_gaps = run_gaps + edge_gaps
        run_buckets = {
            min(expected_samples - 1, max(0, int(round((run_at - timestamps[0]).total_seconds() / cadence_seconds))))
            for run_at in run_times if timestamps[0] <= run_at <= timestamps[-1]
        }
        run_coverage = len(run_buckets) / expected_samples
        continuity_ok = bool(rows) and max(all_run_gaps, default=0) <= cadence_seconds * 2 and run_coverage >= .95
        rights_active_coverage = len(rights_active_buckets[connector_id]) / expected_samples
        rights_continuity_ok = rights_active_coverage >= .95 and rights_explicitly_inactive[connector_id] == 0
        check_passes = (
            rights.get(connector_id) == "active"
            and rights_continuity_ok
            and healthy_rate is not None and healthy_rate >= .95
            and continuity_ok
            and total_items > 0
        )
        connector_checks[connector_id] = {
            "passes": check_passes,
            "rightsStatus": rights.get(connector_id),
            "rightsActiveCoverage": rights_active_coverage,
            "explicitInactiveRightsSamples": rights_explicitly_inactive[connector_id],
            "runs": len(rows),
            "observationsSeen": total_items,
            "familiesSeen": sorted(connector_families[connector_id]),
            "healthyRunRate": healthy_rate,
            "inputDedupHitRate": input_dedup_hit_rate,
            "runCoverage": run_coverage,
            "maximumRunGapSeconds": max(all_run_gaps, default=None),
        }

    stable_families = {
        next(iter(values)) for values in connector_families.values() if len(values) == 1
    }
    signal_family_ok = (
        all(len(values) == 1 for values in connector_families.values())
        and len(stable_families) >= 4
        and {"discussion", "behavior"}.issubset(stable_families)
    )

    pipeline = next((
        item.get("responses", {}).get("pipelineSla") for item in reversed(samples)
        if isinstance(item.get("responses", {}).get("pipelineSla"), dict)
    ), {})
    pipeline_ok = (
        pipeline.get("evidenceStatus") == "eligible"
        and pipeline.get("passesTarget") is True
        and int(pipeline.get("unrecoveredFailures", 0)) == 0
        and int(pipeline.get("invalidFutureTimestamps", 0)) == 0
        and int(pipeline.get("invalidTimestampOrder", 0)) == 0
    )
    data_quality = next((
        item.get("responses", {}).get("dataQuality") for item in reversed(samples)
        if isinstance(item.get("responses", {}).get("dataQuality"), dict)
    ), {})
    data_quality_ok = data_quality.get("evidenceStatus") == "eligible" and data_quality.get("passesTarget") is True

    beta_rows = [
        item.get("responses", {}).get("betaMetrics") for item in samples
        if _sample_is_healthy(item) and isinstance(item.get("responses", {}).get("betaMetrics"), dict)
    ]
    beta = next((
        item.get("responses", {}).get("betaMetrics") for item in reversed(samples)
        if isinstance(item.get("responses", {}).get("betaMetrics"), dict)
    ), {})
    policy_identities = {
        (
            row.get("productMetricPolicyVersion"), row.get("productMetricPolicyDigest"),
            row.get("policyStatus"), row.get("policyFrozenAt"),
            json.dumps(row.get("manualEvaluationPolicy"), sort_keys=True, separators=(",", ":")),
            json.dumps(row.get("acceptanceMonitoringPolicy"), sort_keys=True, separators=(",", ":")),
        )
        for row in beta_rows
    }
    try:
        policy_frozen_at = parse_time(str(beta["policyFrozenAt"]))
    except (KeyError, TypeError, ValueError):
        policy_frozen_at = current + timedelta(days=1)
    monitoring_policy = beta.get("acceptanceMonitoringPolicy") if isinstance(beta.get("acceptanceMonitoringPolicy"), dict) else {}
    manual_policy = beta.get("manualEvaluationPolicy") if isinstance(beta.get("manualEvaluationPolicy"), dict) else {}
    local_monitor_digest = "sha256:" + hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    monitor_implementation_ok = (
        monitoring_policy.get("monitorVersion") == "acceptance-monitor-v1.1"
        and monitoring_policy.get("monitorDigest") == local_monitor_digest
    )
    collection_from_values = {
        row.get("collectionWindow", {}).get("from")
        for row in beta_rows if isinstance(row.get("collectionWindow"), dict)
    }
    try:
        beta_as_of_times = [parse_time(str(row["asOf"])) for row in beta_rows]
        collection_window_ok = (
            len(collection_from_values) == 1
            and next(iter(collection_from_values)) == beta.get("policyFrozenAt")
            and all(right > left for left, right in zip(beta_as_of_times, beta_as_of_times[1:]))
        )
    except (KeyError, TypeError, ValueError):
        collection_window_ok = False
    trusted_keyring_identity = keyring_identity or {}
    try:
        keyring_frozen_at = parse_time(str(trusted_keyring_identity["keyringFrozenAt"]))
    except (KeyError, TypeError, ValueError):
        keyring_frozen_at = current + timedelta(days=1)
    keyring_identity_ok = (
        monitoring_policy.get("keyringVersion") == trusted_keyring_identity.get("keyringVersion")
        and monitoring_policy.get("keyringDigest") == trusted_keyring_identity.get("keyringDigest")
        and monitoring_policy.get("keyringFrozenAt") == trusted_keyring_identity.get("keyringFrozenAt")
        and keyring_frozen_at <= timestamps[0]
    )
    policy_consistency_ok = (
        len(beta_rows) == healthy_samples
        and len(policy_identities) == 1
        and beta.get("policyStatus") == "frozen_for_beta_collection"
        and isinstance(beta.get("productMetricPolicyDigest"), str)
        and str(beta["productMetricPolicyDigest"]).startswith("sha256:")
        and policy_frozen_at <= timestamps[0]
        and monitoring_policy.get("cadenceSeconds") == 900
        and int(monitoring_policy.get("minimumDistinctConnectorFamilies", 0)) >= 4
        and {"discussion", "behavior"}.issubset(set(monitoring_policy.get("requiredSignalFamilies", [])))
        and monitoring_policy.get("requireNonzeroConnectorObservations") is True
        and monitor_implementation_ok
        and keyring_identity_ok
        and collection_window_ok
    )
    collector_authenticity_ok = collector_signatures_ok and keyring_identity_ok
    validated_ranking_ledgers: list[tuple[datetime, dict[str, Any], list[dict[str, Any]]]] = []
    for sample, observed_at in zip(samples, timestamps, strict=True):
        artifact = sample.get("responses", {}).get("rankingLedger") if isinstance(sample.get("responses"), dict) else None
        rows = _validated_ranking_ledger(
            artifact, observed_at, trusted_ledger_keys,
            str(beta.get("productMetricPolicyVersion") or "") or None,
            str(manual_policy.get("requiredThresholdVersion") or "") or None,
        )
        if rows is not None and isinstance(artifact, dict):
            validated_ranking_ledgers.append((observed_at, artifact, rows))
    crossing_continuity_ok = True
    previous_crossings: dict[str, dict[str, Any]] = {}
    for _ledger_at, artifact, _rows in validated_ranking_ledgers:
        current_crossings = {str(row["eventId"]): row for row in artifact.get("leadCrossings", []) if isinstance(row, dict)}
        if not set(previous_crossings).issubset(current_crossings) or any(
            current_crossings[event_id] != row for event_id, row in previous_crossings.items()
        ):
            crossing_continuity_ok = False
        previous_crossings = current_crossings
    score_ledger_ok = len(validated_ranking_ledgers) == len(samples) and crossing_continuity_ok

    fact_ledger_ok = bool(beta_rows)
    previous_counts: dict[str, int] | None = None
    previous_digests: dict[str, str] | None = None
    previous_derived: dict[str, int] | None = None
    ledger_digests_seen: list[dict[str, str]] = []
    for row in beta_rows:
        ledger = row.get("factLedger") if isinstance(row.get("factLedger"), dict) else {}
        counts = ledger.get("counts") if isinstance(ledger.get("counts"), dict) else {}
        digests = ledger.get("digests") if isinstance(ledger.get("digests"), dict) else {}
        count_names = {
            "alertDeliveries", "feedback", "queueEntries", "interactions", "metricIncidents",
            "invalidExclusionAttempts",
        }
        digest_names = {"alertDeliveries", "feedback", "queueEntries", "interactions", "metricIncidents"}
        strong = row.get("strongAlertAcceptance") if isinstance(row.get("strongAlertAcceptance"), dict) else {}
        erroneous = row.get("erroneousStrongAlerts") if isinstance(row.get("erroneousStrongAlerts"), dict) else {}
        triage = row.get("firstTriageSla") if isinstance(row.get("firstTriageSla"), dict) else {}
        review = row.get("activeReviewTime") if isinstance(row.get("activeReviewTime"), dict) else {}
        derived = {
            "strongAlertDenominator": strong.get("denominatorDeliveredStrongAlerts"),
            "workdayStrongAlerts": erroneous.get("workdayStrongAlerts"),
            "erroneousStrongAlerts": erroneous.get("erroneousStrongAlerts"),
            "triageDenominator": triage.get("sample"),
            "completedReviews": review.get("sample"),
        }
        row_ok = (
            ledger.get("version") == "beta-fact-ledger-v1"
            and set(counts) == count_names
            and all(isinstance(counts.get(name), int) and not isinstance(counts.get(name), bool) and counts[name] >= 0 for name in count_names)
            and set(digests) == digest_names
            and all(isinstance(digests.get(name), str) and str(digests[name]).startswith("sha256:") for name in digest_names)
            and all(isinstance(value, int) and not isinstance(value, bool) and value >= 0 for value in derived.values())
        )
        if row_ok and previous_counts is not None and previous_digests is not None and previous_derived is not None:
            row_ok = (
                all(counts[name] >= previous_counts[name] for name in count_names)
                and all(derived[name] >= previous_derived[name] for name in derived)
                and all(
                    counts[name] != previous_counts[name] or digests[name] == previous_digests[name]
                    for name in digest_names
                )
            )
        fact_ledger_ok = fact_ledger_ok and row_ok
        if row_ok:
            previous_counts = {name: int(counts[name]) for name in count_names}
            previous_digests = {name: str(digests[name]) for name in digest_names}
            previous_derived = {name: int(derived[name]) for name in derived}
            ledger_digests_seen.append(previous_digests)

    product_ok = (
        beta.get("evidenceStatus") == "eligible"
        and beta.get("passesMeasuredGates") is True
        and isinstance(beta.get("activeReviewTime"), dict)
        and beta["activeReviewTime"].get("formalMeasurementEligible") is True
        and beta["activeReviewTime"].get("measurementVersion") == "server-heartbeat-v2"
        and isinstance(beta.get("serverObservedReviewTime"), dict)
        and beta["serverObservedReviewTime"].get("formalMeasurementEligible") is True
        and beta["serverObservedReviewTime"].get("measurementVersion") == "server-heartbeat-v2"
        and fact_ledger_ok
    )
    preregistration_commitments = {item.get("manualPreregistrationDigest") for item in samples}
    expected_preregistration_digest = (
        next(iter(preregistration_commitments))
        if len(preregistration_commitments) == 1
        and isinstance(next(iter(preregistration_commitments)), str)
        else None
    )
    snapshot_commitment_times = {
        str(item["manualSnapshotCommitmentDigest"]): timestamp
        for item, timestamp in zip(samples, timestamps, strict=True)
        if isinstance(item.get("manualSnapshotCommitmentDigest"), str)
    }
    manual_ok, manual_report = _manual_evaluation(
        manual_evaluation, str(beta.get("productMetricPolicyVersion") or "") or None,
        str(beta.get("productMetricPolicyDigest") or "") or None, manual_policy,
        trusted_scheduler_keys, reviewer_public_keys or {}, baseline_public_keys or {},
        trusted_ledger_keys,
        expected_preregistration_digest=expected_preregistration_digest,
        snapshot_commitment_times=snapshot_commitment_times,
        evidence_started_at=timestamps[0], evidence_ended_at=timestamps[-1], as_of=current,
        ranking_ledgers=validated_ranking_ledgers,
    )

    if mode == "canary":
        passes: bool | None = all(_sample_is_healthy(item) for item in samples) and time_integrity_ok
        status = "canary_only"
        acceptance_eligible = False
    else:
        acceptance_eligible = (
            span_ok and cadence_ok and evidence_window_ok and time_integrity_ok
            and collector_authenticity_ok and runtime_attestation_ok
        )
        base_passes = (
            acceptance_eligible and all(check["passes"] for check in connector_checks.values())
            and signal_family_ok and policy_consistency_ok and fact_ledger_ok and score_ledger_ok and pipeline_ok and data_quality_ok
        )
        passes = base_passes and product_ok and manual_ok if MODES[mode]["requiresProductEvidence"] else base_passes
        status = "passed" if passes else "failed" if acceptance_eligible else "insufficient"

    return {
        "schemaVersion": "acceptance-monitor-report-v1",
        "mode": mode,
        "generatedAt": utcnow().isoformat(),
        "status": status,
        "acceptanceEligible": acceptance_eligible,
        "passes": passes,
        "scoreLedger": {
            "passes": score_ledger_ok, "validatedArtifacts": len(validated_ranking_ledgers),
            "leadCrossingContinuityPasses": crossing_continuity_ok,
        },
        "monitoring": {
            "samples": len(samples), "healthySamples": healthy_samples, "healthyCadenceBuckets": len(healthy_buckets),
            "expectedSamples": expected_samples,
            "sampleCoverage": sample_coverage, "spanHours": span_seconds / 3600,
            "minimumSpanHours": MODES[mode]["minimumSpanHours"], "maximumSampleGapSeconds": max(gaps, default=0),
            "cadenceSeconds": cadence_seconds, "spanPasses": span_ok, "cadencePasses": cadence_ok,
            "chronologyPasses": chronology_ok, "futureSamples": future_samples, "timeIntegrityPasses": time_integrity_ok,
            "evidenceWindowPasses": evidence_window_ok,
            "collectorAuthenticityPasses": collector_authenticity_ok,
            "collectorSignaturePasses": collector_signatures_ok,
            "schedulerTimestampPasses": scheduler_fields_ok,
            "runtimeAttestationPasses": runtime_attestation_ok,
            "runtimeIdentities": [list(value) for value in sorted(runtime_identities, key=str)],
        },
        "connectors": connector_checks,
        "signalFamilyCoverage": {
            "passes": signal_family_ok, "stableFamilies": sorted(stable_families),
            "minimumDistinctFamilies": 4, "requiredFamilies": ["discussion", "behavior"],
        },
        "frozenPolicy": {
            "passes": policy_consistency_ok, "identitiesSeen": [list(value) for value in sorted(policy_identities, key=str)],
            "latestVersion": beta.get("productMetricPolicyVersion"), "latestDigest": beta.get("productMetricPolicyDigest"),
            "keyringIdentityPasses": keyring_identity_ok, "keyringIdentity": trusted_keyring_identity,
            "fixedCollectionWindowPasses": collection_window_ok,
            "monitorImplementationPasses": monitor_implementation_ok,
            "localMonitorDigest": local_monitor_digest,
        },
        "pipelineSla": {"passes": pipeline_ok, "latest": pipeline},
        "dataQuality": {"passes": data_quality_ok, "latest": data_quality},
        "productMetrics": {
            "required": MODES[mode]["requiresProductEvidence"], "passesMeasuredGates": product_ok,
            "factLedgerContinuityPasses": fact_ledger_ok,
            "signedLedgerDigests": ledger_digests_seen,
            "latest": beta,
        },
        "manualEvaluation": {"required": MODES[mode]["requiresProductEvidence"], "passes": manual_ok, **manual_report},
        "limitations": [
            "canary mode never satisfies a duration gate",
            "a passing report requires unedited source samples plus production storage and authorization evidence",
            "formal modes require scheduler-issued timestamps/run IDs and Ed25519 signatures from a frozen public-key registry",
        ],
    }


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Collect and evaluate rc2 soak/shadow acceptance evidence")
    commands = root.add_subparsers(dest="command", required=True)
    collect = commands.add_parser("collect", help="append one immutable monitoring sample")
    collect.add_argument("--api-url", required=True)
    collect.add_argument("--output", type=Path, required=True)
    collect.add_argument("--api-key", default=os.getenv("RADAR_API_KEY"))
    collect.add_argument("--timeout", type=float, default=10)
    collect.add_argument("--evidence-window-hours", type=int, default=72)
    collect.add_argument(
        "--manual-preregistration", type=Path,
        help="scheduler-signed preregistration artifact; its digest is committed into the signed sample",
    )
    collect.add_argument(
        "--manual-snapshot", type=Path,
        help="scheduler-signed daily ranking snapshot; its commitment digest is bound into this sample",
    )
    report = commands.add_parser("report", help="evaluate a JSONL evidence file")
    report.add_argument("--input", type=Path, required=True)
    report.add_argument("--mode", choices=sorted(MODES), required=True)
    report.add_argument("--cadence-seconds", type=int, default=900)
    report.add_argument("--required-connectors", default=",".join(DEFAULT_CONNECTORS))
    report.add_argument("--manual-evaluation", type=Path)
    report.add_argument("--output", type=Path)
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "collect":
        preregistration = (
            json.loads(args.manual_preregistration.read_text(encoding="utf-8"))
            if args.manual_preregistration else None
        )
        manual_snapshot = (
            json.loads(args.manual_snapshot.read_text(encoding="utf-8"))
            if args.manual_snapshot else None
        )
        sample = collect_sample(
            args.api_url, args.api_key, timeout=args.timeout, evidence_window_hours=args.evidence_window_hours,
            manual_preregistration=preregistration,
            manual_snapshot=manual_snapshot,
        )
        append_sample(args.output, sample)
        print(json.dumps(sample, ensure_ascii=False, indent=2))
        return 0 if sample["sampleHealthy"] else 1
    samples = load_samples(args.input)
    manual = json.loads(args.manual_evaluation.read_text(encoding="utf-8")) if args.manual_evaluation else None
    keyring_path = Path(__file__).resolve().parents[1] / "config" / "acceptance_monitor_public_keys.json"
    scheduler_keys, reviewer_keys, baseline_keys, ledger_keys, keyring_identity = load_keyring(keyring_path)
    report = evaluate_samples(
        samples,
        mode=args.mode,
        cadence_seconds=args.cadence_seconds,
        required_connectors=[value.strip() for value in args.required_connectors.split(",") if value.strip()],
        manual_evaluation=manual,
        scheduler_public_keys=scheduler_keys,
        reviewer_public_keys=reviewer_keys,
        baseline_public_keys=baseline_keys,
        ledger_public_keys=ledger_keys,
        keyring_identity=keyring_identity,
    )
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if report["status"] == "canary_only":
        return 0 if report["passes"] else 1
    return 0 if report["passes"] else 2 if report["status"] == "insufficient" else 1


if __name__ == "__main__":
    sys.exit(main())
