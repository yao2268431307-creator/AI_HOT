"""Fail-closed validation for a rendered production deployment bundle.

The tool never prints configuration values. It is safe to use in CI or on a
deployment host after a secret manager has rendered the workload-specific env
files. ``bootstrap`` validates infrastructure wiring; ``formal`` additionally
requires approved connector rights and the frozen acceptance keyring.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from datetime import datetime
from urllib.parse import parse_qs, unquote, urlsplit


DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
FORMAL_CONNECTORS = ("rss", "hackernews", "github", "huggingface", "arxiv", "openalex")
SERVICE_FILES = {
    "api": "RADAR_API_ENV_FILE",
    "scheduler": "RADAR_SCHEDULER_ENV_FILE",
    "alert": "RADAR_ALERT_ENV_FILE",
}
PLACEHOLDER_MARKERS = ("<", ">", "replace-", "example.com", ".invalid", "your-", "changeme")


class Findings:
    def __init__(self) -> None:
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.passed: list[str] = []

    def require(self, condition: bool, name: str, message: str) -> None:
        if condition:
            self.passed.append(name)
        else:
            self.errors.append(f"{name}: {message}")

    def warn(self, condition: bool, name: str, message: str) -> None:
        if not condition:
            self.warnings.append(f"{name}: {message}")

    def payload(self, phase: str) -> dict[str, object]:
        return {
            "schemaVersion": "production-preflight-v1",
            "phase": phase,
            "qualifies": not self.errors,
            "checksPassed": len(self.passed),
            "errors": self.errors,
            "warnings": self.warnings,
        }


def parse_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ValueError(f"{path.name}:{line_number} is not KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key):
            raise ValueError(f"{path.name}:{line_number} has an invalid key")
        if key in values:
            raise ValueError(f"{path.name}:{line_number} duplicates {key}")
        values[key] = value.strip().strip("\"").strip("'")
    return values


def is_real(value: str | None) -> bool:
    if not value:
        return False
    lowered = value.lower()
    return not any(marker in lowered for marker in PLACEHOLDER_MARKERS)


def json_object(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        result = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return result if isinstance(result, dict) else {}


def positive_map(value: str | None) -> dict[str, float]:
    result: dict[str, float] = {}
    for key, raw_value in json_object(value).items():
        if isinstance(raw_value, bool):
            continue
        try:
            number = float(raw_value)
        except (TypeError, ValueError):
            continue
        if 0 < number < float("inf"):
            result[str(key)] = number
    return result


def timezone_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def valid_acceptance_keyring(keyring: object) -> bool:
    if not isinstance(keyring, dict):
        return False
    if keyring.get("schemaVersion") != "acceptance-ed25519-keyring-v1":
        return False
    if not isinstance(keyring.get("keyringVersion"), str) or len(keyring["keyringVersion"]) < 8:
        return False
    expected_lengths = {
        "schedulerKeys": 1,
        "reviewerKeys": 2,
        "baselineKeys": 1,
        "ledgerKeys": 1,
    }
    key_ids: set[str] = set()
    materials: set[bytes] = set()
    for group, minimum in expected_lengths.items():
        rows = keyring.get(group)
        if not isinstance(rows, list):
            return False
        active_count = 0
        for row in rows:
            if not isinstance(row, dict) or set(row) != {"keyId", "publicKeyBase64", "status"}:
                return False
            if row.get("status") not in {"active", "retired", "revoked"}:
                return False
            active_count += row.get("status") == "active"
            key_id = row.get("keyId")
            encoded = row.get("publicKeyBase64")
            if not isinstance(key_id, str) or len(key_id) < 8 or not isinstance(encoded, str):
                return False
            try:
                material = base64.b64decode(encoded, validate=True)
            except (ValueError, binascii.Error):
                return False
            if len(material) != 32 or key_id in key_ids or material in materials:
                return False
            key_ids.add(key_id)
            materials.add(material)
        if active_count < minimum:
            return False
    return timezone_timestamp(keyring.get("frozenAt")) and len(key_ids) == len(materials)


def secure_file(path: Path, findings: Findings, name: str) -> None:
    findings.require(path.is_file(), name, "file is missing")
    if not path.is_file():
        return
    if os.name != "nt":
        mode = stat.S_IMODE(path.stat().st_mode)
        findings.require(mode & 0o077 == 0, f"{name}.permissions", "must not be group/world readable")
    else:
        findings.warn(False, f"{name}.permissions", "ACL review is required on Windows")


def valid_https(value: str | None) -> bool:
    if not is_real(value):
        return False
    try:
        parts = urlsplit(str(value))
        return parts.scheme == "https" and bool(parts.hostname) and not parts.username and not parts.password
    except ValueError:
        return False


def valid_database_dsn(value: str | None) -> bool:
    if not is_real(value):
        return False
    try:
        parts = urlsplit(str(value))
        ssl_mode = parse_qs(parts.query).get("sslmode", [""])[-1]
        return (
            parts.scheme in {"postgres", "postgresql"}
            and bool(parts.hostname)
            and bool(parts.username)
            and ssl_mode in {"require", "verify-ca", "verify-full"}
        )
    except ValueError:
        return False


def valid_origin(value: str | None) -> bool:
    if not valid_https(value):
        return False
    parts = urlsplit(str(value))
    return parts.path in {"", "/"} and not parts.query and not parts.fragment


def valid_base_url(value: str | None) -> bool:
    if not valid_https(value):
        return False
    parts = urlsplit(str(value))
    return not parts.query and not parts.fragment


def bundle_path(value: str | None, release_path: Path) -> Path:
    candidate = Path(value or "")
    return candidate if candidate.is_absolute() else release_path.parent / candidate


def database_identity(value: str | None) -> tuple[str, str, int | None, str]:
    try:
        parts = urlsplit(str(value or ""))
        return unquote(parts.username or ""), parts.hostname or "", parts.port, parts.path
    except ValueError:
        return "", "", None, ""


def validate_release(release: dict[str, str], findings: Findings) -> dict[str, str]:
    required = {
        "RADAR_API_IMAGE_REPOSITORY", "RADAR_API_IMAGE_DIGEST",
        "RADAR_WEB_IMAGE_REPOSITORY", "RADAR_WEB_IMAGE_DIGEST",
        "RADAR_PUBLIC_API_URL", "RADAR_PUBLIC_WEB_URL", *SERVICE_FILES.values(),
        "RADAR_SOURCE_IDENTITIES_FILE", "RADAR_RSS_FEEDS_FILE",
        "RADAR_RELEASE_REPOSITORY", "RADAR_RELEASE_COMMIT",
        "RADAR_RELEASE_MANIFEST_DIGEST",
    }
    for key in sorted(required):
        findings.require(is_real(release.get(key)), f"release.{key}", "missing or placeholder value")
    for key in ("RADAR_API_IMAGE_DIGEST", "RADAR_WEB_IMAGE_DIGEST"):
        findings.require(bool(DIGEST.fullmatch(release.get(key, ""))), f"release.{key}.format", "must be lowercase sha256")
    findings.require(valid_base_url(release.get("RADAR_PUBLIC_API_URL")), "release.RADAR_PUBLIC_API_URL.https", "must be a credential-free HTTPS base URL")
    findings.require(valid_origin(release.get("RADAR_PUBLIC_WEB_URL")), "release.RADAR_PUBLIC_WEB_URL.https", "must be a credential-free HTTPS origin")
    findings.require(bool(COMMIT.fullmatch(release.get("RADAR_RELEASE_COMMIT", ""))), "release.RADAR_RELEASE_COMMIT.format", "must be a full lowercase commit ID")
    findings.require(bool(DIGEST.fullmatch(release.get("RADAR_RELEASE_MANIFEST_DIGEST", ""))), "release.RADAR_RELEASE_MANIFEST_DIGEST.format", "must bind the signed manifest SHA-256")
    findings.require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", release.get("RADAR_RELEASE_REPOSITORY", ""))), "release.RADAR_RELEASE_REPOSITORY.format", "must be OWNER/REPOSITORY")
    for key in ("RADAR_API_IMAGE_REPOSITORY", "RADAR_WEB_IMAGE_REPOSITORY"):
        findings.require("@" not in release.get(key, ""), f"release.{key}.shape", "repository and digest must be separate")
        findings.require(release.get(key, "").startswith("ghcr.io/"), f"release.{key}.registry", "release workflow publishes only to GHCR")
    findings.require(
        release.get("RADAR_API_IMAGE_REPOSITORY") != release.get("RADAR_WEB_IMAGE_REPOSITORY"),
        "release.imageRepositories.distinct",
        "API and Web repositories must be distinct",
    )
    owner = release.get("RADAR_RELEASE_REPOSITORY", "").split("/", 1)[0].lower()
    findings.require(
        release.get("RADAR_API_IMAGE_REPOSITORY", "").lower()
        == f"ghcr.io/{owner}/ai-hot-radar-api"
        and release.get("RADAR_WEB_IMAGE_REPOSITORY", "").lower()
        == f"ghcr.io/{owner}/ai-hot-radar-web",
        "release.imageRepositories.owner",
        "image repositories must match the signed workflow owner and fixed service names",
    )
    return {
        "api": release.get("RADAR_API_IMAGE_DIGEST", ""),
        "web": release.get("RADAR_WEB_IMAGE_DIGEST", ""),
    }


def require_keys(values: dict[str, str], keys: set[str], findings: Findings, service: str) -> None:
    for key in sorted(keys):
        findings.require(is_real(values.get(key)), f"{service}.{key}", "missing or placeholder value")


def validate_services(
    services: dict[str, dict[str, str]], release_digests: dict[str, str], phase: str,
    public_web_url: str, findings: Findings,
) -> None:
    api, scheduler, alert = services["api"], services["scheduler"], services["alert"]
    require_keys(api, {
        "DATABASE_URL", "DELETION_DATABASE_URL", "RADAR_JWT_PUBLIC_KEYS",
        "RADAR_JWT_ISSUER", "RADAR_JWT_AUDIENCE", "RADAR_INSTANCE_ID",
        "SCORE_LEDGER_ED25519_PRIVATE_KEY", "SCORE_LEDGER_ED25519_KEY_ID",
        "RADAR_RELEASE_IMAGE_DIGESTS", "CORS_ORIGINS", "RADAR_SYSTEM_WORKSPACE_ID",
        "PRODUCT_METRIC_POLICY_FILE", "EXTERNAL_DATA_BUDGET_RMB",
        "CONNECTOR_BUDGETS_RMB_JSON", "SIGNAL_FAMILY_BUDGETS_RMB_JSON",
    }, findings, "api")
    findings.require(api.get("DEMO_MODE") == "false", "api.DEMO_MODE", "must be false")
    findings.require(api.get("AUTH_REQUIRED") == "true", "api.AUTH_REQUIRED", "must be true")
    findings.require(api.get("RADAR_AUTH_MODE") == "jwt", "api.RADAR_AUTH_MODE", "must be jwt")
    findings.require(valid_database_dsn(api.get("DATABASE_URL")), "api.DATABASE_URL.tls", "must be a TLS PostgreSQL DSN")
    findings.require(valid_database_dsn(api.get("DELETION_DATABASE_URL")), "api.DELETION_DATABASE_URL.tls", "must be a TLS PostgreSQL DSN")
    origins = [item.strip() for item in api.get("CORS_ORIGINS", "").split(",") if item.strip()]
    findings.require(bool(origins) and all(valid_origin(item) for item in origins), "api.CORS_ORIGINS.https", "all production origins must be HTTPS origins")
    findings.require(public_web_url in origins, "api.CORS_ORIGINS.web", "must include the deployed Web origin")
    findings.require(api.get("PRODUCT_METRIC_POLICY_FILE") == "/app/config/product_metric_policy.json", "api.PRODUCT_METRIC_POLICY_FILE", "must use the frozen policy shipped in the image")
    findings.require(valid_https(api.get("RADAR_JWT_ISSUER")), "api.RADAR_JWT_ISSUER.https", "must be HTTPS")
    findings.require(bool(json_object(api.get("RADAR_JWT_PUBLIC_KEYS"))), "api.RADAR_JWT_PUBLIC_KEYS.nonempty", "must contain at least one PEM public key")

    require_keys(scheduler, {
        "DATABASE_URL", "REDIS_URL", "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY", "RAW_EVIDENCE_BUCKET", "RADAR_INSTANCE_ID",
        "RADAR_JWT_PUBLIC_KEYS", "RADAR_JWT_ISSUER", "RADAR_JWT_AUDIENCE",
        "EXTERNAL_DATA_BUDGET_RMB", "CONNECTOR_BUDGETS_RMB_JSON",
        "SIGNAL_FAMILY_BUDGETS_RMB_JSON", "RADAR_RELEASE_IMAGE_DIGESTS",
    }, findings, "scheduler")
    findings.require(scheduler.get("DEMO_MODE") == "false", "scheduler.DEMO_MODE", "must be false")
    findings.require(scheduler.get("AUTH_REQUIRED") == "true", "scheduler.AUTH_REQUIRED", "must be true")
    findings.require(scheduler.get("RADAR_AUTH_MODE") == "jwt", "scheduler.RADAR_AUTH_MODE", "must be jwt")
    findings.require(valid_database_dsn(scheduler.get("DATABASE_URL")), "scheduler.DATABASE_URL.tls", "must be a TLS PostgreSQL DSN")
    findings.require(valid_https(scheduler.get("R2_ENDPOINT_URL")), "scheduler.R2_ENDPOINT_URL.https", "must be HTTPS")
    findings.require(scheduler.get("REDIS_URL", "").startswith("rediss://"), "scheduler.REDIS_URL.tls", "must use rediss://")
    findings.require(scheduler.get("SOURCE_IDENTITIES_FILE") == "/run/config/source_identities.json", "scheduler.SOURCE_IDENTITIES_FILE", "must use the reviewed read-only mount")
    findings.require(scheduler.get("RSS_FEEDS_FILE") == "/run/config/feeds.json", "scheduler.RSS_FEEDS_FILE", "must use the reviewed read-only mount")
    findings.require(
        all(
            scheduler.get(key) == api.get(key)
            for key in ("RADAR_JWT_PUBLIC_KEYS", "RADAR_JWT_ISSUER", "RADAR_JWT_AUDIENCE")
        ),
        "services.jwtTrust",
        "API and Scheduler must use the same JWT trust bundle",
    )

    require_keys(alert, {
        "DATABASE_URL", "REDIS_URL", "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY", "RAW_EVIDENCE_BUCKET", "WEBHOOK_SIGNING_SECRET",
        "RADAR_WORKSPACE_IDS", "RADAR_INSTANCE_ID",
    }, findings, "alert")
    findings.require(alert.get("DEMO_MODE") == "false", "alert.DEMO_MODE", "must be false")
    findings.require(valid_database_dsn(alert.get("DATABASE_URL")), "alert.DATABASE_URL.tls", "must be a TLS PostgreSQL DSN")
    findings.require(valid_https(alert.get("R2_ENDPOINT_URL")), "alert.R2_ENDPOINT_URL.https", "must be HTTPS")
    findings.require(alert.get("REDIS_URL", "").startswith("rediss://"), "alert.REDIS_URL.tls", "must use rediss://")
    findings.warn(bool(alert.get("R2_SESSION_TOKEN")), "alert.R2_SESSION_TOKEN", "use a distinct short-lived bucket-scoped Alert identity; R2 does not expose a native delete-only grant")
    if phase == "formal":
        findings.require(bool(alert.get("R2_SESSION_TOKEN")), "alert.R2_SESSION_TOKEN.formal", "formal collection requires short-lived bucket-scoped Alert credentials; the isolated code path is delete-only")
        findings.require(is_real(scheduler.get("GITHUB_TOKEN")), "scheduler.GITHUB_TOKEN.formal", "formal GitHub collection requires an approved token")
        findings.require(
            bool(re.fullmatch(r"[^@\s]+@[^@\s]+", scheduler.get("OPENALEX_MAILTO", ""))),
            "scheduler.OPENALEX_MAILTO.formal",
            "formal OpenAlex collection requires a contact email",
        )

    app_identities = [database_identity(values.get("DATABASE_URL")) for values in services.values()]
    findings.require(
        all(identity[0] == "radar_app" for identity in app_identities),
        "services.databaseRole",
        "API, Scheduler and Alert DATABASE_URL must use radar_app",
    )
    findings.require(
        len({identity[1:] for identity in app_identities}) == 1,
        "services.databaseTarget",
        "all runtime services must use the same database target",
    )
    findings.require(
        database_identity(api.get("DELETION_DATABASE_URL"))[0] == "radar_deletion_worker",
        "api.deletionDatabaseRole",
        "DELETION_DATABASE_URL must use radar_deletion_worker",
    )
    findings.require(
        database_identity(api.get("DELETION_DATABASE_URL"))[1:] == app_identities[0][1:],
        "api.deletionDatabaseTarget",
        "deletion and application identities must use the same database target",
    )
    findings.require(
        scheduler.get("R2_ENDPOINT_URL") == alert.get("R2_ENDPOINT_URL")
        and scheduler.get("RAW_EVIDENCE_BUCKET") == alert.get("RAW_EVIDENCE_BUCKET"),
        "services.r2Target",
        "Scheduler and Alert must address the same evidence bucket",
    )
    findings.require(
        scheduler.get("R2_ACCESS_KEY_ID") != alert.get("R2_ACCESS_KEY_ID"),
        "services.r2IdentitySeparation",
        "Scheduler and Alert must use distinct object-storage identities",
    )

    expected_release = release_digests
    for service_name in ("api", "scheduler"):
        actual = json_object(services[service_name].get("RADAR_RELEASE_IMAGE_DIGESTS"))
        findings.require(actual == expected_release, f"{service_name}.releaseDigests", "must exactly match the release bundle")

    ids = [values.get("RADAR_INSTANCE_ID") for values in services.values()]
    findings.require(len(set(ids)) == 3 and all(value and len(value) >= 8 for value in ids), "services.instanceIds", "must be stable, distinct and at least eight characters")

    forbidden = {
        "api": {"REDIS_URL", "R2_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "YOUTUBE_API_KEY", "WEBHOOK_SIGNING_SECRET"},
        "scheduler": {"DELETION_DATABASE_URL", "WEBHOOK_SIGNING_SECRET", "SCORE_LEDGER_ED25519_PRIVATE_KEY"},
        "alert": {"DELETION_DATABASE_URL", "GITHUB_TOKEN", "YOUTUBE_API_KEY", "X_BEARER_TOKEN", "SCORE_LEDGER_ED25519_PRIVATE_KEY"},
    }
    for service_name, keys in forbidden.items():
        for key in sorted(keys):
            findings.require(not services[service_name].get(key), f"{service_name}.isolation.{key}", "credential must not be present")

    connector_budgets = positive_map(scheduler.get("CONNECTOR_BUDGETS_RMB_JSON"))
    family_budgets = positive_map(scheduler.get("SIGNAL_FAMILY_BUDGETS_RMB_JSON"))
    try:
        global_budget = float(scheduler.get("EXTERNAL_DATA_BUDGET_RMB", ""))
        global_budget_ok = 0 < global_budget < float("inf")
    except ValueError:
        global_budget, global_budget_ok = 0.0, False
    findings.require(global_budget_ok, "scheduler.EXTERNAL_DATA_BUDGET_RMB.value", "must be finite and positive")
    findings.require(sum(connector_budgets.values()) <= global_budget, "scheduler.connectorBudgets.total", "connector allocations exceed the global budget")
    findings.require(sum(family_budgets.values()) <= global_budget, "scheduler.familyBudgets.total", "signal-family allocations exceed the global budget")
    try:
        api_global_budget = float(api.get("EXTERNAL_DATA_BUDGET_RMB", ""))
    except ValueError:
        api_global_budget = -1
    findings.require(
        api_global_budget == global_budget
        and json_object(api.get("CONNECTOR_BUDGETS_RMB_JSON"))
        == json_object(scheduler.get("CONNECTOR_BUDGETS_RMB_JSON"))
        and json_object(api.get("SIGNAL_FAMILY_BUDGETS_RMB_JSON"))
        == json_object(scheduler.get("SIGNAL_FAMILY_BUDGETS_RMB_JSON")),
        "services.budgetPolicy",
        "API reporting and Scheduler enforcement must use identical budget limits",
    )
    metered = {"github": "behavior", "huggingface": "behavior", "openalex": "research"}
    if scheduler.get("YOUTUBE_API_KEY"):
        metered["youtube"] = "behavior"
    for connector, family in metered.items():
        findings.require(connector in connector_budgets, f"scheduler.budget.{connector}", "positive connector budget is required")
        findings.require(family in family_budgets, f"scheduler.budget.family.{family}", "positive signal-family budget is required")
        cost_key = f"CONNECTOR_COST_RMB_{connector.upper()}"
        try:
            cost = float(scheduler.get(cost_key, ""))
            cost_ok = 0 <= cost < float("inf")
        except ValueError:
            cost_ok = False
        findings.require(cost_ok, f"scheduler.{cost_key}", "contract cost per request must be explicit and non-negative")


def validate_registries(
    identities_path: Path,
    feeds_path: Path,
    connector_registry_path: Path,
    rights_path: Path,
    keyring_path: Path,
    product_policy_path: Path,
    phase: str,
    findings: Findings,
) -> None:
    for name, path in (("sourceIdentities", identities_path), ("rssFeeds", feeds_path)):
        findings.require(path.is_file(), f"{name}.file", "file is missing")
    if identities_path.is_file():
        try:
            identities = json.loads(identities_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            identities = None
        valid = isinstance(identities, list) and bool(identities)
        findings.require(valid, "sourceIdentities.json", "must be a non-empty JSON array")
        if valid:
            ids = [str(row.get("sourceId", "")) for row in identities if isinstance(row, dict)]
            findings.require(len(ids) == len(identities) == len(set(ids)) and all(is_real(item) for item in ids), "sourceIdentities.unique", "sourceId values must be real and unique")
    if feeds_path.is_file():
        try:
            feeds = json.loads(feeds_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            feeds = None
        valid = isinstance(feeds, list) and bool(feeds)
        findings.require(valid, "rssFeeds.json", "must be a non-empty JSON array")
        if valid:
            source_ids = [str(row.get("sourceId", "")) for row in feeds if isinstance(row, dict)]
            urls = [str(row.get("url", "")) for row in feeds if isinstance(row, dict)]
            families = [row.get("signalFamily") for row in feeds if isinstance(row, dict)]
            findings.require(len(source_ids) == len(feeds) == len(set(source_ids)) and all(is_real(item) for item in source_ids), "rssFeeds.sources", "sourceId values must be real and unique")
            findings.require(len(urls) == len(feeds) == len(set(urls)) and all(valid_https(item) for item in urls), "rssFeeds.urls", "feed URLs must be unique credential-free HTTPS URLs")
            findings.require(all(item in {"official", "discussion", "research"} for item in families), "rssFeeds.signalFamilies", "unsupported signal family")
            if phase == "formal":
                for index, row in enumerate(feeds):
                    approval = row if isinstance(row, dict) else {}
                    findings.require(
                        str(approval.get("approvalReference", "")).startswith("approved-"),
                        f"rssFeeds.{index}.approvalReference",
                        "each publisher feed needs an approved ticket reference",
                    )
                    findings.require(
                        bool(DIGEST.fullmatch(str(approval.get("approvalEvidenceDigest", "")))),
                        f"rssFeeds.{index}.approvalEvidenceDigest",
                        "each publisher feed needs a signed approval SHA-256",
                    )

    try:
        registry_payload = json.loads(connector_registry_path.read_text(encoding="utf-8"))
        connectors = {str(row["id"]): row for row in registry_payload["connectors"]}
        rights_payload = json.loads(rights_path.read_text(encoding="utf-8"))
        policies = rights_payload["policies"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        connectors, policies = {}, {}
    findings.require(bool(connectors) and bool(policies), "rights.registries", "connector and rights registries must be readable")
    if phase == "formal":
        for connector_id in FORMAL_CONNECTORS:
            row = connectors.get(connector_id, {})
            findings.require(row.get("rightsStatus") == "active", f"rights.{connector_id}.status", "formal connector must be active")
            policy = policies.get(row.get("rightsPolicyId"), {}) if isinstance(row, dict) else {}
            approval = policy.get("legalApproval") if isinstance(policy, dict) else None
            findings.require(isinstance(approval, str) and approval.startswith("approved-"), f"rights.{connector_id}.approval", "signed approval reference is required")
            evidence_digest = policy.get("approvalEvidenceDigest") if isinstance(policy, dict) else None
            findings.require(
                isinstance(evidence_digest, str) and bool(DIGEST.fullmatch(evidence_digest)),
                f"rights.{connector_id}.approvalEvidenceDigest",
                "signed approval evidence SHA-256 is required",
            )
            approved_at = policy.get("approvedAt") if isinstance(policy, dict) else None
            findings.require(
                timezone_timestamp(approved_at),
                f"rights.{connector_id}.approvedAt",
                "timezone-aware approval time is required",
            )
            reviewers = policy.get("approvalReviewers") if isinstance(policy, dict) else None
            reviewer_values = list(reviewers.values()) if isinstance(reviewers, dict) else []
            findings.require(
                isinstance(reviewers, dict)
                and set(reviewers) == {"dataRights", "security", "product"}
                and all(isinstance(value, str) and is_real(value) for value in reviewer_values)
                and len(reviewer_values) == len(set(reviewer_values)) == 3,
                f"rights.{connector_id}.approvalReviewers",
                "three distinct data-rights, security and product approvers are required",
            )
    else:
        pending = [item for item in FORMAL_CONNECTORS if connectors.get(item, {}).get("rightsStatus") != "active"]
        findings.warn(not pending, "rights.pending", "formal collection remains blocked for: " + ",".join(pending))

    try:
        keyring_raw = keyring_path.read_bytes()
        keyring = json.loads(keyring_raw)
        policy = json.loads(product_policy_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        keyring_raw, keyring, policy = b"", {}, {}
    populated = valid_acceptance_keyring(keyring)
    expected_digest = "sha256:" + hashlib.sha256(keyring_raw).hexdigest() if keyring_raw else ""
    monitoring = policy.get("acceptanceMonitoring", {}) if isinstance(policy, dict) else {}
    bound = (
        monitoring.get("keyringVersion") == keyring.get("keyringVersion")
        and monitoring.get("keyringDigest") == expected_digest
        and monitoring.get("keyringFrozenAt") == keyring.get("frozenAt")
    )
    if phase == "formal":
        findings.require(populated, "acceptanceKeyring.populated", "all four independent key groups are required")
        findings.require(bound, "acceptanceKeyring.bound", "product policy must bind the exact keyring")
    else:
        findings.warn(populated and bound, "acceptanceKeyring.pending", "formal 72-hour collection cannot start")


def run(args: argparse.Namespace) -> dict[str, object]:
    findings = Findings()
    release_path = args.release_env.resolve()
    secure_file(release_path, findings, "releaseEnv")
    try:
        release = parse_env(release_path)
    except (OSError, ValueError) as exc:
        findings.errors.append(f"releaseEnv.parse: {exc}")
        return findings.payload(args.phase)
    release_digests = validate_release(release, findings)
    services: dict[str, dict[str, str]] = {}
    for name, key in SERVICE_FILES.items():
        path = bundle_path(release.get(key), release_path)
        secure_file(path, findings, f"{name}Env")
        try:
            services[name] = parse_env(path)
        except (OSError, ValueError) as exc:
            findings.errors.append(f"{name}Env.parse: {exc}")
            services[name] = {}
    validate_services(
        services, release_digests, args.phase,
        release.get("RADAR_PUBLIC_WEB_URL", ""), findings,
    )
    validate_registries(
        bundle_path(release.get("RADAR_SOURCE_IDENTITIES_FILE"), release_path),
        bundle_path(release.get("RADAR_RSS_FEEDS_FILE"), release_path),
        args.connector_registry,
        args.rights_policies,
        args.acceptance_keyring,
        args.product_policy,
        args.phase,
        findings,
    )
    return findings.payload(args.phase)


def parser() -> argparse.ArgumentParser:
    root = Path(__file__).resolve().parents[1]
    result = argparse.ArgumentParser(description="Validate a rendered AI Hot Radar production bundle")
    result.add_argument("--release-env", type=Path, required=True)
    result.add_argument("--phase", choices=("bootstrap", "formal"), default="bootstrap")
    result.add_argument("--connector-registry", type=Path, default=root / "config" / "connector_registry.json")
    result.add_argument("--rights-policies", type=Path, default=root / "config" / "rights_policies.json")
    result.add_argument("--acceptance-keyring", type=Path, default=root / "config" / "acceptance_monitor_public_keys.json")
    result.add_argument("--product-policy", type=Path, default=root / "config" / "product_metric_policy.json")
    result.add_argument("--output", type=Path)
    return result


def main() -> int:
    args = parser().parse_args()
    payload = run(args)
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if payload["qualifies"] is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
