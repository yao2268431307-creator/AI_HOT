from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path

from .contracts import ContentObservation, MetricSnapshot, Observation
from .feature_registry import load_feature_registry
from .normalize import content_fingerprint, normalize_url, sanitize_external_text


@lru_cache(maxsize=4)
def _rights_excerpt_limits(configured_path: str) -> dict[str, int]:
    path = (
        Path(configured_path)
        if configured_path
        else Path(__file__).resolve().parents[3] / "config" / "rights_policies.json"
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    policies = payload.get("policies") if isinstance(payload, dict) else None
    if not isinstance(policies, dict) or not policies:
        raise ValueError("rights policy configuration has no policies")
    limits: dict[str, int] = {}
    required_fields = {
        "rawRetentionDays", "excerptMaxCharacters", "legalApproval", "storedFieldClasses",
        "metricPolicy", "rawPayloadAllowed", "derivedUses", "deletionScope",
    }
    for policy_id, policy in policies.items():
        if not isinstance(policy_id, str) or not isinstance(policy, dict):
            raise ValueError("rights policy configuration is invalid")
        missing = required_fields - set(policy)
        if missing:
            raise ValueError(f"rights policy {policy_id} is missing field-level rules: {sorted(missing)}")
        value = policy.get("excerptMaxCharacters")
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1_000:
            raise ValueError(f"rights policy {policy_id} has an invalid excerpt limit")
        if not isinstance(policy.get("rawRetentionDays"), int) or int(policy["rawRetentionDays"]) < 0:
            raise ValueError(f"rights policy {policy_id} has invalid raw retention")
        if not isinstance(policy.get("storedFieldClasses"), list) or not all(isinstance(item, str) for item in policy["storedFieldClasses"]):
            raise ValueError(f"rights policy {policy_id} has invalid stored field classes")
        if not isinstance(policy.get("rawPayloadAllowed"), bool):
            raise ValueError(f"rights policy {policy_id} has invalid raw payload rule")
        if not isinstance(policy.get("derivedUses"), list) or not all(isinstance(item, str) for item in policy["derivedUses"]):
            raise ValueError(f"rights policy {policy_id} has invalid derived-use rules")
        if not isinstance(policy.get("metricPolicy"), str) or not isinstance(policy.get("deletionScope"), str):
            raise ValueError(f"rights policy {policy_id} has invalid metric/deletion rules")
        limits[policy_id] = value
    return limits


def rights_excerpt_limit(policy_id: str) -> int:
    limits = _rights_excerpt_limits(os.getenv("RIGHTS_POLICIES_PATH", ""))
    if policy_id not in limits:
        raise ValueError(f"unknown rights policy: {policy_id}")
    return limits[policy_id]


def raw_payload_allowed(policy_id: str) -> bool:
    configured = os.getenv("RIGHTS_POLICIES_PATH", "")
    _rights_excerpt_limits(configured)
    path = Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "rights_policies.json"
    return bool(json.loads(path.read_text(encoding="utf-8"))["policies"][policy_id]["rawPayloadAllowed"])


def metric_policy(policy_id: str) -> str:
    configured = os.getenv("RIGHTS_POLICIES_PATH", "")
    _rights_excerpt_limits(configured)
    path = Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "rights_policies.json"
    policy = str(json.loads(path.read_text(encoding="utf-8"))["policies"][policy_id]["metricPolicy"])
    if policy not in {"none", "registered_numeric_metrics_only", "no_metrics_until_provider_verified"}:
        raise ValueError(f"rights policy {policy_id} has an unsupported metric policy")
    return policy


def _registered_metric_names() -> set[str]:
    registry = load_feature_registry()
    return {str(item["inputMetric"]) for item in registry["features"]}


def split_observation(observation: Observation, parser_version: str = "parser-1", rights_policy_id: str | None = None) -> tuple[ContentObservation, list[MetricSnapshot]]:
    effective_policy_id = rights_policy_id or observation.rights_policy_id
    excerpt_limit = rights_excerpt_limit(effective_policy_id)
    if not raw_payload_allowed(effective_policy_id):
        raise ValueError(f"rights policy {effective_policy_id} forbids raw payload collection")
    effective_metric_policy = metric_policy(effective_policy_id)
    if observation.metrics and effective_metric_policy == "none":
        raise ValueError(f"rights policy {effective_policy_id} forbids metric collection")
    if (
        observation.metrics
        and effective_metric_policy == "no_metrics_until_provider_verified"
        and observation.provenance_level != "provider_verified"
    ):
        raise ValueError(f"rights policy {effective_policy_id} forbids metrics before provider verification")
    unregistered_metrics = set(observation.metrics) - _registered_metric_names()
    if unregistered_metrics:
        raise ValueError(
            f"rights policy {effective_policy_id} forbids unregistered metrics: {sorted(unregistered_metrics)}"
        )
    fingerprint = observation.content_fingerprint or content_fingerprint(
        observation.title, observation.text, observation.url,
    )
    content = ContentObservation(
        id=observation.id, connector=observation.platform.lower().replace(" ", "-"), platform=observation.platform,
        externalId=observation.external_id,
        accountId=observation.account_id or observation.source_id,
        entityId=observation.entity_id or observation.source_id,
        publishedAt=observation.published_at, collectedAt=observation.collected_at, language=observation.language,
        title=sanitize_external_text(observation.title) if observation.title else None,
        textExcerpt=sanitize_external_text(observation.text)[:excerpt_limit], canonicalUrl=normalize_url(observation.url),
        contentHash=fingerprint, relation=observation.relation, rawRef=observation.raw_evidence_ref,
        parserVersion=parser_version, rightsPolicyId=effective_policy_id,
        provenanceLevel=observation.provenance_level, deletionState="active",
    )
    metrics = [MetricSnapshot(
        id=hashlib.sha256(f"{observation.id}:{name}:{observation.collected_at.isoformat()}".encode()).hexdigest(),
        subjectType="content", subjectId=observation.id, metricName=name, value=value,
        effectiveAt=observation.collected_at, collectedAt=observation.collected_at,
        isEstimated=False, sourceRevision=observation.raw_evidence_ref, connector=content.connector,
    ) for name, value in observation.metrics.items()]
    return content, metrics
