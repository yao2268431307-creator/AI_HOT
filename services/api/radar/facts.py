from __future__ import annotations

import hashlib
import json
import os
from functools import lru_cache
from pathlib import Path

from .contracts import ContentObservation, MetricSnapshot, Observation
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
    for policy_id, policy in policies.items():
        if not isinstance(policy_id, str) or not isinstance(policy, dict):
            raise ValueError("rights policy configuration is invalid")
        value = policy.get("excerptMaxCharacters")
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 1_000:
            raise ValueError(f"rights policy {policy_id} has an invalid excerpt limit")
        limits[policy_id] = value
    return limits


def rights_excerpt_limit(policy_id: str) -> int:
    limits = _rights_excerpt_limits(os.getenv("RIGHTS_POLICIES_PATH", ""))
    if policy_id not in limits:
        raise ValueError(f"unknown rights policy: {policy_id}")
    return limits[policy_id]


def split_observation(observation: Observation, parser_version: str = "parser-1", rights_policy_id: str | None = None) -> tuple[ContentObservation, list[MetricSnapshot]]:
    effective_policy_id = rights_policy_id or observation.rights_policy_id
    excerpt_limit = rights_excerpt_limit(effective_policy_id)
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
