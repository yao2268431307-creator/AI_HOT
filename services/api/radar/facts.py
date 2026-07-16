from __future__ import annotations

import hashlib

from .contracts import ContentObservation, MetricSnapshot, Observation
from .normalize import content_fingerprint, normalize_url, sanitize_external_text


def split_observation(observation: Observation, parser_version: str = "parser-1", rights_policy_id: str | None = None) -> tuple[ContentObservation, list[MetricSnapshot]]:
    fingerprint = observation.content_fingerprint or content_fingerprint(
        observation.title, observation.text, observation.url,
    )
    content = ContentObservation(
        id=observation.id, connector=observation.platform.lower().replace(" ", "-"), platform=observation.platform,
        externalId=observation.external_id,
        accountId=observation.account_id or observation.source_id,
        entityId=observation.entity_id or observation.source_id,
        publishedAt=observation.published_at, collectedAt=observation.collected_at, language=observation.language,
        title=sanitize_external_text(observation.title) if observation.title else None, textExcerpt=sanitize_external_text(observation.text)[:1000], canonicalUrl=normalize_url(observation.url),
        contentHash=fingerprint, relation=observation.relation, rawRef=observation.raw_evidence_ref,
        parserVersion=parser_version, rightsPolicyId=rights_policy_id or observation.rights_policy_id, deletionState="active",
    )
    metrics = [MetricSnapshot(
        id=hashlib.sha256(f"{observation.id}:{name}:{observation.collected_at.isoformat()}".encode()).hexdigest(),
        subjectType="content", subjectId=observation.id, metricName=name, value=value,
        effectiveAt=observation.collected_at, collectedAt=observation.collected_at,
        isEstimated=False, sourceRevision=observation.raw_evidence_ref, connector=content.connector,
    ) for name, value in observation.metrics.items()]
    return content, metrics
