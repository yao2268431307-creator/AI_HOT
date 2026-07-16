from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import ipaddress
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, HttpUrl, field_validator


def _clickable_http_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.netloc or parts.username or parts.password:
        raise ValueError("evidence URLs must be credential-free absolute http(s) URLs")
    host = (parts.hostname or "").lower()
    if host in {"localhost", "localhost.localdomain"} or host.endswith(".local"):
        raise ValueError("local evidence URLs are not allowed")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    if address and (address.is_private or address.is_loopback or address.is_link_local or address.is_reserved):
        raise ValueError("private evidence URLs are not allowed")
    return value


class EventType(StrEnum):
    MODEL_RELEASE = "model_release"
    DEVELOPER_TOOL_RELEASE = "developer_tool_release"
    RESEARCH_OR_BENCHMARK = "research_or_benchmark"
    OFFICIAL_PRODUCT_RELEASE = "official_product_release"
    SECURITY_INCIDENT = "security_incident"


class LifecycleState(StrEnum):
    INSUFFICIENT_DATA = "insufficient_data"
    DETECTED = "detected"
    EMERGING = "emerging"
    ACCELERATING = "accelerating"
    ESTABLISHED = "established"
    COOLING = "cooling"
    DORMANT = "dormant"
    NOISE = "noise"


class StructureLabel(StrEnum):
    CROSS_PLATFORM_CONFIRMED = "cross_platform_confirmed"
    ADOPTION_CONFIRMED = "adoption_confirmed"
    RESPONSE_CONFIRMED = "response_confirmed"
    PLATFORM_CONCENTRATED = "platform_concentrated"
    COORDINATION_RISK = "coordination_risk"
    ATTENTION_BEHAVIOR_GAP = "attention_behavior_gap"
    EXPECTED_BEHAVIOR_LAG = "expected_behavior_lag"
    OFFICIAL_SOURCE_LED = "official_source_led"
    LOW_SOURCE_DIVERSITY = "low_source_diversity"
    REACTIVATED = "reactivated"


class EvidenceState(StrEnum):
    OBSERVED = "observed"
    MISSING = "missing"
    NOT_APPLICABLE = "not_applicable"
    UNTRUSTED = "untrusted"


class EvidenceStrength(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Estimate(BaseModel):
    point: float = Field(ge=0, le=100)
    low: float = Field(ge=0, le=100)
    high: float = Field(ge=0, le=100)


class CoverageGap(BaseModel):
    feature: str
    state: EvidenceState
    reason: str
    impact: str


class DecisionReason(BaseModel):
    summary: str
    drivers: list[str]
    cautions: list[str] = Field(default_factory=list)


class Observation(BaseModel):
    id: str = Field(max_length=300)
    platform: str = Field(max_length=100)
    external_id: str = Field(alias="externalId", max_length=500)
    source_id: str = Field(alias="sourceId", max_length=500)
    # accountId is the platform account; entityId is the resolved person or
    # organisation that may own several accounts. Keeping both prevents a
    # marketing matrix from being counted as independent discussion.
    account_id: str | None = Field(default=None, alias="accountId")
    entity_id: str | None = Field(default=None, alias="entityId")
    published_at: datetime = Field(alias="publishedAt")
    collected_at: datetime = Field(alias="collectedAt")
    language: Literal["zh", "en", "other"]
    title: str | None = Field(default=None, max_length=1000)
    text: str = Field(max_length=50_000)
    url: str
    metrics: dict[str, float] = Field(default_factory=dict)
    raw_evidence_ref: str = Field(alias="rawEvidenceRef")
    rights_policy_id: str = Field(default="metadata-and-excerpt", alias="rightsPolicyId", max_length=120)
    relation: Literal["original", "repost", "quote", "unknown"] = "unknown"
    content_fingerprint: str | None = Field(default=None, alias="contentFingerprint")
    signal_family: Literal["discussion", "behavior", "official", "research"] = Field(alias="signalFamily")

    model_config = {"populate_by_name": True}

    @field_validator("published_at", "collected_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return _clickable_http_url(value)

    @field_validator("raw_evidence_ref")
    @classmethod
    def validate_raw_evidence_ref(cls, value: str) -> str:
        parts = urlsplit(value)
        if parts.scheme != "r2" or not parts.netloc or not parts.path.lstrip("/") or ".." in parts.path.split("/"):
            raise ValueError("raw evidence references must use r2://bucket/key without traversal")
        return value


class ContentObservation(BaseModel):
    id: str
    schema_version: int = Field(default=1, alias="schemaVersion")
    connector: str
    platform: str
    external_id: str = Field(alias="externalId")
    account_id: str = Field(alias="accountId")
    entity_id: str = Field(alias="entityId")
    published_at: datetime = Field(alias="publishedAt")
    collected_at: datetime = Field(alias="collectedAt")
    language: Literal["zh", "en", "other"]
    title: str | None = None
    text_excerpt: str = Field(alias="textExcerpt", max_length=1000)
    canonical_url: str = Field(alias="canonicalUrl")
    content_hash: str = Field(alias="contentHash")
    relation: Literal["original", "quote", "repost", "reply", "unknown"]
    raw_ref: str | None = Field(default=None, alias="rawRef")
    parser_version: str = Field(alias="parserVersion")
    rights_policy_id: str = Field(alias="rightsPolicyId")
    deletion_state: Literal["active", "tombstoned", "deleted"] = Field(default="active", alias="deletionState")

    model_config = {"populate_by_name": True}

    @field_validator("canonical_url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return _clickable_http_url(value)


class MetricSnapshot(BaseModel):
    id: str
    subject_type: Literal["content", "source", "repository", "model", "event"] = Field(alias="subjectType")
    subject_id: str = Field(alias="subjectId")
    metric_name: str = Field(alias="metricName")
    value: float
    effective_at: datetime = Field(alias="effectiveAt")
    collected_at: datetime = Field(alias="collectedAt")
    is_estimated: bool = Field(default=False, alias="isEstimated")
    source_revision: str | None = Field(default=None, alias="sourceRevision")
    connector: str

    model_config = {"populate_by_name": True}


class Evidence(BaseModel):
    id: str
    source: str
    platform: str
    title: str = Field(max_length=1000)
    url: str
    published_at: datetime = Field(alias="publishedAt")
    kind: Literal["discussion", "behavior", "official", "research"]
    excerpt: str = Field(max_length=2000)

    model_config = {"populate_by_name": True}

    @field_validator("url")
    @classmethod
    def validate_url(cls, value: str) -> str:
        return _clickable_http_url(value)


class MetricPoint(BaseModel):
    at: datetime
    attention: float = Field(ge=0, le=100)
    behavior: float = Field(ge=0, le=100)


class RadarEvent(BaseModel):
    id: str
    narrative_id: str | None = Field(default=None, alias="narrativeId")
    narrative_title: str | None = Field(default=None, alias="narrativeTitle")
    cluster_version: int = Field(default=1, alias="clusterVersion", ge=1)
    parent_cluster_id: str | None = Field(default=None, alias="parentClusterId")
    superseded_by: list[str] = Field(default_factory=list, alias="supersededBy")
    merge_operation_id: str | None = Field(default=None, alias="mergeOperationId")
    split_operation_id: str | None = Field(default=None, alias="splitOperationId")
    effective_at: datetime | None = Field(default=None, alias="effectiveAt")
    title: str
    title_en: str = Field(alias="titleEn")
    event_type: EventType = Field(alias="eventType")
    state: LifecycleState
    labels: list[StructureLabel]
    attention: float = Field(ge=0, le=100)
    behavior: float = Field(ge=0, le=100)
    diversity: float = Field(ge=0, le=100)
    authority: float = Field(ge=0, le=100)
    coordination_risk: float = Field(alias="coordinationRisk", ge=0, le=100)
    coverage: float = Field(ge=0, le=100)
    uncertainty: float = Field(ge=0, le=100)
    evidence_strength: EvidenceStrength = Field(alias="evidenceStrength")
    discussion_evidence_state: EvidenceState = Field(default=EvidenceState.MISSING, alias="discussionEvidenceState")
    behavior_evidence_state: EvidenceState = Field(default=EvidenceState.MISSING, alias="behaviorEvidenceState")
    evidence_score: float = Field(alias="evidenceScore", ge=0, le=100)
    velocity: float
    gap_residual: float = Field(alias="gapResidual")
    first_seen: datetime = Field(alias="firstSeen")
    updated_at: datetime = Field(alias="updatedAt")
    independent_sources: int = Field(alias="independentSources", ge=0)
    platforms: list[str]
    signal_families: list[Literal["discussion", "behavior", "official", "research"]] = Field(default_factory=list, alias="signalFamilies")
    evidence_count: int = Field(default=0, alias="evidenceCount", ge=0)
    driver: str
    coverage_note: str = Field(alias="coverageNote")
    timeline: list[MetricPoint]
    evidence: list[Evidence]
    score_version: str = Field(default="score-0.6.0", alias="scoreVersion")
    threshold_version: str = Field(default="thresholds-2026-07-rc2", alias="thresholdVersion")

    model_config = {"populate_by_name": True}


class EventAssessment(BaseModel):
    event_id: str = Field(alias="eventId")
    cluster_version: int = Field(alias="clusterVersion")
    event_type: EventType = Field(alias="eventType")
    lifecycle_state: LifecycleState = Field(alias="lifecycleState")
    structure_labels: list[StructureLabel] = Field(alias="structureLabels")
    attention_estimate: Estimate | None = Field(default=None, alias="attentionEstimate")
    behavior_estimate: Estimate | None = Field(default=None, alias="behaviorEstimate")
    behavior_kind: str | None = Field(default=None, alias="behaviorKind")
    diversity_estimate: Estimate = Field(alias="diversityEstimate")
    authority_estimate: Estimate = Field(alias="authorityEstimate")
    coordination_risk: Estimate = Field(alias="coordinationRisk")
    coverage: float = Field(ge=0, le=100)
    evidence_strength: EvidenceStrength = Field(alias="evidenceStrength")
    uncertainty: float = Field(ge=0, le=100)
    evidence_mask: dict[str, EvidenceState] = Field(alias="evidenceMask")
    observed_feature_weight: float = Field(alias="observedFeatureWeight", ge=0, le=1)
    expected_feature_weight: float = Field(alias="expectedFeatureWeight", ge=0, le=1)
    sample_size: int = Field(alias="sampleSize", ge=0)
    baseline_maturity: float = Field(alias="baselineMaturity", ge=0, le=1)
    cluster_confidence: float = Field(alias="clusterConfidence", ge=0, le=1)
    decision_reason: DecisionReason = Field(alias="decisionReason")
    missing_evidence: list[CoverageGap] = Field(alias="missingEvidence")
    scoring_version: str = Field(alias="scoringVersion")
    baseline_version: str = Field(alias="baselineVersion")
    observed_at: datetime = Field(alias="observedAt")

    model_config = {"populate_by_name": True}


class ConnectorStatus(BaseModel):
    id: str
    name: str
    family: str
    status: Literal["healthy", "degraded", "paused"]
    latency_minutes: int = Field(alias="latencyMinutes", ge=0)
    observations_24h: int = Field(alias="observations24h", ge=0)
    coverage: float = Field(ge=0, le=100)
    last_success: datetime = Field(alias="lastSuccess")
    note: str
    quota_used: float | None = Field(default=None, alias="quotaUsed", ge=0)
    quota_limit: float | None = Field(default=None, alias="quotaLimit", gt=0)
    cost_rmb_month: float | None = Field(default=None, alias="costRmbMonth", ge=0)
    rights_status: Literal["active", "experimental", "blocked"] = Field(default="active", alias="rightsStatus")

    model_config = {"populate_by_name": True}


class RadarPayload(BaseModel):
    generated_at: datetime = Field(alias="generatedAt")
    window: str
    events: list[RadarEvent]
    connectors: list[ConnectorStatus]

    model_config = {"populate_by_name": True}


class FeedbackRequest(BaseModel):
    event_id: str = Field(alias="eventId")
    action: Literal["confirm", "reject", "observe", "merge", "split", "ignore"]
    reason: str = Field(min_length=3, max_length=2000)
    target_event_id: str | None = Field(default=None, alias="targetEventId")

    model_config = {"populate_by_name": True}


class BehaviorApplicabilityRequest(BaseModel):
    state: Literal["not_applicable", "missing"]
    reason: str = Field(min_length=3, max_length=2000)


class ClusterEditRequest(BaseModel):
    target_event_id: str | None = Field(default=None, alias="targetEventId")
    observation_ids: list[str] = Field(default_factory=list, alias="observationIds")
    reason: str = Field(min_length=3, max_length=2000)

    model_config = {"populate_by_name": True}


class AlertRuleRequest(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    minimum_attention: float = Field(default=70, alias="minimumAttention", ge=0, le=100)
    minimum_evidence_strength: float = Field(default=65, alias="minimumEvidenceStrength", ge=0, le=100)
    webhook_url: HttpUrl | None = Field(default=None, alias="webhookUrl")
    event_types: list[EventType] = Field(default_factory=list, alias="eventTypes")

    model_config = {"populate_by_name": True}


class WatchlistRequest(BaseModel):
    event_id: str = Field(alias="eventId")
    note: str | None = Field(default=None, max_length=500)

    model_config = {"populate_by_name": True}


class WatchlistItem(BaseModel):
    id: str
    event_id: str = Field(alias="eventId")
    note: str | None = None
    created_at: datetime = Field(alias="createdAt")

    model_config = {"populate_by_name": True}


class ProductInteractionRequest(BaseModel):
    kind: Literal["detail_opened", "evidence_opened", "triage_submitted", "watch_toggled", "alert_acknowledged"]
    idempotency_key: str = Field(alias="idempotencyKey", min_length=8, max_length=128)
    session_id: str = Field(alias="sessionId", min_length=8, max_length=128)
    event_id: str | None = Field(default=None, alias="eventId", max_length=300)
    metadata: dict[str, str | float | bool] = Field(default_factory=dict, max_length=20)

    model_config = {"populate_by_name": True}


class MutationReceipt(BaseModel):
    id: str
    accepted: bool = True
    status: Literal["queued", "completed", "rejected"] = "queued"
    operation: str | None = None
    created_at: datetime = Field(alias="createdAt")

    model_config = {"populate_by_name": True}


class StoredScore(BaseModel):
    event_id: str
    score_version: str
    threshold_version: str
    input_from: datetime
    input_to: datetime
    input_digest: str
    drivers: list[str]
    payload: dict[str, Any]
    created_at: datetime
