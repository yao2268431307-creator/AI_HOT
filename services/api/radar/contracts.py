from __future__ import annotations

from datetime import datetime, timezone
from enum import StrEnum
import ipaddress
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, Field, HttpUrl, field_validator, model_validator


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


ProvenanceLevel = Literal["self_authenticating", "provider_verified", "unverified_discovery"]


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
    available_at: datetime | None = Field(default=None, alias="availableAt")
    availability_basis: Literal["provider_timestamp", "first_detected"] = Field(default="first_detected", alias="availabilityBasis")
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
    provenance_level: ProvenanceLevel = Field(default="provider_verified", alias="provenanceLevel")

    model_config = {"populate_by_name": True}

    @field_validator("published_at", "available_at", "collected_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value

    @model_validator(mode="after")
    def normalize_availability(self) -> Observation:
        if self.available_at is None:
            self.available_at = self.collected_at
            self.availability_basis = "first_detected"
        if self.available_at > self.collected_at:
            raise ValueError("availableAt cannot be after collectedAt")
        return self

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

    @model_validator(mode="after")
    def unverified_discovery_has_no_metrics(self) -> Observation:
        if self.provenance_level == "unverified_discovery" and self.metrics:
            raise ValueError("unverified discovery observations cannot carry metric snapshots")
        return self


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
    provenance_level: ProvenanceLevel = Field(default="provider_verified", alias="provenanceLevel")
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
    provenance_level: ProvenanceLevel = Field(default="provider_verified", alias="provenanceLevel")

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
    # Internal optimistic-concurrency token. It is populated by durable
    # repositories and never serialized into API or score payloads.
    storage_revision: int = Field(default=0, exclude=True, ge=0)
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
    classification_status: Literal["supported", "unsupported"] = Field(default="supported", alias="classificationStatus")
    unsupported_reason: str | None = Field(default=None, alias="unsupportedReason")
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
    new_evidence_count: int = Field(default=0, alias="newEvidenceCount", ge=0)
    queue_priority_score: float = Field(default=0, alias="queuePriorityScore", ge=0)
    queue_priority_reasons: list[str] = Field(default_factory=list, alias="queuePriorityReasons")
    review_anchor_at: datetime | None = Field(default=None, alias="reviewAnchorAt")
    driver: str
    coverage_note: str = Field(alias="coverageNote")
    timeline: list[MetricPoint]
    evidence: list[Evidence]
    score_version: str = Field(default="score-0.7.0", alias="scoreVersion")
    threshold_version: str = Field(default="thresholds-2026-07-rc3", alias="thresholdVersion")

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
    rights_status: Literal["active", "pending", "experimental", "blocked"] = Field(default="pending", alias="rightsStatus")

    model_config = {"populate_by_name": True}


class RadarPayload(BaseModel):
    generated_at: datetime = Field(alias="generatedAt")
    data_mode: Literal["live", "recorded_demo"] = Field(alias="dataMode")
    window: str
    events: list[RadarEvent]
    connectors: list[ConnectorStatus]
    total_events: int = Field(alias="totalEvents", ge=0)
    limit: int = Field(ge=1, le=500)
    has_more: bool = Field(alias="hasMore")

    model_config = {"populate_by_name": True}


class FeedbackRequest(BaseModel):
    event_id: str = Field(alias="eventId")
    action: Literal["confirm", "reject", "observe", "merge", "split", "ignore"]
    reason: str = Field(min_length=3, max_length=2000)
    target_event_id: str | None = Field(default=None, alias="targetEventId")
    queue_eligibility_key: str | None = Field(default=None, alias="queueEligibilityKey", min_length=8, max_length=128)
    alert_delivery_key: str | None = Field(default=None, alias="alertDeliveryKey", min_length=8, max_length=256)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def require_explicit_decision_context(self) -> FeedbackRequest:
        if self.action in {"confirm", "reject", "observe"} and not self.queue_eligibility_key:
            raise ValueError("triage feedback requires the queueEligibilityKey captured when detail opened")
        return self


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


class MetricIncidentRequest(BaseModel):
    event_id: str = Field(alias="eventId", min_length=3, max_length=300)
    target_key: str = Field(alias="targetKey", min_length=8, max_length=300)
    canonical_key: str = Field(alias="canonicalKey", min_length=8, max_length=300)
    cause: Literal["worker_retry_after_timeout", "delivery_ack_race", "provider_duplicate_callback"]
    note: str = Field(min_length=3, max_length=1000)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def different_delivery_keys(self) -> MetricIncidentRequest:
        if self.target_key == self.canonical_key:
            raise ValueError("targetKey and canonicalKey must differ")
        return self


class ProductInteractionRequest(BaseModel):
    kind: Literal[
        "detail_opened",
        "evidence_opened",
        "triage_submitted",
        "review_segment_closed",
        "review_heartbeat",
        "watch_toggled",
        "alert_acknowledged",
        "queue_eligible",
        "alert_quality_reviewed",
        "metric_exclusion_recorded",
        "metric_exclusion_reinstated",
    ]
    idempotency_key: str = Field(alias="idempotencyKey", min_length=8, max_length=128)
    session_id: str = Field(alias="sessionId", min_length=8, max_length=128)
    event_id: str | None = Field(default=None, alias="eventId", max_length=300)
    metadata: dict[str, str | float | bool] = Field(default_factory=dict, max_length=20)

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def validate_metric_metadata(self) -> ProductInteractionRequest:
        metadata = self.metadata
        if self.kind == "triage_submitted":
            if metadata.get("action") not in {"confirm", "reject", "observe"}:
                raise ValueError("triage_submitted requires a valid action")
            if metadata.get("measurementVersion") == "server-heartbeat-v2":
                required_v2 = {
                    "feedbackId", "measurementVersion", "idleTimeoutSeconds", "tickCapSeconds",
                    "reviewAttemptId", "queueEligibilityKey", "segmentId",
                }
                if not required_v2.issubset(metadata):
                    raise ValueError("server-timed triage requires the complete timing envelope")
                if not all(isinstance(metadata.get(key), str) and len(str(metadata[key])) >= 8 for key in ("feedbackId", "reviewAttemptId", "queueEligibilityKey", "segmentId")):
                    raise ValueError("server-timed triage identifiers are invalid")
                return self
            timing_keys = {
                "feedbackId", "activeSeconds", "externalWaitSeconds", "measurementVersion",
                "idleTimeoutSeconds", "tickCapSeconds", "reviewAttemptId", "queueEligibilityKey",
            }
            if timing_keys & metadata.keys():
                if not timing_keys.issubset(metadata):
                    raise ValueError("timed triage requires the complete timing envelope")
                if not isinstance(metadata.get("feedbackId"), str) or len(str(metadata["feedbackId"])) < 8:
                    raise ValueError("timed triage requires feedbackId")
                if not all(isinstance(metadata.get(key), str) and len(str(metadata[key])) >= 8 for key in ("reviewAttemptId", "queueEligibilityKey")):
                    raise ValueError("timed triage requires reviewAttemptId and queueEligibilityKey")
                if metadata.get("measurementVersion") not in {"foreground-active-v1", "server-heartbeat-v2"}:
                    raise ValueError("timed triage requires a supported measurement version")
                if metadata.get("measurementVersion") == "server-heartbeat-v2" and (
                    not isinstance(metadata.get("segmentId"), str) or len(str(metadata["segmentId"])) < 8
                ):
                    raise ValueError("server-timed triage requires segmentId")
                for key in ("activeSeconds", "externalWaitSeconds"):
                    value = metadata.get(key)
                    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= float(value) <= 28800:
                        raise ValueError(f"{key} must be between 0 and 28800")
                for key in ("idleTimeoutSeconds", "tickCapSeconds"):
                    value = metadata.get(key)
                    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 1 <= float(value) <= 600:
                        raise ValueError(f"{key} must be between 1 and 600")
        elif self.kind == "review_segment_closed":
            if metadata.get("measurementVersion") == "server-heartbeat-v2":
                required_v2 = {
                    "reviewAttemptId", "queueEligibilityKey", "segmentId", "measurementVersion",
                    "idleTimeoutSeconds", "tickCapSeconds",
                }
                if not self.event_id or not required_v2.issubset(metadata):
                    raise ValueError("server-timed review segment requires the complete envelope")
                if not all(isinstance(metadata.get(key), str) and len(str(metadata[key])) >= 8 for key in ("reviewAttemptId", "queueEligibilityKey", "segmentId")):
                    raise ValueError("server-timed review segment identifiers are invalid")
                return self
            required = {
                "reviewAttemptId", "queueEligibilityKey", "activeSeconds", "externalWaitSeconds",
                "measurementVersion", "idleTimeoutSeconds", "tickCapSeconds",
            }
            if not self.event_id or not required.issubset(metadata):
                raise ValueError("review_segment_closed requires a linked review attempt and timing fields")
            if not all(isinstance(metadata.get(key), str) and len(str(metadata[key])) >= 8 for key in ("reviewAttemptId", "queueEligibilityKey")):
                raise ValueError("review segment identifiers are invalid")
            if metadata.get("measurementVersion") not in {"foreground-active-v1", "server-heartbeat-v2"}:
                raise ValueError("review segment requires a supported measurement version")
            if metadata.get("measurementVersion") == "server-heartbeat-v2" and (
                not isinstance(metadata.get("segmentId"), str) or len(str(metadata["segmentId"])) < 8
            ):
                raise ValueError("server-timed review segment requires segmentId")
            for key in ("activeSeconds", "externalWaitSeconds"):
                value = metadata.get(key)
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= float(value) <= 28800:
                    raise ValueError(f"{key} must be between 0 and 28800")
        elif self.kind == "review_heartbeat":
            required = {
                "reviewAttemptId", "queueEligibilityKey", "segmentId", "sequence",
                "state", "measurementVersion", "idleTimeoutSeconds", "tickCapSeconds",
            }
            if not self.event_id or not required.issubset(metadata):
                raise ValueError("review heartbeat requires the complete server-timed envelope")
            if not all(isinstance(metadata.get(key), str) and len(str(metadata[key])) >= 8 for key in ("reviewAttemptId", "queueEligibilityKey", "segmentId")):
                raise ValueError("review heartbeat identifiers are invalid")
            if metadata.get("measurementVersion") != "server-heartbeat-v2":
                raise ValueError("review heartbeat requires server-heartbeat-v2")
            sequence = metadata.get("sequence")
            if isinstance(sequence, bool) or not isinstance(sequence, (int, float)) or int(sequence) != sequence or not 0 <= int(sequence) <= 100000:
                raise ValueError("review heartbeat sequence is invalid")
            if metadata.get("state") not in {"active", "idle", "external_wait"}:
                raise ValueError("review heartbeat state is invalid")
        elif self.kind == "queue_eligible":
            if not self.event_id or not isinstance(metadata.get("eligibilityKey"), str) or len(str(metadata["eligibilityKey"])) < 8:
                raise ValueError("queue_eligible requires eventId and eligibilityKey")
            if not isinstance(metadata.get("policyVersion"), str):
                raise ValueError("queue_eligible requires policyVersion")
        elif self.kind == "alert_quality_reviewed":
            if not self.event_id or not isinstance(metadata.get("alertDeliveryKey"), str) or len(str(metadata["alertDeliveryKey"])) < 8:
                raise ValueError("alert_quality_reviewed requires eventId and alertDeliveryKey")
            if metadata.get("verdict") not in {"valid", "insufficient_evidence", "incorrect_cluster", "out_of_scope"}:
                raise ValueError("alert_quality_reviewed requires a supported verdict")
            if not isinstance(metadata.get("reason"), str) or not 3 <= len(str(metadata["reason"]).strip()) <= 1000:
                raise ValueError("alert_quality_reviewed requires a review reason")
        elif self.kind in {"metric_exclusion_recorded", "metric_exclusion_reinstated"}:
            if not self.event_id:
                raise ValueError("metric exclusion requires eventId")
            if metadata.get("targetType") not in {"alert", "queue"}:
                raise ValueError("metric exclusion requires targetType alert or queue")
            if not isinstance(metadata.get("targetKey"), str) or len(str(metadata["targetKey"])) < 8:
                raise ValueError("metric exclusion requires targetKey")
            allowed_reasons = (
                {"system_fault_duplicate"}
                if self.kind == "metric_exclusion_recorded" else {"operator_correction"}
            )
            if metadata.get("reason") not in allowed_reasons:
                raise ValueError("metric exclusion requires an auditable operational reason")
            if self.kind == "metric_exclusion_recorded":
                if not isinstance(metadata.get("canonicalKey"), str) or len(str(metadata["canonicalKey"])) < 8:
                    raise ValueError("duplicate exclusions require the earlier canonicalKey")
                if not isinstance(metadata.get("incidentId"), str) or len(str(metadata["incidentId"])) < 8:
                    raise ValueError("duplicate exclusions require an immutable incidentId")
            if not isinstance(metadata.get("note"), str) or not 3 <= len(str(metadata["note"]).strip()) <= 1000:
                raise ValueError("metric exclusion requires an audit note")
        return self


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
    scoring_revision: int = Field(default=1, ge=1)
    baseline_version: str = "baseline-empty"
    baseline_digest: str = "sha256:empty"
    feature_registry_version: str = "unknown"
    feature_registry_digest: str = "sha256:unknown"
    evidence_policy_version: str = "unknown"
    label_policy_version: str = "unknown"
    cluster_version: int = Field(default=1, ge=1)
    identity_version: str = "identity-account-fallback-v1"
    input_observation_ids: list[str] = Field(default_factory=list)
    input_from: datetime
    input_to: datetime
    input_digest: str
    drivers: list[str]
    payload: dict[str, Any]
    created_at: datetime
