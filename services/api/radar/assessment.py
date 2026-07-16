from __future__ import annotations

from .contracts import (
    CoverageGap,
    DecisionReason,
    Estimate,
    EventAssessment,
    EventType,
    EvidenceState,
    RadarEvent,
)


BEHAVIOR_KIND = {
    EventType.MODEL_RELEASE: "downloads_derivatives_and_repository_adoption",
    EventType.DEVELOPER_TOOL_RELEASE: "stars_packages_dependencies_and_contributors",
    EventType.RESEARCH_OR_BENCHMARK: "reproduction_citations_and_benchmark_reuse",
    EventType.OFFICIAL_PRODUCT_RELEASE: "independent_product_or_content_adoption",
    EventType.SECURITY_INCIDENT: "remediation_and_mitigation_adoption",
}

FEATURE_WEIGHTS = {"discussion": .30, "behavior": .35, "official": .20, "research": .15}


def _estimate(value: float, uncertainty: float) -> Estimate:
    half_width = max(4.0, uncertainty * .35)
    return Estimate(point=value, low=max(0, value - half_width), high=min(100, value + half_width))


def event_assessment(event: RadarEvent) -> EventAssessment:
    observed_kinds = set(event.signal_families) | {item.kind for item in event.evidence}
    mask: dict[str, EvidenceState] = {}
    gaps: list[CoverageGap] = []
    for feature in FEATURE_WEIGHTS:
        state = EvidenceState.OBSERVED if feature in observed_kinds else EvidenceState.MISSING
        if feature == "discussion":
            state = event.discussion_evidence_state
            if state == EvidenceState.MISSING and "discussion" in observed_kinds and event.attention > 0:
                state = EvidenceState.OBSERVED
        if feature == "behavior":
            # A platform can be categorized as a behavior source while carrying
            # no metric valid for this event type. The processor's explicit mask
            # is authoritative; the fallback only supports recorded demo events
            # created before the field existed.
            state = event.behavior_evidence_state
            if state == EvidenceState.MISSING and "behavior" in observed_kinds and event.behavior > 0:
                state = EvidenceState.OBSERVED
        mask[feature] = state
        if state == EvidenceState.MISSING:
            gaps.append(CoverageGap(
                feature=feature, state=state, reason=f"当前事件没有可信的 {feature} 信号",
                impact="降低证据强度；该特征不会被其他特征重新加权替代",
            ))

    observed_weight = sum(weight for feature, weight in FEATURE_WEIGHTS.items() if mask[feature] == EvidenceState.OBSERVED)
    expected_weight = sum(weight for feature, weight in FEATURE_WEIGHTS.items() if mask[feature] != EvidenceState.NOT_APPLICABLE)
    baseline_maturity = min(1.0, max(0.0, (len(event.timeline) - 1) / 12))
    cluster_confidence = min(1.0, event.diversity / 100 * .65 + min(1, len(event.evidence) / 5) * .35)
    cautions = [gap.reason for gap in gaps]
    behavior_estimate = None if mask["behavior"] != EvidenceState.OBSERVED else _estimate(event.behavior, event.uncertainty)
    return EventAssessment(
        eventId=event.id,
        clusterVersion=event.cluster_version,
        eventType=event.event_type,
        lifecycleState=event.state,
        structureLabels=event.labels,
        attentionEstimate=_estimate(event.attention, event.uncertainty) if mask["discussion"] == EvidenceState.OBSERVED else None,
        behaviorEstimate=behavior_estimate,
        behaviorKind=BEHAVIOR_KIND[event.event_type] if behavior_estimate else None,
        diversityEstimate=_estimate(event.diversity, event.uncertainty),
        authorityEstimate=_estimate(event.authority, event.uncertainty),
        coordinationRisk=_estimate(event.coordination_risk, event.uncertainty),
        coverage=event.coverage,
        evidenceStrength=event.evidence_strength,
        uncertainty=event.uncertainty,
        evidenceMask=mask,
        observedFeatureWeight=round(observed_weight, 3),
        expectedFeatureWeight=round(expected_weight, 3),
        sampleSize=event.independent_sources,
        baselineMaturity=round(baseline_maturity, 3),
        clusterConfidence=round(cluster_confidence, 3),
        decisionReason=DecisionReason(summary=event.driver, drivers=[part for part in event.driver.split("；") if part], cautions=cautions),
        missingEvidence=gaps,
        scoringVersion=event.score_version,
        baselineVersion="baseline-mad-0.2.0",
        observedAt=event.updated_at,
    )
