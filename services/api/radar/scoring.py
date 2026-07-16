from __future__ import annotations

from dataclasses import dataclass, field
from math import isfinite
from statistics import median
from typing import Iterable

from .contracts import EventType, EvidenceStrength, LifecycleState, StructureLabel


SCORE_VERSION = "score-0.6.0"
THRESHOLD_VERSION = "thresholds-2026-07-rc2"


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    if not isfinite(value):
        return low
    return min(high, max(low, value))


def median_absolute_deviation(values: Iterable[float]) -> float:
    sample = list(values)
    if not sample:
        return 0.0
    center = median(sample)
    return median(abs(value - center) for value in sample)


def robust_z(value: float, history: Iterable[float], epsilon: float = 1e-9) -> float:
    """Robust anomaly score using the median and consistency-corrected MAD."""
    sample = list(history)
    if not sample:
        return 0.0
    center = median(sample)
    mad = median_absolute_deviation(sample)
    if mad <= epsilon:
        return 0.0 if abs(value - center) <= epsilon else (value - center) / epsilon
    return (value - center) / (1.4826 * mad + epsilon)


EVENT_BEHAVIOR_RATIO: dict[EventType, float] = {
    EventType.MODEL_RELEASE: 0.78,
    EventType.DEVELOPER_TOOL_RELEASE: 0.72,
    EventType.RESEARCH_OR_BENCHMARK: 0.50,
    EventType.OFFICIAL_PRODUCT_RELEASE: 0.65,
    EventType.SECURITY_INCIDENT: 0.84,
}

# Versioned V0 confirmation windows. These are deliberately separate from the
# fast detection window: an event can accelerate quickly but needs time for the
# event-type-specific minimum evidence combination to become established.
CONFIRMATION_HOURS: dict[EventType, float] = {
    EventType.MODEL_RELEASE: 12,
    EventType.DEVELOPER_TOOL_RELEASE: 12,
    EventType.RESEARCH_OR_BENCHMARK: 24,
    EventType.OFFICIAL_PRODUCT_RELEASE: 24,
    EventType.SECURITY_INCIDENT: 6,
}

# A cooling event remains visible while the signal decays. It becomes dormant
# only after an event-type-specific period without fresh positive growth.
DORMANCY_HOURS: dict[EventType, float] = {
    EventType.MODEL_RELEASE: 24,
    EventType.DEVELOPER_TOOL_RELEASE: 24,
    EventType.RESEARCH_OR_BENCHMARK: 72,
    EventType.OFFICIAL_PRODUCT_RELEASE: 36,
    EventType.SECURITY_INCIDENT: 12,
}


@dataclass(slots=True)
class ScoreInput:
    event_type: EventType
    attention: float
    behavior: float
    diversity: float
    authority: float
    coordination_risk: float
    coverage: float
    verifiability: float
    sample_reliability: float | None = None
    baseline_maturity: float = 35
    cluster_confidence: float | None = None
    temporal_stability: float = 50
    behavior_applicable: bool = True
    behavior_observed: bool = True
    primary_adoption_observed: bool = False
    primary_response_observed: bool = False
    primary_adoption_score: float = 0.0
    primary_response_score: float = 0.0
    gap_residual_robust_z: float | None = None
    velocity: float = 0.0
    acceleration: float = 0.0
    anomaly_robust_z: float = 0.0
    consecutive_decline: int = 0
    inactive_hours: float = 0.0
    platform_concentration: float = 0.0
    independent_signal_families: int = 0
    independent_platform_families: int = 0
    independent_ownership_entities: int = 0
    consecutive_joint_growth: int = 0
    consecutive_gap_growth: int = 0
    official_source_led: bool = False
    reactivated: bool = False
    previous_state: LifecycleState | None = None


@dataclass(slots=True)
class ScoreResult:
    state: LifecycleState
    labels: list[StructureLabel]
    evidence_strength: float
    uncertainty: float
    gap_residual: float
    drivers: list[str] = field(default_factory=list)
    score_version: str = SCORE_VERSION
    threshold_version: str = THRESHOLD_VERSION


def expected_behavior(event_type: EventType, attention: float, hours_since_first_seen: float = 6.0) -> float:
    ratio = EVENT_BEHAVIOR_RATIO[event_type]
    if event_type == EventType.RESEARCH_OR_BENCHMARK:
        # Reproduction and citation behavior normally trails discussion for research events.
        maturity = clamp(hours_since_first_seen / 24.0, 0.35, 1.0)
        ratio *= maturity
    return clamp(attention * ratio)


def evidence_strength(coverage: float, sample_reliability: float, baseline_maturity: float, cluster_confidence: float, temporal_stability: float) -> float:
    return round(clamp(
        0.30 * clamp(coverage)
        + 0.20 * clamp(sample_reliability)
        + 0.20 * clamp(baseline_maturity)
        + 0.15 * clamp(cluster_confidence)
        + 0.15 * clamp(temporal_stability)
    ), 2)


def strength_tier(value: float) -> EvidenceStrength:
    if value < 45:
        return EvidenceStrength.LOW
    if value < 70:
        return EvidenceStrength.MEDIUM
    return EvidenceStrength.HIGH


def score_event(data: ScoreInput, *, hours_since_first_seen: float = 6.0) -> ScoreResult:
    attention = clamp(data.attention)
    behavior = clamp(data.behavior)
    diversity = clamp(data.diversity)
    authority = clamp(data.authority)
    coordination = clamp(data.coordination_risk)
    coverage = clamp(data.coverage)
    reliability = data.sample_reliability if data.sample_reliability is not None else 0.6 * clamp(data.verifiability) + 0.4 * authority
    cluster_confidence = data.cluster_confidence if data.cluster_confidence is not None else diversity
    raw_strength = evidence_strength(coverage, reliability, data.baseline_maturity, cluster_confidence, data.temporal_stability)
    # Missing families are not reweighted to 100%. A high-quality sliver of data
    # may support a high estimate, but it cannot create high evidence strength.
    strength = round(min(raw_strength, coverage + 15, data.independent_signal_families * 25 + 10), 2)
    uncertainty = round(clamp(100.0 - strength + max(0, 4 - data.independent_signal_families) * 4.0), 2)
    expected = expected_behavior(data.event_type, attention, hours_since_first_seen)
    gap = round(expected - behavior, 2)
    nominal_gap = round(attention * EVENT_BEHAVIOR_RATIO[data.event_type] - behavior, 2)
    labels: list[StructureLabel] = []
    drivers: list[str] = []

    if data.platform_concentration >= 0.80 and behavior >= 60:
        labels.append(StructureLabel.PLATFORM_CONCENTRATED)
        drivers.append("至少 80% 的行为增长集中在单一平台")
    if coordination >= 60:
        labels.append(StructureLabel.COORDINATION_RISK)
        drivers.append("协同发布风险超过阈值")
    if diversity < 45:
        labels.append(StructureLabel.LOW_SOURCE_DIVERSITY)
        drivers.append("独立信源群多样性不足")
    if data.official_source_led:
        labels.append(StructureLabel.OFFICIAL_SOURCE_LED)
    research_lag = (
        data.event_type == EventType.RESEARCH_OR_BENCHMARK
        and hours_since_first_seen < 24
        and nominal_gap >= 25
        and data.consecutive_gap_growth >= 2
    )
    gap_z = data.gap_residual_robust_z if data.gap_residual_robust_z is not None else gap / 12.5
    gap_is_material = data.behavior_applicable and data.behavior_observed and strength >= 45 and gap_z >= 2 and data.consecutive_gap_growth >= 2
    if research_lag:
        labels.append(StructureLabel.EXPECTED_BEHAVIOR_LAG)
        drivers.append("研究类事件仍处于预期行为滞后窗口")
    elif gap_is_material:
        labels.append(StructureLabel.ATTENTION_BEHAVIOR_GAP)
        drivers.append("实际行为显著低于该事件类型的预期行为")

    cross_platform = data.independent_platform_families >= 2 and data.independent_ownership_entities >= 2 and strength >= 45
    adopted = data.behavior_applicable and data.behavior_observed and data.primary_adoption_observed and clamp(data.primary_adoption_score) >= 65 and data.platform_concentration < 0.80 and data.consecutive_joint_growth >= 2
    responded = data.behavior_applicable and data.behavior_observed and data.event_type == EventType.SECURITY_INCIDENT and data.primary_response_observed and clamp(data.primary_response_score) >= 65 and data.consecutive_joint_growth >= 2
    if cross_platform:
        labels.append(StructureLabel.CROSS_PLATFORM_CONFIRMED)
    if adopted:
        labels.append(StructureLabel.ADOPTION_CONFIRMED)
    if responded:
        labels.append(StructureLabel.RESPONSE_CONFIRMED)

    anomaly = data.anomaly_robust_z
    reactivated = data.reactivated or (
        data.previous_state == LifecycleState.DORMANT and anomaly >= 2 and strength >= 45
    )
    if reactivated:
        labels.append(StructureLabel.REACTIVATED)

    if coverage < 40 or data.independent_signal_families < 2:
        state = LifecycleState.INSUFFICIENT_DATA
        drivers.append("数据覆盖或独立信号家族不足，不输出强结论")
    elif reactivated:
        state = LifecycleState.EMERGING
        drivers.append("休眠事件出现 robust Z≥2 的新异常证据，重新进入萌发阶段")
    elif data.previous_state in {LifecycleState.COOLING, LifecycleState.DORMANT} and data.inactive_hours >= DORMANCY_HOURS[data.event_type]:
        state = LifecycleState.DORMANT
        drivers.append("事件已超过类型专用休眠窗口且没有新的正向增长")
    elif data.consecutive_decline >= 2 and data.previous_state in {LifecycleState.ACCELERATING, LifecycleState.ESTABLISHED}:
        state = LifecycleState.COOLING
        drivers.append("已确认事件的归一化速度连续下降")
    elif data.previous_state == LifecycleState.COOLING:
        if anomaly >= 2:
            state = LifecycleState.ACCELERATING if data.consecutive_joint_growth >= 2 else LifecycleState.EMERGING
            drivers.append("降温事件出现新的 robust Z≥2 异常增长，重新进入活跃阶段")
        else:
            state = LifecycleState.COOLING
            drivers.append("事件在类型专用休眠窗口内没有新的异常增长，继续保持降温")
    elif (
        hours_since_first_seen >= CONFIRMATION_HOURS[data.event_type]
        and strength >= 70
        and cross_platform
        and attention >= 70
        and (
            (data.event_type in {EventType.RESEARCH_OR_BENCHMARK, EventType.OFFICIAL_PRODUCT_RELEASE} and not data.behavior_applicable)
            or (data.event_type == EventType.SECURITY_INCIDENT and max(attention, behavior) >= 70)
            or (data.behavior_applicable and data.behavior_observed and behavior >= 65 and data.platform_concentration < .80)
        )
    ):
        state = LifecycleState.ESTABLISHED
        drivers.append("类型专用确认窗口和最低独立证据组合均已满足")
    elif max(attention, behavior if data.behavior_observed else 0) >= 65 and strength >= 45 and data.consecutive_joint_growth >= 2:
        state = LifecycleState.ACCELERATING
        drivers.append("讨论或适用行为连续两个周期保持正增长")
    elif anomaly >= 2 and max(attention, behavior) >= 45 and strength >= 45:
        state = LifecycleState.EMERGING
        drivers.append("至少一个适用增长指标达到 robust Z≥2，且证据强度达到中档")
    elif max(attention, behavior) >= 30:
        state = LifecycleState.DETECTED
        drivers.append("已检测到异常，但证据或扩散范围有限")
    else:
        state = LifecycleState.NOISE
        drivers.append("信号未超过历史噪声区间")

    if state in {LifecycleState.ACCELERATING, LifecycleState.ESTABLISHED} and not cross_platform:
        # A strong state without sufficient source diversity is unsafe; downgrade it.
        state = LifecycleState.EMERGING if anomaly >= 2 and strength >= 45 else LifecycleState.DETECTED
        drivers.append("缺少跨信号家族确认，强状态被降级")

    return ScoreResult(
        state=state,
        labels=list(dict.fromkeys(labels)),
        evidence_strength=strength,
        uncertainty=uncertainty,
        gap_residual=gap,
        drivers=drivers,
    )
