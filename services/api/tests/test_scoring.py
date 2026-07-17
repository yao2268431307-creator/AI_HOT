from __future__ import annotations

import pytest

from radar.contracts import EventType, LifecycleState, StructureLabel
from radar.scoring import ScoreInput, evidence_strength, robust_z, score_event


def base(**overrides: object) -> ScoreInput:
    values: dict[str, object] = {
        "event_type": EventType.MODEL_RELEASE,
        "attention": 80,
        "behavior": 72,
        "diversity": 75,
        "authority": 80,
        "coordination_risk": 10,
        "coverage": 88,
        "verifiability": 90,
        "sample_reliability": 88,
        "baseline_maturity": 90,
        "cluster_confidence": 86,
        "temporal_stability": 85,
        "velocity": 15,
        "anomaly_robust_z": 3,
        "platform_concentration": .35,
        "independent_signal_families": 4,
        "independent_platform_families": 3,
        "independent_ownership_entities": 4,
        "consecutive_joint_growth": 3,
        "consecutive_gap_growth": 0,
        "official_source_led": True,
        "official_source_present": True,
        "research_source_present": True,
        "discussion_source_present": True,
        "primary_adoption_observed": True,
        "primary_adoption_score": 72,
    }
    values.update(overrides)
    return ScoreInput(**values)  # type: ignore[arg-type]


def test_robust_z_uses_mad_and_is_resistant_to_outlier() -> None:
    score = robust_z(13, [9, 10, 10, 11, 1000])
    assert 1.9 < score < 2.1


def test_confirmed_hotspot_requires_joint_growth_and_multiple_families() -> None:
    result = score_event(base())
    assert result.state == LifecycleState.ACCELERATING
    assert StructureLabel.CROSS_PLATFORM_CONFIRMED in result.labels
    assert StructureLabel.ADOPTION_CONFIRMED in result.labels
    assert result.evidence_strength >= 60


def test_secondary_intent_cannot_confirm_adoption_and_security_response_is_separate() -> None:
    intent_only = score_event(base(primary_adoption_observed=False))
    response = score_event(base(
        event_type=EventType.SECURITY_INCIDENT,
        primary_adoption_observed=False,
        primary_response_observed=True,
        primary_response_score=72,
    ))
    assert StructureLabel.ADOPTION_CONFIRMED not in intent_only.labels
    assert StructureLabel.ADOPTION_CONFIRMED not in response.labels
    assert StructureLabel.RESPONSE_CONFIRMED in response.labels


def test_established_requires_the_type_confirmation_window() -> None:
    early = score_event(base(), hours_since_first_seen=6)
    mature = score_event(base(), hours_since_first_seen=24)
    assert early.state == LifecycleState.ACCELERATING
    assert mature.state == LifecycleState.ESTABLISHED


def test_missing_product_behavior_is_not_treated_as_not_applicable() -> None:
    missing = score_event(base(
        event_type=EventType.OFFICIAL_PRODUCT_RELEASE, behavior=0,
        behavior_applicable=True, behavior_observed=False,
    ), hours_since_first_seen=48)
    explicit_na = score_event(base(
        event_type=EventType.OFFICIAL_PRODUCT_RELEASE, behavior=0,
        behavior_applicable=False, behavior_observed=False,
    ), hours_since_first_seen=48)
    assert missing.state != LifecycleState.ESTABLISHED
    assert explicit_na.state == LifecycleState.ESTABLISHED
    assert StructureLabel.ADOPTION_CONFIRMED not in explicit_na.labels


def test_marketing_scissor_gap_is_a_structure_label_not_lifecycle() -> None:
    result = score_event(base(
        event_type=EventType.DEVELOPER_TOOL_RELEASE,
        behavior=28,
        diversity=42,
        coordination_risk=82,
        consecutive_joint_growth=0,
        consecutive_gap_growth=2,
        independent_signal_families=3,
    ), hours_since_first_seen=72)
    assert result.state == LifecycleState.EMERGING
    assert StructureLabel.ATTENTION_BEHAVIOR_GAP in result.labels
    assert StructureLabel.COORDINATION_RISK in result.labels


def test_single_platform_behavior_spike_is_not_confirmed_hotspot() -> None:
    result = score_event(base(
        attention=38,
        behavior=82,
        diversity=24,
        authority=38,
        platform_concentration=.88,
        independent_signal_families=2,
        independent_platform_families=1,
        consecutive_joint_growth=0,
    ))
    assert result.state in {LifecycleState.DETECTED, LifecycleState.EMERGING}
    assert StructureLabel.PLATFORM_CONCENTRATED in result.labels
    assert StructureLabel.CROSS_PLATFORM_CONFIRMED not in result.labels


def test_cross_platform_label_uses_platform_families_not_signal_categories() -> None:
    one_platform = score_event(base(independent_signal_families=4, independent_platform_families=1))
    two_platforms = score_event(base(independent_signal_families=2, independent_platform_families=2))
    assert StructureLabel.CROSS_PLATFORM_CONFIRMED not in one_platform.labels
    assert StructureLabel.CROSS_PLATFORM_CONFIRMED in two_platforms.labels


def test_research_behavior_lag_is_not_mislabeled_as_marketing() -> None:
    result = score_event(base(
        event_type=EventType.RESEARCH_OR_BENCHMARK,
        attention=88,
        behavior=8,
        consecutive_joint_growth=0,
        consecutive_gap_growth=2,
    ), hours_since_first_seen=4)
    assert StructureLabel.EXPECTED_BEHAVIOR_LAG in result.labels
    assert StructureLabel.ATTENTION_BEHAVIOR_GAP not in result.labels


@pytest.mark.parametrize(("event_type", "missing"), [
    (EventType.MODEL_RELEASE, "official_source_present"),
    (EventType.MODEL_RELEASE, "discussion_source_present"),
    (EventType.MODEL_RELEASE, "behavior_observed"),
    (EventType.DEVELOPER_TOOL_RELEASE, "official_source_present"),
    (EventType.DEVELOPER_TOOL_RELEASE, "discussion_source_present"),
    (EventType.DEVELOPER_TOOL_RELEASE, "primary_adoption_observed"),
    (EventType.RESEARCH_OR_BENCHMARK, "research_source_present"),
    (EventType.RESEARCH_OR_BENCHMARK, "discussion_source_present"),
    (EventType.OFFICIAL_PRODUCT_RELEASE, "official_source_present"),
    (EventType.OFFICIAL_PRODUCT_RELEASE, "discussion_source_present"),
    (EventType.SECURITY_INCIDENT, "official_source_present"),
])
def test_each_event_type_minimum_evidence_combination_blocks_strong_state(
    event_type: EventType, missing: str,
) -> None:
    overrides: dict[str, object] = {"event_type": event_type, missing: False}
    if event_type == EventType.SECURITY_INCIDENT:
        overrides.update({"discussion_source_present": False, "primary_response_observed": False})
    result = score_event(base(**overrides), hours_since_first_seen=200)
    assert result.state == LifecycleState.INSUFFICIENT_DATA
    assert "最低证据组合不足" in "；".join(result.drivers)


@pytest.mark.parametrize(("event_type", "inside_hours", "outside_hours"), [
    (EventType.MODEL_RELEASE, 6, 25),
    (EventType.DEVELOPER_TOOL_RELEASE, 12, 49),
    (EventType.RESEARCH_OR_BENCHMARK, 24, 169),
    (EventType.OFFICIAL_PRODUCT_RELEASE, 12, 73),
    (EventType.SECURITY_INCIDENT, 12, 49),
])
def test_type_specific_normal_delay_suppresses_marketing_gap(
    event_type: EventType, inside_hours: float, outside_hours: float,
) -> None:
    values = base(
        event_type=event_type, attention=95, behavior=0,
        consecutive_joint_growth=0, consecutive_gap_growth=2,
        primary_response_observed=event_type == EventType.SECURITY_INCIDENT,
    )
    inside = score_event(values, hours_since_first_seen=inside_hours)
    outside = score_event(values, hours_since_first_seen=outside_hours)
    assert StructureLabel.EXPECTED_BEHAVIOR_LAG in inside.labels
    assert StructureLabel.ATTENTION_BEHAVIOR_GAP not in inside.labels
    assert StructureLabel.EXPECTED_BEHAVIOR_LAG not in outside.labels
    assert StructureLabel.ATTENTION_BEHAVIOR_GAP in outside.labels


def test_low_coverage_never_emits_strong_state() -> None:
    result = score_event(base(coverage=32, independent_signal_families=1))
    assert result.state == LifecycleState.INSUFFICIENT_DATA
    assert result.uncertainty > 60


def test_evidence_strength_is_bounded() -> None:
    assert evidence_strength(100, 100, 100, 100, 100) == 100
    assert evidence_strength(0, 0, 0, 0, 0) == 0
    assert evidence_strength(50, 50, 50, 50, 50) == 50


def test_emerging_requires_robust_growth_anomaly() -> None:
    result = score_event(base(
        attention=60, behavior=50, consecutive_joint_growth=0,
        anomaly_robust_z=1.99,
    ))
    assert result.state == LifecycleState.DETECTED


def test_cooling_requires_two_consecutive_declines() -> None:
    one_decline = score_event(base(
        previous_state=LifecycleState.ACCELERATING,
        consecutive_decline=1, velocity=-10,
    ), hours_since_first_seen=24)
    two_declines = score_event(base(
        previous_state=LifecycleState.ACCELERATING,
        consecutive_decline=2, velocity=-10,
    ), hours_since_first_seen=24)
    assert one_decline.state == LifecycleState.ESTABLISHED
    assert two_declines.state == LifecycleState.COOLING


def test_cooling_becomes_dormant_after_type_window() -> None:
    result = score_event(base(
        previous_state=LifecycleState.COOLING,
        consecutive_decline=2, inactive_hours=24,
    ), hours_since_first_seen=72)
    assert result.state == LifecycleState.DORMANT


def test_cooling_stays_cooling_when_flat_then_reaches_dormancy() -> None:
    first = score_event(base(
        previous_state=LifecycleState.ESTABLISHED,
        consecutive_decline=2, inactive_hours=1,
    ), hours_since_first_seen=48)
    flat = score_event(base(
        previous_state=first.state, anomaly_robust_z=0,
        consecutive_decline=0, consecutive_joint_growth=0, inactive_hours=2,
    ), hours_since_first_seen=49)
    dormant = score_event(base(
        previous_state=flat.state, anomaly_robust_z=0,
        consecutive_decline=0, consecutive_joint_growth=0, inactive_hours=24,
    ), hours_since_first_seen=72)
    assert (first.state, flat.state, dormant.state) == (
        LifecycleState.COOLING, LifecycleState.COOLING, LifecycleState.DORMANT,
    )


def test_dormant_event_reactivates_only_on_new_anomaly() -> None:
    reactivated = score_event(base(
        previous_state=LifecycleState.DORMANT,
        inactive_hours=100, anomaly_robust_z=2.1,
        consecutive_joint_growth=0,
    ), hours_since_first_seen=100)
    assert reactivated.state == LifecycleState.EMERGING
    assert StructureLabel.REACTIVATED in reactivated.labels
