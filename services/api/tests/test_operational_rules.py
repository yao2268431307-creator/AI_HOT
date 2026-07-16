from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import pytest

from radar.alerts import AlertCandidate, AlertPolicyEngine
from radar.alert_worker import AlertDispatcher
from radar.budget import budget_guard
from radar.contracts import AlertRuleRequest, EventType
from radar.evaluation import EvaluationExample, LabeledPrediction, bcubed_cluster_precision_recall, bootstrap_confidence_interval, cohen_kappa, macro_f1, median_lead_minutes, pairwise_cluster_precision, pairwise_cluster_recall, precision_at_k, temporal_entity_holdout
from radar.metrics import Baseline, SignalSnapshot, aggregate_metrics, to_score_input
from radar.source_discovery import SourceCandidate, candidate_score, promote_candidates
from radar.fixtures import seed_repository
from radar.feature_registry import behavior_metric_roles, load_feature_registry
from radar.storage import InMemoryRepository


NOW = datetime.now(timezone.utc)


def snapshot(source: str, group: str, platform: str, family: str, metrics: dict[str, float], previous: dict[str, float], *, fingerprint: str = "", captured_at: datetime = NOW) -> SignalSnapshot:
    return SignalSnapshot(source, group, platform, family, captured_at, metrics, previous, 80, True, fingerprint)


def test_event_type_metrics_require_discussion_and_behavior_families() -> None:
    rows = [
        snapshot("a", "researcher", "HN", "discussion", {"comments": 35}, {"comments": 5}),
        snapshot("b", "developer", "RSS", "discussion", {"mentions": 16}, {"mentions": 1}),
        snapshot("c", "maintainer", "GitHub", "behavior", {"stars": 900, "forks": 80}, {"stars": 100, "forks": 10}),
        *[
            snapshot(f"d-{index}", "model-host", "HF", "behavior", {"downloads": 12000, "derivatives": 18}, {"downloads": 1000, "derivatives": 1}, captured_at=NOW + timedelta(minutes=15 * index))
            for index in range(5)
        ],
        snapshot("e", "official", "Official", "official", {}, {}),
    ]
    metrics = aggregate_metrics(EventType.MODEL_RELEASE, rows, Baseline({"comments": [1, 2, 3, 2, 4]}))
    assert metrics.attention > 50
    assert 35 < metrics.behavior < 55
    assert metrics.independent_signal_families == 3
    assert 60 <= metrics.coverage < 75
    # Only primary adoption/response metrics enter behavior concentration.
    assert metrics.platform_concentration == 1


def test_behavior_family_without_a_type_valid_metric_remains_missing() -> None:
    rows = [snapshot("video", "creator", "YouTube", "behavior", {"views": 100000}, {"views": 1000})]
    metrics = aggregate_metrics(EventType.MODEL_RELEASE, rows)
    assert metrics.behavior_observed is False
    assert metrics.behavior == 0
    assert metrics.independent_signal_families == 0


def test_explicit_na_behavior_is_removed_from_coverage_denominator() -> None:
    rows = [
        snapshot("paper", "authors", "arXiv", "research", {}, {}),
        snapshot("talk", "reviewers", "HN", "discussion", {"comments": 20}, {"comments": 0}),
    ]
    metrics = aggregate_metrics(EventType.RESEARCH_OR_BENCHMARK, rows)
    missing = to_score_input(EventType.RESEARCH_OR_BENCHMARK, metrics, velocity=0, consecutive_joint_growth=0, consecutive_gap_growth=0, behavior_applicable=True)
    not_applicable = to_score_input(EventType.RESEARCH_OR_BENCHMARK, metrics, velocity=0, consecutive_joint_growth=0, consecutive_gap_growth=0, behavior_applicable=False)
    assert not_applicable.coverage > missing.coverage
    assert not_applicable.coverage == min(100, missing.coverage + 32.5)


def test_attention_only_views_expose_coordination_but_not_behavior_concentration() -> None:
    rows = [
        snapshot(f"s{i}", "campaign", "YouTube", "behavior", {"views": 10000 + i}, {"views": 0}, fingerprint="same")
        for i in range(5)
    ] + [snapshot("talker", "campaign", "RSS", "discussion", {"mentions": 10}, {"mentions": 0}, fingerprint="same")]
    metrics = aggregate_metrics(EventType.OFFICIAL_PRODUCT_RELEASE, rows)
    assert metrics.platform_concentration == 0
    assert metrics.behavior_observed is False
    assert metrics.primary_adoption_observed is False
    assert metrics.coordination_risk >= 60


def test_primary_adoption_and_secondary_intent_have_distinct_confirmation_roles() -> None:
    stars = [snapshot("stars", "tool", "GitHub", "behavior", {"stars": 5000}, {"stars": 0})]
    installs = [
        snapshot(f"installs-{index}", f"package-owner-{index}", "Package", "behavior", {"installs": 5000}, {"installs": 0})
        for index in range(5)
    ]
    star_metrics = aggregate_metrics(EventType.DEVELOPER_TOOL_RELEASE, stars)
    install_metrics = aggregate_metrics(EventType.DEVELOPER_TOOL_RELEASE, installs)
    assert star_metrics.behavior_observed is True
    assert star_metrics.behavior == 0
    assert star_metrics.primary_adoption_observed is False
    assert install_metrics.primary_adoption_observed is True


def test_flat_primary_plus_high_secondary_intent_cannot_confirm_adoption() -> None:
    rows = [
        snapshot(f"stars-{index}", f"owner-{index}", "GitHub", "behavior", {"stars": 50000}, {"stars": 0})
        for index in range(5)
    ] + [
        snapshot(f"installs-{index}", f"owner-{index}", "Package", "behavior", {"installs": 1000}, {"installs": 1000})
        for index in range(5)
    ]
    metrics = aggregate_metrics(EventType.DEVELOPER_TOOL_RELEASE, rows)
    assert 0 < metrics.behavior <= 25
    assert metrics.primary_adoption_score == 0
    assert metrics.primary_adoption_observed is False
    scored = to_score_input(
        EventType.DEVELOPER_TOOL_RELEASE,
        metrics,
        velocity=40,
        consecutive_joint_growth=3,
        consecutive_gap_growth=0,
    )
    assert scored.primary_adoption_observed is False


def test_behavior_profile_uses_fixed_denominator_and_family_caps() -> None:
    installs = [
        snapshot(f"install-{index}", f"owner-{index}", "Package", "behavior", {"installs": 100000}, {"installs": 0})
        for index in range(5)
    ]
    intent = [
        snapshot(f"intent-{index}", f"owner-{index}", "GitHub", "behavior", {"stars": 100000, "forks": 100000}, {"stars": 0, "forks": 0})
        for index in range(5)
    ]
    installs_metrics = aggregate_metrics(EventType.DEVELOPER_TOOL_RELEASE, installs)
    intent_metrics = aggregate_metrics(EventType.DEVELOPER_TOOL_RELEASE, intent)
    assert installs_metrics.behavior <= 30
    assert intent_metrics.behavior <= 25
    assert installs_metrics.primary_adoption_score <= 40  # .30 / .75 primary fixed denominator
    assert intent_metrics.primary_adoption_observed is False


def test_accounts_with_one_resolved_owner_do_not_inflate_discussion_or_diversity() -> None:
    one_owner = [
        snapshot(f"account-{index}", "entity:campaign-owner", f"P{index}", "discussion", {"mentions": 10}, {"mentions": 0})
        for index in range(6)
    ]
    independent = [
        snapshot(f"person-{index}", f"entity:person-{index}", f"P{index}", "discussion", {"mentions": 10}, {"mentions": 0})
        for index in range(6)
    ]
    campaign_metrics = aggregate_metrics(EventType.MODEL_RELEASE, one_owner)
    independent_metrics = aggregate_metrics(EventType.MODEL_RELEASE, independent)
    assert campaign_metrics.attention < independent_metrics.attention
    assert campaign_metrics.diversity < independent_metrics.diversity
    assert campaign_metrics.coordination_risk > independent_metrics.coordination_risk


def candidate(item_id: str, score_bias: int = 0) -> SourceCandidate:
    return SourceCandidate(item_id, NOW - timedelta(days=9), 10, 7 + score_bias, 6 + score_bias, 85, 80, 75, 5)


def test_source_promotion_respects_history_quality_and_daily_growth_cap() -> None:
    candidates = [candidate(f"s{i}") for i in range(20)]
    promoted = promote_candidates(candidates, active_count=120, now=NOW)
    assert len(promoted) == 6  # ceil(120 * 5%)
    assert all(item.score >= 58 for item in promoted)
    too_new = SourceCandidate("new", NOW - timedelta(days=2), 20, 20, 20, 100, 100, 100, 0)
    assert too_new not in promote_candidates([too_new], active_count=120, now=NOW)


def test_source_marketing_overlap_penalizes_candidate() -> None:
    organic = candidate("organic")
    matrix = candidate("matrix")
    matrix.marketing_matrix_overlap = 90
    assert candidate_score(organic) > candidate_score(matrix) + 20


def test_budget_guard_never_silently_overspends() -> None:
    assert budget_guard(1000, 2000).collection_multiplier == 1
    assert budget_guard(1850, 2000).collection_multiplier == .35
    over = budget_guard(2100, 2000)
    assert over.stop_paid_connectors is True
    assert over.coverage_penalty >= 25


def test_full_event_masks_drive_penalties_and_alert_deltas_beyond_display_cap() -> None:
    repository = seed_repository(InMemoryRepository())
    event = repository.get_event("evt-open-model")
    assert event is not None
    event = event.model_copy(update={
        "platforms": event.platforms + ["HiddenPlatform"],
        "signal_families": ["official", "discussion", "behavior"],
        "discussion_evidence_state": "observed", "behavior_evidence_state": "observed",
        "evidence_count": 31,
    })
    repository.upsert_event(event)
    assert all(item.platform != "HiddenPlatform" for item in event.evidence)
    assert repository.apply_connector_coverage_penalty("HiddenPlatform", 10, "test") == 1
    previous = {"evidenceCount": 30, "lifecycleState": event.state.value}
    candidate_value = AlertDispatcher._candidate(event, "workspace-a", previous, NOW)
    assert candidate_value.new_evidence_count == 1


def test_alert_policy_enforces_evidence_change_cooldown_and_daily_budgets() -> None:
    policy = AlertPolicyEngine(workspace_daily_limit=3, domain_daily_limit=2, cooldown=timedelta(hours=4))

    def alert(event: str, domain: str = "models", *, at: datetime = NOW, evidence: str = "high", new: int = 1, upgraded: bool = False) -> AlertCandidate:
        return AlertCandidate("workspace-a", event, domain, "accelerating", evidence, new, upgraded, at)

    assert policy.decide_and_record(alert("a")).allowed is True
    assert policy.evaluate(alert("a", at=NOW + timedelta(hours=1))).reason == "event is inside the repeat cooldown"
    assert policy.evaluate(alert("a", at=NOW + timedelta(hours=1), upgraded=True)).allowed is True
    assert policy.evaluate(alert("low", evidence="low")).allowed is False
    assert policy.evaluate(alert("unchanged", new=0)).allowed is False
    assert policy.decide_and_record(alert("b")).allowed is True
    assert policy.evaluate(alert("c")).reason == "domain daily alert budget exhausted"
    assert policy.decide_and_record(alert("security", domain="security")).allowed is True
    assert policy.evaluate(alert("other", domain="research")).reason == "workspace daily alert budget exhausted"


def test_event_cooldown_survives_utc_midnight_while_daily_budget_resets() -> None:
    policy = AlertPolicyEngine(cooldown=timedelta(hours=4))
    before_midnight = datetime(2026, 7, 16, 23, 30, tzinfo=timezone.utc)
    first = AlertCandidate("workspace-a", "event-a", "models", "accelerating", "high", 1, False, before_midnight)
    repeated = AlertCandidate("workspace-a", "event-a", "models", "accelerating", "high", 1, False, before_midnight + timedelta(hours=1))
    other = AlertCandidate("workspace-a", "event-b", "models", "accelerating", "high", 1, False, before_midnight + timedelta(hours=1))
    assert policy.decide_and_record(first).allowed is True
    assert policy.evaluate(repeated).reason == "event is inside the repeat cooldown"
    assert policy.evaluate(other).allowed is True


def test_alert_delivery_reservations_atomically_enforce_domain_budget_and_idempotency() -> None:
    repository = InMemoryRepository()

    def reserve(index: int) -> bool:
        return repository.reserve_alert_delivery({
            "ruleId": f"rule-{index}", "workspaceId": "workspace-a", "eventId": f"event-{index}",
            "domain": "model_release", "lifecycleState": "accelerating", "evidenceStrength": "high",
            "evidenceCount": index + 3, "channel": "in_app", "deliveredAt": NOW,
            "idempotencyKey": f"delivery-{index}",
        })

    with ThreadPoolExecutor(max_workers=12) as pool:
        outcomes = list(pool.map(reserve, range(12)))
    assert sum(outcomes) == 3
    assert reserve(0) is False
    assert len(repository.alert_deliveries) == 3
    first_key = str(repository.alert_deliveries[0]["idempotencyKey"])
    repository.confirm_alert_delivery(first_key, "workspace-a")
    assert repository.alert_deliveries[0]["status"] == "delivered"


@pytest.mark.asyncio
async def test_committed_score_event_matches_rule_and_creates_durable_in_app_alert_once() -> None:
    repository = seed_repository(InMemoryRepository())
    repository.add_alert(
        AlertRuleRequest(name="strong AI events", minimumAttention=70, minimumEvidenceStrength=65),
        "workspace-a", "analyst-a",
    )
    dispatcher = AlertDispatcher(repository, signing_secret="test-secret")
    first = await dispatcher.dispatch_event("evt-open-model", ["workspace-a"], NOW)
    repeated = await dispatcher.dispatch_event("evt-open-model", ["workspace-a"], NOW + timedelta(hours=1))
    assert first.delivered == 1
    assert repeated.delivered == 0
    assert len(repository.alert_deliveries) == 1
    assert repository.alert_deliveries[0]["channel"] == "in_app"


@pytest.mark.asyncio
async def test_unchanged_alert_does_not_reappear_after_midnight_and_cooldown() -> None:
    repository = seed_repository(InMemoryRepository())
    repository.add_alert(AlertRuleRequest(name="strong", minimumAttention=70, minimumEvidenceStrength=65), "workspace-a", "analyst-a")
    dispatcher = AlertDispatcher(repository, signing_secret="test-secret")
    before_midnight = datetime(2026, 7, 16, 20, 0, tzinfo=timezone.utc)
    first = await dispatcher.dispatch_event("evt-open-model", ["workspace-a"], before_midnight)
    unchanged = await dispatcher.dispatch_event("evt-open-model", ["workspace-a"], before_midnight + timedelta(hours=5))
    assert first.delivered == 1
    assert unchanged.delivered == 0
    assert len(repository.alert_deliveries) == 1


def test_evaluation_metrics_keep_ranking_state_and_lead_time_separate() -> None:
    items = [
        LabeledPrediction("a", "accelerating", "accelerating", 95, NOW, NOW + timedelta(minutes=30)),
        LabeledPrediction("b", "accelerating", "noise", 90, NOW, None),
        LabeledPrediction("c", "noise", "noise", 20, NOW, None),
    ]
    assert precision_at_k(items, {"accelerating"}, 2) == .5
    assert 0 < macro_f1(items) < 1
    assert median_lead_minutes(items) == 30


def test_pairwise_cluster_precision_detects_false_merge() -> None:
    predicted = {"1": "a", "2": "a", "3": "b"}
    actual = {"1": "x", "2": "y", "3": "z"}
    assert pairwise_cluster_precision(predicted, actual) == 0


def test_pairwise_recall_and_temporal_entity_holdout_detect_false_splits_without_leakage() -> None:
    predicted = {"1": "a", "2": "b", "3": "c"}
    actual = {"1": "x", "2": "x", "3": "y"}
    assert pairwise_cluster_recall(predicted, actual) == 0
    cutoff = NOW
    examples = [
        EvaluationExample("old-a", "org:a", NOW - timedelta(days=2), "detected"),
        EvaluationExample("old-b", "org:b", NOW - timedelta(days=2), "noise"),
        EvaluationExample("new-a", "org:a", NOW + timedelta(hours=1), "emerging", "established", NOW + timedelta(days=1)),
        EvaluationExample("new-c", "org:c", NOW + timedelta(hours=1), "detected"),
    ]
    train, test = temporal_entity_holdout(examples, cutoff)
    assert {item.entity_id for item in train} == {"org:b"}
    assert {item.entity_id for item in train}.isdisjoint(item.entity_id for item in test)
    with pytest.raises(ValueError, match="future outcome"):
        EvaluationExample("bad", "org:x", NOW, "detected", "noise", NOW - timedelta(minutes=1))


def test_bcubed_reports_false_merge_and_false_split_per_item() -> None:
    predicted = {"1": "a", "2": "a", "3": "b", "4": "c"}
    actual = {"1": "x", "2": "y", "3": "y", "4": "z"}
    precision, recall = bcubed_cluster_precision_recall(predicted, actual)
    assert precision == .75
    assert recall == .75
    assert bcubed_cluster_precision_recall({}, {}) == (1.0, 1.0)


def test_double_label_agreement_and_bootstrap_interval_are_reproducible() -> None:
    assert cohen_kappa(["hot", "hot", "noise", "noise"], ["hot", "hot", "noise", "hot"]) == pytest.approx(.5)
    assert cohen_kappa(["hot", "hot"], ["hot", "hot"]) == 1
    with pytest.raises(ValueError, match="same length"):
        cohen_kappa(["hot"], [])
    values = [0.0, 0.0, 1.0, 1.0]
    first = bootstrap_confidence_interval(values, lambda sample: sum(sample) / len(sample), iterations=200, seed=7)
    second = bootstrap_confidence_interval(values, lambda sample: sum(sample) / len(sample), iterations=200, seed=7)
    assert first == second
    assert first[0] <= .5 <= first[1]


def test_connector_registry_has_required_rights_cost_backfill_and_acceptance_fields() -> None:
    registry_path = Path(__file__).parents[3] / "config" / "connector_registry.json"
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    required = {"id", "priority", "signalFamilies", "discovery", "incrementalKey", "refresh", "backfill", "quotaModel", "worstCaseCostRmbPerRun", "rightsPolicyId", "storedFields", "deletion", "productionState", "acceptanceState"}
    assert len(payload["connectors"]) >= 8
    assert all(required.issubset(connector) for connector in payload["connectors"])
    assert all(connector["productionState"] == "disabled" for connector in payload["connectors"] if connector["id"] == "x")


def test_frozen_feature_registry_is_complete_and_controls_metric_roles() -> None:
    registry = load_feature_registry()
    assert registry["frozen"] is True
    assert len(registry["features"]) >= 20
    roles = behavior_metric_roles()
    assert roles[EventType.DEVELOPER_TOOL_RELEASE]["stars"] == "secondary_intent"
    assert roles[EventType.OFFICIAL_PRODUCT_RELEASE]["views"] == "attention_only"
    assert roles[EventType.SECURITY_INCIDENT]["patch_downloads"] == "primary_response"
