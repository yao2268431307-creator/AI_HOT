from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import hashlib
import json
import sqlite3
import threading
import time

import pytest
from redis.crc import key_slot

from radar import stream_retention as retention_module
from radar import alert_worker as alert_worker_module
from radar.alerts import AlertCandidate, AlertPolicyEngine, WebhookResult
from radar.alert_worker import AlertDispatcher
from radar.budget import budget_guard
from radar.contracts import AlertRuleRequest, EventType, EvidenceStrength, Observation
from radar.connectors.base import BaseConnector
from radar.evaluation import EvaluationExample, LabeledPrediction, bcubed_cluster_precision_recall, bootstrap_confidence_interval, cohen_kappa, macro_f1, median_lead_minutes, pairwise_cluster_precision, pairwise_cluster_recall, precision_at_k, temporal_entity_holdout
from radar.evidence_store import S3EvidenceStore
from radar.metrics import Baseline, SignalSnapshot, aggregate_metrics, to_score_input
from radar.outbox import outbox_recovery_keys
from radar.source_discovery import SourceCandidate, candidate_score, load_source_score_policy, promote_candidates, source_score_policy_digest
from radar.fixtures import seed_repository
from radar.feature_registry import behavior_metric_roles, load_feature_registry
from radar.storage import InMemoryRepository
from radar.stream_retention import (
    ConsumerGroupWatermark,
    ensure_watermarks_did_not_move_backwards,
    minimum_stream_id,
    stream_id_key,
)
from radar.worker import CollectorWorker
from tools import replay_outbox_to_redis as replay_tool
from tools import trim_redis_stream as retention_tool
from tools.replay_outbox_to_redis import execute_replay


NOW = datetime.now(timezone.utc)


def test_s3_evidence_store_forwards_optional_session_token(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_client(service: str, **options: object) -> object:
        captured.update({"service": service, **options})
        return object()

    monkeypatch.setattr("boto3.client", fake_client)
    S3EvidenceStore("https://r2.example", "access", "secret", "session")
    assert captured == {
        "service": "s3",
        "endpoint_url": "https://r2.example",
        "aws_access_key_id": "access",
        "aws_secret_access_key": "secret",
        "aws_session_token": "session",
        "region_name": "auto",
    }


@pytest.mark.asyncio
async def test_alert_dependency_probe_checks_delete_only_r2_capability() -> None:
    class RedisProbe:
        async def ping(self) -> bool:
            return True

    class EvidenceProbe:
        def __init__(self, fails: bool = False) -> None:
            self.fails = fails
            self.calls = 0

        async def probe_delete(self, bucket: str) -> None:
            assert bucket == "raw"
            self.calls += 1
            if self.fails:
                raise RuntimeError("delete denied")

    healthy_store = EvidenceProbe()
    result = await alert_worker_module.probe_alert_dependencies(  # type: ignore[arg-type]
        RedisProbe(), healthy_store, "raw",  # type: ignore[arg-type]
    )
    assert result == (True, True, [])
    failing_store = EvidenceProbe(True)
    redis_ok, r2_delete_ok, failures = await alert_worker_module.probe_alert_dependencies(  # type: ignore[arg-type]
        RedisProbe(), failing_store, "raw",  # type: ignore[arg-type]
    )
    assert redis_ok is True and r2_delete_ok is False
    assert failures == ["r2-delete:RuntimeError"]
    assert healthy_store.calls == failing_store.calls == 1


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


def test_first_cumulative_snapshot_seeds_baseline_without_manufacturing_growth() -> None:
    first = [
        snapshot(f"model-{index}", f"owner-{index}", "HF", "behavior", {"downloads": 10_000_000}, {})
        for index in range(5)
    ]
    seeded = aggregate_metrics(EventType.MODEL_RELEASE, first)
    assert seeded.behavior == 0
    assert seeded.behavior_observed is False
    assert seeded.primary_adoption_observed is False

    second = [
        snapshot(
            f"model-{index}", f"owner-{index}", "HF", "behavior",
            {"downloads": 10_001_000}, {"downloads": 10_000_000},
        )
        for index in range(5)
    ]
    changed = aggregate_metrics(EventType.MODEL_RELEASE, second)
    assert changed.behavior > 0
    assert changed.behavior_observed is True


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
    assert len(promoted) == 6  # floor(120 * 5%)
    assert all(item.score >= 58 for item in promoted)
    assert [item.id for item in promoted] == [
        item.id for item in promote_candidates(list(reversed(candidates)), active_count=120, now=NOW)
    ]
    too_new = SourceCandidate("new", NOW - timedelta(days=2), 20, 20, 20, 100, 100, 100, 0)
    assert too_new not in promote_candidates([too_new], active_count=120, now=NOW)
    assert len(promote_candidates(candidates, active_count=121, now=NOW)) == 6
    assert promote_candidates(candidates, active_count=19, now=NOW) == []


def test_source_policy_freezes_ranking_and_repeated_daily_promotions_fail_closed() -> None:
    policy = load_source_score_policy()
    assert policy.status == "frozen_for_candidate_governance"
    assert policy.timezone_name == "Asia/Shanghai"
    assert policy.ranking_enabled is False
    assert policy.auto_promotion_enabled is False
    assert policy.daily_growth_rounding == "floor"
    assert policy.allow_automatic_bootstrap is False
    assert source_score_policy_digest(policy).startswith("sha256:")
    # Six of the current 126 active sources were already promoted today, which
    # exhausts the 5% cap computed from the start-of-day population of 120.
    assert promote_candidates(
        [candidate("next")], active_count=126, promoted_today=6, now=NOW, policy=policy,
    ) == []


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
async def test_alert_confirmation_failure_keeps_reservation_retryable_until_durable() -> None:
    class FlakyConfirmationRepository(InMemoryRepository):
        confirmation_attempts = 0

        def confirm_alert_delivery(self, idempotency_key: str, workspace_id: str | None = None) -> bool:
            self.confirmation_attempts += 1
            if self.confirmation_attempts == 1:
                raise RuntimeError("injected confirmation failure")
            return super().confirm_alert_delivery(idempotency_key, workspace_id)

    repository = seed_repository(FlakyConfirmationRepository())
    repository.add_alert(
        AlertRuleRequest(name="strong AI events", minimumAttention=70, minimumEvidenceStrength=65),
        "workspace-a", "analyst-a",
    )
    dispatcher = AlertDispatcher(repository, signing_secret="test-secret")
    with pytest.raises(RuntimeError, match="injected confirmation failure"):
        await dispatcher.dispatch_event(
            "evt-open-model", ["workspace-a"], NOW, delivery_key="outbox-cycle-one",
        )
    assert len(repository.alert_deliveries) == 1
    assert repository.alert_deliveries[0]["status"] == "reserved"

    original = repository.get_event("evt-open-model")
    assert original is not None
    repository.upsert_event(original.model_copy(update={
        "score_version": "score-after-confirmation-failure",
        "updated_at": original.updated_at + timedelta(minutes=1),
    }))
    retried = await dispatcher.dispatch_event(
        "evt-open-model", ["workspace-a"], NOW, delivery_key="outbox-cycle-one",
    )
    duplicate = await dispatcher.dispatch_event(
        "evt-open-model", ["workspace-a"], NOW, delivery_key="outbox-cycle-one",
    )
    assert retried.delivered == 1
    assert duplicate.delivered == 0
    assert len(repository.alert_deliveries) == 1
    assert repository.alert_deliveries[0]["status"] == "delivered"

    deleted_repository = seed_repository(FlakyConfirmationRepository())
    deleted_repository.add_alert(
        AlertRuleRequest(name="strong AI events", minimumAttention=70, minimumEvidenceStrength=65),
        "workspace-a", "analyst-a",
    )
    deleted_dispatcher = AlertDispatcher(deleted_repository, signing_secret="test-secret")
    with pytest.raises(RuntimeError, match="injected confirmation failure"):
        await deleted_dispatcher.dispatch_event(
            "evt-open-model", ["workspace-a"], NOW, delivery_key="outbox-before-delete",
        )
    deleted_repository.events.pop("evt-open-model")
    recovered_after_delete = await deleted_dispatcher.dispatch_event(
        "evt-open-model", ["workspace-a"], NOW, delivery_key="outbox-before-delete",
    )
    assert recovered_after_delete.delivered == 1
    assert deleted_repository.alert_deliveries[0]["status"] == "delivered"

    missing_rule_repository = seed_repository(InMemoryRepository())
    delivery_key = "webhook-before-rule-delete"
    message_token = hashlib.sha256(delivery_key.encode()).hexdigest()
    assert missing_rule_repository.reserve_alert_delivery({
        "ruleId": "deleted-rule", "workspaceId": "workspace-a", "eventId": "evt-open-model",
        "domain": "model_release", "lifecycleState": "accelerating", "evidenceStrength": "high",
        "evidenceCount": 4, "channel": "webhook", "deliveredAt": NOW,
        "idempotencyKey": f"workspace-a:deleted-rule:evt-open-model:{message_token}",
    })
    aborted = await AlertDispatcher(
        missing_rule_repository, signing_secret="test-secret",
    ).dispatch_event("evt-open-model", ["workspace-a"], NOW, delivery_key=delivery_key)
    assert aborted.skipped == 1
    assert missing_rule_repository.alert_deliveries[0]["status"] == "aborted"
    assert "rule unavailable" in str(missing_rule_repository.alert_deliveries[0]["terminalReason"])
    missing_rule_repository.alerts["deleted-rule"] = (
        "workspace-a", "analyst-a",
        AlertRuleRequest(name="restored rule", minimumAttention=70, minimumEvidenceStrength=65),
    )
    repeated_aborted = await AlertDispatcher(
        missing_rule_repository, signing_secret="test-secret",
    ).dispatch_event("evt-open-model", ["workspace-a"], NOW, delivery_key=delivery_key)
    assert repeated_aborted.delivered == 0
    assert repeated_aborted.skipped == 1
    assert len(missing_rule_repository.alert_deliveries) == 1


@pytest.mark.asyncio
async def test_replay_closes_redis_and_preserves_primary_error_when_unlock_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BrokenRedis:
        eval_calls = 0
        closed = False

        async def eval(self, *_args: object) -> int:
            self.eval_calls += 1
            if self.eval_calls == 1:
                return 1
            raise RuntimeError("injected unlock failure")

        async def exists(self, _key: str) -> int:
            raise RuntimeError("primary replay failure")

        async def aclose(self) -> None:
            self.closed = True

    broken = BrokenRedis()
    monkeypatch.setattr(
        "tools.replay_outbox_to_redis.Redis.from_url",
        lambda *_args, **_kwargs: broken,
    )
    with pytest.raises(RuntimeError, match="primary replay failure"):
        await execute_replay(
            "postgresql://unused", "redis://unused", "test:stream",
            NOW - timedelta(minutes=1), NOW, 1,
        )
    assert broken.closed is True


@pytest.mark.asyncio
async def test_prefix_verification_cancellation_preserves_cancel_and_cleans_temp_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    started = threading.Event()
    checkpoint_outbox_id = "00000000-0000-0000-0000-000000000001"

    class PrefixRedis:
        async def xrange(self, *_args: object, **_kwargs: object):
            return [("1-0", {"outbox_id": checkpoint_outbox_id})]

    def slow_comparison(_dsn: str, path: Path, *_args: object) -> tuple[int, int, bool]:
        connection = sqlite3.connect(path)
        try:
            started.set()
            time.sleep(0.2)
        finally:
            connection.close()
        return 1, 1, True

    original_named_temporary_file = replay_tool.tempfile.NamedTemporaryFile

    def temporary_file_in_test_directory(*args: object, **kwargs: object):
        kwargs["dir"] = tmp_path
        return original_named_temporary_file(*args, **kwargs)

    monkeypatch.setattr(replay_tool, "compare_replayed_prefix", slow_comparison)
    monkeypatch.setattr(replay_tool.tempfile, "NamedTemporaryFile", temporary_file_in_test_directory)
    task = asyncio.create_task(replay_tool.verify_replayed_prefix(
        PrefixRedis(), "unused", "test:stream", NOW - timedelta(minutes=1), NOW,
        NOW, checkpoint_outbox_id, "1-0", 1,
    ))
    assert await asyncio.to_thread(started.wait, 1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert list(tmp_path.glob("radar-replay-prefix-*.sqlite3")) == []


@pytest.mark.asyncio
async def test_low_evidence_event_never_becomes_a_strong_alert_even_with_a_permissive_rule() -> None:
    repository = seed_repository(InMemoryRepository())
    event = repository.get_event("evt-open-model")
    assert event is not None
    repository.upsert_event(event.model_copy(update={"evidence_strength": EvidenceStrength.LOW, "evidence_score": 10}))
    repository.add_alert(
        AlertRuleRequest(name="permissive rule", minimumAttention=0, minimumEvidenceStrength=0),
        "workspace-a", "analyst-a",
    )
    summary = await AlertDispatcher(repository, signing_secret="test-secret").dispatch_event(
        event.id, ["workspace-a"], NOW,
    )
    assert summary.evaluated == 1
    assert summary.delivered == 0
    assert repository.alert_deliveries == []


@pytest.mark.asyncio
async def test_unverified_discovery_cannot_satisfy_three_evidence_alert_gate() -> None:
    repository = seed_repository(InMemoryRepository())
    event = repository.get_event("evt-open-model")
    assert event is not None and len(event.evidence) >= 3
    evidence = [
        item.model_copy(update={
            "provenance_level": "provider_verified" if index < 2 else "unverified_discovery",
        })
        for index, item in enumerate(event.evidence)
    ]
    repository.upsert_event(event.model_copy(update={"evidence": evidence, "evidence_count": 2}))
    repository.add_alert(
        AlertRuleRequest(name="strong", minimumAttention=0, minimumEvidenceStrength=0),
        "workspace-a", "analyst-a",
    )
    summary = await AlertDispatcher(repository, signing_secret="test-secret").dispatch_event(
        event.id, ["workspace-a"], NOW,
    )
    assert summary.evaluated == 1
    assert summary.delivered == 0
    assert repository.alert_deliveries == []


@pytest.mark.asyncio
async def test_webhook_payload_excludes_unverified_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    repository = seed_repository(InMemoryRepository())
    event = repository.get_event("evt-open-model")
    assert event is not None and len(event.evidence) >= 4
    evidence = [
        item.model_copy(update={
            "provenance_level": "provider_verified" if index < 3 else "unverified_discovery",
        })
        for index, item in enumerate(event.evidence)
    ]
    repository.upsert_event(event.model_copy(update={"evidence": evidence, "evidence_count": 3}))
    repository.add_alert(
        AlertRuleRequest(
            name="verified only", minimumAttention=0, minimumEvidenceStrength=0,
            webhookUrl="https://hooks.example/radar",
        ),
        "workspace-a", "analyst-a",
    )
    captured: dict[str, object] = {}

    async def fake_delivery(
        _url: str, payload: dict[str, object], *_args: object, **_kwargs: object,
    ) -> WebhookResult:
        captured.update(payload)
        return WebhookResult(204, True)

    monkeypatch.setattr(alert_worker_module, "deliver_webhook", fake_delivery)
    summary = await AlertDispatcher(repository, signing_secret="test-secret").dispatch_event(
        event.id, ["workspace-a"], NOW,
    )
    assert summary.delivered == 1
    delivered_evidence = captured["evidence"]
    assert isinstance(delivered_evidence, list) and len(delivered_evidence) == 3
    assert all(item["provenanceLevel"] == "provider_verified" for item in delivered_evidence)


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
    required = {"id", "priority", "signalFamilies", "discovery", "incrementalKey", "refresh", "backfill", "quotaModel", "worstCaseCostRmbPerRun", "rightsPolicyId", "rightsStatus", "storedFields", "deletion", "productionState", "acceptanceState"}
    assert len(payload["connectors"]) >= 8
    assert all(required.issubset(connector) for connector in payload["connectors"])
    assert all(connector["productionState"] == "disabled" for connector in payload["connectors"] if connector["id"] == "x")
    bluesky = next(connector for connector in payload["connectors"] if connector["id"] == "bluesky")
    assert bluesky["rightsStatus"] == "experimental"
    assert bluesky["productionState"] == "disabled-by-default"
    assert bluesky["acceptanceState"] == "blocked-on-rights-and-72h-soak"
    assert all(connector["rightsStatus"] != "active" for connector in payload["connectors"])


def test_production_compose_and_examples_keep_service_secrets_isolated() -> None:
    root = Path(__file__).parents[3]
    compose = (root / "infra" / "compose.production.yml").read_text(encoding="utf-8")
    assert "RADAR_ENV_FILE" not in compose
    assert "RADAR_API_ENV_FILE" in compose
    assert "RADAR_SCHEDULER_ENV_FILE" in compose
    assert "RADAR_ALERT_ENV_FILE" in compose
    assert "RADAR_SOURCE_IDENTITIES_FILE" in compose
    assert "RADAR_RSS_FEEDS_FILE" in compose
    assert "RSS_FEEDS_FILE: /run/config/feeds.json" in compose
    assert "build:" not in compose

    api = (root / ".env.api.example").read_text(encoding="utf-8")
    scheduler = (root / ".env.scheduler.example").read_text(encoding="utf-8")
    alert = (root / ".env.alert.example").read_text(encoding="utf-8")
    assert not any(secret in api for secret in (
        "R2_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "YOUTUBE_API_KEY", "X_BEARER_TOKEN",
        "WEBHOOK_SIGNING_SECRET", "BGE_M3_API_KEY",
    ))
    assert not any(secret in scheduler for secret in (
        "DELETION_DATABASE_URL", "WEBHOOK_SIGNING_SECRET",
        "SCORE_LEDGER_ED25519_PRIVATE_KEY",
    ))
    assert not any(secret in alert for secret in (
        "DELETION_DATABASE_URL", "GITHUB_TOKEN", "YOUTUBE_API_KEY", "X_BEARER_TOKEN",
        "BGE_M3_API_KEY", "SCORE_LEDGER_ED25519_PRIVATE_KEY",
    ))


def test_stream_retention_uses_oldest_group_pending_or_delivery_watermark() -> None:
    watermarks = [
        ConsumerGroupWatermark("alerts", "100-0", 2, "80-0", "80-0"),
        ConsumerGroupWatermark("deletions", "90-0", 0, None, "90-0"),
    ]
    assert minimum_stream_id("120-0", *(item.safe_boundary_id for item in watermarks)) == "80-0"
    assert minimum_stream_id("70-0", *(item.safe_boundary_id for item in watermarks)) == "70-0"
    assert stream_id_key("80-2") == (80, 2)

    ensure_watermarks_did_not_move_backwards(
        watermarks,
        [
            ConsumerGroupWatermark("alerts", "110-0", 1, "90-0", "90-0"),
            ConsumerGroupWatermark("deletions", "100-0", 0, None, "100-0"),
        ],
    )
    with pytest.raises(RuntimeError, match="moved backwards: alerts"):
        ensure_watermarks_did_not_move_backwards(
            watermarks,
            [
                ConsumerGroupWatermark("alerts", "79-0", 0, None, "79-0"),
                ConsumerGroupWatermark("deletions", "100-0", 0, None, "100-0"),
            ],
        )

    plain_keys = outbox_recovery_keys("radar:events")
    assert all("{radar:events}" in key for key in plain_keys)
    assert len({key_slot(value.encode()) for value in ("radar:events", *plain_keys)}) == 1
    tagged_keys = outbox_recovery_keys("radar:{events}:stream")
    assert all("{events}" in key for key in tagged_keys)
    assert len({key_slot(value.encode()) for value in ("radar:{events}:stream", *tagged_keys)}) == 1
    with pytest.raises(ValueError, match="malformed Redis hash tag"):
        outbox_recovery_keys("radar:{}:stream")
    with pytest.raises(ValueError, match="must not be empty"):
        outbox_recovery_keys("")


@pytest.mark.asyncio
async def test_stream_retention_cli_requires_stream_and_reviewed_boundary_confirmation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused")
    monkeypatch.setenv("REDIS_URL", "redis://unused")
    missing_boundary = retention_tool.parser().parse_args([
        "--stream", "radar:events", "--execute", "--confirm-stream", "radar:events",
    ])
    with pytest.raises(SystemExit, match="--confirm-before-id is required"):
        await retention_tool.run(missing_boundary)

    wrong_stream = retention_tool.parser().parse_args([
        "--stream", "radar:events", "--execute", "--confirm-stream", "radar:other",
        "--confirm-before-id", "1-0",
    ])
    with pytest.raises(SystemExit, match="--confirm-stream must exactly match"):
        await retention_tool.run(wrong_stream)


@pytest.mark.asyncio
async def test_stream_retention_cancellation_releases_a_late_postgres_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = threading.Event()
    finish = threading.Event()
    released = threading.Event()
    guard = object()

    def slow_acquire(_dsn: str, _path: Path) -> object:
        started.set()
        assert finish.wait(timeout=5)
        return guard

    def release(value: object) -> None:
        assert value is guard
        released.set()

    monkeypatch.setattr(retention_module, "lock_outbox_ledger", slow_acquire)
    monkeypatch.setattr(retention_module, "release_locked_outbox_ledger", release)
    task = asyncio.create_task(retention_module.acquire_locked_outbox_ledger(
        "postgresql://unused", Path("unused.sqlite3"),
    ))
    assert await asyncio.to_thread(started.wait, 5)
    task.cancel()
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert released.wait(timeout=5)

    redis_started = asyncio.Event()
    redis_finish = asyncio.Event()
    redis_completed = asyncio.Event()

    class SlowRedis:
        async def eval(self, *_args: object) -> list[int]:
            redis_started.set()
            await redis_finish.wait()
            redis_completed.set()
            return [1, 0]

    atomic_trim = asyncio.create_task(
        retention_module.trim_stream_at_verified_watermarks(
            SlowRedis(),  # type: ignore[arg-type]
            "radar:events",
            "exclusive-token",
            "80-0",
            [ConsumerGroupWatermark("alerts", "100-0", 2, "80-0", "80-0")],
        ),
    )
    await redis_started.wait()
    atomic_trim.cancel()
    await asyncio.sleep(0)
    assert not atomic_trim.done()
    redis_finish.set()
    with pytest.raises(asyncio.CancelledError):
        await atomic_trim
    assert redis_completed.is_set()


@pytest.mark.asyncio
async def test_worker_cannot_promote_unapproved_connector_rights() -> None:
    class EmptyXConnector(BaseConnector):
        id = "x"
        platform = "X"
        signal_family = "discussion"

        async def collect(self) -> list[Observation]:
            raise AssertionError("unapproved connector must never perform network collection")

    repository = InMemoryRepository()
    connector = EmptyXConnector()
    try:
        result = await CollectorWorker(repository, [connector]).run_connector(connector)
    finally:
        await connector.close()
    assert result.failed is False
    assert result.skipped is True
    status = repository.get_connector("x")
    assert status.rights_status == "blocked"
    assert status.status == "paused"
    assert status.coverage == 0
    assert "生产采集已安全暂停" in status.note


def test_frozen_feature_registry_is_complete_and_controls_metric_roles() -> None:
    registry = load_feature_registry()
    assert registry["frozen"] is True
    assert len(registry["features"]) >= 20
    roles = behavior_metric_roles()
    assert roles[EventType.DEVELOPER_TOOL_RELEASE]["stars"] == "secondary_intent"
    assert roles[EventType.OFFICIAL_PRODUCT_RELEASE]["views"] == "attention_only"
    assert roles[EventType.SECURITY_INCIDENT]["patch_downloads"] == "primary_response"
