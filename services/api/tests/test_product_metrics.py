from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from radar.auth import Role
from radar.contracts import FeedbackRequest, LifecycleState, Observation
from radar.fixtures import demo_events
from radar.main import create_app
from radar.product_metrics import (
    ProductMetricMinimumSamples,
    beta_product_metrics,
    duty_seconds_between,
    is_in_duty_window,
    load_product_metric_policy,
)
from radar.storage import AUDIT_TRIGGER_SPECS, InMemoryRepository, audit_trigger_specs_verified


UTC = timezone.utc


def at_local(hour: int, minute: int = 0, *, day: int = 13) -> datetime:
    """2026-07-13 is Monday in Asia/Shanghai (UTC+8)."""
    return datetime(2026, 7, day, hour - 8, minute, tzinfo=UTC)


def low_sample_policy():
    policy = load_product_metric_policy()
    return policy.model_copy(update={
        "frozen_at": at_local(8),
        "minimum_samples": ProductMetricMinimumSamples(
            strongAlerts=2, alertWorkdays=1, triageEvents=2, completedReviews=2,
        ),
    })


def server_timed(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Upgrade test interaction fixtures to server-received heartbeat v2."""
    upgraded = [{**row, "metadata": dict(row.get("metadata", {}))} for row in rows]
    opens = {
        str(row["metadata"].get("reviewAttemptId")): row
        for row in upgraded if row.get("kind") == "detail_opened" and row["metadata"].get("reviewAttemptId")
    }
    heartbeats: list[dict[str, object]] = []
    for attempt_id, opened in opens.items():
        segment_id = f"segment-{attempt_id}"
        opened["metadata"].update({"segmentId": segment_id, "measurementVersion": "server-heartbeat-v2"})
        terminal = next((
            row for row in upgraded
            if row.get("kind") in {"triage_submitted", "review_segment_closed"}
            and row["metadata"].get("reviewAttemptId") == attempt_id
        ), None)
        if terminal is None:
            continue
        terminal["metadata"].update({
            "segmentId": segment_id, "measurementVersion": "server-heartbeat-v2",
            "idleTimeoutSeconds": 60, "tickCapSeconds": 5,
        })
        terminal["metadata"].pop("activeSeconds", None)
        terminal["metadata"].pop("externalWaitSeconds", None)
        started = opened["occurredAt"]
        ended = terminal["occurredAt"]
        sequence = 0
        cursor = started
        while cursor <= ended:
            heartbeats.append({
                "kind": "review_heartbeat", "eventId": terminal["eventId"], "actorId": terminal["actorId"],
                "sessionId": terminal.get("sessionId", "server-session"),
                "metadata": {
                    "reviewAttemptId": attempt_id,
                    "queueEligibilityKey": terminal["metadata"]["queueEligibilityKey"],
                    "segmentId": segment_id, "sequence": sequence, "state": "active",
                    "measurementVersion": "server-heartbeat-v2", "idleTimeoutSeconds": 60, "tickCapSeconds": 5,
                },
                "occurredAt": cursor,
            })
            sequence += 1
            cursor += timedelta(seconds=5)
    return upgraded + heartbeats


def test_frozen_policy_and_duty_clock_exclude_nights_and_weekends() -> None:
    policy = load_product_metric_policy()
    friday_1700 = datetime(2026, 7, 17, 9, 0, tzinfo=UTC)
    monday_1000 = datetime(2026, 7, 20, 2, 0, tzinfo=UTC)
    assert policy.version == "product-metrics-2026-07-rc2.9"
    assert policy.status == "frozen_for_beta_collection"
    assert is_in_duty_window(friday_1700, policy)
    assert duty_seconds_between(friday_1700, monday_1000, policy) == 2 * 3600


def test_beta_metrics_use_deliveries_durable_feedback_dual_review_and_linked_timing() -> None:
    policy = low_sample_policy()
    deliveries = [
        {
            "eventId": "event-a", "idempotencyKey": "alert-key-a", "status": "delivered",
            "deliveredAt": at_local(9), "evidenceStrength": "high",
        },
        {
            "eventId": "event-b", "idempotencyKey": "alert-key-b", "status": "delivered",
            "deliveredAt": at_local(9, 10), "evidenceStrength": "high",
        },
    ]
    feedback = [
        {"id": "feedback-a", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm", "createdAt": at_local(9, 10), "queueEligibilityKey": "queue-key-a", "alertDeliveryKey": "alert-key-a"},
        {"id": "feedback-b", "eventId": "event-b", "actorId": "analyst-b", "action": "reject", "createdAt": at_local(9, 30), "queueEligibilityKey": "queue-key-b", "alertDeliveryKey": "alert-key-b"},
    ]
    queue_entries = [
        {"eventId": "event-a", "eligibilityKey": "queue-key-a", "eligibleAt": at_local(9)},
        {"eventId": "event-b", "eligibilityKey": "queue-key-b", "eligibleAt": at_local(9, 10)},
    ]
    interactions = [
        {
            "kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
            "metadata": {"reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-key-a"}, "occurredAt": at_local(9, 7),
        },
        {
            "kind": "detail_opened", "eventId": "event-b", "actorId": "analyst-b", "sessionId": "session-b",
            "metadata": {"reviewAttemptId": "attempt-b", "queueEligibilityKey": "queue-key-b"}, "occurredAt": at_local(9, 24),
        },
        {
            "kind": "alert_quality_reviewed", "eventId": "event-a", "actorId": "reviewer-a",
            "metadata": {"alertDeliveryKey": "alert-key-a", "verdict": "valid"},
            "occurredAt": at_local(9, 15),
        },
        {
            "kind": "alert_quality_reviewed", "eventId": "event-a", "actorId": "reviewer-b",
            "metadata": {"alertDeliveryKey": "alert-key-a", "verdict": "valid"},
            "occurredAt": at_local(9, 16),
        },
        {
            "kind": "alert_quality_reviewed", "eventId": "event-b", "actorId": "reviewer-a",
            "metadata": {"alertDeliveryKey": "alert-key-b", "verdict": "incorrect_cluster"},
            "occurredAt": at_local(10),
        },
        {
            "kind": "alert_quality_reviewed", "eventId": "event-b", "actorId": "reviewer-b",
            "metadata": {"alertDeliveryKey": "alert-key-b", "verdict": "insufficient_evidence"},
            "occurredAt": at_local(10, 5),
        },
        {
            "kind": "triage_submitted", "eventId": "event-a", "actorId": "analyst-a",
            "sessionId": "session-a",
            "metadata": {
                "action": "confirm", "feedbackId": "feedback-a", "activeSeconds": 120,
                "reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-key-a",
                "externalWaitSeconds": 30, "measurementVersion": "foreground-active-v1",
                "idleTimeoutSeconds": 60, "tickCapSeconds": 5,
            },
            "occurredAt": at_local(9, 10),
        },
        {
            "kind": "triage_submitted", "eventId": "event-b", "actorId": "analyst-b",
            "sessionId": "session-b",
            "metadata": {
                "action": "reject", "feedbackId": "feedback-b", "activeSeconds": 240,
                "reviewAttemptId": "attempt-b", "queueEligibilityKey": "queue-key-b",
                "externalWaitSeconds": 90, "measurementVersion": "foreground-active-v1",
                "idleTimeoutSeconds": 60, "tickCapSeconds": 5,
            },
            "occurredAt": at_local(9, 30),
        },
    ]
    interactions = server_timed(interactions)
    result = beta_product_metrics(
        deliveries=deliveries,
        feedback=feedback,
        queue_entries=queue_entries,
        interactions=interactions,
        as_of=at_local(11),
        policy=policy,
    )

    acceptance = result["strongAlertAcceptance"]
    assert acceptance["denominatorDeliveredStrongAlerts"] == 2
    assert acceptance["numeratorAcceptedWithinOneWorkday"] == 1
    assert acceptance["rate"] == 0.5
    assert acceptance["passesTarget"] is False

    errors = result["erroneousStrongAlerts"]
    assert errors["erroneousStrongAlerts"] == 1
    assert errors["alertWorkdays"] == 1
    assert errors["errorRate"] == 0.5
    assert errors["dualReviewCoverage"] == 1
    assert errors["passesTarget"] is False

    sla = result["firstTriageSla"]
    assert sla["sample"] == 2
    assert sla["withinSla"] == 1
    assert sla["rate"] == 0.5
    assert sla["passesTarget"] is False

    active = result["activeReviewTime"]
    assert active["sample"] == 2
    assert active["medianActiveSeconds"] == 270
    assert active["medianExternalWaitSeconds"] == 0
    assert active["passesTarget"] is True
    assert result["serverObservedReviewTime"]["medianServerObservedSeconds"] == 270
    assert result["evidenceStatus"] == "eligible"
    assert result["passesMeasuredGates"] is False


def test_explicit_exclusions_are_separate_and_insufficient_samples_never_pass() -> None:
    policy = load_product_metric_policy().model_copy(update={"frozen_at": at_local(8)})
    result = beta_product_metrics(
        deliveries=[
            {"eventId": "event-a", "idempotencyKey": "alert-original", "status": "delivered", "deliveredAt": at_local(9)},
            {"eventId": "event-a", "idempotencyKey": "alert-duplicate", "status": "delivered", "deliveredAt": at_local(9, 1)},
        ],
        feedback=[
            {"id": "feedback-a", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm", "createdAt": at_local(9, 5), "alertDeliveryKey": "alert-original"},
        ],
        queue_entries=[
            {"eventId": "event-a", "eligibilityKey": "queue-off-duty", "eligibleAt": at_local(20)},
        ],
        interactions=[
            {
                "kind": "metric_exclusion_recorded", "actorId": "owner-a",
                "metadata": {
                    "targetType": "alert", "targetKey": "alert-duplicate", "canonicalKey": "alert-original",
                    "incidentId": "incident-worker-retry", "reason": "system_fault_duplicate",
                    "incidentFactDigest": "sha256:" + "a" * 64,
                },
                "occurredAt": at_local(9, 2),
            },
        ],
        as_of=at_local(21),
        policy=policy,
    )
    acceptance = result["strongAlertAcceptance"]
    assert acceptance["denominatorDeliveredStrongAlerts"] == 1
    assert acceptance["excluded"] == {"system_fault_duplicate": 1}
    assert acceptance["rate"] == 1
    assert acceptance["evidenceStatus"] == "insufficient"
    assert acceptance["passesTarget"] is None
    assert result["firstTriageSla"]["noDutyExcluded"] == 1
    assert result["passesMeasuredGates"] is None


def test_acceptance_counts_a_confirm_after_observe_within_the_same_workday() -> None:
    result = beta_product_metrics(
        deliveries=[
            {"eventId": "event-a", "idempotencyKey": "alert-original", "status": "delivered", "deliveredAt": at_local(9)},
        ],
        feedback=[
            {"id": "feedback-observe", "eventId": "event-a", "actorId": "analyst-a", "action": "observe", "createdAt": at_local(9, 5), "alertDeliveryKey": "alert-original"},
            {"id": "feedback-confirm", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm", "createdAt": at_local(9, 20), "alertDeliveryKey": "alert-original"},
        ],
        queue_entries=[],
        interactions=[],
        as_of=at_local(10),
        policy=load_product_metric_policy().model_copy(update={"frozen_at": at_local(8)}),
    )
    acceptance = result["strongAlertAcceptance"]
    assert acceptance["numeratorAcceptedWithinOneWorkday"] == 1
    assert acceptance["needsObservation"] == 0


def test_owner_can_reinstate_a_metric_row_without_deleting_the_exclusion_audit_trail() -> None:
    policy = load_product_metric_policy().model_copy(update={"frozen_at": at_local(8)})
    result = beta_product_metrics(
        deliveries=[
            {"eventId": "event-a", "idempotencyKey": "alert-original", "status": "delivered", "deliveredAt": at_local(9)},
        ],
        feedback=[],
        queue_entries=[],
        interactions=[
            {
                "kind": "metric_exclusion_recorded", "actorId": "owner-a",
                "metadata": {"targetType": "alert", "targetKey": "alert-original", "reason": "system_fault_duplicate"},
                "occurredAt": at_local(9, 1),
            },
            {
                "kind": "metric_exclusion_reinstated", "actorId": "owner-b",
                "metadata": {"targetType": "alert", "targetKey": "alert-original", "reason": "operator_correction"},
                "occurredAt": at_local(9, 2),
            },
        ],
        as_of=at_local(18, day=14),
        policy=policy,
    )
    assert result["strongAlertAcceptance"]["denominatorDeliveredStrongAlerts"] == 1
    assert result["strongAlertAcceptance"]["excluded"] == {}


def test_rows_before_policy_freeze_or_after_as_of_cannot_enter_beta_denominators() -> None:
    policy = load_product_metric_policy().model_copy(update={"frozen_at": at_local(9)})
    result = beta_product_metrics(
        deliveries=[
            {"eventId": "before", "idempotencyKey": "alert-before", "status": "delivered", "deliveredAt": at_local(8, 59)},
            {"eventId": "inside", "idempotencyKey": "alert-inside", "status": "delivered", "deliveredAt": at_local(9, 1)},
            {"eventId": "future", "idempotencyKey": "alert-future", "status": "delivered", "deliveredAt": at_local(10, 1, day=15)},
        ],
        feedback=[],
        queue_entries=[],
        interactions=[],
        as_of=at_local(18, day=14),
        policy=policy,
    )
    assert result["strongAlertAcceptance"]["denominatorDeliveredStrongAlerts"] == 1
    diagnostics = result["collectionWindow"]["excludedOutsideFrozenWindow"]["deliveries"]
    assert diagnostics == {"beforeFreeze": 1, "afterAsOf": 1, "missingOrInvalidTimestamp": 0}


def test_open_alert_window_is_pending_not_a_failed_acceptance_row() -> None:
    result = beta_product_metrics(
        deliveries=[{"eventId": "event-a", "idempotencyKey": "alert-a", "status": "delivered", "deliveredAt": at_local(9)}],
        feedback=[], queue_entries=[], interactions=[], as_of=at_local(9, 1),
        policy=load_product_metric_policy().model_copy(update={"frozen_at": at_local(8)}),
    )
    assert result["strongAlertAcceptance"]["denominatorDeliveredStrongAlerts"] == 0
    assert result["strongAlertAcceptance"]["pendingWindowOpen"] == 1
    assert result["strongAlertAcceptance"]["unhandled"] == 0


def test_reentered_event_feedback_is_bound_to_the_latest_queue_epoch() -> None:
    result = beta_product_metrics(
        deliveries=[],
        feedback=[{
            "id": "feedback-latest", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm",
            "createdAt": at_local(10, 5), "queueEligibilityKey": "queue-second",
        }],
        queue_entries=[
            {"eventId": "event-a", "eligibilityKey": "queue-first", "eligibleAt": at_local(9)},
            {"eventId": "event-a", "eligibilityKey": "queue-second", "eligibleAt": at_local(10)},
        ],
        interactions=[], as_of=at_local(11), policy=low_sample_policy(),
    )
    assert result["firstTriageSla"]["sample"] == 2
    assert result["firstTriageSla"]["withinSla"] == 1


def test_active_review_counts_one_completed_review_per_queue_epoch() -> None:
    queue = [{"eventId": "event-a", "eligibilityKey": "queue-a", "eligibleAt": at_local(9)}]
    feedback = [
        {"id": f"feedback-{index}", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm",
         "createdAt": at_local(9, 5 + index), "queueEligibilityKey": "queue-a"}
        for index in range(2)
    ]
    interactions = [
        {"kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": f"session-{index}",
         "metadata": {"reviewAttemptId": f"attempt-{index}", "queueEligibilityKey": "queue-a"}, "occurredAt": at_local(9, 3 + 3 * index)}
        for index in range(2)
    ] + [
        {"kind": "triage_submitted", "eventId": "event-a", "actorId": "analyst-a", "sessionId": f"session-{index}",
         "metadata": {"action": "confirm", "feedbackId": f"feedback-{index}", "activeSeconds": 60,
                      "reviewAttemptId": f"attempt-{index}", "queueEligibilityKey": "queue-a",
                      "externalWaitSeconds": 0, "measurementVersion": "foreground-active-v1",
                      "idleTimeoutSeconds": 60, "tickCapSeconds": 5},
         "occurredAt": at_local(9, 5 + index)}
        for index in range(2)
    ]
    interactions = server_timed(interactions)
    result = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=interactions,
        as_of=at_local(10), policy=low_sample_policy(),
    )
    assert result["activeReviewTime"]["sample"] == 1
    assert result["activeReviewTime"]["rejectedOrUnlinkedTelemetry"] == 1


def test_active_review_accumulates_reopened_segments_and_requires_telemetry_coverage() -> None:
    policy = low_sample_policy().model_copy(update={
        "minimum_samples": ProductMetricMinimumSamples(
            strongAlerts=2, alertWorkdays=1, triageEvents=2, completedReviews=1,
        ),
    })
    queue = [{"eventId": "event-a", "eligibilityKey": "queue-a", "eligibleAt": at_local(9)}]
    feedback = [{
        "id": "feedback-a", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm",
        "createdAt": at_local(9, 12), "queueEligibilityKey": "queue-a",
    }]
    interactions = [
        {"kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
         "metadata": {"reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-a"}, "occurredAt": at_local(9)},
        {"kind": "review_segment_closed", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
         "metadata": {"reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-a", "activeSeconds": 600,
                      "externalWaitSeconds": 0, "measurementVersion": "foreground-active-v1",
                      "idleTimeoutSeconds": 60, "tickCapSeconds": 5}, "occurredAt": at_local(9, 10)},
        {"kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-b",
         "metadata": {"reviewAttemptId": "attempt-b", "queueEligibilityKey": "queue-a"}, "occurredAt": at_local(9, 11)},
        {"kind": "triage_submitted", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-b",
         "metadata": {"action": "confirm", "feedbackId": "feedback-a", "reviewAttemptId": "attempt-b",
                      "queueEligibilityKey": "queue-a", "activeSeconds": 10, "externalWaitSeconds": 0,
                      "measurementVersion": "foreground-active-v1", "idleTimeoutSeconds": 60, "tickCapSeconds": 5},
         "occurredAt": at_local(9, 12)},
    ]
    interactions = server_timed(interactions)
    result = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=interactions,
        as_of=at_local(10), policy=policy,
    )
    assert result["activeReviewTime"]["medianActiveSeconds"] == 660
    assert result["activeReviewTime"]["telemetryCoverage"] == 1

    missing = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=[],
        as_of=at_local(10), policy=policy,
    )["activeReviewTime"]
    assert missing["sample"] == 1
    assert missing["validTelemetrySample"] == 0
    assert missing["telemetryCoverage"] == 0
    assert missing["evidenceStatus"] == "insufficient"


def test_heartbeat_state_affects_active_time_but_cannot_reduce_server_wall_guardrail() -> None:
    import copy

    policy = low_sample_policy().model_copy(update={
        "minimum_samples": ProductMetricMinimumSamples(
            strongAlerts=2, alertWorkdays=1, triageEvents=2, completedReviews=1,
        ),
    })
    queue = [{"eventId": "event-a", "eligibilityKey": "queue-a", "eligibleAt": at_local(9)}]
    feedback = [{
        "id": "feedback-a", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm",
        "createdAt": at_local(9, 1), "queueEligibilityKey": "queue-a",
    }]
    interactions = server_timed([
        {
            "kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
            "metadata": {"reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-a"},
            "occurredAt": at_local(9),
        },
        {
            "kind": "triage_submitted", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
            "metadata": {
                "action": "confirm", "feedbackId": "feedback-a", "reviewAttemptId": "attempt-a",
                "queueEligibilityKey": "queue-a",
            },
            "occurredAt": at_local(9, 1),
        },
    ])
    active_result = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=interactions,
        as_of=at_local(10), policy=policy,
    )
    idle_interactions = copy.deepcopy(interactions)
    for row in idle_interactions:
        if row["kind"] == "review_heartbeat":
            row["metadata"]["state"] = "idle"
    idle_result = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=idle_interactions,
        as_of=at_local(10), policy=policy,
    )
    assert active_result["activeReviewTime"]["medianActiveSeconds"] == 60
    assert idle_result["activeReviewTime"]["medianActiveSeconds"] == 0
    assert active_result["serverObservedReviewTime"]["medianServerObservedSeconds"] == 60
    assert idle_result["serverObservedReviewTime"]["medianServerObservedSeconds"] == 60


def test_heartbeat_sequence_gap_rejects_timing_sample() -> None:
    policy = low_sample_policy().model_copy(update={
        "minimum_samples": ProductMetricMinimumSamples(
            strongAlerts=2, alertWorkdays=1, triageEvents=2, completedReviews=1,
        ),
    })
    queue = [{"eventId": "event-a", "eligibilityKey": "queue-a", "eligibleAt": at_local(9)}]
    feedback = [{
        "id": "feedback-a", "eventId": "event-a", "actorId": "analyst-a", "action": "confirm",
        "createdAt": at_local(9, 1), "queueEligibilityKey": "queue-a",
    }]
    interactions = server_timed([
        {
            "kind": "detail_opened", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
            "metadata": {"reviewAttemptId": "attempt-a", "queueEligibilityKey": "queue-a"},
            "occurredAt": at_local(9),
        },
        {
            "kind": "triage_submitted", "eventId": "event-a", "actorId": "analyst-a", "sessionId": "session-a",
            "metadata": {
                "action": "confirm", "feedbackId": "feedback-a", "reviewAttemptId": "attempt-a",
                "queueEligibilityKey": "queue-a",
            },
            "occurredAt": at_local(9, 1),
        },
    ])
    interactions = [
        row for row in interactions
        if row["kind"] != "review_heartbeat" or row["metadata"]["sequence"] != 3
    ]
    result = beta_product_metrics(
        deliveries=[], feedback=feedback, queue_entries=queue, interactions=interactions,
        as_of=at_local(10), policy=policy,
    )
    assert result["activeReviewTime"]["validTelemetrySample"] == 0
    assert result["activeReviewTime"]["evidenceStatus"] == "insufficient"
    assert result["serverObservedReviewTime"]["validTelemetrySample"] == 0


def test_repository_binds_feedback_and_retains_hash_only_quality_history_after_purge() -> None:
    repository = InMemoryRepository()
    event = demo_events()[0]
    before = datetime.now(UTC)
    repository.upsert_event(event.model_copy(update={"updated_at": at_local(8)}))
    assert repository.review_queue_entries[0]["eligibleAt"] >= before
    repository.record_alert_delivery({
        "workspaceId": "workspace-a", "eventId": event.id, "idempotencyKey": "alert-current",
        "status": "delivered", "deliveredAt": datetime.now(UTC),
    })
    receipt = repository.add_feedback(
        FeedbackRequest(
            eventId=event.id, action="confirm", reason="durable linked decision",
            queueEligibilityKey=repository.review_queue_entries[0]["eligibilityKey"], alertDeliveryKey="alert-current",
        ),
        "workspace-a", "analyst-a",
    )
    row = next(item for item in repository.list_feedback("workspace-a", before) if item["id"] == receipt.id)
    assert row["queueEligibilityKey"] == repository.review_queue_entries[0]["eligibilityKey"]
    assert row["alertDeliveryKey"] == "alert-current"

    base = datetime.now(UTC)
    for item_id in ("duplicate-a", "duplicate-b"):
        repository.save_observation_with_outbox(Observation(
            id=item_id, platform="RSS", externalId=item_id, sourceId="purge-me", publishedAt=base,
            collectedAt=base, language="en", title="same", text="same", url=f"https://example.com/{item_id}",
            metrics={}, rawEvidenceRef=f"r2://raw/{item_id}.json", contentFingerprint="same-fingerprint",
            signalFamily="discussion", rightsPolicyId="rss-public-metadata-v1",
        ))
    assert repository.persisted_content_duplicate_stats(before) == (2, 1)
    repository.purge_source("purge-me")
    assert repository.persisted_content_duplicate_stats(before) == (2, 1)


def test_stale_queue_epoch_is_rejected_with_conflict() -> None:
    repository = InMemoryRepository()
    with TestClient(create_app(repository)) as http:
        old_key = next(
            row["eligibilityKey"] for row in reversed(repository.review_queue_entries)
            if row["eventId"] == "evt-open-model"
        )
        event = repository.get_event("evt-open-model")
        assert event is not None
        repository.upsert_event(event.model_copy(update={"state": LifecycleState.DORMANT}))
        repository.upsert_event(event.model_copy(update={"state": LifecycleState.DETECTED}))
        assert repository.review_queue_entries[-1]["eligibilityKey"] != old_key
        response = http.post("/api/v1/feedback", json={
            "eventId": event.id, "action": "confirm", "reason": "stale tab decision",
            "queueEligibilityKey": old_key,
        })
        assert response.status_code == 409


def test_queue_eligibility_is_immutable_transition_fact() -> None:
    repository = InMemoryRepository()
    event = demo_events()[0]
    repository.upsert_event(event)
    assert len(repository.review_queue_entries) == 1
    first_key = repository.review_queue_entries[0]["eligibilityKey"]

    repository.upsert_event(event.model_copy(update={"updated_at": event.updated_at + timedelta(minutes=15)}))
    assert len(repository.review_queue_entries) == 1

    repository.upsert_event(event.model_copy(update={"state": LifecycleState.NOISE}))
    repository.upsert_event(event.model_copy(update={"state": LifecycleState.DETECTED, "updated_at": event.updated_at + timedelta(minutes=30)}))
    assert len(repository.review_queue_entries) == 2
    assert repository.review_queue_entries[1]["eligibilityKey"] != first_key


def test_beta_metrics_api_is_role_guarded_and_system_queue_events_cannot_be_spoofed(monkeypatch) -> None:
    keys = {
        "viewer-key": {"subject": "viewer", "role": Role.VIEWER.name, "workspaceId": "workspace-a"},
        "analyst-key": {"subject": "analyst", "role": Role.ANALYST.name, "workspaceId": "workspace-a"},
        "owner-key": {"subject": "owner", "role": Role.OWNER.name, "workspaceId": "workspace-a"},
    }
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("RADAR_API_KEYS", __import__("json").dumps(keys))
    repository = InMemoryRepository()
    now = datetime.now(UTC)
    repository.record_alert_delivery({
        "workspaceId": "workspace-a", "eventId": "evt-open-model", "idempotencyKey": "alert-canonical-key",
        "status": "delivered", "deliveredAt": now - timedelta(minutes=2),
    })
    repository.record_alert_delivery({
        "workspaceId": "workspace-a", "eventId": "evt-open-model", "idempotencyKey": "alert-target-key",
        "status": "delivered", "deliveredAt": now - timedelta(minutes=1),
    })
    with TestClient(create_app(repository)) as http:
        assert http.get("/api/v1/metrics/beta", headers={"X-API-Key": "viewer-key"}).status_code == 403
        payload = http.get("/api/v1/metrics/beta", headers={"X-API-Key": "analyst-key"})
        assert payload.status_code == 200
        assert payload.json()["metricScope"] == "rc2_beta_product_metrics"
        spoof = http.post("/api/v1/interactions", headers={"X-API-Key": "owner-key"}, json={
            "kind": "queue_eligible", "sessionId": "system-session", "idempotencyKey": "queue-spoof-1",
            "eventId": "evt-open-model", "metadata": {"eligibilityKey": "queue-key-spoof", "policyVersion": "fake"},
        })
        assert spoof.status_code == 403
        malformed_timing = http.post("/api/v1/interactions", headers={"X-API-Key": "analyst-key"}, json={
            "kind": "triage_submitted", "sessionId": "analyst-session", "idempotencyKey": "timing-invalid-1",
            "eventId": "evt-open-model", "metadata": {"action": "confirm", "feedbackId": "feedback-without-clock"},
        })
        assert malformed_timing.status_code == 422
        analyst_exclusion = http.post("/api/v1/interactions", headers={"X-API-Key": "analyst-key"}, json={
            "kind": "metric_exclusion_recorded", "sessionId": "analyst-session", "idempotencyKey": "exclusion-denied-1",
            "eventId": "evt-open-model",
            "metadata": {"targetType": "alert", "targetKey": "alert-target-key", "canonicalKey": "alert-canonical-key", "incidentId": "incident-worker-retry", "reason": "system_fault_duplicate", "note": "duplicate caused by a worker retry"},
        })
        assert analyst_exclusion.status_code == 403
        incident = http.post("/api/v1/metric-incidents", headers={"X-API-Key": "owner-key"}, json={
            "eventId": "evt-open-model", "targetKey": "alert-target-key", "canonicalKey": "alert-canonical-key",
            "cause": "worker_retry_after_timeout", "note": "worker timed out after provider accepted the canonical alert",
        })
        assert incident.status_code == 201
        exclusion = http.post("/api/v1/interactions", headers={"X-API-Key": "owner-key"}, json={
            "kind": "metric_exclusion_recorded", "sessionId": "owner-session-1", "idempotencyKey": "exclusion-owner-1",
            "eventId": "evt-open-model",
            "metadata": {
                "targetType": "alert", "targetKey": "alert-target-key", "canonicalKey": "alert-canonical-key",
                "incidentId": incident.json()["id"], "reason": "system_fault_duplicate",
                "note": "duplicate caused by the recorded worker retry incident",
            },
        })
        assert exclusion.status_code == 202
        assert str(repository.product_interactions[-1]["metadata"]["incidentFactDigest"]).startswith("sha256:")
        viewer_quality = http.post("/api/v1/interactions", headers={"X-API-Key": "viewer-key"}, json={
            "kind": "alert_quality_reviewed", "sessionId": "viewer-session-1", "idempotencyKey": "quality-denied-1",
            "eventId": "evt-open-model", "metadata": {"alertDeliveryKey": "alert-target-key", "verdict": "valid", "reason": "evidence supports the alert"},
        })
        assert viewer_quality.status_code == 403


def test_postgres_migration_contains_queue_facts_and_formal_interaction_kinds() -> None:
    sql = (Path(__file__).parents[3] / "infra" / "postgres" / "001_init.sql").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS review_queue_entries" in sql
    assert "eligibility_key text NOT NULL UNIQUE" in sql
    assert "alert_quality_reviewed" in sql
    assert "metric_exclusion_recorded" in sql
    assert "metric_exclusion_reinstated" in sql
    assert "DROP CONSTRAINT IF EXISTS product_interactions_kind_check" in sql
    assert "pg_advisory_xact_lock" in (Path(__file__).parents[1] / "radar" / "storage.py").read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS content_ingest_history" in sql
    assert "observations_source_fingerprint_idx" in sql
    assert "first_observed_at=COALESCE(first_observed_at,created_at)" in sql
    assert "ALTER TABLE sources ALTER COLUMN last_observed_at SET NOT NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS metric_incidents" in sql
    assert "policy_digest text NOT NULL" in sql
    assert "CREATE TABLE IF NOT EXISTS schema_attestations" in sql
    assert "migration_version','001_init_rc2.6'" in sql
    assert "REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON schema_attestations FROM radar_app" in sql
    assert "'feedback','product_interactions','review_queue_entries','lead_threshold_crossings','content_ingest_history','connector_runs','metric_incidents','source_promotion_facts'" in sql
    assert "%I_append_only" in sql
    assert "CREATE TRIGGER alert_deliveries_immutable" in sql
    assert "CREATE TRIGGER observation_processing_history_monotonic" in sql
    for policy in (
        "feedback_workspace_isolation",
        "alert_rules_workspace_isolation",
        "alert_deliveries_workspace_isolation",
        "metric_incidents_workspace_isolation",
        "watchlists_workspace_isolation",
        "product_interactions_workspace_isolation",
        "cluster_edit_requests_workspace_isolation",
    ):
        assert f"DROP POLICY IF EXISTS {policy}" in sql
    assert "DROP CONSTRAINT IF EXISTS observation_processing_history_observation_id_fkey" in sql
    assert "DROP CONSTRAINT IF EXISTS feedback_event_id_fkey" in sql


def test_runtime_attestation_requires_exact_trigger_attachment_and_function() -> None:
    rows = [
        (name, schema, table, function_schema, function, "O", True, True, True, True)
        for name, (schema, table, function_schema, function) in AUDIT_TRIGGER_SPECS.items()
    ]
    assert audit_trigger_specs_verified(rows)

    wrong_table = list(rows)
    wrong_table[0] = (*wrong_table[0][:2], "unrelated_table", *wrong_table[0][3:])
    assert not audit_trigger_specs_verified(wrong_table)

    wrong_function = list(rows)
    wrong_function[0] = (*wrong_function[0][:4], "noop", *wrong_function[0][5:])
    assert not audit_trigger_specs_verified(wrong_function)

    wrong_function_schema = list(rows)
    wrong_function_schema[0] = (*wrong_function_schema[0][:3], "decoy", *wrong_function_schema[0][4:])
    assert not audit_trigger_specs_verified(wrong_function_schema)

    disabled_or_missing_event = list(rows)
    disabled_or_missing_event[0] = (*disabled_or_missing_event[0][:5], "D", True, True, True, False)
    assert not audit_trigger_specs_verified(disabled_or_missing_event)


def test_postgres_ranking_ledger_uses_one_repeatable_read_database_watermark() -> None:
    source = (Path(__file__).parents[1] / "radar" / "storage.py").read_text(encoding="utf-8")
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in source
    snapshot_method = source.split("def ranking_ledger_snapshot", 2)[-1].split("def commit_scored_event", 1)[0]
    assert snapshot_method.index("FROM events e") < snapshot_method.index("FROM lead_threshold_crossings")
    assert snapshot_method.index("FROM lead_threshold_crossings") < snapshot_method.index("SELECT clock_timestamp()")
