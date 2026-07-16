from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field, field_validator


ERROR_VERDICTS = {"insufficient_evidence", "incorrect_cluster", "out_of_scope"}
TRIAGE_ACTIONS = {"confirm", "reject", "observe"}


class DutyWindow(BaseModel):
    weekdays: list[int] = Field(min_length=1)
    start: time
    end: time

    @field_validator("weekdays")
    @classmethod
    def validate_weekdays(cls, value: list[int]) -> list[int]:
        if len(set(value)) != len(value) or any(day < 0 or day > 6 for day in value):
            raise ValueError("weekdays must be unique integers from 0 (Monday) through 6")
        return value

    @field_validator("end")
    @classmethod
    def validate_end(cls, value: time, info: Any) -> time:
        start = info.data.get("start")
        if start is not None and value <= start:
            raise ValueError("overnight or empty duty windows are not supported")
        return value


class ProductMetricTargets(BaseModel):
    strong_alert_acceptance_rate: float = Field(alias="strongAlertAcceptanceRate", ge=0, le=1)
    erroneous_strong_alerts_per_workday: float = Field(alias="erroneousStrongAlertsPerWorkday", ge=0)
    erroneous_strong_alert_rate: float = Field(alias="erroneousStrongAlertRate", ge=0, le=1)
    first_triage_within_sla_rate: float = Field(alias="firstTriageWithinSlaRate", ge=0, le=1)

    model_config = {"populate_by_name": True}


class ProductMetricMinimumSamples(BaseModel):
    strong_alerts: int = Field(alias="strongAlerts", ge=1)
    alert_workdays: int = Field(alias="alertWorkdays", ge=1)
    triage_events: int = Field(alias="triageEvents", ge=1)
    completed_reviews: int = Field(alias="completedReviews", ge=1)

    model_config = {"populate_by_name": True}


class ManualEvaluationPolicy(BaseModel):
    manual_schema_version: str = Field(alias="manualSchemaVersion")
    manual_schema_digest: str = Field(alias="manualSchemaDigest")
    preregistration_schema_version: str = Field(alias="preregistrationSchemaVersion")
    preregistration_schema_digest: str = Field(alias="preregistrationSchemaDigest")
    snapshot_timezone: str = Field(alias="snapshotTimezone")
    snapshot_local_time: time = Field(alias="snapshotLocalTime")
    snapshot_time_tolerance_minutes: int = Field(alias="snapshotTimeToleranceMinutes", ge=0, le=60)
    required_threshold_version: str = Field(alias="requiredThresholdVersion", min_length=8)
    precision_at_5_target: float = Field(alias="precisionAt5Target", ge=0, le=1)
    precision_at_5_minimum_ci_low: float = Field(alias="precisionAt5MinimumCiLow", ge=0, le=1)
    minimum_snapshots: int = Field(alias="minimumSnapshots", ge=1)
    candidates_per_snapshot: int = Field(alias="candidatesPerSnapshot", ge=1)
    interval_method: str = Field(alias="intervalMethod")
    confidence_level: float = Field(alias="confidenceLevel", gt=0, lt=1)
    minimum_bootstrap_iterations: int = Field(alias="minimumBootstrapIterations", ge=100)
    minimum_discovery_lead_events: int = Field(alias="minimumDiscoveryLeadEvents", ge=1)
    minimum_median_discovery_lead_minutes: float = Field(alias="minimumMedianDiscoveryLeadMinutes")
    lead_eligibility_rule_version: str = Field(alias="leadEligibilityRuleVersion", min_length=8)
    lead_minimum_attention: float = Field(alias="leadMinimumAttention", ge=0, le=100)
    lead_minimum_coverage: float = Field(alias="leadMinimumCoverage", ge=0, le=100)
    lead_lifecycle_states: list[str] = Field(alias="leadLifecycleStates", min_length=1)

    model_config = {"populate_by_name": True}


class AcceptanceMonitoringPolicy(BaseModel):
    monitor_version: str = Field(alias="monitorVersion", min_length=8)
    monitor_digest: str = Field(alias="monitorDigest", pattern=r"^sha256:[0-9a-f]{64}$")
    keyring_version: str = Field(alias="keyringVersion", min_length=8)
    keyring_digest: str = Field(alias="keyringDigest", pattern=r"^sha256:[0-9a-f]{64}$")
    keyring_frozen_at: datetime = Field(alias="keyringFrozenAt")
    cadence_seconds: int = Field(alias="cadenceSeconds", ge=60)
    minimum_distinct_connector_families: int = Field(alias="minimumDistinctConnectorFamilies", ge=2)
    required_signal_families: list[str] = Field(alias="requiredSignalFamilies", min_length=2)
    require_nonzero_connector_observations: bool = Field(alias="requireNonzeroConnectorObservations")

    model_config = {"populate_by_name": True}


class ProductMetricPolicy(BaseModel):
    version: str
    status: str
    frozen_at: datetime = Field(alias="frozenAt")
    timezone_name: str = Field(alias="timezone")
    duty_windows: list[DutyWindow] = Field(alias="dutyWindows", min_length=1)
    reviewable_lifecycle_states: list[str] = Field(alias="reviewableLifecycleStates", min_length=1)
    strong_alert_acceptance_window_duty_minutes: int = Field(alias="strongAlertAcceptanceWindowDutyMinutes", ge=1)
    first_triage_sla_minutes: int = Field(alias="firstTriageSlaMinutes", ge=1)
    active_review_target_seconds: int = Field(alias="activeReviewTargetSeconds", ge=1)
    minimum_alert_quality_review_coverage: float = Field(alias="minimumAlertQualityReviewCoverage", ge=0, le=1)
    minimum_active_review_telemetry_coverage: float = Field(alias="minimumActiveReviewTelemetryCoverage", ge=0, le=1)
    minimum_server_heartbeat_coverage: float = Field(alias="minimumServerHeartbeatCoverage", ge=0, le=1)
    minimum_server_measured_review_seconds: float = Field(alias="minimumServerMeasuredReviewSeconds", ge=0)
    foreground_idle_timeout_seconds: int = Field(alias="foregroundIdleTimeoutSeconds", ge=1, le=600)
    foreground_tick_cap_seconds: int = Field(alias="foregroundTickCapSeconds", ge=1, le=60)
    targets: ProductMetricTargets
    minimum_samples: ProductMetricMinimumSamples = Field(alias="minimumSamples")
    manual_evaluation: ManualEvaluationPolicy = Field(alias="manualEvaluation")
    acceptance_monitoring: AcceptanceMonitoringPolicy = Field(alias="acceptanceMonitoring")
    error_verdicts: list[str] = Field(alias="errorVerdicts", min_length=1)
    notes: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @field_validator("timezone_name")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        ZoneInfo(value)
        return value

    @field_validator("error_verdicts")
    @classmethod
    def validate_error_verdicts(cls, value: list[str]) -> list[str]:
        if not set(value).issubset(ERROR_VERDICTS):
            raise ValueError("unsupported error verdict")
        return value


def policy_path() -> Path:
    configured = os.getenv("PRODUCT_METRIC_POLICY_FILE")
    return Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "product_metric_policy.json"


@lru_cache(maxsize=1)
def load_product_metric_policy() -> ProductMetricPolicy:
    payload = json.loads(policy_path().read_text(encoding="utf-8"))
    return ProductMetricPolicy.model_validate(payload)


def product_metric_policy_digest(policy: ProductMetricPolicy) -> str:
    payload = policy.model_dump(mode="json", by_alias=True)
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(material).hexdigest()


def review_funnel(rows: list[dict[str, object]]) -> dict[str, object]:
    """Exploratory UI funnel retained for continuity; never used as a Beta gate."""
    opened: dict[tuple[str, str], object] = {}
    durations: list[float] = []
    triage = 0
    accepted = 0
    for row in sorted(rows, key=lambda item: item["occurredAt"]):
        key = (str(row["sessionId"]), str(row.get("eventId") or ""))
        if row["kind"] == "detail_opened":
            opened.setdefault(key, row["occurredAt"])
        elif row["kind"] == "triage_submitted":
            triage += 1
            metadata = row.get("metadata") or {}
            if isinstance(metadata, dict) and metadata.get("action") == "confirm":
                accepted += 1
            started = opened.get(key)
            if started is not None:
                durations.append(max(0, (row["occurredAt"] - started).total_seconds()))
    return {
        "metricScope": "exploratory_review_funnel_not_rc2_beta_kpi",
        "detailOpenSample": len({(str(row["sessionId"]), str(row.get("eventId") or "")) for row in rows if row["kind"] == "detail_opened"}),
        "triageSample": triage,
        "timedTriageFromDetailOpenSample": len(durations),
        "triageConfirmRate": accepted / triage if triage else None,
        "triageWithin15MinutesFromDetailOpenRate": sum(value <= 900 for value in durations) / len(durations) if durations else None,
        "medianReviewSecondsFromDetailOpen": median(durations) if durations else None,
        "limitations": [
            "This is not strong-alert acceptance: its denominator is submitted triage decisions, not delivered strong alerts.",
            "The 15-minute clock starts at detail open, not queue eligibility, and does not apply duty-window exclusions.",
        ],
    }


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _duty_intervals(day: date, policy: ProductMetricPolicy) -> list[tuple[datetime, datetime]]:
    zone = ZoneInfo(policy.timezone_name)
    intervals: list[tuple[datetime, datetime]] = []
    for window in policy.duty_windows:
        if day.weekday() not in window.weekdays:
            continue
        intervals.append((datetime.combine(day, window.start, zone), datetime.combine(day, window.end, zone)))
    return intervals


def is_in_duty_window(moment: datetime, policy: ProductMetricPolicy) -> bool:
    local = _utc(moment).astimezone(ZoneInfo(policy.timezone_name))
    return any(start <= local < end for start, end in _duty_intervals(local.date(), policy))


def duty_seconds_between(start: datetime, end: datetime, policy: ProductMetricPolicy) -> float:
    start_utc = _utc(start)
    end_utc = _utc(end)
    if end_utc <= start_utc:
        return 0.0
    zone = ZoneInfo(policy.timezone_name)
    local_start = start_utc.astimezone(zone)
    local_end = end_utc.astimezone(zone)
    current = local_start.date()
    seconds = 0.0
    while current <= local_end.date():
        for interval_start, interval_end in _duty_intervals(current, policy):
            overlap_start = max(local_start, interval_start)
            overlap_end = min(local_end, interval_end)
            if overlap_end > overlap_start:
                seconds += (overlap_end - overlap_start).total_seconds()
        current += timedelta(days=1)
    return seconds


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _gate(*, sample: int, minimum: int, passes: bool | None) -> dict[str, object]:
    eligible = sample >= minimum
    return {
        "sample": sample,
        "minimumSample": minimum,
        "evidenceStatus": "eligible" if eligible else "insufficient",
        "passesTarget": passes if eligible else None,
    }


def _metadata(row: dict[str, object]) -> dict[str, object]:
    value = row.get("metadata")
    return value if isinstance(value, dict) else {}


def _exclusions(interactions: list[dict[str, object]], target_type: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in sorted(interactions, key=lambda item: item["occurredAt"]):
        if row.get("kind") not in {"metric_exclusion_recorded", "metric_exclusion_reinstated"}:
            continue
        metadata = _metadata(row)
        if metadata.get("targetType") == target_type:
            key = str(metadata.get("targetKey") or "")
            if row.get("kind") == "metric_exclusion_reinstated":
                result.pop(key, None)
            elif metadata.get("reason") == "system_fault_duplicate":
                result[key] = str(metadata.get("reason") or "")
    return result


def _valid_alert_duplicate_exclusions(
    delivered: list[dict[str, object]],
    feedback: list[dict[str, object]],
    interactions: list[dict[str, object]],
    policy: ProductMetricPolicy,
) -> tuple[dict[str, str], int]:
    by_key = {
        str(row.get("idempotencyKey") or row.get("deliveryId") or ""): row for row in delivered
    }
    result: dict[str, str] = {}
    invalid = 0
    for row in sorted(interactions, key=lambda item: item["occurredAt"]):
        metadata = _metadata(row)
        if metadata.get("targetType") != "alert":
            continue
        key = str(metadata.get("targetKey") or "")
        if row.get("kind") == "metric_exclusion_reinstated":
            result.pop(key, None)
            continue
        if row.get("kind") != "metric_exclusion_recorded" or metadata.get("reason") != "system_fault_duplicate":
            continue
        target = by_key.get(key)
        canonical = by_key.get(str(metadata.get("canonicalKey") or ""))
        occurred_at = _utc(row["occurredAt"])
        valid = (
            target is not None and canonical is not None and target is not canonical
            and target.get("eventId") == canonical.get("eventId")
            and _utc(canonical["deliveredAt"]) < _utc(target["deliveredAt"]) <= occurred_at
            and duty_seconds_between(target["deliveredAt"], occurred_at, policy) <= policy.first_triage_sla_minutes * 60
            and isinstance(metadata.get("incidentId"), str) and len(str(metadata["incidentId"])) >= 8
            and isinstance(metadata.get("incidentFactDigest"), str)
            and str(metadata["incidentFactDigest"]).startswith("sha256:")
            and not any(
                decision.get("eventId") == target.get("eventId")
                and _utc(target["deliveredAt"]) <= _utc(decision["createdAt"]) <= occurred_at
                for decision in feedback
            )
        )
        if valid:
            result[key] = "system_fault_duplicate"
        else:
            invalid += 1
    return result, invalid


def _strong_alert_metrics(
    deliveries: list[dict[str, object]],
    feedback: list[dict[str, object]],
    interactions: list[dict[str, object]],
    policy: ProductMetricPolicy,
    as_of: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    delivered = [row for row in deliveries if row.get("status", "delivered") == "delivered"]
    delivered.sort(key=lambda row: row["deliveredAt"])
    exclusions, invalid_exclusions = _valid_alert_duplicate_exclusions(delivered, feedback, interactions, policy)
    excluded_by_reason: dict[str, int] = defaultdict(int)
    canonical: list[dict[str, object]] = []
    for row in delivered:
        key = str(row.get("idempotencyKey") or row.get("deliveryId") or "")
        reason = exclusions.get(key)
        if reason:
            excluded_by_reason[reason] += 1
        else:
            canonical.append(row)

    decisions = [row for row in feedback if row.get("action") in TRIAGE_ACTIONS]
    decisions.sort(key=lambda row: row["createdAt"])
    accepted = rejected = observed = late_confirm = 0
    denominator = 0
    pending_window = 0
    window_seconds = policy.strong_alert_acceptance_window_duty_minutes * 60
    for alert in canonical:
        alert_key = str(alert.get("idempotencyKey") or alert.get("deliveryId") or "")
        candidates = [
            row for row in decisions
            if row.get("eventId") == alert.get("eventId")
            and row.get("alertDeliveryKey") == alert_key
            and _utc(row["createdAt"]) >= _utc(alert["deliveredAt"])
        ]
        matured = duty_seconds_between(alert["deliveredAt"], as_of, policy) >= window_seconds
        if not candidates and not matured:
            pending_window += 1
            continue
        denominator += 1
        matching_confirm = next((
            row for row in candidates
            if row.get("action") == "confirm"
            and duty_seconds_between(alert["deliveredAt"], row["createdAt"], policy) <= window_seconds
        ), None)
        if matching_confirm is not None:
            accepted += 1
            continue
        late_matching_confirm = next((row for row in candidates if row.get("action") == "confirm"), None)
        if late_matching_confirm is not None:
            late_confirm += 1
            continue
        matching = candidates[0] if candidates else None
        if matching is None:
            continue
        action = matching.get("action")
        if action == "reject":
            rejected += 1
        elif action == "observe":
            observed += 1
    acceptance_rate = _rate(accepted, denominator)
    acceptance_gate = _gate(
        sample=denominator,
        minimum=policy.minimum_samples.strong_alerts,
        passes=acceptance_rate is not None and acceptance_rate >= policy.targets.strong_alert_acceptance_rate,
    )
    acceptance = {
        **acceptance_gate,
        "numeratorAcceptedWithinOneWorkday": accepted,
        "denominatorDeliveredStrongAlerts": denominator,
        "rate": acceptance_rate,
        "targetRate": policy.targets.strong_alert_acceptance_rate,
        "rejected": rejected,
        "needsObservation": observed,
        "lateConfirm": late_confirm,
        "unhandled": denominator - accepted - rejected - observed - late_confirm,
        "pendingWindowOpen": pending_window,
        "excluded": dict(sorted(excluded_by_reason.items())),
        "invalidOrLateExclusionAttempts": invalid_exclusions,
    }

    review_rows: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in interactions:
        if row.get("kind") != "alert_quality_reviewed":
            continue
        metadata = _metadata(row)
        key = str(metadata.get("alertDeliveryKey") or "")
        if key:
            review_rows[key].append(row)
    zone = ZoneInfo(policy.timezone_name)
    duty_weekdays = {day for window in policy.duty_windows for day in window.weekdays}
    workday_alerts: list[dict[str, object]] = []
    off_duty_day_alerts = 0
    for alert in canonical:
        local_day = _utc(alert["deliveredAt"]).astimezone(zone).date()
        if local_day.weekday() in duty_weekdays:
            workday_alerts.append(alert)
        else:
            off_duty_day_alerts += 1
    erroneous = 0
    pending_dual_review = 0
    reviewed_alerts = 0
    for alert in workday_alerts:
        key = str(alert.get("idempotencyKey") or alert.get("deliveryId") or "")
        verdicts: dict[str, set[str]] = defaultdict(set)
        for row in review_rows.get(key, []):
            if row.get("eventId") != alert.get("eventId") or _utc(row["occurredAt"]) < _utc(alert["deliveredAt"]):
                continue
            metadata = _metadata(row)
            actor = str(row.get("actorId") or "")
            if actor:
                verdicts[str(metadata.get("verdict") or "")].add(actor)
        error_actors = set().union(*(verdicts.get(verdict, set()) for verdict in policy.error_verdicts))
        valid_actors = verdicts.get("valid", set())
        if len(error_actors) >= 2 and not valid_actors:
            erroneous += 1
            reviewed_alerts += 1
        elif len(valid_actors) >= 2 and not error_actors:
            reviewed_alerts += 1
        elif len(error_actors | valid_actors) < 2 or (error_actors and valid_actors):
            pending_dual_review += 1
    workdays = {
        _utc(row["deliveredAt"]).astimezone(zone).date().isoformat()
        for row in workday_alerts
    }
    error_rate = _rate(erroneous, len(workday_alerts))
    errors_per_day = erroneous / len(workdays) if workdays else None
    review_coverage = _rate(reviewed_alerts, len(workday_alerts))
    error_evidence_eligible = (
        len(workdays) >= policy.minimum_samples.alert_workdays
        and review_coverage is not None
        and review_coverage >= policy.minimum_alert_quality_review_coverage
    )
    error_passes = (
            errors_per_day is not None
            and error_rate is not None
            and errors_per_day <= policy.targets.erroneous_strong_alerts_per_workday
            and error_rate <= policy.targets.erroneous_strong_alert_rate
    )
    erroneous_metric = {
        "sample": len(workdays),
        "minimumSample": policy.minimum_samples.alert_workdays,
        "evidenceStatus": "eligible" if error_evidence_eligible else "insufficient",
        "passesTarget": error_passes if error_evidence_eligible else None,
        "alertWorkdays": len(workdays),
        "workdayStrongAlerts": len(workday_alerts),
        "dualReviewedStrongAlerts": reviewed_alerts,
        "dualReviewCoverage": review_coverage,
        "minimumDualReviewCoverage": policy.minimum_alert_quality_review_coverage,
        "erroneousStrongAlerts": erroneous,
        "errorsPerWorkday": errors_per_day,
        "errorRate": error_rate,
        "targetMaxPerWorkday": policy.targets.erroneous_strong_alerts_per_workday,
        "targetMaxRate": policy.targets.erroneous_strong_alert_rate,
        "pendingDualReview": pending_dual_review,
        "offDutyDayAlertsReportedSeparately": off_duty_day_alerts,
    }
    return acceptance, erroneous_metric


def _triage_sla_metrics(
    queue_entries: list[dict[str, object]],
    feedback: list[dict[str, object]],
    interactions: list[dict[str, object]],
    policy: ProductMetricPolicy,
    as_of: datetime,
) -> dict[str, object]:
    exclusions = _exclusions(interactions, "queue")
    decisions = [row for row in feedback if row.get("action") in TRIAGE_ACTIONS]
    decisions.sort(key=lambda row: row["createdAt"])
    entries = sorted(queue_entries, key=lambda row: row["eligibleAt"])
    used_feedback: set[str] = set()
    within = 0
    sample = 0
    pending_window = 0
    no_duty = 0
    deployment_backfills = 0
    excluded_by_reason: dict[str, int] = defaultdict(int)
    threshold_seconds = policy.first_triage_sla_minutes * 60
    for entry in entries:
        key = str(entry.get("eligibilityKey") or "")
        reason = exclusions.get(key)
        if reason:
            excluded_by_reason[reason] += 1
        if entry.get("entryKind") == "deployment_backfill":
            deployment_backfills += 1
            continue
        if not is_in_duty_window(entry["eligibleAt"], policy):
            no_duty += 1
            continue
        matching = next((
            row for row in decisions
            if str(row.get("id") or "") not in used_feedback
            and row.get("eventId") == entry.get("eventId")
            and row.get("queueEligibilityKey") == key
            and _utc(row["createdAt"]) >= _utc(entry["eligibleAt"])
        ), None)
        measured_to = matching["createdAt"] if matching else as_of
        elapsed = duty_seconds_between(entry["eligibleAt"], measured_to, policy)
        if matching is None and elapsed <= threshold_seconds:
            pending_window += 1
            continue
        sample += 1
        if matching is not None:
            used_feedback.add(str(matching.get("id") or ""))
        if matching is not None and elapsed <= threshold_seconds:
            within += 1
    rate = _rate(within, sample)
    gate = _gate(
        sample=sample,
        minimum=policy.minimum_samples.triage_events,
        passes=rate is not None and rate >= policy.targets.first_triage_within_sla_rate,
    )
    return {
        **gate,
        "withinSla": within,
        "rate": rate,
        "targetRate": policy.targets.first_triage_within_sla_rate,
        "slaMinutes": policy.first_triage_sla_minutes,
        "pendingWindowOpen": pending_window,
        "noDutyExcluded": no_duty,
        "excluded": dict(sorted(excluded_by_reason.items())),
        "flaggedSystemFaultDuplicatesRemainInDenominator": True,
        "deploymentBackfillExcludedFromSla": deployment_backfills,
    }


def _review_timing_metrics(
    feedback: list[dict[str, object]],
    queue_entries: list[dict[str, object]],
    interactions: list[dict[str, object]],
    policy: ProductMetricPolicy,
) -> tuple[dict[str, object], dict[str, object]]:
    feedback_by_id = {str(row.get("id") or ""): row for row in feedback}
    active_values: list[float] = []
    wall_values: list[float] = []
    external_values: list[float] = []
    rejected_telemetry = 0
    seen_feedback: set[str] = set()
    seen_eligibilities: set[str] = set()
    eligibility_by_key = {
        str(row.get("eligibilityKey") or ""): row for row in queue_entries if row.get("eligibilityKey")
    }
    completed_eligibilities = {
        str(row.get("queueEligibilityKey"))
        for row in feedback
        if row.get("action") in TRIAGE_ACTIONS
        and str(row.get("queueEligibilityKey") or "") in eligibility_by_key
        and eligibility_by_key[str(row.get("queueEligibilityKey"))].get("eventId") == row.get("eventId")
    }
    detail_opens = [row for row in interactions if row.get("kind") == "detail_opened"]
    closed_segments = [row for row in interactions if row.get("kind") == "review_segment_closed"]
    heartbeats = [row for row in interactions if row.get("kind") == "review_heartbeat"]
    server_timed_reviews = 0

    def server_segment_timing(
        opened: dict[str, object], terminal: dict[str, object], eligibility_key: str, attempt_id: str,
    ) -> tuple[float, float, float] | None:
        opened_metadata = _metadata(opened)
        terminal_metadata = _metadata(terminal)
        segment_id = opened_metadata.get("segmentId")
        if (
            opened_metadata.get("measurementVersion") != "server-heartbeat-v2"
            or terminal_metadata.get("measurementVersion") != "server-heartbeat-v2"
            or terminal_metadata.get("segmentId") != segment_id
            or not isinstance(segment_id, str) or len(segment_id) < 8
        ):
            return None
        rows = sorted(
            [
                heartbeat for heartbeat in heartbeats
                if heartbeat.get("eventId") == terminal.get("eventId")
                and heartbeat.get("actorId") == terminal.get("actorId")
                and _metadata(heartbeat).get("queueEligibilityKey") == eligibility_key
                and _metadata(heartbeat).get("reviewAttemptId") == attempt_id
                and _metadata(heartbeat).get("segmentId") == segment_id
                and _utc(opened["occurredAt"]) <= _utc(heartbeat["occurredAt"]) <= _utc(terminal["occurredAt"])
            ],
            key=lambda item: _utc(item["occurredAt"]),
        )
        if len(rows) < 2:
            return None
        sequences = [_metadata(row).get("sequence") for row in rows]
        if sequences != list(range(len(rows))):
            return None
        active = external = measured = 0.0
        for previous, current in zip(rows, rows[1:]):
            previous_metadata = _metadata(previous)
            if (
                previous_metadata.get("measurementVersion") != "server-heartbeat-v2"
                or previous_metadata.get("idleTimeoutSeconds") != policy.foreground_idle_timeout_seconds
                or previous_metadata.get("tickCapSeconds") != policy.foreground_tick_cap_seconds
            ):
                return None
            delta = (_utc(current["occurredAt"]) - _utc(previous["occurredAt"])).total_seconds()
            if delta < 0:
                return None
            counted = min(float(policy.foreground_tick_cap_seconds), delta)
            measured += counted
            # Heartbeat states are accepted only from authenticated product
            # interactions. Client-reported duration totals are never used.
            # The parallel wall clock below remains an anti-understatement
            # guardrail because no heartbeat state can reduce it.
            state = previous_metadata.get("state")
            if state == "active":
                active += counted
            elif state == "external_wait":
                external += counted
            elif state != "idle":
                return None
        span = (_utc(rows[-1]["occurredAt"]) - _utc(opened["occurredAt"])).total_seconds()
        if (
            span < policy.minimum_server_measured_review_seconds
            or span <= 0
            or measured / span < policy.minimum_server_heartbeat_coverage
        ):
            return None
        return active, external, measured

    for row in sorted(interactions, key=lambda item: item["occurredAt"]):
        if row.get("kind") != "triage_submitted":
            continue
        metadata = _metadata(row)
        feedback_id = str(metadata.get("feedbackId") or "")
        linked = feedback_by_id.get(feedback_id)
        eligibility_key = str(linked.get("queueEligibilityKey") or "") if linked else ""
        eligibility = eligibility_by_key.get(eligibility_key)
        relevant_opens = [
            opened for opened in detail_opens
            if opened.get("eventId") == row.get("eventId")
            and opened.get("actorId") == row.get("actorId")
            and eligibility is not None
            and _utc(opened["occurredAt"]) >= _utc(eligibility["eligibleAt"])
            and _utc(opened["occurredAt"]) <= _utc(row["occurredAt"])
        ]
        open_by_attempt = {
            str(_metadata(opened).get("reviewAttemptId")): opened
            for opened in relevant_opens
            if _metadata(opened).get("queueEligibilityKey") == eligibility_key
            and isinstance(_metadata(opened).get("reviewAttemptId"), str)
        }
        current_attempt = str(metadata.get("reviewAttemptId") or "")
        matching_open = open_by_attempt.get(current_attempt)
        active_value = metadata.get("activeSeconds", 0)
        external_value = metadata.get("externalWaitSeconds", 0)
        if (
            not linked
            or feedback_id in seen_feedback
            or not eligibility
            or eligibility_key in seen_eligibilities
            or not matching_open
            or metadata.get("queueEligibilityKey") != eligibility_key
            or len(open_by_attempt) != len(relevant_opens)
            or eligibility.get("eventId") != row.get("eventId")
            or linked.get("eventId") != row.get("eventId")
            or linked.get("action") != metadata.get("action")
            or linked.get("actorId") != row.get("actorId")
            or _utc(row["occurredAt"]) < _utc(linked["createdAt"])
            or _utc(row["occurredAt"]) > _utc(linked["createdAt"]) + timedelta(minutes=5)
            or metadata.get("measurementVersion") != "server-heartbeat-v2"
            or metadata.get("idleTimeoutSeconds") != policy.foreground_idle_timeout_seconds
            or metadata.get("tickCapSeconds") != policy.foreground_tick_cap_seconds
        ):
            rejected_telemetry += 1
            continue
        active = float(active_value)
        external = float(external_value)
        server_timed = True
        current_server_timing = server_segment_timing(matching_open, row, eligibility_key, current_attempt) if server_timed else None
        if server_timed and current_server_timing is None:
            rejected_telemetry += 1
            continue
        active_segments = [current_server_timing[0] if current_server_timing else active]
        external_segments = [current_server_timing[1] if current_server_timing else external]
        wall_segments = [current_server_timing[2] if current_server_timing else active + external]
        timing_valid = True
        for attempt_id, opened in open_by_attempt.items():
            if attempt_id == current_attempt:
                continue
            matching_segments = [
                segment for segment in closed_segments
                if segment.get("eventId") == row.get("eventId")
                and segment.get("actorId") == row.get("actorId")
                and _metadata(segment).get("queueEligibilityKey") == eligibility_key
                and _metadata(segment).get("reviewAttemptId") == attempt_id
                and _utc(opened["occurredAt"]) <= _utc(segment["occurredAt"]) <= _utc(row["occurredAt"])
            ]
            if len(matching_segments) != 1:
                timing_valid = False
                break
            segment = matching_segments[0]
            segment_metadata = _metadata(segment)
            segment_active = segment_metadata.get("activeSeconds")
            segment_external = segment_metadata.get("externalWaitSeconds")
            segment_wall = (_utc(segment["occurredAt"]) - _utc(opened["occurredAt"])).total_seconds()
            segment_server_timing = server_segment_timing(opened, segment, eligibility_key, attempt_id) if server_timed else None
            if (
                (server_timed and segment_server_timing is None)
                or segment_metadata.get("idleTimeoutSeconds") != policy.foreground_idle_timeout_seconds
                or segment_metadata.get("tickCapSeconds") != policy.foreground_tick_cap_seconds
                or (not server_timed and (
                    isinstance(segment_active, bool) or not isinstance(segment_active, (int, float))
                    or isinstance(segment_external, bool) or not isinstance(segment_external, (int, float))
                    or float(segment_active) < 0 or float(segment_external) < 0
                    or float(segment_active) + float(segment_external) > segment_wall + policy.foreground_tick_cap_seconds
                ))
            ):
                timing_valid = False
                break
            active_segments.append(segment_server_timing[0] if segment_server_timing else float(segment_active))
            external_segments.append(segment_server_timing[1] if segment_server_timing else float(segment_external))
            wall_segments.append(
                segment_server_timing[2]
                if segment_server_timing else float(segment_active) + float(segment_external)
            )
        wall_seconds = (_utc(row["occurredAt"]) - _utc(matching_open["occurredAt"])).total_seconds()
        if not timing_valid or wall_seconds < 0:
            rejected_telemetry += 1
            continue
        seen_feedback.add(feedback_id)
        seen_eligibilities.add(eligibility_key)
        active_values.append(sum(active_segments))
        external_values.append(sum(external_segments))
        wall_values.append(sum(wall_segments))
        server_timed_reviews += int(server_timed)
    median_active = median(active_values) if active_values else None
    completed_reviews = len(completed_eligibilities)
    telemetry_coverage = len(seen_eligibilities) / completed_reviews if completed_reviews else None
    eligible = (
        completed_reviews >= policy.minimum_samples.completed_reviews
        and telemetry_coverage is not None
        and telemetry_coverage >= policy.minimum_active_review_telemetry_coverage
        and server_timed_reviews == len(active_values)
    )
    common = {
        "sample": completed_reviews,
        "minimumSample": policy.minimum_samples.completed_reviews,
        "evidenceStatus": "eligible" if eligible else "insufficient",
        "validTelemetrySample": len(active_values),
        "serverTimedTelemetrySample": server_timed_reviews,
        "formalMeasurementEligible": bool(active_values) and server_timed_reviews == len(active_values),
        "telemetryCoverage": telemetry_coverage,
        "minimumTelemetryCoverage": policy.minimum_active_review_telemetry_coverage,
        "rejectedOrUnlinkedTelemetry": rejected_telemetry,
        "measurementVersion": "server-heartbeat-v2",
        "idleTimeoutSeconds": policy.foreground_idle_timeout_seconds,
        "minimumServerHeartbeatCoverage": policy.minimum_server_heartbeat_coverage,
        "minimumServerMeasuredReviewSeconds": policy.minimum_server_measured_review_seconds,
        "tickCapSeconds": policy.foreground_tick_cap_seconds,
    }
    active_metric = {
        **common,
        "passesTarget": (
            median_active is not None and median_active <= policy.active_review_target_seconds
        ) if eligible else None,
        "medianActiveSeconds": median_active,
        "targetMaxMedianSeconds": policy.active_review_target_seconds,
        "medianExternalWaitSeconds": median(external_values) if external_values else None,
        "durationDefinition": "authenticated-heartbeat-active-state-v1",
        "trustBoundary": "Authenticated Analyst heartbeat state; client duration totals are ignored.",
    }
    server_metric = {
        **common,
        "passesTarget": None,
        "guardrailOnly": True,
        "medianServerObservedSeconds": median(wall_values) if wall_values else None,
        "medianClientDeclaredInactiveSeconds": (
            median([wall - active for wall, active in zip(wall_values, active_values)])
            if wall_values else None
        ),
        "durationDefinition": "server-received-contiguous-heartbeat-wall-v1",
    }
    return active_metric, server_metric


def beta_product_metrics(
    *,
    deliveries: list[dict[str, object]],
    feedback: list[dict[str, object]],
    queue_entries: list[dict[str, object]],
    interactions: list[dict[str, object]],
    incidents: list[dict[str, object]] | None = None,
    as_of: datetime | None = None,
    policy: ProductMetricPolicy | None = None,
) -> dict[str, object]:
    """Calculate frozen rc2 product gates without promoting insufficient samples."""
    current = _utc(as_of or datetime.now(timezone.utc))
    active_policy = policy or load_product_metric_policy()
    frozen_at = _utc(active_policy.frozen_at)

    def filter_window(rows: list[dict[str, object]], field: str) -> tuple[list[dict[str, object]], dict[str, int]]:
        accepted: list[dict[str, object]] = []
        diagnostics = {"beforeFreeze": 0, "afterAsOf": 0, "missingOrInvalidTimestamp": 0}
        for row in rows:
            value = row.get(field)
            if not isinstance(value, datetime):
                diagnostics["missingOrInvalidTimestamp"] += 1
            elif _utc(value) < frozen_at:
                diagnostics["beforeFreeze"] += 1
            elif _utc(value) > current:
                diagnostics["afterAsOf"] += 1
            else:
                accepted.append(row)
        return accepted, diagnostics

    filtered_deliveries, delivery_window_diagnostics = filter_window(deliveries, "deliveredAt")
    filtered_feedback, feedback_window_diagnostics = filter_window(feedback, "createdAt")
    filtered_queue, queue_window_diagnostics = filter_window(queue_entries, "eligibleAt")
    filtered_interactions, interaction_window_diagnostics = filter_window(interactions, "occurredAt")
    filtered_incidents, incident_window_diagnostics = filter_window(incidents or [], "createdAt")

    def fact_digest(rows: list[dict[str, object]]) -> str:
        canonical_rows = sorted(
            json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
            for row in rows
        )
        return "sha256:" + hashlib.sha256(
            json.dumps(canonical_rows, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest()

    acceptance, erroneous = _strong_alert_metrics(
        filtered_deliveries, filtered_feedback, filtered_interactions, active_policy, current,
    )
    triage_sla = _triage_sla_metrics(
        filtered_queue, filtered_feedback, filtered_interactions, active_policy, current,
    )
    active_review, server_observed_review = _review_timing_metrics(
        filtered_feedback, filtered_queue, filtered_interactions, active_policy,
    )
    metrics = [acceptance, erroneous, triage_sla, active_review]
    overall_eligible = all(item["evidenceStatus"] == "eligible" for item in metrics)
    overall_passes = all(item["passesTarget"] is True for item in metrics) if overall_eligible else None
    return {
        "metricScope": "rc2_beta_product_metrics",
        "productMetricPolicyVersion": active_policy.version,
        "productMetricPolicyDigest": product_metric_policy_digest(active_policy),
        "policyStatus": active_policy.status,
        "policyFrozenAt": active_policy.frozen_at.isoformat(),
        "manualEvaluationPolicy": active_policy.manual_evaluation.model_dump(mode="json", by_alias=True),
        "acceptanceMonitoringPolicy": active_policy.acceptance_monitoring.model_dump(mode="json", by_alias=True),
        "asOf": current.isoformat(),
        "collectionWindow": {
            "from": frozen_at.isoformat(),
            "to": current.isoformat(),
            "excludedOutsideFrozenWindow": {
                "deliveries": delivery_window_diagnostics,
                "feedback": feedback_window_diagnostics,
                "queueEntries": queue_window_diagnostics,
                "interactions": interaction_window_diagnostics,
                "metricIncidents": incident_window_diagnostics,
            },
        },
        "evidenceStatus": "eligible" if overall_eligible else "insufficient",
        "passesMeasuredGates": overall_passes,
        "factLedger": {
            "version": "beta-fact-ledger-v1",
            "counts": {
                "alertDeliveries": len(filtered_deliveries),
                "feedback": len(filtered_feedback),
                "queueEntries": len(filtered_queue),
                "interactions": len(filtered_interactions),
                "metricIncidents": len(filtered_incidents),
                "invalidExclusionAttempts": int(acceptance["invalidOrLateExclusionAttempts"]),
            },
            "digests": {
                "alertDeliveries": fact_digest(filtered_deliveries),
                "feedback": fact_digest(filtered_feedback),
                "queueEntries": fact_digest(filtered_queue),
                "interactions": fact_digest(filtered_interactions),
                "metricIncidents": fact_digest(filtered_incidents),
            },
        },
        "strongAlertAcceptance": acceptance,
        "erroneousStrongAlerts": erroneous,
        "firstTriageSla": triage_sla,
        "activeReviewTime": active_review,
        "serverObservedReviewTime": server_observed_review,
        "notCalculatedHere": [
            "Precision@5 requires preregistered daily snapshots and two independent reviewers.",
            "Discovery lead time requires an independent manual baseline for the same frozen event set.",
        ],
        "limitations": [
            "Synthetic and demo rows validate the calculator but are not Beta acceptance evidence.",
            "A metric remains insufficient until its frozen minimum sample is reached.",
            "Active review time trusts authenticated Analyst heartbeat state, not client duration totals; it cannot prevent a credentialed insider from falsely declaring state.",
            "Server-observed review time is a wall-time guardrail, not proof of foreground attention; heartbeat states never reduce it.",
        ],
    }
