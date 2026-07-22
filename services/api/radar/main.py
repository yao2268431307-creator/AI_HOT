from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import time
from collections import deque
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse, Response, StreamingResponse
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from . import __version__
from .assessment import event_assessment
from .auth import Principal, Role, current_principal, jwt_configuration_ready, require_role
from .contracts import AlertRuleRequest, BehaviorApplicabilityRequest, ClusterEditRequest, EventType, FeedbackRequest, MetricIncidentRequest, MutationReceipt, ProductInteractionRequest, RadarPayload, SourceReviewRequest, WatchlistRequest
from .fixtures import seed_repository
from .product_metrics import beta_product_metrics, load_product_metric_policy, review_funnel
from .source_discovery import (
    SourceCandidate, candidate_score, eligible as source_promotion_eligible,
    evidence_eligible as source_evidence_eligible, load_source_score_policy, source_score_policy_digest,
)
from .scoring import replay_score_payload
from .storage import InMemoryRepository, PostgresRepository, score_cycle
from .connectors.base import ConnectorError, ensure_safe_public_url_resolved
from .evidence_store import LocalEvidenceStore
from .runtime import RuntimeProfile, free_only_mode, runtime_profile, validate_local_access_configuration


def _prometheus_label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _percentile_95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)]


def _budget_limit_map(variable_name: str) -> dict[str, float]:
    raw = os.getenv(variable_name, "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{variable_name} must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{variable_name} must be a JSON object")
    result: dict[str, float] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key).strip()
        if not key or isinstance(raw_value, bool):
            raise RuntimeError(f"{variable_name} contains an invalid entry")
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{variable_name}.{key} must be a positive number") from exc
        if not (0 < value < float("inf")):
            raise RuntimeError(f"{variable_name}.{key} must be a positive finite number")
        result[key] = value
    return result


def _image_digests(variable_name: str) -> dict[str, str]:
    try:
        payload = json.loads(os.getenv(variable_name, ""))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict) or set(payload) != {"api", "web"}:
        return {}
    result = {str(key): str(value) for key, value in payload.items()}
    if any(
        not value.startswith("sha256:") or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
        for value in result.values()
    ):
        return {}
    return result


def create_app(repository: InMemoryRepository | PostgresRepository | None = None) -> FastAPI:
    profile = runtime_profile()
    validate_local_access_configuration()
    if repository is None:
        production_dsn = os.getenv("DATABASE_URL") if profile is not RuntimeProfile.DEMO else None
        repository = PostgresRepository(production_dsn) if production_dsn else InMemoryRepository()
    repo = seed_repository(repository) if isinstance(repository, InMemoryRepository) else repository
    app = FastAPI(title="SIGNAL//AI Radar API", version=__version__, docs_url="/docs")
    origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "http://localhost:3210,http://127.0.0.1:3210").split(",") if origin.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["GET", "POST", "DELETE"], allow_headers=["*"])
    app.state.repository = repo
    app.state.http_status_classes = {"2xx": 0, "3xx": 0, "4xx": 0, "5xx": 0}
    app.state.http_durations_seconds = deque(maxlen=2048)
    app.state.local_evidence_store = (
        LocalEvidenceStore(os.getenv("RAW_EVIDENCE_LOCAL_DIR", ".data/evidence"))
        if profile is RuntimeProfile.LOCAL else None
    )

    @app.middleware("http")
    async def observe_http(request: Request, call_next):
        started = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            bucket = f"{min(5, max(2, status_code // 100))}xx"
            app.state.http_status_classes[bucket] = app.state.http_status_classes.get(bucket, 0) + 1
            app.state.http_durations_seconds.append(time.perf_counter() - started)

    def prioritized_events(
        events, principal: Principal, sort_mode: Literal["priority", "latest"] = "priority"
    ):
        minimum = datetime.min.replace(tzinfo=timezone.utc)
        entries = repo.list_review_queue_entries(minimum)
        event_ids = {event.id for event in events}
        latest_entries: dict[str, dict[str, object]] = {}
        for row in entries:
            event_id = str(row["eventId"])
            existing = latest_entries.get(event_id)
            if event_id in event_ids and (
                existing is None or row["eligibleAt"] > existing["eligibleAt"]
            ):
                latest_entries[event_id] = row
        feedback = repo.list_feedback(principal.workspace_id, minimum)
        latest_feedback: dict[str, dict[str, object]] = {}
        for row in feedback:
            event_id = str(row["eventId"])
            existing = latest_feedback.get(event_id)
            if event_id in event_ids and (
                existing is None or row["createdAt"] > existing["createdAt"]
            ):
                latest_feedback[event_id] = row
        watched = {item.event_id for item in repo.list_watchlists(principal.workspace_id)}
        event_anchors: dict[str, datetime] = {}
        for event in events:
            entry = latest_entries.get(event.id)
            feedback_row = latest_feedback.get(event.id)
            event_anchors[event.id] = (
                feedback_row["createdAt"] if feedback_row
                else entry["eligibleAt"] if entry
                else event.first_seen
            )
        priority_context = repo.review_priority_context(event_anchors)
        prioritized = []
        confirmation_labels = {"cross_platform_confirmed", "adoption_confirmed", "response_confirmed"}
        for event in events:
            entry = latest_entries.get(event.id)
            feedback_row = latest_feedback.get(event.id)
            anchor = event_anchors[event.id]
            context = priority_context.get(event.id, {})
            new_evidence_count = int(context.get("newEvidenceCount", 0))
            latest_evidence_at = context.get("latestEvidenceAt")
            if not isinstance(latest_evidence_at, datetime):
                latest_evidence_at = event.updated_at if isinstance(repo, InMemoryRepository) else None
            reasons: list[str] = []
            ranks: list[int] = []
            if new_evidence_count > 0 and event.evidence_strength.value == "high":
                reasons.append(f"新增 {new_evidence_count} 条强证据")
                ranks.append(6)
            if entry and entry["eligibleAt"] >= anchor and event.state.value in {"emerging", "accelerating", "established"}:
                reasons.append(f"生命周期进入{event.state.value}")
                ranks.append(5)
            labels = {label.value for label in event.labels}
            if new_evidence_count > 0 and labels & confirmation_labels:
                reasons.append("新增跨平台、采用或响应确认")
                ranks.append(4)
            if event.id in watched and event.updated_at > anchor:
                reasons.append("关注事件出现变化")
                ranks.append(3)
            runs = context.get("scoreRuns", [])
            if len(runs) >= 2:
                previous_coverage = float(runs[-2]["payload"].get("coverage", event.coverage))
                current_coverage = float(runs[-1]["payload"].get("coverage", event.coverage))
                if runs[-1]["inputTo"] > anchor and previous_coverage - current_coverage >= 15:
                    reasons.append(f"覆盖度下降 {previous_coverage - current_coverage:.0f} 分")
                    ranks.append(2)
            if event.evidence_strength.value == "low":
                reasons.append("低证据候选，需补充信号")
                ranks.append(1)
            priority = max(ranks, default=0) * 1000 + min(999, new_evidence_count * 25 + max(0, event.velocity) + event.evidence_score / 10)
            prioritized.append(event.model_copy(update={
                "new_evidence_count": new_evidence_count, "queue_priority_score": round(priority, 2),
                "queue_priority_reasons": reasons, "review_anchor_at": anchor,
                "latest_evidence_at": latest_evidence_at,
            }))
        if sort_mode == "latest":
            return sorted(
                prioritized,
                key=lambda event: (
                    -(event.latest_evidence_at or event.first_seen).timestamp(),
                    -event.queue_priority_score,
                    event.id,
                ),
            )
        return sorted(prioritized, key=lambda event: (-event.queue_priority_score, -event.updated_at.timestamp(), event.id))

    def runtime_health_details() -> dict[str, object]:
        attestation = repo.runtime_attestation()
        auth_required = os.getenv("AUTH_REQUIRED", "false").lower() == "true"
        auth_mode = os.getenv("RADAR_AUTH_MODE", "api_keys")
        jwt_ready = jwt_configuration_ready()
        now = datetime.now(timezone.utc)
        required_components = (
            {"scheduler", "collector-worker", "retention-worker", "local-outbox-dispatcher"}
            if profile is RuntimeProfile.LOCAL
            else {"scheduler", "collector-worker", "retention-worker", "outbox-publisher", "alert-consumer"}
        )
        runtime_rows = attestation.get("runtimeComponents", [])
        fresh_components: set[str] = set()
        fresh_component_details: dict[str, dict[str, object]] = {}
        for row in runtime_rows:
            if not isinstance(row, dict) or not isinstance(row.get("lastSeenAt"), datetime):
                continue
            details = row.get("details") if isinstance(row.get("details"), dict) else {}
            configured_interval = details.get("intervalSeconds", 0)
            interval = float(configured_interval) if isinstance(configured_interval, (int, float)) else 0
            allowed_age = max(180.0, min(1_800.0, interval * 1.5 + 60.0))
            if (now - row["lastSeenAt"]).total_seconds() <= allowed_age:
                component_id = str(row["componentId"])
                fresh_components.add(component_id)
                fresh_component_details[component_id] = details
        runtime_components_ready = required_components <= fresh_components
        release_image_digests = _image_digests("RADAR_RELEASE_IMAGE_DIGESTS")
        actual_image_digests = _image_digests("RADAR_ACTUAL_IMAGE_DIGESTS")
        release_images_pinned = (
            set(release_image_digests) == {"api", "web"}
            and actual_image_digests == release_image_digests
        )
        collector_details = fresh_component_details.get("collector-worker", {})
        alert_details = fresh_component_details.get("alert-consumer", {})
        local_evidence_status = (
            app.state.local_evidence_store.status()
            if isinstance(app.state.local_evidence_store, LocalEvidenceStore)
            else None
        )

        def dependency_probe_is_fresh(details: dict[str, object]) -> bool:
            try:
                probe_at = datetime.fromisoformat(str(details.get("dependencyProbeAt")).replace("Z", "+00:00"))
                if probe_at.tzinfo is None:
                    probe_at = probe_at.replace(tzinfo=timezone.utc)
                probe_age = (now - probe_at).total_seconds()
                return -60 <= probe_age <= 1_800
            except (TypeError, ValueError):
                return False

        collector_dependency_probe_fresh = dependency_probe_is_fresh(collector_details)
        alert_dependency_probe_fresh = dependency_probe_is_fresh(alert_details)
        dependency_probes_ready = (
            collector_details.get("redisVerified") is True
            and collector_details.get("r2ReadWriteVerified") is True
            and collector_dependency_probe_fresh
            and fresh_component_details.get("outbox-publisher", {}).get("redisVerified") is True
            and alert_details.get("redisVerified") is True
            and alert_details.get("r2DeleteVerified") is True
            and alert_dependency_probe_fresh
        )
        # The API process intentionally does not hold Redis/R2 credentials.
        # Fresh worker proofs are the configuration and reachability evidence.
        redis_configured = (
            collector_details.get("redisVerified") is True
            and fresh_component_details.get("outbox-publisher", {}).get("redisVerified") is True
            and alert_details.get("redisVerified") is True
        )
        r2_configured = (
            collector_details.get("r2ReadWriteVerified") is True
            and alert_details.get("r2DeleteVerified") is True
        )
        recovery = attestation.get("disasterRecoveryAttestation")
        disaster_recovery_ready = (
            isinstance(recovery, dict) and recovery.get("status") == "passed"
            and int(recovery.get("measuredRpoSeconds", 86401)) <= 3600
            and int(recovery.get("measuredRtoSeconds", 86401)) <= 14400
            and isinstance(recovery.get("performedAt"), datetime)
            and (now - recovery["performedAt"]).days <= 90
        )
        budget_reconciliation_pending = repo.connector_budget_reconciliation_count()
        production_ready = (
            attestation.get("storageBackend") == "postgresql"
            and attestation.get("rlsVerified") is True
            and attestation.get("migrationVersion") == "001_init_rc3.1"
            and attestation.get("auditTriggersVerified") is True
            and attestation.get("migrationMarkerReadOnly") is True
            and attestation.get("databaseUser") == "radar_app"
            and attestation.get("databaseRoleSuperuser") is False
            and attestation.get("databaseRoleBypassRls") is False
            and attestation.get("databaseRoleLeastPrivilege") is True
            and attestation.get("deletionRoleReady") is True
            and isinstance(attestation.get("instanceId"), str) and len(str(attestation["instanceId"])) >= 8
            and isinstance(attestation.get("databaseClockSkewSeconds"), (int, float))
            and float(attestation["databaseClockSkewSeconds"]) <= 5
            and auth_required
            and auth_mode == "jwt"
            and jwt_ready
            and redis_configured and r2_configured and runtime_components_ready and dependency_probes_ready
            and disaster_recovery_ready
            and release_images_pinned
            and budget_reconciliation_pending == 0
        )
        local_ready = (
            profile is RuntimeProfile.LOCAL
            and attestation.get("storageBackend") == "postgresql"
            and required_components <= fresh_components
        )
        return {
            "status": "ok", "version": __version__, "events": len(repo.list_events()),
            "time": datetime.now(timezone.utc).isoformat(), **attestation,
            "authRequired": auth_required, "authMode": auth_mode,
            "jwtConfigurationReady": jwt_ready,
            "redisConfigured": redis_configured, "r2Configured": r2_configured,
            "requiredRuntimeComponents": sorted(required_components),
            "freshRuntimeComponents": sorted(fresh_components),
            "runtimeComponentsReady": runtime_components_ready,
            "dependencyProbesReady": dependency_probes_ready,
            "alertR2DeleteProbeReady": (
                alert_details.get("r2DeleteVerified") is True and alert_dependency_probe_fresh
            ),
            "disasterRecoveryReady": disaster_recovery_ready,
            "releaseImagesPinned": release_images_pinned,
            "budgetReconciliationPending": budget_reconciliation_pending,
            "releaseImageDigests": release_image_digests,
            "actualImageDigests": actual_image_digests,
            "productionReady": production_ready,
            "localReady": local_ready,
            "runtimeProfile": profile.value,
            "freeOnlyMode": free_only_mode(),
            "enabledConnectors": collector_details.get("enabledConnectors", []),
            "embedding": collector_details.get("embedding", {"state": "not_started"}),
            "evidenceStorage": collector_details.get("evidenceStorage", local_evidence_status),
            "lastCollectionStartedAt": collector_details.get("lastCycleStartedAt"),
            "lastCollectionFinishedAt": collector_details.get("lastCycleFinishedAt"),
            "lastCollectionDurationSeconds": collector_details.get("lastCycleDurationSeconds"),
        }

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {
            "status": "ok", "version": __version__,
            "time": datetime.now(timezone.utc).isoformat(),
            "runtimeProfile": profile.value,
        }

    @app.get("/health/ready")
    async def readiness() -> JSONResponse:
        payload = runtime_health_details()
        production = profile is RuntimeProfile.PRODUCTION
        status_code = 200 if not production or payload["productionReady"] is True else 503
        return JSONResponse(status_code=status_code, content={
            "status": "ready" if status_code == 200 else "not_ready",
            "version": __version__, "runtimeProfile": profile.value,
            "productionReady": payload["productionReady"], "localReady": payload["localReady"],
        })

    @app.get("/api/v1/operations/runtime-health")
    async def runtime_health(principal: Principal = Depends(current_principal)) -> dict[str, object]:
        require_role(principal, Role.OWNER)
        return runtime_health_details()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def prometheus_metrics(principal: Principal = Depends(current_principal)) -> PlainTextResponse:
        """Bounded-cardinality operational metrics for the private scraper."""
        require_role(principal, Role.OWNER)
        now = datetime.now(timezone.utc)
        runs = repo.list_connector_runs(now - timedelta(hours=24))
        grouped: dict[tuple[str, str], int] = {}
        connector_latencies: dict[str, list[int]] = {}
        for row in runs:
            connector_id = str(row["connectorId"])
            status = str(row["status"])
            grouped[(connector_id, status)] = grouped.get((connector_id, status), 0) + 1
            connector_latencies.setdefault(connector_id, []).append(int(row["latencyMs"]))
        connectors = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        backlog = repo.pipeline_backlog_counts()
        attestation = repo.runtime_attestation()
        spend = repo.monthly_connector_spend(now)
        budget_limit = float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000"))
        lines = [
            "# HELP radar_events_current Current event clusters.",
            "# TYPE radar_events_current gauge",
            f"radar_events_current {len(repo.list_events())}",
            "# HELP radar_http_requests_total Process-local HTTP requests by status class.",
            "# TYPE radar_http_requests_total counter",
        ]
        for status_class, count in sorted(app.state.http_status_classes.items()):
            lines.append(f'radar_http_requests_total{{status_class="{status_class}"}} {count}')
        durations = list(app.state.http_durations_seconds)
        lines.extend([
            "# HELP radar_http_request_duration_p95_seconds Process-local HTTP duration p95 over the latest 2048 requests.",
            "# TYPE radar_http_request_duration_p95_seconds gauge",
            f"radar_http_request_duration_p95_seconds {_percentile_95([round(value * 1_000_000) for value in durations]) / 1_000_000:.6f}",
            "# HELP radar_connector_runs_24h Connector runs in the trailing 24 hours.",
            "# TYPE radar_connector_runs_24h gauge",
        ])
        for (connector_id, status), count in sorted(grouped.items()):
            lines.append(
                f'radar_connector_runs_24h{{connector="{_prometheus_label(connector_id)}",status="{_prometheus_label(status)}"}} {count}'
            )
        lines.extend([
            "# HELP radar_connector_latency_p95_seconds_24h Connector run latency p95 in the trailing 24 hours.",
            "# TYPE radar_connector_latency_p95_seconds_24h gauge",
        ])
        for connector_id, latencies in sorted(connector_latencies.items()):
            lines.append(
                f'radar_connector_latency_p95_seconds_24h{{connector="{_prometheus_label(connector_id)}"}} {_percentile_95(latencies) / 1000:.3f}'
            )
        lines.extend([
            "# HELP radar_connector_coverage_ratio Current connector coverage from zero to one.",
            "# TYPE radar_connector_coverage_ratio gauge",
        ])
        for connector in sorted(connectors, key=lambda item: item.id):
            lines.append(
                f'radar_connector_coverage_ratio{{connector="{_prometheus_label(connector.id)}",status="{_prometheus_label(connector.status)}"}} {connector.coverage / 100:.4f}'
            )
        lines.extend([
            "# HELP radar_observation_processing_backlog Observations waiting for processing.",
            "# TYPE radar_observation_processing_backlog gauge",
            f'radar_observation_processing_backlog {backlog["processingPending"]}',
            "# HELP radar_outbox_backlog Durable events waiting for publication.",
            "# TYPE radar_outbox_backlog gauge",
            f'radar_outbox_backlog {backlog["outboxPending"]}',
            "# HELP radar_runtime_component_heartbeat_age_seconds Runtime component heartbeat age.",
            "# TYPE radar_runtime_component_heartbeat_age_seconds gauge",
            "# HELP radar_runtime_component_last_cycle_duration_seconds Last reported component cycle duration.",
            "# TYPE radar_runtime_component_last_cycle_duration_seconds gauge",
        ])
        for row in attestation.get("runtimeComponents", []):
            if not isinstance(row, dict) or not isinstance(row.get("lastSeenAt"), datetime):
                continue
            age = max(0.0, (now - row["lastSeenAt"]).total_seconds())
            lines.append(
                f'radar_runtime_component_heartbeat_age_seconds{{component="{_prometheus_label(row.get("componentId", "unknown"))}"}} {age:.3f}'
            )
            details = row.get("details") if isinstance(row.get("details"), dict) else {}
            duration = details.get("lastCycleDurationSeconds")
            if isinstance(duration, (int, float)):
                lines.append(
                    f'radar_runtime_component_last_cycle_duration_seconds{{component="{_prometheus_label(row.get("componentId", "unknown"))}"}} {float(duration):.3f}'
                )
        utilization = spend / budget_limit if budget_limit > 0 else 1
        reconciliation_pending = repo.connector_budget_reconciliation_count()
        lines.extend([
            "# HELP radar_external_data_budget_limit_rmb Configured monthly external-data budget.",
            "# TYPE radar_external_data_budget_limit_rmb gauge",
            f"radar_external_data_budget_limit_rmb {budget_limit:.4f}",
            "# HELP radar_external_data_budget_spend_rmb Recorded monthly connector spend.",
            "# TYPE radar_external_data_budget_spend_rmb gauge",
            f"radar_external_data_budget_spend_rmb {spend:.4f}",
            "# HELP radar_external_data_budget_utilization_ratio Monthly budget utilization.",
            "# TYPE radar_external_data_budget_utilization_ratio gauge",
            f"radar_external_data_budget_utilization_ratio {utilization:.6f}",
            "# HELP radar_connector_budget_reconciliation_pending Expired connector budget reservations awaiting operator reconciliation.",
            "# TYPE radar_connector_budget_reconciliation_pending gauge",
            f"radar_connector_budget_reconciliation_pending {reconciliation_pending}",
        ])
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")

    @app.get("/api/v1/radar", response_model=RadarPayload, response_model_by_alias=True)
    async def radar(
        window: str = Query(default="6h", pattern=r"^(1h|6h|24h|7d)$"),
        sort: Literal["priority", "latest"] = Query(default="priority"),
        limit: int = Query(default=200, ge=1, le=500),
        principal: Principal = Depends(current_principal),
    ) -> RadarPayload:
        connectors = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        generated_at = datetime.now(timezone.utc)
        window_delta = {"1h": timedelta(hours=1), "6h": timedelta(hours=6), "24h": timedelta(hours=24), "7d": timedelta(days=7)}[window]
        cutoff = generated_at - window_delta
        events = []
        for event in repo.list_events():
            points = [point for point in event.timeline if point.at >= cutoff]
            if event.updated_at >= cutoff or points:
                events.append(event.model_copy(update={"timeline": points}))
        prioritized = prioritized_events(events, principal, sort)
        if sort == "latest":
            prioritized = [
                event for event in prioritized
                if event.latest_evidence_at is not None and event.latest_evidence_at >= cutoff
            ]
        return RadarPayload(
            generatedAt=generated_at,
            dataMode="recorded_demo" if isinstance(repo, InMemoryRepository) else "live",
            window=window, sort=sort, events=prioritized[:limit], connectors=connectors,
            totalEvents=len(prioritized), limit=limit, hasMore=len(prioritized) > limit,
        )

    def require_event(event_id: str):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return event

    def require_global_governance(principal: Principal) -> None:
        require_role(principal, Role.ANALYST)
        system_workspace = os.getenv("RADAR_SYSTEM_WORKSPACE_ID", "system-governance")
        if os.getenv("AUTH_REQUIRED", "false").lower() == "true" and principal.workspace_id != system_workspace:
            raise HTTPException(403, "global governance changes require the offline governance workspace")

    @app.get("/api/v1/review-queue")
    async def review_queue(principal: Principal = Depends(current_principal)):
        items = prioritized_events(repo.list_events(), principal)
        entries = repo.list_review_queue_entries(datetime.min.replace(tzinfo=timezone.utc))
        latest_entry = {str(row["eventId"]): row for row in entries}
        return {"generatedAt": datetime.now(timezone.utc).isoformat(), "items": [
            {
                "event": event.model_dump(mode="json", by_alias=True),
                "assessment": event_assessment(event).model_dump(mode="json", by_alias=True),
                "queueEligibility": (
                    {
                        "eligibilityKey": latest_entry[event.id]["eligibilityKey"],
                        "eligibleAt": latest_entry[event.id]["eligibleAt"],
                        "policyVersion": latest_entry[event.id]["policyVersion"],
                    }
                    if event.id in latest_entry else None
                ),
            }
            for event in items
        ]}

    @app.get("/api/v1/events/{event_id}")
    async def event_detail(event_id: str, principal: Principal = Depends(current_principal)):
        event = require_event(event_id)
        queue_entry = max(
            (row for row in repo.list_review_queue_entries(datetime.min.replace(tzinfo=timezone.utc)) if row["eventId"] == event_id),
            key=lambda row: row["eligibleAt"], default=None,
        )
        alert_delivery = max(
            (
                row for row in repo.list_alert_deliveries(principal.workspace_id, datetime.min.replace(tzinfo=timezone.utc))
                if row["eventId"] == event_id and row.get("status", "delivered") == "delivered"
            ),
            key=lambda row: row["deliveredAt"], default=None,
        )
        return {
            "event": event.model_dump(mode="json", by_alias=True),
            "assessment": event_assessment(event).model_dump(mode="json", by_alias=True),
            "decisionContext": {
                "queueEligibilityKey": queue_entry["eligibilityKey"] if queue_entry else None,
                "alertDeliveryKey": (
                    alert_delivery.get("idempotencyKey") or alert_delivery.get("deliveryId")
                    if alert_delivery else None
                ),
                "capturedAt": datetime.now(timezone.utc).isoformat(),
            },
        }

    @app.get("/api/v1/events/{event_id}/score-runs")
    async def event_score_runs(event_id: str, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        require_event(event_id)
        items = []
        for score in repo.list_score_runs(event_id):
            try:
                replayed = replay_score_payload(score.payload)
                expected = score.payload.get("_replay", {}).get("expected", {})
                replay_matches = (
                    replayed.state.value == expected.get("state")
                    and [label.value for label in replayed.labels] == expected.get("labels")
                    and replayed.evidence_strength == expected.get("evidenceStrength")
                    and replayed.uncertainty == expected.get("uncertainty")
                    and replayed.gap_residual == expected.get("gapResidual")
                )
                replay_error = None
            except (TypeError, ValueError, KeyError) as exc:
                replay_matches = False
                replay_error = str(exc)
            items.append({
                "cycleId": score_cycle(score.input_to), "revision": score.scoring_revision,
                "inputFrom": score.input_from, "inputTo": score.input_to, "inputDigest": score.input_digest,
                "inputObservationIds": score.input_observation_ids,
                "scoreVersion": score.score_version, "thresholdVersion": score.threshold_version,
                "baselineVersion": score.baseline_version, "baselineDigest": score.baseline_digest,
                "featureRegistryVersion": score.feature_registry_version,
                "featureRegistryDigest": score.feature_registry_digest,
                "evidencePolicyVersion": score.evidence_policy_version,
                "labelPolicyVersion": score.label_policy_version,
                "clusterVersion": score.cluster_version, "identityVersion": score.identity_version,
                "drivers": score.drivers, "createdAt": score.created_at,
                "replayMatches": replay_matches, "replayError": replay_error,
            })
        return {"eventId": event_id, "items": items}

    @app.get("/api/v1/events/{event_id}/assessment")
    async def assessment(event_id: str, _: Principal = Depends(current_principal)):
        return event_assessment(require_event(event_id)).model_dump(mode="json", by_alias=True)

    @app.get("/api/v1/events/{event_id}/timeline")
    async def event_timeline(event_id: str, _: Principal = Depends(current_principal)):
        event = require_event(event_id)
        return {"eventId": event_id, "lifecycleState": event.state, "points": [point.model_dump(mode="json") for point in event.timeline], "scoringVersion": event.score_version}

    @app.get("/api/v1/events/{event_id}/evidence")
    async def event_evidence(event_id: str, _: Principal = Depends(current_principal)):
        event = require_event(event_id)
        assessment_value = event_assessment(event)
        return {"eventId": event_id, "items": [item.model_dump(mode="json", by_alias=True) for item in event.evidence], "missingEvidence": [gap.model_dump(mode="json") for gap in assessment_value.missing_evidence], "evidenceMask": assessment_value.evidence_mask}

    @app.get("/api/v1/events/{event_id}/evidence/{evidence_id}/raw", include_in_schema=False)
    async def raw_event_evidence(
        event_id: str,
        evidence_id: str,
        _: Principal = Depends(current_principal),
    ) -> Response:
        if profile is not RuntimeProfile.LOCAL or not isinstance(app.state.local_evidence_store, LocalEvidenceStore):
            raise HTTPException(404, "local raw evidence is unavailable")
        require_event(event_id)
        observation = next(
            (
                item for item in repo.list_event_observations(event_id)
                if f"{event_id}:{item.id}" == evidence_id
            ),
            None,
        )
        if observation is None or not observation.raw_evidence_ref:
            raise HTTPException(404, "raw evidence not found")
        try:
            body = await app.state.local_evidence_store.get(observation.raw_evidence_ref)
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(404, "raw evidence not found") from exc
        suffix = observation.raw_evidence_ref.rsplit(".", 1)[-1].lower()
        media_type = {
            "json": "application/json", "xml": "application/xml", "txt": "text/plain",
        }.get(suffix, "application/octet-stream")
        return Response(body, media_type=media_type, headers={"Cache-Control": "private, no-store"})

    @app.get("/api/v1/events/{event_id}/members")
    async def event_members(event_id: str, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        require_event(event_id)
        members = repo.list_event_observations(event_id)
        # Split commands require immutable Observation IDs, never truncated
        # evidence-card IDs. Duplicate metric snapshots collapse to one member.
        by_id = {item.id: item for item in members}
        return {
            "eventId": event_id,
            "items": [
                {
                    "id": item.id,
                    "title": item.title or item.text[:120] or item.id,
                    "source": item.source_id,
                    "platform": item.platform,
                    "publishedAt": item.published_at.isoformat(),
                }
                for item in by_id.values()
            ],
        }

    @app.get("/api/v1/events/{event_id}/lineage")
    async def lineage(event_id: str, principal: Principal = Depends(current_principal)):
        event = require_event(event_id)
        graph = repo.get_event_lineage(event_id)
        return {
            "eventId": event_id, "clusterVersion": event.cluster_version, "supersededBy": event.superseded_by,
            "parents": graph["parents"], "children": graph["children"],
            "currentObservationCount": event.evidence_count or len(event.evidence),
            "pendingOperations": repo.list_cluster_edits(event_id, principal.workspace_id),
        }

    @app.post("/api/v1/events/{event_id}/merge", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def merge_event(event_id: str, request: ClusterEditRequest, principal: Principal = Depends(current_principal)):
        require_global_governance(principal)
        require_event(event_id)
        if not request.target_event_id:
            raise HTTPException(422, "targetEventId is required for merge")
        if request.target_event_id == event_id:
            raise HTTPException(422, "source and target events must differ")
        require_event(request.target_event_id)
        try:
            return repo.queue_cluster_edit(event_id, "merge", request, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/v1/events/{event_id}/split", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def split_event(event_id: str, request: ClusterEditRequest, principal: Principal = Depends(current_principal)):
        require_global_governance(principal)
        require_event(event_id)
        if not request.observation_ids:
            raise HTTPException(422, "observationIds are required for split")
        try:
            return repo.queue_cluster_edit(event_id, "split", request, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.post("/api/v1/cluster-operations/{operation_id}/execute")
    async def execute_cluster_operation(operation_id: str, principal: Principal = Depends(current_principal)):
        require_global_governance(principal)
        try:
            return repo.execute_cluster_edit(operation_id, principal.workspace_id)
        except ValueError as exc:
            status = 409 if "version conflict" in str(exc) or "not queued" in str(exc) else 404
            raise HTTPException(status, str(exc)) from exc

    @app.post("/api/v1/cluster-operations/{operation_id}/revert")
    async def revert_cluster_operation(operation_id: str, principal: Principal = Depends(current_principal)):
        require_global_governance(principal)
        try:
            return repo.revert_cluster_edit(operation_id, principal.workspace_id)
        except ValueError as exc:
            status = 409 if "completed" in str(exc) or "version conflict" in str(exc) else 404
            raise HTTPException(status, str(exc)) from exc

    @app.post("/api/v1/events/{event_id}/feedback", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def event_feedback(event_id: str, request: FeedbackRequest, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        require_event(event_id)
        normalized = request.model_copy(update={"event_id": event_id})
        try:
            return repo.add_feedback(normalized, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/v1/events/{event_id}/behavior-applicability", response_model=MutationReceipt, response_model_by_alias=True)
    async def behavior_applicability(event_id: str, request: BehaviorApplicabilityRequest, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        system_workspace = os.getenv("RADAR_SYSTEM_WORKSPACE_ID", "system-governance")
        if os.getenv("AUTH_REQUIRED", "false").lower() == "true" and principal.workspace_id != system_workspace:
            raise HTTPException(403, "global applicability changes require the offline governance workspace")
        event = require_event(event_id)
        if request.state == "not_applicable" and event.event_type not in {EventType.RESEARCH_OR_BENCHMARK, EventType.OFFICIAL_PRODUCT_RELEASE}:
            raise HTTPException(422, "not_applicable is allowed only for research/benchmark or official product events")
        if request.state == "not_applicable" and (str(event.behavior_evidence_state) == "observed" or event.behavior > 0):
            raise HTTPException(422, "not_applicable conflicts with currently observed behavior evidence")
        return repo.set_behavior_applicability(event_id, request, principal.workspace_id, principal.subject)

    @app.get("/api/v1/coverage")
    async def coverage(_: Principal = Depends(current_principal)):
        values = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        families = sorted({value.family for value in values})
        spend = repo.monthly_connector_spend() + float(os.getenv("EXTERNAL_DATA_SPEND_RMB", "0"))
        budget = float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000"))
        connector_limits = _budget_limit_map("CONNECTOR_BUDGETS_RMB_JSON")
        family_limits = _budget_limit_map("SIGNAL_FAMILY_BUDGETS_RMB_JSON")
        connector_scopes = []
        for connector_id, limit in sorted(connector_limits.items()):
            connector_spend = repo.monthly_connector_spend(connector_ids={connector_id})
            connector_scopes.append({
                "scope": connector_id, "spent": connector_spend, "limit": limit,
                "remaining": max(0, limit - connector_spend),
                "hardPaused": connector_spend >= limit,
            })
        family_scopes = []
        for family, limit in sorted(family_limits.items()):
            connector_ids = {value.id for value in values if value.family == family}
            family_spend = repo.monthly_connector_spend(connector_ids=connector_ids)
            family_scopes.append({
                "scope": family, "spent": family_spend, "limit": limit,
                "remaining": max(0, limit - family_spend),
                "hardPaused": family_spend >= limit,
            })
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(), "signalFamilies": families,
            "budget": {
                "currency": "CNY", "spent": spend, "limit": budget,
                "remaining": max(0, budget - spend),
                "connectorLimits": connector_scopes, "signalFamilyLimits": family_scopes,
            },
            "connectors": [value.model_dump(mode="json", by_alias=True) for value in values],
        }

    @app.get("/api/v1/topics/{event_id}")
    async def topic(event_id: str, _: Principal = Depends(current_principal)):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return event.model_dump(mode="json", by_alias=True)

    @app.get("/api/v1/topics/{event_id}/timeseries")
    async def timeseries(event_id: str, _: Principal = Depends(current_principal)):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return {"eventId": event_id, "points": [point.model_dump(mode="json") for point in event.timeline], "scoreVersion": event.score_version}

    @app.get("/api/v1/topics/{event_id}/evidence")
    async def evidence(event_id: str, _: Principal = Depends(current_principal)):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return {"eventId": event_id, "items": [item.model_dump(mode="json", by_alias=True) for item in event.evidence], "coverageNote": event.coverage_note}

    @app.get("/api/v1/topics/{event_id}/graph")
    async def graph(event_id: str, _: Principal = Depends(current_principal)):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        nodes = [{"id": f"event:{event.id}", "type": "event", "label": event.title}]
        links = []
        for item in event.evidence:
            source_id = f"source:{item.source.lower().replace(' ', '-')}"
            nodes.append({"id": source_id, "type": "source", "label": item.source, "platform": item.platform})
            links.append({"source": source_id, "target": f"event:{event.id}", "relation": item.kind})
        return {"eventId": event_id, "nodes": nodes, "links": links}

    @app.get("/api/v1/sources")
    async def sources(
        status: str | None = Query(default=None, pattern="^(candidate|active|paused|blocked)$"),
        language: str | None = Query(default=None, pattern="^(zh|en|other)$"),
        platform: str | None = Query(default=None, max_length=100),
        query: str | None = Query(default=None, max_length=200),
        limit: int = Query(default=200, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        _: Principal = Depends(current_principal),
    ) -> dict[str, object]:
        generated_at = datetime.now(timezone.utc)
        policy = load_source_score_policy()
        items: list[dict[str, object]] = []
        all_profiles = repo.list_source_profiles()
        for row in all_profiles:
            created_at = row.get("createdAt")
            if not isinstance(created_at, datetime):
                continue
            candidate = SourceCandidate(
                id=str(row["id"]), discovered_at=created_at,
                valid_observations=int(row.get("validObservations") or 0),
                early_hits=int(row.get("earlyHits") or 0), confirmed_hits=int(row.get("confirmedHits") or 0),
                originality=float(row.get("originality") or 0), domain_focus=float(row.get("domainFocus") or 0),
                authority=float(row.get("authority") or 0),
                marketing_matrix_overlap=float(row.get("marketingMatrixOverlap") or 0),
            )
            quality_calibrated = row.get("qualityCalibrated") is True
            history_eligible = source_evidence_eligible(candidate, now=generated_at, policy=policy)
            promotion_eligible = (
                quality_calibrated
                and source_promotion_eligible(candidate, now=generated_at, policy=policy)
            )
            age_days = max(0, int((generated_at - created_at.astimezone(timezone.utc)).total_seconds() // 86400))
            blocked_reasons = []
            if not quality_calibrated:
                blocked_reasons.append("原创度、领域集中度、权威度和营销矩阵重合度尚未完成历史校准")
            if candidate.valid_observations < policy.minimum_valid_observations:
                blocked_reasons.append(
                    f"有效观测 {candidate.valid_observations}/{policy.minimum_valid_observations}"
                )
            if age_days < policy.minimum_history_days:
                blocked_reasons.append(f"历史 {age_days}/{policy.minimum_history_days} 天")
            score = candidate_score(candidate, policy) if quality_calibrated else None
            if score is not None and score < policy.minimum_promotion_score:
                blocked_reasons.append(f"候选分 {score:.2f}/{policy.minimum_promotion_score:.2f}")
            if not policy.auto_promotion_enabled:
                blocked_reasons.append("冻结策略尚未开启自动晋级")
            if not policy.ranking_enabled:
                blocked_reasons.append("SourceScore 排行等待真实结果集校准")
            item = {
                **row,
                "candidateScore": score,
                "scoreEvidenceStatus": "eligible" if quality_calibrated and history_eligible else "insufficient",
                "promotionEligible": promotion_eligible,
                "rankEligible": promotion_eligible and policy.ranking_enabled,
                "blockedReasons": blocked_reasons,
                "historyDays": age_days,
                "scoreVersion": policy.version,
            }
            items.append(item)
        if status:
            items = [item for item in items if item.get("status") == status]
        if language:
            items = [item for item in items if item.get("language") == language]
        if platform:
            items = [item for item in items if str(item.get("platform", "")).lower() == platform.lower()]
        if query:
            lowered = query.lower()
            items = [
                item for item in items
                if lowered in (
                    f"{item.get('id', '')} {item.get('displayName', '')} {item.get('platform', '')} "
                    f"{' '.join(str(value) for value in item.get('accountIds', []))} "
                    f"{' '.join(str(value) for value in item.get('entityIds', []))}"
                ).lower()
            ]
        status_order = {"active": 0, "candidate": 1, "paused": 2, "blocked": 3}
        items.sort(key=lambda item: (
            status_order.get(str(item.get("status")), 9),
            -int(item.get("validObservations") or 0), str(item.get("id")),
        ))
        counts = {
            source_status: sum(item.get("status") == source_status for item in all_profiles)
            for source_status in ("candidate", "active", "paused", "blocked")
        }
        total = len(items)
        page = items[offset:offset + limit]
        return {
            "generatedAt": generated_at.isoformat(), "items": page, "total": total,
            "offset": offset, "limit": limit, "hasMore": offset + len(page) < total,
            "counts": counts, "active": counts["active"],
            "activeCapacity": policy.active_capacity, "candidateCapacity": policy.candidate_capacity,
            "systemCapacity": policy.system_capacity,
            "sourceScorePolicy": {
                "version": policy.version, "digest": source_score_policy_digest(policy),
                "status": policy.status, "frozenAt": policy.frozen_at.isoformat(),
                "timezone": policy.timezone_name,
                "rankingEnabled": policy.ranking_enabled,
                "autoPromotionEnabled": policy.auto_promotion_enabled,
                "minimumValidObservations": policy.minimum_valid_observations,
                "minimumHistoryDays": policy.minimum_history_days,
                "minimumPromotionScore": policy.minimum_promotion_score,
                "dailyGrowthRate": policy.daily_growth_rate,
                "dailyGrowthRounding": policy.daily_growth_rounding,
                "allowAutomaticBootstrap": policy.allow_automatic_bootstrap,
            },
        }

    @app.post("/api/v1/sources/promotions/run")
    async def run_source_promotions(principal: Principal = Depends(current_principal)) -> dict[str, object]:
        require_role(principal, Role.OWNER)
        require_global_governance(principal)
        return repo.promote_source_candidates()

    @app.post("/api/v1/sources/{source_id}/review", response_model=MutationReceipt, response_model_by_alias=True)
    async def review_source(
        source_id: str,
        request: SourceReviewRequest,
        principal: Principal = Depends(current_principal),
    ) -> MutationReceipt:
        require_role(principal, Role.OWNER)
        if profile is RuntimeProfile.PRODUCTION:
            require_global_governance(principal)
        try:
            return repo.review_source_status(
                source_id,
                request.status,
                principal.subject,
                request.reason,
            )
        except KeyError as exc:
            raise HTTPException(404, "source not found") from exc
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc

    @app.get("/api/v1/connectors")
    async def connectors(_: Principal = Depends(current_principal)):
        values = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        return {"items": [value.model_dump(mode="json", by_alias=True) for value in values]}

    @app.get("/api/v1/operations/connector-runs")
    async def connector_runs(hours: int = Query(default=72, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        rows = repo.list_connector_runs(since)
        grouped: dict[str, dict[str, object]] = {}
        for row in rows:
            connector_id = str(row["connectorId"])
            current = grouped.setdefault(connector_id, {
                "connectorId": connector_id, "runs": 0, "healthyRuns": 0, "inserted": 0,
                "duplicates": 0, "failedRuns": 0, "maxLatencyMs": 0,
            })
            current["runs"] = int(current["runs"]) + 1
            current["inserted"] = int(current["inserted"]) + int(row["inserted"])
            current["duplicates"] = int(current["duplicates"]) + int(row["duplicates"])
            current["maxLatencyMs"] = max(int(current["maxLatencyMs"]), int(row["latencyMs"]))
            if row["status"] == "healthy":
                current["healthyRuns"] = int(current["healthyRuns"]) + 1
            else:
                current["failedRuns"] = int(current["failedRuns"]) + 1
        for current in grouped.values():
            observed = int(current["inserted"]) + int(current["duplicates"])
            current["duplicateRate"] = int(current["duplicates"]) / observed if observed else None
            current["healthyRunRate"] = int(current["healthyRuns"]) / int(current["runs"]) if current["runs"] else None
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(), "windowHours": hours,
            "items": rows, "summary": list(grouped.values()),
        }

    @app.get("/api/v1/operations/pipeline-sla")
    async def pipeline_sla(hours: int = Query(default=72, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        current = datetime.now(timezone.utc)
        since = current - timedelta(hours=hours)
        rows = repo.list_observation_processing_history(since)
        threshold_seconds = 15 * 60
        completed_latencies: list[float] = []
        within = matured_pending = pending_window = invalid_future = invalid_order = unrecovered_failures = 0
        for row in rows:
            collected_at = row["collectedAt"]
            completed_at = row.get("completedAt")
            if collected_at > current:
                invalid_future += 1
                continue
            if completed_at is not None:
                if completed_at < collected_at:
                    invalid_order += 1
                    continue
                latency = (completed_at - collected_at).total_seconds()
                completed_latencies.append(latency)
                if latency <= threshold_seconds:
                    within += 1
            elif (current - collected_at).total_seconds() > threshold_seconds:
                matured_pending += 1
                if row.get("lastError"):
                    unrecovered_failures += 1
            else:
                pending_window += 1
        sample = len(completed_latencies) + matured_pending
        rate = within / sample if sample else None
        ordered = sorted(completed_latencies)
        p95 = ordered[max(0, (len(ordered) * 95 + 99) // 100 - 1)] if ordered else None
        return {
            "generatedAt": current.isoformat(), "windowHours": hours,
            "metricScope": "collection_to_scoring_completion",
            "sample": sample, "within15Minutes": within, "rate": rate, "targetRate": .95,
            "evidenceStatus": "eligible" if sample else "insufficient",
            "passesTarget": rate >= .95 if rate is not None else None,
            "completedP95Seconds": p95, "maturedPending": matured_pending,
            "pendingWindowOpen": pending_window, "invalidFutureTimestamps": invalid_future,
            "invalidTimestampOrder": invalid_order, "unrecoveredFailures": unrecovered_failures,
        }

    @app.get("/api/v1/operations/discovery-freshness")
    async def discovery_freshness(hours: int = Query(default=72, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        current = datetime.now(timezone.utc)
        rows = repo.list_observation_freshness(current - timedelta(hours=hours))
        groups: dict[str, dict[str, object]] = {}
        invalid_order = 0
        for row in rows:
            connector_id = str(row["connectorId"])
            target_minutes = 60 if connector_id in {"arxiv", "openalex", "research"} else 15
            available_at = row["availableAt"]
            collected_at = row["collectedAt"]
            if collected_at < available_at:
                invalid_order += 1
                continue
            latency_seconds = max(0.0, (collected_at - available_at).total_seconds())
            group = groups.setdefault(connector_id, {
                "connectorId": connector_id, "targetMinutes": target_minutes, "sample": 0,
                "withinTarget": 0, "providerTimestampSamples": 0, "firstDetectedFallbackSamples": 0,
                "latencies": [],
            })
            group["sample"] = int(group["sample"]) + 1
            group["withinTarget"] = int(group["withinTarget"]) + int(latency_seconds <= target_minutes * 60)
            basis_key = "providerTimestampSamples" if row["availabilityBasis"] == "provider_timestamp" else "firstDetectedFallbackSamples"
            group[basis_key] = int(group[basis_key]) + 1
            group["latencies"].append(latency_seconds)
        items = []
        for group in groups.values():
            latencies = sorted(group.pop("latencies"))
            sample = int(group["sample"])
            provider_sample = int(group["providerTimestampSamples"])
            items.append({
                **group, "rate": int(group["withinTarget"]) / sample if sample else None,
                "p95Seconds": latencies[max(0, (len(latencies) * 95 + 99) // 100 - 1)] if latencies else None,
                "measurementQuality": "provider_visible" if provider_sample == sample else "limited_first_detection_fallback",
                "passesTarget": int(group["withinTarget"]) / sample >= .95 if sample else None,
            })
        return {
            "generatedAt": current, "windowHours": hours, "metricScope": "provider_available_to_collected",
            "items": sorted(items, key=lambda item: item["connectorId"]), "invalidTimestampOrder": invalid_order,
            "measurementLimitation": "first_detected fallback cannot measure time before the first successful probe",
        }

    @app.get("/api/v1/operations/data-quality")
    async def data_quality(hours: int = Query(default=72, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        current = datetime.now(timezone.utc)
        total, duplicates = repo.persisted_content_duplicate_stats(current - timedelta(hours=hours))
        rate = duplicates / total if total else None
        return {
            "generatedAt": current.isoformat(), "windowHours": hours,
            "metricScope": "persisted_within_connector_content_fingerprint_duplicates",
            "sample": total, "persistedDuplicates": duplicates, "rate": rate, "targetMaxRate": .05,
            "evidenceStatus": "eligible" if total else "insufficient",
            "passesTarget": rate < .05 if rate is not None else None,
            "limitations": [
                "Cross-connector copies are not treated as storage duplicates because they may be independent propagation evidence.",
                "Input dedup hits from polling are reported separately and do not count as persisted duplicate leakage.",
            ],
        }

    @app.post("/api/v1/feedback", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def feedback(request: FeedbackRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        require_role(principal, Role.ANALYST)
        if repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        try:
            return repo.add_feedback(request, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/v1/alert-rules", response_model=MutationReceipt, response_model_by_alias=True, status_code=201)
    @app.post("/api/v1/alerts/rules", response_model=MutationReceipt, response_model_by_alias=True, status_code=201, include_in_schema=False)
    async def alerts(request: AlertRuleRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        require_role(principal, Role.ANALYST)
        if profile is RuntimeProfile.LOCAL and request.webhook_url:
            raise HTTPException(422, "local profile supports in-app alerts only")
        if request.webhook_url:
            try:
                await ensure_safe_public_url_resolved(str(request.webhook_url))
            except ConnectorError as exc:
                raise HTTPException(422, f"unsafe webhook URL: {exc}") from exc
        return repo.add_alert(request, principal.workspace_id, principal.subject)

    @app.get("/api/v1/alerts")
    async def alert_feed(hours: int = Query(default=24, ge=1, le=168), principal: Principal = Depends(current_principal)):
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return {"generatedAt": datetime.now(timezone.utc).isoformat(), "items": repo.list_alert_deliveries(principal.workspace_id, since)}

    @app.post("/api/v1/watchlists", response_model=MutationReceipt, response_model_by_alias=True, status_code=201)
    async def watchlist(request: WatchlistRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        if repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        return repo.add_watchlist(request, principal.workspace_id, principal.subject)

    @app.get("/api/v1/watchlists")
    async def watchlists(principal: Principal = Depends(current_principal)):
        return {"items": [item.model_dump(by_alias=True, mode="json") for item in repo.list_watchlists(principal.workspace_id)]}

    @app.delete("/api/v1/watchlists/{event_id}", response_model=MutationReceipt, response_model_by_alias=True)
    async def unwatch(event_id: str, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        receipt = repo.remove_watchlist(event_id, principal.workspace_id)
        if receipt is None:
            raise HTTPException(404, "watchlist entry not found")
        return receipt

    @app.post("/api/v1/interactions", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def product_interaction(request: ProductInteractionRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        if request.kind == "queue_eligible":
            raise HTTPException(403, "queue eligibility is a system-generated scoring fact")
        if request.kind == "alert_quality_reviewed":
            require_role(principal, Role.ANALYST)
        if request.kind in {"metric_exclusion_recorded", "metric_exclusion_reinstated"}:
            require_role(principal, Role.OWNER)
        if request.event_id and repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        try:
            return repo.record_product_interaction(request, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/v1/metric-incidents", response_model=MutationReceipt, response_model_by_alias=True, status_code=201)
    async def create_metric_incident(
        request: MetricIncidentRequest,
        principal: Principal = Depends(current_principal),
    ) -> MutationReceipt:
        require_role(principal, Role.OWNER)
        if repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        try:
            return repo.create_metric_incident(request, principal.workspace_id, principal.subject)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/v1/metrics/review")
    async def review_metrics(hours: int = Query(default=168, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return {"generatedAt": datetime.now(timezone.utc).isoformat(), "windowHours": hours, **review_funnel(repo.list_product_interactions(principal.workspace_id, since))}

    @app.get("/api/v1/metrics/beta")
    async def beta_metrics(hours: int = Query(default=2160, ge=24, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        as_of = datetime.now(timezone.utc)
        policy = load_product_metric_policy()
        since = policy.frozen_at.astimezone(timezone.utc)
        return {
            "generatedAt": as_of.isoformat(),
            "windowHours": (as_of - since).total_seconds() / 3600,
            "requestedMinimumWindowHours": hours,
            **beta_product_metrics(
                deliveries=repo.list_alert_deliveries(principal.workspace_id, since),
                feedback=repo.list_feedback(principal.workspace_id, since),
                queue_entries=repo.list_review_queue_entries(since),
                interactions=repo.list_product_interactions(principal.workspace_id, since),
                incidents=repo.list_metric_incidents(principal.workspace_id, since),
                as_of=as_of,
                policy=policy,
            ),
        }

    @app.get("/api/v1/metrics/ranking-ledger")
    async def ranking_ledger(principal: Principal = Depends(current_principal)) -> dict[str, object]:
        require_role(principal, Role.ANALYST)
        generated_at, ranking_facts, lead_crossings = repo.ranking_ledger_snapshot()
        policy = load_product_metric_policy()
        reviewable = set(policy.reviewable_lifecycle_states)
        rows: list[dict[str, object]] = []
        for fact in ranking_facts:
            event = fact["event"]
            assert hasattr(event, "id")
            score_run_id = fact.get("scoreRunId")
            score = round(.45 * event.attention + .35 * event.behavior + .20 * event.coverage, 6)
            has_score_fact = isinstance(score_run_id, str) and len(score_run_id) >= 8
            eligible = (
                has_score_fact and not event.superseded_by and event.state.value in reviewable
                and event.coverage >= 40 and event.evidence_strength.value in {"medium", "high"}
            )
            rows.append({
                "eventId": event.id, "score": score, "scoreRunId": score_run_id,
                "scoreRunAt": fact.get("scoreRunAt").isoformat() if isinstance(fact.get("scoreRunAt"), datetime) else None,
                "firstDetectedAt": event.first_seen.isoformat(),
                "lifecycleState": event.state.value, "coverage": event.coverage,
                "eligibleTop5": eligible,
            })
        rows.sort(key=lambda row: (-float(row["score"]), str(row["eventId"])))
        for rank, row in enumerate(rows, 1):
            row["rank"] = rank
        artifact: dict[str, object] = {
            "schemaVersion": "signed-score-ledger-v1",
            "productMetricPolicyVersion": policy.version,
            "thresholdVersion": policy.manual_evaluation.required_threshold_version,
            "selectionRuleVersion": "daily-top5-score-v2",
            "generatedAt": generated_at.isoformat(),
            "rows": rows,
            "leadCrossings": [
                {
                    "eventId": row["eventId"],
                    "crossedAt": row["crossedAt"].isoformat() if isinstance(row.get("crossedAt"), datetime) else row.get("crossedAt"),
                    "scoreRunId": row["scoreRunId"], "thresholdVersion": row["thresholdVersion"],
                    "policyVersion": row["policyVersion"],
                }
                for row in lead_crossings
                if row.get("thresholdVersion") == policy.manual_evaluation.required_threshold_version
                and row.get("policyVersion") == policy.version
            ],
            "ledgerKeyId": os.getenv("SCORE_LEDGER_ED25519_KEY_ID", ""),
        }
        private_value = os.getenv("SCORE_LEDGER_ED25519_PRIVATE_KEY", "")
        try:
            private_bytes = base64.b64decode(private_value, validate=True)
            if len(private_bytes) != 32 or len(str(artifact["ledgerKeyId"])) < 8:
                raise ValueError("invalid score ledger signing identity")
            material = json.dumps(artifact, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
            artifact["ledgerSignature"] = base64.b64encode(
                Ed25519PrivateKey.from_private_bytes(private_bytes).sign(material)
            ).decode()
            artifact["ledgerDigest"] = "sha256:" + hashlib.sha256(material).hexdigest()
        except (ValueError, TypeError):
            artifact["ledgerSignature"] = None
            artifact["ledgerDigest"] = None
        return artifact

    @app.delete("/api/v1/privacy/sources/{source_id}")
    async def erase_source(source_id: str, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
        require_global_governance(principal)
        deleted = repo.purge_source(source_id)
        return {"sourceId": source_id, "observationsDeleted": deleted, "rawEvidenceDeletionQueued": True}

    async def stream() -> AsyncIterator[str]:
        while True:
            current_events = repo.list_events()
            snapshot = {"generatedAt": datetime.now(timezone.utc).isoformat(), "eventCount": len(current_events), "maxVelocity": max((event.velocity for event in current_events), default=0)}
            yield f"event: radar\ndata: {json.dumps(snapshot)}\n\n"
            await asyncio.sleep(15)

    @app.get("/api/v1/stream")
    @app.get("/api/v1/stream/radar", include_in_schema=False)
    async def radar_stream(_: Principal = Depends(current_principal)) -> StreamingResponse:
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    return app


app = create_app()
