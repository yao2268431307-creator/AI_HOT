from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from . import __version__
from .assessment import event_assessment
from .auth import Principal, Role, current_principal, require_role
from .contracts import AlertRuleRequest, BehaviorApplicabilityRequest, ClusterEditRequest, EventType, FeedbackRequest, MutationReceipt, ProductInteractionRequest, RadarPayload, WatchlistRequest
from .fixtures import seed_repository
from .product_metrics import review_funnel
from .storage import InMemoryRepository, PostgresRepository
from .connectors.base import ConnectorError, ensure_safe_public_url_resolved


def create_app(repository: InMemoryRepository | PostgresRepository | None = None) -> FastAPI:
    if repository is None:
        production_dsn = os.getenv("DATABASE_URL") if os.getenv("DEMO_MODE", "true").lower() == "false" else None
        repository = PostgresRepository(production_dsn) if production_dsn else InMemoryRepository()
    repo = seed_repository(repository) if isinstance(repository, InMemoryRepository) else repository
    app = FastAPI(title="SIGNAL//AI Radar API", version=__version__, docs_url="/docs")
    origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",") if origin.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_credentials=True, allow_methods=["GET", "POST", "DELETE"], allow_headers=["*"])
    app.state.repository = repo

    @app.get("/health")
    async def health() -> dict[str, object]:
        return {"status": "ok", "version": __version__, "events": len(repo.list_events()), "time": datetime.now(timezone.utc).isoformat()}

    @app.get("/api/v1/radar", response_model=RadarPayload, response_model_by_alias=True)
    async def radar(window: str = Query(default="6h", pattern=r"^(1h|6h|24h|7d)$"), _: Principal = Depends(current_principal)) -> RadarPayload:
        connectors = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        generated_at = datetime.now(timezone.utc)
        window_delta = {"1h": timedelta(hours=1), "6h": timedelta(hours=6), "24h": timedelta(hours=24), "7d": timedelta(days=7)}[window]
        cutoff = generated_at - window_delta
        events = []
        for event in repo.list_events():
            points = [point for point in event.timeline if point.at >= cutoff]
            if event.updated_at >= cutoff or points:
                events.append(event.model_copy(update={"timeline": points}))
        return RadarPayload(generatedAt=generated_at, window=window, events=events, connectors=connectors)

    def require_event(event_id: str):
        event = repo.get_event(event_id)
        if event is None:
            raise HTTPException(404, "event not found")
        return event

    def require_global_governance(principal: Principal) -> None:
        require_role(principal, Role.ANALYST)
        system_workspace = os.getenv("RADAR_SYSTEM_WORKSPACE_ID", "system-governance")
        if os.getenv("AUTH_REQUIRED", "false").lower() == "true" and principal.workspace_id != system_workspace:
            raise HTTPException(403, "global cluster topology changes require the offline governance workspace")

    @app.get("/api/v1/review-queue")
    async def review_queue(_: Principal = Depends(current_principal)):
        items = repo.list_events()
        return {"generatedAt": datetime.now(timezone.utc).isoformat(), "items": [
            {"event": event.model_dump(mode="json", by_alias=True), "assessment": event_assessment(event).model_dump(mode="json", by_alias=True)}
            for event in items
        ]}

    @app.get("/api/v1/events/{event_id}")
    async def event_detail(event_id: str, _: Principal = Depends(current_principal)):
        event = require_event(event_id)
        return {"event": event.model_dump(mode="json", by_alias=True), "assessment": event_assessment(event).model_dump(mode="json", by_alias=True)}

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
        return repo.add_feedback(normalized, principal.workspace_id, principal.subject)

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
        spend = repo.monthly_connector_spend()
        budget = float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000"))
        return {
            "generatedAt": datetime.now(timezone.utc).isoformat(), "signalFamilies": families,
            "budget": {"currency": "CNY", "spent": spend, "limit": budget, "remaining": max(0, budget - spend)},
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
    async def sources(_: Principal = Depends(current_principal)):
        grouped: dict[str, dict[str, object]] = {}
        for event in repo.list_events():
            for item in event.evidence:
                current = grouped.setdefault(item.source, {"id": item.source.lower().replace(" ", "-"), "name": item.source, "platform": item.platform, "hits": 0})
                current["hits"] = int(current["hits"]) + 1
        return {"items": list(grouped.values()), "active": len(grouped), "candidateCapacity": 500, "systemCapacity": 2000}

    @app.get("/api/v1/connectors")
    async def connectors(_: Principal = Depends(current_principal)):
        values = list(repo.connectors.values()) if isinstance(repo, InMemoryRepository) else repo.list_connectors()
        return {"items": [value.model_dump(mode="json", by_alias=True) for value in values]}

    @app.post("/api/v1/feedback", response_model=MutationReceipt, response_model_by_alias=True, status_code=202)
    async def feedback(request: FeedbackRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        require_role(principal, Role.ANALYST)
        if repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        return repo.add_feedback(request, principal.workspace_id, principal.subject)

    @app.post("/api/v1/alert-rules", response_model=MutationReceipt, response_model_by_alias=True, status_code=201)
    @app.post("/api/v1/alerts/rules", response_model=MutationReceipt, response_model_by_alias=True, status_code=201, include_in_schema=False)
    async def alerts(request: AlertRuleRequest, principal: Principal = Depends(current_principal)) -> MutationReceipt:
        require_role(principal, Role.ANALYST)
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
        if request.event_id and repo.get_event(request.event_id) is None:
            raise HTTPException(404, "event not found")
        return repo.record_product_interaction(request, principal.workspace_id, principal.subject)

    @app.get("/api/v1/metrics/review")
    async def review_metrics(hours: int = Query(default=168, ge=1, le=2160), principal: Principal = Depends(current_principal)):
        require_role(principal, Role.ANALYST)
        since = datetime.now(timezone.utc) - timedelta(hours=hours)
        return {"generatedAt": datetime.now(timezone.utc).isoformat(), "windowHours": hours, **review_funnel(repo.list_product_interactions(principal.workspace_id, since))}

    @app.delete("/api/v1/privacy/sources/{source_id}")
    async def erase_source(source_id: str, principal: Principal = Depends(current_principal)):
        require_role(principal, Role.OWNER)
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
