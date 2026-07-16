from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient

from radar.contracts import Observation, WatchlistRequest
from radar.main import create_app
from radar.storage import InMemoryRepository


def client() -> TestClient:
    return TestClient(create_app(InMemoryRepository()))


def test_radar_contract_and_evidence_traceability() -> None:
    with client() as http:
        response = http.get("/api/v1/radar?window=6h")
        assert response.status_code == 200
        payload = response.json()
        assert payload["window"] == "6h"
        assert payload["dataMode"] == "recorded_demo"
        assert len(payload["events"]) >= 5
        confirmed = next(event for event in payload["events"] if event["state"] == "accelerating")
        assert len(confirmed["evidence"]) >= 3
        assert confirmed["scoreVersion"]
        assert confirmed["thresholdVersion"]

        evidence = http.get(f"/api/v1/topics/{confirmed['id']}/evidence")
        assert evidence.status_code == 200
        assert len(evidence.json()["items"]) >= 3


def test_radar_window_filters_event_timelines_and_old_events() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    now = datetime.now(timezone.utc)
    event = repository.get_event("evt-open-model")
    assert event is not None
    repository.upsert_event(event.model_copy(update={
        "updated_at": now,
        "timeline": [
            event.timeline[0].model_copy(update={"at": now - timedelta(hours=2)}),
            event.timeline[-1].model_copy(update={"at": now - timedelta(minutes=20)}),
        ],
    }))
    old = repository.get_event("evt-benchmark")
    assert old is not None
    repository.upsert_event(old.model_copy(update={
        "updated_at": now - timedelta(hours=2),
        "timeline": [point.model_copy(update={"at": now - timedelta(hours=2)}) for point in old.timeline],
    }))
    with TestClient(app) as http:
        payload = http.get("/api/v1/radar?window=1h").json()
    target = next(item for item in payload["events"] if item["id"] == "evt-open-model")
    assert len(target["timeline"]) == 1
    assert all(item["id"] != "evt-benchmark" for item in payload["events"])


def test_feedback_watchlist_and_unknown_event() -> None:
    with client() as http:
        context = http.get("/api/v1/events/evt-open-model").json()["decisionContext"]
        accepted = http.post("/api/v1/feedback", json={"eventId": "evt-open-model", "action": "confirm", "reason": "证据链完整且跨平台同步", **context})
        assert accepted.status_code == 202
        assert accepted.json()["accepted"] is True
        stale = http.post("/api/v1/feedback", json={
            "eventId": "evt-open-model", "action": "reject", "reason": "stale context must not be rebound",
            "queueEligibilityKey": "queue-key-does-not-exist",
        })
        assert stale.status_code == 409

        watched = http.post("/api/v1/watchlists", json={"eventId": "evt-open-model", "note": "持续观察"})
        assert watched.status_code == 201
        updated = http.post("/api/v1/watchlists", json={"eventId": "evt-open-model", "note": "更新备注"})
        assert updated.status_code == 201
        assert updated.json()["operation"] == "watchlist.update"
        listed = http.get("/api/v1/watchlists")
        assert listed.status_code == 200
        assert listed.json()["items"] == [{
            "id": watched.json()["id"], "eventId": "evt-open-model", "note": "更新备注", "createdAt": watched.json()["createdAt"],
        }]
        removed = http.delete("/api/v1/watchlists/evt-open-model")
        assert removed.status_code == 200
        assert http.get("/api/v1/watchlists").json()["items"] == []
        assert http.delete("/api/v1/watchlists/evt-open-model").status_code == 404
        missing = http.post("/api/v1/watchlists", json={"eventId": "does-not-exist"})
        assert missing.status_code == 404


def test_product_interactions_capture_review_funnel_without_trusting_workspace_input() -> None:
    repository = InMemoryRepository()
    with TestClient(create_app(repository)) as http:
        opened = http.post("/api/v1/interactions", json={
            "kind": "detail_opened", "sessionId": "session-12345678", "eventId": "evt-open-model",
            "idempotencyKey": "interaction-open-1", "metadata": {"window": "6H"},
        })
        assert opened.status_code == 202
        assert repository.product_interactions[0]["workspaceId"] == "local-workspace"
        assert repository.product_interactions[0]["actorId"] == "local-preview"
        assert repository.product_interactions[0]["metadata"] == {"window": "6H"}
        triaged = http.post("/api/v1/interactions", json={
            "kind": "triage_submitted", "sessionId": "session-12345678", "eventId": "evt-open-model",
            "idempotencyKey": "interaction-triage-1", "metadata": {"action": "confirm"},
        })
        assert triaged.status_code == 202
        metrics = http.get("/api/v1/metrics/review").json()
        assert metrics["triageSample"] == 1
        assert metrics["metricScope"] == "exploratory_review_funnel_not_rc2_beta_kpi"
        assert metrics["triageConfirmRate"] == 1
        assert metrics["triageWithin15MinutesFromDetailOpenRate"] == 1
        duplicate = http.post("/api/v1/interactions", json={
            "kind": "triage_submitted", "sessionId": "session-12345678", "eventId": "evt-open-model",
            "idempotencyKey": "interaction-triage-1", "metadata": {"action": "reject"},
        })
        assert duplicate.json()["id"] == triaged.json()["id"]
        assert len(repository.product_interactions) == 2
        assert http.post("/api/v1/interactions", json={
            "kind": "detail_opened", "sessionId": "session-12345678", "eventId": "missing", "idempotencyKey": "interaction-missing-1",
        }).status_code == 404


def test_operational_endpoints_report_duplicate_rate_and_collection_to_scoring_sla() -> None:
    repository = InMemoryRepository()
    current = datetime.now(timezone.utc)
    repository.record_connector_run(
        "rss", current - timedelta(minutes=2), current - timedelta(minutes=1),
        "healthy", inserted=9, duplicates=1, coverage=90,
    )
    item = Observation(
        id="sla-observation", platform="RSS", externalId="sla-observation", sourceId="source-a",
        publishedAt=current - timedelta(minutes=12), collectedAt=current - timedelta(minutes=10),
        language="en", title="SLA event", text="A processing latency fact", url="https://example.com/sla",
        metrics={}, rawEvidenceRef="r2://raw/sla-observation", contentFingerprint="sla-fingerprint",
        signalFamily="official", relation="original",
    )
    assert repository.save_observation_with_outbox(item)
    revision = repository.claim_observation_processing(item.id)
    assert revision == 1
    repository.complete_observation_processing(item.id, revision)
    duplicate = item.model_copy(update={
        "id": "sla-observation-copy", "external_id": "sla-observation-copy",
        "collected_at": current - timedelta(minutes=9), "raw_evidence_ref": "r2://raw/sla-observation-copy",
    })
    assert repository.save_observation_with_outbox(duplicate)

    with TestClient(create_app(repository)) as http:
        connector_payload = http.get("/api/v1/operations/connector-runs?hours=1").json()
        rss = next(row for row in connector_payload["summary"] if row["connectorId"] == "rss")
        assert rss["runs"] == 1
        assert rss["duplicateRate"] == 0.1
        assert rss["healthyRunRate"] == 1

        pipeline = http.get("/api/v1/operations/pipeline-sla?hours=1").json()
        assert pipeline["metricScope"] == "collection_to_scoring_completion"
        assert pipeline["sample"] == 1
        assert pipeline["within15Minutes"] == 1
        assert pipeline["rate"] == 1
        assert pipeline["passesTarget"] is True

        quality = http.get("/api/v1/operations/data-quality?hours=1").json()
        assert quality["metricScope"] == "persisted_within_connector_content_fingerprint_duplicates"
        assert quality["sample"] == 2
        assert quality["persistedDuplicates"] == 1
        assert quality["rate"] == 0.5
        assert quality["passesTarget"] is False


def test_coalesced_processing_revisions_each_receive_a_completion_fact() -> None:
    repository = InMemoryRepository()
    current = datetime.now(timezone.utc)
    first = Observation(
        id="coalesced-observation", platform="GitHub", externalId="repo-a", sourceId="source-a",
        publishedAt=current, collectedAt=current, language="en", title="Repository", text="metric revisions",
        url="https://github.com/example/repo", metrics={"stars": 10},
        rawEvidenceRef="r2://raw/coalesced-1", contentFingerprint="coalesced-fingerprint",
        signalFamily="behavior", relation="original",
    )
    second = first.model_copy(update={
        "collected_at": current + timedelta(minutes=1), "metrics": {"stars": 12},
        "raw_evidence_ref": "r2://raw/coalesced-2",
    })
    assert repository.save_observation_with_outbox(first)
    assert repository.save_observation_with_outbox(second)
    revision = repository.claim_observation_processing(first.id)
    assert revision == 2
    repository.complete_observation_processing(first.id, revision)
    history = repository.list_observation_processing_history(current - timedelta(minutes=1))
    assert [row["revision"] for row in history] == [1, 2]
    assert all(row["completedAt"] is not None for row in history)


def test_role_enforcement_when_auth_is_enabled(monkeypatch) -> None:
    keys = {
        "viewer-key": {"subject": "viewer", "role": "VIEWER", "workspaceId": "workspace-a"},
        "analyst-key": {"subject": "analyst", "role": "ANALYST", "workspaceId": "workspace-a"},
        "owner-key": {"subject": "owner", "role": "OWNER", "workspaceId": "workspace-a"},
        "governance-key": {"subject": "cluster-reviewer", "role": "ANALYST", "workspaceId": "system-governance"},
        "governance-owner-key": {"subject": "source-governor", "role": "OWNER", "workspaceId": "system-governance"},
    }
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("RADAR_API_KEYS", json.dumps(keys))
    with client() as http:
        assert http.get("/api/v1/radar").status_code == 401
        assert http.get("/api/v1/radar", headers={"X-API-Key": "viewer-key"}).status_code == 200
        context = http.get("/api/v1/events/evt-open-model", headers={"X-API-Key": "analyst-key"}).json()["decisionContext"]
        denied = http.post("/api/v1/feedback", headers={"X-API-Key": "viewer-key"}, json={"eventId": "evt-open-model", "action": "confirm", "reason": "viewer cannot edit", **context})
        assert denied.status_code == 403
        allowed = http.post("/api/v1/feedback", headers={"X-API-Key": "analyst-key"}, json={"eventId": "evt-open-model", "action": "confirm", "reason": "analyst can edit", **context})
        assert allowed.status_code == 202
        global_change = http.post(
            "/api/v1/events/evt-benchmark/behavior-applicability",
            headers={"X-API-Key": "analyst-key"},
            json={"state": "missing", "reason": "workspace analyst cannot change the global applicability mask"},
        )
        assert global_change.status_code == 403
        global_owner_change = http.post(
            "/api/v1/events/evt-benchmark/behavior-applicability",
            headers={"X-API-Key": "owner-key"},
            json={"state": "missing", "reason": "tenant owner is not the offline governance workspace"},
        )
        assert global_owner_change.status_code == 403
        tenant_cluster_change = http.post(
            "/api/v1/events/evt-open-model/merge",
            headers={"X-API-Key": "analyst-key"},
            json={"targetEventId": "evt-benchmark", "reason": "tenant cannot mutate global topology"},
        )
        assert tenant_cluster_change.status_code == 403
        governance_cluster_change = http.post(
            "/api/v1/events/evt-open-model/merge",
            headers={"X-API-Key": "governance-key"},
            json={"targetEventId": "evt-benchmark", "reason": "offline-reviewed global topology correction"},
        )
        assert governance_cluster_change.status_code == 202
        assert http.post(
            "/api/v1/sources/promotions/run", headers={"X-API-Key": "owner-key"},
        ).status_code == 403
        governed_promotion = http.post(
            "/api/v1/sources/promotions/run", headers={"X-API-Key": "governance-owner-key"},
        )
        assert governed_promotion.status_code == 200
        assert governed_promotion.json()["autoPromotionEnabled"] is False
        assert governed_promotion.json()["promotedSourceIds"] == []


def test_graph_and_source_capacity_contract() -> None:
    with client() as http:
        graph = http.get("/api/v1/topics/evt-open-model/graph").json()
        assert len(graph["nodes"]) >= 4
        assert all(link["target"] == "event:evt-open-model" for link in graph["links"])
        sources = http.get("/api/v1/sources").json()
        assert sources["candidateCapacity"] == 500
        assert sources["systemCapacity"] == 2000
        assert sources["counts"]["candidate"] > 0
        assert sources["sourceScorePolicy"]["rankingEnabled"] is False
        assert sources["sourceScorePolicy"]["autoPromotionEnabled"] is False
        assert sources["sourceScorePolicy"]["timezone"] == "Asia/Shanghai"
        assert sources["sourceScorePolicy"]["minimumValidObservations"] == 5
        assert sources["sourceScorePolicy"]["minimumHistoryDays"] == 7
        assert sources["sourceScorePolicy"]["dailyGrowthRounding"] == "floor"
        assert sources["sourceScorePolicy"]["allowAutomaticBootstrap"] is False
        assert sources["items"][0]["candidateScore"] is None
        assert sources["items"][0]["scoreEvidenceStatus"] == "insufficient"
        assert sources["items"][0]["rankEligible"] is False
        assert any("尚未完成历史校准" in reason for reason in sources["items"][0]["blockedReasons"])


def test_source_catalog_paginates_and_searches_all_active_and_candidate_capacity() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    repository.source_profiles.clear()
    observed_at = datetime.now(timezone.utc) - timedelta(days=10)
    for index in range(700):
        source_id = f"source-{index:03d}"
        repository.register_source_candidate(
            source_id=source_id, display_name=f"Source {index:03d}", platform="RSS", language="en",
            observed_at=observed_at, reason="capacity_regression_fixture",
            account_id="account-tail-699" if index == 699 else None,
            entity_id="entity-tail-699" if index == 699 else None,
        )
        if index < 200:
            repository.source_profiles[source_id]["status"] = "active"
    with TestClient(app) as http:
        first = http.get("/api/v1/sources?limit=500").json()
        assert first["total"] == 700
        assert len(first["items"]) == 500
        assert first["hasMore"] is True
        assert first["offset"] == 0
        second = http.get("/api/v1/sources?limit=500&offset=500").json()
        assert len(second["items"]) == 200
        assert second["hasMore"] is False
        tail = http.get("/api/v1/sources?status=candidate&query=source-699&limit=20").json()
        assert tail["total"] == 1
        assert [item["id"] for item in tail["items"]] == ["source-699"]
        assert http.get("/api/v1/sources?query=account-tail-699").json()["items"][0]["id"] == "source-699"
        assert http.get("/api/v1/sources?query=entity-tail-699").json()["items"][0]["id"] == "source-699"


def test_revised_assessment_contract_exposes_na_masks_and_non_precise_strength() -> None:
    with client() as http:
        assessment = http.get("/api/v1/events/evt-benchmark/assessment")
        assert assessment.status_code == 200
        payload = assessment.json()
        assert payload["clusterVersion"] >= 1
        assert payload["evidenceStrength"] in {"low", "medium", "high"}
        assert payload["evidenceMask"]["official"] == "missing"
        assert payload["observedFeatureWeight"] < payload["expectedFeatureWeight"]
        assert payload["missingEvidence"]
        assert payload["baselineMaturity"] <= 1
        assert payload["clusterConfidence"] <= 1

        queue = http.get("/api/v1/review-queue").json()
        assert queue["items"][0]["assessment"]["decisionReason"]["drivers"]
        coverage = http.get("/api/v1/coverage").json()
        assert coverage["connectors"]
        assert coverage["budget"] == {"currency": "CNY", "spent": 0, "limit": 2000, "remaining": 2000}
        youtube = next(item for item in coverage["connectors"] if item["id"] == "youtube")
        assert youtube["quotaUsed"] == 7200
        assert youtube["rightsStatus"] == "blocked"


def test_analyst_can_explicitly_mark_research_behavior_not_applicable() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    event = repository.get_event("evt-benchmark")
    assert event is not None
    repository.upsert_event(event.model_copy(update={
        "behavior": 0,
        "evidence": [item for item in event.evidence if item.kind != "behavior"],
        "behavior_evidence_state": "missing",
    }))
    repository.event_observations["evt-benchmark"] = {"obs-for-rescore": .9}
    repository.observation_processing["obs-for-rescore"] = {
        "revision": 1, "processedRevision": 1, "attempts": 1, "leaseUntil": None, "lastError": None,
    }
    with TestClient(app) as http:
        response = http.post("/api/v1/events/evt-benchmark/behavior-applicability", json={
            "state": "not_applicable", "reason": "该基准公告当前没有可定义的采用行为",
        })
        assert response.status_code == 200
        assessment = http.get("/api/v1/events/evt-benchmark/assessment").json()
    assert assessment["evidenceMask"]["behavior"] == "not_applicable"
    assert assessment["expectedFeatureWeight"] == .65
    assert repository.outbox[-1]["kind"] == "event.rescore.requested"
    assert repository.get_event("evt-benchmark").state == "insufficient_data"
    assert repository.observation_processing["obs-for-rescore"]["revision"] == 2


def test_na_rejects_full_behavior_mask_even_when_display_evidence_has_no_behavior() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    event = repository.get_event("evt-benchmark")
    assert event is not None
    repository.upsert_event(event.model_copy(update={
        "behavior": 0, "behavior_evidence_state": "observed", "signal_families": ["research", "behavior"],
        "evidence": [item for item in event.evidence if item.kind != "behavior"],
    }))
    with TestClient(app) as http:
        response = http.post("/api/v1/events/evt-benchmark/behavior-applicability", json={
            "state": "not_applicable", "reason": "should conflict with full mask",
        })
    assert response.status_code == 422


def test_workspace_identity_is_server_derived_for_mutations(monkeypatch) -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    keys = {
        "a": {"subject": "analyst-a", "role": "ANALYST", "workspaceId": "workspace-a"},
        "b": {"subject": "analyst-b", "role": "ANALYST", "workspaceId": "workspace-b"},
    }
    monkeypatch.setenv("AUTH_REQUIRED", "true")
    monkeypatch.setenv("RADAR_API_KEYS", json.dumps(keys))
    with TestClient(app) as http:
        for key in keys:
            response = http.post("/api/v1/watchlists", headers={"X-API-Key": key}, json={"eventId": "evt-open-model"})
            assert response.status_code == 201
    workspaces = {value[0] for value in repository.watchlists.values()}
    assert workspaces == {"workspace-a", "workspace-b"}
    with TestClient(app) as http:
        assert len(http.get("/api/v1/watchlists", headers={"X-API-Key": "a"}).json()["items"]) == 1
        assert len(http.get("/api/v1/watchlists", headers={"X-API-Key": "b"}).json()["items"]) == 1
        assert http.delete("/api/v1/watchlists/evt-open-model", headers={"X-API-Key": "a"}).status_code == 200
        assert http.get("/api/v1/watchlists", headers={"X-API-Key": "a"}).json()["items"] == []
        assert len(http.get("/api/v1/watchlists", headers={"X-API-Key": "b"}).json()["items"]) == 1


def test_cluster_edit_is_an_explicit_auditable_async_command() -> None:
    repository = InMemoryRepository()
    with TestClient(create_app(repository)) as http:
        response = http.post(
            "/api/v1/events/evt-open-model/merge",
            json={"targetEventId": "evt-benchmark", "reason": "两簇共享同一个官方发布与规范化 URL"},
        )
        assert response.status_code == 202
        receipt = response.json()
        assert receipt["status"] == "queued"
        assert receipt["operation"] == "cluster.merge"
        lineage = http.get("/api/v1/events/evt-open-model/lineage").json()
        assert lineage["pendingOperations"][0]["id"] == receipt["id"]
        assert "reason" not in lineage["pendingOperations"][0]
        same = http.post(
            "/api/v1/events/evt-open-model/merge",
            json={"targetEventId": "evt-open-model", "reason": "invalid self merge"},
        )
        assert same.status_code == 422


def test_cluster_merge_executes_with_lineage_watch_inheritance_and_revert() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    repository.event_observations["evt-open-model"] = {"obs-a": .91}
    repository.event_observations["evt-benchmark"] = {"obs-b": .84}
    with TestClient(app) as http:
        watch = http.post("/api/v1/watchlists", json={"eventId": "evt-open-model", "note": "follow lineage"})
        assert watch.status_code == 201
        queued = http.post("/api/v1/events/evt-open-model/merge", json={
            "targetEventId": "evt-benchmark", "reason": "官方发布和实体完全一致",
        }).json()
        executed = http.post(f"/api/v1/cluster-operations/{queued['id']}/execute")
        assert executed.status_code == 200
        child_id = executed.json()["resultEventIds"][0]
        assert repository.event_observations[child_id] == {"obs-a": .91, "obs-b": .84}
        assert {event.id for event in repository.list_events()}.isdisjoint({"evt-open-model", "evt-benchmark"})
        assert repository.score_runs == []
        lineage = http.get("/api/v1/events/evt-open-model/lineage").json()
        assert lineage["supersededBy"] == [child_id]
        assert lineage["children"][0]["childEventId"] == child_id
        child_lineage = http.get(f"/api/v1/events/{child_id}/lineage").json()
        assert child_lineage["pendingOperations"][0]["id"] == queued["id"]
        assert child_lineage["pendingOperations"][0]["status"] == "completed"
        watched = http.get("/api/v1/watchlists").json()["items"]
        assert {item["eventId"] for item in watched} == {child_id}

        reverted = http.post(f"/api/v1/cluster-operations/{queued['id']}/revert")
        assert reverted.status_code == 200
        assert repository.event_observations["evt-open-model"] == {"obs-a": .91}
        assert repository.event_observations["evt-benchmark"] == {"obs-b": .84}
        assert {event.id for event in repository.list_events()}.issuperset({"evt-open-model", "evt-benchmark"})
        assert {item["eventId"] for item in http.get("/api/v1/watchlists").json()["items"]} == {"evt-open-model"}


def test_cluster_revert_invalidates_command_queued_before_the_topology_change() -> None:
    repository = InMemoryRepository()
    repository.event_observations["evt-open-model"] = {"obs-a": .91, "obs-b": .82}
    repository.event_observations["evt-benchmark"] = {"obs-c": .84}
    with TestClient(create_app(repository)) as http:
        stale = http.post("/api/v1/events/evt-open-model/split", json={
            "observationIds": ["obs-a"], "reason": "queued before a different topology edit",
        }).json()
        merge = http.post("/api/v1/events/evt-open-model/merge", json={
            "targetEventId": "evt-benchmark", "reason": "newer analyst decision",
        }).json()
        assert http.post(f"/api/v1/cluster-operations/{merge['id']}/execute").status_code == 200
        assert http.post(f"/api/v1/cluster-operations/{merge['id']}/revert").status_code == 200
        assert http.post(f"/api/v1/cluster-operations/{stale['id']}/execute").status_code == 409


def test_watch_in_another_workspace_resolves_to_active_cluster_successor() -> None:
    repository = InMemoryRepository()
    repository.event_observations["evt-open-model"] = {"obs-a": .91}
    repository.event_observations["evt-benchmark"] = {"obs-b": .84}
    repository.add_watchlist(WatchlistRequest(eventId="evt-open-model", note="other workspace"), "workspace-b", "analyst-b")
    with TestClient(create_app(repository)) as http:
        merge = http.post("/api/v1/events/evt-open-model/merge", json={
            "targetEventId": "evt-benchmark", "reason": "global topology mutation",
        }).json()
        child_id = http.post(f"/api/v1/cluster-operations/{merge['id']}/execute").json()["resultEventIds"][0]
    assert [item.event_id for item in repository.list_watchlists("workspace-b")] == [child_id]


def test_cluster_split_creates_two_successors_and_optimistic_lock_rejects_stale_command() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    repository.event_observations["evt-open-model"] = {"obs-a": .91, "obs-b": .84, "obs-c": .72}
    with TestClient(app) as http:
        queued = http.post("/api/v1/events/evt-open-model/split", json={
            "observationIds": ["obs-a"], "reason": "版本实体冲突，需要拆分",
        }).json()
        executed = http.post(f"/api/v1/cluster-operations/{queued['id']}/execute")
        assert executed.status_code == 200
        child_ids = executed.json()["resultEventIds"]
        assert len(child_ids) == 2
        assert {frozenset(repository.event_observations[child_id]) for child_id in child_ids} == {frozenset({"obs-a"}), frozenset({"obs-b", "obs-c"})}

        repository.event_observations[child_ids[0]] = {"obs-a": .91, "obs-x": .5}
        next_command = http.post(f"/api/v1/events/{child_ids[0]}/split", json={
            "observationIds": ["obs-x"], "reason": "新增成员形成独立事件",
        }).json()
        event = repository.get_event(child_ids[0])
        assert event is not None
        repository.upsert_event(event.model_copy(update={"cluster_version": event.cluster_version + 1}))
        conflict = http.post(f"/api/v1/cluster-operations/{next_command['id']}/execute")
        assert conflict.status_code == 409


def test_cluster_member_endpoint_returns_real_observation_ids_for_split() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    now = datetime.now(timezone.utc)
    for observation_id in ("member-a", "member-b"):
        item = Observation(
            id=observation_id, platform="RSS", externalId=observation_id, sourceId=f"source-{observation_id}",
            publishedAt=now, collectedAt=now, language="en", title=f"Observation {observation_id}",
            text="cluster member", url=f"https://example.com/{observation_id}", rawEvidenceRef=f"r2://raw/{observation_id}.json",
            signalFamily="discussion",
        )
        repository.save_observation_with_outbox(item)
        repository.assign_observation("evt-open-model", observation_id, .9, "test")
    with TestClient(app) as http:
        response = http.get("/api/v1/events/evt-open-model/members")
    assert response.status_code == 200
    assert {item["id"] for item in response.json()["items"]} == {"member-a", "member-b"}


def test_superseded_event_rejects_new_cluster_command() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    repository.event_observations["evt-open-model"] = {"obs-a": .91}
    repository.event_observations["evt-benchmark"] = {"obs-b": .84}
    with TestClient(app) as http:
        queued = http.post("/api/v1/events/evt-open-model/merge", json={
            "targetEventId": "evt-benchmark", "reason": "共同官方实体与发布时间",
        }).json()
        assert http.post(f"/api/v1/cluster-operations/{queued['id']}/execute").status_code == 200
        rejected = http.post("/api/v1/events/evt-open-model/merge", json={
                "targetEventId": "evt-agent-campaign", "reason": "过期页面不能继续发起修订",
        })
        assert rejected.status_code == 422


def test_alert_rule_rejects_private_webhook_before_persistence() -> None:
    repository = InMemoryRepository()
    with TestClient(create_app(repository)) as http:
        response = http.post("/api/v1/alert-rules", json={
            "name": "unsafe hook", "webhookUrl": "http://127.0.0.1/admin",
        })
    assert response.status_code == 422
    assert repository.alerts == {}
