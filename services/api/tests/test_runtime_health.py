from __future__ import annotations

from datetime import timedelta

from fastapi.testclient import TestClient

from radar.main import create_app
from radar.storage import InMemoryRepository, utcnow


def test_scheduled_component_heartbeat_age_respects_declared_interval() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    for component in (
        "scheduler", "collector-worker", "retention-worker", "outbox-publisher", "alert-consumer",
    ):
        repository.heartbeat_runtime_component(
            component, "scheduled-instance",
            {
                "intervalSeconds": 900,
                **({
                    "redisVerified": True, "r2ReadWriteVerified": True,
                    "dependencyProbeAt": utcnow().isoformat(),
                } if component == "collector-worker" else {}),
                **({"redisVerified": True} if component == "outbox-publisher" else {}),
                **({
                    "redisVerified": True, "r2DeleteVerified": True,
                    "dependencyProbeAt": utcnow().isoformat(),
                } if component == "alert-consumer" else {}),
            },
        )
    for component in ("scheduler", "collector-worker", "retention-worker", "outbox-publisher"):
        repository.runtime_components[component]["lastSeenAt"] = utcnow() - timedelta(minutes=10)
    with TestClient(app) as client:
        assert client.get("/api/v1/operations/runtime-health").json()["runtimeComponentsReady"] is True
    repository.runtime_components["collector-worker"]["lastSeenAt"] = utcnow() - timedelta(minutes=25)
    with TestClient(app) as client:
        health = client.get("/api/v1/operations/runtime-health").json()
    assert health["runtimeComponentsReady"] is False
    assert "collector-worker" not in health["freshRuntimeComponents"]


def test_production_readiness_is_503_when_attestation_gates_are_missing(monkeypatch) -> None:
    monkeypatch.setenv("DEMO_MODE", "false")
    with TestClient(create_app(InMemoryRepository())) as client:
        liveness = client.get("/health")
        assert liveness.status_code == 200
        assert "instanceId" not in liveness.json()
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["productionReady"] is False


def test_stale_dependency_probe_fails_readiness_even_with_fresh_component_heartbeat() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    for component in (
        "scheduler", "collector-worker", "retention-worker", "outbox-publisher", "alert-consumer",
    ):
        repository.heartbeat_runtime_component(
            component, "fresh-instance",
            {
                "intervalSeconds": 900,
                **({
                    "redisVerified": True, "r2ReadWriteVerified": True,
                    "dependencyProbeAt": (utcnow() - timedelta(minutes=31)).isoformat(),
                } if component == "collector-worker" else {}),
                **({"redisVerified": True} if component == "outbox-publisher" else {}),
                **({
                    "redisVerified": True, "r2DeleteVerified": True,
                    "dependencyProbeAt": utcnow().isoformat(),
                } if component == "alert-consumer" else {}),
            },
        )
    with TestClient(app) as client:
        health = client.get("/api/v1/operations/runtime-health").json()
    assert health["runtimeComponentsReady"] is True
    assert health["dependencyProbesReady"] is False


def test_stale_alert_delete_probe_fails_readiness_even_with_fresh_heartbeat() -> None:
    repository = InMemoryRepository()
    app = create_app(repository)
    for component in (
        "scheduler", "collector-worker", "retention-worker", "outbox-publisher", "alert-consumer",
    ):
        repository.heartbeat_runtime_component(
            component, "fresh-instance",
            {
                **({
                    "redisVerified": True, "r2ReadWriteVerified": True,
                    "dependencyProbeAt": utcnow().isoformat(),
                } if component == "collector-worker" else {}),
                **({"redisVerified": True} if component == "outbox-publisher" else {}),
                **({
                    "redisVerified": True, "r2DeleteVerified": True,
                    "dependencyProbeAt": (utcnow() - timedelta(minutes=31)).isoformat(),
                } if component == "alert-consumer" else {}),
            },
        )
    with TestClient(app) as client:
        health = client.get("/api/v1/operations/runtime-health").json()
    assert health["runtimeComponentsReady"] is True
    assert health["alertR2DeleteProbeReady"] is False
    assert health["dependencyProbesReady"] is False
