from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json

import pytest
from fastapi.testclient import TestClient

from radar.evidence_store import LocalEvidenceStore
from radar.embeddings import LocalBgeM3Provider
from radar.main import create_app
from radar.runner import build_connectors, build_embedding_provider, validate_runtime_configuration
from radar.runtime import validate_local_access_configuration
from radar.storage import InMemoryRepository


def configure_local(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_PROFILE", "local")
    monkeypatch.setenv("AUTH_REQUIRED", "false")
    monkeypatch.setenv("RADAR_API_HOST", "127.0.0.1")
    monkeypatch.setenv("RADAR_WEB_HOST", "127.0.0.1")
    monkeypatch.setenv("CORS_ORIGINS", "http://127.0.0.1:3210")
    monkeypatch.setenv("DATABASE_URL", "postgresql://radar_app:secret@127.0.0.1:5432/ai_hot")
    monkeypatch.setenv("EXTERNAL_DATA_BUDGET_RMB", "0")
    monkeypatch.setenv("FREE_ONLY_MODE", "true")
    for name in ("R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"):
        monkeypatch.delenv(name, raising=False)


def test_local_runtime_requires_no_managed_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_local(monkeypatch)
    monkeypatch.delenv("REDIS_URL", raising=False)
    monkeypatch.delenv("RADAR_JWT_PUBLIC_KEYS", raising=False)
    assert validate_runtime_configuration() is False


def test_unauthenticated_local_runtime_rejects_nonloopback_bind(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_local(monkeypatch)
    monkeypatch.setenv("RADAR_API_HOST", "0.0.0.0")
    with pytest.raises(RuntimeError, match="loopback"):
        validate_local_access_configuration()


@pytest.mark.asyncio
async def test_free_only_connector_selection_blocks_metered_sources(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    configure_local(monkeypatch)
    monkeypatch.setenv("RSS_FEEDS_FILE", str(tmp_path / "missing.json"))
    monkeypatch.setenv("BLUESKY_JETSTREAM_ENABLED", "false")
    monkeypatch.setenv("ENABLED_CONNECTORS", "hackernews,github,huggingface,arxiv")
    connectors = build_connectors(LocalEvidenceStore(tmp_path / "raw"))
    try:
        assert {item.id for item in connectors} == {"hackernews", "github", "huggingface", "arxiv"}
        assert all(item.estimated_cost_per_request_rmb == 0 for item in connectors)
    finally:
        for connector in connectors:
            await connector.close()
    monkeypatch.setenv("ENABLED_CONNECTORS", "openalex")
    with pytest.raises(RuntimeError, match="FREE_ONLY_MODE.*openalex"):
        build_connectors(LocalEvidenceStore(tmp_path / "blocked"))


def test_local_embedding_provider_is_lazy_and_reports_state(monkeypatch: pytest.MonkeyPatch) -> None:
    configure_local(monkeypatch)
    monkeypatch.setenv("EMBEDDING_BACKEND", "local_bge_m3")
    provider = build_embedding_provider()
    assert provider is not None
    assert provider.status()["state"] == "not_loaded"
    assert provider.status()["backend"] == "local_bge_m3"


@pytest.mark.asyncio
async def test_local_embedding_failure_enters_cooldown(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    provider = LocalBgeM3Provider(cache_dir=tmp_path, retry_cooldown_seconds=30)
    attempts = 0

    def fail(_texts: list[str]) -> list[list[float]]:
        nonlocal attempts
        attempts += 1
        raise OSError("offline")

    monkeypatch.setattr(provider, "_embed_sync", fail)
    with pytest.raises(OSError, match="offline"):
        await provider.embed(["测试 AI model"])
    with pytest.raises(RuntimeError, match="cooling down"):
        await provider.embed(["second attempt"])
    assert attempts == 1
    assert provider.status()["state"] == "degraded"
    assert provider.status()["retryAfterSeconds"] > 0


@pytest.mark.asyncio
async def test_local_evidence_capacity_uses_auditable_placeholder(tmp_path) -> None:
    store = LocalEvidenceStore(tmp_path, max_bytes=128)
    reference = "r2://local/rss/2026/07/21/large.json"
    await store.put(reference, b"x" * 5000, "application/json")
    payload = json.loads((await store.get(reference)).decode())
    assert payload["storageOmitted"] is True
    assert payload["originalBytes"] == 5000
    with pytest.raises(ValueError):
        await store.get("r2://local/../outside")


def test_local_profile_rejects_external_webhook_rules(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    configure_local(monkeypatch)
    monkeypatch.setenv("RAW_EVIDENCE_LOCAL_DIR", str(tmp_path))
    with TestClient(create_app(InMemoryRepository())) as client:
        response = client.post("/api/v1/alert-rules", json={
            "name": "external hook", "webhookUrl": "https://hooks.example.com/radar",
        })
        health = client.get("/api/v1/operations/runtime-health").json()
    assert response.status_code == 422
    assert health["runtimeProfile"] == "local"
    assert health["freeOnlyMode"] is True
    assert health["redisConfigured"] is False
    assert health["r2Configured"] is False


def test_local_owner_can_manually_review_source_status(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    configure_local(monkeypatch)
    repository = InMemoryRepository()
    repository.register_source_candidate(
        source_id="official-lab", display_name="Official Lab", platform="RSS", language="en",
        observed_at=datetime.now(timezone.utc),
        reason="manually_reviewed_seed",
    )
    with TestClient(create_app(repository)) as client:
        activated = client.post("/api/v1/sources/official-lab/review", json={
            "status": "active", "reason": "Official feed manually verified for local monitoring",
        })
        duplicate = client.post("/api/v1/sources/official-lab/review", json={
            "status": "active", "reason": "Duplicate review must fail",
        })
    assert activated.status_code == 200
    assert activated.json()["operation"] == "source.review.active"
    assert duplicate.status_code == 422
    assert repository.list_source_profiles()[0]["status"] == "active"
    assert repository.source_promotion_facts[-1]["transitionKind"] == "manual_review"
