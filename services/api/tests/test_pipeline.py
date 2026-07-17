from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
import json
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from radar import alerts as alert_module
from radar import runner as runner_module
from radar import storage as storage_module
from radar.assessment import event_assessment
from radar.alerts import sign_payload, verify_payload
from radar.clustering import ClusterCandidate, choose_cluster, entities
from radar.connectors import base as connector_base
from radar.connectors.arxiv import ArxivConnector
from radar.connectors.base import BaseConnector, ConnectorError, ensure_safe_public_url, ensure_safe_public_url_resolved, ensure_safe_public_websocket_url
from radar.connectors.bluesky import BlueskyJetstreamConnector, JETSTREAM_CURSOR_VERSION
from radar.connectors.github import GitHubConnector
from radar.connectors.huggingface import HuggingFaceConnector
from radar.connectors.research import OpenAlexConnector
from radar.connectors.rss import RSSConnector
from radar.connectors.youtube import YouTubeConnector
from radar.contracts import BehaviorApplicabilityRequest, ClusterEditRequest, EventType, FeedbackRequest, LifecycleState, MetricPoint, Observation, StoredScore
from radar.embeddings import BgeM3Provider, cosine_similarity
from radar.evidence_store import LocalEvidenceStore, evidence_key
from radar.deletion import SourceDeletionConsumer
from radar.facts import split_observation
from radar.fixtures import demo_events, seed_repository
from radar.identities import SourceIdentity, SourceIdentityResolver
from radar.normalize import canonical_text, content_fingerprint, normalize_url, sanitize_external_text
from radar.processor import EventProcessor
from radar.retention import RawEvidenceRetentionWorker, load_retention_days
from radar.evidence_store import S3EvidenceStore
from radar.scoring import replay_score_payload
from radar.storage import InMemoryRepository, PostgresRepository, utcnow
from radar.source_discovery import load_source_score_policy, source_score_policy_digest
from radar.worker import CollectorWorker
from radar.budget import budget_guard


def observation(item_id: str = "obs-1") -> Observation:
    return Observation(
        id=item_id, platform="RSS", externalId=item_id, sourceId="official-lab",
        publishedAt=datetime.now(timezone.utc), collectedAt=datetime.now(timezone.utc), language="zh",
        title="Qwen 模型发布", text="开放权重与模型卡", url="https://example.com/post?utm_source=x",
        metrics={}, rawEvidenceRef=f"r2://raw/{item_id}.json", relation="original",
        contentFingerprint=content_fingerprint("Qwen 模型发布", "开放权重与模型卡", "https://example.com/post"), signalFamily="official",
    )


def test_url_normalization_and_fingerprint_remove_tracking_noise() -> None:
    left = "https://www.Example.com/a/?utm_source=x&b=2&a=1#top"
    right = "https://example.com/a?a=1&b=2"
    assert normalize_url(left) == right
    assert content_fingerprint("Title", "Text", left) == content_fingerprint("Title", "Text", right)


def test_untrusted_text_strips_markup_bidi_controls_and_unicode_confusables() -> None:
    raw = "<script>alert(1)</script> ＡＩ\u202eexe\u202c  发布"
    assert sanitize_external_text(raw) == "alert(1) AIexe 发布"
    assert canonical_text(raw) == "alert(1) aiexe 发布"


def test_observation_and_outbox_are_deduplicated_at_same_boundary() -> None:
    repository = InMemoryRepository()
    item = observation()
    assert repository.save_observation_with_outbox(item) is True
    assert repository.save_observation_with_outbox(item) is False
    assert len(repository.observations) == 1
    assert len(repository.outbox) == 1


def test_ingestion_discovers_source_without_counting_duplicate_content_as_valid() -> None:
    repository = InMemoryRepository()
    first = observation("source-first").model_copy(update={
        "account_id": "rss:official-lab", "entity_id": "org:official-lab",
        "content_fingerprint": None,
    })
    duplicate = first.model_copy(update={
        "id": "source-duplicate", "external_id": "source-duplicate",
        "collected_at": first.collected_at + timedelta(minutes=1),
        "account_id": "mirror:official-lab",
        "url": "https://example.com/post?utm_source=mirror",
    })
    distinct = first.model_copy(update={
        "id": "source-distinct", "external_id": "source-distinct",
        "collected_at": first.collected_at + timedelta(minutes=2),
        "content_fingerprint": "distinct-source-content",
    })
    assert repository.save_observation_with_outbox(first)
    assert repository.save_observation_with_outbox(duplicate)
    assert repository.save_observation_with_outbox(distinct)
    profile = repository.list_source_profiles()[0]
    assert profile["status"] == "candidate"
    assert profile["validObservations"] == 2
    assert profile["qualityCalibrated"] is False
    assert profile["accountIds"] == ["mirror:official-lab", "rss:official-lab"]
    assert profile["entityIds"] == ["org:official-lab"]
    assert "duplicate_content_observation" in profile["discoveryReasons"]


def test_governed_source_promotion_is_audited_and_cannot_repeat_same_day(monkeypatch) -> None:
    policy = load_source_score_policy().model_copy(update={"auto_promotion_enabled": True})
    monkeypatch.setattr(storage_module, "load_source_score_policy", lambda: policy)
    repository = InMemoryRepository()
    promoted_at = datetime(2026, 7, 17, 1, tzinfo=timezone.utc)
    for index in range(20):
        repository.register_source_candidate(
            source_id=f"seed-{index}", display_name=f"Seed {index}", platform="RSS", language="en",
            observed_at=promoted_at - timedelta(days=30), reason="manually_reviewed_seed",
        )
        repository.source_profiles[f"seed-{index}"]["status"] = "active"
    repository.register_source_candidate(
        source_id="candidate-a", display_name="Candidate A", platform="RSS", language="en",
        observed_at=promoted_at - timedelta(days=9), reason="early_by_trusted_source",
    )
    profile = repository.source_profiles["candidate-a"]
    profile.update({
        "validObservations": 10, "earlyHits": 7, "confirmedHits": 6,
        "originality": 85.0, "domainFocus": 80.0, "authority": 75.0,
        "marketingMatrixOverlap": 5.0, "qualityCalibrated": True,
    })
    result = repository.promote_source_candidates(promoted_at)
    assert result["promotedSourceIds"] == ["candidate-a"]
    assert repository.source_promotion_facts[0]["policyDigest"] == source_score_policy_digest(policy)

    repository.register_source_candidate(
        source_id="candidate-b", display_name="Candidate B", platform="RSS", language="en",
        observed_at=promoted_at - timedelta(days=9), reason="early_by_trusted_source",
    )
    repository.source_profiles["candidate-b"].update({
        "validObservations": 10, "earlyHits": 7, "confirmedHits": 6,
        "originality": 85.0, "domainFocus": 80.0, "authority": 75.0,
        "marketingMatrixOverlap": 5.0, "qualityCalibrated": True,
    })
    assert repository.promote_source_candidates(promoted_at + timedelta(hours=1))["promotedSourceIds"] == []


def test_postgres_source_validity_claim_is_locked_before_duplicate_probe() -> None:
    source = (Path(__file__).parents[1] / "radar" / "storage.py").read_text(encoding="utf-8")
    start = source.index("source-valid-observation:", source.index("class PostgresRepository"))
    probe = source.index("NOT EXISTS (", start)
    assert "pg_advisory_xact_lock" in source[start - 300:start]
    assert start < probe


def test_new_metric_revision_does_not_break_an_active_processing_lease() -> None:
    repository = InMemoryRepository()
    first = observation("leased").model_copy(update={"metrics": {"downloads": 10}})
    second = first.model_copy(update={
        "collected_at": first.collected_at + timedelta(minutes=15), "metrics": {"downloads": 20},
    })
    repository.save_observation_with_outbox(first)
    assert repository.claim_observation_processing(first.id) == 1
    repository.save_observation_with_outbox(second)
    assert repository.claim_observation_processing(first.id) is None
    repository.complete_observation_processing(first.id, 1)
    assert repository.claim_observation_processing(first.id) == 2


def test_content_fact_and_metric_snapshots_are_separate_append_only_contracts() -> None:
    item = observation()
    item.metrics = {"comments": 12, "score": 30}
    content, snapshots = split_observation(item)
    assert content.content_hash == item.content_fingerprint
    assert content.canonical_url == "https://example.com/post"
    assert {snapshot.metric_name for snapshot in snapshots} == {"comments", "score"}
    repository = InMemoryRepository()
    assert repository.save_observation_with_outbox(item) is True
    assert len(repository.metric_facts) == 2
    assert {entry["kind"] for entry in repository.outbox} == {"observation.created", "metric_snapshots.created"}


def test_rights_policy_excerpt_limit_is_enforced_during_fact_split() -> None:
    item = observation("rights-excerpt").model_copy(update={
        "rights_policy_id": "youtube-api-review-required-v1",
        "text": "x" * 800,
    })
    content, _ = split_observation(item)
    assert len(content.text_excerpt) == 500


def test_rights_policy_rejects_unregistered_and_unverified_metrics() -> None:
    with pytest.raises(ValueError, match="unregistered metrics"):
        split_observation(observation("rights-unregistered").model_copy(update={
            "metrics": {"invented_engagement": 100},
        }))
    with pytest.raises(ValueError, match="before provider verification"):
        split_observation(observation("rights-unverified").model_copy(update={
            "rights_policy_id": "bluesky-appview-jetstream-experimental-v1",
            "provenance_level": "unverified_discovery", "metrics": {"comments": 3},
        }))
    verified = observation("rights-verified").model_copy(update={
        "rights_policy_id": "bluesky-appview-jetstream-experimental-v1",
        "provenance_level": "provider_verified", "metrics": {"comments": 3},
    })
    _, metrics = split_observation(verified)
    assert [item.metric_name for item in metrics] == ["comments"]


def test_provenance_upgrade_is_durable_and_schedules_a_new_revision() -> None:
    repository = InMemoryRepository()
    candidate = observation("provenance-upgrade").model_copy(update={
        "provenance_level": "unverified_discovery", "metrics": {},
    })
    verified = candidate.model_copy(update={
        "provenance_level": "provider_verified", "metrics": {"comments": 8},
        "collected_at": candidate.collected_at + timedelta(minutes=1),
        "raw_evidence_ref": "r2://raw/provenance-upgrade-verified.json",
    })
    assert repository.save_observation_with_outbox(candidate) is True
    assert repository.list_source_profiles()[0]["validObservations"] == 0
    assert repository.save_observation_with_outbox(verified) is True
    assert repository.observations[candidate.id].provenance_level == "provider_verified"
    assert repository.observation_processing[candidate.id]["revision"] == 2
    assert repository.list_source_profiles()[0]["validObservations"] == 1
    assert {row["kind"] for row in repository.outbox} == {"observation.created", "metric_snapshots.created"}
    assert candidate.raw_evidence_ref in repository.raw_evidence_deletions
    ignored_ref = "r2://raw/provenance-upgrade-ignored-unverified.json"
    ignored = verified.model_copy(update={
        "provenance_level": "unverified_discovery", "metrics": {"score": 1_000_000_000},
        "raw_evidence_ref": ignored_ref,
    })
    assert repository.save_observation_with_outbox(ignored) is False
    assert repository.observations[candidate.id].provenance_level == "provider_verified"
    assert repository.raw_evidence_deletions[ignored_ref]["status"] == "pending"
    repository.raw_evidence_deletions[verified.raw_evidence_ref] = {
        "rightsPolicyId": verified.rights_policy_id, "status": "pending", "attempts": 0,
        "nextAttemptAt": datetime.now(timezone.utc), "leaseUntil": None, "lastError": None,
    }
    assert verified.raw_evidence_ref not in repository.expire_raw_evidence({}, datetime.now(timezone.utc))


def test_unverified_metrics_are_rejected_and_storage_drops_model_copy_bypass() -> None:
    with pytest.raises(ValueError, match="cannot carry metric"):
        observation("untrusted-metrics").model_validate({
            **observation("untrusted-metrics").model_dump(mode="python", by_alias=True),
            "provenanceLevel": "unverified_discovery",
            "metrics": {"score": 1_000_000_000},
        })
    bypassed = observation("untrusted-metrics-bypass").model_copy(update={
        "provenance_level": "unverified_discovery", "metrics": {"score": 1_000_000_000},
    })
    repository = InMemoryRepository()
    assert repository.save_observation_with_outbox(bypassed) is True
    assert repository.metric_facts == {}


def test_unverified_evidence_cannot_raise_feature_weight_or_cluster_confidence() -> None:
    repository = InMemoryRepository()
    seed_repository(repository)
    baseline = repository.list_events()[0]
    event = baseline.model_copy(update={
        "signal_families": [], "diversity": 0, "evidence_count": 0,
        "discussion_evidence_state": "missing", "behavior_evidence_state": "missing",
        "evidence": [baseline.evidence[0].model_copy(update={
            "kind": "official", "provenance_level": "unverified_discovery",
        })],
    })
    assessment = event_assessment(event)
    assert assessment.evidence_mask["official"] == "missing"
    assert assessment.cluster_confidence == 0


def test_metric_history_is_reconstructed_for_delta_scoring_instead_of_latest_only() -> None:
    repository = InMemoryRepository()
    first = observation("metric-history")
    first.metrics = {"downloads": 100}
    second = first.model_copy(deep=True)
    second.collected_at = first.collected_at + timedelta(minutes=15)
    second.metrics = {"downloads": 145}
    assert repository.save_observation_with_outbox(first) is True
    assert repository.save_observation_with_outbox(second) is True
    repository.assign_observation("evt-history", first.id, .9, "cluster-test")
    history = repository.list_event_observations("evt-history")
    assert [item.metrics["downloads"] for item in history] == [100, 145]
    assert history[1].collected_at - history[0].collected_at == timedelta(minutes=15)


def test_each_metric_snapshot_traces_its_own_raw_archive_and_purge_cleans_all_revisions() -> None:
    repository = InMemoryRepository()
    first = observation("metric-raw").model_copy(update={"metrics": {"downloads": 10}, "raw_evidence_ref": "r2://raw/metric-v1.json"})
    second = first.model_copy(update={
        "collected_at": first.collected_at + timedelta(minutes=15),
        "metrics": {"downloads": 25}, "raw_evidence_ref": "r2://raw/metric-v2.json",
    })
    repository.save_observation_with_outbox(first)
    repository.save_observation_with_outbox(second)
    assert {item.source_revision for item in repository.metric_facts.values()} == {
        "r2://raw/metric-v1.json", "r2://raw/metric-v2.json",
    }
    repository.purge_source("official-lab")
    assert repository.list_source_profiles() == []
    assert repository.outbox[-1]["raw_evidence_refs"] == ["r2://raw/metric-v1.json", "r2://raw/metric-v2.json"]


def test_source_purge_removes_metric_facts_resets_strong_state_and_preserves_shared_raw_objects() -> None:
    repository = InMemoryRepository()
    shared = "r2://raw/shared/batch.json"
    first = observation("delete-a").model_copy(update={"source_id": "source-a", "raw_evidence_ref": shared, "metrics": {"comments": 10}})
    second = observation("delete-b").model_copy(update={"source_id": "source-b", "raw_evidence_ref": shared, "metrics": {"comments": 20}})
    for item in (first, second):
        repository.save_observation_with_outbox(item)
    event = seed_repository(InMemoryRepository()).get_event("evt-open-model")
    assert event is not None
    event = event.model_copy(update={"evidence": [event.evidence[0].model_copy(update={"source": "source-a"})] + event.evidence[1:]})
    repository.upsert_event(event)
    assert repository.purge_source("source-a") == 1
    assert all(fact.subject_id != "delete-a" for fact in repository.metric_facts.values())
    assert repository.outbox[-1]["raw_evidence_refs"] == []
    invalidated = repository.get_event("evt-open-model")
    assert invalidated is not None
    assert invalidated.state == "insufficient_data"
    assert invalidated.labels == []
    assert invalidated.attention == 0
    assert repository.purge_source("source-b") == 1
    assert repository.outbox[-1]["raw_evidence_refs"] == [shared]


def test_source_purge_invalidates_event_even_when_source_is_hidden_from_capped_evidence() -> None:
    repository = seed_repository(InMemoryRepository())
    hidden = observation("hidden-member").model_copy(update={"source_id": "source-hidden"})
    repository.save_observation_with_outbox(hidden)
    repository.assign_observation("evt-open-model", hidden.id, .9, "test")
    before = repository.get_event("evt-open-model")
    assert before is not None and before.state == "accelerating"
    assert all(item.source != "source-hidden" for item in before.evidence)
    repository.purge_source("source-hidden")
    after = repository.get_event("evt-open-model")
    assert after is not None
    assert after.state == "insufficient_data"
    assert after.attention == 0


@pytest.mark.asyncio
async def test_source_deletion_consumer_physically_deletes_raw_object_and_invalidates_cache(tmp_path) -> None:
    store = LocalEvidenceStore(tmp_path)
    reference = "r2://raw/delete/me.json"
    await store.put(reference, b"evidence", "application/json")

    class CacheSpy:
        tags: list[str] = []

        async def invalidate(self, tags: list[str]) -> None:
            self.tags.extend(tags)

    cache = CacheSpy()
    await SourceDeletionConsumer(store, cache).handle({"rawEvidenceRefs": [reference], "cacheTags": ["radar", "source:a"]})
    assert not (tmp_path / evidence_key(reference)).exists()
    assert cache.tags == ["radar", "source:a"]


def test_reviewed_identity_registry_resolves_cross_platform_accounts_to_one_owner() -> None:
    resolver = SourceIdentityResolver([
        SourceIdentity("lab-rss", "rss:lab", "org:lab"),
        SourceIdentity("github:lab", "github:lab", "org:lab"),
    ])
    rss = resolver.resolve(observation("identity-rss").model_copy(update={"source_id": "lab-rss"}))
    github = resolver.resolve(observation("identity-github").model_copy(update={"source_id": "github:lab", "platform": "GitHub"}))
    unknown = resolver.resolve(observation("identity-unknown").model_copy(update={"source_id": "alice", "platform": "HN"}))
    assert rss.entity_id == github.entity_id == "org:lab"
    assert unknown.entity_id == "account-entity:hn:alice"


def test_score_run_persists_versions_input_window_digest_and_drivers() -> None:
    repository = InMemoryRepository()
    now = datetime.now(timezone.utc)
    score = StoredScore(
        event_id="evt-1", score_version="score-0.3.0", threshold_version="thresholds-2026-07",
        input_from=now - timedelta(minutes=15), input_to=now, input_digest="sha256:abc",
        drivers=["讨论与行为同涨"], payload={"state": "accelerating"}, created_at=now,
    )
    repository.save_score(score)
    assert repository.score_runs[0].input_digest == "sha256:abc"
    assert repository.outbox[-1]["kind"] == "score.created"


def test_same_cycle_score_revisions_are_append_only_and_digest_idempotent() -> None:
    repository = InMemoryRepository()
    now = datetime(2026, 7, 17, 0, 7, tzinfo=timezone.utc)
    first = StoredScore(
        event_id="evt-revision", score_version="score-test", threshold_version="threshold-test",
        input_from=now - timedelta(minutes=5), input_to=now, input_digest="sha256:first",
        drivers=["first"], payload={"state": "detected"}, created_at=now,
    )
    second = first.model_copy(update={"input_digest": "sha256:second", "drivers": ["late revision"]})
    repository.save_score(first)
    repository.save_score(second)
    repository.save_score(second)
    assert [(row.input_digest, row.scoring_revision) for row in repository.score_runs] == [
        ("sha256:first", 1), ("sha256:second", 2),
    ]
    assert sum(row["kind"] == "score.created" for row in repository.outbox) == 2


@pytest.mark.asyncio
async def test_processed_score_can_be_replayed_and_persistent_baseline_survives_restart() -> None:
    repository = InMemoryRepository()
    processor = EventProcessor(repository)
    first = observation("replay-event").model_copy(update={
        "external_id": "replay-event", "source_id": "replay-source",
        "published_at": datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc),
        "collected_at": datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc),
        "metrics": {"comments": 10}, "signal_family": "discussion",
    })
    second = first.model_copy(update={
        "collected_at": first.collected_at + timedelta(minutes=15), "metrics": {"comments": 40},
        "raw_evidence_ref": "r2://raw/replay-event-second",
    })
    assert repository.save_observation_with_outbox(first)
    await processor.process(first)
    assert repository.save_observation_with_outbox(second)
    await processor.process(second)
    stored = repository.score_runs[-1]
    replayed = replay_score_payload(stored.payload)
    expected = stored.payload["_replay"]["expected"]
    assert replayed.state.value == expected["state"]
    assert [label.value for label in replayed.labels] == expected["labels"]
    assert replayed.evidence_strength == expected["evidenceStrength"]
    assert stored.feature_registry_version != "unknown"
    assert stored.baseline_digest.startswith("sha256:")
    assert stored.input_observation_ids == ["replay-event"]
    history, digest, count = repository.load_baseline_history(EventType(stored.payload["eventType"]))
    assert count == 1 and history["rss:comments"] == [30.0]
    restarted = EventProcessor(repository)
    restarted_history, restarted_digest, restarted_count = repository.load_baseline_history(EventType(stored.payload["eventType"]))
    assert (restarted_history, restarted_digest, restarted_count) == (history, digest, count)
    assert restarted.repository is repository


def test_postgres_feedback_and_applicability_paths_bind_their_own_request_fields() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.statements: list[tuple[str, object]] = []
            self._row: tuple[object, ...] | None = None
            self.rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql: str, params: object = None) -> None:
            self.statements.append((sql, params))
            if "SELECT current_score" in sql:
                self._row = ({"coverage": 50, "behaviorEvidenceState": "missing"},)
            elif "SELECT clock_timestamp()" in sql:
                self._row = (datetime.now(timezone.utc),)
            elif "SELECT eligibility_key" in sql:
                self._row = ("queue-key-test",)
            else:
                self._row = None

        def fetchone(self):
            return self._row

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self.value = cursor
            self.committed = False

        def cursor(self):
            return self.value

        def commit(self) -> None:
            self.committed = True

    class Repository(PostgresRepository):
        def __init__(self) -> None:
            super().__init__("unused")
            self.cursor_spy = Cursor()
            self.connection_spy = Connection(self.cursor_spy)

        @contextmanager
        def connection(self):
            yield self.connection_spy

    repository = Repository()
    feedback = FeedbackRequest(
        eventId="evt-1", action="confirm", reason="three independent sources",
        queueEligibilityKey="queue-key-test",
    )
    repository.add_feedback(feedback, "workspace-a", "analyst-a")
    applicability = BehaviorApplicabilityRequest(state="not_applicable", reason="behavior has no defined denominator")
    repository.set_behavior_applicability("evt-1", applicability, "workspace-a", "owner-a")
    assert repository.connection_spy.committed is True
    assert any("feedback.created" in sql for sql, _ in repository.cursor_spy.statements)
    assert any("event.rescore.requested" in sql for sql, _ in repository.cursor_spy.statements)


def test_cluster_assignment_uses_shared_url_entity_and_time() -> None:
    item = observation()
    candidate = ClusterCandidate(
        event_id="evt-qwen", title="Qwen 模型发布并开放权重", urls={normalize_url(item.url)},
        entities=entities("Qwen 模型发布"), latest_at=item.published_at,
    )
    decision = choose_cluster(item, [candidate])
    assert decision.create_new is False
    assert decision.event_id == "evt-qwen"
    assert "共享规范化 URL" in decision.reasons


class FakeJetstreamSocket:
    def __init__(self, messages: list[str]) -> None:
        self.messages = list(messages)

    async def recv(self) -> str:
        if not self.messages:
            raise TimeoutError
        return self.messages.pop(0)


class FakeJetstreamContext:
    def __init__(self, messages: list[str]) -> None:
        self.socket = FakeJetstreamSocket(messages)

    async def __aenter__(self) -> FakeJetstreamSocket:
        return self.socket

    async def __aexit__(self, *_args: object) -> None:
        return None


def jetstream_event(*, text: str, time_us: int, cid: str = "bafyreiaitestcid", rkey: str = "post-one") -> dict[str, object]:
    return {
        "did": "did:plc:testauthor", "time_us": time_us, "kind": "commit",
        "commit": {
            "operation": "create", "collection": "app.bsky.feed.post", "rkey": rkey,
            "cid": cid, "record": {"text": text, "createdAt": "2026-07-17T00:00:00Z"},
        },
    }


@pytest.mark.asyncio
async def test_bluesky_jetstream_hydrates_exact_appview_identity_and_persists_cursor(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    messages = [
        json.dumps(jetstream_event(text="ordinary gardening note", time_us=event_time - 1)),
        json.dumps(jetstream_event(text="New AI model release", time_us=event_time)),
    ]
    captured: dict[str, object] = {}
    verified_text = "AI model release " + ("x" * 700)

    def connect_factory(url: str, **kwargs: object) -> FakeJetstreamContext:
        captured.update({"url": url, "kwargs": kwargs})
        return FakeJetstreamContext(messages)

    uri = "at://did:plc:testauthor/app.bsky.feed.post/post-one"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params.get_list("uris") == [uri]
        return httpx.Response(200, json={"posts": [{
            "uri": uri, "cid": "bafyreiaitestcid", "author": {"did": "did:plc:testauthor"},
            "record": {"text": verified_text, "createdAt": "2026-07-17T00:00:00Z"},
            "likeCount": 12, "repostCount": 3, "quoteCount": 2, "replyCount": 4,
        }]})

    store = LocalEvidenceStore(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=store, connect_factory=connect_factory,
            max_messages=2, idle_timeout_seconds=.1, max_attempts=1,
        )
        connector.restore_checkpoint({
            "cursorVersion": JETSTREAM_CURSOR_VERSION,
            "cursorTimeUs": event_time - 10_000_000,
        })
        items = await connector.collect()
        checkpoint = connector.next_checkpoint(items)
    assert len(items) == 1
    assert items[0].provenance_level == "unverified_discovery"
    assert items[0].metrics == {}
    assert len(items[0].text) == 500
    assert checkpoint["cursorTimeUs"] == event_time
    assert "wantedCollections=app.bsky.feed.post" in str(captured["url"])
    assert f"cursor={event_time - 15_000_000}" in str(captured["url"])
    archived = json.loads((tmp_path / evidence_key(items[0].raw_evidence_ref)).read_text(encoding="utf-8"))
    assert archived["provenanceLevel"] == "unverified_discovery"
    assert archived["identityVerified"] is True
    assert archived["appView"]["cid"] == "bafyreiaitestcid"
    assert len(archived["appView"]["record"]["text"]) > 500


@pytest.mark.asyncio
async def test_unverified_bluesky_discovery_cannot_score_or_emit_score_outbox(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    messages = [json.dumps(jetstream_event(text="AI agent launch", time_us=event_time))]

    def connect_factory(*_args: object, **_kwargs: object) -> FakeJetstreamContext:
        return FakeJetstreamContext(messages)

    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"posts": [{
        "uri": "at://did:plc:testauthor/app.bsky.feed.post/post-one",
        "cid": "wrong-cid", "author": {"did": "did:plc:testauthor"}, "record": {"text": "AI agent launch"},
    }]}))
    async with httpx.AsyncClient(transport=transport) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=LocalEvidenceStore(tmp_path),
            connect_factory=connect_factory, max_messages=1, idle_timeout_seconds=.1, max_attempts=1,
        )
        items = await connector.collect()
    assert len(items) == 1 and items[0].provenance_level == "unverified_discovery"
    assert items[0].metrics == {}
    assert "/bluesky/discovery/" in items[0].raw_evidence_ref
    repository = InMemoryRepository()
    repository.save_observation_with_outbox(items[0])
    event = await EventProcessor(repository).process(items[0])
    assert event.attention == event.behavior == event.evidence_score == 0
    assert event.independent_sources == event.evidence_count == 0
    assert event.discussion_evidence_state == "untrusted"
    assert event.evidence[0].provenance_level == "unverified_discovery"
    assessment = event_assessment(event)
    assert assessment.attention_estimate is None
    assert assessment.evidence_mask["discussion"] == "untrusted"
    assert assessment.cluster_confidence == 0
    assert any(gap.feature == "discussion" and gap.state == "untrusted" for gap in assessment.missing_evidence)
    assert repository.score_runs == []
    assert not any(row["kind"] == "score.created" for row in repository.outbox)
    verified = items[0].model_copy(update={
        "provenance_level": "provider_verified", "metrics": {"comments": 6},
        "collected_at": items[0].collected_at + timedelta(minutes=15),
        "raw_evidence_ref": "r2://raw/bluesky/verified-upgrade.json",
    })
    assert repository.save_observation_with_outbox(verified) is True
    upgraded = await EventProcessor(repository).process(verified)
    assert upgraded.discussion_evidence_state == "observed"
    assert upgraded.evidence_count == upgraded.independent_sources == 1
    assert upgraded.evidence[0].provenance_level == "provider_verified"
    assert len(repository.score_runs) == 1


@pytest.mark.asyncio
async def test_verified_revision_resets_discovery_only_type_and_first_seen() -> None:
    repository = InMemoryRepository()
    now = datetime.now(timezone.utc)
    candidate = observation("discovery-scoring-state").model_copy(update={
        "provenance_level": "unverified_discovery", "metrics": {},
        "published_at": now - timedelta(days=30), "collected_at": now - timedelta(days=30),
        "title": "AI model weights", "text": "AI model weights",
    })
    assert repository.save_observation_with_outbox(candidate) is True
    await EventProcessor(repository).process(candidate)
    verified = candidate.model_copy(update={
        "provenance_level": "provider_verified", "published_at": now,
        "collected_at": now, "title": "CVE security advisory",
        "text": "CVE security advisory for an AI service",
        "content_fingerprint": "verified-security-fingerprint",
        "raw_evidence_ref": "r2://raw/discovery-scoring-state-verified.json",
    })
    assert repository.save_observation_with_outbox(verified) is True
    upgraded = await EventProcessor(repository).process(verified)
    assert upgraded.event_type == "security_incident"
    assert upgraded.first_seen == now


@pytest.mark.asyncio
async def test_bluesky_scope_is_rechecked_against_verified_appview_text(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    uri = "at://did:plc:testauthor/app.bsky.feed.post/post-one"
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"posts": [{
        "uri": uri, "cid": "bafyreiaitestcid", "author": {"did": "did:plc:testauthor"},
        "record": {"text": "ordinary gardening notes"}, "likeCount": 10_000,
    }]}))
    async with httpx.AsyncClient(transport=transport) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=LocalEvidenceStore(tmp_path),
            connect_factory=lambda *_args, **_kwargs: FakeJetstreamContext([
                json.dumps(jetstream_event(text="AI launch", time_us=event_time)),
            ]), max_messages=1, idle_timeout_seconds=.1, max_attempts=1,
        )
        items = await connector.collect()
    assert len(items) == 1
    assert items[0].provenance_level == "unverified_discovery"
    assert items[0].metrics == {}


@pytest.mark.asyncio
async def test_bluesky_appview_failure_degrades_run_without_advancing_checkpoint(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    transport = httpx.MockTransport(lambda _: httpx.Response(503, text="temporarily unavailable"))
    repository = InMemoryRepository()
    async with httpx.AsyncClient(transport=transport) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=LocalEvidenceStore(tmp_path),
            connect_factory=lambda *_args, **_kwargs: FakeJetstreamContext([
                json.dumps(jetstream_event(text="AI launch", time_us=event_time)),
            ]), max_messages=1, idle_timeout_seconds=.1, max_attempts=1,
        )
        result = await CollectorWorker(repository, [connector], allow_unapproved_rights_for_nonproduction=True).run_connector(connector)
    assert result.failed is True
    assert repository.get_connector_checkpoint(connector.id) == {}
    assert repository.observations == {}
    assert repository.get_connector(connector.id).status == "degraded"


@pytest.mark.asyncio
async def test_bluesky_checkpoint_stops_at_appview_batch_boundary(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    messages = [
        json.dumps(jetstream_event(
            text=f"AI launch {index}", time_us=event_time + index,
            cid=f"cid-{index}", rkey=f"post-{index}",
        ))
        for index in range(30)
    ]

    def connect_factory(*_args: object, **_kwargs: object) -> FakeJetstreamContext:
        return FakeJetstreamContext(messages)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"posts": [
            {
                "uri": uri, "cid": f"cid-{uri.rsplit('-', 1)[1]}",
                "author": {"did": "did:plc:testauthor"},
                "record": {"text": f"AI launch {uri.rsplit('-', 1)[1]}"},
            }
            for uri in request.url.params.get_list("uris")
        ]})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=LocalEvidenceStore(tmp_path),
            connect_factory=connect_factory, max_messages=30, idle_timeout_seconds=.1, max_attempts=1,
        )
        first = await connector.collect()
        checkpoint = connector.next_checkpoint(first)
        assert len(first) == 25
        assert checkpoint["cursorTimeUs"] == event_time + 24
        connector.restore_checkpoint(checkpoint)
        second = await connector.collect()
    assert len(second) == 5
    assert connector.next_checkpoint(second)["cursorTimeUs"] == event_time + 29


@pytest.mark.asyncio
async def test_bluesky_same_uri_different_cids_are_verified_per_revision(tmp_path) -> None:
    event_time = int((datetime.now(timezone.utc) - timedelta(minutes=1)).timestamp() * 1_000_000)
    uri = "at://did:plc:testauthor/app.bsky.feed.post/post-one"
    messages = [
        json.dumps(jetstream_event(text="AI old", time_us=event_time, cid="cid-old")),
        json.dumps(jetstream_event(text="AI current", time_us=event_time + 1, cid="cid-current")),
    ]
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json={"posts": [{
        "uri": uri, "cid": "cid-current", "author": {"did": "did:plc:testauthor"},
        "record": {"text": "AI current"},
    }]}))
    async with httpx.AsyncClient(transport=transport) as client:
        connector = BlueskyJetstreamConnector(
            client=client, evidence_store=LocalEvidenceStore(tmp_path),
            connect_factory=lambda *_args, **_kwargs: FakeJetstreamContext(messages),
            max_messages=2, idle_timeout_seconds=.1, max_attempts=1,
        )
        items = await connector.collect()
    assert [item.provenance_level for item in items] == ["unverified_discovery", "unverified_discovery"]
    assert items[1].text == "AI current"


def test_bluesky_configuration_and_checkpoint_fail_closed() -> None:
    with pytest.raises(ConnectorError):
        ensure_safe_public_websocket_url("ws://127.0.0.1/private")
    with pytest.raises(ConnectorError, match="keywords"):
        BlueskyJetstreamConnector(keywords=(), connect_factory=lambda: FakeJetstreamContext([]))
    with pytest.raises(ConnectorError, match="wss"):
        BlueskyJetstreamConnector(
            endpoint="ws://example.com/subscribe",
            connect_factory=lambda: FakeJetstreamContext([]),
        )
    with pytest.raises(ConnectorError, match="https"):
        BlueskyJetstreamConnector(
            appview_endpoint="http://example.com/xrpc/app.bsky.feed.getPosts",
            connect_factory=lambda: FakeJetstreamContext([]),
        )
    connector = BlueskyJetstreamConnector(connect_factory=lambda: FakeJetstreamContext([]))
    with pytest.raises(ConnectorError, match="checkpoint"):
        connector.restore_checkpoint({"cursorVersion": "unknown", "cursorTimeUs": 1})


@pytest.mark.asyncio
async def test_runner_keeps_bluesky_disabled_by_default(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("BLUESKY_JETSTREAM_ENABLED", raising=False)
    default_connectors = runner_module.build_connectors(LocalEvidenceStore(tmp_path / "default"))
    try:
        assert all(connector.id != "bluesky" for connector in default_connectors)
    finally:
        for connector in default_connectors:
            await connector.close()
    monkeypatch.setenv("BLUESKY_JETSTREAM_ENABLED", "true")
    enabled_connectors = runner_module.build_connectors(LocalEvidenceStore(tmp_path / "enabled"))
    try:
        bluesky = [connector for connector in enabled_connectors if connector.id == "bluesky"]
        assert len(bluesky) == 1
        assert isinstance(bluesky[0], BlueskyJetstreamConnector)
    finally:
        for connector in enabled_connectors:
            await connector.close()


@pytest.mark.asyncio
async def test_default_runner_connectors_cannot_collect_while_registry_rights_are_pending(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
    monkeypatch.delenv("BLUESKY_JETSTREAM_ENABLED", raising=False)
    monkeypatch.setenv("RSS_FEEDS_FILE", str(tmp_path / "missing-feeds.json"))
    connectors = runner_module.build_connectors(LocalEvidenceStore(tmp_path / "raw"))
    repository = InMemoryRepository()
    worker = CollectorWorker(repository, connectors)
    try:
        runs = await worker.run_once()
    finally:
        await worker.close()
    assert runs
    assert all(run.skipped and not run.failed for run in runs)
    assert repository.observations == {}
    assert all(repository.get_connector(run.connector_id).status == "paused" for run in runs)


@pytest.mark.asyncio
async def test_rss_connector_emits_unified_observation_contract() -> None:
    feed = b"""<?xml version='1.0'?><rss><channel><item><guid>42</guid><title>AI model released</title><description>weights available</description><link>https://lab.example/model?utm_campaign=x</link><pubDate>Wed, 16 Jul 2026 08:00:00 GMT</pubDate></item></channel></rss>"""
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=feed, headers={"content-type": "application/rss+xml"}))
    async with httpx.AsyncClient(transport=transport) as client:
        connector = RSSConnector([("lab", "https://feed.example/rss")], client=client, max_attempts=1)
        items = await connector.collect()
    assert len(items) == 1
    assert items[0].signal_family == "official"
    assert items[0].url == "https://lab.example/model"
    assert items[0].raw_evidence_ref.startswith("r2://raw/rss/")


@pytest.mark.asyncio
async def test_one_broken_rss_feed_does_not_discard_healthy_feed_observations() -> None:
    feed = b"<?xml version='1.0'?><rss><channel><item><guid>ok</guid><title>Model</title><link>https://lab.example/model</link></item></channel></rss>"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=feed) if request.url.host == "good.example" else httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        connector = RSSConnector([
            ("good", "https://good.example/rss"), ("bad", "https://bad.example/rss"),
        ], client=client, max_attempts=1)
        items = await connector.collect()
    assert [item.source_id for item in items] == ["good"]


@pytest.mark.asyncio
async def test_one_invalid_rss_item_does_not_discard_other_items_or_feeds() -> None:
    feed = b"""<?xml version='1.0'?><rss><channel>
      <item><guid>ok</guid><title>Good</title><link>https://lab.example/good</link></item>
      <item><guid>bad</guid><title>Bad</title><link>mailto:unsafe@example.com</link></item>
    </channel></rss>"""
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=feed))) as client:
        items = await RSSConnector([("lab", "https://feed.example/rss")], client=client, max_attempts=1).collect()
    assert [item.title for item in items] == ["Good"]


@pytest.mark.asyncio
async def test_rss_archives_raw_evidence_to_the_configured_store(tmp_path) -> None:
    feed = b"<?xml version='1.0'?><rss><channel><item><guid>42</guid><title>Model</title><link>https://lab.example/model</link></item></channel></rss>"
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=feed, headers={"content-type": "application/rss+xml"}))
    store = LocalEvidenceStore(tmp_path)
    async with httpx.AsyncClient(transport=transport) as client:
        items = await RSSConnector([("lab", "https://feed.example/rss")], client=client, max_attempts=1, evidence_store=store).collect()
    archived = tmp_path / evidence_key(items[0].raw_evidence_ref)
    assert archived.read_bytes() == feed


@pytest.mark.asyncio
async def test_arxiv_connector_parses_atom_and_archives_only_the_item(tmp_path) -> None:
    atom = b"""<?xml version='1.0' encoding='UTF-8'?>
    <feed xmlns='http://www.w3.org/2005/Atom'><entry>
      <id>http://arxiv.org/abs/2607.01234v1</id><updated>2026-07-16T08:00:00Z</updated>
      <published>2026-07-16T08:00:00Z</published><title>Reliable AI Radar</title>
      <summary>A benchmark for emerging AI events.</summary><author><name>Alice</name></author>
    </entry></feed>"""
    transport = httpx.MockTransport(lambda _: httpx.Response(200, content=atom, headers={"content-type": "application/atom+xml"}))
    async with httpx.AsyncClient(transport=transport) as client:
        items = await ArxivConnector(client=client, max_attempts=1, evidence_store=LocalEvidenceStore(tmp_path)).collect()
    assert len(items) == 1
    assert items[0].signal_family == "research"
    assert items[0].url == "https://arxiv.org/abs/2607.01234v1"
    archived = (tmp_path / evidence_key(items[0].raw_evidence_ref)).read_bytes()
    assert b"Reliable AI Radar" in archived
    assert b"<ns0:feed" not in archived


@pytest.mark.asyncio
async def test_openalex_connector_tolerates_null_author_identifier_and_future_date(monkeypatch) -> None:
    collected = datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("radar.connectors.research.utcnow", lambda: collected)
    payload = {"results": [{
        "id": "https://openalex.org/W123",
        "title": "AI signal detection",
        "publication_date": "2050-01-01",
        "primary_location": {"landing_page_url": "https://example.org/paper"},
        "authorships": [{"author": {"id": None, "display_name": "Anonymous"}}],
        "topics": [],
        "cited_by_count": 0,
        "counts_by_year": [],
    }]}
    transport = httpx.MockTransport(lambda _: httpx.Response(200, json=payload))
    async with httpx.AsyncClient(transport=transport) as client:
        items = await OpenAlexConnector(client=client, max_attempts=1).collect()
    assert len(items) == 1
    assert items[0].source_id == "openalex:unknown"
    assert items[0].external_id == "W123"
    assert items[0].published_at == collected


@pytest.mark.asyncio
async def test_metric_connectors_keep_stable_content_ids_across_collections(monkeypatch) -> None:
    first_at = datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)
    second_at = first_at + timedelta(minutes=15)

    github_payloads = iter([
        {"items": [{"id": 7, "full_name": "lab/radar", "description": "AI radar", "html_url": "https://github.com/lab/radar", "updated_at": "2026-07-16T08:00:00Z", "owner": {"login": "lab"}, "stargazers_count": 10, "forks_count": 1, "open_issues_count": 0, "subscribers_count": 2}]},
        {"items": [{"id": 7, "full_name": "lab/radar", "description": "AI radar", "html_url": "https://github.com/lab/radar", "updated_at": "2026-07-16T08:00:00Z", "owner": {"login": "lab"}, "stargazers_count": 25, "forks_count": 2, "open_issues_count": 0, "subscribers_count": 3}]},
    ])
    github_times = iter([first_at, second_at])
    monkeypatch.setattr("radar.connectors.github.utcnow", lambda: next(github_times))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=next(github_payloads)))) as client:
        connector = GitHubConnector(client=client, max_attempts=1)
        github_first, github_second = (await connector.collect())[0], (await connector.collect())[0]

    hf_payloads = iter([
        [{"modelId": "lab/model", "createdAt": "2026-07-16T08:00:00Z", "tags": ["text-generation"], "downloads": 100, "likes": 5, "trendingScore": 1}],
        [{"modelId": "lab/model", "createdAt": "2026-07-16T08:00:00Z", "tags": ["text-generation"], "downloads": 150, "likes": 8, "trendingScore": 2}],
    ])
    hf_times = iter([first_at, second_at])
    monkeypatch.setattr("radar.connectors.huggingface.utcnow", lambda: next(hf_times))
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=next(hf_payloads)))) as client:
        connector = HuggingFaceConnector(client=client, max_attempts=1)
        hf_first, hf_second = (await connector.collect())[0], (await connector.collect())[0]

    video_round = 0

    def youtube_handler(request: httpx.Request) -> httpx.Response:
        nonlocal video_round
        if request.url.path.endswith("/search"):
            return httpx.Response(200, json={"items": [{"id": {"videoId": "video-1"}}]})
        video_round += 1
        return httpx.Response(200, json={"items": [{
            "id": "video-1", "snippet": {"channelId": "channel-1", "title": "AI demo", "description": "demo", "publishedAt": "2026-07-16T08:00:00Z"},
            "statistics": {"viewCount": str(100 * video_round), "likeCount": str(10 * video_round), "commentCount": str(video_round)},
        }]})

    youtube_times = iter([first_at, second_at])
    monkeypatch.setattr("radar.connectors.youtube.utcnow", lambda: next(youtube_times))
    async with httpx.AsyncClient(transport=httpx.MockTransport(youtube_handler)) as client:
        connector = YouTubeConnector("key", client=client, max_attempts=1)
        youtube_first, youtube_second = (await connector.collect())[0], (await connector.collect())[0]

    repository = InMemoryRepository()
    for first, second in ((github_first, github_second), (hf_first, hf_second), (youtube_first, youtube_second)):
        assert first.id == second.id
        assert first.raw_evidence_ref != second.raw_evidence_ref
        assert repository.save_observation_with_outbox(first) is True
        assert repository.save_observation_with_outbox(second) is True
    assert len(repository.observations) == 3
    assert len(repository.metric_facts) > 3


@pytest.mark.asyncio
async def test_per_item_archives_allow_selective_physical_source_deletion(tmp_path, monkeypatch) -> None:
    collected = datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("radar.connectors.github.utcnow", lambda: collected)
    payload = {"items": [
        {"id": 1, "full_name": "delete-me/model", "description": "private deletion marker", "html_url": "https://github.com/delete-me/model", "updated_at": "2026-07-16T08:00:00Z", "owner": {"login": "delete-me"}, "stargazers_count": 1, "forks_count": 0, "open_issues_count": 0, "subscribers_count": 0},
        {"id": 2, "full_name": "keep-me/model", "description": "retained marker", "html_url": "https://github.com/keep-me/model", "updated_at": "2026-07-16T08:00:00Z", "owner": {"login": "keep-me"}, "stargazers_count": 2, "forks_count": 0, "open_issues_count": 0, "subscribers_count": 0},
    ]}
    store = LocalEvidenceStore(tmp_path)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=payload))) as client:
        items = await GitHubConnector(client=client, max_attempts=1, evidence_store=store).collect()
    delete_item = next(item for item in items if item.source_id == "github:delete-me")
    keep_item = next(item for item in items if item.source_id == "github:keep-me")
    repository = InMemoryRepository()
    for item in items:
        repository.save_observation_with_outbox(item)
    repository.purge_source("github:delete-me")

    class CacheSpy:
        async def invalidate(self, tags: list[str]) -> None:
            return None

    await SourceDeletionConsumer(store, CacheSpy()).handle(repository.outbox[-1]["payload"])
    assert not (tmp_path / evidence_key(delete_item.raw_evidence_ref)).exists()
    retained = (tmp_path / evidence_key(keep_item.raw_evidence_ref)).read_text(encoding="utf-8")
    assert "retained marker" in retained
    assert "private deletion marker" not in retained


class FailingConnector(BaseConnector):
    id = "broken"
    platform = "Broken Source"
    signal_family = "discussion"

    async def collect(self) -> list[Observation]:
        raise ConnectorError("rate limited")


class StaticConnector(BaseConnector):
    id = "static"
    platform = "Static"
    signal_family = "mixed"

    def __init__(self, items: list[Observation]) -> None:
        super().__init__(max_attempts=1)
        self.items = items

    async def collect(self) -> list[Observation]:
        return self.items


def test_production_runner_configuration_requires_scoped_budgets_and_image_digests(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    identity_path = tmp_path / "identities.json"
    identity_path.write_text(
        json.dumps([{"sourceId": "rss:lab", "accountId": "rss:lab", "entityId": "org:lab"}]),
        encoding="utf-8",
    )
    public_pem = Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    required = {
        "DEMO_MODE": "false", "DATABASE_URL": "postgresql://example",
        "REDIS_URL": "redis://example", "R2_ENDPOINT_URL": "https://r2.example",
        "R2_ACCESS_KEY_ID": "access", "R2_SECRET_ACCESS_KEY": "secret",
        "RADAR_INSTANCE_ID": "stable-instance", "SOURCE_IDENTITIES_FILE": str(identity_path),
        "RADAR_JWT_PUBLIC_KEYS": json.dumps({"key": public_pem}), "RADAR_JWT_ISSUER": "issuer",
        "RADAR_JWT_AUDIENCE": "audience", "AUTH_REQUIRED": "true", "RADAR_AUTH_MODE": "jwt",
        "RAW_EVIDENCE_BUCKET": "raw",
        "CONNECTOR_BUDGETS_RMB_JSON": '{"youtube":500}',
        "SIGNAL_FAMILY_BUDGETS_RMB_JSON": '{"discussion":900}',
        "RADAR_RELEASE_IMAGE_DIGESTS": json.dumps({
            "api": "sha256:" + "a" * 64, "web": "sha256:" + "b" * 64,
        }),
        "RADAR_ACTUAL_IMAGE_DIGESTS": json.dumps({
            "api": "sha256:" + "a" * 64, "web": "sha256:" + "b" * 64,
        }),
    }
    for key, value in required.items():
        monkeypatch.setenv(key, value)
    assert runner_module.validate_runtime_configuration() is True
    monkeypatch.setenv("CONNECTOR_BUDGETS_RMB_JSON", '{"youtube":0}')
    with pytest.raises(RuntimeError, match="positive finite"):
        runner_module.validate_runtime_configuration()
    monkeypatch.setenv("CONNECTOR_BUDGETS_RMB_JSON", '{"youtube":500}')
    monkeypatch.setenv("RADAR_RELEASE_IMAGE_DIGESTS", '{"api":"latest","web":"latest"}')
    with pytest.raises(RuntimeError, match="sha256"):
        runner_module.validate_runtime_configuration()


def test_production_budget_coverage_requires_every_metered_connector_and_family() -> None:
    github = GitHubConnector(max_attempts=1)
    huggingface = HuggingFaceConnector(max_attempts=1)
    try:
        with pytest.raises(RuntimeError, match="github.*huggingface.*behavior"):
            runner_module.validate_budget_coverage([github, huggingface], {}, {})
        with pytest.raises(RuntimeError, match="signalFamilies=behavior"):
            runner_module.validate_budget_coverage(
                [github, huggingface], {"github": 100, "huggingface": 100}, {},
            )
        runner_module.validate_budget_coverage(
            [github, huggingface],
            {"github": 100, "huggingface": 100},
            {"behavior": 200},
        )
    finally:
        asyncio.run(github.close())
        asyncio.run(huggingface.close())


def test_fixed_scheduler_cadence_does_not_add_cycle_duration() -> None:
    target, delay = runner_module.next_scheduled_cycle(1000.0, 900, 1025.0)
    assert target == 1900.0
    assert delay == 875.0
    skipped_target, skipped_delay = runner_module.next_scheduled_cycle(1000.0, 900, 2920.0)
    assert skipped_target == 3700.0
    assert skipped_delay == 780.0


@pytest.mark.asyncio
async def test_runtime_dependency_probe_rechecks_both_dependencies() -> None:
    class RedisProbe:
        async def ping(self) -> bool:
            return True

    class EvidenceProbe:
        def __init__(self, fails: bool = False) -> None:
            self.fails = fails
            self.calls = 0

        async def probe(self, bucket: str) -> None:
            assert bucket == "raw"
            self.calls += 1
            if self.fails:
                raise RuntimeError("unavailable")

    healthy_store = EvidenceProbe()
    assert await runner_module.probe_runtime_dependencies(  # type: ignore[arg-type]
        RedisProbe(), healthy_store, "raw",  # type: ignore[arg-type]
    ) == (True, True, [])
    failing_store = EvidenceProbe(True)
    redis_ok, r2_ok, failures = await runner_module.probe_runtime_dependencies(  # type: ignore[arg-type]
        RedisProbe(), failing_store, "raw",  # type: ignore[arg-type]
    )
    assert redis_ok is True and r2_ok is False
    assert failures == ["r2:RuntimeError"]
    assert healthy_store.calls == failing_store.calls == 1


@pytest.mark.asyncio
async def test_failed_processing_is_retried_even_when_content_has_no_new_metric_snapshot() -> None:
    repository = InMemoryRepository()
    real = EventProcessor(repository)

    class FlakyProcessor:
        calls = 0

        async def process(self, item: Observation):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("transient clustering failure")
            return await real.process(item)

    processor = FlakyProcessor()
    worker = CollectorWorker(repository, [StaticConnector([observation("retry-once")])], processor, allow_unapproved_rights_for_nonproduction=True)  # type: ignore[arg-type]
    first = await worker.run_once()
    second = await worker.run_once()
    await worker.close()
    assert first[0].failed is True
    assert second[0].failed is False
    assert second[0].duplicates == 1
    assert processor.calls == 2
    assert len(repository.events) == 1
    assert len(repository.score_runs) == 1
    assert repository.observation_processing["retry-once"]["processedRevision"] == 1


@pytest.mark.asyncio
async def test_pending_observation_is_not_retried_after_its_rights_policy_is_blocked() -> None:
    repository = InMemoryRepository()
    item = observation("rights-revoked-before-retry").model_copy(update={
        "rights_policy_id": "youtube-api-review-required-v1",
    })
    assert repository.save_observation_with_outbox(item)

    class MustNotRunProcessor:
        async def process(self, _item: Observation):
            raise AssertionError("blocked stored evidence must not enter processing")

    worker = CollectorWorker(repository, [], MustNotRunProcessor())  # type: ignore[arg-type]
    assert await worker.retry_pending() == 0
    state = repository.observation_processing[item.id]
    assert state["attempts"] == 0
    assert state["processedRevision"] == 0


@pytest.mark.asyncio
async def test_lost_processing_ack_does_not_append_duplicate_score_or_timeline_point() -> None:
    repository = InMemoryRepository()
    item = observation("ack-lost")
    repository.save_observation_with_outbox(item)
    processor = EventProcessor(repository)
    first = await processor.process(item)
    second = await processor.process(item)
    assert second.id == first.id
    assert len(repository.score_runs) == 1
    assert len(second.timeline) == len(first.timeline)
    assert sum(entry["kind"] == "score.created" for entry in repository.outbox) == 1


@pytest.mark.asyncio
async def test_connector_failure_degrades_coverage_instead_of_emitting_zero_heat() -> None:
    repository = InMemoryRepository()
    worker = CollectorWorker(repository, [FailingConnector(max_attempts=1)], allow_unapproved_rights_for_nonproduction=True)
    result = await worker.run_once()
    await worker.close()
    assert result[0].failed is True
    assert repository.connectors["broken"].status == "degraded"
    assert "冻结上一指标" in repository.connectors["broken"].note


@pytest.mark.asyncio
async def test_runtime_budget_guard_pauses_metered_connector_without_collecting() -> None:
    connector = StaticConnector([observation("must-not-run")])
    connector.metered = True
    repository = InMemoryRepository()
    worker = CollectorWorker(repository, [connector], budget_decision=budget_guard(2100, 2000), allow_unapproved_rights_for_nonproduction=True)
    result = await worker.run_once()
    assert result[0].skipped is True
    assert repository.observations == {}
    assert repository.connectors["static"].status == "paused"
    assert repository.connectors["static"].coverage <= 65
    first_coverage = repository.connectors["static"].coverage
    await worker.run_once()
    assert repository.connectors["static"].coverage == first_coverage
    await worker.close()


@pytest.mark.asyncio
async def test_connector_24h_observations_are_rolling_not_the_latest_run_count() -> None:
    repository = InMemoryRepository()
    connector = StaticConnector([observation("rolling")])
    worker = CollectorWorker(repository, [connector], allow_unapproved_rights_for_nonproduction=True)
    await worker.run_once()
    first_checkpoint = repository.get_connector_checkpoint("static")
    assert first_checkpoint["latestExternalId"] == "rolling"
    await worker.run_once()
    await worker.close()
    assert connector.checkpoint["latestExternalId"] == "rolling"
    assert repository.connectors["static"].observations_24h == 1
    assert len(repository.connector_runs) == 2


@pytest.mark.asyncio
async def test_failed_connector_does_not_advance_durable_checkpoint() -> None:
    repository = InMemoryRepository()
    repository.save_connector_checkpoint("broken", {"cursor": "keep-me"})
    worker = CollectorWorker(repository, [FailingConnector(max_attempts=1)], allow_unapproved_rights_for_nonproduction=True)
    result = await worker.run_once()
    await worker.close()
    assert result[0].failed is True
    assert repository.get_connector_checkpoint("broken") == {"cursor": "keep-me"}


@pytest.mark.asyncio
async def test_dynamic_cost_ledger_prevents_a_run_whose_worst_case_crosses_budget() -> None:
    repository = InMemoryRepository()
    now = datetime.now(timezone.utc)
    repository.record_connector_run("prior", now, now, "healthy", 0, 0, 90, estimated_cost_rmb=95)
    connector = StaticConnector([observation("over-cap")])
    connector.metered = True
    connector.estimated_cost_per_request_rmb = 10
    connector.expected_requests_per_collect = 1
    worker = CollectorWorker(repository, [connector], monthly_budget_limit=100, allow_unapproved_rights_for_nonproduction=True)
    result = await worker.run_once()
    await worker.close()
    assert result[0].skipped is True
    assert repository.observations == {}
    assert repository.connectors["static"].status == "paused"


@pytest.mark.asyncio
async def test_metered_worker_reserves_budget_before_provider_collection_and_confirms_it() -> None:
    repository = InMemoryRepository()

    class ReservationAwareConnector(StaticConnector):
        async def collect(self) -> list[Observation]:
            assert any(
                row["connectorId"] == self.id and row["status"] == "reserved"
                for row in repository.connector_budget_reservations.values()
            )
            return await super().collect()

    connector = ReservationAwareConnector([observation("atomically-reserved")])
    connector.metered = True
    connector.estimated_cost_per_request_rmb = 1
    worker = CollectorWorker(
        repository,
        [connector],
        monthly_budget_limit=100,
        connector_budget_limits={"static": 100},
        signal_family_budget_limits={"mixed": 100},
        allow_unapproved_rights_for_nonproduction=True,
    )
    runs = await worker.run_once()
    await worker.close()
    assert runs[0].failed is False
    reservations = list(repository.connector_budget_reservations.values())
    assert len(reservations) == 1
    assert reservations[0]["status"] == "confirmed"


@pytest.mark.asyncio
async def test_expired_budget_reservation_enters_visible_reconciliation_without_releasing_credit() -> None:
    repository = InMemoryRepository()
    owner_token = repository.reserve_connector_budget(
        "metered", "behavior", 60, 100, 100, 100, {"metered"}, 0,
    )
    assert owner_token is not None
    repository.connector_budget_reservations[owner_token]["leaseUntil"] = utcnow() - timedelta(seconds=1)
    worker = CollectorWorker(repository, [], monthly_budget_limit=100)
    await worker.run_once()
    assert repository.connector_budget_reconciliation_count() == 1
    assert repository.monthly_connector_spend() == 60
    assert repository.connector_budget_reservations[owner_token]["status"] == "reconciliation_required"


@pytest.mark.asyncio
async def test_connector_budget_gate_pauses_only_the_connector_that_would_overspend() -> None:
    class SecondMeteredConnector(StaticConnector):
        id = "other-metered"
        platform = "Other Metered"
        signal_family = "research"

    repository = InMemoryRepository()
    now = datetime.now(timezone.utc)
    repository.record_connector_run(
        "static", now, now, "healthy", 0, 0, 90, estimated_cost_rmb=45
    )
    limited = StaticConnector([observation("connector-limited")])
    allowed = SecondMeteredConnector([observation("connector-allowed")])
    for connector in (limited, allowed):
        connector.metered = True
        connector.estimated_cost_per_request_rmb = 1
        connector.expected_requests_per_collect = 1
    worker = CollectorWorker(
        repository,
        [limited, allowed],
        monthly_budget_limit=500,
        connector_budget_limits={"static": 50, "other-metered": 100},
        allow_unapproved_rights_for_nonproduction=True,
    )
    runs = await worker.run_once()
    await worker.close()
    assert runs[0].skipped is True
    assert runs[1].skipped is False
    assert "连接器 static" in repository.connectors["static"].note
    assert "connector-allowed" in repository.observations
    assert "connector-limited" not in repository.observations


@pytest.mark.asyncio
async def test_signal_family_budget_gate_uses_the_family_ledger() -> None:
    class SameFamilyConnector(StaticConnector):
        id = "same-family"

    repository = InMemoryRepository()
    now = datetime.now(timezone.utc)
    repository.record_connector_run(
        "same-family", now, now, "healthy", 0, 0, 90, estimated_cost_rmb=55
    )
    connector = SameFamilyConnector([observation("family-limited")])
    connector.metered = True
    connector.estimated_cost_per_request_rmb = 1
    connector.expected_requests_per_collect = 1
    worker = CollectorWorker(
        repository,
        [connector],
        monthly_budget_limit=500,
        signal_family_budget_limits={"mixed": 60},
        allow_unapproved_rights_for_nonproduction=True,
    )
    runs = await worker.run_once()
    await worker.close()
    assert runs[0].skipped is True
    assert "信号族 mixed" in repository.connectors["same-family"].note
    assert repository.observations == {}


def test_budget_scope_configuration_rejects_non_positive_values() -> None:
    with pytest.raises(ValueError, match="positive finite"):
        CollectorWorker(
            InMemoryRepository(), [], connector_budget_limits={"youtube": 0}
        )


@pytest.mark.asyncio
async def test_real_worker_closes_collection_cluster_score_event_and_outbox_loop() -> None:
    now = datetime.now(timezone.utc)
    items = [
        Observation(id="official-1", platform="RSS", externalId="official-1", sourceId="qwen-lab", publishedAt=now, collectedAt=now, language="zh", title="Qwen 模型发布", text="Qwen 开放新模型权重", url="https://lab.example/qwen-release", metrics={}, rawEvidenceRef="r2://raw/official-1", contentFingerprint="fp-official", signalFamily="official", relation="original"),
        Observation(id="discussion-1", platform="HN", externalId="discussion-1", sourceId="hn:alice", publishedAt=now, collectedAt=now, language="en", title="Qwen 模型发布", text="Developers discuss the Qwen model release", url="https://news.example/qwen", metrics={"comments": 40}, rawEvidenceRef="r2://raw/discussion-1", contentFingerprint="fp-discussion", signalFamily="discussion", relation="original"),
        Observation(id="behavior-1", platform="GitHub", externalId="behavior-1", sourceId="github:qwen", publishedAt=now, collectedAt=now, language="en", title="Qwen 模型发布", text="Qwen inference repository", url="https://github.com/qwen/example", metrics={"stars": 900, "forks": 80}, rawEvidenceRef="r2://raw/behavior-1", contentFingerprint="fp-behavior", signalFamily="behavior", relation="original"),
    ]
    repository = InMemoryRepository()
    connector = StaticConnector(items)
    processor = EventProcessor(repository)
    worker = CollectorWorker(repository, [connector], processor, allow_unapproved_rights_for_nonproduction=True)
    runs = await worker.run_once()
    await worker.close()
    assert runs[0].inserted == 3
    assert len(repository.events) == 1
    event = next(iter(repository.events.values()))
    assert event.independent_sources == 3
    assert {item.kind for item in event.evidence} == {"official", "discussion", "behavior"}
    assert len(repository.score_runs) == 3
    assert [item.scoring_revision for item in repository.score_runs] == [1, 2, 3]
    assert sum(item["kind"] == "score.created" for item in repository.outbox) == 3
    assert repository.purge_source("qwen-lab") == 1
    redacted = next(iter(repository.events.values()))
    assert all(item.source != "qwen-lab" for item in redacted.evidence)
    assert redacted.evidence_strength == "low"
    assert repository.outbox[-1]["raw_evidence_refs"] == ["r2://raw/official-1"]
    assert repository.outbox[-1]["affected_event_ids"] == [event.id]
    assert repository.list_pending_observation_ids()
    # Rescoring is driven by the durable processing revision; no connector has
    # to emit another metric snapshot after the deletion.
    retry_worker = CollectorWorker(repository, [], processor, allow_unapproved_rights_for_nonproduction=True)
    assert await retry_worker.retry_pending() >= 1
    rescored = repository.get_event(event.id)
    assert rescored is not None
    # Stars/forks are secondary intent only. After the official source is
    # deleted, discussion plus secondary intent cannot satisfy the minimum
    # evidence combination for a strong state.
    assert rescored.state == "insufficient_data"
    assert all(item.source != "qwen-lab" for item in rescored.evidence)


@pytest.mark.asyncio
@pytest.mark.parametrize("title", [
    "AI startup closes Series C funding round",
    "Government publishes new AI regulation proposal",
])
async def test_unsupported_financing_and_policy_candidates_never_enter_scoring_or_alert_outbox(title: str) -> None:
    now = datetime.now(timezone.utc)
    shared_url = "https://example.com/unsupported-candidate"
    rows = [
        Observation(
            id="unsupported-official", platform="RSS", externalId="unsupported-official",
            sourceId="media", publishedAt=now, collectedAt=now, language="en",
            title=title, text=title, url=shared_url, metrics={},
            rawEvidenceRef="r2://raw/unsupported-official", contentFingerprint="unsupported-official",
            signalFamily="official", relation="original",
        ),
        Observation(
            id="unsupported-discussion", platform="HN", externalId="unsupported-discussion",
            sourceId="commenter", publishedAt=now, collectedAt=now + timedelta(minutes=1), language="en",
            title=title, text=f"Discussion: {title}", url=shared_url, metrics={"comments": 200},
            rawEvidenceRef="r2://raw/unsupported-discussion", contentFingerprint="unsupported-discussion",
            signalFamily="discussion", relation="original",
        ),
        Observation(
            id="unsupported-behavior", platform="GitHub", externalId="unsupported-behavior",
            sourceId="repository", publishedAt=now, collectedAt=now + timedelta(minutes=2), language="en",
            title=title, text=title, url=shared_url, metrics={"stars": 50_000},
            rawEvidenceRef="r2://raw/unsupported-behavior", contentFingerprint="unsupported-behavior",
            signalFamily="behavior", relation="original",
        ),
    ]
    repository = InMemoryRepository()
    processor = EventProcessor(repository)
    for item in rows:
        assert repository.save_observation_with_outbox(item)
        result = await processor.process(item)
        assert result.classification_status == "unsupported"
        assert result.state == "insufficient_data"
    assert len(repository.events) == 1
    assert repository.score_runs == []
    assert not any(item["kind"] == "score.created" for item in repository.outbox)
    event = next(iter(repository.events.values()))
    assert "不进入数值评分或强告警" in event.driver


@pytest.mark.asyncio
async def test_time_driven_refresh_cools_and_dormants_event_without_new_observations() -> None:
    repository = seed_repository(InMemoryRepository())
    signal_at = datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)
    current = repository.get_event("evt-open-model")
    assert current is not None
    current = current.model_copy(update={
        "state": LifecycleState.ESTABLISHED, "first_seen": signal_at - timedelta(hours=12),
        "updated_at": signal_at, "attention": 84, "behavior": 78,
        "coverage": 86, "evidence_score": 90, "evidence_strength": "high",
        "timeline": [
            MetricPoint(at=signal_at - timedelta(minutes=15), attention=82, behavior=76),
            MetricPoint(at=signal_at, attention=84, behavior=78),
        ],
    })
    repository.upsert_event(current)

    def item(
        item_id: str, source: str, platform: str, family: str,
        collected_at: datetime, metrics: dict[str, float],
    ) -> Observation:
        return Observation(
            id=item_id, platform=platform, externalId=item_id, sourceId=source,
            publishedAt=signal_at - timedelta(hours=12), collectedAt=collected_at,
            language="en", title="Open model release", text="Open model release",
            url=f"https://example.com/{item_id}", metrics=metrics,
            rawEvidenceRef=f"r2://raw/{item_id}-{int(collected_at.timestamp())}",
            contentFingerprint=item_id, signalFamily=family, relation="original",
        )

    official = item("quiet-official", "lab", "RSS", "official", signal_at, {})
    assert repository.save_observation_with_outbox(official)
    repository.assign_observation(current.id, official.id, 1, "test")

    discussion_first = item("quiet-discussion", "person", "HN", "discussion", signal_at - timedelta(minutes=15), {"comments": 50})
    discussion_second = item("quiet-discussion", "person", "HN", "discussion", signal_at, {"comments": 150})
    assert repository.save_observation_with_outbox(discussion_first)
    assert repository.save_observation_with_outbox(discussion_second)
    repository.assign_observation(current.id, discussion_second.id, 1, "test")

    for index in range(5):
        first = item(
            f"quiet-behavior-{index}", f"owner-{index}", "Hugging Face", "behavior",
            signal_at - timedelta(minutes=15), {"downloads": 1_000},
        )
        second = item(
            f"quiet-behavior-{index}", f"owner-{index}", "Hugging Face", "behavior",
            signal_at, {"downloads": 20_000},
        )
        assert repository.save_observation_with_outbox(first)
        assert repository.save_observation_with_outbox(second)
        repository.assign_observation(current.id, second.id, 1, "test")

    processor = EventProcessor(repository)
    assert await processor.refresh_time_driven(signal_at + timedelta(hours=6, minutes=15)) == 1
    after_one_decline = repository.get_event(current.id)
    assert after_one_decline is not None
    assert after_one_decline.state == "established"

    assert await processor.refresh_time_driven(signal_at + timedelta(hours=6, minutes=30)) == 1
    cooling = repository.get_event(current.id)
    assert cooling is not None
    assert cooling.state == "cooling"

    assert await processor.refresh_time_driven(signal_at + timedelta(hours=24)) == 1
    dormant = repository.get_event(current.id)
    assert dormant is not None
    assert dormant.state == "dormant"
    assert dormant.attention == 0
    assert dormant.behavior == 0
    assert dormant.timeline[-1].at == signal_at + timedelta(hours=24)


@pytest.mark.asyncio
async def test_new_valid_behavior_evidence_overrides_manual_na_in_same_score_cycle(monkeypatch) -> None:
    from radar import processor as processor_module

    repository = seed_repository(InMemoryRepository())
    current = repository.get_event("evt-benchmark")
    assert current is not None
    repository.upsert_event(current.model_copy(update={"behavior_evidence_state": "not_applicable"}))
    incoming = Observation(
        id="benchmark-behavior", platform="GitHub", externalId="benchmark-behavior", sourceId="research-lab",
        publishedAt=current.updated_at, collectedAt=current.updated_at + timedelta(minutes=15), language="zh",
        title=current.title, text=current.title, url=current.evidence[0].url,
        metrics={"reproductions": 1}, rawEvidenceRef="r2://raw/benchmark-behavior.json",
        contentFingerprint="benchmark-behavior-fingerprint", signalFamily="behavior", relation="original",
    )
    baseline = incoming.model_copy(update={
        "collected_at": current.updated_at,
        "metrics": {"reproductions": 0},
        "raw_evidence_ref": "r2://raw/benchmark-behavior-baseline.json",
    })
    repository.save_observation_with_outbox(baseline)
    repository.save_observation_with_outbox(incoming)
    captured: dict[str, object] = {}
    actual_score_event = processor_module.score_event

    def capture_score_input(data, **kwargs):
        captured["behaviorApplicable"] = data.behavior_applicable
        captured["behaviorObserved"] = data.behavior_observed
        return actual_score_event(data, **kwargs)

    monkeypatch.setattr(processor_module, "score_event", capture_score_input)
    updated = await EventProcessor(repository).process(incoming)
    assert captured == {"behaviorApplicable": True, "behaviorObserved": True}
    assert updated.behavior_evidence_state == "observed"


def test_ssrf_and_webhook_signing_guards() -> None:
    with pytest.raises(ConnectorError):
        ensure_safe_public_url("http://127.0.0.1/admin")
    body_a, signature_a = sign_payload({"event": "x", "score": 80}, "secret", timestamp=1000)
    body_b, signature_b = sign_payload({"score": 80, "event": "x"}, "secret", timestamp=1000)
    assert body_a == body_b
    assert signature_a == signature_b
    assert signature_a.startswith("t=1000,kid=primary,v1=")
    assert verify_payload(body_a, signature_a, {"primary": "secret"}, now=1200) is True
    assert verify_payload(body_a, signature_a, {"primary": "secret"}, now=1301) is False


@pytest.mark.asyncio
async def test_outbound_webhook_has_timestamp_idempotency_key_and_body_limit(monkeypatch) -> None:
    async def allow_test_host(_: str) -> None:
        return None

    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(request.headers)
        return httpx.Response(204)

    monkeypatch.setattr(alert_module, "ensure_safe_public_url_resolved", allow_test_host)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await alert_module.deliver_webhook(
            "https://hooks.example/events", {"eventId": "evt-1"}, "secret", client,
            idempotency_key="workspace:rule:event:1", key_id="rotating-2026-07",
        )
        with pytest.raises(ValueError, match="body exceeds"):
            await alert_module.deliver_webhook("https://hooks.example/events", {"body": "x" * 100}, "secret", client, max_body_bytes=10)
    assert result.delivered is True
    assert captured["x-signal-idempotency-key"] == "workspace:rule:event:1"
    assert captured["x-signal-key-id"] == "rotating-2026-07"
    assert captured["x-signal-timestamp"]


@pytest.mark.asyncio
async def test_connector_revalidates_redirect_destinations_against_ssrf() -> None:
    transport = httpx.MockTransport(lambda _: httpx.Response(302, headers={"location": "http://127.0.0.1/internal"}))
    async with httpx.AsyncClient(transport=transport) as client:
        connector = RSSConnector([("lab", "https://feed.example/rss")], client=client, max_attempts=1)
        with pytest.raises(ConnectorError, match="private network"):
            await connector.collect()


@pytest.mark.asyncio
async def test_dns_resolution_to_loopback_is_blocked(monkeypatch) -> None:
    monkeypatch.setattr(connector_base.socket, "getaddrinfo", lambda *_args, **_kwargs: [(2, 1, 6, "", ("127.0.0.1", 0))])
    with pytest.raises(ConnectorError, match="DNS resolved"):
        await ensure_safe_public_url_resolved("https://hooks.example/webhook")


@pytest.mark.asyncio
async def test_bge_m3_provider_preserves_input_order_and_shape() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/embeddings"
        return httpx.Response(200, json={"data": [
            {"index": 1, "embedding": [0.0, 1.0, 0.0]},
            {"index": 0, "embedding": [1.0, 0.0, 0.0]},
        ]})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = BgeM3Provider("https://embedding.example", dimensions=3)
        vectors = await provider.embed(["中文模型", "English model"], client)
    assert vectors == [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    assert cosine_similarity(vectors[0], vectors[0]) == pytest.approx(1)
    assert cosine_similarity(vectors[0], vectors[1]) == 0


@pytest.mark.asyncio
async def test_event_processor_uses_cached_cross_language_embeddings_for_cluster_candidates() -> None:
    class SameVectorProvider:
        calls = 0
        model = "test-cross-language"
        dimensions = 2
        batch_sizes: list[int] = []

        async def embed(self, texts: list[str]) -> list[list[float]]:
            self.calls += 1
            self.batch_sizes.append(len(texts))
            return [[1.0, 0.0] for _ in texts]

    now = datetime.now(timezone.utc)
    first = Observation(
        id="qwen-cn", platform="RSS", externalId="qwen-cn", sourceId="qwen-lab", publishedAt=now,
        collectedAt=now, language="zh", title="Qwen 新模型正式发布", text="开放全新模型权重",
        url="https://lab.example/releases/qwen", metrics={}, rawEvidenceRef="r2://raw/qwen-cn",
        contentFingerprint="qwen-cn", signalFamily="official", relation="original",
    )
    second = Observation(
        id="qwen-en", platform="HN", externalId="qwen-en", sourceId="hn:alice", publishedAt=now + timedelta(minutes=5),
        collectedAt=now + timedelta(minutes=5), language="en", title="Qwen announces a new model", text="New open weights are available",
        url="https://news.example/items/qwen", metrics={"comments": 10}, rawEvidenceRef="r2://raw/qwen-en",
        contentFingerprint="qwen-en", signalFamily="discussion", relation="original",
    )
    repository = InMemoryRepository()
    provider = SameVectorProvider()
    processor = EventProcessor(repository, provider)  # type: ignore[arg-type]
    for item in (first, second):
        repository.save_observation_with_outbox(item)
        await processor.process(item)
    assert len(repository.events) == 1
    assert provider.calls == 2
    event_id = next(iter(repository.events))
    assert repository.load_event_embeddings(
        {event_id: processor._event_embedding_titles[event_id]},
        "test-cross-language:2", 2,
    )[event_id] == [1.0, 0.0]
    third = second.model_copy(update={
        "id": "qwen-third", "external_id": "qwen-third", "source_id": "hn:bob",
        "collected_at": now + timedelta(minutes=10), "published_at": now + timedelta(minutes=10),
        "raw_evidence_ref": "r2://raw/qwen-third", "content_fingerprint": "qwen-third",
    })
    restarted = EventProcessor(repository, provider)  # type: ignore[arg-type]
    repository.save_observation_with_outbox(third)
    await restarted.process(third)
    assert len(repository.events) == 1
    assert provider.batch_sizes[-1] == 1
    restarted.invalidate_events([event_id])
    assert repository.load_event_embeddings(
        {event_id: processor._event_embedding_titles[event_id]},
        "test-cross-language:2", 2,
    ) == {}


@pytest.mark.asyncio
async def test_event_processor_embedding_cache_is_bounded_to_hnsw_and_priority_shortlists() -> None:
    class FixedProvider:
        model = "bounded-cache"
        dimensions = 2

        async def embed(self, texts: list[str]) -> list[list[float]]:
            return [[1.0, 0.0] for _ in texts]

    repository = InMemoryRepository()
    template = demo_events()[0]
    for index in range(100):
        event = template.model_copy(update={
            "id": f"bounded-event-{index:03d}",
            "title": f"Candidate {index:03d}",
            "title_en": f"Candidate {index:03d}",
            "storage_revision": 0,
        })
        repository.upsert_event(event)
        title = f"{event.title} {event.title_en}"
        repository.save_event_embedding(event.id, "bounded-cache:2", title, [1.0, 0.0])
    now = datetime.now(timezone.utc)
    incoming = Observation(
        id="bounded-incoming", platform="RSS", externalId="bounded-incoming",
        sourceId="bounded-source", publishedAt=now, collectedAt=now, language="en",
        title="Novel bounded cache release", text="Novel bounded cache release",
        url="https://example.com/bounded-incoming", metrics={},
        rawEvidenceRef="r2://raw/bounded-incoming", contentFingerprint="bounded-incoming",
        signalFamily="official", relation="original",
    )
    assert repository.save_observation_with_outbox(incoming)
    processor = EventProcessor(repository, FixedProvider())  # type: ignore[arg-type]
    await processor.process(incoming)
    assert len(processor._event_embeddings) <= 76
    assert len(processor._event_embedding_titles) == len(processor._event_embeddings)


@pytest.mark.asyncio
async def test_same_15_minute_batch_has_one_timeline_cycle_append_only_score_revisions_and_no_self_calibration() -> None:
    now = datetime(2026, 7, 16, 8, 7, tzinfo=timezone.utc)
    shared_url = "https://example.com/model-batch"
    rows = [
        Observation(id="batch-official", platform="RSS", externalId="o", sourceId="lab", publishedAt=now, collectedAt=now, language="en", title="Model batch release", text="official release", url=shared_url, metrics={}, rawEvidenceRef="r2://raw/o", contentFingerprint="o", signalFamily="official", relation="original"),
        Observation(id="batch-discussion", platform="HN", externalId="d", sourceId="alice", publishedAt=now, collectedAt=now, language="en", title="Model batch release", text="discussion", url=shared_url, metrics={"comments": 40}, rawEvidenceRef="r2://raw/d", contentFingerprint="d", signalFamily="discussion", relation="original"),
        Observation(id="batch-behavior-a", platform="Hugging Face", externalId="b1", sourceId="hf", publishedAt=now, collectedAt=now, language="en", title="Model batch release", text="downloads", url=shared_url, metrics={"downloads": 12000}, rawEvidenceRef="r2://raw/b1", contentFingerprint="b1", signalFamily="behavior", relation="original"),
        Observation(id="batch-behavior-b", platform="GitHub", externalId="b2", sourceId="gh", publishedAt=now, collectedAt=now, language="en", title="Model batch release", text="forks", url=shared_url, metrics={"forks": 500}, rawEvidenceRef="r2://raw/b2", contentFingerprint="b2", signalFamily="behavior", relation="original"),
        Observation(id="batch-research", platform="arXiv", externalId="r", sourceId="paper", publishedAt=now, collectedAt=now, language="en", title="Model batch release", text="paper context", url=shared_url, metrics={}, rawEvidenceRef="r2://raw/r", contentFingerprint="r", signalFamily="research", relation="original"),
    ]
    repository = InMemoryRepository()
    processor = EventProcessor(repository)
    behavior_before_research = 0.0
    for index, item in enumerate(rows):
        repository.save_observation_with_outbox(item)
        event = await processor.process(item)
        if index == 3:
            behavior_before_research = event.behavior
    assert len(repository.events) == 1
    assert len(event.timeline) == 1
    assert event.timeline[0].at == datetime(2026, 7, 16, 8, 0, tzinfo=timezone.utc)
    assert event.behavior == behavior_before_research
    assert event.velocity == 0
    assert len(repository.score_runs) == len(rows)
    assert [item.scoring_revision for item in repository.score_runs] == list(range(1, len(rows) + 1))
    assert len({item.input_digest for item in repository.score_runs}) == len(rows)
    assert sum(entry["kind"] == "score.created" for entry in repository.outbox) == len(rows)


@pytest.mark.asyncio
async def test_incompatible_behavior_source_stays_missing_in_event_assessment() -> None:
    now = datetime.now(timezone.utc)
    repository = InMemoryRepository()
    processor = EventProcessor(repository)
    rows = [
        Observation(id="mask-official", platform="RSS", externalId="o", sourceId="lab", publishedAt=now, collectedAt=now, language="en", title="Model release", text="model weights", url="https://example.com/mask", metrics={}, rawEvidenceRef="r2://raw/mo", contentFingerprint="mo", signalFamily="official", relation="original"),
        Observation(id="mask-video", platform="YouTube", externalId="v", sourceId="video", publishedAt=now, collectedAt=now, language="en", title="Model release", text="demo views", url="https://example.com/mask", metrics={"views": 100000}, rawEvidenceRef="r2://raw/mv", contentFingerprint="mv", signalFamily="behavior", relation="original"),
    ]
    for item in rows:
        repository.save_observation_with_outbox(item)
        event = await processor.process(item)
    assert event.behavior_evidence_state == "missing"
    assert event_assessment(event).evidence_mask["behavior"] == "missing"


@pytest.mark.asyncio
async def test_rights_policy_retention_deletes_raw_object_but_keeps_minimum_facts(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    repository = InMemoryRepository()
    item = observation("retention-old").model_copy(update={
        "collected_at": now - timedelta(days=31), "published_at": now - timedelta(days=31),
        "raw_evidence_ref": "r2://raw/retention-old.json", "rights_policy_id": "metadata-and-excerpt",
        "metrics": {"comments": 9},
    })
    repository.save_observation_with_outbox(item)
    store = LocalEvidenceStore(tmp_path)
    await store.put(item.raw_evidence_ref, b"restricted full payload", "application/json")
    worker = RawEvidenceRetentionWorker(repository, store, {"metadata-and-excerpt": 30})
    assert await worker.run_once(now) == 1
    assert not (tmp_path / evidence_key(item.raw_evidence_ref)).exists()
    retained = repository.observations[item.id]
    assert retained.title == item.title
    assert retained.content_fingerprint == item.content_fingerprint
    assert retained.raw_evidence_ref == ""
    assert repository.raw_evidence_deletions[item.raw_evidence_ref]["status"] == "completed"


@pytest.mark.asyncio
async def test_retention_does_not_delete_raw_object_still_referenced_by_unexpired_fact(tmp_path) -> None:
    now = datetime.now(timezone.utc)
    repository = InMemoryRepository()
    shared_ref = "r2://raw/shared-retention.json"
    old = observation("old-shared").model_copy(update={"collected_at": now - timedelta(days=31), "raw_evidence_ref": shared_ref})
    fresh = observation("fresh-shared").model_copy(update={"collected_at": now, "raw_evidence_ref": shared_ref})
    repository.save_observation_with_outbox(old)
    repository.save_observation_with_outbox(fresh)
    store = LocalEvidenceStore(tmp_path)
    await store.put(shared_ref, b"shared", "application/json")
    worker = RawEvidenceRetentionWorker(repository, store, {"metadata-and-excerpt": 30})
    assert await worker.run_once(now) == 0
    assert (tmp_path / evidence_key(shared_ref)).exists()
    assert load_retention_days()["metadata-and-excerpt"] == 30


@pytest.mark.asyncio
async def test_s3_partial_delete_failure_does_not_report_success() -> None:
    class PartialFailureClient:
        def delete_objects(self, **_: object) -> dict[str, object]:
            return {"Deleted": [{"Key": "ok.json"}], "Errors": [{"Key": "failed.json", "Code": "InternalError"}]}

    store = object.__new__(S3EvidenceStore)
    store.client = PartialFailureClient()
    with pytest.raises(RuntimeError, match="failed.json"):
        await store.delete_many(["r2://raw/ok.json", "r2://raw/failed.json"])
    with pytest.raises(ValueError, match="r2://"):
        await store.delete_many(["https://example.com/not-an-object"])


@pytest.mark.asyncio
async def test_poison_raw_object_does_not_block_or_confirm_other_deletions() -> None:
    now = datetime.now(timezone.utc)
    repository = InMemoryRepository()
    for item_id in ("good", "poison"):
        repository.save_observation_with_outbox(observation(item_id).model_copy(update={
            "collected_at": now - timedelta(days=31),
            "raw_evidence_ref": f"r2://raw/{item_id}.json",
            "rights_policy_id": "metadata-and-excerpt",
        }))

    class OnePoisonObject:
        async def put(self, reference: str, body: bytes, content_type: str) -> None:
            return None

        async def delete_many(self, references: list[str]) -> None:
            if references == ["r2://raw/poison.json"]:
                raise RuntimeError("object is temporarily locked")

    worker = RawEvidenceRetentionWorker(repository, OnePoisonObject(), {"metadata-and-excerpt": 30})
    assert await worker.run_once(now) == 1
    assert repository.raw_evidence_deletions["r2://raw/good.json"]["status"] == "completed"
    failed = repository.raw_evidence_deletions["r2://raw/poison.json"]
    assert failed["status"] == "pending"
    assert failed["lastError"] == "object is temporarily locked"
    assert failed["nextAttemptAt"] > now


@pytest.mark.asyncio
async def test_collector_cycle_executes_durable_cluster_commands_before_collection() -> None:
    repository = seed_repository(InMemoryRepository())
    repository.event_observations["evt-open-model"] = {"obs-a": .9}
    repository.event_observations["evt-benchmark"] = {"obs-b": .8}
    receipt = repository.queue_cluster_edit(
        "evt-open-model", "merge",
        ClusterEditRequest(targetEventId="evt-benchmark", reason="same official entity and release"),
        "workspace-local", "analyst-local",
    )
    worker = CollectorWorker(repository, [])
    assert await worker.run_once() == []
    assert repository.cluster_edits[receipt.id]["status"] == "completed"
    assert len(repository.cluster_edits[receipt.id]["resultEventIds"]) == 1
