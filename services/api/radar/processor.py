from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone

from .clustering import ClusterCandidate, choose_cluster, entities
from .contracts import Evidence, EvidenceState, EventType, LifecycleState, MetricPoint, Observation, RadarEvent, StoredScore
from .embeddings import BgeM3Provider
from .metrics import Baseline, SignalSnapshot, aggregate_metrics, to_score_input
from .normalize import canonical_text, normalize_url, sanitize_external_text
from .scoring import SCORE_VERSION, THRESHOLD_VERSION, expected_behavior, score_event, strength_tier
from .storage import InMemoryRepository, PostgresRepository


CLUSTER_VERSION = "cluster-0.2.0"


def _score_bucket(value: datetime) -> datetime:
    return value.replace(minute=(value.minute // 15) * 15, second=0, microsecond=0)


def infer_event_type(observation: Observation) -> EventType:
    text = canonical_text(f"{observation.title or ''} {observation.text}")
    if re.search(r"\b(cve|vulnerability|exploit|security advisory)\b|漏洞|供应链攻击|安全公告", text):
        return EventType.SECURITY_INCIDENT
    if observation.signal_family == "research" or observation.platform.lower() in {"arxiv", "openalex"} or re.search(r"\b(paper|benchmark|evaluation)\b|论文|基准|评测", text):
        return EventType.RESEARCH_OR_BENCHMARK
    if observation.platform.lower() == "hugging face" or re.search(r"\b(model|weights|checkpoint|llm)\b|模型|权重|多模态", text):
        return EventType.MODEL_RELEASE
    if observation.platform.lower() == "github" or re.search(r"\b(sdk|library|framework|developer tool|agent)\b|开发工具|框架|智能体", text):
        return EventType.DEVELOPER_TOOL_RELEASE
    return EventType.OFFICIAL_PRODUCT_RELEASE


def _authority(observation: Observation) -> float:
    return {"official": 92, "research": 82, "behavior": 68, "discussion": 52}.get(observation.signal_family, 45)


def _snapshots(observations: list[Observation]) -> list[SignalSnapshot]:
    grouped: dict[tuple[str, str], list[Observation]] = defaultdict(list)
    for observation in observations:
        if observation.provenance_level == "unverified_discovery":
            continue
        grouped[(observation.platform, observation.external_id)].append(observation)
    rows: list[SignalSnapshot] = []
    for items in grouped.values():
        items.sort(key=lambda item: item.collected_at)
        current = items[-1]
        previous = items[-2].metrics if len(items) > 1 else {}
        rows.append(SignalSnapshot(
            source_id=current.account_id or current.source_id,
            source_group=current.entity_id or current.source_id,
            platform=current.platform,
            signal_family=current.signal_family,
            captured_at=current.published_at,
            metrics=current.metrics,
            previous_metrics=previous,
            authority=_authority(current),
            verifiable=bool(current.raw_evidence_ref),
            content_fingerprint=current.content_fingerprint or "",
        ))
    return rows


def _evidence_items(event_id: str, observations: list[Observation]) -> list[Evidence]:
    evidence_items: list[Evidence] = []
    seen_urls: set[str] = set()
    ordered = sorted(
        observations,
        key=lambda item: (item.provenance_level == "unverified_discovery", item.published_at),
    )
    for item in ordered:
        normalized = normalize_url(item.url)
        if normalized in seen_urls:
            continue
        seen_urls.add(normalized)
        evidence_items.append(Evidence(
            id=f"{event_id}:{item.id}", source=item.source_id, platform=item.platform,
            title=sanitize_external_text(item.title or item.text[:100]), url=item.url,
            publishedAt=item.published_at, kind=item.signal_family,
            excerpt=canonical_text(item.text)[:240], provenanceLevel=item.provenance_level,
        ))
    return evidence_items


def _growth_streak(points: list[MetricPoint], current_attention: float, current_behavior: float) -> int:
    values = [(point.attention, point.behavior) for point in points] + [(current_attention, current_behavior)]
    streak = 0
    for previous, current in zip(reversed(values[:-1]), reversed(values[1:]), strict=True):
        if (current[0] > previous[0] or current[1] > previous[1]) and current[0] >= previous[0] and current[1] >= previous[1]:
            streak += 1
        else:
            break
    return streak


def _gap_streak(event_type: EventType, points: list[MetricPoint], current_attention: float, current_behavior: float) -> int:
    values = [(point.attention, point.behavior) for point in points] + [(current_attention, current_behavior)]
    gaps = [expected_behavior(event_type, attention) - behavior for attention, behavior in values]
    streak = 0
    for previous, current in zip(reversed(gaps[:-1]), reversed(gaps[1:]), strict=True):
        if current > previous:
            streak += 1
        else:
            break
    return streak


def _decline_streak(points: list[MetricPoint], current_attention: float, current_behavior: float) -> int:
    totals = [point.attention + point.behavior for point in points] + [current_attention + current_behavior]
    streak = 0
    for previous, current in zip(reversed(totals[:-1]), reversed(totals[1:]), strict=True):
        if current < previous:
            streak += 1
        else:
            break
    return streak


def _inactive_hours(points: list[MetricPoint], bucket_at: datetime, current_attention: float, current_behavior: float, first_seen: datetime) -> float:
    values = [(point.at, point.attention + point.behavior) for point in points]
    values.append((bucket_at, current_attention + current_behavior))
    last_growth_at = first_seen
    if values and values[0][1] > 0:
        last_growth_at = values[0][0]
    for previous, current in zip(values[:-1], values[1:], strict=True):
        if current[1] > previous[1]:
            last_growth_at = current[0]
    return max(0.0, (bucket_at - last_growth_at).total_seconds() / 3600)


class EventProcessor:
    """Connect collected observations to clustering, metrics, scoring and persistence."""

    def __init__(self, repository: InMemoryRepository | PostgresRepository, embedding_provider: BgeM3Provider | None = None) -> None:
        self.repository = repository
        self.embedding_provider = embedding_provider
        self._event_embeddings: dict[str, list[float]] = {}
        self._event_embedding_titles: dict[str, str] = {}
        self._process_lock = asyncio.Lock()
        self._baseline_history: dict[EventType, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        self._baseline_seen: set[tuple[str, str, datetime, str, float]] = set()
        self._pending_baselines: dict[str, tuple[datetime, EventType, list[SignalSnapshot]]] = {}

    def _record_baseline(self, event_type: EventType, snapshots: list[SignalSnapshot]) -> None:
        history = self._baseline_history[event_type]
        for snapshot in snapshots:
            for metric, value in snapshot.metrics.items():
                key = (snapshot.platform, snapshot.source_id, snapshot.captured_at, metric, value)
                if key in self._baseline_seen:
                    continue
                self._baseline_seen.add(key)
                bucket = history[f"{snapshot.platform.lower()}:{metric}"]
                bucket.append(max(0.0, value - snapshot.previous_metrics.get(metric, 0)))
                del bucket[:-1000]

    def invalidate_events(self, event_ids: list[str]) -> None:
        """Drop embeddings derived from titles affected by deletion or edits."""
        for event_id in event_ids:
            self._event_embeddings.pop(event_id, None)
            self._event_embedding_titles.pop(event_id, None)

    def _candidates(self) -> list[ClusterCandidate]:
        candidates: list[ClusterCandidate] = []
        for event in self.repository.list_events():
            title = f"{event.title} {event.title_en}"
            if self._event_embedding_titles.get(event.id) not in {None, title}:
                # Titles are rebuilt after deletion/merge/split. Never reuse an
                # embedding derived from content that is no longer a member.
                self._event_embeddings.pop(event.id, None)
            candidates.append(ClusterCandidate(
                event_id=event.id, title=title,
                urls={normalize_url(item.url) for item in event.evidence},
                entities=entities(title), latest_at=event.updated_at,
            ))
        return candidates

    def _skeleton(self, observation: Observation, event_id: str) -> RadarEvent:
        title = sanitize_external_text(observation.title or observation.text[:120]) or "未命名 AI 事件"
        evidence = Evidence(
            id=f"{event_id}:{observation.id}", source=observation.source_id, platform=observation.platform,
            title=title, url=observation.url, publishedAt=observation.published_at,
            kind=observation.signal_family, excerpt=canonical_text(observation.text)[:240],
            provenanceLevel=observation.provenance_level,
        )
        trusted = observation.provenance_level != "unverified_discovery"
        return RadarEvent(
            id=event_id, title=title, titleEn=title, eventType=infer_event_type(observation),
            state=LifecycleState.INSUFFICIENT_DATA, labels=[], attention=0, behavior=0,
            diversity=0, authority=_authority(observation) if trusted else 0, coordinationRisk=0, coverage=25 if trusted else 0,
            uncertainty=100, evidenceStrength="low", evidenceScore=0, velocity=0, gapResidual=0,
            firstSeen=observation.published_at, updatedAt=observation.collected_at,
            independentSources=1 if trusted else 0, platforms=[observation.platform],
            signalFamilies=[observation.signal_family] if trusted else [], evidenceCount=1 if trusted else 0,
            discussionEvidenceState=(
                EvidenceState.OBSERVED if trusted and observation.signal_family == "discussion"
                else EvidenceState.UNTRUSTED if observation.signal_family == "discussion"
                else EvidenceState.MISSING
            ),
            driver=(
                "首条观测已入簇，等待独立信号家族确认。" if trusted
                else "发现候选尚未由提供方复核，不参与评分或告警。"
            ),
            coverageNote=("仅有一个信号家族。" if trusted else "仅有未复核发现候选；覆盖度为 0，不输出强结论。"),
            timeline=[], evidence=[evidence],
        )

    async def process(self, observation: Observation) -> RadarEvent:
        # Connector collection is concurrent, but cluster assignment and the
        # resulting score transition must be ordered to avoid duplicate events.
        async with self._process_lock:
            return await self._process_one(observation)

    async def _process_one(self, observation: Observation) -> RadarEvent:
        candidates = self._candidates()
        observation_embedding: list[float] | None = None
        if self.embedding_provider:
            missing = [candidate for candidate in candidates if candidate.event_id not in self._event_embeddings]
            texts = [f"{observation.title or ''}\n{observation.text}"] + [candidate.title for candidate in missing]
            try:
                vectors = await self.embedding_provider.embed(texts)
                observation_embedding = vectors[0]
                for candidate, vector in zip(missing, vectors[1:], strict=True):
                    self._event_embeddings[candidate.event_id] = vector
                    self._event_embedding_titles[candidate.event_id] = candidate.title
                for candidate in candidates:
                    candidate.embedding = self._event_embeddings.get(candidate.event_id)
            except Exception:
                # Semantic inference is an optional enrichment. Hard identifiers,
                # entities and time remain available when the model is down.
                observation_embedding = None
        decision = choose_cluster(observation, candidates, observation_embedding=observation_embedding)
        if decision.create_new:
            created_new = True
            fingerprint = observation.content_fingerprint or hashlib.sha256(observation.id.encode()).hexdigest()
            event_id = f"evt-{fingerprint[:16]}"
            current = self._skeleton(observation, event_id)
            self.repository.upsert_event(current)
            cluster_score = 1.0
            if observation_embedding:
                self._event_embeddings[event_id] = observation_embedding
                self._event_embedding_titles[event_id] = f"{current.title} {current.title_en}"
        else:
            created_new = False
            event_id = decision.event_id or ""
            current = self.repository.get_event(event_id)
            if current is None:
                raise RuntimeError(f"cluster decision referenced missing event: {event_id}")
            cluster_score = decision.score

        new_assignment = self.repository.assign_observation(event_id, observation.id, cluster_score, CLUSTER_VERSION)
        observations = self.repository.list_event_observations(event_id)
        trusted_observations = [
            item for item in observations if item.provenance_level != "unverified_discovery"
        ]
        unverified_count = len(observations) - len(trusted_observations)
        if observation.provenance_level == "unverified_discovery":
            # Discovery-only facts may enrich the analyst's evidence panel, but
            # must not create a score run, move lifecycle state, or emit an
            # alert outbox record. Provider verification of this same stable ID
            # schedules a later full scoring revision.
            candidate_evidence = _evidence_items(event_id, observations)[:30]
            suffix = f"；另有 {unverified_count} 条未复核发现候选，不参与评分或告警。"
            coverage_note = current.coverage_note
            if "条未复核发现候选" not in coverage_note:
                coverage_note += suffix
            updated_candidate = current.model_copy(update={
                "cluster_version": current.cluster_version if not new_assignment else current.cluster_version + 1,
                "evidence": candidate_evidence,
                "discussion_evidence_state": (
                    current.discussion_evidence_state
                    if current.discussion_evidence_state == EvidenceState.OBSERVED
                    else EvidenceState.UNTRUSTED
                ),
                "coverage_note": coverage_note,
            })
            self.repository.upsert_event(updated_candidate)
            return updated_candidate
        if not trusted_observations:
            raise RuntimeError("verified observation revision was not persisted as trusted provenance")
        digest_material = {
            "observations": [
                {
                    "id": item.id, "collectedAt": item.collected_at.isoformat(),
                    "metrics": item.metrics, "provenanceLevel": item.provenance_level,
                }
                for item in trusted_observations
            ],
            "behaviorApplicability": current.behavior_evidence_state.value,
            "scoreVersion": SCORE_VERSION,
            "thresholdVersion": THRESHOLD_VERSION,
        }
        digest = hashlib.sha256(json.dumps(digest_material, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        input_digest = f"sha256:{digest}"
        # The score/event/outbox commit can succeed immediately before the
        # processing acknowledgement is lost. A durable input digest turns the
        # retry into an acknowledgement-only operation instead of appending a
        # duplicate point and changing velocity/streaks.
        if self.repository.has_score_input_digest(event_id, input_digest):
            committed = self.repository.get_event(event_id)
            if committed is None:
                raise RuntimeError(f"score digest exists without event state: {event_id}")
            return committed
        signal_snapshots = _snapshots(trusted_observations)
        evaluation_at = max(item.collected_at for item in trusted_observations)
        bucket_at = _score_bucket(evaluation_at)
        earliest = min(trusted_observations, key=lambda item: item.published_at)
        initializing_from_discovery = current.evidence_count == 0 and not current.timeline
        effective_event_type = infer_event_type(earliest) if initializing_from_discovery else current.event_type
        effective_first_seen = (
            earliest.published_at
            if initializing_from_discovery
            else min(current.first_seen, earliest.published_at)
        )
        pending_baseline = self._pending_baselines.get(event_id)
        if pending_baseline and pending_baseline[0] < bucket_at:
            self._record_baseline(pending_baseline[1], pending_baseline[2])
        baseline_history = self._baseline_history[effective_event_type]
        metrics = aggregate_metrics(effective_event_type, signal_snapshots, Baseline(dict(baseline_history)))
        baseline_samples = max((len(values) for values in baseline_history.values()), default=0)
        # Do not let an event calibrate itself while its 15-minute scoring bucket
        # is still being assembled. The latest bucket becomes historical only
        # when a later bucket arrives.
        self._pending_baselines[event_id] = (bucket_at, effective_event_type, signal_snapshots)
        prior_timeline = [point for point in current.timeline if point.at < bucket_at]
        previous_point = prior_timeline[-1] if prior_timeline else None
        velocity = 0.0 if previous_point is None else round(((metrics.attention - previous_point.attention) + (metrics.behavior - previous_point.behavior)) / 2, 2)
        joint_growth = _growth_streak(prior_timeline[-3:], metrics.attention, metrics.behavior)
        gap_growth = _gap_streak(effective_event_type, prior_timeline[-2:], metrics.attention, metrics.behavior)
        decline_streak = _decline_streak(prior_timeline[-3:], metrics.attention, metrics.behavior)
        inactive_hours = _inactive_hours(
            prior_timeline, bucket_at, metrics.attention, metrics.behavior, effective_first_seen,
        )
        # A newly observed, type-valid behavior metric immediately supersedes a
        # previous manual N/A mark for this scoring cycle.
        behavior_applicable = metrics.behavior_observed or current.behavior_evidence_state != EvidenceState.NOT_APPLICABLE
        score_input = to_score_input(
            effective_event_type, metrics, velocity=velocity, consecutive_joint_growth=joint_growth,
            consecutive_gap_growth=gap_growth, consecutive_decline=decline_streak,
            inactive_hours=inactive_hours, official_source_led=earliest.signal_family == "official",
            baseline_maturity=min(100, max(10, baseline_samples / 28 * 100)),
            temporal_stability=min(100, max(20, joint_growth / 3 * 100)),
            previous_state=current.state,
            behavior_applicable=behavior_applicable,
        )
        result = score_event(
            score_input,
            hours_since_first_seen=max(0, (evaluation_at - effective_first_seen).total_seconds() / 3600),
        )

        evidence_items = _evidence_items(event_id, observations)
        verified_evidence_count = sum(
            item.provenance_level != "unverified_discovery" for item in evidence_items
        )
        point = MetricPoint(at=bucket_at, attention=metrics.attention, behavior=metrics.behavior)
        timeline = sorted([item for item in current.timeline if item.at != bucket_at] + [point], key=lambda item: item.at)[-672:]
        drivers = result.drivers + metrics.drivers
        expected_families = 4 if behavior_applicable else 3
        coverage_suffix = "缺失会进入不确定性。" if behavior_applicable else "行为已明确标记为不适用并从覆盖分母移除。"
        updated = current.model_copy(update={
            "cluster_version": current.cluster_version if created_new or not new_assignment else current.cluster_version + 1,
            "title": earliest.title or earliest.text[:120] or "未命名 AI 事件",
            "title_en": earliest.title or earliest.text[:120] or "Untitled AI event",
            "event_type": effective_event_type,
            "first_seen": effective_first_seen,
            "state": result.state, "labels": result.labels, "attention": metrics.attention,
            "behavior": metrics.behavior, "diversity": metrics.diversity, "authority": metrics.authority,
            "coordination_risk": metrics.coordination_risk, "coverage": score_input.coverage,
            "uncertainty": result.uncertainty, "evidence_strength": strength_tier(result.evidence_strength),
            "behavior_evidence_state": (
                EvidenceState.OBSERVED if metrics.behavior_observed
                else EvidenceState.NOT_APPLICABLE if current.behavior_evidence_state == EvidenceState.NOT_APPLICABLE
                else EvidenceState.MISSING
            ),
            "discussion_evidence_state": (
                EvidenceState.OBSERVED if any(item.signal_family == "discussion" for item in trusted_observations)
                else EvidenceState.UNTRUSTED if any(item.signal_family == "discussion" for item in observations)
                else EvidenceState.MISSING
            ),
            "evidence_score": result.evidence_strength,
            "velocity": velocity, "gap_residual": result.gap_residual, "updated_at": evaluation_at,
            "independent_sources": len({item.entity_id or item.source_id for item in trusted_observations}),
            "platforms": list(dict.fromkeys(item.platform for item in trusted_observations)),
            "signal_families": list(dict.fromkeys(item.signal_family for item in trusted_observations)),
            "evidence_count": verified_evidence_count,
            "driver": "；".join(drivers[:4]),
            "coverage_note": (
                f"覆盖 {metrics.independent_signal_families}/{expected_families} 个适用的独立信号家族；{coverage_suffix}"
                + (f"；另有 {unverified_count} 条未复核发现候选，不参与评分或告警。" if unverified_count else "")
            ),
            "timeline": timeline, "evidence": evidence_items[:30],
            "score_version": SCORE_VERSION, "threshold_version": THRESHOLD_VERSION,
        })
        score_record = StoredScore(
            event_id=event_id, score_version=SCORE_VERSION, threshold_version=THRESHOLD_VERSION,
            input_from=min(item.collected_at for item in trusted_observations),
            input_to=max(item.collected_at for item in trusted_observations), input_digest=input_digest,
            drivers=drivers, payload=updated.model_dump(mode="json", by_alias=True),
            created_at=datetime.now(timezone.utc),
        )
        self.repository.commit_scored_event(updated, score_record)
        return updated
