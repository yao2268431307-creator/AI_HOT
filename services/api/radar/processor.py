from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections import defaultdict
from datetime import datetime, timezone

from .clustering import ClusterCandidate, choose_cluster, entities
from .contracts import Evidence, EvidenceState, EventType, LifecycleState, MetricPoint, Observation, RadarEvent, StoredScore, StructureLabel
from .embeddings import BgeM3Provider
from .feature_registry import feature_registry_identity
from .metrics import Baseline, SignalSnapshot, aggregate_metrics, to_score_input
from .normalize import canonical_text, normalize_url, sanitize_external_text
from .scoring import ACTIVE_WINDOW_HOURS, COOLING_START_HOURS, EVIDENCE_POLICY_VERSION, LABEL_POLICY_VERSION, SCORE_VERSION, THRESHOLD_VERSION, clamp, expected_behavior, score_event, score_input_record, strength_tier
from .storage import ConcurrentScoreConflict, InMemoryRepository, PostgresRepository


CLUSTER_VERSION = "cluster-0.2.0"

UNSUPPORTED_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("融资与资本事件不属于 V1 AI 产品/研究热点模型", re.compile(
        r"\b(fundraising|funding round|series [a-f]|seed round|venture capital|valuation|acquisition|acquired)\b|融资|估值|收购|并购|资本市场",
    )),
    ("政策与监管事件不属于 V1 AI 产品/研究热点模型", re.compile(
        r"\b(policy proposal|regulation|regulatory|legislation|executive order|antitrust)\b|政策|监管|立法|行政令|反垄断",
    )),
)


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


def unsupported_reason(observation: Observation) -> str | None:
    text = canonical_text(f"{observation.title or ''} {observation.text}")
    return next((reason for reason, pattern in UNSUPPORTED_PATTERNS if pattern.search(text)), None)


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
            captured_at=current.collected_at,
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

    def _record_baseline(self, event_id: str, event_type: EventType, snapshots: list[SignalSnapshot]) -> None:
        for snapshot in snapshots:
            for metric, value in snapshot.metrics.items():
                if metric not in snapshot.previous_metrics:
                    continue
                material = "|".join((
                    event_id, event_type.value, snapshot.platform.lower(), snapshot.source_id,
                    snapshot.captured_at.isoformat(), metric, str(snapshot.previous_metrics[metric]), str(value),
                ))
                self.repository.append_baseline_sample(
                    "sha256:" + hashlib.sha256(material.encode()).hexdigest(), event_id, event_type,
                    f"{snapshot.platform.lower()}:{metric}", snapshot.captured_at,
                    max(0.0, value - snapshot.previous_metrics[metric]),
                )

    def invalidate_events(self, event_ids: list[str]) -> None:
        """Drop embeddings derived from titles affected by deletion or edits."""
        for event_id in event_ids:
            self._event_embeddings.pop(event_id, None)
            self._event_embedding_titles.pop(event_id, None)
        self.repository.delete_event_embeddings(event_ids)

    def _stable_labels(
        self, event_id: str, current: RadarEvent, raw_labels: list[StructureLabel], bucket_at: datetime,
    ) -> list[StructureLabel]:
        """Confirm label additions/removals across two distinct score cycles."""
        previous_cycle_runs = [
            score for score in self.repository.list_score_runs(event_id)
            if _score_bucket(score.input_to) < bucket_at
        ]
        previous_raw: set[StructureLabel] | None = None
        if previous_cycle_runs:
            replay = previous_cycle_runs[-1].payload.get("_replay", {})
            expected = replay.get("expected", {}) if isinstance(replay, dict) else {}
            values = expected.get("labels", []) if isinstance(expected, dict) else []
            previous_raw = {StructureLabel(value) for value in values}
        active = set(current.labels)
        raw = set(raw_labels)
        stable: set[StructureLabel] = set()
        for label in raw:
            if label in active or label == StructureLabel.REACTIVATED or (previous_raw is not None and label in previous_raw):
                stable.add(label)
        for label in active - raw:
            if previous_raw is None or label in previous_raw:
                stable.add(label)
        return [label for label in StructureLabel if label in stable]

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
        not_supported = unsupported_reason(observation)
        return RadarEvent(
            id=event_id, title=title, titleEn=title, eventType=infer_event_type(observation),
            classificationStatus="unsupported" if not_supported else "supported",
            unsupportedReason=not_supported,
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
            for attempt in range(3):
                try:
                    return await self._process_one(observation)
                except ConcurrentScoreConflict:
                    if attempt == 2:
                        raise
            raise RuntimeError("unreachable scoring retry state")

    async def refresh_time_driven(self, evaluation_at: datetime | None = None) -> int:
        """Advance quiet events without inventing a new provider observation."""
        now = evaluation_at or datetime.now(timezone.utc)
        async with self._process_lock:
            refreshed = 0
            for event in self.repository.list_events():
                if event.superseded_by or event.classification_status == "unsupported" or not event.timeline:
                    continue
                try:
                    changed = await self._refresh_quiet_event(event, now)
                except ConcurrentScoreConflict:
                    latest = self.repository.get_event(event.id)
                    changed = bool(latest and await self._refresh_quiet_event(latest, now))
                refreshed += int(changed)
            return refreshed

    async def _refresh_quiet_event(self, current: RadarEvent, evaluation_at: datetime) -> bool:
        bucket_at = _score_bucket(evaluation_at)
        if current.timeline and current.timeline[-1].at >= bucket_at:
            return False
        observations = [
            item for item in self.repository.list_event_observations(current.id)
            if item.provenance_level != "unverified_discovery"
        ]
        if not observations:
            return False
        last_signal_at = max(item.collected_at for item in observations)
        inactive_hours = max(0.0, (evaluation_at - last_signal_at).total_seconds() / 3600)
        cooling_start = COOLING_START_HOURS[current.event_type]
        if inactive_hours < cooling_start:
            return False

        active_window = ACTIVE_WINDOW_HOURS[current.event_type]
        decay_span = max(0.25, active_window - cooling_start)
        decay_factor = clamp((active_window - inactive_hours) / decay_span, 0, 1)
        signal_bucket = _score_bucket(last_signal_at)
        base_point = next(
            (point for point in reversed(current.timeline) if point.at <= signal_bucket),
            current.timeline[0],
        )
        attention = round(base_point.attention * decay_factor, 2)
        behavior = round(base_point.behavior * decay_factor, 2)
        prior_timeline = [point for point in current.timeline if point.at < bucket_at]
        previous_point = prior_timeline[-1]
        velocity = round(((attention - previous_point.attention) + (behavior - previous_point.behavior)) / 2, 2)
        decline_streak = _decline_streak(prior_timeline[-3:], attention, behavior)

        signal_snapshots = _snapshots(observations)
        baseline_history, baseline_digest, baseline_fact_count = self.repository.load_baseline_history(current.event_type, current.id)
        feature_registry_version, feature_registry_digest = feature_registry_identity()
        metrics = aggregate_metrics(current.event_type, signal_snapshots, Baseline(baseline_history))
        behavior_applicable = metrics.behavior_observed or current.behavior_evidence_state != EvidenceState.NOT_APPLICABLE
        score_input = to_score_input(
            current.event_type, metrics, velocity=velocity,
            consecutive_joint_growth=0, consecutive_gap_growth=0,
            consecutive_decline=decline_streak, inactive_hours=inactive_hours,
            official_source_led=bool(current.labels and "official_source_led" in current.labels),
            baseline_maturity=max(10, current.evidence_score),
            temporal_stability=max(20, current.evidence_score),
            previous_state=current.state, behavior_applicable=behavior_applicable,
        )
        score_input.attention = attention
        score_input.behavior = behavior
        score_input.coverage = current.coverage
        score_input.cluster_confidence = current.diversity
        score_input.anomaly_robust_z = 0
        hours_since_first_seen = max(0, (evaluation_at - current.first_seen).total_seconds() / 3600)
        result = score_event(score_input, hours_since_first_seen=hours_since_first_seen)
        stable_labels = self._stable_labels(current.id, current, result.labels, bucket_at)
        point = MetricPoint(at=bucket_at, attention=attention, behavior=behavior)
        timeline = sorted(current.timeline + [point], key=lambda item: item.at)[-672:]
        drivers = [*result.drivers, f"距最后可信采集已 {inactive_hours:.2f} 小时，执行时间驱动衰减"]
        updated = current.model_copy(update={
            "state": result.state, "labels": stable_labels,
            "attention": attention, "behavior": behavior,
            "coverage": score_input.coverage, "uncertainty": result.uncertainty,
            "evidence_strength": strength_tier(result.evidence_strength),
            "evidence_score": result.evidence_strength, "velocity": velocity,
            "gap_residual": result.gap_residual, "updated_at": evaluation_at,
            "driver": "；".join(drivers[:4]), "timeline": timeline,
        })
        digest_material = {
            "mode": "time_driven", "eventId": current.id,
            "evaluationBucket": bucket_at.isoformat(), "lastSignalAt": last_signal_at.isoformat(),
            "previousState": current.state.value, "attention": attention, "behavior": behavior,
            "scoreVersion": SCORE_VERSION, "thresholdVersion": THRESHOLD_VERSION,
            "featureRegistryVersion": feature_registry_version, "featureRegistryDigest": feature_registry_digest,
            "baselineDigest": baseline_digest, "evidencePolicyVersion": EVIDENCE_POLICY_VERSION,
            "labelPolicyVersion": LABEL_POLICY_VERSION, "clusterVersion": current.cluster_version,
            "activeWindowHours": active_window, "coolingStartHours": cooling_start,
        }
        input_digest = "sha256:" + hashlib.sha256(
            json.dumps(digest_material, sort_keys=True).encode(),
        ).hexdigest()
        if self.repository.has_score_input_digest(current.id, input_digest):
            return False
        replay_payload = updated.model_dump(mode="json", by_alias=True)
        replay_payload["_replay"] = {
            "scoreInput": score_input_record(score_input), "hoursSinceFirstSeen": hours_since_first_seen,
            "expected": {
                "state": result.state.value, "labels": [label.value for label in result.labels],
                "evidenceStrength": result.evidence_strength, "uncertainty": result.uncertainty,
                "gapResidual": result.gap_residual,
            },
        }
        self.repository.commit_scored_event(updated, StoredScore(
            event_id=current.id, score_version=SCORE_VERSION, threshold_version=THRESHOLD_VERSION,
            baseline_version=f"baseline-{current.event_type.value}-{baseline_fact_count}", baseline_digest=baseline_digest,
            feature_registry_version=feature_registry_version, feature_registry_digest=feature_registry_digest,
            evidence_policy_version=EVIDENCE_POLICY_VERSION, label_policy_version=LABEL_POLICY_VERSION,
            cluster_version=current.cluster_version, identity_version="identity-account-fallback-v1",
            input_observation_ids=sorted({item.id for item in observations}),
            input_from=last_signal_at, input_to=evaluation_at, input_digest=input_digest,
            drivers=drivers, payload=replay_payload,
            created_at=datetime.now(timezone.utc),
        ))
        return True

    async def _process_one(self, observation: Observation) -> RadarEvent:
        candidates = self._candidates()
        observation_embedding: list[float] | None = None
        if self.embedding_provider:
            try:
                observation_embedding = (
                    await self.embedding_provider.embed([f"{observation.title or ''}\n{observation.text}"])
                )[0]
                model_version = f"{self.embedding_provider.model}:{self.embedding_provider.dimensions}"
                titles = {candidate.event_id: candidate.title for candidate in candidates}
                nearest = self.repository.nearest_event_embeddings(
                    observation_embedding, titles, model_version, limit=50,
                )
                cycle_embeddings = dict(nearest)
                observation_urls = {normalize_url(observation.url)}
                observation_entities = entities(f"{observation.title or ''} {observation.text}")
                priority_candidates = sorted(
                    (candidate for candidate in candidates if candidate.event_id not in cycle_embeddings),
                    key=lambda candidate: (
                        not bool(candidate.urls & observation_urls or candidate.entities & observation_entities),
                        -candidate.latest_at.timestamp(), candidate.event_id,
                    ),
                )[:25]
                priority_titles = {
                    candidate.event_id: candidate.title for candidate in priority_candidates
                }
                cycle_embeddings.update(self.repository.load_event_embeddings(
                    priority_titles, model_version, self.embedding_provider.dimensions,
                ))
                missing = [
                    candidate for candidate in priority_candidates
                    if candidate.event_id not in cycle_embeddings
                ]
                missing_vectors = (
                    await self.embedding_provider.embed([candidate.title for candidate in missing])
                    if missing else []
                )
                for candidate, vector in zip(missing, missing_vectors, strict=True):
                    cycle_embeddings[candidate.event_id] = vector
                    self.repository.save_event_embedding(
                        candidate.event_id,
                        model_version,
                        candidate.title,
                        vector,
                    )
                # Keep only this cycle's HNSW top-K plus bounded hard/recent
                # candidates. Persisted vectors outside the shortlist are not
                # loaded or regenerated, so Python similarity remains bounded.
                self._event_embeddings = cycle_embeddings
                self._event_embedding_titles = {
                    event_id: titles[event_id] for event_id in cycle_embeddings
                }
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
            current = self.repository.get_event(event_id) or current
            cluster_score = 1.0
            if observation_embedding:
                self._event_embeddings[event_id] = observation_embedding
                self._event_embedding_titles[event_id] = f"{current.title} {current.title_en}"
                self.repository.save_event_embedding(
                    event_id,
                    f"{self.embedding_provider.model}:{self.embedding_provider.dimensions}",
                    self._event_embedding_titles[event_id],
                    observation_embedding,
                )
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
        earliest = min(trusted_observations, key=lambda item: item.published_at)
        classification_basis = next(
            (item for item in sorted(trusted_observations, key=lambda item: item.published_at)
             if item.signal_family in {"official", "research"}),
            earliest,
        )
        not_supported = unsupported_reason(classification_basis)
        if not_supported:
            evidence_items = _evidence_items(event_id, observations)
            verified_evidence_count = sum(
                item.provenance_level != "unverified_discovery" for item in evidence_items
            )
            updated_unsupported = current.model_copy(update={
                "classification_status": "unsupported", "unsupported_reason": not_supported,
                "event_type": infer_event_type(classification_basis),
                "first_seen": min(item.published_at for item in trusted_observations),
                "state": LifecycleState.INSUFFICIENT_DATA, "labels": [],
                "attention": 0, "behavior": 0, "diversity": 0, "coordination_risk": 0,
                "coverage": 0, "uncertainty": 100, "evidence_strength": "low",
                "evidence_score": 0, "velocity": 0, "gap_residual": 0,
                "updated_at": max(item.collected_at for item in trusted_observations),
                "independent_sources": len({item.entity_id or item.source_id for item in trusted_observations}),
                "platforms": list(dict.fromkeys(item.platform for item in trusted_observations)),
                "signal_families": list(dict.fromkeys(item.signal_family for item in trusted_observations)),
                "evidence_count": verified_evidence_count,
                "driver": f"未支持队列：{not_supported}。该事件不进入数值评分或强告警。",
                "coverage_note": "V1 仅评分五类 AI 产品、开发、研究与安全事件；该候选保留证据供人工分流。",
                "timeline": [], "evidence": evidence_items[:30],
            })
            # Deliberately no StoredScore and no score.created outbox: an
            # unsupported candidate is a triage item, not a forced profile.
            self.repository.upsert_event(updated_unsupported)
            return updated_unsupported
        signal_snapshots = _snapshots(trusted_observations)
        evaluation_at = max(item.collected_at for item in trusted_observations)
        bucket_at = _score_bucket(evaluation_at)
        initializing_from_discovery = current.evidence_count == 0 and not current.timeline
        effective_event_type = infer_event_type(earliest) if initializing_from_discovery else current.event_type
        effective_first_seen = (
            earliest.published_at
            if initializing_from_discovery
            else min(current.first_seen, earliest.published_at)
        )
        baseline_history, baseline_digest, baseline_fact_count = self.repository.load_baseline_history(effective_event_type, event_id)
        feature_registry_version, feature_registry_digest = feature_registry_identity()
        digest_material = {
            "observations": [
                {
                    "id": item.id, "externalId": item.external_id, "sourceId": item.source_id,
                    "accountId": item.account_id, "entityId": item.entity_id,
                    "platform": item.platform, "signalFamily": item.signal_family,
                    "publishedAt": item.published_at.isoformat(), "availableAt": item.available_at.isoformat() if item.available_at else None,
                    "availabilityBasis": item.availability_basis, "collectedAt": item.collected_at.isoformat(),
                    "contentFingerprint": item.content_fingerprint, "metrics": item.metrics,
                    "rawEvidenceRef": item.raw_evidence_ref, "provenanceLevel": item.provenance_level,
                }
                for item in sorted(trusted_observations, key=lambda row: (row.id, row.collected_at))
            ],
            "behaviorApplicability": current.behavior_evidence_state.value,
            "clusterAssignmentVersion": CLUSTER_VERSION,
            "clusterVersion": current.cluster_version,
            "featureRegistryVersion": feature_registry_version,
            "featureRegistryDigest": feature_registry_digest,
            "baselineDigest": baseline_digest,
            "scoreVersion": SCORE_VERSION,
            "thresholdVersion": THRESHOLD_VERSION,
            "evidencePolicyVersion": EVIDENCE_POLICY_VERSION,
            "labelPolicyVersion": LABEL_POLICY_VERSION,
            "identityVersion": "identity-account-fallback-v1",
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
        metrics = aggregate_metrics(effective_event_type, signal_snapshots, Baseline(dict(baseline_history)))
        baseline_samples = max((len(values) for values in baseline_history.values()), default=0)
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
        hours_since_first_seen = max(0, (evaluation_at - effective_first_seen).total_seconds() / 3600)
        result = score_event(score_input, hours_since_first_seen=hours_since_first_seen)
        stable_labels = self._stable_labels(event_id, current, result.labels, bucket_at)

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
            "classification_status": "supported", "unsupported_reason": None,
            "first_seen": effective_first_seen,
            "state": result.state, "labels": stable_labels, "attention": metrics.attention,
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
        replay_payload = updated.model_dump(mode="json", by_alias=True)
        replay_payload["_replay"] = {
            "scoreInput": score_input_record(score_input),
            "hoursSinceFirstSeen": hours_since_first_seen,
            "expected": {
                "state": result.state.value, "labels": [label.value for label in result.labels],
                "evidenceStrength": result.evidence_strength, "uncertainty": result.uncertainty,
                "gapResidual": result.gap_residual,
            },
        }
        score_record = StoredScore(
            event_id=event_id, score_version=SCORE_VERSION, threshold_version=THRESHOLD_VERSION,
            baseline_version=f"baseline-{effective_event_type.value}-{baseline_fact_count}",
            baseline_digest=baseline_digest,
            feature_registry_version=feature_registry_version, feature_registry_digest=feature_registry_digest,
            evidence_policy_version=EVIDENCE_POLICY_VERSION, label_policy_version=LABEL_POLICY_VERSION,
            cluster_version=updated.cluster_version, identity_version="identity-account-fallback-v1",
            input_observation_ids=sorted({item.id for item in trusted_observations}),
            input_from=min(item.collected_at for item in trusted_observations),
            input_to=max(item.collected_at for item in trusted_observations), input_digest=input_digest,
            drivers=drivers, payload=replay_payload,
            created_at=datetime.now(timezone.utc),
        )
        self.repository.commit_scored_event(updated, score_record)
        self._record_baseline(event_id, effective_event_type, signal_snapshots)
        return updated
