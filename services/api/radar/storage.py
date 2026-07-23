from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator
from zoneinfo import ZoneInfo

from .contracts import AlertRuleRequest, BehaviorApplicabilityRequest, ClusterEditRequest, ConnectorStatus, EventType, EvidenceState, EvidenceStrength, FeedbackRequest, LifecycleState, MetricIncidentRequest, MetricSnapshot, MutationReceipt, Observation, ProductInteractionRequest, RadarEvent, StoredScore, WatchlistItem, WatchlistRequest
from .facts import split_observation
from .product_metrics import TRIAGE_ACTIONS, load_product_metric_policy
from .source_discovery import SourceCandidate, load_source_score_policy, promote_candidates, source_score_policy_digest


AUDIT_TRIGGER_SPECS = {
    "feedback_append_only": ("public", "feedback", "public", "reject_audit_fact_mutation"),
    "product_interactions_append_only": ("public", "product_interactions", "public", "reject_audit_fact_mutation"),
    "review_queue_entries_append_only": ("public", "review_queue_entries", "public", "reject_audit_fact_mutation"),
    "lead_threshold_crossings_append_only": (
        "public", "lead_threshold_crossings", "public", "reject_audit_fact_mutation",
    ),
    "content_ingest_history_append_only": ("public", "content_ingest_history", "public", "reject_audit_fact_mutation"),
    "connector_runs_append_only": ("public", "connector_runs", "public", "reject_audit_fact_mutation"),
    "metric_incidents_append_only": ("public", "metric_incidents", "public", "reject_audit_fact_mutation"),
    "source_promotion_facts_append_only": (
        "public", "source_promotion_facts", "public", "reject_audit_fact_mutation",
    ),
    "score_history_erasure_audit_append_only": (
        "public", "score_history_erasure_audit", "public", "reject_audit_fact_mutation",
    ),
    "connector_budget_reconciliation_audit_append_only": (
        "public", "connector_budget_reconciliation_audit", "public", "reject_audit_fact_mutation",
    ),
    "alert_deliveries_immutable": ("public", "alert_deliveries", "public", "protect_alert_delivery_fact"),
    "observation_processing_history_monotonic": (
        "public", "observation_processing_history", "public", "protect_processing_history_fact",
    ),
}


def audit_trigger_specs_verified(rows: list[tuple[object, ...]]) -> bool:
    """Require exact trigger identity, attachment, function, mode and events."""
    expected = {
        (name, schema, table, function_schema, function, "O", True, True, True, True)
        for name, (schema, table, function_schema, function) in AUDIT_TRIGGER_SPECS.items()
    }
    normalized = {
        (
            str(name), str(schema), str(table), str(function_schema), str(function), str(enabled),
            bool(row_level), bool(before), bool(on_delete), bool(on_update),
        )
        for name, schema, table, function_schema, function, enabled, row_level, before, on_delete, on_update in rows
    }
    return normalized == expected


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ConcurrentScoreConflict(RuntimeError):
    """The durable event changed after the scorer read its input snapshot."""


def score_cycle(value: datetime) -> str:
    bucket = value.replace(minute=(value.minute // 15) * 15, second=0, microsecond=0)
    return bucket.isoformat()


def _queue_eligibility(event: RadarEvent, eligible_at: datetime | None = None) -> dict[str, object] | None:
    policy = load_product_metric_policy()
    if event.state.value not in policy.reviewable_lifecycle_states:
        return None
    entered_at = eligible_at or utcnow()
    entry_id = str(uuid.uuid4())
    material = f"{event.id}|{event.cluster_version}|{entered_at.isoformat()}|{policy.version}|{entry_id}"
    return {
        "id": entry_id,
        "eventId": event.id,
        "eligibilityKey": f"qe-{hashlib.sha256(material.encode()).hexdigest()}",
        "eligibleAt": entered_at,
        "lifecycleState": event.state.value,
        "clusterVersion": event.cluster_version,
        "scoreVersion": event.score_version,
        "policyVersion": policy.version,
        "entryKind": "transition",
    }


def _entered_review_queue(previous: RadarEvent | None, current: RadarEvent) -> bool:
    return _entered_review_queue_state(previous.state.value if previous else None, current.state.value)


def _entered_review_queue_state(previous_state: str | None, current_state: str) -> bool:
    reviewable = set(load_product_metric_policy().reviewable_lifecycle_states)
    return current_state in reviewable and previous_state not in reviewable


def _meets_lead_threshold(event: RadarEvent) -> bool:
    policy = load_product_metric_policy().manual_evaluation
    return (
        event.state.value in policy.lead_lifecycle_states
        and event.attention >= policy.lead_minimum_attention
        and event.coverage >= policy.lead_minimum_coverage
    )


class InMemoryRepository:
    """Deterministic repository for tests, local preview and disconnected operation."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.observations: dict[str, Observation] = {}
        self.events: dict[str, RadarEvent] = {}
        self.connectors: dict[str, ConnectorStatus] = {}
        self.feedback: dict[str, dict[str, object]] = {}
        self.alerts: dict[str, tuple[str, str, AlertRuleRequest]] = {}
        self.alert_deliveries: list[dict[str, object]] = []
        self.watchlists: dict[str, tuple[str, str, WatchlistRequest, datetime]] = {}
        self.cluster_edits: dict[str, dict[str, object]] = {}
        self.lineage_edges: list[dict[str, object]] = []
        self.outbox: list[dict[str, object]] = []
        self.score_runs: list[StoredScore] = []
        self.baseline_samples: dict[str, dict[str, object]] = {}
        self.event_observations: dict[str, dict[str, float]] = {}
        self.metric_facts: dict[str, MetricSnapshot] = {}
        self.observation_processing: dict[str, dict[str, object]] = {}
        self.observation_processing_history: list[dict[str, object]] = []
        self.content_ingest_history: list[dict[str, object]] = []
        self.connector_runs: list[dict[str, object]] = []
        self.connector_checkpoints: dict[str, dict[str, object]] = {}
        self.product_interactions: list[dict[str, object]] = []
        self.metric_incidents: list[dict[str, object]] = []
        self.review_queue_entries: list[dict[str, object]] = []
        self.lead_threshold_crossings: dict[tuple[str, str, str], dict[str, object]] = {}
        self.source_profiles: dict[str, dict[str, object]] = {}
        self.source_content_fingerprints: set[tuple[str, str]] = set()
        self.source_promotion_facts: list[dict[str, object]] = []
        self.raw_evidence_deletions: dict[str, dict[str, object]] = {}
        self.runtime_components: dict[str, dict[str, object]] = {}
        self.event_embeddings: dict[tuple[str, str], dict[str, object]] = {}
        self.connector_budget_reservations: dict[str, dict[str, object]] = {}
        self.workspace_memberships: dict[tuple[str, str], dict[str, str]] = {}
        self.revoked_token_jtis: set[str] = set()

    def set_workspace_membership(
        self, subject: str, workspace_id: str, role: str, status: str = "active",
    ) -> None:
        self.workspace_memberships[(workspace_id, subject)] = {"role": role, "status": status}

    def resolve_workspace_membership(self, subject: str, workspace_id: str) -> str | None:
        membership = self.workspace_memberships.get((workspace_id, subject))
        return str(membership["role"]) if membership and membership.get("status") == "active" else None

    def is_token_revoked(self, jti: str) -> bool:
        return jti in self.revoked_token_jtis

    def runtime_attestation(self) -> dict[str, object]:
        return {
            "storageBackend": "in_memory", "rlsVerified": False,
            "migrationVersion": None, "instanceId": "local-demo", "databaseClockSkewSeconds": None,
            "runtimeComponents": [row.copy() for row in self.runtime_components.values()],
            "disasterRecoveryAttestation": None,
        }

    def heartbeat_runtime_component(self, component_id: str, instance_id: str, details: dict[str, object] | None = None) -> None:
        self.runtime_components[component_id] = {
            "componentId": component_id, "instanceId": instance_id, "lastSeenAt": utcnow(), "details": details or {},
        }

    def save_observation_with_outbox(self, observation: Observation) -> bool:
        """Observation and outbox record share the same lock/commit boundary."""
        with self._lock:
            content, snapshots = split_observation(observation)
            if observation.provenance_level == "unverified_discovery":
                snapshots = []
            content_inserted = observation.id not in self.observations
            provenance_upgraded = False
            if not content_inserted:
                existing = self.observations[observation.id]
                provenance_upgraded = (
                    existing.provenance_level == "unverified_discovery"
                    and observation.provenance_level != "unverified_discovery"
                )
                if provenance_upgraded:
                    previous_raw_ref = existing.raw_evidence_ref
                    self.observations[observation.id] = observation
                    source_content_key = (observation.source_id, content.content_hash)
                    source_content_duplicate = source_content_key in self.source_content_fingerprints
                    self.source_content_fingerprints.add(source_content_key)
                    self.register_source_candidate(
                        source_id=observation.source_id,
                        display_name=observation.source_id,
                        platform=observation.platform,
                        language=observation.language,
                        observed_at=observation.collected_at,
                        reason=(
                            "provider_verified_upgrade"
                            if not source_content_duplicate else "duplicate_content_observation"
                        ),
                        account_id=observation.account_id,
                        entity_id=observation.entity_id,
                        counts_as_valid=not source_content_duplicate,
                    )
                    remaining_raw_refs = {
                        item.raw_evidence_ref for item in self.observations.values() if item.raw_evidence_ref
                    } | {
                        item.source_revision for item in self.metric_facts.values() if item.source_revision
                    }
                    if previous_raw_ref and previous_raw_ref != observation.raw_evidence_ref and previous_raw_ref not in remaining_raw_refs:
                        self.raw_evidence_deletions[previous_raw_ref] = {
                            "rightsPolicyId": existing.rights_policy_id, "status": "pending", "attempts": 0,
                            "nextAttemptAt": utcnow(), "leaseUntil": None, "lastError": None,
                        }
                elif observation.provenance_level == "unverified_discovery":
                    # A trusted row must never be downgraded by a transient
                    # hydration failure. The connector has already archived this
                    # ignored revision, so queue its unique object for deletion
                    # instead of leaving an untracked orphan.
                    remaining_raw_refs = {
                        item.raw_evidence_ref for item in self.observations.values() if item.raw_evidence_ref
                    } | {
                        item.source_revision for item in self.metric_facts.values() if item.source_revision
                    }
                    if observation.raw_evidence_ref and observation.raw_evidence_ref not in remaining_raw_refs:
                        self.raw_evidence_deletions[observation.raw_evidence_ref] = {
                            "rightsPolicyId": observation.rights_policy_id, "status": "pending", "attempts": 0,
                            "nextAttemptAt": utcnow(), "leaseUntil": None, "lastError": None,
                        }
            if content_inserted:
                source_content_key = (observation.source_id, content.content_hash)
                source_content_duplicate = source_content_key in self.source_content_fingerprints
                trusted = observation.provenance_level != "unverified_discovery"
                if trusted:
                    self.source_content_fingerprints.add(source_content_key)
                self.observations[observation.id] = observation
                reason = (
                    "unverified_discovery" if not trusted
                    else "duplicate_content_observation" if source_content_duplicate
                    else f"connector_observation:{content.connector}"
                )
                self.register_source_candidate(
                    source_id=observation.source_id,
                    display_name=observation.source_id,
                    platform=observation.platform,
                    language=observation.language,
                    observed_at=observation.collected_at,
                    reason=reason,
                    account_id=observation.account_id,
                    entity_id=observation.entity_id,
                    counts_as_valid=trusted and not source_content_duplicate,
                )
                self.content_ingest_history.append({
                    "observationId": observation.id, "connectorId": content.connector,
                    "contentFingerprint": observation.content_fingerprint or content.content_hash,
                    "persistedAt": utcnow(),
                })
                self.outbox.append({"id": str(uuid.uuid4()), "kind": "observation.created", "aggregate_id": observation.id, "created_at": utcnow()})
            new_snapshots = [snapshot for snapshot in snapshots if snapshot.id not in self.metric_facts]
            for snapshot in new_snapshots:
                self.metric_facts[snapshot.id] = snapshot
            if new_snapshots:
                self.outbox.append({"id": str(uuid.uuid4()), "kind": "metric_snapshots.created", "aggregate_id": observation.id, "created_at": utcnow(), "snapshot_ids": [item.id for item in new_snapshots]})
            changed = content_inserted or provenance_upgraded or bool(new_snapshots)
            if changed:
                state = self.observation_processing.setdefault(observation.id, {"revision": 0, "processedRevision": 0, "attempts": 0, "leaseUntil": None, "lastError": None})
                state["revision"] = int(state["revision"]) + 1
                self.observation_processing_history.append({
                    "observationId": observation.id,
                    "revision": int(state["revision"]),
                    "collectedAt": observation.collected_at,
                    "enqueuedAt": utcnow(),
                    "completedAt": None,
                    "attempts": 0,
                    "lastFailedAt": None,
                    "lastError": None,
                })
            return changed

    def register_source_candidate(
        self, *, source_id: str, display_name: str, platform: str, language: str,
        observed_at: datetime, reason: str, account_id: str | None = None, entity_id: str | None = None,
        counts_as_valid: bool = True,
    ) -> None:
        with self._lock:
            profile = self.source_profiles.setdefault(source_id, {
                "id": source_id, "displayName": display_name, "platform": platform,
                "language": language, "status": "candidate", "validObservations": 0,
                "earlyHits": 0, "confirmedHits": 0, "originality": 0.0,
                "domainFocus": 0.0, "authority": 0.0, "marketingMatrixOverlap": 0.0,
                "qualityCalibrated": False, "discoveryReasons": set(), "accountIds": set(),
                "entityIds": set(), "createdAt": observed_at, "firstObservedAt": observed_at,
                "lastObservedAt": observed_at, "activatedAt": None,
            })
            if counts_as_valid:
                profile["validObservations"] = int(profile["validObservations"]) + 1
            profile["lastObservedAt"] = max(profile["lastObservedAt"], observed_at)
            profile["firstObservedAt"] = min(profile["firstObservedAt"], observed_at)
            profile["discoveryReasons"].add(reason)
            if account_id:
                profile["accountIds"].add(account_id)
            if entity_id:
                profile["entityIds"].add(entity_id)

    def list_source_profiles(self) -> list[dict[str, object]]:
        with self._lock:
            return [
                {
                    **profile,
                    "discoveryReasons": sorted(profile["discoveryReasons"]),
                    "accountIds": sorted(profile["accountIds"]),
                    "entityIds": sorted(profile["entityIds"]),
                }
                for profile in self.source_profiles.values()
            ]

    def promote_source_candidates(self, now: datetime | None = None) -> dict[str, object]:
        promoted_at = now or utcnow()
        policy = load_source_score_policy()
        local_day = promoted_at.astimezone(ZoneInfo(policy.timezone_name)).date()
        with self._lock:
            active_count = sum(profile["status"] == "active" for profile in self.source_profiles.values())
            promoted_today = sum(
                fact["promotedAt"].astimezone(ZoneInfo(policy.timezone_name)).date() == local_day
                and fact["toStatus"] == "active"
                for fact in self.source_promotion_facts
            )
            candidates = [
                SourceCandidate(
                    id=str(profile["id"]), discovered_at=profile["createdAt"],
                    valid_observations=int(profile["validObservations"]), early_hits=int(profile["earlyHits"]),
                    confirmed_hits=int(profile["confirmedHits"]), originality=float(profile["originality"]),
                    domain_focus=float(profile["domainFocus"]), authority=float(profile["authority"]),
                    marketing_matrix_overlap=float(profile["marketingMatrixOverlap"]),
                )
                for profile in self.source_profiles.values()
                if profile["status"] == "candidate" and profile["qualityCalibrated"] is True
            ]
            promoted = promote_candidates(
                candidates, active_count, now=promoted_at, promoted_today=promoted_today, policy=policy,
            ) if policy.auto_promotion_enabled else []
            for candidate in promoted:
                profile = self.source_profiles[candidate.id]
                profile["status"] = "active"
                profile["activatedAt"] = promoted_at
                self.source_promotion_facts.append({
                    "id": str(uuid.uuid4()), "sourceId": candidate.id,
                    "fromStatus": "candidate", "toStatus": "active", "score": candidate.score,
                    "policyVersion": policy.version, "policyDigest": source_score_policy_digest(policy),
                    "promotedAt": promoted_at,
                })
            return {
                "promotedSourceIds": [candidate.id for candidate in promoted],
                "activeBefore": active_count, "promotedEarlierToday": promoted_today,
                "policyVersion": policy.version, "autoPromotionEnabled": policy.auto_promotion_enabled,
            }

    def review_source_status(
        self, source_id: str, status: str, actor_id: str, reason: str,
    ) -> MutationReceipt:
        policy = load_source_score_policy()
        reviewed_at = utcnow()
        with self._lock:
            profile = self.source_profiles.get(source_id)
            if profile is None:
                raise KeyError(source_id)
            previous = str(profile["status"])
            if previous == status:
                raise ValueError(f"source is already {status}")
            if status == "active":
                active_count = sum(item["status"] == "active" for item in self.source_profiles.values())
                if active_count >= policy.active_capacity:
                    raise ValueError("active source capacity reached")
            profile["status"] = status
            if status == "active":
                profile["activatedAt"] = reviewed_at
            self.source_promotion_facts.append({
                "id": str(uuid.uuid4()), "sourceId": source_id,
                "fromStatus": previous, "toStatus": status, "score": 0.0,
                "policyVersion": policy.version, "policyDigest": source_score_policy_digest(policy),
                "promotedAt": reviewed_at, "reviewedBy": actor_id,
                "reviewReason": reason, "transitionKind": "manual_review",
            })
        return MutationReceipt(
            id=source_id, status="completed", operation=f"source.review.{status}", createdAt=reviewed_at,
        )

    def claim_observation_processing(self, observation_id: str, lease_seconds: int = 300) -> int | None:
        with self._lock:
            state = self.observation_processing.get(observation_id)
            if not state or int(state["processedRevision"]) >= int(state["revision"]):
                return None
            lease_until = state.get("leaseUntil")
            if isinstance(lease_until, datetime) and lease_until > utcnow():
                return None
            state["leaseUntil"] = utcnow() + timedelta(seconds=lease_seconds)
            state["attempts"] = int(state["attempts"]) + 1
            for row in self.observation_processing_history:
                if row["observationId"] == observation_id and row["revision"] == int(state["revision"]):
                    row["attempts"] = int(row["attempts"]) + 1
            return int(state["revision"])

    def complete_observation_processing(self, observation_id: str, revision: int) -> None:
        with self._lock:
            state = self.observation_processing[observation_id]
            state["processedRevision"] = max(int(state["processedRevision"]), revision)
            state["leaseUntil"] = None
            state["lastError"] = None
            completed_at = utcnow()
            for row in self.observation_processing_history:
                if row["observationId"] == observation_id and int(row["revision"]) <= revision and row["completedAt"] is None:
                    row["completedAt"] = completed_at

    def fail_observation_processing(self, observation_id: str, revision: int, error: str) -> None:
        with self._lock:
            state = self.observation_processing[observation_id]
            if int(state["processedRevision"]) < revision:
                state["leaseUntil"] = None
                state["lastError"] = error[:1000]
                failed_at = utcnow()
                for row in self.observation_processing_history:
                    if row["observationId"] == observation_id and row["revision"] == revision:
                        row["lastFailedAt"] = failed_at
                        row["lastError"] = error[:1000]

    def list_observation_processing_history(self, since: datetime) -> list[dict[str, object]]:
        with self._lock:
            return [row.copy() for row in self.observation_processing_history if row["enqueuedAt"] >= since]

    def list_pending_observation_ids(self, limit: int = 100) -> list[str]:
        now = utcnow()
        with self._lock:
            return [
                item_id for item_id, state in self.observation_processing.items()
                if int(state["processedRevision"]) < int(state["revision"])
                and (state.get("leaseUntil") is None or state["leaseUntil"] < now)
            ][:limit]

    def pipeline_backlog_counts(self) -> dict[str, int]:
        with self._lock:
            processing_pending = sum(
                int(state["processedRevision"]) < int(state["revision"])
                for state in self.observation_processing.values()
            )
            return {
                "processingPending": processing_pending,
                "outboxPending": len(self.outbox),
            }

    def get_latest_observation(self, observation_id: str) -> Observation | None:
        base = self.observations.get(observation_id)
        if base is None:
            return None
        snapshots = [item for item in self.metric_facts.values() if item.subject_id == observation_id]
        if not snapshots:
            return base
        latest_at = max(item.collected_at for item in snapshots)
        latest = [item for item in snapshots if item.collected_at == latest_at]
        return base.model_copy(update={
            "collected_at": latest_at,
            "metrics": {item.metric_name: item.value for item in latest},
            "raw_evidence_ref": next((item.source_revision for item in latest if item.source_revision), base.raw_evidence_ref),
        })

    def upsert_event(self, event: RadarEvent) -> None:
        with self._lock:
            previous = self.events.get(event.id)
            self.events[event.id] = event
            if _entered_review_queue(previous, event):
                entry = _queue_eligibility(event)
                if entry and not any(row["eligibilityKey"] == entry["eligibilityKey"] for row in self.review_queue_entries):
                    self.review_queue_entries.append(entry)

    def save_score(self, score: StoredScore) -> None:
        with self._lock:
            self._append_score(score)

    def _append_score(self, score: StoredScore) -> StoredScore:
        existing = next((item for item in self.score_runs if item.event_id == score.event_id and item.input_digest == score.input_digest), None)
        if existing is not None:
            return existing
        cycle = score_cycle(score.input_to)
        revisions = [
            item.scoring_revision for item in self.score_runs
            if item.event_id == score.event_id and score_cycle(item.input_to) == cycle
        ]
        appended = score.model_copy(update={"scoring_revision": max(revisions, default=0) + 1})
        self.score_runs.append(appended)
        self.outbox.append({
            "id": str(uuid.uuid4()), "kind": "score.created", "aggregate_id": score.event_id,
            "created_at": utcnow(), "cycle_id": cycle, "revision": appended.scoring_revision,
        })
        return appended

    def has_score_input_digest(self, event_id: str, input_digest: str) -> bool:
        return any(score.event_id == event_id and score.input_digest == input_digest for score in self.score_runs)

    def list_score_runs(self, event_id: str) -> list[StoredScore]:
        return sorted(
            (score for score in self.score_runs if score.event_id == event_id),
            key=lambda score: (score.input_to, score.scoring_revision, score.created_at),
        )

    def review_priority_context(
        self, event_anchors: dict[str, datetime]
    ) -> dict[str, dict[str, object]]:
        with self._lock:
            result: dict[str, dict[str, object]] = {}
            for event_id, anchor in event_anchors.items():
                observation_ids = self.event_observations.get(event_id, {})
                trusted_observations = [
                    observation
                    for observation_id in observation_ids
                    if (observation := self.observations.get(observation_id)) is not None
                    and observation.provenance_level != "unverified_discovery"
                ]
                new_evidence_count = len({
                    observation.id
                    for observation in trusted_observations
                    if observation.collected_at > anchor
                })
                runs = self.list_score_runs(event_id)[-2:]
                result[event_id] = {
                    "newEvidenceCount": new_evidence_count,
                    "latestEvidenceAt": max(
                        (observation.collected_at for observation in trusted_observations),
                        default=None,
                    ),
                    "scoreRuns": [
                        {"payload": item.payload, "inputTo": item.input_to}
                        for item in runs
                    ],
                }
            return result

    def latest_evidence_times(self, event_ids: set[str]) -> dict[str, datetime]:
        with self._lock:
            result: dict[str, datetime] = {}
            for event_id in event_ids:
                trusted_times = [
                    observation.collected_at
                    for observation_id in self.event_observations.get(event_id, {})
                    if (observation := self.observations.get(observation_id)) is not None
                    and observation.provenance_level != "unverified_discovery"
                ]
                event = self.events.get(event_id)
                latest = max(trusted_times, default=event.updated_at if event else None)
                if latest is not None:
                    result[event_id] = latest
            return result

    def load_event_embeddings(
        self, event_titles: dict[str, str], model_version: str, dimensions: int
    ) -> dict[str, list[float]]:
        result: dict[str, list[float]] = {}
        with self._lock:
            for event_id, title in event_titles.items():
                row = self.event_embeddings.get((event_id, model_version))
                title_hash = hashlib.sha256(title.encode()).hexdigest()
                if row and row["titleHash"] == title_hash and row["dimensions"] == dimensions:
                    result[event_id] = list(row["embedding"])
        return result

    def save_event_embedding(
        self, event_id: str, model_version: str, title: str, embedding: list[float]
    ) -> None:
        if not embedding:
            raise ValueError("event embedding cannot be empty")
        with self._lock:
            self.event_embeddings[(event_id, model_version)] = {
                "titleHash": hashlib.sha256(title.encode()).hexdigest(),
                "dimensions": len(embedding), "embedding": list(embedding),
            }

    def nearest_event_embeddings(
        self, query_embedding: list[float], event_titles: dict[str, str], model_version: str, limit: int = 50,
    ) -> dict[str, list[float]]:
        query_norm = math.sqrt(sum(value * value for value in query_embedding)) or 1.0
        ranked: list[tuple[float, str, list[float]]] = []
        with self._lock:
            for (event_id, stored_model), row in self.event_embeddings.items():
                title = event_titles.get(event_id)
                vector = list(row["embedding"])
                if (
                    stored_model != model_version or title is None
                    or row["titleHash"] != hashlib.sha256(title.encode()).hexdigest()
                    or len(vector) != len(query_embedding)
                ):
                    continue
                vector_norm = math.sqrt(sum(value * value for value in vector)) or 1.0
                similarity = sum(
                    left * right for left, right in zip(query_embedding, vector, strict=True)
                ) / (query_norm * vector_norm)
                ranked.append((-similarity, event_id, vector))
        return {event_id: vector for _, event_id, vector in sorted(ranked)[:limit]}

    def delete_event_embeddings(self, event_ids: list[str]) -> None:
        with self._lock:
            deleting = set(event_ids)
            self.event_embeddings = {
                key: value for key, value in self.event_embeddings.items()
                if key[0] not in deleting
            }

    def append_baseline_sample(
        self, fact_key: str, source_event_id: str, event_type: EventType, baseline_key: str, observed_at: datetime, value: float,
    ) -> bool:
        with self._lock:
            if fact_key in self.baseline_samples:
                return False
            self.baseline_samples[fact_key] = {
                "factKey": fact_key, "sourceEventId": source_event_id, "eventType": event_type.value, "baselineKey": baseline_key,
                "observedAt": observed_at, "value": max(0.0, value),
            }
            return True

    def load_baseline_history(self, event_type: EventType, exclude_event_id: str | None = None) -> tuple[dict[str, list[float]], str, int]:
        rows = sorted(
            (row for row in self.baseline_samples.values() if row["eventType"] == event_type.value and row["sourceEventId"] != exclude_event_id),
            key=lambda row: (str(row["baselineKey"]), row["observedAt"], str(row["factKey"])),
        )
        history: dict[str, list[float]] = {}
        for row in rows:
            history.setdefault(str(row["baselineKey"]), []).append(float(row["value"]))
        for values in history.values():
            del values[:-1000]
        material = [
            [row["factKey"], row["baselineKey"], row["observedAt"].isoformat(), row["value"]]
            for row in rows
        ]
        digest = "sha256:" + hashlib.sha256(json.dumps(material, separators=(",", ":")).encode()).hexdigest()
        return history, digest, len(rows)

    def list_ranking_score_facts(self) -> list[dict[str, object]]:
        with self._lock:
            rows: list[dict[str, object]] = []
            for event in self.events.values():
                scores = [score for score in self.score_runs if score.event_id == event.id]
                latest = max(scores, key=lambda score: score.input_to) if scores else None
                rows.append({
                    "event": event,
                    "scoreRunId": latest.input_digest if latest else None,
                    "scoreRunAt": latest.input_to if latest else None,
                })
            return rows

    def list_lead_threshold_crossings(self) -> list[dict[str, object]]:
        with self._lock:
            return [{"eventId": key[0], **row} for key, row in self.lead_threshold_crossings.items()]

    def ranking_ledger_snapshot(
        self,
    ) -> tuple[datetime, list[dict[str, object]], list[dict[str, object]]]:
        """Read ranking facts, crossing facts and the watermark under one lock."""
        with self._lock:
            ranking_facts = self.list_ranking_score_facts()
            crossings = self.list_lead_threshold_crossings()
            return utcnow(), ranking_facts, crossings

    def commit_scored_event(self, event: RadarEvent, score: StoredScore) -> None:
        """Atomically update current state, append its reproducibility record and enqueue outbox."""
        with self._lock:
            previous = self.events.get(event.id)
            self.events[event.id] = event
            self._append_score(score)
            crossing_key = (event.id, score.threshold_version, load_product_metric_policy().version)
            if _meets_lead_threshold(event) and crossing_key not in self.lead_threshold_crossings:
                self.lead_threshold_crossings[crossing_key] = {
                    "crossedAt": utcnow(), "scoreRunId": score.input_digest,
                    "thresholdVersion": score.threshold_version,
                    "policyVersion": load_product_metric_policy().version,
                }
            if _entered_review_queue(previous, event):
                entry = _queue_eligibility(event)
                if entry and not any(row["eligibilityKey"] == entry["eligibilityKey"] for row in self.review_queue_entries):
                    self.review_queue_entries.append(entry)

    def assign_observation(self, event_id: str, observation_id: str, cluster_score: float, assignment_version: str) -> bool:
        with self._lock:
            members = self.event_observations.setdefault(event_id, {})
            created = observation_id not in members
            members[observation_id] = cluster_score
            return created

    def list_event_observations(self, event_id: str) -> list[Observation]:
        ids = self.event_observations.get(event_id, {})
        values: list[Observation] = []
        for item_id in ids:
            if item_id not in self.observations:
                continue
            base = self.observations[item_id]
            snapshots = sorted((item for item in self.metric_facts.values() if item.subject_id == item_id), key=lambda item: item.collected_at)
            by_capture: dict[datetime, dict[str, float]] = {}
            for snapshot in snapshots:
                by_capture.setdefault(snapshot.collected_at, {})[snapshot.metric_name] = snapshot.value
            if by_capture:
                values.extend(base.model_copy(update={"collected_at": captured_at, "metrics": metrics}) for captured_at, metrics in by_capture.items())
            else:
                values.append(base.model_copy(update={"metrics": {}}))
        return sorted(values, key=lambda item: (item.published_at, item.collected_at))

    def list_observation_freshness(self, since: datetime) -> list[dict[str, object]]:
        return [
            {
                "id": item.id, "connectorId": item.platform.lower().replace(" ", "-"),
                "availableAt": item.available_at or item.collected_at, "collectedAt": item.collected_at,
                "availabilityBasis": item.availability_basis,
            }
            for item in self.observations.values() if item.collected_at >= since
        ]

    def rollup_and_retain_event_metrics(self, at: datetime, raw_retention_days: int = 90, rollup_retention_days: int = 730) -> dict[str, int]:
        # The in-memory preview keeps only each event's bounded timeline.
        return {"rolledUp": 0, "rawDeleted": 0, "rollupsDeleted": 0}

    def upsert_connector(self, connector: ConnectorStatus) -> None:
        with self._lock:
            self.connectors[connector.id] = connector

    def reserve_connector_budget(
        self, connector_id: str, signal_family: str, maximum_cost_rmb: float,
        total_limit_rmb: float, connector_limit_rmb: float | None,
        family_limit_rmb: float | None, family_connector_ids: set[str], base_spend_rmb: float = 0,
    ) -> str | None:
        with self._lock:
            total_spend = base_spend_rmb + self.monthly_connector_spend()
            connector_spend = self.monthly_connector_spend(connector_ids={connector_id})
            family_spend = self.monthly_connector_spend(connector_ids=family_connector_ids)
            if total_spend + maximum_cost_rmb > total_limit_rmb:
                return None
            if connector_limit_rmb is not None and connector_spend + maximum_cost_rmb > connector_limit_rmb:
                return None
            if family_limit_rmb is not None and family_spend + maximum_cost_rmb > family_limit_rmb:
                return None
            reservation_id = str(uuid.uuid4())
            self.connector_budget_reservations[reservation_id] = {
                "connectorId": connector_id, "signalFamily": signal_family,
                "reservedAmountRmb": maximum_cost_rmb, "status": "reserved", "createdAt": utcnow(),
                "leaseUntil": utcnow() + timedelta(hours=1),
            }
            return reservation_id

    def reconcile_expired_connector_budget_reservations(self) -> int:
        now = utcnow()
        changed = 0
        with self._lock:
            for reservation in self.connector_budget_reservations.values():
                if reservation["status"] == "reserved" and reservation["leaseUntil"] < now:
                    reservation.update({
                        "status": "reconciliation_required",
                        "reconciliationReason": "reservation lease expired before run confirmation",
                    })
                    changed += 1
        return changed

    def connector_budget_reconciliation_count(self) -> int:
        now = utcnow()
        return sum(
            row["status"] == "reconciliation_required"
            or (row["status"] == "reserved" and row["leaseUntil"] < now)
            for row in self.connector_budget_reservations.values()
        )

    def record_connector_run(self, connector_id: str, started_at: datetime, finished_at: datetime, status: str, inserted: int, duplicates: int, coverage: float, error: str | None = None, estimated_cost_rmb: float = 0, budget_reservation_id: str | None = None) -> None:
        with self._lock:
            if budget_reservation_id:
                reservation = self.connector_budget_reservations.get(budget_reservation_id)
                if not reservation or reservation["status"] not in {"reserved", "reconciliation_required"} or reservation["connectorId"] != connector_id:
                    raise RuntimeError("budget reservation is missing or no longer active")
                if estimated_cost_rmb > float(reservation["reservedAmountRmb"]) + 1e-9:
                    raise RuntimeError("actual connector cost exceeded its worst-case reservation")
            self.connector_runs.append({
                "connectorId": connector_id, "startedAt": started_at, "finishedAt": finished_at,
                "status": status, "inserted": inserted, "duplicates": duplicates,
                "latencyMs": max(0, int((finished_at - started_at).total_seconds() * 1000)),
                "coverage": coverage, "error": error, "estimatedCostRmb": max(0, estimated_cost_rmb),
            })
            if budget_reservation_id:
                self.connector_budget_reservations[budget_reservation_id].update({
                    "status": "confirmed", "actualAmountRmb": max(0, estimated_cost_rmb),
                    "confirmedAt": utcnow(),
                })
            # Keep enough ledger history to cover any calendar month plus
            # operational lookback; pruning at 8 days would silently forget
            # earlier monthly spend.
            cutoff = utcnow() - timedelta(days=40)
            self.connector_runs = [item for item in self.connector_runs if item["startedAt"] >= cutoff]

    def connector_stats_24h(self, connector_id: str) -> tuple[int, int]:
        cutoff = utcnow() - timedelta(hours=24)
        rows = [item for item in self.connector_runs if item["connectorId"] == connector_id and item["startedAt"] >= cutoff]
        inserted = sum(int(item["inserted"]) for item in rows)
        latencies = sorted(int(item["latencyMs"]) for item in rows)
        p95_ms = latencies[max(0, (len(latencies) * 95 + 99) // 100 - 1)] if latencies else 0
        return inserted, (p95_ms + 59_999) // 60_000

    def list_connector_runs(self, since: datetime) -> list[dict[str, object]]:
        with self._lock:
            return [row.copy() for row in self.connector_runs if row["startedAt"] >= since]

    def persisted_content_duplicate_stats(self, since: datetime) -> tuple[int, int]:
        rows = [item for item in self.content_ingest_history if item["persistedAt"] >= since]
        keys = [(item["connectorId"], item["contentFingerprint"]) for item in rows]
        return len(rows), len(keys) - len(set(keys))

    def monthly_connector_spend(self, at: datetime | None = None, connector_ids: set[str] | None = None) -> float:
        current = at or utcnow()
        confirmed = sum(
            float(item.get("estimatedCostRmb", 0)) for item in self.connector_runs
            if item["startedAt"].year == current.year and item["startedAt"].month == current.month
            and (connector_ids is None or str(item["connectorId"]) in connector_ids)
        )
        reserved = sum(
            float(item.get("actualAmountRmb") if item["status"] == "reconciled_charged" else item["reservedAmountRmb"])
            for item in self.connector_budget_reservations.values()
            if item["status"] in {"reserved", "reconciliation_required", "reconciled_charged"}
            and item["createdAt"].year == current.year
            and item["createdAt"].month == current.month
            and (connector_ids is None or str(item["connectorId"]) in connector_ids)
        )
        return round(confirmed + reserved, 4)

    def get_connector(self, connector_id: str) -> ConnectorStatus | None:
        return self.connectors.get(connector_id)

    def get_connector_checkpoint(self, connector_id: str) -> dict[str, object]:
        with self._lock:
            return self.connector_checkpoints.get(connector_id, {}).copy()

    def save_connector_checkpoint(self, connector_id: str, payload: dict[str, object]) -> None:
        with self._lock:
            self.connector_checkpoints[connector_id] = payload.copy()

    def expire_raw_evidence(self, retention_days: dict[str, int], at: datetime) -> list[str]:
        with self._lock:
            candidates: dict[str, str] = {}
            for observation_id, observation in list(self.observations.items()):
                days = retention_days.get(observation.rights_policy_id)
                if days is None or observation.collected_at > at - timedelta(days=days):
                    continue
                if observation.raw_evidence_ref:
                    candidates[observation.raw_evidence_ref] = observation.rights_policy_id
                    self.observations[observation_id] = observation.model_copy(update={"raw_evidence_ref": ""})
                for snapshot_id, snapshot in list(self.metric_facts.items()):
                    if snapshot.subject_id == observation_id and snapshot.source_revision and snapshot.collected_at <= at - timedelta(days=days):
                        candidates[snapshot.source_revision] = observation.rights_policy_id
                        self.metric_facts[snapshot_id] = snapshot.model_copy(update={"source_revision": None})
            remaining = {item.raw_evidence_ref for item in self.observations.values() if item.raw_evidence_ref} | {item.source_revision for item in self.metric_facts.values() if item.source_revision}
            for reference, policy_id in candidates.items():
                if reference not in remaining:
                    self.raw_evidence_deletions.setdefault(reference, {
                        "rightsPolicyId": policy_id, "status": "pending", "attempts": 0,
                        "nextAttemptAt": at, "leaseUntil": None, "lastError": None,
                    })
            pending: list[str] = []
            for reference, item in self.raw_evidence_deletions.items():
                if item["status"] != "pending" or item["nextAttemptAt"] > at or (item["leaseUntil"] and item["leaseUntil"] > at):
                    continue
                if reference in remaining:
                    continue
                item["attempts"] = int(item["attempts"]) + 1
                item["leaseUntil"] = at + timedelta(minutes=5)
                pending.append(reference)
                if len(pending) >= 1000:
                    break
            return pending

    def raw_evidence_is_referenced(self, reference: str) -> bool:
        with self._lock:
            return any(item.raw_evidence_ref == reference for item in self.observations.values()) or any(
                item.source_revision == reference for item in self.metric_facts.values()
            )

    def release_raw_evidence_deletion(self, reference: str) -> None:
        with self._lock:
            item = self.raw_evidence_deletions.get(reference)
            if item and item["status"] == "pending":
                item["leaseUntil"] = None

    def confirm_raw_evidence_deletions(self, references: list[str]) -> None:
        with self._lock:
            for reference in references:
                if reference in self.raw_evidence_deletions:
                    self.raw_evidence_deletions[reference].update({"status": "completed", "deletedAt": utcnow(), "leaseUntil": None, "lastError": None})

    def fail_raw_evidence_deletion(self, reference: str, error: str, at: datetime | None = None) -> None:
        with self._lock:
            item = self.raw_evidence_deletions.get(reference)
            if not item:
                return
            current = at or utcnow()
            delay = min(3600, 60 * (2 ** min(int(item["attempts"]), 6)))
            item.update({"lastError": error[:1000], "leaseUntil": None, "nextAttemptAt": current + timedelta(seconds=delay)})

    def get_event(self, event_id: str) -> RadarEvent | None:
        return self.events.get(event_id)

    def list_events(self) -> list[RadarEvent]:
        return sorted((event for event in self.events.values() if not event.superseded_by), key=lambda event: (event.velocity, event.evidence_score), reverse=True)

    def apply_connector_coverage_penalty(self, platform: str, penalty: float, note: str) -> int:
        changed = 0
        with self._lock:
            for event_id, event in list(self.events.items()):
                if not any(item.lower() == platform.lower() for item in event.platforms):
                    continue
                score = max(0, event.evidence_score - penalty)
                tier = "low" if score < 45 else "medium" if score < 70 else "high"
                self.events[event_id] = event.model_copy(update={
                    "coverage": max(0, event.coverage - penalty), "evidence_score": score,
                    "evidence_strength": EvidenceStrength(tier), "uncertainty": min(100, event.uncertainty + penalty),
                    "coverage_note": f"{event.coverage_note}；{note}",
                })
                changed += 1
        return changed

    def add_feedback(self, request: FeedbackRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), operation=f"feedback.{request.action}", createdAt=utcnow())
        with self._lock:
            eligible_rows = [
                row for row in self.review_queue_entries
                if row["eventId"] == request.event_id and row["eligibleAt"] <= receipt.created_at
            ]
            latest_eligible = max(eligible_rows, key=lambda row: (row["eligibleAt"], row["eligibilityKey"]), default=None)
            if request.action in TRIAGE_ACTIONS and (
                latest_eligible is None or latest_eligible["eligibilityKey"] != request.queue_eligibility_key
            ):
                raise ValueError("queue eligibility is stale, unrelated, or not yet effective")
            alert = next((
                row for row in self.alert_deliveries
                if row["workspaceId"] == workspace_id and row["eventId"] == request.event_id
                and row.get("status", "delivered") == "delivered"
                and row.get("idempotencyKey") == request.alert_delivery_key
                and row["deliveredAt"] <= receipt.created_at
            ), None)
            if request.alert_delivery_key and alert is None:
                raise ValueError("alert delivery does not belong to this workspace/event or is not delivered")
            self.feedback[receipt.id] = {
                "id": receipt.id, "workspaceId": workspace_id, "actorId": actor_id,
                **request.model_dump(mode="json", by_alias=True), "createdAt": receipt.created_at,
                "queueEligibilityKey": request.queue_eligibility_key,
                "alertDeliveryKey": request.alert_delivery_key,
            }
            self.outbox.append({"id": str(uuid.uuid4()), "kind": "feedback.created", "aggregate_id": receipt.id, "created_at": utcnow()})
        return receipt

    def list_feedback(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        return [value.copy() for value in self.feedback.values() if value["workspaceId"] == workspace_id and value["createdAt"] >= since]

    def list_review_queue_entries(self, since: datetime) -> list[dict[str, object]]:
        with self._lock:
            return [row.copy() for row in self.review_queue_entries if row["eligibleAt"] >= since]

    def set_behavior_applicability(self, event_id: str, request: BehaviorApplicabilityRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), status="queued", operation="event.behavior_applicability", createdAt=utcnow())
        with self._lock:
            event = self.events[event_id]
            coverage = event.coverage
            if request.state == "not_applicable" and event.behavior_evidence_state != "not_applicable":
                coverage = min(100, coverage + 32.5)
            elif request.state == "missing" and event.behavior_evidence_state == "not_applicable":
                coverage = max(0, coverage - 32.5)
            self.events[event_id] = event.model_copy(update={
                "behavior_evidence_state": EvidenceState(request.state), "state": LifecycleState.INSUFFICIENT_DATA, "labels": [],
                "behavior": 0, "gap_residual": 0, "coverage": coverage,
                "driver": "行为适用性已人工修订，重新评分完成前不输出强结论。",
                "coverage_note": "行为适用性已人工修订并更新覆盖分母；重新评分已排队。",
            })
            for observation_id in self.event_observations.get(event_id, {}):
                state = self.observation_processing.get(observation_id)
                if state:
                    state["revision"] = int(state["revision"]) + 1
            self.outbox.append({
                "id": str(uuid.uuid4()), "kind": "event.rescore.requested", "aggregate_id": event_id,
                "created_at": utcnow(), "workspace_id": workspace_id, "actor_id": actor_id,
                "payload": {"state": request.state, "reason": request.reason},
            })
        return receipt

    def queue_cluster_edit(self, event_id: str, operation: str, request: ClusterEditRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        with self._lock:
            source = self.events.get(event_id)
            if source is None or source.superseded_by:
                raise ValueError("source event is missing or superseded")
            parents = [source]
            if operation == "merge":
                target = self.events.get(request.target_event_id or "")
                if target is None or target.superseded_by:
                    raise ValueError("target event is missing or superseded")
                parents.append(target)
            if operation == "split":
                assigned = set(self.event_observations.get(event_id, {}))
                requested = set(request.observation_ids)
                if not requested.issubset(assigned):
                    raise ValueError("split observations must belong to the source event")
                if requested == assigned:
                    raise ValueError("split must leave at least one observation in the source event")
            receipt = MutationReceipt(id=str(uuid.uuid4()), status="queued", operation=f"cluster.{operation}", createdAt=utcnow())
            self.cluster_edits[receipt.id] = {
                "id": receipt.id, "eventId": event_id, "operation": operation,
                "targetEventId": request.target_event_id, "observationIds": request.observation_ids,
                "reason": request.reason, "workspaceId": workspace_id, "actorId": actor_id,
                "status": "queued", "createdAt": receipt.created_at,
                "expectedVersions": {event.id: event.cluster_version for event in parents},
            }
            self.outbox.append({"id": str(uuid.uuid4()), "kind": "cluster.edit.requested", "aggregate_id": event_id, "operation_id": receipt.id, "workspace_id": workspace_id, "created_at": utcnow()})
        return receipt

    @staticmethod
    def _successor_event(template: RadarEvent, event_id: str, version: int, operation_id: str, operation: str, _member_count: int, parent_cluster_id: str) -> RadarEvent:
        now = utcnow()
        return template.model_copy(update={
            "id": event_id, "cluster_version": version, "parent_cluster_id": parent_cluster_id,
            "superseded_by": [], "merge_operation_id": operation_id if operation == "merge" else None,
            "split_operation_id": operation_id if operation == "split" else None, "effective_at": now,
            "state": LifecycleState.INSUFFICIENT_DATA, "labels": [], "attention": 0, "behavior": 0,
            "diversity": 0, "authority": 0, "coordination_risk": 0, "coverage": 0, "uncertainty": 100,
            "evidence_strength": EvidenceStrength.LOW, "discussion_evidence_state": EvidenceState.MISSING,
            "behavior_evidence_state": EvidenceState.MISSING, "evidence_score": 0, "velocity": 0, "gap_residual": 0,
            "updated_at": now, "independent_sources": 0, "platforms": [], "signal_families": [],
            # Membership is not evidence strength. In particular, discovery-only
            # members must never inflate this public/alert-facing field. A fresh
            # trusted scoring pass rebuilds the count from provenance-filtered
            # observations.
            "evidence_count": 0, "driver": "聚类成员已修订，等待新版本重新评分。",
            "coverage_note": "合并/拆分后的新事件尚未完成评分，不输出强结论。", "timeline": [], "evidence": [],
        })

    def _inherit_watchlists(self, parent_ids: list[str], child_ids: list[str], workspace_id: str) -> list[str]:
        inherited: list[str] = []
        parent_watches = [value for value in self.watchlists.values() if value[0] == workspace_id and value[2].event_id in parent_ids]
        for inherited_workspace_id, actor_id, request, created_at in parent_watches:
            for child_id in child_ids:
                if any(value[0] == inherited_workspace_id and value[2].event_id == child_id for value in self.watchlists.values()):
                    continue
                watch_id = str(uuid.uuid4())
                self.watchlists[watch_id] = (inherited_workspace_id, actor_id, request.model_copy(update={"event_id": child_id}), created_at)
                inherited.append(watch_id)
        return inherited

    def execute_cluster_edit(self, operation_id: str, workspace_id: str | None = None) -> dict[str, object]:
        with self._lock:
            item = self.cluster_edits.get(operation_id)
            if item is None or (workspace_id is not None and item["workspaceId"] != workspace_id):
                raise ValueError("cluster operation not found")
            if item["status"] != "queued":
                raise ValueError("cluster operation is not queued")
            parent_ids = [str(item["eventId"])] + ([str(item["targetEventId"])] if item["operation"] == "merge" else [])
            parents = [self.events[event_id] for event_id in parent_ids]
            expected = item["expectedVersions"]
            if any(parent.superseded_by or parent.cluster_version != expected[parent.id] for parent in parents):
                item.update({"status": "failed", "error": "cluster version conflict", "completedAt": utcnow()})
                raise ValueError("cluster version conflict")
            assignments = {event_id: dict(self.event_observations.get(event_id, {})) for event_id in parent_ids}
            reverse_assignments = {observation_id: event_id for event_id, members in assignments.items() for observation_id in members}
            version = max(parent.cluster_version for parent in parents) + 1
            child_ids: list[str] = []
            child_members: list[dict[str, float]] = []
            if item["operation"] == "merge":
                child_ids = [f"evt-{uuid.uuid4()}"]
                child_members = [{observation_id: score for members in assignments.values() for observation_id, score in members.items()}]
            else:
                selected = set(item["observationIds"])
                source_members = assignments[parent_ids[0]]
                child_ids = [f"evt-{uuid.uuid4()}", f"evt-{uuid.uuid4()}"]
                child_members = [
                    {observation_id: score for observation_id, score in source_members.items() if observation_id in selected},
                    {observation_id: score for observation_id, score in source_members.items() if observation_id not in selected},
                ]
            now = utcnow()
            for child_id, members in zip(child_ids, child_members, strict=True):
                self.events[child_id] = self._successor_event(parents[0], child_id, version, operation_id, str(item["operation"]), len(members), parent_ids[0])
                self.event_observations[child_id] = members
                for observation_id in members:
                    state = self.observation_processing.get(observation_id)
                    if state:
                        state["revision"] = int(state["revision"]) + 1
                self.outbox.append({"id": str(uuid.uuid4()), "kind": "event.rescore.requested", "aggregate_id": child_id, "created_at": now, "operation_id": operation_id})
            for parent in parents:
                self.events[parent.id] = parent.model_copy(update={"superseded_by": child_ids})
                self.event_observations.pop(parent.id, None)
            for parent_id in parent_ids:
                for child_id in child_ids:
                    self.lineage_edges.append({"operationId": operation_id, "parentEventId": parent_id, "childEventId": child_id, "effectiveAt": now, "revertedAt": None})
            inherited_watch_ids = self._inherit_watchlists(parent_ids, child_ids, str(item["workspaceId"]))
            item.update({
                "status": "completed", "completedAt": now, "resultEventIds": child_ids,
                "resultVersions": {child_id: version for child_id in child_ids},
                "reversePayload": {"assignments": reverse_assignments, "parentEventIds": parent_ids, "inheritedWatchIds": inherited_watch_ids},
            })
            return {key: value for key, value in item.items() if key not in {"reason", "workspaceId", "actorId", "reversePayload"}}

    def revert_cluster_edit(self, operation_id: str, workspace_id: str) -> dict[str, object]:
        with self._lock:
            item = self.cluster_edits.get(operation_id)
            if item is None or item["workspaceId"] != workspace_id:
                raise ValueError("cluster operation not found")
            if item["status"] != "completed":
                raise ValueError("only a completed cluster operation can be reverted")
            child_ids = list(item["resultEventIds"])
            expected = item["resultVersions"]
            if any(self.events[child_id].superseded_by or self.events[child_id].cluster_version != expected[child_id] for child_id in child_ids):
                raise ValueError("cluster version conflict")
            reverse = item["reversePayload"]
            parent_ids = list(reverse["parentEventIds"])
            restored: dict[str, dict[str, float]] = {parent_id: {} for parent_id in parent_ids}
            all_child_members = {observation_id: score for child_id in child_ids for observation_id, score in self.event_observations.get(child_id, {}).items()}
            for observation_id, parent_id in reverse["assignments"].items():
                if observation_id in all_child_members:
                    restored[parent_id][observation_id] = all_child_members[observation_id]
            now = utcnow()
            for parent_id, members in restored.items():
                parent = self.events[parent_id]
                self.events[parent_id] = parent.model_copy(update={
                    "superseded_by": [],
                    # Revert is a new topology mutation. Incrementing prevents a
                    # command queued against the pre-merge parent from executing.
                    "cluster_version": parent.cluster_version + 1,
                })
                self.event_observations[parent_id] = members
                self.outbox.append({"id": str(uuid.uuid4()), "kind": "event.rescore.requested", "aggregate_id": parent_id, "created_at": now, "operation_id": operation_id})
            for child_id in child_ids:
                child = self.events[child_id]
                self.events[child_id] = child.model_copy(update={"superseded_by": parent_ids})
                self.event_observations.pop(child_id, None)
            for watch_id in reverse["inheritedWatchIds"]:
                self.watchlists.pop(watch_id, None)
            for edge in self.lineage_edges:
                if edge["operationId"] == operation_id:
                    edge["revertedAt"] = now
            item.update({"status": "reverted", "revertedAt": now})
            return {key: value for key, value in item.items() if key not in {"reason", "workspaceId", "actorId", "reversePayload"}}

    def execute_pending_cluster_edits(self, limit: int = 20) -> int:
        pending = [item_id for item_id, item in self.cluster_edits.items() if item["status"] == "queued"][:limit]
        completed = 0
        for operation_id in pending:
            try:
                self.execute_cluster_edit(operation_id)
            except ValueError:
                continue
            completed += 1
        return completed

    def get_event_lineage(self, event_id: str) -> dict[str, list[dict[str, object]]]:
        return {
            "parents": [edge.copy() for edge in self.lineage_edges if edge["childEventId"] == event_id],
            "children": [edge.copy() for edge in self.lineage_edges if edge["parentEventId"] == event_id],
        }

    def list_cluster_edits(self, event_id: str, workspace_id: str) -> list[dict[str, object]]:
        return [
            {key: value for key, value in item.items() if key not in {"reason", "workspaceId", "actorId", "reversePayload"}}
            for item in self.cluster_edits.values()
            if item["workspaceId"] == workspace_id and (
                item["eventId"] == event_id
                or item.get("targetEventId") == event_id
                or event_id in item.get("resultEventIds", [])
            )
        ]

    def add_alert(self, request: AlertRuleRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), status="completed", operation="alert_rule.create", createdAt=utcnow())
        self.alerts[receipt.id] = (workspace_id, actor_id, request)
        return receipt

    def list_alert_rules(self, workspace_id: str) -> list[tuple[str, AlertRuleRequest]]:
        return [(rule_id, value[2]) for rule_id, value in self.alerts.items() if value[0] == workspace_id]

    def list_alert_deliveries(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        cutoff = utcnow() - timedelta(minutes=15)
        return [
            item.copy()
            for item in self.alert_deliveries
            if item["workspaceId"] == workspace_id
            and item["deliveredAt"] >= since
            and (
                item.get("status", "delivered") in {"delivered", "aborted"}
                or (item.get("status") == "reserved" and item["deliveredAt"] >= cutoff)
            )
        ]

    def last_alert_delivery(self, workspace_id: str, event_id: str) -> dict[str, object] | None:
        rows = [item for item in self.alert_deliveries if item["workspaceId"] == workspace_id and item["eventId"] == event_id and item.get("status", "delivered") == "delivered"]
        return max(rows, key=lambda item: item["deliveredAt"]).copy() if rows else None

    def get_alert_delivery_by_idempotency(self, workspace_id: str, idempotency_key: str) -> dict[str, object] | None:
        with self._lock:
            row = next((item for item in self.alert_deliveries if item["workspaceId"] == workspace_id and item.get("idempotencyKey") == idempotency_key), None)
            return row.copy() if row else None

    def list_alert_reservations_for_message(self, workspace_id: str, event_id: str, message_token: str) -> list[dict[str, object]]:
        suffix = f":{message_token}"
        with self._lock:
            return [item.copy() for item in self.alert_deliveries if (
                item["workspaceId"] == workspace_id
                and item["eventId"] == event_id
                and item.get("status") == "reserved"
                and str(item.get("idempotencyKey", "")).endswith(suffix)
            )]

    def record_alert_delivery(self, item: dict[str, object]) -> None:
        with self._lock:
            self.alert_deliveries.append(item.copy())

    def reserve_alert_delivery(self, item: dict[str, object], *, workspace_limit: int = 10, domain_limit: int = 3, cooldown: timedelta = timedelta(hours=4), allow_cooldown_bypass: bool = False) -> bool:
        with self._lock:
            now = item["deliveredAt"]
            key = str(item["idempotencyKey"])
            if any(row.get("idempotencyKey") == key for row in self.alert_deliveries):
                return False
            active_cutoff = now - timedelta(minutes=15)
            today = [row for row in self.alert_deliveries if (
                row["workspaceId"] == item["workspaceId"]
                and row["deliveredAt"].date() == now.date()
                and (row.get("status", "delivered") == "delivered" or (row.get("status") == "reserved" and row["deliveredAt"] >= active_cutoff))
            )]
            if len(today) >= workspace_limit or sum(row["domain"] == item["domain"] for row in today) >= domain_limit:
                return False
            recent = [row for row in self.alert_deliveries if (
                row["workspaceId"] == item["workspaceId"]
                and row["eventId"] == item["eventId"]
                and row["deliveredAt"] >= now - cooldown
                and (row.get("status", "delivered") == "delivered" or (row.get("status") == "reserved" and row["deliveredAt"] >= active_cutoff))
            )]
            if recent and not allow_cooldown_bypass:
                return False
            reserved = item.copy()
            reserved["status"] = "reserved"
            self.alert_deliveries.append(reserved)
            return True

    def confirm_alert_delivery(self, idempotency_key: str, workspace_id: str | None = None) -> bool:
        with self._lock:
            for item in self.alert_deliveries:
                if item.get("idempotencyKey") == idempotency_key and (workspace_id is None or item["workspaceId"] == workspace_id):
                    if item.get("status") == "reserved":
                        item["status"] = "delivered"
                    if item.get("status") == "delivered":
                        return True
            raise RuntimeError(f"alert reservation {idempotency_key} is not confirmable")

    def abort_alert_reservation(self, idempotency_key: str, reason: str, workspace_id: str | None = None) -> bool:
        if not reason.strip():
            raise ValueError("alert reservation terminal reason is required")
        with self._lock:
            for item in self.alert_deliveries:
                if item.get("idempotencyKey") == idempotency_key and (workspace_id is None or item["workspaceId"] == workspace_id):
                    if item.get("status") == "reserved":
                        item["status"] = "aborted"
                        item["terminalReason"] = reason[:1000]
                    if item.get("status") == "aborted":
                        return True
            raise RuntimeError(f"alert reservation {idempotency_key} is not abortable")

    def release_alert_reservation(self, idempotency_key: str, workspace_id: str | None = None) -> None:
        with self._lock:
            self.alert_deliveries = [item for item in self.alert_deliveries if not (item.get("idempotencyKey") == idempotency_key and item.get("status") == "reserved")]

    def add_watchlist(self, request: WatchlistRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        with self._lock:
            existing = next(((item_id, value) for item_id, value in self.watchlists.items() if value[0] == workspace_id and value[2].event_id == request.event_id), None)
            if existing:
                item_id, value = existing
                self.watchlists[item_id] = (workspace_id, actor_id, request, value[3])
                return MutationReceipt(id=item_id, status="completed", operation="watchlist.update", createdAt=value[3])
            created_at = utcnow()
            receipt = MutationReceipt(id=str(uuid.uuid4()), status="completed", operation="watchlist.create", createdAt=created_at)
            self.watchlists[receipt.id] = (workspace_id, actor_id, request, created_at)
            return receipt

    def list_watchlists(self, workspace_id: str) -> list[WatchlistItem]:
        with self._lock:
            # Cluster topology is global while watches are workspace scoped.
            # Resolve every workspace lazily to active leaf events so a merge
            # performed in another workspace cannot leave stale visible watches.
            pending = [value for value in self.watchlists.values() if value[0] == workspace_id]
            for inherited_workspace_id, actor_id, request, created_at in pending:
                queue = list(self.events.get(request.event_id).superseded_by) if self.events.get(request.event_id) else []
                visited: set[str] = set()
                while queue:
                    successor_id = queue.pop(0)
                    if successor_id in visited:
                        continue
                    visited.add(successor_id)
                    successor = self.events.get(successor_id)
                    if successor and successor.superseded_by:
                        queue.extend(successor.superseded_by)
                        continue
                    if successor and not any(value[0] == workspace_id and value[2].event_id == successor_id for value in self.watchlists.values()):
                        watch_id = str(uuid.uuid4())
                        self.watchlists[watch_id] = (
                            inherited_workspace_id,
                            actor_id,
                            request.model_copy(update={"event_id": successor_id}),
                            created_at,
                        )
            return sorted(
                [
                    WatchlistItem(id=item_id, eventId=value[2].event_id, note=value[2].note, createdAt=value[3])
                    for item_id, value in self.watchlists.items()
                    if value[0] == workspace_id
                    and self.events.get(value[2].event_id) is not None
                    and not self.events[value[2].event_id].superseded_by
                ],
                key=lambda item: item.created_at,
                reverse=True,
            )

    def remove_watchlist(self, event_id: str, workspace_id: str) -> MutationReceipt | None:
        with self._lock:
            existing_id = next((item_id for item_id, value in self.watchlists.items() if value[0] == workspace_id and value[2].event_id == event_id), None)
            if existing_id is None:
                return None
            del self.watchlists[existing_id]
            return MutationReceipt(id=existing_id, status="completed", operation="watchlist.delete", createdAt=utcnow())

    def record_product_interaction(
        self,
        request: ProductInteractionRequest,
        workspace_id: str,
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> MutationReceipt:
        with self._lock:
            existing = next(
                (row for row in self.product_interactions if row["workspaceId"] == workspace_id and row["idempotencyKey"] == request.idempotency_key),
                None,
            )
            if existing:
                return MutationReceipt(
                    id=str(existing["id"]), status="completed", operation=f"interaction.{existing['kind']}", createdAt=existing["occurredAt"],
                )
            if request.kind == "metric_exclusion_recorded":
                incident_id = str(request.metadata.get("incidentId") or "")
                incident = next(
                    (
                        row for row in self.metric_incidents
                        if row["workspaceId"] == workspace_id and row["id"] == incident_id
                    ),
                    None,
                )
                if (
                    incident is None
                    or incident["eventId"] != request.event_id
                    or incident["targetKey"] != request.metadata.get("targetKey")
                    or incident["canonicalKey"] != request.metadata.get("canonicalKey")
                ):
                    raise ValueError("metric exclusion must reference a matching immutable incident fact")
                request = request.model_copy(update={
                    "metadata": {**request.metadata, "incidentFactDigest": incident["factDigest"]},
                })
            receipt = MutationReceipt(
                id=str(uuid.uuid4()), status="completed", operation=f"interaction.{request.kind}", createdAt=occurred_at or utcnow(),
            )
            self.product_interactions.append({
                "id": receipt.id, "workspaceId": workspace_id, "actorId": actor_id,
                **request.model_dump(mode="json", by_alias=True), "occurredAt": receipt.created_at,
            })
        return receipt

    def create_metric_incident(
        self,
        request: MetricIncidentRequest,
        workspace_id: str,
        actor_id: str,
    ) -> MutationReceipt:
        with self._lock:
            rows = {
                str(row.get("idempotencyKey")): row
                for row in self.alert_deliveries
                if row.get("workspaceId") == workspace_id and row.get("status", "delivered") == "delivered"
            }
            target = rows.get(request.target_key)
            canonical = rows.get(request.canonical_key)
            if (
                target is None or canonical is None
                or target.get("eventId") != request.event_id
                or canonical.get("eventId") != request.event_id
                or target.get("deliveredAt") <= canonical.get("deliveredAt")
                or target.get("deliveredAt") > canonical.get("deliveredAt") + timedelta(minutes=15)
            ):
                raise ValueError("incident requires two matching delivered alerts in canonical order within 15 minutes")
            created_at = utcnow()
            incident_id = str(uuid.uuid4())
            fact = {
                "id": incident_id, "workspaceId": workspace_id, "actorId": actor_id,
                **request.model_dump(mode="json", by_alias=True), "createdAt": created_at,
            }
            fact["factDigest"] = "sha256:" + hashlib.sha256(
                json.dumps(fact, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode()
            ).hexdigest()
            self.metric_incidents.append(fact)
            return MutationReceipt(
                id=incident_id, status="completed", operation="metric_incident.create", createdAt=created_at,
            )

    def list_metric_incidents(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self._lock:
            return [
                row.copy() for row in self.metric_incidents
                if row["workspaceId"] == workspace_id and row["createdAt"] >= since
            ]

    def list_product_interactions(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self._lock:
            return [row.copy() for row in self.product_interactions if row["workspaceId"] == workspace_id and row["occurredAt"] >= since]

    def purge_source(self, source_id: str) -> int:
        """Delete source observations and evidence references, then emit a propagation event."""
        with self._lock:
            observation_ids = [item_id for item_id, item in self.observations.items() if item.source_id == source_id]
            deleting = set(observation_ids)
            affected_event_ids = {
                event_id for event_id, members in self.event_observations.items()
                if any(item_id in members for item_id in deleting)
            } | {
                event_id for event_id, event in self.events.items()
                if any(item.source == source_id for item in event.evidence)
            }
            candidate_refs = {
                self.observations[item_id].raw_evidence_ref for item_id in observation_ids
            } | {
                item.source_revision for item in self.metric_facts.values()
                if item.subject_id in deleting and item.source_revision
            }
            retained_refs = {
                item.raw_evidence_ref for item_id, item in self.observations.items() if item_id not in deleting
            } | {
                item.source_revision for item in self.metric_facts.values()
                if item.subject_id not in deleting and item.source_revision
            }
            raw_refs = sorted(ref for ref in candidate_refs - retained_refs if ref)
            for item_id in observation_ids:
                del self.observations[item_id]
                self.observation_processing.pop(item_id, None)
            self.source_profiles.pop(source_id, None)
            self.source_content_fingerprints = {
                key for key in self.source_content_fingerprints if key[0] != source_id
            }
            self.metric_facts = {key: value for key, value in self.metric_facts.items() if value.subject_id not in deleting}
            for event_id, members in list(self.event_observations.items()):
                for item_id in deleting:
                    members.pop(item_id, None)
                if not members:
                    self.event_observations.pop(event_id, None)
            for event_id in affected_event_ids:
                event = self.events.get(event_id)
                if event is None:
                    continue
                filtered = [item for item in event.evidence if item.source != source_id]
                remaining_ids = list(self.event_observations.get(event_id, {}))
                remaining = [self.observations[item_id] for item_id in remaining_ids if item_id in self.observations]
                if not remaining and not filtered:
                    # No retained source can justify even a tombstoned event
                    # shell. Removing it also removes every derived title/body.
                    self.events.pop(event_id, None)
                    self.event_observations.pop(event_id, None)
                    continue
                if remaining:
                    title_source = min(remaining, key=lambda item: (item.published_at, item.collected_at))
                    rebuilt_title = title_source.title or title_source.text[:120] or "未命名 AI 事件"
                    for item_id in remaining_ids:
                        state = self.observation_processing.get(item_id)
                        if state:
                            state["revision"] = int(state["revision"]) + 1
                else:
                    rebuilt_title = filtered[0].title
                self.events[event_id] = event.model_copy(update={
                    "title": rebuilt_title, "title_en": rebuilt_title,
                    "evidence": filtered, "state": LifecycleState.INSUFFICIENT_DATA, "labels": [],
                    "attention": 0, "behavior": 0, "diversity": 0, "authority": 0,
                    "coordination_risk": 0, "coverage": 0, "velocity": 0, "gap_residual": 0,
                    "independent_sources": 0, "platforms": [],
                    "signal_families": [], "evidence_count": 0,
                    "evidence_strength": EvidenceStrength.LOW, "evidence_score": 0, "uncertainty": 100,
                    "discussion_evidence_state": EvidenceState.MISSING,
                    "behavior_evidence_state": EvidenceState.MISSING,
                    "driver": "来源删除后派生结论已失效，等待重新评分。",
                    "coverage_note": "删除传播已清除成员和指标事实；当前不得输出强结论。",
                    "updated_at": utcnow(),
                })
            self.score_runs = [score for score in self.score_runs if score.event_id not in affected_event_ids]
            self.delete_event_embeddings(list(affected_event_ids))
            cache_tags = ["radar", "events", f"source:{source_id}"] + [f"event:{event_id}" for event_id in sorted(affected_event_ids)]
            self.outbox.append({
                "id": str(uuid.uuid4()), "kind": "source.erased", "aggregate_id": source_id,
                "created_at": utcnow(), "raw_evidence_refs": raw_refs,
                "cache_tags": cache_tags,
                "affected_event_ids": sorted(affected_event_ids),
                "payload": {"rawEvidenceRefs": raw_refs, "cacheTags": cache_tags, "affectedEventIds": sorted(affected_event_ids)},
            })
            return len(observation_ids)


class PostgresRepository:
    """PostgreSQL repository. psycopg is imported lazily so scoring tests stay lightweight."""

    _pools: dict[str, object] = {}
    _pools_lock = threading.Lock()

    def __init__(self, dsn: str, deletion_dsn: str | None = None) -> None:
        self.dsn = dsn
        self.deletion_dsn = deletion_dsn or os.getenv("DELETION_DATABASE_URL")

    @classmethod
    def _pool_for(cls, dsn: str) -> object:
        pool = cls._pools.get(dsn)
        if pool is not None:
            return pool
        with cls._pools_lock:
            pool = cls._pools.get(dsn)
            if pool is not None:
                return pool
            from psycopg_pool import ConnectionPool

            min_size = int(os.getenv("POSTGRES_POOL_MIN_SIZE", "1"))
            max_size = int(os.getenv("POSTGRES_POOL_MAX_SIZE", "4"))
            timeout = float(os.getenv("POSTGRES_POOL_TIMEOUT_SECONDS", "30"))
            if min_size < 0 or max_size < 1 or min_size > max_size:
                raise RuntimeError(
                    "PostgreSQL pool sizes require 0 <= POSTGRES_POOL_MIN_SIZE <= "
                    "POSTGRES_POOL_MAX_SIZE and POSTGRES_POOL_MAX_SIZE >= 1",
                )
            if timeout <= 0:
                raise RuntimeError("POSTGRES_POOL_TIMEOUT_SECONDS must be positive")
            pool = ConnectionPool(
                conninfo=dsn,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout,
                open=False,
                name="ai-hot-postgres",
            )
            pool.open()
            cls._pools[dsn] = pool
            return pool

    @contextmanager
    def connection(self) -> Iterator[object]:
        pool = self._pool_for(self.dsn)
        with pool.connection() as connection:
            yield connection

    @contextmanager
    def deletion_connection(self) -> Iterator[object]:
        import psycopg

        if not self.deletion_dsn:
            raise RuntimeError("source erasure requires the isolated deletion-worker database role")
        with psycopg.connect(self.deletion_dsn) as connection:
            yield connection

    def resolve_workspace_membership(self, subject: str, workspace_id: str) -> str | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT role FROM workspace_memberships
                    WHERE workspace_id=%s AND subject=%s AND status='active'""",
                    (workspace_id, subject),
                )
                row = cursor.fetchone()
        return str(row[0]) if row else None

    def is_token_revoked(self, jti: str) -> bool:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT EXISTS (SELECT 1 FROM jwt_revocations WHERE jti=%s AND expires_at>clock_timestamp())",
                    (jti,),
                )
                row = cursor.fetchone()
        return bool(row and row[0])

    def runtime_attestation(self) -> dict[str, object]:
        required_rls_tables = {
            "feedback", "alert_rules", "alert_deliveries", "watchlists",
            "product_interactions", "cluster_edit_requests", "metric_incidents",
            "workspace_memberships",
        }
        deletion_role_ready = False
        deletion_role_name: str | None = None
        if self.deletion_dsn:
            try:
                with self.deletion_connection() as deletion_connection:
                    with deletion_connection.cursor() as deletion_cursor:
                        deletion_cursor.execute(
                            """SELECT current_user,roles.rolsuper,roles.rolcreatedb,
                            roles.rolcreaterole,roles.rolinherit,roles.rolreplication,roles.rolbypassrls,
                            (SELECT count(*) FROM pg_auth_members membership
                             WHERE membership.roleid=roles.oid OR membership.member=roles.oid),
                            has_function_privilege(current_user,'erase_source_score_history(text)','EXECUTE'),
                            has_table_privilege(current_user,'events','DELETE'),
                            has_table_privilege(current_user,'score_history_erasure_audit','INSERT')
                            FROM pg_roles roles WHERE roles.rolname=current_user"""
                        )
                        deletion_row = deletion_cursor.fetchone()
                deletion_role_name = str(deletion_row[0]) if deletion_row else None
                deletion_role_ready = bool(
                    deletion_row and deletion_row[0] == "radar_deletion_worker"
                    and all(value is False for value in deletion_row[1:7])
                    and deletion_row[7] == 0
                    and deletion_row[8] is True and deletion_row[9] is False
                    and deletion_row[10] is False
                )
            except Exception:
                deletion_role_ready = False
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT clock_timestamp(),current_user,roles.rolsuper,roles.rolcreatedb,
                    roles.rolcreaterole,roles.rolinherit,roles.rolreplication,roles.rolbypassrls,
                    (SELECT count(*) FROM pg_auth_members membership
                     WHERE membership.roleid=roles.oid OR membership.member=roles.oid)
                    FROM pg_roles roles WHERE roles.rolname=current_user"""
                )
                (
                    database_time, database_user, role_superuser, role_create_db,
                    role_create_role, role_inherit, role_replication, role_bypass_rls,
                    role_memberships,
                ) = cursor.fetchone()
                cursor.execute("SELECT value FROM schema_attestations WHERE key='migration_version'")
                migration_row = cursor.fetchone()
                cursor.execute(
                    """SELECT relation.relname,relation.relrowsecurity,relation.relforcerowsecurity
                    FROM pg_class relation
                    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                    WHERE namespace.nspname='public' AND relation.relname=ANY(%s)""",
                    (list(required_rls_tables),),
                )
                rls_rows = {str(row[0]): bool(row[1] and row[2]) for row in cursor.fetchall()}
                cursor.execute(
                    """SELECT trigger.tgname,namespace.nspname,relation.relname,
                    procedure_namespace.nspname,procedure.proname,
                    trigger.tgenabled,
                    (trigger.tgtype & 1)<>0,(trigger.tgtype & 2)<>0,
                    (trigger.tgtype & 8)<>0,(trigger.tgtype & 16)<>0
                    FROM pg_trigger trigger
                    JOIN pg_class relation ON relation.oid=trigger.tgrelid
                    JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
                    JOIN pg_proc procedure ON procedure.oid=trigger.tgfoid
                    JOIN pg_namespace procedure_namespace ON procedure_namespace.oid=procedure.pronamespace
                    WHERE NOT trigger.tgisinternal AND trigger.tgname=ANY(%s)""",
                    (list(AUDIT_TRIGGER_SPECS),),
                )
                audit_trigger_rows = cursor.fetchall()
                cursor.execute(
                    """SELECT
                    has_table_privilege(current_user,'schema_attestations','SELECT'),
                    has_table_privilege(current_user,'schema_attestations','INSERT'),
                    has_table_privilege(current_user,'schema_attestations','UPDATE'),
                    has_table_privilege(current_user,'schema_attestations','DELETE')"""
                )
                marker_privileges = cursor.fetchone()
                cursor.execute(
                    """SELECT component_id,instance_id,last_seen_at,details
                    FROM runtime_component_heartbeats ORDER BY component_id"""
                )
                runtime_components = [
                    {"componentId": row[0], "instanceId": row[1], "lastSeenAt": row[2], "details": row[3]}
                    for row in cursor.fetchall()
                ]
                cursor.execute(
                    """SELECT performed_at,backup_reference,restored_instance_id,verification_digest,
                      measured_rpo_seconds,measured_rto_seconds,status,operator_subject
                    FROM disaster_recovery_attestations ORDER BY performed_at DESC,created_at DESC LIMIT 1"""
                )
                recovery_row = cursor.fetchone()
                recovery_attestation = ({
                    "performedAt": recovery_row[0], "backupReference": recovery_row[1],
                    "restoredInstanceId": recovery_row[2], "verificationDigest": recovery_row[3],
                    "measuredRpoSeconds": recovery_row[4], "measuredRtoSeconds": recovery_row[5],
                    "status": recovery_row[6], "operatorSubject": recovery_row[7],
                } if recovery_row else None)
        return {
            "storageBackend": "postgresql",
            "rlsVerified": set(rls_rows) == required_rls_tables and all(rls_rows.values()),
            "migrationVersion": str(migration_row[0]) if migration_row else None,
            "auditTriggersVerified": audit_trigger_specs_verified(audit_trigger_rows),
            "migrationMarkerReadOnly": bool(marker_privileges[0] and not any(marker_privileges[1:])),
            "instanceId": os.getenv("RADAR_INSTANCE_ID", ""),
            "databaseUser": str(database_user),
            "databaseRoleSuperuser": bool(role_superuser),
            "databaseRoleBypassRls": bool(role_bypass_rls),
            "databaseRoleLeastPrivilege": not any((
                role_superuser, role_create_db, role_create_role, role_inherit,
                role_replication, role_bypass_rls, role_memberships,
            )),
            "deletionRole": deletion_role_name,
            "deletionRoleReady": deletion_role_ready,
            "databaseClockSkewSeconds": abs((utcnow() - database_time).total_seconds()),
            "runtimeComponents": runtime_components,
            "disasterRecoveryAttestation": recovery_attestation,
        }

    def heartbeat_runtime_component(self, component_id: str, instance_id: str, details: dict[str, object] | None = None) -> None:
        if not component_id or not instance_id:
            raise ValueError("runtime component and instance IDs are required")
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO runtime_component_heartbeats (component_id,instance_id,last_seen_at,details)
                    VALUES (%s,%s,clock_timestamp(),%s::jsonb)
                    ON CONFLICT (component_id) DO UPDATE SET instance_id=excluded.instance_id,
                      last_seen_at=excluded.last_seen_at,details=excluded.details""",
                    (component_id, instance_id, json.dumps(details or {}, ensure_ascii=False)),
                )
            connection.commit()

    def save_observation_with_outbox(self, observation: Observation) -> bool:
        content, snapshots = split_observation(observation)
        if observation.provenance_level == "unverified_discovery":
            snapshots = []
        digest = content.content_hash
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO observations (id,schema_version,connector,platform,external_id,source_id,account_id,entity_id,published_at,available_at,availability_basis,collected_at,
                      language,title,body,url,normalized_url,content_fingerprint,metrics,raw_evidence_ref,parser_version,rights_policy_id,provenance_level,deletion_state,signal_family)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'{}'::jsonb,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id) DO NOTHING RETURNING id
                    """,
                    (content.id, content.schema_version, content.connector, content.platform, content.external_id, observation.source_id,
                     content.account_id, content.entity_id, content.published_at, observation.available_at, observation.availability_basis,
                     content.collected_at, content.language, content.title,
                     content.text_excerpt, observation.url, content.canonical_url, digest, content.raw_ref or "", content.parser_version,
                     content.rights_policy_id, content.provenance_level, content.deletion_state, observation.signal_family),
                )
                content_inserted = cursor.fetchone() is not None
                provenance_upgraded = False
                previous_raw_ref: str | None = None
                if not content_inserted and observation.provenance_level != "unverified_discovery":
                    cursor.execute(
                        """SELECT raw_evidence_ref FROM observations
                        WHERE id=%s AND provenance_level='unverified_discovery' FOR UPDATE""",
                        (observation.id,),
                    )
                    previous_row = cursor.fetchone()
                    if previous_row is not None:
                        previous_raw_ref = str(previous_row[0])
                        cursor.execute(
                            """UPDATE observations SET
                              schema_version=%s,connector=%s,platform=%s,external_id=%s,source_id=%s,
                              account_id=%s,entity_id=%s,published_at=%s,available_at=%s,availability_basis=%s,collected_at=%s,language=%s,
                              title=%s,body=%s,url=%s,normalized_url=%s,content_fingerprint=%s,
                              raw_evidence_ref=%s,parser_version=%s,rights_policy_id=%s,
                              provenance_level=%s,deletion_state=%s,signal_family=%s
                            WHERE id=%s AND provenance_level='unverified_discovery'""",
                            (
                                content.schema_version, content.connector, content.platform, content.external_id,
                                observation.source_id, content.account_id, content.entity_id, content.published_at,
                                observation.available_at, observation.availability_basis, content.collected_at, content.language, content.title, content.text_excerpt,
                                observation.url, content.canonical_url, digest, content.raw_ref or "",
                                content.parser_version, content.rights_policy_id, content.provenance_level,
                                content.deletion_state, observation.signal_family, observation.id,
                            ),
                        )
                        provenance_upgraded = cursor.rowcount == 1
                    if provenance_upgraded:
                        cursor.execute(
                            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                            (f"source-valid-observation:{observation.source_id}:{digest}",),
                        )
                        cursor.execute(
                            """UPDATE sources SET valid_observations=valid_observations+1,
                            last_observed_at=GREATEST(last_observed_at,%s),
                            discovery_reasons=ARRAY(
                              SELECT DISTINCT reason FROM unnest(discovery_reasons||ARRAY['provider_verified_upgrade']) reason
                            ),updated_at=clock_timestamp()
                            WHERE id=%s AND NOT EXISTS (
                              SELECT 1 FROM observations
                              WHERE source_id=%s AND content_fingerprint=%s AND id<>%s
                                AND provenance_level<>'unverified_discovery'
                            )""",
                            (
                                observation.collected_at, observation.source_id, observation.source_id,
                                digest, observation.id,
                            ),
                        )
                        if cursor.rowcount == 0:
                            cursor.execute(
                                """UPDATE sources SET last_observed_at=GREATEST(last_observed_at,%s),
                                discovery_reasons=ARRAY(
                                  SELECT DISTINCT reason FROM unnest(discovery_reasons||ARRAY['duplicate_content_observation']) reason
                                ),updated_at=clock_timestamp() WHERE id=%s""",
                                (observation.collected_at, observation.source_id),
                            )
                        if previous_raw_ref and previous_raw_ref != observation.raw_evidence_ref:
                            cursor.execute(
                                """INSERT INTO raw_evidence_deletions (reference,rights_policy_id)
                                SELECT %s,%s WHERE NOT EXISTS (
                                  SELECT 1 FROM observations WHERE raw_evidence_ref=%s
                                ) AND NOT EXISTS (
                                  SELECT 1 FROM metric_snapshots WHERE source_revision=%s
                                ) ON CONFLICT (reference) DO NOTHING""",
                                (
                                    previous_raw_ref, observation.rights_policy_id,
                                    previous_raw_ref, previous_raw_ref,
                                ),
                            )
                        cursor.execute(
                            """INSERT INTO content_ingest_history
                            (observation_id,connector_id,content_fingerprint,persisted_at)
                            VALUES (%s,%s,%s,clock_timestamp())""",
                            (observation.id, content.connector, digest),
                        )
                elif not content_inserted and observation.provenance_level == "unverified_discovery":
                    cursor.execute(
                        """INSERT INTO raw_evidence_deletions
                        (reference,rights_policy_id,status,attempts,next_attempt_at,lease_until,last_error,deleted_at)
                        SELECT %s,%s,'pending',0,clock_timestamp(),NULL,NULL,NULL
                        WHERE %s<>''
                          AND NOT EXISTS (SELECT 1 FROM observations WHERE raw_evidence_ref=%s)
                          AND NOT EXISTS (SELECT 1 FROM metric_snapshots WHERE source_revision=%s)
                        ON CONFLICT (reference) DO UPDATE SET
                          rights_policy_id=excluded.rights_policy_id,status='pending',attempts=0,
                          next_attempt_at=clock_timestamp(),lease_until=NULL,last_error=NULL,deleted_at=NULL""",
                        (
                            observation.raw_evidence_ref, observation.rights_policy_id,
                            observation.raw_evidence_ref, observation.raw_evidence_ref,
                            observation.raw_evidence_ref,
                        ),
                    )
                if content_inserted:
                    trusted = observation.provenance_level != "unverified_discovery"
                    valid_increment = 1 if trusted else 0
                    discovery_reason = f"connector_observation:{content.connector}" if trusted else "unverified_discovery"
                    # The observation rows themselves intentionally allow duplicate
                    # fingerprints so data-quality leakage remains measurable. This
                    # lock serializes only the source-validity claim: after waiting,
                    # READ COMMITTED gives the NOT EXISTS statement a fresh snapshot.
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                        (f"source-valid-observation:{observation.source_id}:{digest}",),
                    )
                    cursor.execute(
                        """INSERT INTO sources
                        (id,platform,display_name,status,language,valid_observations,
                         discovered_reason,discovery_reasons,first_observed_at,last_observed_at)
                        SELECT %s,%s,%s,'candidate',%s,%s,%s,ARRAY[%s]::text[],%s,%s
                        WHERE %s=0 OR NOT EXISTS (
                          SELECT 1 FROM observations
                          WHERE source_id=%s AND content_fingerprint=%s AND id<>%s
                            AND provenance_level<>'unverified_discovery'
                        )
                        ON CONFLICT (id) DO UPDATE SET
                          valid_observations=sources.valid_observations+excluded.valid_observations,
                          last_observed_at=GREATEST(sources.last_observed_at,excluded.last_observed_at),
                          first_observed_at=LEAST(sources.first_observed_at,excluded.first_observed_at),
                          discovery_reasons=ARRAY(
                            SELECT DISTINCT reason FROM unnest(sources.discovery_reasons||excluded.discovery_reasons) reason
                          ),
                          updated_at=clock_timestamp()""",
                        (
                            observation.source_id, observation.platform, observation.source_id,
                            observation.language, valid_increment, discovery_reason, discovery_reason,
                            observation.collected_at, observation.collected_at,
                            valid_increment,
                            observation.source_id, digest, observation.id,
                        ),
                    )
                    if cursor.rowcount == 0:
                        cursor.execute(
                            """UPDATE sources SET last_observed_at=GREATEST(last_observed_at,%s),
                            discovery_reasons=ARRAY(
                              SELECT DISTINCT reason FROM unnest(discovery_reasons||ARRAY['duplicate_content_observation']) reason
                            ),updated_at=clock_timestamp() WHERE id=%s""",
                            (observation.collected_at, observation.source_id),
                        )
                    cursor.execute(
                        """INSERT INTO content_ingest_history
                        (observation_id,connector_id,content_fingerprint,persisted_at)
                        VALUES (%s,%s,%s,clock_timestamp())""",
                        (observation.id, content.connector, digest),
                    )
                    cursor.execute("INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('observation.created',%s,%s::jsonb)", (observation.id, json.dumps({"observationId": observation.id})))
                snapshot_ids: list[str] = []
                for snapshot in snapshots:
                    cursor.execute(
                        """
                        INSERT INTO metric_snapshots (id,subject_type,subject_id,metric_name,value,effective_at,collected_at,is_estimated,source_revision,connector)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING id
                        """,
                        (snapshot.id, snapshot.subject_type, snapshot.subject_id, snapshot.metric_name, snapshot.value,
                         snapshot.effective_at, snapshot.collected_at, snapshot.is_estimated, snapshot.source_revision, snapshot.connector),
                    )
                    if cursor.fetchone():
                        snapshot_ids.append(snapshot.id)
                if snapshot_ids:
                    cursor.execute("INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('metric_snapshots.created',%s,%s::jsonb)", (observation.id, json.dumps({"snapshotIds": snapshot_ids})))
                if content_inserted or provenance_upgraded or snapshot_ids:
                    cursor.execute(
                        """
                        INSERT INTO observation_processing (observation_id,revision,processed_revision)
                        VALUES (%s,1,0) ON CONFLICT (observation_id) DO UPDATE
                        SET revision=observation_processing.revision+1,updated_at=now()
                        RETURNING revision
                        """,
                        (observation.id,),
                    )
                    revision = int(cursor.fetchone()[0])
                    cursor.execute(
                        """INSERT INTO observation_processing_history
                        (observation_id,revision,collected_at,enqueued_at)
                        VALUES (%s,%s,%s,now()) ON CONFLICT (observation_id,revision) DO NOTHING""",
                        (observation.id, revision, observation.collected_at),
                    )
            connection.commit()
        return content_inserted or provenance_upgraded or bool(snapshot_ids)

    def list_source_profiles(self) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT s.id,s.display_name,s.platform,s.language,s.status,s.valid_observations,
                    s.early_hits,s.confirmed_hits,s.originality,s.domain_focus,s.authority,
                    s.marketing_matrix_overlap,s.quality_calibrated,s.discovery_reasons,
                    s.created_at,s.first_observed_at,s.last_observed_at,s.activated_at,
                    COALESCE((SELECT array_agg(DISTINCT o.account_id) FROM observations o
                              WHERE o.source_id=s.id AND o.account_id IS NOT NULL),'{}'::text[]),
                    COALESCE((SELECT array_agg(DISTINCT o.entity_id) FROM observations o
                              WHERE o.source_id=s.id AND o.entity_id IS NOT NULL),'{}'::text[])
                    FROM sources s ORDER BY s.created_at,s.id"""
                )
                rows = cursor.fetchall()
        return [
            {
                "id": row[0], "displayName": row[1], "platform": row[2], "language": row[3],
                "status": row[4], "validObservations": row[5], "earlyHits": row[6],
                "confirmedHits": row[7], "originality": float(row[8]), "domainFocus": float(row[9]),
                "authority": float(row[10]), "marketingMatrixOverlap": float(row[11]),
                "qualityCalibrated": row[12], "discoveryReasons": list(row[13] or []),
                "createdAt": row[14], "firstObservedAt": row[15], "lastObservedAt": row[16],
                "activatedAt": row[17], "accountIds": list(row[18] or []), "entityIds": list(row[19] or []),
            }
            for row in rows
        ]

    def promote_source_candidates(self, now: datetime | None = None) -> dict[str, object]:
        policy = load_source_score_policy()
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended('source-promotions',0))")
                cursor.execute("SELECT COALESCE(%s::timestamptz,clock_timestamp())", (now,))
                promoted_at = cursor.fetchone()[0]
                cursor.execute("SELECT count(*) FROM sources WHERE status='active'")
                active_count = int(cursor.fetchone()[0])
                cursor.execute(
                    """SELECT count(*) FROM source_promotion_facts
                    WHERE to_status='active'
                    AND promoted_at >= (date_trunc('day',%s::timestamptz AT TIME ZONE %s) AT TIME ZONE %s)""",
                    (promoted_at, policy.timezone_name, policy.timezone_name),
                )
                promoted_today = int(cursor.fetchone()[0])
                cursor.execute(
                    """SELECT id,created_at,valid_observations,early_hits,confirmed_hits,
                    originality,domain_focus,authority,marketing_matrix_overlap
                    FROM sources WHERE status='candidate' AND quality_calibrated=true FOR UPDATE"""
                )
                candidates = [
                    SourceCandidate(
                        id=str(row[0]), discovered_at=row[1], valid_observations=int(row[2]),
                        early_hits=int(row[3]), confirmed_hits=int(row[4]), originality=float(row[5]),
                        domain_focus=float(row[6]), authority=float(row[7]),
                        marketing_matrix_overlap=float(row[8]),
                    )
                    for row in cursor.fetchall()
                ]
                promoted = promote_candidates(
                    candidates, active_count, now=promoted_at, promoted_today=promoted_today, policy=policy,
                ) if policy.auto_promotion_enabled else []
                for candidate in promoted:
                    cursor.execute(
                        """UPDATE sources SET status='active',lead_score=%s,score_version=%s,
                        activated_at=%s,updated_at=clock_timestamp()
                        WHERE id=%s AND status='candidate'""",
                        (candidate.score, policy.version, promoted_at, candidate.id),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError("source promotion lost its locked candidate")
                    cursor.execute(
                        """INSERT INTO source_promotion_facts
                        (source_id,from_status,to_status,score,policy_version,policy_digest,promoted_at)
                        VALUES (%s,'candidate','active',%s,%s,%s,%s)""",
                        (
                            candidate.id, candidate.score, policy.version,
                            source_score_policy_digest(policy), promoted_at,
                        ),
                    )
            connection.commit()
        return {
            "promotedSourceIds": [candidate.id for candidate in promoted],
            "activeBefore": active_count, "promotedEarlierToday": promoted_today,
            "policyVersion": policy.version, "autoPromotionEnabled": policy.auto_promotion_enabled,
        }

    def review_source_status(
        self, source_id: str, status: str, actor_id: str, reason: str,
    ) -> MutationReceipt:
        policy = load_source_score_policy()
        if status not in {"candidate", "active", "paused", "blocked"}:
            raise ValueError("unsupported source status")
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT status,lead_score FROM sources WHERE id=%s FOR UPDATE",
                    (source_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    raise KeyError(source_id)
                previous, score = str(row[0]), float(row[1])
                if previous == status:
                    raise ValueError(f"source is already {status}")
                if status == "active":
                    cursor.execute("SELECT count(*) FROM sources WHERE status='active'")
                    if int(cursor.fetchone()[0]) >= policy.active_capacity:
                        raise ValueError("active source capacity reached")
                cursor.execute(
                    """UPDATE sources SET status=%s,
                    activated_at=CASE WHEN %s='active' THEN clock_timestamp() ELSE activated_at END,
                    updated_at=clock_timestamp() WHERE id=%s RETURNING updated_at""",
                    (status, status, source_id),
                )
                reviewed_at = cursor.fetchone()[0]
                cursor.execute(
                    """INSERT INTO source_promotion_facts
                    (source_id,from_status,to_status,score,policy_version,policy_digest,promoted_at,
                     reviewed_by,review_reason,transition_kind)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'manual_review')""",
                    (
                        source_id, previous, status, score, policy.version,
                        source_score_policy_digest(policy), reviewed_at, actor_id, reason,
                    ),
                )
            connection.commit()
        return MutationReceipt(
            id=source_id, status="completed", operation=f"source.review.{status}", createdAt=reviewed_at,
        )

    def claim_observation_processing(self, observation_id: str, lease_seconds: int = 300) -> int | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE observation_processing SET lease_until=now()+(%s * interval '1 second'),attempts=attempts+1,updated_at=now()
                    WHERE observation_id=%s AND processed_revision < revision AND (lease_until IS NULL OR lease_until < now())
                    RETURNING revision
                    """,
                    (lease_seconds, observation_id),
                )
                row = cursor.fetchone()
                if row:
                    cursor.execute(
                        """UPDATE observation_processing_history SET attempts=attempts+1
                        WHERE observation_id=%s AND revision=%s""",
                        (observation_id, int(row[0])),
                    )
            connection.commit()
        return int(row[0]) if row else None

    def complete_observation_processing(self, observation_id: str, revision: int) -> None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE observation_processing SET processed_revision=GREATEST(processed_revision,%s),lease_until=NULL,last_error=NULL,updated_at=now()
                    WHERE observation_id=%s
                    """,
                    (revision, observation_id),
                )
                cursor.execute(
                    """UPDATE observation_processing_history SET completed_at=COALESCE(completed_at,now())
                    WHERE observation_id=%s AND revision<=%s""",
                    (observation_id, revision),
                )
            connection.commit()

    def fail_observation_processing(self, observation_id: str, revision: int, error: str) -> None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE observation_processing SET lease_until=NULL,last_error=%s,updated_at=now()
                    WHERE observation_id=%s AND processed_revision < %s
                    """,
                    (error[:1000], observation_id, revision),
                )
                cursor.execute(
                    """UPDATE observation_processing_history SET last_failed_at=now(),last_error=%s
                    WHERE observation_id=%s AND revision=%s AND completed_at IS NULL""",
                    (error[:1000], observation_id, revision),
                )
            connection.commit()

    def list_observation_processing_history(self, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT observation_id,revision,collected_at,enqueued_at,completed_at,attempts,last_failed_at,last_error
                    FROM observation_processing_history WHERE enqueued_at>=%s ORDER BY enqueued_at""",
                    (since,),
                )
                rows = cursor.fetchall()
        return [
            {
                "observationId": row[0], "revision": row[1], "collectedAt": row[2], "enqueuedAt": row[3],
                "completedAt": row[4], "attempts": row[5], "lastFailedAt": row[6], "lastError": row[7],
            }
            for row in rows
        ]

    def list_pending_observation_ids(self, limit: int = 100) -> list[str]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT observation_id FROM observation_processing
                    WHERE processed_revision < revision AND (lease_until IS NULL OR lease_until < now())
                    ORDER BY updated_at LIMIT %s
                    """,
                    (limit,),
                )
                rows = cursor.fetchall()
        return [str(row[0]) for row in rows]

    def pipeline_backlog_counts(self) -> dict[str, int]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT
                      (SELECT count(*) FROM observation_processing WHERE processed_revision < revision),
                      (SELECT count(*) FROM outbox WHERE published_at IS NULL)"""
                )
                row = cursor.fetchone()
        return {"processingPending": int(row[0]), "outboxPending": int(row[1])}

    def get_latest_observation(self, observation_id: str) -> Observation | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT id,platform,external_id,source_id,account_id,entity_id,published_at,available_at,availability_basis,collected_at,
                           language,title,body,url,raw_evidence_ref,content_fingerprint,signal_family,rights_policy_id,provenance_level
                    FROM observations WHERE id=%s
                    """,
                    (observation_id,),
                )
                row = cursor.fetchone()
                if row is None:
                    return None
                cursor.execute(
                    """
                    SELECT metric_name,value,collected_at,source_revision FROM metric_snapshots
                    WHERE subject_type='content' AND subject_id=%s ORDER BY collected_at DESC
                    """,
                    (observation_id,),
                )
                metric_rows = cursor.fetchall()
        collected_at = row[9]
        metrics: dict[str, float] = {}
        raw_ref = row[14]
        if metric_rows:
            collected_at = metric_rows[0][2]
            latest = [item for item in metric_rows if item[2] == collected_at]
            metrics = {str(item[0]): float(item[1]) for item in latest}
            raw_ref = next((item[3] for item in latest if item[3]), raw_ref)
        return Observation(
            id=row[0], platform=row[1], externalId=row[2], sourceId=row[3], accountId=row[4], entityId=row[5],
            publishedAt=row[6], availableAt=row[7], availabilityBasis=row[8], collectedAt=collected_at,
            language=row[10], title=row[11], text=row[12], url=row[13], metrics=metrics,
            rawEvidenceRef=raw_ref, contentFingerprint=row[15], signalFamily=row[16], rightsPolicyId=row[17],
            provenanceLevel=row[18], relation="unknown",
        )

    def upsert_event(self, event: RadarEvent) -> None:
        payload = json.dumps(event.model_dump(mode="json", by_alias=True), ensure_ascii=False)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (event.id,))
                cursor.execute("SELECT lifecycle_state,version FROM events WHERE id=%s FOR UPDATE", (event.id,))
                previous_row = cursor.fetchone()
                previous_state = str(previous_row[0]) if previous_row else None
                durable_revision = int(previous_row[1]) if previous_row else 0
                if durable_revision != event.storage_revision:
                    raise ConcurrentScoreConflict(
                        f"event {event.id} changed from revision {event.storage_revision} to {durable_revision}"
                    )
                cursor.execute(
                    """
                    INSERT INTO events (id, canonical_title_zh, canonical_title_en, event_type, lifecycle_state,
                      structure_labels, first_seen_at, last_seen_at, current_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id) DO UPDATE SET canonical_title_zh=excluded.canonical_title_zh,
                      canonical_title_en=excluded.canonical_title_en, event_type=excluded.event_type,
                      lifecycle_state=excluded.lifecycle_state, structure_labels=excluded.structure_labels,
                      last_seen_at=excluded.last_seen_at, current_score=excluded.current_score,
                      version=events.version+1, updated_at=now()
                    """,
                    (event.id, event.title, event.title_en, event.event_type, event.state,
                     [str(label) for label in event.labels], event.first_seen, event.updated_at, payload),
                )
                if _entered_review_queue_state(previous_state, event.state.value):
                    self._insert_review_queue_entry(cursor, event)
            connection.commit()

    def save_score(self, score: StoredScore) -> None:
        cycle = score_cycle(score.input_to)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (score.event_id,))
                cursor.execute(
                    "SELECT COALESCE(max(scoring_revision),0)+1 FROM score_runs WHERE event_id=%s AND cycle_id=%s",
                    (score.event_id, cycle),
                )
                revision = int(cursor.fetchone()[0])
                cursor.execute(
                    """
                    INSERT INTO score_runs
                      (event_id,cycle_id,scoring_revision,score_version,threshold_version,
                       baseline_version,baseline_digest,feature_registry_version,feature_registry_digest,
                       evidence_policy_version,label_policy_version,cluster_version,identity_version,input_observation_ids,
                       input_from,input_to,input_digest,drivers,payload,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s::jsonb,%s::jsonb,%s)
                    ON CONFLICT (event_id,input_digest) DO NOTHING
                    RETURNING scoring_revision
                    """,
                    (
                        score.event_id, cycle, revision, score.score_version, score.threshold_version,
                        score.baseline_version, score.baseline_digest, score.feature_registry_version, score.feature_registry_digest,
                        score.evidence_policy_version, score.label_policy_version, score.cluster_version, score.identity_version,
                        json.dumps(score.input_observation_ids), score.input_from, score.input_to, score.input_digest,
                        json.dumps(score.drivers, ensure_ascii=False), json.dumps(score.payload, ensure_ascii=False), score.created_at,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted:
                    cursor.execute(
                        "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('score.created',%s,%s::jsonb)",
                        (score.event_id, json.dumps({"scoreVersion": score.score_version, "inputDigest": score.input_digest, "cycleId": cycle, "revision": inserted[0]})),
                    )
            connection.commit()

    def has_score_input_digest(self, event_id: str, input_digest: str) -> bool:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM score_runs WHERE event_id=%s AND input_digest=%s LIMIT 1", (event_id, input_digest))
                return cursor.fetchone() is not None

    def list_score_runs(self, event_id: str) -> list[StoredScore]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT score_version,threshold_version,scoring_revision,baseline_version,baseline_digest,
                      feature_registry_version,feature_registry_digest,evidence_policy_version,label_policy_version,
                      cluster_version,identity_version,input_observation_ids,input_from,input_to,input_digest,drivers,payload,created_at
                    FROM score_runs WHERE event_id=%s ORDER BY input_to,scoring_revision,created_at""",
                    (event_id,),
                )
                rows = cursor.fetchall()
        return [StoredScore(
            event_id=event_id, score_version=row[0], threshold_version=row[1], scoring_revision=row[2],
            baseline_version=row[3], baseline_digest=row[4], feature_registry_version=row[5],
            feature_registry_digest=row[6], evidence_policy_version=row[7], label_policy_version=row[8],
            cluster_version=row[9], identity_version=row[10], input_observation_ids=row[11],
            input_from=row[12], input_to=row[13], input_digest=row[14], drivers=row[15], payload=row[16], created_at=row[17],
        ) for row in rows]

    def review_priority_context(
        self, event_anchors: dict[str, datetime]
    ) -> dict[str, dict[str, object]]:
        if not event_anchors:
            return {}
        event_ids = list(event_anchors)
        anchors = [event_anchors[event_id] for event_id in event_ids]
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """WITH anchors(event_id,anchor_at) AS (
                      SELECT * FROM unnest(%s::text[],%s::timestamptz[])
                    )
                    SELECT anchors.event_id,
                      count(DISTINCT observations.id) FILTER (
                        WHERE observations.collected_at>anchors.anchor_at
                      ),
                      max(observations.collected_at)
                    FROM anchors
                    LEFT JOIN event_observations
                      ON event_observations.event_id=anchors.event_id
                    LEFT JOIN observations
                     ON observations.id=event_observations.observation_id
                     AND observations.provenance_level<>'unverified_discovery'
                    GROUP BY anchors.event_id""",
                    (event_ids, anchors),
                )
                evidence_rows = cursor.fetchall()
                cursor.execute(
                    """SELECT event_id,payload,input_to,recent_rank FROM (
                      SELECT event_id,payload,input_to,
                        row_number() OVER (
                          PARTITION BY event_id
                          ORDER BY input_to DESC,scoring_revision DESC,created_at DESC
                        ) AS recent_rank
                      FROM score_runs WHERE event_id=ANY(%s)
                    ) ranked WHERE recent_rank<=2
                    ORDER BY event_id,recent_rank DESC""",
                    (event_ids,),
                )
                score_rows = cursor.fetchall()
        result = {
            event_id: {"newEvidenceCount": 0, "latestEvidenceAt": None, "scoreRuns": []}
            for event_id in event_ids
        }
        for event_id, count, latest_evidence_at in evidence_rows:
            result[str(event_id)]["newEvidenceCount"] = int(count)
            result[str(event_id)]["latestEvidenceAt"] = latest_evidence_at
        for event_id, payload, input_to, _ in score_rows:
            score_runs = result[str(event_id)]["scoreRuns"]
            assert isinstance(score_runs, list)
            score_runs.append({"payload": payload, "inputTo": input_to})
        return result

    def latest_evidence_times(self, event_ids: set[str]) -> dict[str, datetime]:
        if not event_ids:
            return {}
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT event_observations.event_id,max(observations.collected_at)
                    FROM event_observations
                    JOIN observations ON observations.id=event_observations.observation_id
                    WHERE event_observations.event_id=ANY(%s)
                      AND observations.provenance_level<>'unverified_discovery'
                    GROUP BY event_observations.event_id""",
                    (list(event_ids),),
                )
                rows = cursor.fetchall()
        return {str(event_id): collected_at for event_id, collected_at in rows}

    def load_event_embeddings(
        self, event_titles: dict[str, str], model_version: str, dimensions: int
    ) -> dict[str, list[float]]:
        if not event_titles or dimensions != 1024:
            return {}
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT event_id,title_hash,embedding::text FROM event_embeddings
                    WHERE event_id=ANY(%s) AND model_version=%s AND dimensions=%s""",
                    (list(event_titles), model_version, dimensions),
                )
                rows = cursor.fetchall()
        result: dict[str, list[float]] = {}
        for event_id, title_hash, embedding_text in rows:
            title = event_titles.get(str(event_id))
            if title is not None and title_hash == hashlib.sha256(title.encode()).hexdigest():
                result[str(event_id)] = [float(value) for value in json.loads(embedding_text)]
        return result

    def save_event_embedding(
        self, event_id: str, model_version: str, title: str, embedding: list[float]
    ) -> None:
        if len(embedding) != 1024:
            raise ValueError("PostgreSQL event embeddings require 1024 dimensions")
        vector_literal = "[" + ",".join(format(float(value), ".9g") for value in embedding) + "]"
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO event_embeddings
                      (event_id,model_version,dimensions,title_hash,embedding,updated_at)
                    VALUES (%s,%s,1024,%s,%s::vector,now())
                    ON CONFLICT (event_id,model_version) DO UPDATE SET
                      dimensions=excluded.dimensions,title_hash=excluded.title_hash,
                      embedding=excluded.embedding,updated_at=excluded.updated_at""",
                    (event_id, model_version, hashlib.sha256(title.encode()).hexdigest(), vector_literal),
                )
            connection.commit()

    def nearest_event_embeddings(
        self, query_embedding: list[float], event_titles: dict[str, str], model_version: str, limit: int = 50,
    ) -> dict[str, list[float]]:
        if len(query_embedding) != 1024 or not event_titles or limit < 1:
            return {}
        vector_literal = "[" + ",".join(format(float(value), ".9g") for value in query_embedding) + "]"
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT embeddings.event_id,embeddings.title_hash,embeddings.embedding::text
                    FROM event_embeddings embeddings
                    JOIN events ON events.id=embeddings.event_id
                    WHERE embeddings.model_version=%s AND cardinality(events.superseded_by)=0
                    ORDER BY embeddings.embedding <=> %s::vector LIMIT %s""",
                    (model_version, vector_literal, min(500, limit)),
                )
                rows = cursor.fetchall()
        result: dict[str, list[float]] = {}
        for event_id, title_hash, embedding_text in rows:
            title = event_titles.get(str(event_id))
            if title is not None and title_hash == hashlib.sha256(title.encode()).hexdigest():
                result[str(event_id)] = [float(value) for value in json.loads(embedding_text)]
        return result

    def delete_event_embeddings(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("DELETE FROM event_embeddings WHERE event_id=ANY(%s)", (event_ids,))
            connection.commit()

    def append_baseline_sample(
        self, fact_key: str, source_event_id: str, event_type: EventType, baseline_key: str, observed_at: datetime, value: float,
    ) -> bool:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO baseline_samples (fact_key,source_event_id,event_type,baseline_key,observed_at,value)
                    VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (fact_key) DO NOTHING""",
                    (fact_key, source_event_id, event_type.value, baseline_key, observed_at, max(0.0, value)),
                )
                inserted = cursor.rowcount == 1
            connection.commit()
        return inserted

    def load_baseline_history(self, event_type: EventType, exclude_event_id: str | None = None) -> tuple[dict[str, list[float]], str, int]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT fact_key,baseline_key,observed_at,value FROM baseline_samples
                    WHERE event_type=%s AND source_event_id<>COALESCE(%s,'')
                    ORDER BY baseline_key,observed_at,fact_key""",
                    (event_type.value, exclude_event_id),
                )
                rows = cursor.fetchall()
        history: dict[str, list[float]] = {}
        for _, baseline_key, _, value in rows:
            history.setdefault(str(baseline_key), []).append(float(value))
        for values in history.values():
            del values[:-1000]
        material = [[row[0], row[1], row[2].isoformat(), float(row[3])] for row in rows]
        digest = "sha256:" + hashlib.sha256(json.dumps(material, separators=(",", ":")).encode()).hexdigest()
        return history, digest, len(rows)

    def list_ranking_score_facts(self) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT e.current_score,s.input_digest,s.input_to
                    FROM events e
                    LEFT JOIN LATERAL (
                      SELECT input_digest,input_to FROM score_runs
                      WHERE event_id=e.id ORDER BY input_to DESC,created_at DESC LIMIT 1
                    ) s ON true
                    WHERE cardinality(e.superseded_by)=0"""
                )
                rows = cursor.fetchall()
        return [
            {
                "event": RadarEvent.model_validate(row[0]),
                "scoreRunId": row[1],
                "scoreRunAt": row[2],
            }
            for row in rows
        ]

    def list_lead_threshold_crossings(self) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT event_id,crossed_at,score_run_ref,threshold_version,policy_version
                    FROM lead_threshold_crossings ORDER BY crossed_at,event_id"""
                )
                rows = cursor.fetchall()
        return [
            {"eventId": row[0], "crossedAt": row[1], "scoreRunId": row[2], "thresholdVersion": row[3], "policyVersion": row[4]}
            for row in rows
        ]

    def ranking_ledger_snapshot(
        self,
    ) -> tuple[datetime, list[dict[str, object]], list[dict[str, object]]]:
        """Return one DB-clock-watermarked repeatable-read ledger snapshot."""
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cursor.execute(
                    """SELECT e.current_score,s.input_digest,s.input_to
                    FROM events e
                    LEFT JOIN LATERAL (
                      SELECT input_digest,input_to FROM score_runs
                      WHERE event_id=e.id ORDER BY input_to DESC,created_at DESC LIMIT 1
                    ) s ON true
                    WHERE cardinality(e.superseded_by)=0"""
                )
                score_rows = cursor.fetchall()
                cursor.execute(
                    """SELECT event_id,crossed_at,score_run_ref,threshold_version,policy_version
                    FROM lead_threshold_crossings ORDER BY crossed_at,event_id"""
                )
                crossing_rows = cursor.fetchall()
                cursor.execute("SELECT clock_timestamp()")
                generated_at = cursor.fetchone()[0]
            connection.commit()
        ranking_facts = [
            {
                "event": RadarEvent.model_validate(row[0]),
                "scoreRunId": row[1],
                "scoreRunAt": row[2],
            }
            for row in score_rows
        ]
        crossings = [
            {
                "eventId": row[0], "crossedAt": row[1], "scoreRunId": row[2],
                "thresholdVersion": row[3], "policyVersion": row[4],
            }
            for row in crossing_rows
        ]
        return generated_at, ranking_facts, crossings

    def commit_scored_event(self, event: RadarEvent, score: StoredScore) -> None:
        payload = json.dumps(event.model_dump(mode="json", by_alias=True), ensure_ascii=False)
        cycle = score_cycle(score.input_to)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (event.id,))
                cursor.execute("SELECT lifecycle_state,version FROM events WHERE id=%s FOR UPDATE", (event.id,))
                previous_row = cursor.fetchone()
                previous_state = str(previous_row[0]) if previous_row else None
                durable_revision = int(previous_row[1]) if previous_row else 0
                if durable_revision != event.storage_revision:
                    raise ConcurrentScoreConflict(
                        f"event {event.id} changed from revision {event.storage_revision} to {durable_revision}"
                    )
                cursor.execute(
                    """
                    INSERT INTO events (id,canonical_title_zh,canonical_title_en,event_type,lifecycle_state,
                      structure_labels,first_seen_at,last_seen_at,current_score)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    ON CONFLICT (id) DO UPDATE SET canonical_title_zh=excluded.canonical_title_zh,
                      canonical_title_en=excluded.canonical_title_en,event_type=excluded.event_type,
                      lifecycle_state=excluded.lifecycle_state,structure_labels=excluded.structure_labels,
                      last_seen_at=excluded.last_seen_at,current_score=excluded.current_score,
                      version=events.version+1,updated_at=now()
                    """,
                    (event.id, event.title, event.title_en, event.event_type, event.state,
                     [str(label) for label in event.labels], event.first_seen, event.updated_at, payload),
                )
                cursor.execute(
                    """
                    INSERT INTO score_runs
                      (event_id,cycle_id,scoring_revision,score_version,threshold_version,
                       baseline_version,baseline_digest,feature_registry_version,feature_registry_digest,
                       evidence_policy_version,label_policy_version,cluster_version,identity_version,input_observation_ids,
                       input_from,input_to,input_digest,drivers,payload,created_at)
                    SELECT
                      %(event_id)s,%(cycle_id)s,COALESCE(max(scoring_revision),0)+1,
                      %(score_version)s,%(threshold_version)s,
                      %(baseline_version)s,%(baseline_digest)s,
                      %(feature_registry_version)s,%(feature_registry_digest)s,
                      %(evidence_policy_version)s,%(label_policy_version)s,
                      %(cluster_version)s,%(identity_version)s,%(input_observation_ids)s::jsonb,
                      %(input_from)s,%(input_to)s,%(input_digest)s,
                      %(drivers)s::jsonb,%(payload)s::jsonb,%(created_at)s
                    FROM score_runs WHERE event_id=%(event_id)s AND cycle_id=%(cycle_id)s
                    ON CONFLICT (event_id,input_digest) DO NOTHING
                    RETURNING id,scoring_revision
                    """,
                    {
                        "event_id": score.event_id, "cycle_id": cycle,
                        "score_version": score.score_version, "threshold_version": score.threshold_version,
                        "baseline_version": score.baseline_version, "baseline_digest": score.baseline_digest,
                        "feature_registry_version": score.feature_registry_version,
                        "feature_registry_digest": score.feature_registry_digest,
                        "evidence_policy_version": score.evidence_policy_version,
                        "label_policy_version": score.label_policy_version,
                        "cluster_version": score.cluster_version, "identity_version": score.identity_version,
                        "input_observation_ids": json.dumps(score.input_observation_ids),
                        "input_from": score.input_from, "input_to": score.input_to,
                        "input_digest": score.input_digest,
                        "drivers": json.dumps(score.drivers, ensure_ascii=False),
                        "payload": json.dumps(score.payload, ensure_ascii=False), "created_at": score.created_at,
                    },
                )
                inserted = cursor.fetchone()
                if inserted:
                    cursor.execute(
                        "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('score.created',%s,%s::jsonb)",
                        (event.id, json.dumps({"scoreVersion": score.score_version, "inputDigest": score.input_digest, "cycleId": cycle, "revision": inserted[1]})),
                    )
                    cursor.execute(
                        """INSERT INTO event_metric_snapshots
                          (event_id,captured_at,attention,behavior,diversity,authority,coordination_risk,coverage,evidence_strength,score_run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (event_id,captured_at) DO UPDATE SET
                          attention=excluded.attention,behavior=excluded.behavior,diversity=excluded.diversity,
                          authority=excluded.authority,coordination_risk=excluded.coordination_risk,
                          coverage=excluded.coverage,evidence_strength=excluded.evidence_strength,score_run_id=excluded.score_run_id""",
                        (
                            event.id, score.input_to, event.attention, event.behavior, event.diversity, event.authority,
                            event.coordination_risk, event.coverage, event.evidence_score, inserted[0],
                        ),
                    )
                if _meets_lead_threshold(event):
                    cursor.execute(
                        """INSERT INTO lead_threshold_crossings
                        (event_id,crossed_at,score_run_ref,threshold_version,policy_version)
                        VALUES (%s,clock_timestamp(),%s,%s,%s)
                        ON CONFLICT (event_id,threshold_version,policy_version) DO NOTHING""",
                        (event.id, score.input_digest, score.threshold_version, load_product_metric_policy().version),
                    )
                if _entered_review_queue_state(previous_state, event.state.value):
                    self._insert_review_queue_entry(cursor, event)
            connection.commit()

    def assign_observation(self, event_id: str, observation_id: str, cluster_score: float, assignment_version: str) -> bool:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM event_observations WHERE event_id=%s AND observation_id=%s", (event_id, observation_id))
                created = cursor.fetchone() is None
                cursor.execute(
                    """
                    INSERT INTO event_observations (event_id,observation_id,cluster_score,assignment_version)
                    VALUES (%s,%s,%s,%s) ON CONFLICT (event_id,observation_id) DO UPDATE
                    SET cluster_score=excluded.cluster_score, assignment_version=excluded.assignment_version
                    """,
                    (event_id, observation_id, cluster_score, assignment_version),
                )
            connection.commit()
        return created

    def list_event_observations(self, event_id: str) -> list[Observation]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT o.id,o.platform,o.external_id,o.source_id,o.account_id,o.entity_id,o.published_at,
                      o.available_at,o.availability_basis,COALESCE(ms.collected_at,o.collected_at),o.language,o.title,o.body,o.url,
                      COALESCE(jsonb_object_agg(ms.metric_name,ms.value) FILTER (WHERE ms.metric_name IS NOT NULL),'{}'::jsonb),
                      o.raw_evidence_ref,o.content_fingerprint,o.signal_family,o.rights_policy_id,o.provenance_level
                    FROM observations o
                    JOIN event_observations eo ON eo.observation_id=o.id
                    LEFT JOIN metric_snapshots ms ON ms.subject_id=o.id
                    WHERE eo.event_id=%s
                    GROUP BY o.id,o.platform,o.external_id,o.source_id,o.account_id,o.entity_id,o.published_at,
                      o.available_at,o.availability_basis,o.collected_at,ms.collected_at,o.language,o.title,o.body,o.url,o.raw_evidence_ref,o.content_fingerprint,o.signal_family,o.rights_policy_id,o.provenance_level
                    ORDER BY o.published_at,COALESCE(ms.collected_at,o.collected_at)
                    """,
                    (event_id,),
                )
                rows = cursor.fetchall()
        return [Observation(
            id=row[0], platform=row[1], externalId=row[2], sourceId=row[3], accountId=row[4], entityId=row[5],
            publishedAt=row[6], availableAt=row[7], availabilityBasis=row[8], collectedAt=row[9], language=row[10],
            title=row[11], text=row[12], url=row[13], metrics=row[14], rawEvidenceRef=row[15],
            contentFingerprint=row[16], signalFamily=row[17], rightsPolicyId=row[18], provenanceLevel=row[19], relation="unknown",
        ) for row in rows]

    def list_observation_freshness(self, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT id,connector,available_at,collected_at,availability_basis
                    FROM observations WHERE collected_at>=%s ORDER BY collected_at,id""",
                    (since,),
                )
                rows = cursor.fetchall()
        return [
            {"id": row[0], "connectorId": row[1], "availableAt": row[2], "collectedAt": row[3], "availabilityBasis": row[4]}
            for row in rows
        ]

    def rollup_and_retain_event_metrics(self, at: datetime, raw_retention_days: int = 90, rollup_retention_days: int = 730) -> dict[str, int]:
        if raw_retention_days < 1 or rollup_retention_days < raw_retention_days:
            raise ValueError("event metric retention windows are invalid")
        raw_cutoff = at - timedelta(days=raw_retention_days)
        rollup_cutoff = (at - timedelta(days=rollup_retention_days)).date()
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO event_metric_daily_rollups
                      (event_id,day,samples,attention_avg,attention_max,behavior_avg,behavior_max,coverage_min,evidence_strength_max)
                    SELECT event_id,captured_at::date,count(*),avg(attention),max(attention),avg(behavior),max(behavior),min(coverage),max(evidence_strength)
                    FROM event_metric_snapshots WHERE captured_at<%s GROUP BY event_id,captured_at::date
                    ON CONFLICT (event_id,day) DO UPDATE SET
                      samples=excluded.samples,attention_avg=excluded.attention_avg,attention_max=excluded.attention_max,
                      behavior_avg=excluded.behavior_avg,behavior_max=excluded.behavior_max,
                      coverage_min=excluded.coverage_min,evidence_strength_max=excluded.evidence_strength_max""",
                    (raw_cutoff,),
                )
                rolled_up = cursor.rowcount
                cursor.execute("DELETE FROM event_metric_snapshots WHERE captured_at<%s", (raw_cutoff,))
                raw_deleted = cursor.rowcount
                cursor.execute("DELETE FROM event_metric_daily_rollups WHERE day<%s", (rollup_cutoff,))
                rollups_deleted = cursor.rowcount
            connection.commit()
        return {"rolledUp": rolled_up, "rawDeleted": raw_deleted, "rollupsDeleted": rollups_deleted}

    def get_event(self, event_id: str) -> RadarEvent | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_score,version FROM events WHERE id=%s", (event_id,))
                row = cursor.fetchone()
        return (
            RadarEvent.model_validate(row[0]).model_copy(update={"storage_revision": int(row[1])})
            if row else None
        )

    def list_events(self) -> list[RadarEvent]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_score,version FROM events WHERE cardinality(superseded_by)=0 ORDER BY (current_score->>'velocity')::numeric DESC NULLS LAST")
                rows = cursor.fetchall()
        return [
            RadarEvent.model_validate(row[0]).model_copy(update={"storage_revision": int(row[1])})
            for row in rows
        ]

    def apply_connector_coverage_penalty(self, platform: str, penalty: float, note: str) -> int:
        events = self.list_events()
        changed = 0
        for event in events:
            if not any(item.lower() == platform.lower() for item in event.platforms):
                continue
            score = max(0, event.evidence_score - penalty)
            tier = "low" if score < 45 else "medium" if score < 70 else "high"
            self.upsert_event(event.model_copy(update={
                "coverage": max(0, event.coverage - penalty), "evidence_score": score,
                "evidence_strength": EvidenceStrength(tier), "uncertainty": min(100, event.uncertainty + penalty),
                "coverage_note": f"{event.coverage_note}；{note}",
            }))
            changed += 1
        return changed

    def upsert_connector(self, connector: ConnectorStatus) -> None:
        payload = json.dumps(connector.model_dump(mode="json", by_alias=True), ensure_ascii=False)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO connector_status (id,payload) VALUES (%s,%s::jsonb) ON CONFLICT (id) DO UPDATE SET payload=excluded.payload, updated_at=now()",
                    (connector.id, payload),
                )
            connection.commit()

    def reserve_connector_budget(
        self, connector_id: str, signal_family: str, maximum_cost_rmb: float,
        total_limit_rmb: float, connector_limit_rmb: float | None,
        family_limit_rmb: float | None, family_connector_ids: set[str], base_spend_rmb: float = 0,
    ) -> str | None:
        current = utcnow()
        month_start = current.date().replace(day=1)
        connector_ids = sorted(family_connector_ids)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"connector-budget:{month_start.isoformat()}",),
                )
                cursor.execute(
                    """UPDATE connector_budget_reservations SET status='reconciliation_required',
                    reconciliation_reason='reservation lease expired before run confirmation'
                    WHERE status='reserved' AND lease_until<clock_timestamp()"""
                )
                cursor.execute(
                    """SELECT
                      COALESCE((SELECT sum(estimated_cost_rmb) FROM connector_runs WHERE started_at>=date_trunc('month',clock_timestamp())),0)
                        + COALESCE((SELECT sum(CASE WHEN status='reconciled_charged' THEN actual_amount_rmb ELSE reserved_amount_rmb END) FROM connector_budget_reservations WHERE month_start=%s AND status IN ('reserved','reconciliation_required','reconciled_charged')),0),
                      COALESCE((SELECT sum(estimated_cost_rmb) FROM connector_runs WHERE started_at>=date_trunc('month',clock_timestamp()) AND connector_id=%s),0)
                        + COALESCE((SELECT sum(CASE WHEN status='reconciled_charged' THEN actual_amount_rmb ELSE reserved_amount_rmb END) FROM connector_budget_reservations WHERE month_start=%s AND status IN ('reserved','reconciliation_required','reconciled_charged') AND connector_id=%s),0),
                      COALESCE((SELECT sum(estimated_cost_rmb) FROM connector_runs WHERE started_at>=date_trunc('month',clock_timestamp()) AND connector_id=ANY(%s)),0)
                        + COALESCE((SELECT sum(CASE WHEN status='reconciled_charged' THEN actual_amount_rmb ELSE reserved_amount_rmb END) FROM connector_budget_reservations WHERE month_start=%s AND status IN ('reserved','reconciliation_required','reconciled_charged') AND signal_family=%s),0)""",
                    (month_start, connector_id, month_start, connector_id, connector_ids, month_start, signal_family),
                )
                total_spend, connector_spend, family_spend = (float(value) for value in cursor.fetchone())
                if base_spend_rmb + total_spend + maximum_cost_rmb > total_limit_rmb:
                    return None
                if connector_limit_rmb is not None and connector_spend + maximum_cost_rmb > connector_limit_rmb:
                    return None
                if family_limit_rmb is not None and family_spend + maximum_cost_rmb > family_limit_rmb:
                    return None
                cursor.execute(
                    """INSERT INTO connector_budget_reservations
                    (month_start,connector_id,signal_family,reserved_amount_rmb)
                    VALUES (%s,%s,%s,%s) RETURNING owner_token""",
                    (month_start, connector_id, signal_family, maximum_cost_rmb),
                )
                reservation_id = str(cursor.fetchone()[0])
            connection.commit()
        return reservation_id

    def reconcile_expired_connector_budget_reservations(self) -> int:
        current = utcnow().date().replace(day=1)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (f"connector-budget:{current.isoformat()}",),
                )
                cursor.execute(
                    """UPDATE connector_budget_reservations SET status='reconciliation_required',
                    reconciliation_reason='reservation lease expired before run confirmation'
                    WHERE status='reserved' AND lease_until<clock_timestamp()"""
                )
                changed = cursor.rowcount
            connection.commit()
        return changed

    def connector_budget_reconciliation_count(self) -> int:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT count(*) FROM connector_budget_reservations
                    WHERE status='reconciliation_required'
                       OR (status='reserved' AND lease_until<clock_timestamp())"""
                )
                row = cursor.fetchone()
        return int(row[0])

    def record_connector_run(self, connector_id: str, started_at: datetime, finished_at: datetime, status: str, inserted: int, duplicates: int, coverage: float, error: str | None = None, estimated_cost_rmb: float = 0, budget_reservation_id: str | None = None) -> None:
        latency_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))
        with self.connection() as connection:
            with connection.cursor() as cursor:
                if budget_reservation_id:
                    cursor.execute(
                        """SELECT reserved_amount_rmb FROM connector_budget_reservations
                        WHERE owner_token=%s AND connector_id=%s
                          AND status IN ('reserved','reconciliation_required') FOR UPDATE""",
                        (budget_reservation_id, connector_id),
                    )
                    reservation = cursor.fetchone()
                    if reservation is None:
                        raise RuntimeError("budget reservation is missing or no longer active")
                    if estimated_cost_rmb > float(reservation[0]) + 1e-9:
                        raise RuntimeError("actual connector cost exceeded its worst-case reservation")
                cursor.execute(
                    """
                    INSERT INTO connector_runs
                      (connector_id,started_at,finished_at,status,inserted_count,duplicate_count,latency_ms,coverage,error,estimated_cost_rmb)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (connector_id, started_at, finished_at, status, inserted, duplicates, latency_ms, coverage, error, max(0, estimated_cost_rmb)),
                )
                if budget_reservation_id:
                    cursor.execute(
                        """UPDATE connector_budget_reservations SET status='confirmed',
                        actual_amount_rmb=%s,confirmed_at=clock_timestamp(),
                        reconciliation_reason=NULL WHERE owner_token=%s""",
                        (max(0, estimated_cost_rmb), budget_reservation_id),
                    )
            connection.commit()

    def connector_stats_24h(self, connector_id: str) -> tuple[int, int]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COALESCE(SUM(inserted_count),0),
                           COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms),0)
                    FROM connector_runs WHERE connector_id=%s AND started_at >= now()-interval '24 hours'
                    """,
                    (connector_id,),
                )
                row = cursor.fetchone()
        return int(row[0]), (int(float(row[1])) + 59_999) // 60_000

    def list_connector_runs(self, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT connector_id,started_at,finished_at,status,inserted_count,duplicate_count,
                    latency_ms,coverage,error,estimated_cost_rmb
                    FROM connector_runs WHERE started_at>=%s ORDER BY started_at""",
                    (since,),
                )
                rows = cursor.fetchall()
        return [
            {
                "connectorId": row[0], "startedAt": row[1], "finishedAt": row[2], "status": row[3],
                "inserted": row[4], "duplicates": row[5], "latencyMs": row[6], "coverage": row[7],
                "error": row[8], "estimatedCostRmb": float(row[9]),
            }
            for row in rows
        ]

    def persisted_content_duplicate_stats(self, since: datetime) -> tuple[int, int]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT count(*),count(*)-count(DISTINCT (connector_id,content_fingerprint))
                    FROM content_ingest_history WHERE persisted_at>=%s""",
                    (since,),
                )
                row = cursor.fetchone()
        return int(row[0]), int(row[1])

    def monthly_connector_spend(self, at: datetime | None = None, connector_ids: set[str] | None = None) -> float:
        current = at or utcnow()
        month_start = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        next_month = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT
                      COALESCE((SELECT SUM(estimated_cost_rmb) FROM connector_runs
                        WHERE started_at >= %s AND started_at < %s
                        AND (%s::text[] IS NULL OR connector_id=ANY(%s))),0)
                      + COALESCE((SELECT SUM(CASE WHEN status='reconciled_charged' THEN actual_amount_rmb ELSE reserved_amount_rmb END) FROM connector_budget_reservations
                        WHERE month_start=%s AND status IN ('reserved','reconciliation_required','reconciled_charged')
                        AND (%s::text[] IS NULL OR connector_id=ANY(%s))),0)""",
                    (
                        month_start,
                        next_month,
                        list(connector_ids) if connector_ids is not None else None,
                        list(connector_ids) if connector_ids is not None else None,
                        month_start.date(),
                        list(connector_ids) if connector_ids is not None else None,
                        list(connector_ids) if connector_ids is not None else None,
                    ),
                )
                row = cursor.fetchone()
        return float(row[0])

    def list_connectors(self) -> list[ConnectorStatus]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT payload FROM connector_status ORDER BY id")
                rows = cursor.fetchall()
        return [ConnectorStatus.model_validate(row[0]) for row in rows]

    def get_connector(self, connector_id: str) -> ConnectorStatus | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT payload FROM connector_status WHERE id=%s", (connector_id,))
                row = cursor.fetchone()
        return ConnectorStatus.model_validate(row[0]) if row else None

    def get_connector_checkpoint(self, connector_id: str) -> dict[str, object]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT payload FROM connector_checkpoints WHERE connector_id=%s", (connector_id,))
                row = cursor.fetchone()
        return dict(row[0]) if row else {}

    def save_connector_checkpoint(self, connector_id: str, payload: dict[str, object]) -> None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """INSERT INTO connector_checkpoints (connector_id,payload) VALUES (%s,%s::jsonb)
                    ON CONFLICT (connector_id) DO UPDATE SET payload=excluded.payload,updated_at=now()""",
                    (connector_id, json.dumps(payload, ensure_ascii=False)),
                )
            connection.commit()

    def expire_raw_evidence(self, retention_days: dict[str, int], at: datetime) -> list[str]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                candidates: dict[str, str] = {}
                for policy_id, days in retention_days.items():
                    cutoff = at - timedelta(days=days)
                    cursor.execute(
                        """SELECT raw_evidence_ref FROM observations WHERE rights_policy_id=%s AND collected_at<=%s
                        AND raw_evidence_ref<>'' FOR UPDATE""",
                        (policy_id, cutoff),
                    )
                    for row in cursor.fetchall():
                        candidates[row[0]] = policy_id
                    cursor.execute(
                        """UPDATE observations SET raw_evidence_ref='',deletion_state='tombstoned'
                        WHERE rights_policy_id=%s AND collected_at<=%s AND raw_evidence_ref<>''""",
                        (policy_id, cutoff),
                    )
                    cursor.execute(
                        """SELECT ms.source_revision FROM metric_snapshots ms JOIN observations o ON o.id=ms.subject_id
                        WHERE ms.subject_type='content' AND o.rights_policy_id=%s AND ms.collected_at<=%s AND ms.source_revision IS NOT NULL""",
                        (policy_id, cutoff),
                    )
                    for row in cursor.fetchall():
                        candidates[row[0]] = policy_id
                    cursor.execute(
                        """UPDATE metric_snapshots ms SET source_revision=NULL FROM observations o
                        WHERE ms.subject_type='content' AND ms.subject_id=o.id AND o.rights_policy_id=%s
                        AND ms.collected_at<=%s AND ms.source_revision IS NOT NULL""",
                        (policy_id, cutoff),
                    )
                for reference, policy_id in candidates.items():
                    cursor.execute(
                        """SELECT EXISTS(SELECT 1 FROM observations WHERE raw_evidence_ref=%s)
                        OR EXISTS(SELECT 1 FROM metric_snapshots WHERE source_revision=%s)""",
                        (reference, reference),
                    )
                    if not cursor.fetchone()[0]:
                        cursor.execute(
                            """INSERT INTO raw_evidence_deletions (reference,rights_policy_id) VALUES (%s,%s)
                            ON CONFLICT (reference) DO NOTHING""",
                            (reference, policy_id),
                        )
                cursor.execute(
                    """SELECT reference FROM raw_evidence_deletions
                    WHERE status='pending' AND next_attempt_at<=%s AND (lease_until IS NULL OR lease_until<=%s)
                      AND NOT EXISTS (
                        SELECT 1 FROM observations WHERE raw_evidence_ref=raw_evidence_deletions.reference
                      )
                      AND NOT EXISTS (
                        SELECT 1 FROM metric_snapshots WHERE source_revision=raw_evidence_deletions.reference
                      )
                    ORDER BY queued_at FOR UPDATE SKIP LOCKED LIMIT 1000""",
                    (at, at),
                )
                pending = [row[0] for row in cursor.fetchall()]
                if pending:
                    cursor.execute(
                        """UPDATE raw_evidence_deletions SET attempts=attempts+1,lease_until=%s+(interval '5 minutes')
                        WHERE reference=ANY(%s)""",
                        (at, pending),
                    )
            connection.commit()
        return pending

    def raw_evidence_is_referenced(self, reference: str) -> bool:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT EXISTS(SELECT 1 FROM observations WHERE raw_evidence_ref=%s)
                    OR EXISTS(SELECT 1 FROM metric_snapshots WHERE source_revision=%s)""",
                    (reference, reference),
                )
                return bool(cursor.fetchone()[0])

    def release_raw_evidence_deletion(self, reference: str) -> None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE raw_evidence_deletions SET lease_until=NULL WHERE reference=%s AND status='pending'",
                    (reference,),
                )
            connection.commit()

    def confirm_raw_evidence_deletions(self, references: list[str]) -> None:
        if not references:
            return
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE raw_evidence_deletions SET status='completed',deleted_at=now(),last_error=NULL,lease_until=NULL WHERE reference=ANY(%s)",
                    (references,),
                )
            connection.commit()

    def fail_raw_evidence_deletion(self, reference: str, error: str, at: datetime | None = None) -> None:
        current = at or utcnow()
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE raw_evidence_deletions SET last_error=%s,lease_until=NULL,
                    next_attempt_at=%s + (LEAST(3600,60*power(2,LEAST(attempts,6))) * interval '1 second')
                    WHERE reference=%s AND status='pending'""",
                    (error[:1000], current, reference),
                )
            connection.commit()

    @staticmethod
    def _set_workspace(cursor: object, workspace_id: str) -> None:
        cursor.execute("SELECT set_config('app.workspace_id',%s,true)", (workspace_id,))

    @staticmethod
    def _insert_review_queue_entry(cursor: object, event: RadarEvent) -> None:
        # Queue eligibility is an operational fact. Use the database clock from
        # this transaction, never the event's source/data timestamp.
        cursor.execute("SELECT clock_timestamp()")
        entered_at = cursor.fetchone()[0]
        entry = _queue_eligibility(event, entered_at)
        if entry is None:
            return
        cursor.execute(
            """INSERT INTO review_queue_entries
            (id,event_id,eligibility_key,eligible_at,lifecycle_state,cluster_version,score_version,policy_version,entry_kind)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (eligibility_key) DO NOTHING""",
            (
                entry["id"], entry["eventId"], entry["eligibilityKey"], entry["eligibleAt"],
                entry["lifecycleState"], entry["clusterVersion"], entry["scoreVersion"], entry["policyVersion"], entry["entryKind"],
            ),
        )

    def add_feedback(self, request: FeedbackRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT clock_timestamp()")
                created_at = cursor.fetchone()[0]
                receipt = MutationReceipt(
                    id=str(uuid.uuid4()), operation=f"feedback.{request.action}", createdAt=created_at,
                )
                cursor.execute(
                    """SELECT eligibility_key FROM review_queue_entries
                    WHERE event_id=%s AND eligible_at<=%s
                    ORDER BY eligible_at DESC,eligibility_key DESC LIMIT 1 FOR SHARE""",
                    (request.event_id, receipt.created_at),
                )
                queue_row = cursor.fetchone()
                if request.action in TRIAGE_ACTIONS and (
                    queue_row is None or queue_row[0] != request.queue_eligibility_key
                ):
                    raise ValueError("queue eligibility is stale, unrelated, or not yet effective")
                cursor.execute(
                    """SELECT idempotency_key FROM alert_deliveries
                    WHERE workspace_id=%s AND event_id=%s AND idempotency_key=%s
                    AND status='delivered' AND delivered_at<=%s LIMIT 1""",
                    (workspace_id, request.event_id, request.alert_delivery_key, receipt.created_at),
                )
                alert_row = cursor.fetchone()
                if request.alert_delivery_key and alert_row is None:
                    raise ValueError("alert delivery does not belong to this workspace/event or is not delivered")
                cursor.execute(
                    """INSERT INTO feedback
                    (id,workspace_id,event_id,actor_id,action,reason,target_event_id,queue_eligibility_key,alert_delivery_key,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        receipt.id, workspace_id, request.event_id, actor_id, request.action, request.reason,
                        request.target_event_id, request.queue_eligibility_key, request.alert_delivery_key,
                        receipt.created_at,
                    ),
                )
                cursor.execute("INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('feedback.created',%s,%s::jsonb)", (receipt.id, json.dumps({"feedbackId": receipt.id})))
            connection.commit()
        return receipt

    def list_feedback(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT id,event_id,actor_id,action,reason,target_event_id,created_at,queue_eligibility_key,alert_delivery_key
                    FROM feedback WHERE workspace_id=%s AND created_at>=%s ORDER BY created_at""",
                    (workspace_id, since),
                )
                rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]), "workspaceId": workspace_id, "eventId": row[1], "actorId": row[2],
                "action": row[3], "reason": row[4], "targetEventId": row[5], "createdAt": row[6],
                "queueEligibilityKey": row[7], "alertDeliveryKey": row[8],
            }
            for row in rows
        ]

    def list_review_queue_entries(self, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT id,event_id,eligibility_key,eligible_at,lifecycle_state,cluster_version,score_version,policy_version,entry_kind
                    FROM review_queue_entries WHERE eligible_at>=%s ORDER BY eligible_at""",
                    (since,),
                )
                rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]), "eventId": row[1], "eligibilityKey": row[2], "eligibleAt": row[3],
                "lifecycleState": row[4], "clusterVersion": row[5], "scoreVersion": row[6], "policyVersion": row[7], "entryKind": row[8],
            }
            for row in rows
        ]

    def set_behavior_applicability(self, event_id: str, request: BehaviorApplicabilityRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), status="queued", operation="event.behavior_applicability", createdAt=utcnow())
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT current_score FROM events WHERE id=%s", (event_id,))
                existing_row = cursor.fetchone()
                if existing_row is None:
                    raise KeyError(event_id)
                current_payload = existing_row[0]
                current_coverage = float(current_payload.get("coverage", 0))
                current_state = current_payload.get("behaviorEvidenceState", "missing")
                if request.state == "not_applicable" and current_state != "not_applicable":
                    current_coverage = min(100, current_coverage + 32.5)
                elif request.state == "missing" and current_state == "not_applicable":
                    current_coverage = max(0, current_coverage - 32.5)
                cursor.execute(
                    """
                    UPDATE events SET lifecycle_state='insufficient_data',structure_labels='{}',
                      current_score=current_score || %s::jsonb,updated_at=now()
                    WHERE id=%s
                    """,
                    (json.dumps({
                        "behaviorEvidenceState": request.state, "state": "insufficient_data", "labels": [],
                        "behavior": 0, "gapResidual": 0, "coverage": current_coverage,
                        "driver": "行为适用性已人工修订，重新评分完成前不输出强结论。",
                        "coverageNote": "行为适用性已人工修订并更新覆盖分母；重新评分已排队。",
                    }, ensure_ascii=False), event_id),
                )
                cursor.execute(
                    """
                    UPDATE observation_processing SET revision=revision+1,updated_at=now()
                    WHERE observation_id IN (SELECT observation_id FROM event_observations WHERE event_id=%s)
                    """,
                    (event_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO feedback (id,workspace_id,event_id,actor_id,action,reason)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    (receipt.id, workspace_id, event_id, actor_id, f"behavior_{request.state}", request.reason),
                )
                cursor.execute(
                    "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('event.rescore.requested',%s,%s::jsonb)",
                    (event_id, json.dumps({"state": request.state, "reason": request.reason, "workspaceId": workspace_id, "actorId": actor_id}, ensure_ascii=False)),
                )
            connection.commit()
        return receipt

    def queue_cluster_edit(self, event_id: str, operation: str, request: ClusterEditRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), status="queued", operation=f"cluster.{operation}", createdAt=utcnow())
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                parent_ids = [event_id] + ([request.target_event_id] if operation == "merge" and request.target_event_id else [])
                cursor.execute("SELECT id,cluster_version,cardinality(superseded_by) FROM events WHERE id=ANY(%s) FOR SHARE", (parent_ids,))
                version_rows = cursor.fetchall()
                if len(version_rows) != len(parent_ids) or any(row[2] for row in version_rows):
                    raise ValueError("source or target event is missing or superseded")
                expected_versions = {row[0]: row[1] for row in version_rows}
                if operation == "split":
                    cursor.execute("SELECT observation_id FROM event_observations WHERE event_id=%s", (event_id,))
                    assigned = {row[0] for row in cursor.fetchall()}
                    requested = set(request.observation_ids)
                    if not requested.issubset(assigned):
                        raise ValueError("split observations must belong to the source event")
                    if requested == assigned:
                        raise ValueError("split must leave at least one observation in the source event")
                cursor.execute(
                    """
                    INSERT INTO cluster_edit_requests
                      (id,workspace_id,event_id,actor_id,operation,target_event_id,observation_ids,reason,status,expected_versions)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'queued',%s::jsonb)
                    """,
                    (receipt.id, workspace_id, event_id, actor_id, operation, request.target_event_id, request.observation_ids, request.reason, json.dumps(expected_versions)),
                )
                cursor.execute(
                    "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('cluster.edit.requested',%s,%s::jsonb)",
                    (event_id, json.dumps({"operationId": receipt.id, "operation": operation, "workspaceId": workspace_id})),
                )
            connection.commit()
        return receipt

    def list_cluster_edits(self, event_id: str, workspace_id: str) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    SELECT id,event_id,operation,target_event_id,observation_ids,status,created_at,completed_at,result_event_ids,reverted_at
                    FROM cluster_edit_requests
                    WHERE workspace_id=%s AND (event_id=%s OR target_event_id=%s OR %s=ANY(result_event_ids))
                    ORDER BY created_at DESC
                    """,
                    (workspace_id, event_id, event_id, event_id),
                )
                rows = cursor.fetchall()
        return [{
            "id": str(row[0]), "eventId": row[1], "operation": row[2], "targetEventId": row[3],
            "observationIds": row[4], "status": row[5], "createdAt": row[6], "completedAt": row[7],
            "resultEventIds": row[8], "revertedAt": row[9],
        } for row in rows]

    def execute_cluster_edit(self, operation_id: str, workspace_id: str | None = None) -> dict[str, object]:
        if not workspace_id:
            raise ValueError("workspace_id is required for an RLS-scoped cluster operation")
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT event_id,operation,target_event_id,observation_ids,status,expected_versions
                    FROM cluster_edit_requests WHERE id=%s AND workspace_id=%s FOR UPDATE""",
                    (operation_id, workspace_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ValueError("cluster operation not found")
                event_id, operation, target_event_id, observation_ids, status, expected_versions = row
                if status != "queued":
                    raise ValueError("cluster operation is not queued")
                parent_ids = [event_id] + ([target_event_id] if operation == "merge" else [])
                cursor.execute(
                    "SELECT id,current_score,cluster_version,superseded_by FROM events WHERE id=ANY(%s) FOR UPDATE",
                    (parent_ids,),
                )
                parent_rows = cursor.fetchall()
                if len(parent_rows) != len(parent_ids):
                    raise ValueError("cluster version conflict")
                parent_by_id = {item[0]: item for item in parent_rows}
                if any(parent_by_id[parent_id][3] or parent_by_id[parent_id][2] != int(expected_versions[parent_id]) for parent_id in parent_ids):
                    raise ValueError("cluster version conflict")
                parents = [RadarEvent.model_validate(parent_by_id[parent_id][1]) for parent_id in parent_ids]
                cursor.execute("SELECT event_id,observation_id,cluster_score FROM event_observations WHERE event_id=ANY(%s)", (parent_ids,))
                assignments: dict[str, dict[str, float]] = {parent_id: {} for parent_id in parent_ids}
                for assigned_event_id, observation_id, cluster_score in cursor.fetchall():
                    assignments[assigned_event_id][observation_id] = float(cluster_score)
                reverse_assignments = {observation_id: assigned_event_id for assigned_event_id, members in assignments.items() for observation_id in members}
                version = max(int(parent_by_id[parent_id][2]) for parent_id in parent_ids) + 1
                if operation == "merge":
                    child_ids = [f"evt-{uuid.uuid4()}"]
                    child_members = [{observation_id: score for members in assignments.values() for observation_id, score in members.items()}]
                else:
                    requested = set(observation_ids)
                    source_members = assignments[event_id]
                    if not requested or not requested.issubset(source_members) or requested == set(source_members):
                        raise ValueError("split membership changed before execution")
                    child_ids = [f"evt-{uuid.uuid4()}", f"evt-{uuid.uuid4()}"]
                    child_members = [
                        {observation_id: score for observation_id, score in source_members.items() if observation_id in requested},
                        {observation_id: score for observation_id, score in source_members.items() if observation_id not in requested},
                    ]
                now = utcnow()
                for child_id, members in zip(child_ids, child_members, strict=True):
                    child = InMemoryRepository._successor_event(parents[0], child_id, version, operation_id, operation, len(members), event_id)
                    payload = json.dumps(child.model_dump(mode="json", by_alias=True), ensure_ascii=False)
                    cursor.execute(
                        """INSERT INTO events (id,canonical_title_zh,canonical_title_en,event_type,lifecycle_state,structure_labels,
                        first_seen_at,last_seen_at,current_score,cluster_version,parent_cluster_id,merge_operation_id,split_operation_id,effective_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s)""",
                        (child.id, child.title, child.title_en, child.event_type, child.state, [], child.first_seen, child.updated_at,
                         payload, version, event_id, operation_id if operation == "merge" else None, operation_id if operation == "split" else None, now),
                    )
                    if members:
                        cursor.execute(
                            "UPDATE event_observations SET event_id=%s,assignment_version=%s WHERE observation_id=ANY(%s)",
                            (child_id, f"human:{operation_id}", list(members)),
                        )
                        cursor.execute(
                            "UPDATE observation_processing SET revision=revision+1,updated_at=now() WHERE observation_id=ANY(%s)",
                            (list(members),),
                        )
                    cursor.execute(
                        "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('event.rescore.requested',%s,%s::jsonb)",
                        (child_id, json.dumps({"operationId": operation_id, "clusterVersion": version})),
                    )
                superseded_patch = json.dumps({"supersededBy": child_ids})
                cursor.execute(
                    "UPDATE events SET superseded_by=%s,current_score=current_score || %s::jsonb,updated_at=now() WHERE id=ANY(%s)",
                    (child_ids, superseded_patch, parent_ids),
                )
                for parent_id in parent_ids:
                    for child_id in child_ids:
                        cursor.execute(
                            "INSERT INTO event_lineage_edges (operation_id,parent_event_id,child_event_id,effective_at) VALUES (%s,%s,%s,%s)",
                            (operation_id, parent_id, child_id, now),
                        )
                inherited_watch_ids: list[str] = []
                for child_id in child_ids:
                    cursor.execute(
                        """INSERT INTO watchlists (id,workspace_id,actor_id,event_id,note,created_at)
                        SELECT gen_random_uuid(),workspace_id,actor_id,%s,note,created_at FROM watchlists
                        WHERE workspace_id=%s AND event_id=ANY(%s) ON CONFLICT (workspace_id,event_id) DO NOTHING RETURNING id""",
                        (child_id, workspace_id, parent_ids),
                    )
                    inherited_watch_ids.extend(str(item[0]) for item in cursor.fetchall())
                reverse_payload = {
                    "assignments": reverse_assignments, "parentEventIds": parent_ids,
                    "inheritedWatchIds": inherited_watch_ids,
                }
                result_versions = {child_id: version for child_id in child_ids}
                cursor.execute(
                    """UPDATE cluster_edit_requests SET status='completed',completed_at=%s,result_event_ids=%s,
                    result_versions=%s::jsonb,reverse_payload=%s::jsonb WHERE id=%s""",
                    (now, child_ids, json.dumps(result_versions), json.dumps(reverse_payload), operation_id),
                )
            connection.commit()
        return {
            "id": operation_id, "eventId": event_id, "operation": operation, "targetEventId": target_event_id,
            "observationIds": observation_ids, "status": "completed", "completedAt": now, "resultEventIds": child_ids,
            "resultVersions": result_versions,
        }

    def revert_cluster_edit(self, operation_id: str, workspace_id: str) -> dict[str, object]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT event_id,operation,target_event_id,status,result_event_ids,result_versions,reverse_payload
                    FROM cluster_edit_requests WHERE id=%s AND workspace_id=%s FOR UPDATE""",
                    (operation_id, workspace_id),
                )
                row = cursor.fetchone()
                if row is None:
                    raise ValueError("cluster operation not found")
                event_id, operation, target_event_id, status, child_ids, result_versions, reverse = row
                if status != "completed":
                    raise ValueError("only a completed cluster operation can be reverted")
                cursor.execute("SELECT id,cluster_version,superseded_by FROM events WHERE id=ANY(%s) FOR UPDATE", (child_ids,))
                child_rows = cursor.fetchall()
                child_by_id = {item[0]: item for item in child_rows}
                if len(child_rows) != len(child_ids) or any(child_by_id[child_id][2] or child_by_id[child_id][1] != int(result_versions[child_id]) for child_id in child_ids):
                    raise ValueError("cluster version conflict")
                parent_ids = list(reverse["parentEventIds"])
                cursor.execute("SELECT observation_id,cluster_score FROM event_observations WHERE event_id=ANY(%s)", (child_ids,))
                current_scores = {item[0]: float(item[1]) for item in cursor.fetchall()}
                for parent_id in parent_ids:
                    member_ids = [observation_id for observation_id, original_parent in reverse["assignments"].items() if original_parent == parent_id and observation_id in current_scores]
                    if member_ids:
                        cursor.execute(
                            "UPDATE event_observations SET event_id=%s,assignment_version=%s WHERE observation_id=ANY(%s)",
                            (parent_id, f"revert:{operation_id}", member_ids),
                        )
                        cursor.execute("UPDATE observation_processing SET revision=revision+1,updated_at=now() WHERE observation_id=ANY(%s)", (member_ids,))
                    cursor.execute(
                        """UPDATE events SET superseded_by='{}',cluster_version=cluster_version+1,
                        current_score=current_score || jsonb_build_object('supersededBy','[]'::jsonb,'clusterVersion',cluster_version+1),
                        updated_at=now() WHERE id=%s""",
                        (parent_id,),
                    )
                    cursor.execute(
                        "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('event.rescore.requested',%s,%s::jsonb)",
                        (parent_id, json.dumps({"operationId": operation_id, "reverted": True})),
                    )
                child_patch = json.dumps({"supersededBy": parent_ids})
                cursor.execute(
                    "UPDATE events SET superseded_by=%s,current_score=current_score || %s::jsonb,updated_at=now() WHERE id=ANY(%s)",
                    (parent_ids, child_patch, child_ids),
                )
                inherited_ids = list(reverse.get("inheritedWatchIds", []))
                if inherited_ids:
                    cursor.execute("DELETE FROM watchlists WHERE workspace_id=%s AND id::text=ANY(%s)", (workspace_id, inherited_ids))
                now = utcnow()
                cursor.execute("UPDATE event_lineage_edges SET reverted_at=%s WHERE operation_id=%s", (now, operation_id))
                cursor.execute("UPDATE cluster_edit_requests SET status='reverted',reverted_at=%s WHERE id=%s", (now, operation_id))
            connection.commit()
        return {
            "id": operation_id, "eventId": event_id, "operation": operation, "targetEventId": target_event_id,
            "status": "reverted", "revertedAt": now, "resultEventIds": child_ids,
        }

    def execute_pending_cluster_edits(self, limit: int = 20) -> int:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT payload->>'operationId',payload->>'workspaceId' FROM outbox
                    WHERE kind='cluster.edit.requested' AND payload->>'clusterExecutedAt' IS NULL ORDER BY created_at LIMIT %s""",
                    (limit,),
                )
                pending = cursor.fetchall()
        completed = 0
        for operation_id, workspace_id in pending:
            if not operation_id or not workspace_id:
                continue
            try:
                self.execute_cluster_edit(operation_id, workspace_id)
            except ValueError as exc:
                with self.connection() as connection:
                    with connection.cursor() as cursor:
                        self._set_workspace(cursor, workspace_id)
                        cursor.execute(
                            """UPDATE cluster_edit_requests SET
                            status=CASE WHEN status='queued' THEN 'failed' ELSE status END,
                            error=CASE WHEN status='queued' THEN %s ELSE error END,
                            completed_at=CASE WHEN status='queued' THEN now() ELSE completed_at END
                            WHERE id=%s""",
                            (str(exc), operation_id),
                        )
                        cursor.execute(
                            """UPDATE outbox SET payload=jsonb_set(
                            jsonb_set(payload,'{clusterExecutedAt}',to_jsonb(now()::text),true),
                            '{clusterExecutionError}',to_jsonb(%s::text),true)
                            WHERE kind='cluster.edit.requested' AND payload->>'operationId'=%s""",
                            (str(exc), operation_id),
                        )
                    connection.commit()
                continue
            with self.connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """UPDATE outbox SET payload=jsonb_set(payload,'{clusterExecutedAt}',to_jsonb(now()::text),true)
                        WHERE kind='cluster.edit.requested' AND payload->>'operationId'=%s""",
                        (operation_id,),
                    )
                connection.commit()
            completed += 1
        return completed

    def get_event_lineage(self, event_id: str) -> dict[str, list[dict[str, object]]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT operation_id,parent_event_id,child_event_id,effective_at,reverted_at
                    FROM event_lineage_edges WHERE parent_event_id=%s OR child_event_id=%s ORDER BY effective_at""",
                    (event_id, event_id),
                )
                rows = cursor.fetchall()
        values = [{
            "operationId": str(row[0]), "parentEventId": row[1], "childEventId": row[2],
            "effectiveAt": row[3], "revertedAt": row[4],
        } for row in rows]
        return {
            "parents": [item for item in values if item["childEventId"] == event_id],
            "children": [item for item in values if item["parentEventId"] == event_id],
        }

    def add_alert(self, request: AlertRuleRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        receipt = MutationReceipt(id=str(uuid.uuid4()), status="completed", operation="alert_rule.create", createdAt=utcnow())
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("INSERT INTO alert_rules (id,workspace_id,actor_id,payload) VALUES (%s,%s,%s,%s::jsonb)", (receipt.id, workspace_id, actor_id, request.model_dump_json(by_alias=True)))
            connection.commit()
        return receipt

    def list_alert_rules(self, workspace_id: str) -> list[tuple[str, AlertRuleRequest]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT id,payload FROM alert_rules WHERE workspace_id=%s ORDER BY created_at", (workspace_id,))
                rows = cursor.fetchall()
        return [(str(row[0]), AlertRuleRequest.model_validate(row[1])) for row in rows]

    def list_alert_deliveries(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    SELECT id,rule_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,delivered_at,channel,idempotency_key,status,terminal_reason
                    FROM alert_deliveries WHERE workspace_id=%s AND delivered_at >= %s
                      AND (status IN ('delivered','aborted') OR (status='reserved' AND delivered_at >= now()-interval '15 minutes'))
                    ORDER BY delivered_at
                    """,
                    (workspace_id, since),
                )
                rows = cursor.fetchall()
        return [{
            "deliveryId": str(row[0]), "ruleId": str(row[1]), "workspaceId": workspace_id,
            "eventId": row[2], "domain": row[3], "lifecycleState": row[4], "evidenceStrength": row[5],
            "evidenceCount": row[6], "deliveredAt": row[7], "channel": row[8],
            "idempotencyKey": row[9], "status": row[10], "terminalReason": row[11],
        } for row in rows]

    def last_alert_delivery(self, workspace_id: str, event_id: str) -> dict[str, object] | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    SELECT id,rule_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,delivered_at,channel,idempotency_key,status,terminal_reason
                    FROM alert_deliveries WHERE workspace_id=%s AND event_id=%s
                      AND status='delivered'
                    ORDER BY delivered_at DESC LIMIT 1
                    """,
                    (workspace_id, event_id),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "deliveryId": str(row[0]), "ruleId": str(row[1]), "workspaceId": workspace_id,
            "eventId": row[2], "domain": row[3], "lifecycleState": row[4], "evidenceStrength": row[5],
            "evidenceCount": row[6], "deliveredAt": row[7], "channel": row[8],
            "idempotencyKey": row[9], "status": row[10], "terminalReason": row[11],
        }

    def get_alert_delivery_by_idempotency(self, workspace_id: str, idempotency_key: str) -> dict[str, object] | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    SELECT id,rule_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,delivered_at,channel,idempotency_key,status,terminal_reason
                    FROM alert_deliveries WHERE workspace_id=%s AND idempotency_key=%s
                    """,
                    (workspace_id, idempotency_key),
                )
                row = cursor.fetchone()
        if row is None:
            return None
        return {
            "deliveryId": str(row[0]), "ruleId": str(row[1]), "workspaceId": workspace_id,
            "eventId": row[2], "domain": row[3], "lifecycleState": row[4], "evidenceStrength": row[5],
            "evidenceCount": row[6], "deliveredAt": row[7], "channel": row[8],
            "idempotencyKey": row[9], "status": row[10], "terminalReason": row[11],
        }

    def list_alert_reservations_for_message(self, workspace_id: str, event_id: str, message_token: str) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    SELECT id,rule_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,delivered_at,channel,idempotency_key,status,terminal_reason
                    FROM alert_deliveries
                    WHERE workspace_id=%s AND event_id=%s AND status='reserved' AND idempotency_key LIKE %s
                    ORDER BY delivered_at
                    """,
                    (workspace_id, event_id, f"%:{message_token}"),
                )
                rows = cursor.fetchall()
        return [{
            "deliveryId": str(row[0]), "ruleId": str(row[1]), "workspaceId": workspace_id,
            "eventId": row[2], "domain": row[3], "lifecycleState": row[4], "evidenceStrength": row[5],
            "evidenceCount": row[6], "deliveredAt": row[7], "channel": row[8],
            "idempotencyKey": row[9], "status": row[10], "terminalReason": row[11],
        } for row in rows]

    def record_alert_delivery(self, item: dict[str, object]) -> None:
        workspace_id = str(item["workspaceId"])
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """
                    INSERT INTO alert_deliveries
                      (rule_id,workspace_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,channel,delivered_at,idempotency_key,status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'delivered')
                    """,
                    (item["ruleId"], workspace_id, item["eventId"], item["domain"], item["lifecycleState"],
                     item["evidenceStrength"], item["evidenceCount"], item["channel"], item["deliveredAt"],
                     item.get("idempotencyKey", str(uuid.uuid4()))),
                )
            connection.commit()

    def reserve_alert_delivery(self, item: dict[str, object], *, workspace_limit: int = 10, domain_limit: int = 3, cooldown: timedelta = timedelta(hours=4), allow_cooldown_bypass: bool = False) -> bool:
        workspace_id = str(item["workspaceId"])
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (workspace_id,))
                cursor.execute("SELECT 1 FROM alert_deliveries WHERE idempotency_key=%s", (item["idempotencyKey"],))
                if cursor.fetchone():
                    connection.commit()
                    return False
                cursor.execute(
                    """SELECT count(*),count(*) FILTER (WHERE domain=%s) FROM alert_deliveries
                    WHERE workspace_id=%s
                      AND (delivered_at AT TIME ZONE 'UTC')::date=(%s::timestamptz AT TIME ZONE 'UTC')::date
                      AND (status='delivered' OR (status='reserved' AND delivered_at >= now()-interval '15 minutes'))""",
                    (item["domain"], workspace_id, item["deliveredAt"]),
                )
                budget_row = cursor.fetchone()
                if int(budget_row[0]) >= workspace_limit or int(budget_row[1]) >= domain_limit:
                    connection.commit()
                    return False
                if not allow_cooldown_bypass:
                    cursor.execute(
                        """SELECT 1 FROM alert_deliveries
                        WHERE workspace_id=%s AND event_id=%s AND delivered_at >= %s
                          AND (status='delivered' OR (status='reserved' AND delivered_at >= now()-interval '15 minutes')) LIMIT 1""",
                        (workspace_id, item["eventId"], item["deliveredAt"] - cooldown),
                    )
                    if cursor.fetchone():
                        connection.commit()
                        return False
                cursor.execute(
                    """
                    INSERT INTO alert_deliveries
                      (rule_id,workspace_id,event_id,domain,lifecycle_state,evidence_strength,evidence_count,channel,delivered_at,idempotency_key,status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'reserved')
                    """,
                    (item["ruleId"], workspace_id, item["eventId"], item["domain"], item["lifecycleState"],
                     item["evidenceStrength"], item["evidenceCount"], item["channel"], item["deliveredAt"], item["idempotencyKey"]),
                )
            connection.commit()
        return True

    def confirm_alert_delivery(self, idempotency_key: str, workspace_id: str | None = None) -> bool:
        if not workspace_id:
            raise ValueError("workspace_id is required for an RLS-scoped alert confirmation")
        status: str | None = None
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """UPDATE alert_deliveries SET status='delivered'
                    WHERE workspace_id=%s AND idempotency_key=%s AND status='reserved'
                    RETURNING status""",
                    (workspace_id, idempotency_key),
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        "SELECT status FROM alert_deliveries WHERE workspace_id=%s AND idempotency_key=%s",
                        (workspace_id, idempotency_key),
                    )
                    existing = cursor.fetchone()
                    status = str(existing[0]) if existing else None
                else:
                    status = str(row[0])
            connection.commit()
        if status != "delivered":
            raise RuntimeError(f"alert reservation {idempotency_key} is not confirmable")
        return True

    def abort_alert_reservation(self, idempotency_key: str, reason: str, workspace_id: str | None = None) -> bool:
        if not workspace_id:
            raise ValueError("workspace_id is required for an RLS-scoped alert abort")
        if not reason.strip():
            raise ValueError("alert reservation terminal reason is required")
        status: str | None = None
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """UPDATE alert_deliveries SET status='aborted',terminal_reason=%s
                    WHERE workspace_id=%s AND idempotency_key=%s AND status='reserved'
                    RETURNING status""",
                    (reason[:1000], workspace_id, idempotency_key),
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        "SELECT status FROM alert_deliveries WHERE workspace_id=%s AND idempotency_key=%s",
                        (workspace_id, idempotency_key),
                    )
                    existing = cursor.fetchone()
                    status = str(existing[0]) if existing else None
                else:
                    status = str(row[0])
            connection.commit()
        if status != "aborted":
            raise RuntimeError(f"alert reservation {idempotency_key} is not abortable")
        return True

    def release_alert_reservation(self, idempotency_key: str, workspace_id: str | None = None) -> None:
        if not workspace_id:
            raise ValueError("workspace_id is required for an RLS-scoped alert release")
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("DELETE FROM alert_deliveries WHERE workspace_id=%s AND idempotency_key=%s AND status='reserved'", (workspace_id, idempotency_key))
            connection.commit()

    def add_watchlist(self, request: WatchlistRequest, workspace_id: str, actor_id: str) -> MutationReceipt:
        proposed_id = str(uuid.uuid4())
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """INSERT INTO watchlists (id,workspace_id,actor_id,event_id,note)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (workspace_id,event_id) DO UPDATE SET actor_id=EXCLUDED.actor_id,note=EXCLUDED.note
                    RETURNING id,created_at,(xmax = 0) AS inserted""",
                    (proposed_id, workspace_id, actor_id, request.event_id, request.note),
                )
                item_id, created_at, inserted = cursor.fetchone()
            connection.commit()
        return MutationReceipt(id=str(item_id), status="completed", operation="watchlist.create" if inserted else "watchlist.update", createdAt=created_at)

    def list_watchlists(self, workspace_id: str) -> list[WatchlistItem]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """WITH RECURSIVE watched(actor_id,event_id,note,created_at) AS (
                      SELECT actor_id,event_id,note,created_at FROM watchlists WHERE workspace_id=%s
                      UNION
                      SELECT watched.actor_id,successor.event_id,watched.note,watched.created_at
                      FROM watched JOIN events ON events.id=watched.event_id
                      CROSS JOIN LATERAL unnest(events.superseded_by) AS successor(event_id)
                    )
                    INSERT INTO watchlists (id,workspace_id,actor_id,event_id,note,created_at)
                    SELECT gen_random_uuid(),%s,watched.actor_id,watched.event_id,watched.note,watched.created_at FROM watched
                    JOIN events ON events.id=watched.event_id
                    WHERE cardinality(events.superseded_by)=0
                    ON CONFLICT (workspace_id,event_id) DO NOTHING""",
                    (workspace_id, workspace_id),
                )
                cursor.execute(
                    """SELECT watchlists.id,watchlists.event_id,watchlists.note,watchlists.created_at
                    FROM watchlists JOIN events ON events.id=watchlists.event_id
                    WHERE watchlists.workspace_id=%s AND cardinality(events.superseded_by)=0
                    ORDER BY watchlists.created_at DESC""",
                    (workspace_id,),
                )
                return [WatchlistItem(id=str(row[0]), eventId=row[1], note=row[2], createdAt=row[3]) for row in cursor.fetchall()]

    def remove_watchlist(self, event_id: str, workspace_id: str) -> MutationReceipt | None:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("DELETE FROM watchlists WHERE workspace_id=%s AND event_id=%s RETURNING id", (workspace_id, event_id))
                row = cursor.fetchone()
            connection.commit()
        if row is None:
            return None
        return MutationReceipt(id=str(row[0]), status="completed", operation="watchlist.delete", createdAt=utcnow())

    def create_metric_incident(
        self,
        request: MetricIncidentRequest,
        workspace_id: str,
        actor_id: str,
    ) -> MutationReceipt:
        incident_id = str(uuid.uuid4())
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT clock_timestamp()")
                created_at = cursor.fetchone()[0]
                fact = {
                    "id": incident_id, "workspaceId": workspace_id, "actorId": actor_id,
                    **request.model_dump(mode="json", by_alias=True), "createdAt": created_at,
                }
                fact_digest = "sha256:" + hashlib.sha256(
                    json.dumps(fact, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode()
                ).hexdigest()
                cursor.execute(
                    """SELECT idempotency_key,event_id,delivered_at,status FROM alert_deliveries
                    WHERE workspace_id=%s AND idempotency_key IN (%s,%s) FOR SHARE""",
                    (workspace_id, request.target_key, request.canonical_key),
                )
                deliveries = {str(row[0]): row for row in cursor.fetchall()}
                target = deliveries.get(request.target_key)
                canonical = deliveries.get(request.canonical_key)
                if (
                    target is None or canonical is None
                    or target[1] != request.event_id or canonical[1] != request.event_id
                    or target[3] != "delivered" or canonical[3] != "delivered"
                    or target[2] <= canonical[2]
                    or target[2] > canonical[2] + timedelta(minutes=15)
                    or created_at > target[2] + timedelta(minutes=15)
                ):
                    raise ValueError("incident requires two matching delivered alerts in canonical order within 15 minutes")
                cursor.execute(
                    """INSERT INTO metric_incidents
                    (id,workspace_id,actor_id,event_id,target_key,canonical_key,cause,note,fact_digest,created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        incident_id, workspace_id, actor_id, request.event_id, request.target_key,
                        request.canonical_key, request.cause, request.note, fact_digest, created_at,
                    ),
                )
            connection.commit()
        return MutationReceipt(
            id=incident_id, status="completed", operation="metric_incident.create", createdAt=created_at,
        )

    def list_metric_incidents(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT id,actor_id,event_id,target_key,canonical_key,cause,note,fact_digest,created_at
                    FROM metric_incidents WHERE workspace_id=%s AND created_at>=%s ORDER BY created_at""",
                    (workspace_id, since),
                )
                rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]), "workspaceId": workspace_id, "actorId": row[1], "eventId": row[2],
                "targetKey": row[3], "canonicalKey": row[4], "cause": row[5], "note": row[6],
                "factDigest": row[7], "createdAt": row[8],
            }
            for row in rows
        ]

    def record_product_interaction(
        self,
        request: ProductInteractionRequest,
        workspace_id: str,
        actor_id: str,
        occurred_at: datetime | None = None,
    ) -> MutationReceipt:
        proposed_id = str(uuid.uuid4())
        metadata = request.metadata.copy()
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute("SELECT clock_timestamp()")
                proposed_at = cursor.fetchone()[0]
                if request.kind == "metric_exclusion_recorded":
                    cursor.execute(
                        """SELECT event_id,target_key,canonical_key,fact_digest,created_at FROM metric_incidents
                        WHERE workspace_id=%s AND id=%s""",
                        (workspace_id, str(metadata.get("incidentId") or "")),
                    )
                    incident = cursor.fetchone()
                    if (
                        incident is None
                        or incident[0] != request.event_id
                        or incident[1] != metadata.get("targetKey")
                        or incident[2] != metadata.get("canonicalKey")
                        or incident[4] > proposed_at
                    ):
                        raise ValueError("metric exclusion must reference a matching immutable incident fact")
                    metadata["incidentFactDigest"] = str(incident[3])
                cursor.execute(
                    """INSERT INTO product_interactions
                    (id,workspace_id,actor_id,event_id,session_id,idempotency_key,kind,metadata,occurred_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                    ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
                    RETURNING id,occurred_at,kind""",
                    (proposed_id, workspace_id, actor_id, request.event_id, request.session_id, request.idempotency_key,
                     request.kind, json.dumps(metadata, ensure_ascii=False), proposed_at),
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        "SELECT id,occurred_at,kind FROM product_interactions WHERE workspace_id=%s AND idempotency_key=%s",
                        (workspace_id, request.idempotency_key),
                    )
                    row = cursor.fetchone()
            connection.commit()
        return MutationReceipt(id=str(row[0]), status="completed", operation=f"interaction.{row[2]}", createdAt=row[1])

    def list_product_interactions(self, workspace_id: str, since: datetime) -> list[dict[str, object]]:
        with self.connection() as connection:
            with connection.cursor() as cursor:
                self._set_workspace(cursor, workspace_id)
                cursor.execute(
                    """SELECT id,actor_id,session_id,event_id,idempotency_key,kind,metadata,occurred_at FROM product_interactions
                    WHERE workspace_id=%s AND occurred_at>=%s ORDER BY occurred_at""",
                    (workspace_id, since),
                )
                rows = cursor.fetchall()
        return [
            {
                "id": str(row[0]), "actorId": row[1], "sessionId": row[2], "eventId": row[3],
                "idempotencyKey": row[4], "kind": row[5], "metadata": row[6], "occurredAt": row[7],
            }
            for row in rows
        ]

    def purge_source(self, source_id: str) -> int:
        with self.deletion_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT id FROM observations WHERE source_id=%s", (source_id,))
                observation_ids = [row[0] for row in cursor.fetchall()]
                cursor.execute(
                    """
                    SELECT DISTINCT eo.event_id
                    FROM event_observations eo JOIN observations o ON o.id=eo.observation_id
                    WHERE o.source_id=%s
                    UNION
                    SELECT id FROM events WHERE current_score->'evidence' @> %s::jsonb
                    """,
                    (source_id, json.dumps([{"source": source_id}])),
                )
                affected_event_ids = [row[0] for row in cursor.fetchall()]
                if affected_event_ids:
                    cursor.execute(
                        "SELECT event_ids FROM erase_source_score_history(%s)",
                        (source_id,),
                    )
                    authorized_event_ids = list(cursor.fetchone()[0])
                    if set(authorized_event_ids) != set(affected_event_ids):
                        raise RuntimeError("source erasure event set changed during authorization")
                cursor.execute(
                    """
                    WITH candidate(ref) AS (
                      SELECT deleting.raw_evidence_ref FROM observations deleting WHERE deleting.source_id=%s
                      UNION
                      SELECT snapshots.source_revision
                      FROM metric_snapshots snapshots
                      JOIN observations deleting ON deleting.id=snapshots.subject_id
                      WHERE snapshots.subject_type='content' AND deleting.source_id=%s
                    ), retained(ref) AS (
                      SELECT kept.raw_evidence_ref FROM observations kept WHERE kept.source_id<>%s
                      UNION
                      SELECT snapshots.source_revision
                      FROM metric_snapshots snapshots
                      JOIN observations kept ON kept.id=snapshots.subject_id
                      WHERE snapshots.subject_type='content' AND kept.source_id<>%s
                    )
                    SELECT ref FROM candidate WHERE ref IS NOT NULL AND ref<>''
                    EXCEPT SELECT ref FROM retained WHERE ref IS NOT NULL AND ref<>''
                    """,
                    (source_id, source_id, source_id, source_id),
                )
                raw_refs = [row[0] for row in cursor.fetchall()]
                if observation_ids:
                    cursor.execute("DELETE FROM metric_snapshots WHERE subject_type='content' AND subject_id=ANY(%s)", (observation_ids,))
                cursor.execute("DELETE FROM observations WHERE source_id=%s", (source_id,))
                deleted = cursor.rowcount
                cursor.execute("DELETE FROM sources WHERE id=%s", (source_id,))
                reset_at = utcnow()
                for event_id in affected_event_ids:
                    cursor.execute(
                        """
                        SELECT o.id,COALESCE(NULLIF(o.title,''),left(o.body,120),'未命名 AI 事件')
                        FROM event_observations eo JOIN observations o ON o.id=eo.observation_id
                        WHERE eo.event_id=%s ORDER BY o.published_at,o.collected_at LIMIT 1
                        """,
                        (event_id,),
                    )
                    title_row = cursor.fetchone()
                    if title_row is None:
                        # Keep an inert, hidden tombstone so the application role
                        # never needs DELETE on events (which would cascade into
                        # immutable score history). All source-derived content is
                        # replaced and the non-empty successor marker excludes it
                        # from active-event queries.
                        erased_successor = f"erased:{source_id}"
                        cursor.execute(
                            """UPDATE events SET canonical_title_zh='已删除事件',
                              canonical_title_en='Deleted event',lifecycle_state='insufficient_data',
                              structure_labels='{}',superseded_by=%s,
                              current_score=current_score || %s::jsonb,updated_at=now()
                            WHERE id=%s""",
                            ([erased_successor], json.dumps({
                                "title": "已删除事件", "titleEn": "Deleted event",
                                "state": "insufficient_data", "labels": [],
                                "classificationStatus": "unsupported",
                                "unsupportedReason": "source_erased", "attention": 0,
                                "behavior": 0, "coverage": 0, "evidenceScore": 0,
                                "uncertainty": 100, "independentSources": 0,
                                "platforms": [], "signalFamilies": [], "evidenceCount": 0,
                                "evidence": [], "supersededBy": [erased_successor],
                                "driver": "来源删除后事件已转为不可见审计墓碑。",
                                "coverageNote": "全部合法成员已删除。",
                            }, ensure_ascii=False), event_id),
                        )
                        continue
                    rebuilt_title = str(title_row[1])
                    cursor.execute(
                        """
                        UPDATE observation_processing SET revision=revision+1,updated_at=now()
                        WHERE observation_id IN (
                          SELECT observation_id FROM event_observations WHERE event_id=%s
                        )
                        """,
                        (event_id,),
                    )
                    cursor.execute(
                        """
                        UPDATE events SET canonical_title_zh=%s,canonical_title_en=%s,
                          lifecycle_state='insufficient_data',structure_labels='{}',
                          current_score = jsonb_set(
                            current_score || %s::jsonb,
                            '{evidence}', COALESCE((
                              SELECT jsonb_agg(item) FROM jsonb_array_elements(current_score->'evidence') item
                              WHERE item->>'source' <> %s
                            ), '[]'::jsonb)
                          ), updated_at=now()
                        WHERE id=%s
                        """,
                        (rebuilt_title, rebuilt_title, json.dumps({
                            "title": rebuilt_title, "titleEn": rebuilt_title,
                            "state": "insufficient_data", "labels": [], "attention": 0, "behavior": 0,
                            "diversity": 0, "authority": 0, "coordinationRisk": 0, "coverage": 0,
                            "velocity": 0, "gapResidual": 0, "independentSources": 0, "platforms": [],
                            "signalFamilies": [], "evidenceCount": 0,
                            "evidenceStrength": "low", "evidenceScore": 0, "uncertainty": 100,
                            "discussionEvidenceState": "missing", "behaviorEvidenceState": "missing",
                            "driver": "来源删除后派生结论已失效，等待重新评分。",
                            "coverageNote": "删除传播已清除成员和指标事实；当前不得输出强结论。",
                            "updatedAt": reset_at.isoformat(),
                        }, ensure_ascii=False), source_id, event_id),
                    )
                if affected_event_ids:
                    cursor.execute("DELETE FROM event_embeddings WHERE event_id=ANY(%s)", (affected_event_ids,))
                cache_tags = ["radar", "events", f"source:{source_id}"] + [f"event:{event_id}" for event_id in sorted(affected_event_ids)]
                cursor.execute(
                    "INSERT INTO outbox (kind,aggregate_id,payload) VALUES ('source.erased',%s,%s::jsonb)",
                    (source_id, json.dumps({"rawEvidenceRefs": raw_refs, "cacheTags": cache_tags, "affectedEventIds": sorted(affected_event_ids)})),
                )
            connection.commit()
        return deleted
