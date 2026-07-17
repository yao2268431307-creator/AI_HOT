from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
import math

from .budget import BudgetDecision, budget_guard
from .connectors import BaseConnector
from .contracts import ConnectorStatus
from .governance import connector_rights_status, rights_policy_status
from .identities import SourceIdentityResolver
from .processor import EventProcessor
from .storage import InMemoryRepository, PostgresRepository, utcnow


@dataclass(slots=True)
class ConnectorRun:
    connector_id: str
    inserted: int
    duplicates: int
    failed: bool
    error: str | None = None
    skipped: bool = False


class CollectorWorker:
    def __init__(
        self,
        repository: InMemoryRepository | PostgresRepository,
        connectors: list[BaseConnector],
        processor: EventProcessor | None = None,
        identity_resolver: SourceIdentityResolver | None = None,
        budget_decision: BudgetDecision | None = None,
        *,
        monthly_budget_limit: float | None = None,
        connector_budget_limits: dict[str, float] | None = None,
        signal_family_budget_limits: dict[str, float] | None = None,
        base_external_spend: float = 0,
        allow_unapproved_rights_for_nonproduction: bool = False,
    ) -> None:
        self.repository = repository
        self.connectors = connectors
        self.processor = processor
        self.identity_resolver = identity_resolver or SourceIdentityResolver()
        self.budget_decision = budget_decision
        self.monthly_budget_limit = monthly_budget_limit
        self.connector_budget_limits = self._validated_budget_limits(
            connector_budget_limits, "connector"
        )
        self.signal_family_budget_limits = self._validated_budget_limits(
            signal_family_budget_limits, "signal family"
        )
        self.base_external_spend = max(0, base_external_spend)
        # Deliberately has no environment-variable path in runner.py. Tests may
        # opt in explicitly, while every normal Worker construction fails closed.
        self.allow_unapproved_rights_for_nonproduction = allow_unapproved_rights_for_nonproduction
        self._budget_credit: dict[str, float] = {}
        self._budget_penalized: set[str] = set()
        self._budget_hard_pause: set[str] = set()

    @staticmethod
    def _validated_budget_limits(
        limits: dict[str, float] | None, scope: str
    ) -> dict[str, float]:
        validated: dict[str, float] = {}
        for raw_key, raw_limit in (limits or {}).items():
            key = str(raw_key).strip()
            limit = float(raw_limit)
            if not key or not math.isfinite(limit) or limit <= 0:
                raise ValueError(f"{scope} budget limits require non-empty keys and positive finite values")
            validated[key] = limit
        return validated

    def _budget_skip_reason(self, connector: BaseConnector) -> str | None:
        if not connector.metered:
            return None
        if self.monthly_budget_limit is not None:
            spend = self.base_external_spend + self.repository.monthly_connector_spend()
            self.budget_decision = budget_guard(spend, self.monthly_budget_limit)
        decision = self.budget_decision
        if decision is not None and decision.stop_paid_connectors:
            self._budget_hard_pause.add(connector.id)
            return "月度外部数据预算已用尽，付费/计量连接器已暂停"
        if connector.estimated_cost_per_request_rmb is None:
            self._budget_hard_pause.add(connector.id)
            return f"{connector.id} 未配置每请求成本；为防止静默超支已安全暂停"
        if connector.estimated_cost_per_request_rmb < 0:
            self._budget_hard_pause.add(connector.id)
            return f"{connector.id} 每请求成本为负数；配置无效，已安全暂停"
        # Each logical request may traverse at most six validated redirects per
        # retry attempt (BaseConnector.get). Reserve that worst case before any
        # provider request so a successful but expensive run cannot overspend.
        maximum_run_cost = (
            connector.estimated_cost_per_request_rmb
            * connector.expected_requests_per_collect
            * connector.max_attempts
            * 6
        )
        if self.monthly_budget_limit is not None:
            spend = self.base_external_spend + self.repository.monthly_connector_spend()
            if spend + maximum_run_cost > self.monthly_budget_limit:
                self._budget_hard_pause.add(connector.id)
                return "本轮最坏成本可能突破月度预算，计量连接器已提前暂停"
        connector_limit = self.connector_budget_limits.get(connector.id)
        if connector_limit is not None:
            connector_spend = self.repository.monthly_connector_spend(
                connector_ids={connector.id}
            )
            if connector_spend + maximum_run_cost > connector_limit:
                self._budget_hard_pause.add(connector.id)
                return (
                    f"连接器 {connector.id} 的月度预算上限为 {connector_limit:.2f} 元；"
                    "本轮最坏成本可能越界，已提前暂停"
                )
        family_limit = self.signal_family_budget_limits.get(connector.signal_family)
        if family_limit is not None:
            family_connector_ids = {
                item.id
                for item in self.connectors
                if item.signal_family == connector.signal_family
            }
            family_spend = self.repository.monthly_connector_spend(
                connector_ids=family_connector_ids
            )
            if family_spend + maximum_run_cost > family_limit:
                self._budget_hard_pause.add(connector.id)
                return (
                    f"信号族 {connector.signal_family} 的月度预算上限为 {family_limit:.2f} 元；"
                    "本轮最坏成本可能越界，已提前暂停"
                )
        if decision is None:
            return None
        multiplier = decision.collection_multiplier
        if multiplier >= 1:
            return None
        credit = self._budget_credit.get(connector.id, 1 - multiplier) + multiplier
        if credit < 1:
            self._budget_credit[connector.id] = credit
            return f"预算利用率 {decision.utilization:.0%}，本周期按策略降频"
        self._budget_credit[connector.id] = credit - 1
        return None

    @staticmethod
    def _maximum_run_cost(connector: BaseConnector) -> float:
        return (
            float(connector.estimated_cost_per_request_rmb or 0)
            * connector.expected_requests_per_collect
            * connector.max_attempts
            * 6
        )

    def _pause_for_budget(
        self, connector: BaseConnector, rights_status: str, reason: str,
    ) -> ConnectorRun:
        existing = self.repository.get_connector(connector.id)
        stopped = connector.id in self._budget_hard_pause or bool(
            self.budget_decision and self.budget_decision.stop_paid_connectors
        )
        penalty = max(
            25 if stopped else 0,
            self.budget_decision.coverage_penalty if self.budget_decision else 0,
        )
        if penalty and connector.id not in self._budget_penalized:
            self.repository.apply_connector_coverage_penalty(
                connector.platform, penalty, reason,
            )
            self._budget_penalized.add(connector.id)
        self.repository.upsert_connector(ConnectorStatus(
            id=connector.id, name=connector.platform, family=connector.signal_family,
            status="paused" if stopped else "degraded",
            latencyMinutes=existing.latency_minutes if existing else 0,
            observations24h=existing.observations_24h if existing else 0,
            # Budget pressure is a level, not a per-cycle debit. Repeated
            # skipped runs must not compound coverage down to zero.
            coverage=min(existing.coverage if existing else 90, max(0, 90 - penalty)),
            lastSuccess=existing.last_success if existing else utcnow() - timedelta(days=1),
            note=reason, rightsStatus=rights_status,
        ))
        return ConnectorRun(connector.id, 0, 0, False, None, True)

    async def run_connector(self, connector: BaseConnector) -> ConnectorRun:
        started = utcnow()
        inserted = 0
        duplicates = 0
        estimated_cost = 0.0
        rights_status = connector_rights_status(connector.id)
        if rights_status != "active" and not self.allow_unapproved_rights_for_nonproduction:
            existing = self.repository.get_connector(connector.id)
            reason = f"数据权利状态为 {rights_status}；未获 active 审批，生产采集已安全暂停"
            self.repository.upsert_connector(ConnectorStatus(
                id=connector.id, name=connector.platform, family=connector.signal_family,
                status="paused", latencyMinutes=existing.latency_minutes if existing else 0,
                observations24h=existing.observations_24h if existing else 0,
                coverage=0, lastSuccess=existing.last_success if existing else utcnow() - timedelta(days=1),
                note=reason, rightsStatus=rights_status,
            ))
            return ConnectorRun(connector.id, 0, 0, False, None, True)
        budget_reason = self._budget_skip_reason(connector)
        if budget_reason:
            return self._pause_for_budget(connector, rights_status, budget_reason)
        budget_reservation_id: str | None = None
        if connector.metered and self.monthly_budget_limit is not None:
            family_connector_ids = {
                item.id for item in self.connectors
                if item.signal_family == connector.signal_family
            }
            budget_reservation_id = self.repository.reserve_connector_budget(
                connector.id,
                connector.signal_family,
                self._maximum_run_cost(connector),
                self.monthly_budget_limit,
                self.connector_budget_limits.get(connector.id),
                self.signal_family_budget_limits.get(connector.signal_family),
                family_connector_ids,
                self.base_external_spend,
            )
            if budget_reservation_id is None:
                self._budget_hard_pause.add(connector.id)
                return self._pause_for_budget(
                    connector,
                    rights_status,
                    "本轮最坏成本无法取得原子预算预留，计量连接器已提前暂停",
                )
        try:
            connector.restore_checkpoint(self.repository.get_connector_checkpoint(connector.id))
            observations = await connector.collect()
            requests = connector.drain_request_count()
            estimated_cost = requests * float(connector.estimated_cost_per_request_rmb or 0)
            for raw_observation in observations:
                observation = self.identity_resolver.resolve(raw_observation)
                if observation.rights_policy_id == "metadata-and-excerpt":
                    observation = observation.model_copy(update={"rights_policy_id": connector.rights_policy_id})
                changed = self.repository.save_observation_with_outbox(observation)
                if changed:
                    inserted += 1
                else:
                    duplicates += 1
                revision = self.repository.claim_observation_processing(observation.id) if self.processor else None
                if revision is not None:
                    try:
                        await self.processor.process(observation)
                    except Exception as exc:
                        self.repository.fail_observation_processing(observation.id, revision, str(exc))
                        raise
                    else:
                        self.repository.complete_observation_processing(observation.id, revision)
            finished = utcnow()
            self.repository.save_connector_checkpoint(connector.id, connector.next_checkpoint(observations))
            self.repository.record_connector_run(
                connector.id, started, finished, "healthy", inserted, duplicates, 90,
                estimated_cost_rmb=estimated_cost,
                budget_reservation_id=budget_reservation_id,
            )
            observations_24h, latency = self.repository.connector_stats_24h(connector.id)
            self.repository.upsert_connector(ConnectorStatus(
                id=connector.id, name=connector.platform, family=connector.signal_family,
                status="healthy", latencyMinutes=latency, observations24h=observations_24h, coverage=90,
                lastSuccess=finished, note="最近一轮采集成功；延迟字段为 24H 采集轮次耗时 P95",
                rightsStatus=rights_status,
            ))
            return ConnectorRun(connector.id, inserted, duplicates, False)
        except Exception as exc:
            existing = self.repository.get_connector(connector.id)
            finished = utcnow()
            estimated_cost += connector.drain_request_count() * float(connector.estimated_cost_per_request_rmb or 0)
            coverage = max(0, (existing.coverage if existing else 60) - 15)
            self.repository.record_connector_run(
                connector.id, started, finished, "failed", inserted, duplicates,
                coverage, str(exc)[:1000], estimated_cost,
                budget_reservation_id=budget_reservation_id,
            )
            observations_24h, latency = self.repository.connector_stats_24h(connector.id)
            self.repository.upsert_connector(ConnectorStatus(
                id=connector.id, name=connector.platform, family=connector.signal_family,
                status="degraded", latencyMinutes=latency,
                observations24h=observations_24h,
                coverage=coverage,
                lastSuccess=existing.last_success if existing else utcnow() - timedelta(days=1),
                note=f"连接器失败，冻结上一指标：{str(exc)[:120]}",
                rightsStatus=rights_status,
            ))
            return ConnectorRun(connector.id, inserted, duplicates, True, str(exc))

    async def retry_pending(self, limit: int = 100) -> int:
        if self.processor is None:
            return 0
        completed = 0
        for observation_id in self.repository.list_pending_observation_ids(limit):
            observation = self.repository.get_latest_observation(observation_id)
            if observation is None:
                revision = self.repository.claim_observation_processing(observation_id)
                if revision is None:
                    continue
                self.repository.fail_observation_processing(observation_id, revision, "observation disappeared before retry")
                continue
            if (
                rights_policy_status(observation.rights_policy_id) != "active"
                and not self.allow_unapproved_rights_for_nonproduction
            ):
                continue
            revision = self.repository.claim_observation_processing(observation_id)
            if revision is None:
                continue
            try:
                await self.processor.process(observation)
            except Exception as exc:
                self.repository.fail_observation_processing(observation_id, revision, str(exc))
            else:
                self.repository.complete_observation_processing(observation_id, revision)
                completed += 1
        return completed

    async def run_once(self) -> list[ConnectorRun]:
        # A crash after reservation is never silently released. Expired leases
        # move to an explicit reconciliation state that remains budget-counted
        # until an operator resolves the provider charge.
        self.repository.reconcile_expired_connector_budget_reservations()
        # Cluster corrections are durable async commands. Execute them before
        # processing new content so successor memberships are the active view.
        self.repository.execute_pending_cluster_edits()
        # Retry durable pending work independently of whether the source emits
        # the observation again. This closes the collection -> processing gap.
        await self.retry_pending()
        # Free connectors can run concurrently. Metered connectors are ordered
        # so every run refreshes the durable monthly ledger before the next one
        # reserves its worst-case cost.
        indexed: dict[int, ConnectorRun] = {}
        free = [(index, connector) for index, connector in enumerate(self.connectors) if not connector.metered]
        if free:
            values = await asyncio.gather(*(self.run_connector(connector) for _, connector in free))
            indexed.update((index, value) for (index, _), value in zip(free, values, strict=True))
        for index, connector in ((index, connector) for index, connector in enumerate(self.connectors) if connector.metered):
            indexed[index] = await self.run_connector(connector)
        refresh_time_driven = getattr(self.processor, "refresh_time_driven", None)
        if callable(refresh_time_driven):
            await refresh_time_driven()
        return [indexed[index] for index in range(len(self.connectors))]

    async def close(self) -> None:
        await asyncio.gather(*(connector.close() for connector in self.connectors))
