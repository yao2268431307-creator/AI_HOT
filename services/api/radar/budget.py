from __future__ import annotations

from dataclasses import dataclass

from .scoring import clamp


@dataclass(frozen=True, slots=True)
class BudgetDecision:
    utilization: float
    collection_multiplier: float
    coverage_penalty: float
    stop_paid_connectors: bool


def budget_guard(spend: float, monthly_limit: float) -> BudgetDecision:
    if monthly_limit <= 0:
        raise ValueError("monthly_limit must be positive")
    utilization = max(0, spend / monthly_limit)
    if utilization < .75:
        return BudgetDecision(utilization, 1.0, 0, False)
    if utilization < .9:
        return BudgetDecision(utilization, .7, 6, False)
    if utilization < 1:
        return BudgetDecision(utilization, .35, 14, False)
    return BudgetDecision(utilization, 0, clamp(25 + (utilization - 1) * 50), True)
