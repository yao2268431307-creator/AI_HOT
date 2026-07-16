from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil

from .scoring import clamp


@dataclass(slots=True)
class SourceCandidate:
    id: str
    discovered_at: datetime
    valid_observations: int
    early_hits: int
    confirmed_hits: int
    originality: float
    domain_focus: float
    authority: float
    marketing_matrix_overlap: float
    score: float = 0


def candidate_score(candidate: SourceCandidate) -> float:
    hit_rate = candidate.confirmed_hits / max(1, candidate.valid_observations)
    early_rate = candidate.early_hits / max(1, candidate.valid_observations)
    risk_penalty = clamp(candidate.marketing_matrix_overlap) * .35
    value = early_rate * 30 + hit_rate * 25 + clamp(candidate.originality) * .18 + clamp(candidate.domain_focus) * .12 + clamp(candidate.authority) * .15 - risk_penalty
    return round(clamp(value), 2)


def eligible(candidate: SourceCandidate, *, now: datetime | None = None, minimum_score: float = 58) -> bool:
    now = now or datetime.now(timezone.utc)
    age = now - candidate.discovered_at
    return candidate.valid_observations >= 5 and age >= timedelta(days=7) and candidate_score(candidate) >= minimum_score


def promote_candidates(candidates: list[SourceCandidate], active_count: int, *, now: datetime | None = None, daily_growth_rate: float = .05, absolute_capacity: int = 2000) -> list[SourceCandidate]:
    remaining_capacity = max(0, absolute_capacity - active_count)
    daily_cap = max(1, ceil(active_count * daily_growth_rate)) if active_count else 1
    limit = min(remaining_capacity, daily_cap)
    ranked = sorted((candidate for candidate in candidates if eligible(candidate, now=now)), key=candidate_score, reverse=True)
    promoted = ranked[:limit]
    for candidate in promoted:
        candidate.score = candidate_score(candidate)
    return promoted

