from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
from math import floor
import os
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, Field, field_validator, model_validator

from .scoring import clamp


class SourceScoreWeights(BaseModel):
    early_hit_rate: float = Field(alias="earlyHitRate", ge=0, le=1)
    confirmed_hit_rate: float = Field(alias="confirmedHitRate", ge=0, le=1)
    originality: float = Field(ge=0, le=1)
    domain_focus: float = Field(alias="domainFocus", ge=0, le=1)
    authority: float = Field(ge=0, le=1)
    marketing_matrix_penalty: float = Field(alias="marketingMatrixPenalty", ge=0, le=1)

    model_config = {"populate_by_name": True}


class SourceScorePolicy(BaseModel):
    version: str = Field(min_length=8)
    status: str
    frozen_at: datetime = Field(alias="frozenAt")
    timezone_name: str = Field(alias="timezone")
    ranking_enabled: bool = Field(alias="rankingEnabled")
    auto_promotion_enabled: bool = Field(alias="autoPromotionEnabled")
    minimum_valid_observations: int = Field(alias="minimumValidObservations", ge=1)
    minimum_history_days: int = Field(alias="minimumHistoryDays", ge=1)
    minimum_promotion_score: float = Field(alias="minimumPromotionScore", ge=0, le=100)
    daily_growth_rate: float = Field(alias="dailyGrowthRate", gt=0, le=.05)
    daily_growth_rounding: Literal["floor"] = Field(alias="dailyGrowthRounding")
    allow_automatic_bootstrap: bool = Field(alias="allowAutomaticBootstrap")
    active_capacity: int = Field(alias="activeCapacity", ge=1)
    candidate_capacity: int = Field(alias="candidateCapacity", ge=1)
    system_capacity: int = Field(alias="systemCapacity", ge=1)
    weights: SourceScoreWeights
    notes: list[str] = Field(default_factory=list)

    model_config = {"populate_by_name": True}

    @field_validator("frozen_at")
    @classmethod
    def require_timezone_aware_freeze(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source score frozenAt must include an explicit timezone")
        return value

    @field_validator("timezone_name")
    @classmethod
    def require_valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as error:
            raise ValueError("source score timezone must be a valid IANA timezone") from error
        return value

    @model_validator(mode="after")
    def validate_capacities(self) -> "SourceScorePolicy":
        if not self.active_capacity <= self.candidate_capacity <= self.system_capacity:
            raise ValueError("source capacities must be ordered active <= candidate <= system")
        positive_weight = (
            self.weights.early_hit_rate + self.weights.confirmed_hit_rate
            + self.weights.originality + self.weights.domain_focus + self.weights.authority
        )
        if abs(positive_weight - 1) > 1e-9:
            raise ValueError("positive source score weights must sum to one")
        return self


def source_policy_path() -> Path:
    configured = os.getenv("SOURCE_SCORE_POLICY_FILE")
    return Path(configured) if configured else Path(__file__).resolve().parents[3] / "config" / "source_score_policy.json"


@lru_cache(maxsize=1)
def load_source_score_policy() -> SourceScorePolicy:
    return SourceScorePolicy.model_validate_json(source_policy_path().read_text(encoding="utf-8"))


def source_score_policy_digest(policy: SourceScorePolicy | None = None) -> str:
    active_policy = policy or load_source_score_policy()
    material = json.dumps(
        active_policy.model_dump(mode="json", by_alias=True),
        ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode()
    return "sha256:" + hashlib.sha256(material).hexdigest()


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


def candidate_score(candidate: SourceCandidate, policy: SourceScorePolicy | None = None) -> float:
    active_policy = policy or load_source_score_policy()
    weights = active_policy.weights
    hit_rate = candidate.confirmed_hits / max(1, candidate.valid_observations)
    early_rate = candidate.early_hits / max(1, candidate.valid_observations)
    risk_penalty = clamp(candidate.marketing_matrix_overlap) * weights.marketing_matrix_penalty
    value = 100 * (
        early_rate * weights.early_hit_rate
        + hit_rate * weights.confirmed_hit_rate
        + (clamp(candidate.originality) / 100) * weights.originality
        + (clamp(candidate.domain_focus) / 100) * weights.domain_focus
        + (clamp(candidate.authority) / 100) * weights.authority
        - (risk_penalty / 100)
    )
    return round(clamp(value), 2)


def evidence_eligible(
    candidate: SourceCandidate, *, now: datetime | None = None, policy: SourceScorePolicy | None = None,
) -> bool:
    active_policy = policy or load_source_score_policy()
    now = now or datetime.now(timezone.utc)
    age = now - candidate.discovered_at
    return (
        candidate.valid_observations >= active_policy.minimum_valid_observations
        and age >= timedelta(days=active_policy.minimum_history_days)
    )


def eligible(
    candidate: SourceCandidate, *, now: datetime | None = None, policy: SourceScorePolicy | None = None,
) -> bool:
    active_policy = policy or load_source_score_policy()
    return (
        evidence_eligible(candidate, now=now, policy=active_policy)
        and candidate_score(candidate, active_policy) >= active_policy.minimum_promotion_score
    )


def promote_candidates(
    candidates: list[SourceCandidate], active_count: int, *, now: datetime | None = None,
    promoted_today: int = 0, policy: SourceScorePolicy | None = None,
) -> list[SourceCandidate]:
    active_policy = policy or load_source_score_policy()
    remaining_capacity = max(0, active_policy.active_capacity - active_count)
    start_of_day_active = max(0, active_count - promoted_today)
    daily_cap = floor(start_of_day_active * active_policy.daily_growth_rate)
    if start_of_day_active == 0 and active_policy.allow_automatic_bootstrap:
        daily_cap = 1
    limit = min(remaining_capacity, max(0, daily_cap - promoted_today))
    ranked = sorted(
        (candidate for candidate in candidates if eligible(candidate, now=now, policy=active_policy)),
        key=lambda item: (-candidate_score(item, active_policy), item.discovered_at, item.id),
    )
    promoted = ranked[:limit]
    for candidate in promoted:
        candidate.score = candidate_score(candidate, active_policy)
    return promoted
