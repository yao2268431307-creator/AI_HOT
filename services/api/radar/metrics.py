from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from math import exp, log1p

from .contracts import EventType, LifecycleState
from .feature_registry import behavior_feature_rules
from .scoring import ScoreInput, clamp, robust_z


METRIC_RULES = behavior_feature_rules()
METRIC_ROLES = {event_type: {metric: str(rule["role"]) for metric, rule in rules.items()} for event_type, rules in METRIC_RULES.items()}
BEHAVIOR_METRICS: dict[EventType, tuple[str, ...]] = {event_type: tuple(roles) for event_type, roles in METRIC_ROLES.items()}


@dataclass(slots=True)
class SignalSnapshot:
    source_id: str
    source_group: str
    platform: str
    signal_family: str
    captured_at: datetime
    metrics: dict[str, float]
    previous_metrics: dict[str, float] = field(default_factory=dict)
    authority: float = 50
    verifiable: bool = True
    content_fingerprint: str = ""


@dataclass(slots=True)
class Baseline:
    metric_history: dict[str, list[float]] = field(default_factory=dict)


@dataclass(slots=True)
class MetricBundle:
    attention: float
    behavior: float
    diversity: float
    authority: float
    coordination_risk: float
    coverage: float
    verifiability: float
    platform_concentration: float
    independent_signal_families: int
    independent_platform_families: int
    independent_ownership_entities: int
    behavior_observed: bool
    primary_adoption_observed: bool
    primary_response_observed: bool
    primary_adoption_score: float
    primary_response_score: float
    official_source_present: bool
    research_source_present: bool
    discussion_source_present: bool
    drivers: list[str]
    max_anomaly_z: float = 0.0


def _normalized_growth(metric: str, value: float, previous: float, baseline: Baseline, platform: str) -> float:
    growth = max(0.0, value - previous)
    history = baseline.metric_history.get(f"{platform.lower()}:{metric}", baseline.metric_history.get(metric, []))
    if history:
        z = clamp(robust_z(growth, history), -4, 8)
        return round(100 / (1 + exp(-0.9 * (z - 1))), 2)
    return clamp(log1p(growth) * 11)


def _growth_anomaly_z(metric: str, value: float, previous: float, baseline: Baseline, platform: str) -> float:
    history = baseline.metric_history.get(f"{platform.lower()}:{metric}", baseline.metric_history.get(metric, []))
    if not history:
        return 0.0
    return round(clamp(robust_z(max(0.0, value - previous), history), -4, 8), 3)


def _effective_diversity(values: list[str]) -> float:
    if len(values) < 2:
        return 0.0
    counts = Counter(values)
    total = sum(counts.values())
    hhi = sum((count / total) ** 2 for count in counts.values())
    maximum = 1 - 1 / max(2, len(counts))
    return clamp((1 - hhi) / maximum * 100) if maximum else 0.0


def _platform_family(platform: str) -> str:
    value = platform.strip().lower()
    if value in {"github", "gitlab", "npm", "pypi", "package", "crates", "maven"}:
        return "developer_ecosystem"
    if value in {"hugging face", "huggingface", "hf"}:
        return "model_hub"
    if value in {"hn", "hacker news", "x", "twitter", "bluesky", "reddit"}:
        return "social_discussion"
    if value in {"arxiv", "openalex", "semantic scholar"}:
        return "research_index"
    if value in {"youtube", "bilibili"}:
        return "video"
    if value in {"rss", "official", "blog", "media"}:
        return "publisher_official"
    return value or "unknown"


def aggregate_metrics(event_type: EventType, snapshots: list[SignalSnapshot], baseline: Baseline | None = None) -> MetricBundle:
    baseline = baseline or Baseline()
    if not snapshots:
        return MetricBundle(0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, False, False, False, 0, 0, False, False, False, ["没有可用观测"])

    discussion = [snapshot for snapshot in snapshots if snapshot.signal_family == "discussion"]
    behavior_rows = [snapshot for snapshot in snapshots if snapshot.signal_family == "behavior"]
    # Count resolved owners rather than raw platform accounts. A coordinated
    # matrix operated by one entity must not manufacture source diversity.
    unique_discussers = len({snapshot.source_group for snapshot in discussion})
    discussion_growth: list[float] = []
    anomaly_scores: list[float] = []
    for snapshot in discussion:
        for metric in ("comments", "replies", "mentions", "score"):
            if metric in snapshot.metrics:
                if metric not in snapshot.previous_metrics:
                    continue
                discussion_growth.append(_normalized_growth(metric, snapshot.metrics[metric], snapshot.previous_metrics[metric], baseline, snapshot.platform))
                anomaly_scores.append(_growth_anomaly_z(metric, snapshot.metrics[metric], snapshot.previous_metrics[metric], baseline, snapshot.platform))
    attention = clamp(unique_discussers * 8 + (sum(discussion_growth) / len(discussion_growth) if discussion_growth else 0) * .55)

    valid_metrics = BEHAVIOR_METRICS[event_type]
    feature_samples: dict[str, list[float]] = defaultdict(list)
    feature_sample_keys: dict[str, set[tuple[str, datetime]]] = defaultdict(set)
    feature_platform_samples: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for snapshot in behavior_rows:
        for metric in valid_metrics:
            if metric not in snapshot.metrics:
                continue
            # Cumulative provider counters need two snapshots. Treat the first
            # value as a baseline seed, never as growth from an invented zero.
            if metric not in snapshot.previous_metrics:
                continue
            role = METRIC_ROLES[event_type][metric]
            if role == "attention_only":
                continue
            rule = METRIC_RULES[event_type][metric]
            if float(rule["weight"]) <= 0 or not snapshot.verifiable:
                continue
            score = _normalized_growth(metric, snapshot.metrics[metric], snapshot.previous_metrics[metric], baseline, snapshot.platform)
            feature_samples[metric].append(score)
            feature_sample_keys[metric].add((snapshot.source_group, snapshot.captured_at))
            feature_platform_samples[metric][_platform_family(snapshot.platform)].append(score)
            anomaly_scores.append(_growth_anomaly_z(metric, snapshot.metrics[metric], snapshot.previous_metrics[metric], baseline, snapshot.platform))

    feature_scores = {
        metric: sum(scores) / len(scores)
        for metric, scores in feature_samples.items()
        if len(feature_sample_keys[metric]) >= int(METRIC_RULES[event_type][metric]["minimumSample"])
    }

    def fixed_profile_estimate(roles: set[str] | None = None) -> tuple[float, float]:
        applicable = {
            metric: rule for metric, rule in METRIC_RULES[event_type].items()
            if rule["role"] != "attention_only"
            and float(rule["weight"]) > 0
            and (roles is None or str(rule["role"]) in roles)
        }
        applicable_weight = sum(float(rule["weight"]) for rule in applicable.values())
        if not applicable_weight:
            return 0.0, 0.0
        family_contribution: Counter[str] = Counter()
        family_caps: dict[str, float] = {}
        observed_weight = 0.0
        for metric, rule in applicable.items():
            family = str(rule["platformFamily"])
            family_caps[family] = float(rule["familyCap"])
            if metric not in feature_scores:
                continue
            weight = float(rule["weight"])
            observed_weight += weight
            family_contribution[family] += weight * feature_scores[metric]
        numerator = sum(min(value, family_caps[family] * 100) for family, value in family_contribution.items())
        return clamp(numerator / applicable_weight), clamp(observed_weight / applicable_weight * 100)

    behavior, behavior_feature_coverage = fixed_profile_estimate()
    primary_adoption_score, _ = fixed_profile_estimate({"primary_adoption"})
    primary_response_score, _ = fixed_profile_estimate({"primary_response"})
    primary_adoption_observed = any(
        metric in feature_scores and rule["role"] == "primary_adoption" and feature_scores[metric] > 0
        for metric, rule in METRIC_RULES[event_type].items()
    )
    primary_response_observed = any(
        metric in feature_scores and rule["role"] == "primary_response" and feature_scores[metric] > 0
        for metric, rule in METRIC_RULES[event_type].items()
    )

    platform_growth: Counter[str] = Counter()
    for metric, by_platform in feature_platform_samples.items():
        if metric not in feature_scores:
            continue
        weight = float(METRIC_RULES[event_type][metric]["weight"])
        sample_count = sum(len(scores) for scores in by_platform.values())
        for platform_family, scores in by_platform.items():
            platform_growth[platform_family] += weight * sum(scores) / sample_count

    groups = [snapshot.source_group for snapshot in snapshots]
    platforms = [snapshot.platform for snapshot in snapshots]
    diversity = clamp(_effective_diversity(groups) * .6 + _effective_diversity(platforms) * .4)
    authority = clamp(sum(snapshot.authority for snapshot in snapshots) / len(snapshots))
    verifiability = 100 * sum(snapshot.verifiable for snapshot in snapshots) / len(snapshots)
    families = {snapshot.signal_family for snapshot in snapshots if snapshot.signal_family != "behavior"}
    behavior_observed = bool(feature_samples)
    if behavior_observed:
        families.add("behavior")
    required = {"discussion", "behavior"}
    coverage = clamp(len(families) / 4 * 70 + len(required & families) / 2 * 30)
    if behavior_observed:
        coverage = clamp(coverage - 32.5 * (1 - behavior_feature_coverage / 100))

    total_growth = sum(platform_growth.values())
    concentration = max(platform_growth.values(), default=0) / total_growth if total_growth else 0.0
    fingerprints = Counter(snapshot.content_fingerprint for snapshot in snapshots if snapshot.content_fingerprint)
    same_text_share = max(fingerprints.values(), default=0) / len(snapshots)
    owner_counts = Counter(snapshot.source_group for snapshot in snapshots)
    owner_share = max(owner_counts.values(), default=0) / len(snapshots)
    minute_buckets = Counter(snapshot.captured_at.replace(second=0, microsecond=0) for snapshot in snapshots)
    time_share = max(minute_buckets.values(), default=0) / len(snapshots)
    coordination = clamp(max(0, owner_share - .25) * 75 + max(0, same_text_share - .25) * 55 + max(0, time_share - .25) * 45)

    ownership_entities = len(set(snapshot.source_group for snapshot in snapshots))
    platform_families = len({_platform_family(snapshot.platform) for snapshot in snapshots})
    drivers = [f"{ownership_entities} 个所有权去重后的独立信源", f"{platform_families} 个平台家族", f"{len(families)} 个信号家族"]
    if concentration >= .8:
        drivers.append("行为增长集中在单一平台")
    if coordination >= 60:
        drivers.append("文本与发布时间存在协同模式")
    return MetricBundle(
        round(attention, 2), round(behavior, 2), round(diversity, 2), round(authority, 2),
        round(coordination, 2), round(coverage, 2), round(verifiability, 2),
        round(concentration, 4), len(families), platform_families, ownership_entities, behavior_observed,
        primary_adoption_observed, primary_response_observed,
        round(primary_adoption_score, 2), round(primary_response_score, 2),
        any(snapshot.signal_family == "official" for snapshot in snapshots),
        any(snapshot.signal_family == "research" for snapshot in snapshots),
        any(snapshot.signal_family == "discussion" for snapshot in snapshots),
        drivers,
        max(anomaly_scores, default=0.0),
    )


def to_score_input(event_type: EventType, metrics: MetricBundle, *, velocity: float, consecutive_joint_growth: int, consecutive_gap_growth: int, consecutive_decline: int = 0, inactive_hours: float = 0, official_source_led: bool = False, baseline_maturity: float = 35, temporal_stability: float = 50, previous_state: LifecycleState | None = None, behavior_applicable: bool = True) -> ScoreInput:
    effective_coverage = metrics.coverage
    if not behavior_applicable and not metrics.behavior_observed:
        # N/A removes behavior from the denominator; it is not a missing-data
        # penalty. In the four-family formula behavior accounts for 17.5 family
        # points plus 15 required-family points.
        effective_coverage = clamp(effective_coverage + 32.5)
    return ScoreInput(
        event_type=event_type, attention=metrics.attention, behavior=metrics.behavior,
        diversity=metrics.diversity, authority=metrics.authority, coordination_risk=metrics.coordination_risk,
        coverage=effective_coverage, verifiability=metrics.verifiability, velocity=velocity,
        anomaly_robust_z=metrics.max_anomaly_z, consecutive_decline=consecutive_decline,
        inactive_hours=max(0, inactive_hours),
        platform_concentration=metrics.platform_concentration, independent_signal_families=metrics.independent_signal_families,
        independent_platform_families=metrics.independent_platform_families,
        independent_ownership_entities=metrics.independent_ownership_entities,
        consecutive_joint_growth=consecutive_joint_growth, consecutive_gap_growth=consecutive_gap_growth,
        official_source_led=official_source_led, baseline_maturity=baseline_maturity,
        cluster_confidence=metrics.diversity, temporal_stability=temporal_stability,
        behavior_applicable=behavior_applicable,
        behavior_observed=metrics.behavior_observed,
        primary_adoption_observed=metrics.primary_adoption_observed,
        primary_response_observed=metrics.primary_response_observed,
        primary_adoption_score=metrics.primary_adoption_score,
        primary_response_score=metrics.primary_response_score,
        official_source_present=metrics.official_source_present,
        research_source_present=metrics.research_source_present,
        discussion_source_present=metrics.discussion_source_present,
        previous_state=previous_state,
    )
