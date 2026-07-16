from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import math
import random
from collections.abc import Callable, Sequence
from typing import TypeVar


T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class LabeledPrediction:
    event_id: str
    predicted: str
    actual: str
    rank_score: float
    predicted_at: datetime
    confirmed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EvaluationExample:
    event_id: str
    entity_id: str
    observed_at: datetime
    point_in_time_label: str
    outcome_label: str | None = None
    outcome_at: datetime | None = None

    def __post_init__(self) -> None:
        if self.outcome_at is not None and self.outcome_at < self.observed_at:
            raise ValueError("future outcome cannot precede the point-in-time observation")


def temporal_entity_holdout(items: list[EvaluationExample], cutoff: datetime) -> tuple[list[EvaluationExample], list[EvaluationExample]]:
    """Time split with complete entity holdout to prevent event-family leakage."""
    test = [item for item in items if item.observed_at >= cutoff]
    test_entities = {item.entity_id for item in test}
    train = [item for item in items if item.observed_at < cutoff and item.entity_id not in test_entities]
    return train, test


def precision_at_k(items: list[LabeledPrediction], positive_labels: set[str], k: int) -> float:
    if k <= 0:
        raise ValueError("k must be positive")
    selected = sorted(items, key=lambda item: item.rank_score, reverse=True)[:k]
    if not selected:
        return 0.0
    return sum(item.actual in positive_labels for item in selected) / len(selected)


def macro_f1(items: list[LabeledPrediction]) -> float:
    labels = sorted({item.predicted for item in items} | {item.actual for item in items})
    if not labels:
        return 0.0
    scores: list[float] = []
    for label in labels:
        tp = sum(item.predicted == label and item.actual == label for item in items)
        fp = sum(item.predicted == label and item.actual != label for item in items)
        fn = sum(item.predicted != label and item.actual == label for item in items)
        precision = tp / (tp + fp) if tp + fp else 0
        recall = tp / (tp + fn) if tp + fn else 0
        scores.append(2 * precision * recall / (precision + recall) if precision + recall else 0)
    return sum(scores) / len(scores)


def false_alerts_per_day(items: list[LabeledPrediction], alert_states: set[str]) -> float:
    if not items:
        return 0.0
    days = max(1, (max(item.predicted_at for item in items).date() - min(item.predicted_at for item in items).date()).days + 1)
    false_alerts = sum(item.predicted in alert_states and item.actual not in alert_states for item in items)
    return false_alerts / days


def median_lead_minutes(items: list[LabeledPrediction]) -> float | None:
    values = sorted((item.confirmed_at - item.predicted_at).total_seconds() / 60 for item in items if item.confirmed_at is not None)
    if not values:
        return None
    middle = len(values) // 2
    return values[middle] if len(values) % 2 else (values[middle - 1] + values[middle]) / 2


def pairwise_cluster_precision(predicted_cluster: dict[str, str], actual_cluster: dict[str, str]) -> float:
    ids = sorted(set(predicted_cluster) & set(actual_cluster))
    predicted_pairs = [(left, right) for index, left in enumerate(ids) for right in ids[index + 1:] if predicted_cluster[left] == predicted_cluster[right]]
    if not predicted_pairs:
        return 1.0
    correct = sum(actual_cluster[left] == actual_cluster[right] for left, right in predicted_pairs)
    return correct / len(predicted_pairs)


def pairwise_cluster_recall(predicted_cluster: dict[str, str], actual_cluster: dict[str, str]) -> float:
    ids = sorted(set(predicted_cluster) & set(actual_cluster))
    actual_pairs = [(left, right) for index, left in enumerate(ids) for right in ids[index + 1:] if actual_cluster[left] == actual_cluster[right]]
    if not actual_pairs:
        return 1.0
    recovered = sum(predicted_cluster[left] == predicted_cluster[right] for left, right in actual_pairs)
    return recovered / len(actual_pairs)


def bcubed_cluster_precision_recall(predicted_cluster: dict[str, str], actual_cluster: dict[str, str]) -> tuple[float, float]:
    """Return item-weighted B-cubed precision/recall for the common observation set."""
    ids = sorted(set(predicted_cluster) & set(actual_cluster))
    if not ids:
        return 1.0, 1.0
    predicted_members = {item_id: {other for other in ids if predicted_cluster[other] == predicted_cluster[item_id]} for item_id in ids}
    actual_members = {item_id: {other for other in ids if actual_cluster[other] == actual_cluster[item_id]} for item_id in ids}
    precisions = [len(predicted_members[item_id] & actual_members[item_id]) / len(predicted_members[item_id]) for item_id in ids]
    recalls = [len(predicted_members[item_id] & actual_members[item_id]) / len(actual_members[item_id]) for item_id in ids]
    return sum(precisions) / len(ids), sum(recalls) / len(ids)


def cohen_kappa(left_labels: Sequence[str], right_labels: Sequence[str]) -> float:
    """Agreement beyond chance for a double-labeled evaluation set."""
    if len(left_labels) != len(right_labels):
        raise ValueError("label sequences must have the same length")
    if not left_labels:
        return 0.0
    labels = set(left_labels) | set(right_labels)
    observed = sum(left == right for left, right in zip(left_labels, right_labels, strict=True)) / len(left_labels)
    expected = sum((left_labels.count(label) / len(left_labels)) * (right_labels.count(label) / len(right_labels)) for label in labels)
    if math.isclose(expected, 1.0):
        return 1.0 if math.isclose(observed, 1.0) else 0.0
    return (observed - expected) / (1 - expected)


def bootstrap_confidence_interval(
    items: Sequence[T],
    statistic: Callable[[list[T]], float],
    *,
    iterations: int = 1000,
    confidence: float = .95,
    seed: int = 0,
) -> tuple[float, float]:
    """Deterministic percentile bootstrap interval for shareable evaluation reports."""
    if not items:
        raise ValueError("bootstrap requires at least one item")
    if iterations < 100:
        raise ValueError("bootstrap requires at least 100 iterations")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be between 0 and 1")
    generator = random.Random(seed)
    estimates = sorted(statistic([items[generator.randrange(len(items))] for _ in items]) for _ in range(iterations))
    tail = (1 - confidence) / 2
    lower_index = max(0, math.floor(tail * (iterations - 1)))
    upper_index = min(iterations - 1, math.ceil((1 - tail) * (iterations - 1)))
    return estimates[lower_index], estimates[upper_index]
