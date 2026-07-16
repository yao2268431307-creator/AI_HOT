from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from radar.evaluation import (  # noqa: E402
    LabeledPrediction,
    bcubed_cluster_precision_recall,
    bootstrap_confidence_interval,
    false_alerts_per_day,
    macro_f1,
    median_lead_minutes,
    pairwise_cluster_precision,
    pairwise_cluster_recall,
    precision_at_k,
)


def parse_time(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def load_rows(path: Path) -> list[dict[str, object]]:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        raise ValueError("evaluation input is empty")
    return rows


def prediction(row: dict[str, object]) -> LabeledPrediction:
    return LabeledPrediction(
        event_id=str(row["eventId"]), predicted=str(row["predicted"]), actual=str(row["actual"]),
        rank_score=float(row.get("rankScore", 0)), predicted_at=parse_time(str(row["predictedAt"])) or datetime.min,
        confirmed_at=parse_time(str(row["confirmedAt"])) if row.get("confirmedAt") else None,
    )


def build_report(rows: list[dict[str, object]], k: int, positive_labels: set[str]) -> dict[str, object]:
    predictions = [prediction(row) for row in rows]
    grouped: dict[str, list[LabeledPrediction]] = defaultdict(list)
    for row, item in zip(rows, predictions, strict=True):
        grouped[str(row.get("eventType", "unknown"))].append(item)
    f1_interval = bootstrap_confidence_interval(predictions, macro_f1, iterations=2000, seed=17)
    report: dict[str, object] = {
        "sampleSize": len(predictions),
        "precisionAtK": {"k": k, "point": precision_at_k(predictions, positive_labels, k)},
        "macroF1": {"point": macro_f1(predictions), "bootstrap95": list(f1_interval)},
        "falseAlertsPerDay": false_alerts_per_day(predictions, positive_labels),
        "medianLeadMinutes": median_lead_minutes(predictions),
        "byEventType": {key: {"sampleSize": len(values), "macroF1": macro_f1(values)} for key, values in sorted(grouped.items())},
    }
    cluster_rows = [row for row in rows if row.get("observationId") and row.get("predictedCluster") and row.get("actualCluster")]
    if cluster_rows:
        predicted_clusters = {str(row["observationId"]): str(row["predictedCluster"]) for row in cluster_rows}
        actual_clusters = {str(row["observationId"]): str(row["actualCluster"]) for row in cluster_rows}
        bcubed_precision, bcubed_recall = bcubed_cluster_precision_recall(predicted_clusters, actual_clusters)
        report["clustering"] = {
            "sampleSize": len(cluster_rows),
            "pairwisePrecision": pairwise_cluster_precision(predicted_clusters, actual_clusters),
            "pairwiseRecall": pairwise_cluster_recall(predicted_clusters, actual_clusters),
            "bcubedPrecision": bcubed_precision,
            "bcubedRecall": bcubed_recall,
        }
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate the frozen AI radar evaluation report from JSONL labels.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--k", type=int, default=5)
    parser.add_argument("--positive-label", action="append", default=["accelerating", "established"])
    args = parser.parse_args()
    report = build_report(load_rows(args.input), args.k, set(args.positive_label))
    body = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(body + "\n", encoding="utf-8")
    else:
        print(body)


if __name__ == "__main__":
    main()
