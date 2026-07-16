from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = Path(__file__).parents[3] / "tools" / "evaluate.py"
SPEC = importlib.util.spec_from_file_location("radar_evaluate_cli", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_evaluation_report_includes_intervals_groups_and_cluster_quality() -> None:
    rows = [
        {"eventId": "a", "eventType": "model_release", "predicted": "accelerating", "actual": "accelerating", "rankScore": 90, "predictedAt": "2026-07-15T08:00:00+00:00", "confirmedAt": "2026-07-15T09:00:00+00:00", "observationId": "1", "predictedCluster": "p1", "actualCluster": "a1"},
        {"eventId": "b", "eventType": "model_release", "predicted": "accelerating", "actual": "noise", "rankScore": 80, "predictedAt": "2026-07-15T08:00:00+00:00", "observationId": "2", "predictedCluster": "p1", "actualCluster": "a2"},
        {"eventId": "c", "eventType": "security_incident", "predicted": "noise", "actual": "noise", "rankScore": 10, "predictedAt": "2026-07-16T08:00:00+00:00", "observationId": "3", "predictedCluster": "p2", "actualCluster": "a2"},
    ]
    report = MODULE.build_report(rows, 2, {"accelerating", "established"})
    assert report["sampleSize"] == 3
    assert report["precisionAtK"]["point"] == .5
    assert len(report["macroF1"]["bootstrap95"]) == 2
    assert report["byEventType"]["security_incident"]["sampleSize"] == 1
    assert report["clustering"]["pairwisePrecision"] == 0
    assert report["clustering"]["bcubedPrecision"] < 1
