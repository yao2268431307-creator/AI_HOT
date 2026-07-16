"""CPU-only capacity probe for the V1 scoring core.

This deliberately does not claim PostgreSQL/Redis latency. It verifies that the
10k-source / 5m-observation / 2k-active-event cardinalities can be streamed and
scored without retaining all observations in memory.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from radar.contracts import EventType  # noqa: E402
from radar.scoring import ScoreInput, score_event  # noqa: E402


def run(source_count: int, observation_count: int, event_count: int) -> dict[str, object]:
    started = time.perf_counter()
    attention = [0] * event_count
    behavior = [0] * event_count
    family_masks = [0] * event_count
    seed = 0xC0FFEE
    for index in range(observation_count):
        # Small deterministic LCG keeps the benchmark reproducible and avoids allocating rows.
        seed = (1664525 * seed + 1013904223) & 0xFFFFFFFF
        event_id = seed % event_count
        source_id = (seed >> 8) % source_count
        family = (source_id + index) & 3
        family_masks[event_id] |= 1 << family
        if family == 0:
            attention[event_id] += 1
        elif family == 1:
            behavior[event_id] += 1

    ingest_seconds = time.perf_counter() - started
    score_started = time.perf_counter()
    states: dict[str, int] = {}
    for event_id in range(event_count):
        families = family_masks[event_id].bit_count()
        result = score_event(ScoreInput(
            event_type=list(EventType)[event_id % len(EventType)],
            attention=min(100, attention[event_id] / 5), behavior=min(100, behavior[event_id] / 5),
            diversity=65, authority=72, coordination_risk=18, coverage=min(100, families * 25),
            verifiability=85, velocity=10, platform_concentration=.35,
            independent_signal_families=families, consecutive_joint_growth=3, consecutive_gap_growth=0,
        ))
        states[result.state] = states.get(result.state, 0) + 1
    score_seconds = time.perf_counter() - score_started
    total_seconds = time.perf_counter() - started
    return {
        "sources": source_count, "observations": observation_count, "events": event_count,
        "ingestSeconds": round(ingest_seconds, 3), "scoreSeconds": round(score_seconds, 3),
        "totalSeconds": round(total_seconds, 3), "observationsPerSecond": round(observation_count / max(total_seconds, 1e-9)),
        "states": states, "processId": os.getpid(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", type=int, default=10_000)
    parser.add_argument("--observations", type=int, default=5_000_000)
    parser.add_argument("--events", type=int, default=2_000)
    args = parser.parse_args()
    print(json.dumps(run(args.sources, args.observations, args.events), ensure_ascii=False))

