from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from fastapi.testclient import TestClient  # noqa: E402
from radar.main import create_app  # noqa: E402
from radar.storage import InMemoryRepository  # noqa: E402


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


if __name__ == "__main__":
    durations: list[float] = []
    with TestClient(create_app(InMemoryRepository())) as client:
        for _ in range(120):
            started = time.perf_counter()
            response = client.get("/api/v1/radar?window=6h")
            response.raise_for_status()
            durations.append((time.perf_counter() - started) * 1000)
    print(json.dumps({
        "requests": len(durations), "p50Ms": round(statistics.median(durations), 3),
        "p95Ms": round(percentile(durations, .95), 3), "maxMs": round(max(durations), 3),
        "environment": "in-process FastAPI with in-memory fixture; not production network/cache",
    }, ensure_ascii=False))
