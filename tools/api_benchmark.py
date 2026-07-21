from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from fastapi.testclient import TestClient  # noqa: E402
from radar.main import create_app  # noqa: E402
from radar.storage import InMemoryRepository  # noqa: E402


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def live_request(base_url: str) -> None:
    target = f"{base_url.rstrip('/')}/api/v1/radar?window=6h"
    request = Request(target, headers={"Accept": "application/json"})
    with urlopen(request, timeout=20) as response:  # noqa: S310 - loopback validated below
        if response.status != 200:
            raise RuntimeError(f"radar returned HTTP {response.status}")
        response.read()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="")
    parser.add_argument("--requests", type=int, default=120)
    args = parser.parse_args()
    if args.requests < 1:
        raise ValueError("requests must be positive")
    durations: list[float] = []
    if args.base_url:
        parts = urlsplit(args.base_url)
        if parts.scheme != "http" or parts.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("live benchmark only accepts a loopback HTTP API")
        for _ in range(args.requests):
            started = time.perf_counter()
            live_request(args.base_url)
            durations.append((time.perf_counter() - started) * 1000)
        environment = "loopback FastAPI with local PostgreSQL; sequential client requests"
    else:
        with TestClient(create_app(InMemoryRepository())) as client:
            for _ in range(args.requests):
                started = time.perf_counter()
                response = client.get("/api/v1/radar?window=6h")
                response.raise_for_status()
                durations.append((time.perf_counter() - started) * 1000)
        environment = "in-process FastAPI with in-memory fixture; not local PostgreSQL"
    print(json.dumps({
        "requests": len(durations), "p50Ms": round(statistics.median(durations), 3),
        "p95Ms": round(percentile(durations, .95), 3), "maxMs": round(max(durations), 3),
        "environment": environment,
    }, ensure_ascii=False))
