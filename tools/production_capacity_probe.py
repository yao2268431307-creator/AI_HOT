"""Collect honest capacity evidence from a deployed production-shaped stack.

Unlike ``synthetic_load.py``, this probe refuses to pass on fixture data. It
requires a PostgreSQL DSN, a production-ready health attestation, target data
cardinalities, a completed collector/scoring cycle, and measured HTTP latency.
It is read-only and emits a machine-readable evidence document.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

import psycopg


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *_args: object, **_kwargs: object) -> None:
        return None


NO_REDIRECT_OPENER = build_opener(NoRedirect)


def evidence_digest(payload: dict[str, object]) -> str:
    material = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    return "sha256:" + hashlib.sha256(material).hexdigest()


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("at least one sample is required")
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, (len(ordered) * int(fraction * 100) + 99) // 100 - 1))]


def get_json(base_url: str, path: str, token: str, timeout: float) -> dict[str, object]:
    request = Request(
        f"{base_url.rstrip('/')}{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with NO_REDIRECT_OPENER.open(request, timeout=timeout) as response:
        return json.loads(response.read())


def dataset_counts(dsn: str) -> dict[str, int]:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT
              (SELECT count(*) FROM sources),
              (SELECT count(*) FROM observations),
              (SELECT count(*) FROM events WHERE cardinality(superseded_by)=0)"""
        )
        row = cursor.fetchone()
    return {"sources": int(row[0]), "observations": int(row[1]), "activeEvents": int(row[2])}


def latest_collector_cycle(dsn: str) -> dict[str, object] | None:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT last_seen_at,details FROM runtime_component_heartbeats
            WHERE component_id='collector-worker'"""
        )
        row = cursor.fetchone()
    if row is None:
        return None
    details = row[1] if isinstance(row[1], dict) else json.loads(row[1])
    return {"lastSeenAt": row[0].isoformat(), **details}


def secret_value(direct: str | None, path: Path | None, env_name: str) -> str:
    configured = direct or os.getenv(env_name)
    if configured and path:
        raise ValueError(f"provide only one direct/{env_name} value or secret file")
    value = path.read_text(encoding="utf-8").strip() if path else configured
    if not value:
        raise ValueError(f"missing {env_name} or corresponding secret file")
    return value


def run(
    *, dsn: str, base_url: str, token: str, requests: int, timeout: float,
    minimum_sources: int, minimum_observations: int, minimum_events: int,
    maximum_cycle_seconds: float, maximum_p95_ms: float,
) -> dict[str, object]:
    if requests < 20:
        raise ValueError("at least 20 HTTP requests are required")
    target = urlsplit(base_url)
    if (
        target.scheme != "https" or not target.hostname or target.username
        or target.password or target.query or target.fragment
    ):
        raise ValueError("capacity target must be a credential-free HTTPS base URL")
    health = get_json(base_url, "/api/v1/operations/runtime-health", token, timeout)
    counts = dataset_counts(dsn)
    cycle = latest_collector_cycle(dsn)
    durations: list[float] = []
    # One warm-up is intentionally excluded.
    get_json(base_url, "/api/v1/radar?window=6h", token, timeout)
    for _ in range(requests):
        started = time.perf_counter()
        get_json(base_url, "/api/v1/radar?window=6h", token, timeout)
        durations.append((time.perf_counter() - started) * 1000)
    p95_ms = percentile(durations, .95)
    cycle_duration = cycle.get("lastCycleDurationSeconds") if cycle else None
    gates = {
        "productionReady": health.get("productionReady") is True,
        "sourceCapacity": counts["sources"] >= minimum_sources,
        "observationCapacity": counts["observations"] >= minimum_observations,
        "eventCapacity": counts["activeEvents"] >= minimum_events,
        "cycleWithinTarget": isinstance(cycle_duration, (int, float)) and float(cycle_duration) <= maximum_cycle_seconds,
        "radarP95WithinTarget": p95_ms <= maximum_p95_ms,
    }
    payload: dict[str, object] = {
        "evidenceType": "production-capacity-probe-v1",
        "measuredAt": datetime.now(timezone.utc).isoformat(),
        "target": base_url, "dataset": counts, "collectorCycle": cycle,
        "http": {
            "requests": requests, "p50Ms": round(statistics.median(durations), 3),
            "p95Ms": round(p95_ms, 3), "maxMs": round(max(durations), 3),
        },
        "thresholds": {
            "minimumSources": minimum_sources, "minimumObservations": minimum_observations,
            "minimumActiveEvents": minimum_events, "maximumCycleSeconds": maximum_cycle_seconds,
            "maximumRadarP95Ms": maximum_p95_ms,
        },
        "gates": gates, "qualifies": all(gates.values()),
    }
    payload["evidenceDigest"] = evidence_digest(payload)
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", help="prefer --dsn-file to avoid process-list exposure")
    parser.add_argument("--dsn-file", type=Path)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--token", help="prefer --token-file to avoid process-list exposure")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--requests", type=int, default=120)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--minimum-sources", type=int, default=10_000)
    parser.add_argument("--minimum-observations", type=int, default=5_000_000)
    parser.add_argument("--minimum-events", type=int, default=2_000)
    parser.add_argument("--maximum-cycle-seconds", type=float, default=300)
    parser.add_argument("--maximum-p95-ms", type=float, default=500)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        dsn = secret_value(args.dsn, args.dsn_file, "POSTGRES_READONLY_DSN")
        token = secret_value(args.token, args.token_file, "RADAR_OWNER_JWT")
        result = run(
            dsn=dsn, base_url=args.base_url, token=token,
            requests=args.requests, timeout=args.timeout,
            minimum_sources=args.minimum_sources, minimum_observations=args.minimum_observations,
            minimum_events=args.minimum_events, maximum_cycle_seconds=args.maximum_cycle_seconds,
            maximum_p95_ms=args.maximum_p95_ms,
        )
    except (HTTPError, URLError, OSError, psycopg.Error, ValueError) as exc:
        result = {
            "evidenceType": "production-capacity-probe-v1", "qualifies": False,
            "error": str(exc), "measuredAt": datetime.now(timezone.utc).isoformat(),
        }
        result["evidenceDigest"] = evidence_digest(result)
    rendered = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    raise SystemExit(0 if result.get("qualifies") is True else 1)
