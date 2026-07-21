from __future__ import annotations

import argparse
import atexit
import ctypes
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from urllib.request import Request, urlopen


ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


def keep_windows_awake() -> bool:
    """Hold a process-scoped Windows system wake lock without admin rights."""
    if sys.platform != "win32":
        return False
    result = ctypes.windll.kernel32.SetThreadExecutionState(  # type: ignore[attr-defined]
        ES_CONTINUOUS | ES_SYSTEM_REQUIRED,
    )
    if not result:
        raise OSError("Windows refused the system wake lock")
    atexit.register(
        lambda: ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS),  # type: ignore[attr-defined]
    )
    return True


def fetch(base_url: str, path: str) -> dict[str, object]:
    request = Request(f"{base_url.rstrip('/')}{path}", headers={"Accept": "application/json"})
    with urlopen(request, timeout=20) as response:  # noqa: S310 - fixed loopback URL by parser validation
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path} did not return an object")
    return payload


def collect(base_url: str) -> dict[str, object]:
    observed_at = datetime.now(timezone.utc).isoformat()
    errors: list[str] = []
    responses: dict[str, object] = {}
    for name, path in {
        "health": "/api/v1/operations/runtime-health",
        "radar": "/api/v1/radar?window=24h&limit=1",
        "coverage": "/api/v1/coverage",
        "pipeline": "/api/v1/operations/pipeline-sla?hours=72",
        "quality": "/api/v1/operations/data-quality?hours=72",
    }.items():
        try:
            responses[name] = fetch(base_url, path)
        except Exception as exc:  # keep monitoring after transient failures
            errors.append(f"{name}:{type(exc).__name__}")
    health = responses.get("health") if isinstance(responses.get("health"), dict) else {}
    coverage = responses.get("coverage") if isinstance(responses.get("coverage"), dict) else {}
    pipeline = responses.get("pipeline") if isinstance(responses.get("pipeline"), dict) else {}
    quality = responses.get("quality") if isinstance(responses.get("quality"), dict) else {}
    budget = coverage.get("budget") if isinstance(coverage.get("budget"), dict) else {}
    cost_safe = (
        health.get("runtimeProfile") == "local"
        and health.get("freeOnlyMode") is True
        and health.get("redisConfigured") is False
        and health.get("r2Configured") is False
        and float(budget.get("spent", -1)) == 0
        and float(budget.get("limit", -1)) == 0
    )
    finished_raw = health.get("lastCollectionFinishedAt")
    try:
        finished_at = datetime.fromisoformat(str(finished_raw).replace("Z", "+00:00"))
        collection_age_seconds = max(0.0, (datetime.now(timezone.utc) - finished_at).total_seconds())
    except (TypeError, ValueError):
        collection_age_seconds = float("inf")
    operationally_healthy = (
        not errors
        and health.get("status") == "ok"
        and health.get("localReady") is True
        and health.get("runtimeComponentsReady") is True
        and collection_age_seconds <= 1800
        and pipeline.get("unrecoveredFailures") == 0
        and pipeline.get("passesTarget") is True
        and quality.get("passesTarget") is True
    )
    return {
        "observedAt": observed_at,
        "healthy": operationally_healthy,
        "costSafe": cost_safe,
        "collectionAgeSeconds": None if collection_age_seconds == float("inf") else round(collection_age_seconds, 3),
        "errors": errors,
        "responses": responses,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor the zero-subscription local runtime")
    parser.add_argument("--api-url", default="http://127.0.0.1:8017")
    parser.add_argument("--duration-hours", type=float, default=72)
    parser.add_argument("--interval-seconds", type=int, default=900)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    if args.api_url not in {"http://127.0.0.1:8017", "http://localhost:8017"}:
        raise ValueError("local soak monitor only accepts the loopback API")
    if args.duration_hours <= 0 or args.interval_seconds <= 0:
        raise ValueError("duration and interval must be positive")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.summary.parent.mkdir(parents=True, exist_ok=True)
    wake_lock = keep_windows_awake()
    started = time.monotonic()
    duration = args.duration_hours * 3600
    samples: list[dict[str, object]] = []
    while True:
        sample = collect(args.api_url)
        samples.append(sample)
        with args.output.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
        elapsed = time.monotonic() - started
        if elapsed >= duration:
            break
        time.sleep(min(args.interval_seconds, max(0, duration - elapsed)))
    summary = {
        "startedAt": samples[0]["observedAt"],
        "finishedAt": samples[-1]["observedAt"],
        "durationHours": (time.monotonic() - started) / 3600,
        "samples": len(samples),
        "healthySamples": sum(item["healthy"] is True for item in samples),
        "costSafeSamples": sum(item["costSafe"] is True for item in samples),
        "windowsWakeLock": wake_lock,
        "passed": all(item["healthy"] is True and item["costSafe"] is True for item in samples),
    }
    args.summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
