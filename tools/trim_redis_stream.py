"""Safely trim recoverable Redis Stream entries behind every consumer group."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import sys

from redis.asyncio import Redis


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from radar.stream_retention import maintain_stream_retention  # noqa: E402


def nonnegative_hours(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a whole number of hours") from exc
    if not 0 <= parsed <= 8_760:
        raise argparse.ArgumentTypeError("must be from 0 to 8760")
    return parsed


def positive_capacity(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--stream", default="radar:events")
    value.add_argument(
        "--retention-hours", type=nonnegative_hours,
        default=nonnegative_hours(os.getenv("REDIS_STREAM_RETENTION_HOURS", "168")),
    )
    value.add_argument(
        "--capacity-limit", type=positive_capacity,
        default=positive_capacity(os.getenv("REDIS_STREAM_CAPACITY_LIMIT", "1000000")),
    )
    value.add_argument("--required-group", action="append")
    value.add_argument("--execute", action="store_true")
    value.add_argument("--confirm-stream")
    value.add_argument(
        "--confirm-before-id",
        help="safeTrimMinId copied exactly from the immediately preceding dry-run",
    )
    return value


async def run(args: argparse.Namespace) -> dict[str, object]:
    dsn = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    if not redis_url:
        raise SystemExit("REDIS_URL is required")
    if args.execute and args.confirm_stream != args.stream:
        raise SystemExit("--confirm-stream must exactly match --stream in execute mode")
    if args.execute and not args.confirm_before_id:
        raise SystemExit("--confirm-before-id is required in execute mode")
    configured_groups = args.required_group or [
        item.strip()
        for item in os.getenv("RADAR_REQUIRED_STREAM_GROUPS", "radar-alerts").split(",")
        if item.strip()
    ]
    if not configured_groups:
        raise SystemExit("at least one --required-group is required")
    redis = Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=10,
        socket_timeout=30,
        health_check_interval=30,
    )
    try:
        return await maintain_stream_retention(
            dsn, redis, args.stream,
            required_groups=set(configured_groups),
            retention_hours=args.retention_hours,
            capacity_limit=args.capacity_limit,
            execute=args.execute,
            confirmed_trim_min_id=args.confirm_before_id,
        )
    finally:
        await redis.aclose()


def main() -> int:
    args = parser().parse_args()
    print(json.dumps(asyncio.run(run(args)), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
