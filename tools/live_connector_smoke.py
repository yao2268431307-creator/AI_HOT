"""Run a bounded live connectivity smoke against public metadata connectors.

This command is deliberately separate from deterministic CI.  It proves that
the current parser and network path can consume one live response; it does not
prove provider authorization, sustained availability, quota safety, or the
72-hour acceptance gate.
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time
from typing import Callable


API_ROOT = Path(__file__).resolve().parents[1] / "services" / "api"
sys.path.insert(0, str(API_ROOT))

from radar.connectors import (  # noqa: E402
    ArxivConnector,
    BaseConnector,
    BlueskyJetstreamConnector,
    GitHubConnector,
    HackerNewsConnector,
    HuggingFaceConnector,
    OpenAlexConnector,
)


ConnectorFactory = Callable[[argparse.Namespace], BaseConnector]


def bounded_hn_items(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer from 1 to 10") from exc
    if not 1 <= count <= 10:
        raise argparse.ArgumentTypeError("must be from 1 to 10")
    return count


def bounded_bluesky_messages(value: str) -> int:
    try:
        count = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer from 1 to 2000") from exc
    if not 1 <= count <= 2_000:
        raise argparse.ArgumentTypeError("must be from 1 to 2000")
    return count


def connector_factories() -> dict[str, ConnectorFactory]:
    return {
        "hackernews": lambda args: HackerNewsConnector(max_items=args.hn_max_items, max_attempts=2),
        "github": lambda args: GitHubConnector(query=args.github_query, max_attempts=2),
        "huggingface": lambda args: HuggingFaceConnector(search=args.hf_search, max_attempts=2),
        "arxiv": lambda args: ArxivConnector(query=args.arxiv_query, max_attempts=2),
        "openalex": lambda args: OpenAlexConnector(
            search=args.openalex_search,
            api_key=os.getenv("OPENALEX_API_KEY") or None,
            mailto=os.getenv("OPENALEX_MAILTO") or None,
            max_attempts=2,
        ),
        "bluesky": lambda args: BlueskyJetstreamConnector(
            endpoint=args.bluesky_endpoint,
            max_messages=args.bluesky_max_messages,
            idle_timeout_seconds=args.bluesky_idle_timeout_seconds,
            max_attempts=2,
        ),
    }


async def probe(connector: BaseConnector) -> dict[str, object]:
    started = time.perf_counter()
    try:
        rows = await connector.collect()
        if not rows:
            raise RuntimeError("connector returned zero observations")
        return {
            "connector": connector.id,
            "status": "pass",
            "observations": len(rows),
            "requests": connector.drain_request_count(),
            "latencyMs": round((time.perf_counter() - started) * 1000),
            "sampleExternalId": rows[0].external_id if rows else None,
            "samplePublishedAt": rows[0].published_at.isoformat() if rows else None,
            "provenanceCounts": {
                level: sum(row.provenance_level == level for row in rows)
                for level in ("self_authenticating", "provider_verified", "unverified_discovery")
            },
        }
    except Exception as exc:  # noqa: BLE001 - a smoke must report provider/parser failures uniformly
        return {
            "connector": connector.id,
            "status": "fail",
            "observations": 0,
            "requests": connector.drain_request_count(),
            "latencyMs": round((time.perf_counter() - started) * 1000),
            "error": f"{type(exc).__name__}: {exc}",
        }
    finally:
        await connector.close()


async def run(args: argparse.Namespace) -> int:
    factories = connector_factories()
    unknown = sorted(set(args.connectors) - set(factories))
    if unknown:
        raise SystemExit(f"unknown connectors: {', '.join(unknown)}")
    results = await asyncio.gather(*(probe(factories[name](args)) for name in args.connectors))
    payload = {
        "probeType": "live-public-metadata",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "single bounded read; not a 72-hour soak",
            "does not prove contractual rights or production credentials",
            "does not write the production repository or raw evidence store",
            "Bluesky Jetstream remains discovery-only; only exact AppView matches are provider-verified",
        ],
        "results": results,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 1 if any(row["status"] != "pass" for row in results) else 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--connectors",
        nargs="+",
        default=["hackernews", "github", "huggingface", "arxiv", "openalex"],
    )
    parser.add_argument("--hn-max-items", type=bounded_hn_items, default=3)
    parser.add_argument("--github-query", default="topic:artificial-intelligence")
    parser.add_argument("--hf-search", default="")
    parser.add_argument("--arxiv-query", default="cat:cs.AI")
    parser.add_argument("--openalex-search", default="artificial intelligence")
    parser.add_argument("--bluesky-max-messages", type=bounded_bluesky_messages, default=500)
    parser.add_argument("--bluesky-idle-timeout-seconds", type=float, default=3.0)
    parser.add_argument("--bluesky-endpoint", default="wss://jetstream2.us-east.bsky.network/subscribe")
    return parser.parse_args()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run(parse_args())))
