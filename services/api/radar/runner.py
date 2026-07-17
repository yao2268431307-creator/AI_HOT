from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import json
import os
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from redis.asyncio import Redis

from .connectors import ArxivConnector, BaseConnector, BlueskyJetstreamConnector, GitHubConnector, HackerNewsConnector, HuggingFaceConnector, OpenAlexConnector, RSSConnector, YouTubeConnector
from .connectors.bluesky import DEFAULT_AI_KEYWORDS
from .outbox import RedisOutboxPublisher
from .processor import EventProcessor
from .evidence_store import LocalEvidenceStore, RawEvidenceStore, S3EvidenceStore
from .embeddings import BgeM3Provider
from .storage import InMemoryRepository, PostgresRepository
from .worker import CollectorWorker
from .identities import SourceIdentityResolver
from .retention import RawEvidenceRetentionWorker
from .auth import jwt_configuration_ready


def parse_budget_limits(variable_name: str) -> dict[str, float]:
    """Parse an optional JSON object of positive RMB limits, failing closed."""
    raw = os.getenv(variable_name, "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{variable_name} must be a JSON object") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{variable_name} must be a JSON object")
    limits: dict[str, float] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key).strip()
        if not key or isinstance(raw_value, bool):
            raise RuntimeError(f"{variable_name} contains an invalid budget entry")
        try:
            value = float(raw_value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{variable_name}.{key} must be a positive number") from exc
        if not (value > 0 and value < float("inf")):
            raise RuntimeError(f"{variable_name}.{key} must be a positive finite number")
        limits[key] = value
    return limits


def validate_budget_coverage(
    connectors: list[BaseConnector],
    connector_limits: dict[str, float],
    family_limits: dict[str, float],
) -> None:
    metered = [connector for connector in connectors if connector.metered]
    missing_connectors = sorted(
        connector.id for connector in metered if connector.id not in connector_limits
    )
    missing_families = sorted({
        connector.signal_family for connector in metered
        if connector.signal_family not in family_limits
    })
    if missing_connectors or missing_families:
        details: list[str] = []
        if missing_connectors:
            details.append(f"connectors={','.join(missing_connectors)}")
        if missing_families:
            details.append(f"signalFamilies={','.join(missing_families)}")
        raise RuntimeError(
            "every enabled metered connector requires connector and signal-family budgets: "
            + "; ".join(details)
        )


def validate_image_digests(variable_name: str) -> dict[str, str]:
    raw = os.getenv(variable_name, "").strip()
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{variable_name} must be a JSON object") from exc
    if not isinstance(payload, dict) or set(payload) != {"api", "web"}:
        raise RuntimeError(f"{variable_name} must contain exactly api and web")
    digests = {str(key): str(value) for key, value in payload.items()}
    if any(
        not value.startswith("sha256:") or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
        for value in digests.values()
    ):
        raise RuntimeError("release image digests must be lowercase sha256 values")
    return digests


def next_scheduled_cycle(
    previous_target: float, interval_seconds: int, monotonic_now: float,
) -> tuple[float, float]:
    """Advance on the fixed cadence, skipping missed slots without drift."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    target = previous_target + interval_seconds
    if target <= monotonic_now:
        target += (int((monotonic_now - target) // interval_seconds) + 1) * interval_seconds
    return target, max(0.0, target - monotonic_now)


async def probe_runtime_dependencies(
    redis: Redis, evidence_store: RawEvidenceStore, bucket: str,
) -> tuple[bool, bool, list[str]]:
    """Re-probe production dependencies without reusing a startup result."""
    failures: list[str] = []
    try:
        redis_verified = bool(await redis.ping())
    except Exception as exc:  # provider clients expose several transport errors
        redis_verified = False
        failures.append(f"redis:{type(exc).__name__}")
    try:
        await evidence_store.probe(bucket)
        r2_verified = True
    except Exception as exc:  # preserve only the error class in heartbeat/logs
        r2_verified = False
        failures.append(f"r2:{type(exc).__name__}")
    return redis_verified, r2_verified, failures


def validate_runtime_configuration() -> bool:
    """Fail closed when a production runner would silently use local substitutes."""
    production = os.getenv("DEMO_MODE", "true").lower() == "false"
    if not production:
        return False
    required = (
        "DATABASE_URL", "REDIS_URL", "R2_ENDPOINT_URL", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY",
        "RADAR_INSTANCE_ID", "SOURCE_IDENTITIES_FILE", "RADAR_JWT_PUBLIC_KEYS", "RADAR_JWT_ISSUER", "RADAR_JWT_AUDIENCE",
        "CONNECTOR_BUDGETS_RMB_JSON", "SIGNAL_FAMILY_BUDGETS_RMB_JSON", "RADAR_RELEASE_IMAGE_DIGESTS",
        "RADAR_ACTUAL_IMAGE_DIGESTS", "RAW_EVIDENCE_BUCKET",
    )
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise RuntimeError(f"production runner is missing required dependencies: {', '.join(missing)}")
    if os.getenv("AUTH_REQUIRED", "false").lower() != "true":
        raise RuntimeError("production runner requires AUTH_REQUIRED=true")
    if os.getenv("RADAR_AUTH_MODE", "api_keys") != "jwt":
        raise RuntimeError("production runner requires RADAR_AUTH_MODE=jwt")
    if len(os.environ["RADAR_INSTANCE_ID"]) < 8:
        raise RuntimeError("RADAR_INSTANCE_ID must be stable and at least eight characters")
    identities_path = Path(os.environ["SOURCE_IDENTITIES_FILE"])
    if not identities_path.is_file():
        raise RuntimeError("SOURCE_IDENTITIES_FILE must reference a readable reviewed registry")
    SourceIdentityResolver.from_json_file(identities_path)
    if not jwt_configuration_ready():
        raise RuntimeError("production runner requires a usable JWT issuer, audience and public keyring")
    parse_budget_limits("CONNECTOR_BUDGETS_RMB_JSON")
    parse_budget_limits("SIGNAL_FAMILY_BUDGETS_RMB_JSON")
    declared_images = validate_image_digests("RADAR_RELEASE_IMAGE_DIGESTS")
    actual_images = validate_image_digests("RADAR_ACTUAL_IMAGE_DIGESTS")
    if declared_images != actual_images:
        raise RuntimeError("running image digests do not match the signed release manifest")
    budget = float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000"))
    base_spend = float(os.getenv("EXTERNAL_DATA_SPEND_RMB", "0"))
    if not (0 < budget < float("inf")) or not (0 <= base_spend < float("inf")):
        raise RuntimeError("external data budget and spend must be finite and non-negative")
    return True


def build_evidence_store() -> RawEvidenceStore:
    if os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        return S3EvidenceStore(
            os.environ["R2_ENDPOINT_URL"],
            os.environ["R2_ACCESS_KEY_ID"],
            os.environ["R2_SECRET_ACCESS_KEY"],
            os.getenv("R2_SESSION_TOKEN") or None,
        )
    return LocalEvidenceStore(os.getenv("RAW_EVIDENCE_LOCAL_DIR", ".data/evidence"))


def build_connectors(evidence_store: RawEvidenceStore | None = None) -> list[BaseConnector]:
    evidence_store = evidence_store or build_evidence_store()
    connectors: list[BaseConnector] = [
        HackerNewsConnector(max_items=int(os.getenv("HN_MAX_ITEMS", "30")), evidence_store=evidence_store),
        GitHubConnector(query=os.getenv("GITHUB_QUERY", "topic:artificial-intelligence"), token=os.getenv("GITHUB_TOKEN") or None, evidence_store=evidence_store),
        HuggingFaceConnector(search=os.getenv("HF_SEARCH", ""), evidence_store=evidence_store),
        ArxivConnector(query=os.getenv("ARXIV_QUERY", "cat:cs.AI OR cat:cs.CL OR cat:cs.LG"), evidence_store=evidence_store),
        OpenAlexConnector(search=os.getenv("OPENALEX_SEARCH", "artificial intelligence"), mailto=os.getenv("OPENALEX_MAILTO") or None, evidence_store=evidence_store),
    ]
    feeds_path = Path(os.getenv("RSS_FEEDS_FILE", "config/feeds.local.json"))
    if feeds_path.exists():
        rows = json.loads(feeds_path.read_text(encoding="utf-8"))
        feeds = [(row["sourceId"], row["url"], row.get("signalFamily", "official")) for row in rows]
        connectors.insert(0, RSSConnector(feeds, evidence_store=evidence_store))
    if os.getenv("YOUTUBE_API_KEY"):
        connectors.append(YouTubeConnector(os.environ["YOUTUBE_API_KEY"], query=os.getenv("YOUTUBE_QUERY", "AI model"), evidence_store=evidence_store))
    if os.getenv("BLUESKY_JETSTREAM_ENABLED", "false").lower() == "true":
        keywords = tuple(
            value.strip() for value in os.getenv("BLUESKY_AI_KEYWORDS", "").split(",") if value.strip()
        )
        connectors.append(BlueskyJetstreamConnector(
            endpoint=os.getenv("BLUESKY_JETSTREAM_ENDPOINT", "wss://jetstream2.us-east.bsky.network/subscribe"),
            appview_endpoint=os.getenv("BLUESKY_APPVIEW_ENDPOINT", "https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts"),
            keywords=keywords or DEFAULT_AI_KEYWORDS,
            max_messages=int(os.getenv("BLUESKY_MAX_MESSAGES", "500")),
            idle_timeout_seconds=float(os.getenv("BLUESKY_IDLE_TIMEOUT_SECONDS", "3")),
            evidence_store=evidence_store,
        ))
    return connectors


async def run(*, once: bool, interval_seconds: int) -> None:
    production = validate_runtime_configuration()
    dsn = os.getenv("DATABASE_URL")
    repository = PostgresRepository(dsn) if dsn else InMemoryRepository()
    embedding_provider = None
    if os.getenv("BGE_M3_BASE_URL"):
        embedding_provider = BgeM3Provider(
            os.environ["BGE_M3_BASE_URL"], api_key=os.getenv("BGE_M3_API_KEY") or None,
            dimensions=int(os.getenv("BGE_M3_DIMENSIONS", "1024")),
        )
    identities_path = Path(os.getenv("SOURCE_IDENTITIES_FILE", "config/source_identities.local.json"))
    identity_resolver = SourceIdentityResolver.from_json_file(identities_path) if identities_path.exists() else SourceIdentityResolver()
    processor = EventProcessor(repository, embedding_provider)
    evidence_store = build_evidence_store()
    connectors = build_connectors(evidence_store)
    connector_budget_limits = parse_budget_limits("CONNECTOR_BUDGETS_RMB_JSON")
    signal_family_budget_limits = parse_budget_limits("SIGNAL_FAMILY_BUDGETS_RMB_JSON")
    if production:
        validate_budget_coverage(
            connectors, connector_budget_limits, signal_family_budget_limits,
        )
    worker = CollectorWorker(
        repository, connectors, processor, identity_resolver,
        monthly_budget_limit=float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000")),
        connector_budget_limits=connector_budget_limits,
        signal_family_budget_limits=signal_family_budget_limits,
        base_external_spend=float(os.getenv("EXTERNAL_DATA_SPEND_RMB", "0")),
    )
    retention_worker = RawEvidenceRetentionWorker(repository, evidence_store)
    redis: Redis | None = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True) if dsn and os.getenv("REDIS_URL") else None
    publisher = RedisOutboxPublisher(dsn, redis) if dsn and redis else None
    if production and (not isinstance(repository, PostgresRepository) or redis is None or publisher is None or isinstance(evidence_store, LocalEvidenceStore)):
        raise RuntimeError("production runner cannot use in-memory, local evidence, or publisher-less fallbacks")
    redis_verified = False
    r2_verified = False
    instance_id = os.getenv("RADAR_INSTANCE_ID", "local-runner")
    invalidation_task: asyncio.Task[None] | None = None
    invalidation_pubsub = None
    if redis:
        invalidation_pubsub = redis.pubsub()
        await invalidation_pubsub.subscribe("radar:cache:invalidate")

        async def listen_for_event_invalidations() -> None:
            assert invalidation_pubsub is not None
            async for message in invalidation_pubsub.listen():
                if message.get("type") != "message":
                    continue
                try:
                    tags = json.loads(str(message.get("data", "[]")))
                except json.JSONDecodeError:
                    continue
                processor.invalidate_events([str(tag).split(":", 1)[1] for tag in tags if str(tag).startswith("event:")])

        invalidation_task = asyncio.create_task(listen_for_event_invalidations())
    try:
        next_cycle_at = time.monotonic()
        while True:
            repository.heartbeat_runtime_component("scheduler", instance_id, {"intervalSeconds": interval_seconds})
            cycle_started = datetime.now(timezone.utc)
            dependency_probe_at = datetime.now(timezone.utc)
            dependency_failures: list[str] = []
            if production:
                assert redis is not None
                redis_verified, r2_verified, dependency_failures = await probe_runtime_dependencies(
                    redis, evidence_store, os.environ["RAW_EVIDENCE_BUCKET"],
                )
                if not redis_verified or not r2_verified:
                    failure_details = {
                        "intervalSeconds": interval_seconds,
                        "lastCycleStartedAt": cycle_started.isoformat(),
                        "dependencyProbeAt": dependency_probe_at.isoformat(),
                        "redisVerified": redis_verified,
                        "r2ReadWriteVerified": r2_verified,
                        "dependencyProbeFailures": dependency_failures,
                    }
                    repository.heartbeat_runtime_component(
                        "collector-worker", instance_id, failure_details,
                    )
                    print(json.dumps({
                        "level": "error", "event": "runtime_dependency_probe_failed",
                        **failure_details,
                    }, ensure_ascii=False))
                    raise RuntimeError("production dependency probe failed before collection")
            cycle_timer = time.perf_counter()
            runs = await worker.run_once()
            cycle_duration = time.perf_counter() - cycle_timer
            cycle_details = {
                "connectorCount": len(runs),
                "intervalSeconds": interval_seconds,
                "inserted": sum(item.inserted for item in runs),
                "duplicates": sum(item.duplicates for item in runs),
                "failed": sum(item.failed for item in runs),
                "skipped": sum(item.skipped for item in runs),
                "lastCycleStartedAt": cycle_started.isoformat(),
                "lastCycleFinishedAt": datetime.now(timezone.utc).isoformat(),
                "lastCycleDurationSeconds": round(cycle_duration, 3),
                "redisVerified": redis_verified, "r2ReadWriteVerified": r2_verified,
                "dependencyProbeAt": dependency_probe_at.isoformat(),
                "dependencyProbeFailures": dependency_failures,
            }
            repository.heartbeat_runtime_component("collector-worker", instance_id, cycle_details)
            print(json.dumps({
                "level": "info", "event": "collector_cycle_completed",
                **cycle_details, "runs": [asdict(item) for item in runs],
            }, ensure_ascii=False, default=str))
            expired = await retention_worker.run_once()
            metric_retention = repository.rollup_and_retain_event_metrics(
                datetime.now(timezone.utc),
                int(os.getenv("EVENT_METRIC_RAW_RETENTION_DAYS", "90")),
                int(os.getenv("EVENT_METRIC_ROLLUP_RETENTION_DAYS", "730")),
            )
            repository.heartbeat_runtime_component(
                "retention-worker", instance_id,
                {"intervalSeconds": interval_seconds, "lastExpired": expired, **metric_retention},
            )
            print(json.dumps({
                "level": "info", "event": "retention_cycle_completed",
                "rawEvidenceExpired": expired, "eventMetrics": metric_retention,
            }, ensure_ascii=False))
            if publisher:
                published = await publisher.publish_batch()
                repository.heartbeat_runtime_component(
                    "outbox-publisher", instance_id,
                    {"intervalSeconds": interval_seconds, "lastPublished": published, "redisVerified": redis_verified},
                )
                print(json.dumps({
                    "level": "info", "event": "outbox_publish_completed", "published": published,
                }, ensure_ascii=False))
            if once:
                break
            monotonic_now = time.monotonic()
            next_cycle_at, sleep_seconds = next_scheduled_cycle(
                next_cycle_at, interval_seconds, monotonic_now,
            )
            await asyncio.sleep(sleep_seconds)
    finally:
        await worker.close()
        if invalidation_task:
            invalidation_task.cancel()
            with suppress(asyncio.CancelledError):
                await invalidation_task
        if invalidation_pubsub:
            await invalidation_pubsub.aclose()
        if redis:
            await redis.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=900)
    args = parser.parse_args()
    asyncio.run(run(once=args.once, interval_seconds=args.interval_seconds))
