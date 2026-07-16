from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
import json
import os
from dataclasses import asdict
from pathlib import Path

from redis.asyncio import Redis

from .connectors import ArxivConnector, BaseConnector, GitHubConnector, HackerNewsConnector, HuggingFaceConnector, OpenAlexConnector, RSSConnector, YouTubeConnector
from .outbox import RedisOutboxPublisher
from .processor import EventProcessor
from .evidence_store import LocalEvidenceStore, RawEvidenceStore, S3EvidenceStore
from .embeddings import BgeM3Provider
from .storage import InMemoryRepository, PostgresRepository
from .worker import CollectorWorker
from .identities import SourceIdentityResolver
from .retention import RawEvidenceRetentionWorker


def build_evidence_store() -> RawEvidenceStore:
    if os.getenv("R2_ENDPOINT_URL") and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY"):
        return S3EvidenceStore(os.environ["R2_ENDPOINT_URL"], os.environ["R2_ACCESS_KEY_ID"], os.environ["R2_SECRET_ACCESS_KEY"])
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
    return connectors


async def run(*, once: bool, interval_seconds: int) -> None:
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
    worker = CollectorWorker(
        repository, build_connectors(evidence_store), processor, identity_resolver,
        monthly_budget_limit=float(os.getenv("EXTERNAL_DATA_BUDGET_RMB", "2000")),
        base_external_spend=float(os.getenv("EXTERNAL_DATA_SPEND_RMB", "0")),
    )
    retention_worker = RawEvidenceRetentionWorker(repository, evidence_store)
    redis: Redis | None = Redis.from_url(os.environ["REDIS_URL"], decode_responses=True) if dsn and os.getenv("REDIS_URL") else None
    publisher = RedisOutboxPublisher(dsn, redis) if dsn and redis else None
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
        while True:
            runs = await worker.run_once()
            print(json.dumps([asdict(item) for item in runs], ensure_ascii=False, default=str))
            print(json.dumps({"rawEvidenceExpired": await retention_worker.run_once()}, ensure_ascii=False))
            if publisher:
                print(await publisher.publish_batch())
            if once:
                break
            await asyncio.sleep(interval_seconds)
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
