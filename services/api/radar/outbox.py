from __future__ import annotations

import json
import time
from dataclasses import dataclass

from redis.asyncio import Redis


@dataclass(frozen=True, slots=True)
class PublishResult:
    published: int
    failed: int


class RedisOutboxPublisher:
    """Publish PostgreSQL outbox rows to Redis Streams with at-least-once delivery."""

    def __init__(self, dsn: str, redis: Redis, stream: str = "radar:events") -> None:
        self.dsn = dsn
        self.redis = redis
        self.stream = stream

    async def publish_batch(self, limit: int = 200) -> PublishResult:
        import psycopg

        published = 0
        failed = 0
        with psycopg.connect(self.dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT id,kind,aggregate_id,payload FROM outbox WHERE published_at IS NULL ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT %s",
                    (limit,),
                )
                rows = cursor.fetchall()
                for outbox_id, kind, aggregate_id, payload in rows:
                    try:
                        await self.redis.xadd(self.stream, {
                            "outbox_id": str(outbox_id), "kind": kind, "aggregate_id": aggregate_id,
                            "payload": json.dumps(payload, ensure_ascii=False),
                        }, maxlen=100_000, approximate=True)
                        cursor.execute("UPDATE outbox SET published_at=now(), attempts=attempts+1 WHERE id=%s", (outbox_id,))
                        published += 1
                    except Exception as exc:  # Redis/network errors must leave the row retryable.
                        cursor.execute("UPDATE outbox SET attempts=attempts+1,last_error=%s WHERE id=%s", (str(exc)[:1000], outbox_id))
                        failed += 1
                connection.commit()
        if published:
            seven_days_ago_ms = int((time.time() - 7 * 24 * 3600) * 1000)
            await self.redis.xtrim(self.stream, minid=f"{seven_days_ago_ms}-0", approximate=True)
        return PublishResult(published, failed)
