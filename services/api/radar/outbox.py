from __future__ import annotations

import asyncio
from contextlib import suppress
import hashlib
import json
import secrets
import sys
from dataclasses import dataclass
from datetime import datetime

from redis.asyncio import Redis


PARTICIPANT_LEASE_SECONDS = 300
STREAM_OUTBOX_KINDS = (
    "observation.created",
    "metric_snapshots.created",
    "score.created",
    "feedback.created",
    "cluster.edit.requested",
    "event.rescore.requested",
    "source.erased",
)


def outbox_recovery_keys(stream: str) -> tuple[str, str, str]:
    """Return cluster-slot-compatible state, replay-lock and participant keys."""
    digest = hashlib.sha256(stream.encode()).hexdigest()[:16]
    base = f"radar:{{outbox-recovery-{digest}}}"
    return f"{base}:state", f"{base}:lock", f"{base}:participants"


async def register_stream_participant(redis: Redis, stream: str, role: str) -> str:
    """Join normal stream processing unless an exclusive replay is active."""
    state_key, lock_key, participants_key = outbox_recovery_keys(stream)
    token = f"{role}:{secrets.token_hex(16)}"
    registered = await redis.eval(
        """local now=tonumber(redis.call('TIME')[1])
        redis.call('ZREMRANGEBYSCORE',KEYS[3],'-inf',now)
        if redis.call('EXISTS',KEYS[2])==1 or redis.call('HGET',KEYS[1],'status')=='running' then
          return 0
        end
        redis.call('ZADD',KEYS[3],now+tonumber(ARGV[2]),ARGV[1])
        redis.call('EXPIRE',KEYS[3],tonumber(ARGV[2]))
        return 1""",
        3, state_key, lock_key, participants_key, token, PARTICIPANT_LEASE_SECONDS,
    )
    if registered != 1:
        raise RuntimeError(f"outbox replay is active for stream {stream}")
    return token


async def renew_stream_participant(redis: Redis, stream: str, token: str) -> None:
    """Renew a normal-processing lease and fail closed if replay took ownership."""
    _, lock_key, participants_key = outbox_recovery_keys(stream)
    renewed = await redis.eval(
        """local now=tonumber(redis.call('TIME')[1])
        local score=redis.call('ZSCORE',KEYS[2],ARGV[1])
        if redis.call('EXISTS',KEYS[1])==1 or not score or tonumber(score)<=now then return 0 end
        redis.call('ZADD',KEYS[2],now+tonumber(ARGV[2]),ARGV[1])
        redis.call('EXPIRE',KEYS[2],tonumber(ARGV[2]))
        return 1""",
        2, lock_key, participants_key, token, PARTICIPANT_LEASE_SECONDS,
    )
    if renewed != 1:
        raise RuntimeError(f"stream participant lease was lost for {stream}")


async def release_stream_participant(redis: Redis, stream: str, token: str) -> None:
    """Leave normal stream processing after a bounded unit of work."""
    _, _, participants_key = outbox_recovery_keys(stream)
    await redis.zrem(participants_key, token)


async def acquire_stream_exclusive(
    redis: Redis,
    stream: str,
    role: str,
    *,
    allow_running_replay_state: bool = False,
    lease_seconds: int = PARTICIPANT_LEASE_SECONDS,
) -> str:
    """Fence normal participants for a bounded maintenance operation."""
    state_key, lock_key, participants_key = outbox_recovery_keys(stream)
    token = f"{role}:{secrets.token_hex(16)}"
    acquired = await redis.eval(
        """local now=tonumber(redis.call('TIME')[1])
        redis.call('ZREMRANGEBYSCORE',KEYS[3],'-inf',now)
        if redis.call('EXISTS',KEYS[2])==1 or redis.call('ZCARD',KEYS[3])>0 then return 0 end
        if ARGV[3]=='0' and redis.call('HGET',KEYS[1],'status')=='running' then return 0 end
        redis.call('SET',KEYS[2],ARGV[1],'EX',ARGV[2])
        return 1""",
        3, state_key, lock_key, participants_key,
        token, lease_seconds, "1" if allow_running_replay_state else "0",
    )
    if acquired != 1:
        raise RuntimeError(f"active replay or stream participant prevents exclusive maintenance for {stream}")
    return token


async def renew_stream_exclusive(
    redis: Redis,
    stream: str,
    token: str,
    *,
    lease_seconds: int = PARTICIPANT_LEASE_SECONDS,
) -> None:
    _, lock_key, _ = outbox_recovery_keys(stream)
    renewed = await redis.eval(
        """if redis.call('GET',KEYS[1])==ARGV[1]
        then return redis.call('EXPIRE',KEYS[1],ARGV[2]) else return 0 end""",
        1, lock_key, token, lease_seconds,
    )
    if renewed != 1:
        raise RuntimeError(f"exclusive stream maintenance lease was lost for {stream}")


async def release_stream_exclusive(redis: Redis, stream: str, token: str) -> None:
    _, lock_key, _ = outbox_recovery_keys(stream)
    released = await redis.eval(
        "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",
        1, lock_key, token,
    )
    if released != 1:
        raise RuntimeError(f"exclusive stream maintenance lease is no longer owned for {stream}")


async def maintain_stream_exclusive(
    redis: Redis,
    stream: str,
    token: str,
    lost: asyncio.Event,
    interval_seconds: int = 30,
) -> None:
    while not lost.is_set():
        try:
            await asyncio.wait_for(lost.wait(), timeout=interval_seconds)
            return
        except TimeoutError:
            pass
        try:
            await renew_stream_exclusive(redis, stream, token)
        except Exception:
            lost.set()
            return


async def maintain_stream_participant(
    redis: Redis,
    stream: str,
    token: str,
    lost: asyncio.Event,
    interval_seconds: int = 30,
) -> None:
    while not lost.is_set():
        try:
            await asyncio.wait_for(lost.wait(), timeout=interval_seconds)
            return
        except TimeoutError:
            pass
        try:
            await renew_stream_participant(redis, stream, token)
        except Exception:
            lost.set()
            return


@dataclass(frozen=True, slots=True)
class PublishResult:
    published: int
    failed: int


@dataclass(frozen=True, slots=True)
class ReplayResult:
    replayed: int
    next_created_at: datetime | None
    next_id: str | None
    next_stream_id: str | None
    complete: bool


class RedisOutboxPublisher:
    """Publish PostgreSQL outbox rows to Redis Streams with at-least-once delivery."""

    def __init__(self, dsn: str, redis: Redis, stream: str = "radar:events") -> None:
        self.dsn = dsn
        self.redis = redis
        self.stream = stream

    def _before_mark_published(self, outbox_id: str) -> None:
        """Failure-injection seam for the XADD/SQL crash window."""

    def _record_failure(self, outbox_id: str, error: Exception) -> None:
        import psycopg

        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(
                """UPDATE outbox SET attempts=attempts+1,last_error=%s
                WHERE id=%s AND published_at IS NULL""",
                (str(error)[:1000], outbox_id),
            )
            connection.commit()

    async def _register_active_publisher(self) -> str:
        return await register_stream_participant(self.redis, self.stream, "publisher")

    async def _renew_active_publisher(self, token: str) -> None:
        await renew_stream_participant(self.redis, self.stream, token)

    async def _release_active_publisher(self, token: str) -> None:
        await release_stream_participant(self.redis, self.stream, token)

    async def publish_batch(self, limit: int = 200) -> PublishResult:
        import psycopg

        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        publisher_token = await self._register_active_publisher()
        publisher_lost = asyncio.Event()
        lease_task = asyncio.create_task(maintain_stream_participant(
            self.redis, self.stream, publisher_token, publisher_lost,
        ))
        published = 0
        failed = 0
        attempted = 0
        after_created_at: datetime | None = None
        after_id: str | None = None
        try:
            with psycopg.connect(self.dsn) as connection:
                while attempted < limit:
                    if publisher_lost.is_set():
                        raise RuntimeError(f"outbox publisher lease was lost for stream {self.stream}")
                    await self._renew_active_publisher(publisher_token)
                    outbox_id: str | None = None
                    try:
                        with connection.transaction(), connection.cursor() as cursor:
                            query = """SELECT id,kind,aggregate_id,payload,created_at FROM outbox
                                WHERE published_at IS NULL"""
                            parameters: tuple[object, ...] = ()
                            if after_created_at is not None and after_id is not None:
                                query += " AND (created_at,id)>(%s,%s::uuid)"
                                parameters = (after_created_at, after_id)
                            query += " ORDER BY created_at,id FOR UPDATE SKIP LOCKED LIMIT 1"
                            cursor.execute(query, parameters)
                            row = cursor.fetchone()
                            if row is None:
                                break
                            raw_outbox_id, kind, aggregate_id, payload, created_at = row
                            outbox_id = str(raw_outbox_id)
                            after_created_at = created_at
                            after_id = outbox_id
                            attempted += 1
                            if kind not in STREAM_OUTBOX_KINDS:
                                raise RuntimeError(
                                    f"outbox kind is not registered for stream publication: {kind}",
                                )
                            await self.redis.xadd(self.stream, {
                                "outbox_id": outbox_id, "kind": kind, "aggregate_id": aggregate_id,
                                "payload": json.dumps(payload, ensure_ascii=False),
                            })
                            if publisher_lost.is_set():
                                raise RuntimeError(f"outbox publisher lease was lost for stream {self.stream}")
                            self._before_mark_published(outbox_id)
                            cursor.execute(
                                "UPDATE outbox SET published_at=now(),attempts=attempts+1,last_error=NULL WHERE id=%s",
                                (outbox_id,),
                            )
                        published += 1
                    except Exception as exc:
                        if outbox_id is None:
                            raise
                        # The per-row transaction has already rolled back. Record
                        # the retryable failure in a fresh transaction so PostgreSQL
                        # errors do not leave the publishing connection aborted.
                        self._record_failure(outbox_id, exc)
                        failed += 1
            return PublishResult(published, failed)
        finally:
            active_exception = sys.exc_info()[0] is not None
            publisher_lost.set()
            lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await lease_task
            try:
                await self._release_active_publisher(publisher_token)
            except Exception:
                if not active_exception:
                    raise

    async def replay_batch(
        self,
        *,
        since: datetime,
        until: datetime,
        after_created_at: datetime | None = None,
        after_id: str | None = None,
        limit: int = 200,
        kinds: tuple[str, ...] = STREAM_OUTBOX_KINDS,
    ) -> ReplayResult:
        """Replay consumer-relevant committed rows after a Redis data loss.

        This deliberately does not mutate PostgreSQL publication facts. A
        repeated recovery run can therefore emit duplicate stream entries;
        downstream handlers must remain idempotent by stable ``outbox_id`` or
        their domain idempotency key.
        """
        import psycopg

        if since >= until:
            raise ValueError("since must be earlier than until")
        if limit < 1 or limit > 10_000:
            raise ValueError("limit must be between 1 and 10000")
        if not kinds:
            raise ValueError("at least one replay kind is required")
        if (after_created_at is None) != (after_id is None):
            raise ValueError("after_created_at and after_id must be provided together")
        query = """SELECT id,kind,aggregate_id,payload,created_at FROM outbox
            WHERE published_at IS NOT NULL AND created_at >= %s AND created_at < %s
              AND kind=ANY(%s)"""
        parameters: list[object] = [since, until, list(kinds)]
        if after_created_at is not None and after_id is not None:
            query += " AND (created_at,id)>(%s,%s::uuid)"
            parameters.extend((after_created_at, after_id))
        query += " ORDER BY created_at,id LIMIT %s"
        parameters.append(limit)
        with psycopg.connect(self.dsn) as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            rows = cursor.fetchall()
        next_stream_id: str | None = None
        for outbox_id, kind, aggregate_id, payload, _ in rows:
            next_stream_id = await self.redis.xadd(self.stream, {
                "outbox_id": str(outbox_id), "kind": kind, "aggregate_id": aggregate_id,
                "payload": json.dumps(payload, ensure_ascii=False), "replay": "true",
            })
        if not rows:
            return ReplayResult(0, after_created_at, after_id, None, True)
        last = rows[-1]
        return ReplayResult(len(rows), last[4], str(last[0]), next_stream_id, len(rows) < limit)
