"""Safely rebuild the consumer-relevant Redis Stream from PostgreSQL Outbox.

The command is dry-run by default. Execute mode requires an exact stream-name
confirmation, refuses an unrelated non-empty stream, and stores a resumable
checkpoint in the same Redis instance. Normal publishing and consumers should
be unscheduled before execution; participant leases make overlap fail closed.
"""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import sqlite3
import sys
import tempfile
import time
from uuid import UUID

import psycopg
from redis.asyncio import Redis


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "api"))

from radar.outbox import (  # noqa: E402
    RedisOutboxPublisher,
    STREAM_FENCE_PROTOCOL_VERSION,
    STREAM_OUTBOX_KINDS,
    outbox_recovery_keys,
)


REPLAY_KINDS = STREAM_OUTBOX_KINDS
REPLAY_LOCK_SECONDS = 300
REPLAY_HEARTBEAT_SECONDS = 30
REPLAY_PREFIX_VERIFY_TIMEOUT_SECONDS = 240


def aware_datetime(value: str) -> datetime:
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("datetime must include a timezone")
    return parsed.astimezone(timezone.utc)


def bounded_batch_size(value: str) -> int:
    try:
        size = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer from 1 to 1000") from exc
    if not 1 <= size <= 1_000:
        raise argparse.ArgumentTypeError("must be from 1 to 1000")
    return size


def replay_candidate_summary(dsn: str, since: datetime, until: datetime) -> dict[str, object]:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """SELECT count(*),min(created_at),max(created_at),clock_timestamp(),
              count(*) FILTER (WHERE kind='score.created'),
              count(*) FILTER (WHERE kind='source.erased')
            FROM outbox WHERE published_at IS NOT NULL AND created_at >= %s AND created_at < %s
              AND kind=ANY(%s)""",
            (since, until, list(REPLAY_KINDS)),
        )
        count, earliest, latest, database_now, scores, deletions = cursor.fetchone()
    return {
        "candidateCount": count,
        "scoreEvents": scores,
        "sourceDeletions": deletions,
        "earliestCreatedAt": earliest.isoformat() if earliest else None,
        "latestCreatedAt": latest.isoformat() if latest else None,
        "databaseNow": database_now.isoformat(),
    }


def expected_state(since: datetime, until: datetime, stream: str) -> dict[str, str]:
    return {
        "since": since.isoformat(),
        "until": until.isoformat(),
        "stream": stream,
        "kinds": ",".join(REPLAY_KINDS),
        "protocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
    }


async def write_checkpoint(
    redis: Redis,
    lock_key: str,
    state_key: str,
    lock_token: str,
    mapping: dict[str, str],
) -> None:
    arguments: list[str] = [lock_token, str(REPLAY_LOCK_SECONDS)]
    for key, value in mapping.items():
        arguments.extend((key, value))
    written = await redis.eval(
        """if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
        for i=3,#ARGV,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end
        redis.call('EXPIRE',KEYS[1],tonumber(ARGV[2]))
        return 1""",
        2, lock_key, state_key, *arguments,
    )
    if written != 1:
        raise RuntimeError("replay ownership lock was lost before checkpoint commit")


async def reset_completed_checkpoint(redis: Redis, lock_key: str, state_key: str, lock_token: str) -> None:
    reset = await redis.eval(
        """if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
        redis.call('DEL',KEYS[2])
        redis.call('EXPIRE',KEYS[1],tonumber(ARGV[2]))
        return 1""",
        2, lock_key, state_key, lock_token, REPLAY_LOCK_SECONDS,
    )
    if reset != 1:
        raise RuntimeError("replay ownership lock was lost before completed-state reset")


async def maintain_replay_lock(redis: Redis, lock_key: str, lock_token: str, lost: asyncio.Event) -> None:
    while not lost.is_set():
        try:
            await asyncio.wait_for(lost.wait(), timeout=REPLAY_HEARTBEAT_SECONDS)
            return
        except TimeoutError:
            pass
        try:
            renewed = await redis.eval(
                """if redis.call('GET',KEYS[1])==ARGV[1]
                then return redis.call('EXPIRE',KEYS[1],ARGV[2]) else return 0 end""",
                1, lock_key, lock_token, REPLAY_LOCK_SECONDS,
            )
        except Exception:
            lost.set()
            return
        if renewed != 1:
            lost.set()
            return


def compare_replayed_prefix(
    dsn: str,
    actual_db_path: Path,
    since: datetime,
    until: datetime,
    after_created_at: datetime,
    after_id: str,
) -> tuple[int, int, bool]:
    """Stream an exact PostgreSQL/SQLite prefix comparison off the event loop."""
    actual_connection = sqlite3.connect(actual_db_path)
    try:
        with (
            psycopg.connect(dsn, connect_timeout=10) as connection,
            connection.cursor(name=f"replay_prefix_{secrets.token_hex(8)}") as expected_cursor,
        ):
            connection.execute(
                "SELECT set_config('statement_timeout',%s,true)",
                (f"{REPLAY_PREFIX_VERIFY_TIMEOUT_SECONDS}s",),
            )
            expected_cursor.execute(
                """SELECT id FROM outbox
                WHERE published_at IS NOT NULL AND created_at >= %s AND created_at < %s
                  AND kind=ANY(%s) AND (created_at,id) <= (%s,%s::uuid)
                ORDER BY created_at,id""",
                (since, until, list(REPLAY_KINDS), after_created_at, after_id),
            )
            actual_cursor = actual_connection.execute(
                "SELECT id FROM replay_prefix_actual ORDER BY ordinal",
            )
            actual_count = int(actual_connection.execute(
                "SELECT count(*) FROM replay_prefix_actual",
            ).fetchone()[0])
            expected_count = 0
            order_matches = True
            deadline = time.monotonic() + REPLAY_PREFIX_VERIFY_TIMEOUT_SECONDS
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"replay prefix verification exceeded {REPLAY_PREFIX_VERIFY_TIMEOUT_SECONDS} seconds",
                    )
                expected_batch = [str(row[0]) for row in expected_cursor.fetchmany(1_000)]
                actual_batch = [str(row[0]) for row in actual_cursor.fetchmany(1_000)]
                expected_count += len(expected_batch)
                if expected_batch != actual_batch:
                    order_matches = False
                if not expected_batch and not actual_batch:
                    break
            return expected_count, actual_count, order_matches
    finally:
        actual_connection.close()


async def run_prefix_comparison(*args: object) -> tuple[int, int, bool]:
    """Keep the worker alive on cancellation until it releases the temp DB."""
    task = asyncio.create_task(asyncio.to_thread(compare_replayed_prefix, *args))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        with suppress(BaseException):
            task.result()
        raise


async def verify_replayed_prefix(
    redis: Redis,
    dsn: str,
    stream: str,
    since: datetime,
    until: datetime,
    after_created_at: datetime,
    after_id: str,
    after_stream_id: str,
    replayed: int,
) -> None:
    """Prove that the whole checkpointed PostgreSQL prefix still exists in Redis.

    A crash may leave duplicate Stream entries after a checkpoint, so the
    comparison uses each ``outbox_id`` at its first Redis occurrence.  The
    temporary on-disk ledger keeps the check exact without retaining a
    potentially multi-million-item set in Python memory.  The PostgreSQL scan
    runs outside the event loop so the replay-lock heartbeat can continue.
    """
    handle = tempfile.NamedTemporaryFile(prefix="radar-replay-prefix-", suffix=".sqlite3", delete=False)
    actual_db_path = Path(handle.name)
    handle.close()
    try:
        actual_connection = sqlite3.connect(actual_db_path)
        try:
            actual_connection.execute(
                """CREATE TABLE replay_prefix_actual (
                  ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
                  id TEXT NOT NULL UNIQUE
                )""",
            )
            start_id = "-"
            while True:
                rows = await redis.xrange(
                    stream, min=start_id, max=after_stream_id, count=1_000,
                )
                if not rows:
                    break
                actual_ids: list[tuple[str]] = []
                for _, fields in rows:
                    raw_id = fields.get("outbox_id")
                    try:
                        normalized = str(UUID(str(raw_id)))
                    except (TypeError, ValueError, AttributeError) as exc:
                        raise RuntimeError(
                            "replay checkpoint prefix contains a missing or malformed outbox_id",
                        ) from exc
                    actual_ids.append((normalized,))
                actual_connection.executemany(
                    "INSERT OR IGNORE INTO replay_prefix_actual (id) VALUES (?)",
                    actual_ids,
                )
                actual_connection.commit()
                last_stream_id = str(rows[-1][0])
                if last_stream_id == after_stream_id:
                    break
                start_id = f"({last_stream_id}"
        finally:
            actual_connection.close()
        expected_count, actual_count, order_matches = await run_prefix_comparison(
            dsn, actual_db_path, since, until, after_created_at, after_id,
        )
    finally:
        active_exception = sys.exc_info()[0] is not None
        try:
            actual_db_path.unlink(missing_ok=True)
        except OSError:
            if not active_exception:
                raise
    if (
        replayed != expected_count
        or actual_count != expected_count
        or not order_matches
    ):
        raise RuntimeError(
            "replay checkpoint prefix integrity mismatch: "
            f"checkpoint={replayed}, expected={expected_count}, "
            f"actualUnique={actual_count}, orderMatches={order_matches}",
        )


async def execute_replay(
    dsn: str,
    redis_url: str,
    stream: str,
    since: datetime,
    until: datetime,
    batch_size: int,
) -> dict[str, object]:
    redis = Redis.from_url(redis_url, decode_responses=True)
    state_key, lock_key, participants_key = outbox_recovery_keys(stream)
    lock_token = secrets.token_hex(16)
    expected = expected_state(since, until, stream)
    owns_lock = False
    heartbeat: asyncio.Task[None] | None = None
    lock_lost = asyncio.Event()
    try:
        acquired = await redis.eval(
            """local now=tonumber(redis.call('TIME')[1])
            redis.call('ZREMRANGEBYSCORE',KEYS[2],'-inf',now)
            if redis.call('EXISTS',KEYS[1])==1 or redis.call('ZCARD',KEYS[2])>0 then return 0 end
            redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[2])
            return 1""",
            2, lock_key, participants_key, lock_token, REPLAY_LOCK_SECONDS,
        )
        if acquired != 1:
            raise RuntimeError(f"another replay or active stream processor owns {lock_key}")
        owns_lock = True
        heartbeat = asyncio.create_task(maintain_replay_lock(redis, lock_key, lock_token, lock_lost))

        stream_exists = bool(await redis.exists(stream))
        state = await redis.hgetall(state_key)
        if stream_exists and not state:
            raise RuntimeError("target stream already exists without a matching replay checkpoint")
        if state and any(state.get(key) != value for key, value in expected.items()):
            raise RuntimeError(
                "existing replay checkpoint does not match this stream/window/kind/protocol set",
            )
        if state.get("status") == "completed":
            if stream_exists:
                raise RuntimeError("this replay window is already marked completed")
            # A later loss of the rebuilt Stream must start from the beginning;
            # completed cursors are never reused against an absent target.
            await reset_completed_checkpoint(redis, lock_key, state_key, lock_token)
            state = {}
        elif state and state.get("status") != "running":
            raise RuntimeError("existing replay checkpoint has an unsupported status")

        after_created_at = aware_datetime(state["afterCreatedAt"]) if state.get("afterCreatedAt") else None
        after_id = state.get("afterId") or None
        after_stream_id = state.get("afterStreamId") or None
        replayed = int(state.get("replayed", "0"))
        if replayed < 0:
            raise RuntimeError("replay checkpoint count cannot be negative")
        progressed = replayed > 0 or any((after_created_at, after_id, after_stream_id))
        if progressed and not all((after_created_at, after_id, after_stream_id)):
            raise RuntimeError("progressed replay checkpoint is missing a PostgreSQL or Redis cursor")
        if progressed:
            if not stream_exists:
                raise RuntimeError("target stream disappeared after replay progress; reset state and restart the full window")
            checkpoint_rows = await redis.xrange(stream, min=after_stream_id, max=after_stream_id, count=1)
            if not checkpoint_rows or checkpoint_rows[0][1].get("outbox_id") != after_id:
                raise RuntimeError("target stream no longer contains the checkpointed outbox row; reset state and restart the full window")
            await verify_replayed_prefix(
                redis, dsn, stream, since, until,
                after_created_at, after_id, after_stream_id, replayed,
            )
            if lock_lost.is_set():
                raise RuntimeError("replay ownership heartbeat was lost during prefix verification")
        elif stream_exists and await redis.xlen(stream) > 0:
            raise RuntimeError("target stream contains entries before the first replay checkpoint")

        await write_checkpoint(redis, lock_key, state_key, lock_token, {
            **expected,
            "status": "running",
            "replayed": str(replayed),
            "startedAt": state.get("startedAt") or datetime.now(timezone.utc).isoformat(),
            "afterCreatedAt": after_created_at.isoformat() if after_created_at else "",
            "afterId": after_id or "",
            "afterStreamId": after_stream_id or "",
        })
        publisher = RedisOutboxPublisher(dsn, redis, stream)
        while True:
            if lock_lost.is_set():
                raise RuntimeError("replay ownership heartbeat was lost")
            result = await publisher.replay_batch(
                since=since,
                until=until,
                after_created_at=after_created_at,
                after_id=after_id,
                limit=batch_size,
                kinds=REPLAY_KINDS,
                exclusive_token=lock_token,
            )
            replayed += result.replayed
            after_created_at = result.next_created_at
            after_id = result.next_id
            after_stream_id = result.next_stream_id or after_stream_id
            checkpoint = {
                "replayed": str(replayed),
                "afterCreatedAt": after_created_at.isoformat() if after_created_at else "",
                "afterId": after_id or "",
                "afterStreamId": after_stream_id or "",
                "updatedAt": datetime.now(timezone.utc).isoformat(),
            }
            await write_checkpoint(redis, lock_key, state_key, lock_token, checkpoint)
            if result.complete:
                break
        await write_checkpoint(redis, lock_key, state_key, lock_token, {
            "status": "completed",
            "completedAt": datetime.now(timezone.utc).isoformat(),
        })
        return {
            "stream": stream,
            "replayed": replayed,
            "streamLength": await redis.xlen(stream),
            "stateKey": state_key,
            "status": "completed",
            "fenceProtocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
        }
    finally:
        active_exception = sys.exc_info()[0] is not None
        lock_lost.set()
        if heartbeat:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        unlock_error: Exception | None = None
        close_error: Exception | None = None
        if owns_lock:
            try:
                await redis.eval(
                    "if redis.call('get',KEYS[1])==ARGV[1] then return redis.call('del',KEYS[1]) else return 0 end",
                    1, lock_key, lock_token,
                )
            except Exception as exc:
                unlock_error = exc
        try:
            await redis.aclose()
        except Exception as exc:
            close_error = exc
        if not active_exception:
            if unlock_error:
                raise unlock_error
            if close_error:
                raise close_error


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--since", required=True, type=aware_datetime)
    value.add_argument("--until", required=True, type=aware_datetime)
    value.add_argument("--stream", default="radar:events")
    value.add_argument("--batch-size", type=bounded_batch_size, default=200)
    value.add_argument("--execute", action="store_true")
    value.add_argument(
        "--confirm-stream",
        help="execute mode requires this value to exactly match --stream",
    )
    return value


def main() -> int:
    args = parser().parse_args()
    if args.since >= args.until:
        raise SystemExit("--since must be earlier than --until")
    dsn = os.getenv("DATABASE_URL")
    redis_url = os.getenv("REDIS_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is required")
    summary = {
        "mode": "execute" if args.execute else "dry-run",
        "stream": args.stream,
        "since": args.since.isoformat(),
        "until": args.until.isoformat(),
        "kinds": list(REPLAY_KINDS),
        "fenceProtocolVersion": STREAM_FENCE_PROTOCOL_VERSION,
        **replay_candidate_summary(dsn, args.since, args.until),
    }
    if not args.execute:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.confirm_stream != args.stream:
        raise SystemExit("--confirm-stream must exactly match --stream in execute mode")
    if not redis_url:
        raise SystemExit("REDIS_URL is required in execute mode")
    database_now = datetime.fromisoformat(str(summary["databaseNow"]))
    if args.until > database_now:
        raise SystemExit("execute mode requires --until to be no later than the PostgreSQL clock")
    result = asyncio.run(execute_replay(
        dsn, redis_url, args.stream, args.since, args.until, args.batch_size,
    ))
    print(json.dumps({**summary, **result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
