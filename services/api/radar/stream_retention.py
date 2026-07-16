from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import sys
import tempfile
from uuid import UUID

import psycopg
from redis.asyncio import Redis

from .outbox import (
    STREAM_OUTBOX_KINDS,
    acquire_stream_exclusive,
    maintain_stream_exclusive,
    release_stream_exclusive,
    renew_stream_exclusive,
)


@dataclass(frozen=True, slots=True)
class ConsumerGroupWatermark:
    name: str
    last_delivered_id: str
    pending: int
    earliest_pending_id: str | None
    safe_boundary_id: str


@dataclass(slots=True)
class LockedOutboxLedger:
    connection: psycopg.Connection
    total: int
    missing: int
    missing_sample: list[str]


def stream_id_key(value: str) -> tuple[int, int]:
    try:
        milliseconds, sequence = value.split("-", 1)
        parsed = int(milliseconds), int(sequence)
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError(f"invalid Redis Stream ID: {value!r}") from exc
    if parsed[0] < 0 or parsed[1] < 0:
        raise RuntimeError(f"invalid Redis Stream ID: {value!r}")
    return parsed


def minimum_stream_id(*values: str) -> str:
    if not values:
        raise ValueError("at least one Stream ID is required")
    return min(values, key=stream_id_key)


async def inspect_consumer_groups(
    redis: Redis,
    stream: str,
    required_groups: set[str],
) -> list[ConsumerGroupWatermark]:
    raw_groups = await redis.xinfo_groups(stream)
    if not raw_groups:
        raise RuntimeError("safe trimming requires at least one Redis consumer group")
    discovered = {str(item["name"]) for item in raw_groups}
    missing = sorted(required_groups - discovered)
    if missing:
        raise RuntimeError(f"required Redis consumer groups are missing: {', '.join(missing)}")
    result: list[ConsumerGroupWatermark] = []
    for item in raw_groups:
        name = str(item["name"])
        pending = int(item.get("pending", 0))
        last_delivered_id = str(item["last-delivered-id"])
        stream_id_key(last_delivered_id)
        earliest_pending_id: str | None = None
        if pending:
            summary = await redis.xpending(stream, name)
            raw_minimum = summary.get("min") if isinstance(summary, dict) else None
            if not raw_minimum:
                raise RuntimeError(f"consumer group {name} reports pending rows without a minimum ID")
            earliest_pending_id = str(raw_minimum)
            stream_id_key(earliest_pending_id)
        safe_boundary_id = minimum_stream_id(
            last_delivered_id,
            *([earliest_pending_id] if earliest_pending_id else []),
        )
        result.append(ConsumerGroupWatermark(
            name=name,
            last_delivered_id=last_delivered_id,
            pending=pending,
            earliest_pending_id=earliest_pending_id,
            safe_boundary_id=safe_boundary_id,
        ))
    return sorted(result, key=lambda item: item.name)


def lock_outbox_ledger(dsn: str, ledger_path: Path) -> LockedOutboxLedger:
    """Verify replay support and hold row locks until the caller trims Redis."""
    ledger = sqlite3.connect(ledger_path)
    connection: psycopg.Connection | None = None
    try:
        connection = psycopg.connect(dsn, connect_timeout=10)
        total = int(ledger.execute("SELECT count(*) FROM candidate_outbox_ids").fetchone()[0])
        missing_sample: list[str] = []
        verified = 0
        cursor = ledger.execute("SELECT id FROM candidate_outbox_ids ORDER BY id")
        connection.execute("SELECT set_config('statement_timeout','30s',true)")
        while True:
            rows = cursor.fetchmany(1_000)
            if not rows:
                break
            identifiers = [str(row[0]) for row in rows]
            postgres_cursor = connection.execute(
                """SELECT id::text FROM outbox
                WHERE published_at IS NOT NULL
                  AND kind=ANY(%s)
                  AND id=ANY(%s::uuid[])
                FOR SHARE""",
                (list(STREAM_OUTBOX_KINDS), identifiers),
            )
            found = {str(row[0]) for row in postgres_cursor.fetchall()}
            verified += len(found)
            if len(missing_sample) < 10:
                missing_sample.extend(
                    value for value in identifiers if value not in found
                )
                del missing_sample[10:]
        return LockedOutboxLedger(connection, total, total - verified, missing_sample)
    except BaseException:
        if connection is not None:
            connection.close()
        raise
    finally:
        ledger.close()


def release_locked_outbox_ledger(locked: LockedOutboxLedger) -> None:
    try:
        locked.connection.rollback()
    finally:
        locked.connection.close()


async def acquire_locked_outbox_ledger(dsn: str, ledger_path: Path) -> LockedOutboxLedger:
    """Acquire the PG guard without leaking it when the caller is cancelled."""
    task = asyncio.create_task(asyncio.to_thread(lock_outbox_ledger, dsn, ledger_path))
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
        if not task.cancelled() and task.exception() is None:
            cleanup = asyncio.create_task(asyncio.to_thread(
                release_locked_outbox_ledger, task.result(),
            ))
            while not cleanup.done():
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
        raise


async def release_locked_outbox_ledger_safely(locked: LockedOutboxLedger) -> None:
    task = asyncio.create_task(asyncio.to_thread(release_locked_outbox_ledger, locked))
    try:
        await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        raise


async def build_candidate_ledger(
    redis: Redis,
    stream: str,
    trim_min_id: str,
    ledger_path: Path,
) -> tuple[int, int]:
    if stream_id_key(trim_min_id) == (0, 0):
        connection = sqlite3.connect(ledger_path)
        try:
            connection.execute("CREATE TABLE candidate_outbox_ids (id TEXT PRIMARY KEY)")
            connection.commit()
        finally:
            connection.close()
        return 0, 0
    connection = sqlite3.connect(ledger_path)
    invalid_entries = 0
    entry_count = 0
    try:
        connection.execute("CREATE TABLE candidate_outbox_ids (id TEXT PRIMARY KEY)")
        start_id = "-"
        maximum_id = f"({trim_min_id}"
        while True:
            rows = await redis.xrange(stream, min=start_id, max=maximum_id, count=1_000)
            if not rows:
                break
            normalized: list[tuple[str]] = []
            for _, fields in rows:
                entry_count += 1
                raw_id = fields.get("outbox_id")
                try:
                    normalized.append((str(UUID(str(raw_id))),))
                except (AttributeError, TypeError, ValueError):
                    invalid_entries += 1
            connection.executemany(
                "INSERT OR IGNORE INTO candidate_outbox_ids (id) VALUES (?)",
                normalized,
            )
            connection.commit()
            start_id = f"({rows[-1][0]}"
        return entry_count, invalid_entries
    finally:
        connection.close()


def ensure_watermarks_did_not_move_backwards(
    before: list[ConsumerGroupWatermark],
    after: list[ConsumerGroupWatermark],
) -> None:
    initial = {item.name: item for item in before}
    current = {item.name: item for item in after}
    if set(initial) != set(current):
        raise RuntimeError("Redis consumer group membership changed during retention inspection")
    moved_backwards = sorted(
        name for name in initial
        if stream_id_key(current[name].safe_boundary_id) < stream_id_key(initial[name].safe_boundary_id)
    )
    if moved_backwards:
        raise RuntimeError(
            "Redis consumer group safety watermark moved backwards: " + ", ".join(moved_backwards),
        )


async def trim_stream_at_verified_watermarks(
    redis: Redis,
    stream: str,
    trim_min_id: str,
    expected_groups: list[ConsumerGroupWatermark],
) -> int:
    """Atomically recheck the complete consumer-group state and trim the prefix."""
    arguments: list[str | int] = [trim_min_id, len(expected_groups)]
    for group in expected_groups:
        arguments.extend((
            group.name,
            group.last_delivered_id,
            group.pending,
            group.earliest_pending_id or "",
        ))
    operation = asyncio.create_task(redis.eval(
        """local groups=redis.call('XINFO','GROUPS',KEYS[1])
        local expected_count=tonumber(ARGV[2])
        if #groups~=expected_count then return {0,'consumer group membership changed'} end
        local expected={}
        local position=3
        for index=1,expected_count do
          expected[ARGV[position]]={ARGV[position+1],ARGV[position+2],ARGV[position+3]}
          position=position+4
        end
        for _,raw_group in ipairs(groups) do
          local attributes={}
          for index=1,#raw_group,2 do
            attributes[tostring(raw_group[index])]=tostring(raw_group[index+1])
          end
          local name=attributes['name']
          local wanted=expected[name]
          if not wanted then return {0,'consumer group membership changed'} end
          local pending=tonumber(attributes['pending'] or '-1')
          local earliest=''
          if pending>0 then
            local summary=redis.call('XPENDING',KEYS[1],name)
            if not summary[2] then return {0,'pending consumer group has no minimum ID'} end
            earliest=tostring(summary[2])
          end
          if attributes['last-delivered-id']~=wanted[1]
            or tostring(pending)~=wanted[2]
            or earliest~=wanted[3] then
            return {0,'consumer group state changed'}
          end
        end
        local trimmed=redis.call('XTRIM',KEYS[1],'MINID','=',ARGV[1])
        return {1,trimmed}""",
        1,
        stream,
        *arguments,
    ))
    try:
        result = await asyncio.shield(operation)
    except asyncio.CancelledError:
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        raise
    if not isinstance(result, (list, tuple)) or len(result) != 2:
        raise RuntimeError("Redis returned an invalid atomic retention result")
    if int(result[0]) != 1:
        raise RuntimeError(f"atomic Redis retention refused: {result[1]}")
    return int(result[1])


async def maintain_stream_retention(
    dsn: str,
    redis: Redis,
    stream: str,
    *,
    required_groups: set[str],
    retention_hours: int,
    capacity_limit: int,
    execute: bool = False,
    confirmed_trim_min_id: str | None = None,
) -> dict[str, object]:
    if retention_hours < 0:
        raise ValueError("retention_hours cannot be negative")
    if capacity_limit < 1:
        raise ValueError("capacity_limit must be positive")
    if not required_groups:
        raise ValueError("at least one required consumer group is required")
    if not await redis.exists(stream):
        raise RuntimeError(f"Redis Stream does not exist: {stream}")
    if str(await redis.type(stream)) != "stream":
        raise RuntimeError(f"Redis key is not a Stream: {stream}")

    exclusive_token = await acquire_stream_exclusive(redis, stream, "retention")
    exclusive_lost = asyncio.Event()
    lease_task = asyncio.create_task(maintain_stream_exclusive(
        redis, stream, exclusive_token, exclusive_lost,
    ))
    ledger_path: Path | None = None
    try:
        temporary = tempfile.NamedTemporaryFile(
            prefix="radar-stream-retention-", suffix=".sqlite3", delete=False,
        )
        ledger_path = Path(temporary.name)
        temporary.close()
        groups_before = await inspect_consumer_groups(redis, stream, required_groups)
        redis_time = await redis.time()
        now_milliseconds = int(redis_time[0]) * 1_000 + int(redis_time[1]) // 1_000
        cutoff_milliseconds = max(0, now_milliseconds - retention_hours * 3_600_000)
        retention_boundary = f"{cutoff_milliseconds}-0"
        consumer_boundary = minimum_stream_id(
            *(item.safe_boundary_id for item in groups_before),
        )
        calculated_trim_min_id = minimum_stream_id(retention_boundary, consumer_boundary)
        trim_min_id = calculated_trim_min_id
        if execute:
            if confirmed_trim_min_id is None:
                raise RuntimeError("execute requires the safeTrimMinId from a preceding dry-run")
            stream_id_key(confirmed_trim_min_id)
            if stream_id_key(calculated_trim_min_id) < stream_id_key(confirmed_trim_min_id):
                raise RuntimeError(
                    "current safety boundary moved behind the confirmed safeTrimMinId; run dry-run again",
                )
            trim_min_id = confirmed_trim_min_id
        before_length = int(await redis.xlen(stream))
        candidate_entries, invalid_entries = await build_candidate_ledger(
            redis, stream, trim_min_id, ledger_path,
        )
        locked_outbox = await acquire_locked_outbox_ledger(dsn, ledger_path)
        try:
            unique_outbox_ids = locked_outbox.total
            missing_outbox_ids = locked_outbox.missing
            missing_sample = locked_outbox.missing_sample
            groups_after = await inspect_consumer_groups(redis, stream, required_groups)
            ensure_watermarks_did_not_move_backwards(groups_before, groups_after)
            if exclusive_lost.is_set():
                raise RuntimeError("exclusive stream retention lease was lost before trim")
            estimated_after_length = max(0, before_length - candidate_entries)
            report: dict[str, object] = {
                "mode": "execute" if execute else "dry-run",
                "stream": stream,
                "retentionHours": retention_hours,
                "capacityLimit": capacity_limit,
                "beforeLength": before_length,
                "retentionBoundaryId": retention_boundary,
                "consumerBoundaryId": consumer_boundary,
                "safeTrimMinId": trim_min_id,
                "calculatedSafeTrimMinId": calculated_trim_min_id,
                "recoverableKinds": list(STREAM_OUTBOX_KINDS),
                "candidateEntries": candidate_entries,
                "uniqueOutboxIds": unique_outbox_ids,
                "invalidOutboxIdEntries": invalid_entries,
                "missingPublishedOutboxIds": missing_outbox_ids,
                "missingOutboxIdSample": missing_sample,
                "estimatedAfterLength": estimated_after_length,
                "capacityStatusAfterSafeTrim": (
                    "within_limit" if estimated_after_length <= capacity_limit else "over_limit"
                ),
                "groups": [
                    {
                        "name": item.name,
                        "lastDeliveredId": item.last_delivered_id,
                        "pending": item.pending,
                        "earliestPendingId": item.earliest_pending_id,
                        "safeBoundaryId": item.safe_boundary_id,
                    }
                    for item in groups_before
                ],
                "trimmedEntries": 0,
            }
            if not execute:
                return report
            if invalid_entries or missing_outbox_ids:
                raise RuntimeError(
                    "safe trim refused because candidate entries are not recoverable by the supported Outbox replay kinds",
                )
            await renew_stream_exclusive(redis, stream, exclusive_token)
            trimmed = 0
            if stream_id_key(trim_min_id) != (0, 0):
                trimmed = await trim_stream_at_verified_watermarks(
                    redis, stream, trim_min_id, groups_after,
                )
            if trimmed > candidate_entries:
                raise RuntimeError("Redis trimmed more entries than the verified candidate ledger")
            after_length = int(await redis.xlen(stream))
            return {
                **report,
                "trimmedEntries": trimmed,
                "afterLength": after_length,
                "capacityStatusAfterSafeTrim": (
                    "within_limit" if after_length <= capacity_limit else "over_limit"
                ),
            }
        finally:
            primary_error = sys.exc_info()[1]
            try:
                await release_locked_outbox_ledger_safely(locked_outbox)
            except BaseException as cleanup_error:
                if primary_error is None:
                    raise
                primary_error.add_note(
                    "PostgreSQL Outbox guard cleanup also failed: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}",
                )
    finally:
        active_exception = sys.exc_info()[0] is not None
        exclusive_lost.set()
        lease_task.cancel()
        with suppress(asyncio.CancelledError):
            await lease_task
        release_error: Exception | None = None
        try:
            await release_stream_exclusive(redis, stream, exclusive_token)
        except Exception as exc:
            release_error = exc
        try:
            if ledger_path is not None:
                ledger_path.unlink(missing_ok=True)
        except OSError:
            if not active_exception:
                raise
        if release_error and not active_exception:
            raise release_error
