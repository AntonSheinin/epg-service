"""
Database operations for EPG data

This module contains all database CRUD operations for channels and programs.
"""
import asyncio
import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from time import perf_counter
from typing import cast

from sqlalchemy import delete, func, select, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.models import Program
from app.services.fetch_types import ChannelPayload, ProgramPayload


logger = logging.getLogger(__name__)


async def delete_old_programs(db: AsyncSession, cutoff_time: datetime) -> int:
    """
    Delete programs older than specified cutoff time.

    Args:
        db: Database session
        cutoff_time: Delete programs with start_time before this

    Returns:
        Number of deleted programs
    """
    result = await db.execute(
        select(func.count(Program.id)).where(Program.start_time < cutoff_time)
    )
    deleted_count = result.scalar_one_or_none() or 0

    stmt = delete(Program).where(Program.start_time < cutoff_time)
    await db.execute(stmt)

    logger.info("Deleted %s old programs (start_time < %s)", deleted_count, cutoff_time.date())
    return deleted_count


async def delete_future_programs(db: AsyncSession, cutoff_time: datetime) -> int:
    """
    Delete programs scheduled after the specified cutoff.

    Args:
        db: Database session
        cutoff_time: Remove programs with start_time greater than this value

    Returns:
        Number of deleted programs
    """
    result = await db.execute(
        select(func.count(Program.id)).where(Program.start_time > cutoff_time)
    )
    deleted_count = result.scalar_one_or_none() or 0

    stmt = delete(Program).where(Program.start_time > cutoff_time)
    await db.execute(stmt)

    logger.info("Deleted %s future programs (start_time > %s)", deleted_count, cutoff_time.date())
    return deleted_count


async def _drop_program_time_index(db: AsyncSession) -> None:
    """Drop the program time index before bulk insert to avoid repetitive rebuilds."""
    await db.execute(text("DROP INDEX IF EXISTS idx_programs_channel_time"))
    logger.debug("Dropped idx_programs_channel_time prior to bulk insert")


async def _create_program_time_index(db: AsyncSession) -> None:
    """Recreate the program time index after bulk insert completes."""
    await db.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_programs_channel_time "
            "ON programs (xmltv_channel_id, start_time)"
        )
    )
    logger.debug("Recreated idx_programs_channel_time after bulk insert")


async def store_channels(db: AsyncSession, channels: Sequence[ChannelPayload]) -> None:
    """
    Store channels in database using UPSERT semantics for immediate availability.

    Args:
        db: Database session
        channels: Iterable of ChannelPayload objects
    """
    channel_list = list(channels)
    if not channel_list:
        logger.debug("No channels to store")
        return

    # Deduplicate by xmltv_id while preserving last occurrence
    deduped_channels: dict[str, ChannelPayload] = {
        channel.xmltv_id: channel for channel in channel_list
    }

    logger.info("Storing %s channels", len(deduped_channels))

    channel_stmt = text(
        """
        INSERT INTO channels (xmltv_id, display_name, icon_url, created_at)
        VALUES (:xmltv_id, :display_name, :icon_url, :created_at)
        ON CONFLICT(xmltv_id) DO UPDATE SET
            display_name = excluded.display_name,
            icon_url = COALESCE(excluded.icon_url, channels.icon_url)
        """
    )

    payload = [
        {
            "xmltv_id": channel.xmltv_id,
            "display_name": channel.display_name,
            "icon_url": channel.icon_url,
            "created_at": datetime.now(timezone.utc),
        }
        for channel in deduped_channels.values()
    ]

    chunk_size = 1000
    for start_index in range(0, len(payload), chunk_size):
        chunk = payload[start_index:start_index + chunk_size]
        await db.execute(channel_stmt, chunk)

    logger.debug("Channel upsert complete using executemany batches")


async def prepare_program_bulk_insert(db: AsyncSession, *, drop_index: bool = True) -> None:
    """Prepare SQLite connection for high-throughput program inserts."""
    logger.info("Preparing database connection for bulk program insert")
    connection = await db.connection()
    await connection.exec_driver_sql("PRAGMA synchronous = OFF")
    await connection.exec_driver_sql("PRAGMA wal_autocheckpoint = 0")
    await connection.exec_driver_sql("PRAGMA temp_store = MEMORY")
    await connection.exec_driver_sql("PRAGMA cache_size = -200000")
    await connection.exec_driver_sql("BEGIN IMMEDIATE")
    if drop_index:
        await _drop_program_time_index(db)


async def finalize_program_bulk_insert(
    db: AsyncSession,
    *,
    rebuild_index: bool = False,
    checkpoint: bool = False,
) -> None:
    """Restore SQLite connection settings after bulk insert."""
    logger.info(
        "Finalizing bulk insert step (rebuild_index=%s, checkpoint=%s)",
        rebuild_index,
        checkpoint,
    )
    if db.in_transaction():
        await db.commit()
    if rebuild_index:
        await _create_program_time_index(db)
        await db.commit()
    if checkpoint:
        await _run_wal_checkpoint(db, retries=6)
    connection = await db.connection()
    await connection.exec_driver_sql("PRAGMA wal_autocheckpoint = 1000")
    await connection.exec_driver_sql("PRAGMA synchronous = NORMAL")


async def _run_wal_checkpoint(db: AsyncSession, *, retries: int = 5) -> None:
    """Attempt WAL checkpoint until no readers remain or retries exhausted."""
    connection = await db.connection()
    backoff = 1.0
    for attempt in range(1, retries + 1):
        result = await connection.exec_driver_sql("PRAGMA wal_checkpoint(TRUNCATE)")
        row = result.fetchone()
        if row is None:
            logger.warning("WAL checkpoint returned no data; skipping further attempts")
            return
        busy, log_frames, checkpointed = row
        if busy == 0:
            logger.info(
                "WAL checkpoint completed (log_frames=%s, checkpointed=%s)",
                log_frames,
                checkpointed,
            )
            return

        logger.warning(
            "WAL checkpoint busy on attempt %s/%s (log_frames=%s, checkpointed=%s); retrying in %.1fs",
            attempt,
            retries,
            log_frames,
            checkpointed,
            backoff,
        )
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30.0)

    logger.error(
        "WAL checkpoint could not complete after %s attempts; readers may still be active",
        retries,
    )


async def store_programs(
    db: AsyncSession,
    programs: Sequence[ProgramPayload],
    existing_program_ids: set[str] | None = None,
) -> int:
    """
    Store programs in database using batch upserts with ON CONFLICT DO NOTHING.

    Args:
        db: Database session
        programs: Iterable of ProgramPayload objects
        existing_program_ids: Optional shared cache of already persisted program IDs
            used to short-circuit duplicate inserts during long fetch cycles.

    Returns:
        Number of programs successfully inserted
    """
    program_list = list(programs)
    if not program_list:
        logger.debug("No programs to store")
        return 0

    total_programs = len(program_list)
    logger.info("Storing %s programs", total_programs)

    chunk_size = 100000
    inserted_count = 0
    duplicates = 0
    dedupe_cache = existing_program_ids if existing_program_ids is not None else set()
    seen_ids: set[str] = set()

    insert_stmt = text(
        """
        INSERT INTO programs (
            id,
            xmltv_channel_id,
            start_time,
            stop_time,
            title,
            description,
            created_at
        )
        VALUES (
            :id,
            :xmltv_channel_id,
            :start_time,
            :stop_time,
            :title,
            :description,
            :created_at
        )
        ON CONFLICT(xmltv_channel_id, start_time, title) DO NOTHING
        """
    )

    chunk_number = 0
    for start_index in range(0, total_programs, chunk_size):
        chunk_number += 1
        loop_start = perf_counter()

        chunk = program_list[start_index:start_index + chunk_size]
        original_chunk_size = len(chunk)

        now = datetime.now(timezone.utc)
        payload: list[dict[str, object]] = []
        for program in chunk:
            program_id = program.id
            if program_id in dedupe_cache or program_id in seen_ids:
                continue

            seen_ids.add(program_id)
            if not program.created_at:
                program.created_at = now

            payload.append(
                {
                    "id": program_id,
                    "xmltv_channel_id": program.xmltv_channel_id,
                    "start_time": program.start_time,
                    "stop_time": program.stop_time,
                    "title": program.title,
                    "description": program.description,
                    "created_at": program.created_at,
                }
            )

        chunk_duplicates = original_chunk_size - len(payload)
        if chunk_duplicates:
            duplicates += chunk_duplicates

        if not payload:
            logger.info(
                "Chunk %s skipped (all %s programs already present)",
                chunk_number,
                original_chunk_size,
            )
            continue

        execute_start = perf_counter()
        try:
            raw_result = await db.execute(insert_stmt, payload)
        except IntegrityError as exc:
            logger.error("Integrity error while inserting programs: %s", exc, exc_info=True)
            continue
        execute_duration = perf_counter() - execute_start

        cursor_result = cast(CursorResult, raw_result)
        rowcount = cursor_result.rowcount
        if rowcount is None or rowcount < 0:
            chunk_inserted = len(payload)
        else:
            chunk_inserted = rowcount

        inserted_count += chunk_inserted

        if chunk_inserted:
            for entry in payload:
                dedupe_cache.add(cast(str, entry["id"]))

        conflict_skips = len(payload) - chunk_inserted
        if conflict_skips > 0:
            duplicates += conflict_skips

        total_duration = perf_counter() - loop_start
        logger.info(
            "Chunk %s persisted: payload=%s, inserted=%s, duplicates_pre=%s, conflict_skips=%s,"
            " exec_time=%.2fs, total_time=%.2fs",
            chunk_number,
            len(payload),
            chunk_inserted,
            chunk_duplicates,
            conflict_skips,
            execute_duration,
            total_duration,
        )

    logger.info(
        "Program store complete: %s inserted, %s ignored due to duplicates",
        inserted_count,
        duplicates,
    )

    return inserted_count
