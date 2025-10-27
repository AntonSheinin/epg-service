"""
Database operations for EPG data

This module contains all database CRUD operations for channels and programs.
"""
import logging
from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import delete, func, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from app.models import Channel, Program
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
    channels = list(channels)
    if not channels:
        logger.debug("No channels to store")
        return

    logger.info("Storing %s channels", len(channels))
    channel_values = [
        {
            "xmltv_id": channel.xmltv_id,
            "display_name": channel.display_name,
            "icon_url": channel.icon_url,
        }
        for channel in channels
    ]

    stmt = sqlite_insert(Channel).values(channel_values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["xmltv_id"],
        set_={
            "display_name": stmt.excluded.display_name,
            "icon_url": func.coalesce(stmt.excluded.icon_url, Channel.icon_url),
        },
    )

    await db.execute(stmt)
    logger.debug("Channel upsert complete")


async def store_programs(db: AsyncSession, programs: Sequence[ProgramPayload]) -> int:
    """
    Store programs in database using batch upserts with ON CONFLICT DO NOTHING.

    Args:
        db: Database session
        programs: Iterable of ProgramPayload objects

    Returns:
        Number of programs successfully inserted
    """
    program_list = list(programs)
    if not program_list:
        logger.debug("No programs to store")
        return 0

    total_programs = len(program_list)
    logger.info("Storing %s programs", total_programs)

    chunk_size = 20000
    inserted_count = 0
    duplicates = 0
    index_dropped = False

    try:
        if program_list:
            try:
                await _drop_program_time_index(db)
                index_dropped = True
            except Exception as exc:
                logger.warning("Could not drop idx_programs_channel_time: %s", exc, exc_info=True)

        for start_index in range(0, total_programs, chunk_size):
            chunk = program_list[start_index:start_index + chunk_size]
            now = datetime.now(timezone.utc)
            payload = [
                {
                    "id": program.id,
                    "xmltv_channel_id": program.xmltv_channel_id,
                    "start_time": program.start_time,
                    "stop_time": program.stop_time,
                    "title": program.title,
                    "description": program.description,
                    "created_at": program.created_at or now,
                }
                for program in chunk
            ]

            stmt = sqlite_insert(Program).values(payload)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["xmltv_channel_id", "start_time", "title"]
            )
            stmt = stmt.returning(Program.id)

            try:
                result = await db.execute(stmt)
            except IntegrityError as exc:
                logger.error("Integrity error while inserting programs: %s", exc, exc_info=True)
                continue

            inserted_ids = result.scalars().all()
            chunk_inserted = len(inserted_ids)

            inserted_count += chunk_inserted
            duplicates += len(chunk) - chunk_inserted

            logger.debug(
                "Inserted %s programs in current chunk (%s duplicates ignored)",
                chunk_inserted,
                len(chunk) - chunk_inserted,
            )

        logger.info(
            "Program store complete: %s inserted, %s ignored due to duplicates",
            inserted_count,
            duplicates,
        )

        return inserted_count
    finally:
        if index_dropped:
            try:
                await _create_program_time_index(db)
            except Exception as exc:
                logger.error("Failed to recreate idx_programs_channel_time: %s", exc, exc_info=True)
