"""
Database operations for EPG data

This module contains all database CRUD operations for channels and programs.
"""
import logging
from datetime import datetime

from sqlalchemy import func, select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Channel, Program
from app.utils.data_merging import ChannelTuple, ProgramDict


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
    cutoff_iso = cutoff_time.isoformat()

    # Get count before deletion
    result = await db.execute(select(func.count(Program.id)).where(Program.start_time < cutoff_iso))
    deleted_count = result.scalar() or 0

    # Delete programs using delete statement
    stmt = delete(Program).where(Program.start_time < cutoff_iso)
    await db.execute(stmt)
    await db.commit()

    logger.info(f"Deleted {deleted_count} old programs (before {cutoff_time.date()})")
    return deleted_count


async def store_channels(db: AsyncSession, channels: list[ChannelTuple]) -> None:
    """
    Store channels in database using SQLAlchemy ORM merge operation.

    Args:
        db: Database session
        channels: List of channel tuples (xmltv_id, display_name, icon_url)
    """
    if not channels:
        return

    # Create Channel ORM objects and merge them into the session
    # merge() handles INSERT OR REPLACE semantics for SQLite
    for xmltv_id, display_name, icon_url in channels:
        channel = Channel(
            xmltv_id=xmltv_id,
            display_name=display_name,
            icon_url=icon_url
        )
        await db.merge(channel)

    await db.commit()
    logger.info(f"Stored {len(channels)} channels")


async def store_programs(db: AsyncSession, programs: list[ProgramDict]) -> int:
    """
    Store programs in database using batch operations for efficiency.

    Instead of checking each program individually (N+1 queries), this loads all
    existing IDs in one query and then batch inserts new programs.

    Args:
        db: Database session
        programs: List of program dictionaries

    Returns:
        Number of programs actually inserted (excluding duplicates)
    """
    if not programs:
        return 0

    # Step 1: Get all existing program IDs in a single query (not N queries)
    program_ids = [p['id'] for p in programs]
    existing_result = await db.execute(
        select(Program.id).where(Program.id.in_(program_ids))
    )
    existing_ids = {row[0] for row in existing_result.fetchall()}

    # Step 2: Create Program objects only for new programs
    new_programs = []
    for program_dict in programs:
        if program_dict['id'] not in existing_ids:
            try:
                new_programs.append(Program(
                    id=program_dict['id'],
                    xmltv_channel_id=program_dict['xmltv_channel_id'],
                    start_time=program_dict['start_time'],
                    stop_time=program_dict['stop_time'],
                    title=program_dict['title'],
                    description=program_dict.get('description')
                ))
            except Exception as e:
                # Skip programs with invalid data
                logger.debug(f"Skipping program {program_dict['id']}: {str(e)}")

    # Step 3: Batch insert all new programs (one operation)
    if new_programs:
        db.add_all(new_programs)
        await db.commit()

    skipped_count = len(programs) - len(new_programs)
    logger.info(f"Stored {len(new_programs)} new programs (skipped {skipped_count} duplicates)")

    return len(new_programs)
