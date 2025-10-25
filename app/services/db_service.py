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
    Delete programs older than specified cutoff time

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
    Store channels in database using SQLAlchemy ORM merge operation

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
    Store programs in database and return count of inserted programs

    Args:
        db: Database session
        programs: List of program dictionaries

    Returns:
        Number of programs actually inserted (excluding duplicates)
    """
    inserted_count = 0
    skipped_count = 0

    for program_dict in programs:
        try:
            # Check if program already exists
            existing = await db.execute(
                select(Program).where(Program.id == program_dict['id'])
            )
            if existing.scalar_one_or_none() is None:
                db.add(Program(
                    id=program_dict['id'],
                    xmltv_channel_id=program_dict['xmltv_channel_id'],
                    start_time=program_dict['start_time'],
                    stop_time=program_dict['stop_time'],
                    title=program_dict['title'],
                    description=program_dict.get('description')
                ))
                inserted_count += 1
            else:
                skipped_count += 1
        except Exception as e:
            # Skip programs that violate unique constraints
            logger.debug(f"Skipping duplicate program: {program_dict['title']} on {program_dict['xmltv_channel_id']}: {str(e)}")
            skipped_count += 1

    await db.commit()

    logger.info(f"Stored {inserted_count} new programs (skipped {skipped_count} duplicates)")

    return inserted_count
