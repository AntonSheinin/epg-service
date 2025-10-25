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

    Note: This function does NOT commit the transaction. The caller must handle
    transaction management using async context managers (db.begin()).

    Args:
        db: Database session
        channels: List of channel tuples (xmltv_id, display_name, icon_url)
    """
    if not channels:
        logger.debug("No channels to store")
        return

    logger.info(f"Starting to store {len(channels)} channels...")
    logger.debug(f"  First 3 channels: {[ch[0] for ch in channels[:3]]}")

    # Create Channel ORM objects and merge them into the session
    # merge() handles INSERT OR REPLACE semantics for SQLite
    stored_count = 0
    for xmltv_id, display_name, icon_url in channels:
        try:
            channel = Channel(
                xmltv_id=xmltv_id,
                display_name=display_name,
                icon_url=icon_url
            )
            await db.merge(channel)
            stored_count += 1
        except Exception as e:
            logger.error(f"Error storing channel {xmltv_id}: {e}")
            raise

    logger.debug(f"Merged {stored_count} channel objects into session (awaiting transaction commit)...")
    logger.info(f"Prepared {len(channels)} channels for storage")


async def store_programs(db: AsyncSession, programs: list[ProgramDict]) -> int:
    """
    Store programs in database using batch operations for efficiency.

    Instead of checking each program individually (N+1 queries), this loads all
    existing IDs in one query and then batch inserts new programs.

    Note: This function does NOT commit the transaction. The caller must handle
    transaction management using async context managers (db.begin()).

    Args:
        db: Database session
        programs: List of program dictionaries

    Returns:
        Number of programs actually inserted (excluding duplicates)
    """
    if not programs:
        logger.debug("No programs to store")
        return 0

    logger.info(f"Starting to store {len(programs)} programs...")

    # Step 1: Build a set of existing program keys based on the UNIQUE constraint:
    # (xmltv_channel_id, start_time, title)
    logger.debug(f"Querying for existing programs using constraint keys...")
    existing_result = await db.execute(
        select(Program.xmltv_channel_id, Program.start_time, Program.title).select_from(Program)
    )
    existing_keys = set()
    for row in existing_result.fetchall():
        # Create a tuple key: (channel, start_time, title)
        existing_keys.add((row[0], row[1], row[2]))
    logger.info(f"Found {len(existing_keys)} existing programs in database")

    # Step 2: Create Program objects only for new programs
    new_programs = []
    skipped_invalid = 0
    skipped_duplicate = 0

    logger.debug(f"Creating Program objects for new programs...")
    for program_dict in programs:
        # Create the constraint key for this program
        program_key = (
            program_dict['xmltv_channel_id'],
            program_dict['start_time'],
            program_dict['title']
        )

        # Skip if this program already exists
        if program_key in existing_keys:
            skipped_duplicate += 1
            continue

        try:
            new_programs.append(Program(
                id=program_dict['id'],
                xmltv_channel_id=program_dict['xmltv_channel_id'],
                start_time=program_dict['start_time'],
                stop_time=program_dict['stop_time'],
                title=program_dict['title'],
                description=program_dict.get('description')
            ))
        except (ValueError, TypeError, KeyError) as e:
            # Skip programs with invalid data types or missing keys
            skipped_invalid += 1
            logger.warning(f"Skipping program with invalid data - {program_dict.get('id', 'unknown')}: {type(e).__name__}: {e}")
        except Exception as e:
            # Log unexpected exceptions with full context
            logger.error(f"Unexpected error processing program {program_dict.get('id', 'unknown')}: {e}", exc_info=True)
            skipped_invalid += 1

    logger.info(f"Created {len(new_programs)} valid Program objects, {skipped_duplicate} duplicates skipped, {skipped_invalid} invalid programs skipped")

    # Step 3: Batch insert all new programs (one operation)
    if new_programs:
        logger.debug(f"Batch inserting {len(new_programs)} programs into session...")
        db.add_all(new_programs)
        logger.info(f"Prepared {len(new_programs)} new programs for storage (awaiting transaction commit)...")

    logger.info(f"Store preparation complete: {len(new_programs)} new programs staged, {skipped_duplicate} duplicates skipped, {skipped_invalid} invalid programs skipped")

    return len(new_programs)
