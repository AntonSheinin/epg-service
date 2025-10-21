"""
Database operations for EPG data

This module contains all database CRUD operations for channels and programs.
"""
import logging
from datetime import datetime

import aiosqlite

from app.utils.data_merging import ChannelTuple, ProgramDict


logger = logging.getLogger(__name__)

async def delete_old_programs(db: aiosqlite.Connection, cutoff_time: datetime) -> int:
    """
    Delete programs older than specified cutoff time

    Args:
        db: Database connection
        cutoff_time: Delete programs with start_time before this

    Returns:
        Number of deleted programs
    """
    cursor = await db.execute("DELETE FROM programs WHERE start_time < ?",(cutoff_time.isoformat(),))
    deleted_count = cursor.rowcount
    logger.info(f"Deleted {deleted_count} old programs (before {cutoff_time.date()})")
    return deleted_count


async def store_channels(db: aiosqlite.Connection, channels: list[ChannelTuple]) -> None:
    """
    Store channels in database (INSERT OR REPLACE)

    Args:
        db: Database connection
        channels: List of channel tuples (xmltv_id, display_name, icon_url)
    """
    await db.executemany("INSERT OR REPLACE INTO channels (xmltv_id, display_name, icon_url) VALUES (?, ?, ?)",
        channels
    )
    logger.info(f"Stored {len(channels)} channels")


async def store_programs(db: aiosqlite.Connection, programs: list[ProgramDict]) -> int:
    """
    Store programs in database (INSERT OR IGNORE) and return count of inserted programs

    Args:
        db: Database connection
        programs: List of program dictionaries

    Returns:
        Number of programs actually inserted (excluding duplicates)
    """
    cursor = await db.execute("SELECT COUNT(*) FROM programs")
    result = await cursor.fetchone()
    before_count = result[0] if result else 0

    program_tuples = [
        (
            p['id'],
            p['xmltv_channel_id'],
            p['start_time'],
            p['stop_time'],
            p['title'],
            p['description']
        )
        for p in programs
    ]

    await db.executemany(
        """INSERT OR IGNORE INTO programs
           (id, xmltv_channel_id, start_time, stop_time, title, description)
           VALUES (?, ?, ?, ?, ?, ?)""",
        program_tuples
    )

    cursor = await db.execute("SELECT COUNT(*) FROM programs")
    result = await cursor.fetchone()
    after_count = result[0] if result else 0

    inserted_count = after_count - before_count
    skipped_count = len(programs) - inserted_count

    logger.info(f"Stored {inserted_count} new programs (skipped {skipped_count} duplicates)")

    return inserted_count
