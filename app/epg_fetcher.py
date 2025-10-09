from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import asyncio

import aiosqlite
import httpx
import aiofiles

from app.config import settings
from app.xmltv_parser import parse_xmltv_file

logger = logging.getLogger("epg_service.fetcher")


async def fetch_and_process() -> dict:
    """
    Fetch EPG from source, parse and store in database

    Returns:
        Dictionary with fetch statistics or error
    """
    logger.info("="*60)
    logger.info(f"Starting EPG fetch at {datetime.now(timezone.utc).isoformat()}")
    logger.info("="*60)

    if not settings.epg_source_url:
        logger.error("EPG_SOURCE_URL not configured")
        return {"error": "EPG_SOURCE_URL not configured"}

    temp_file = None
    try:
        now = datetime.now(timezone.utc)
        time_from = now - timedelta(days=14)
        time_to = now + timedelta(days=7)

        logger.info(f"Time window: {time_from.date()} to {time_to.date()}")
        logger.info(f"Source: {settings.epg_source_url}")

        temp_file = await _download_xmltv(settings.epg_source_url)
        channels, programs = await _parse_xmltv(temp_file, time_from, time_to)
        stats = await _store_data(channels, programs, time_from)

        logger.info("="*60)
        logger.info("EPG fetch completed successfully")
        logger.info("="*60)

        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **stats
        }

    except httpx.HTTPError as e:
        error_msg = f"HTTP error during EPG fetch: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}
    except Exception as e:
        error_msg = f"Error during EPG fetch: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}
    finally:
        if temp_file and temp_file.exists():
            try:
                temp_file.unlink()
                logger.debug(f"Cleaned up temporary file: {temp_file}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file}: {e}")


async def _download_xmltv(url: str) -> Path:
    """Download XMLTV file from URL"""
    logger.info("Downloading XMLTV file...")

    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.get(url)
        response.raise_for_status()

        temp_file = Path("/tmp/epg.xml")

        async with aiofiles.open(temp_file, 'wb') as f:
            await f.write(response.content)

        file_size = len(response.content) / (1024 * 1024)
        logger.info(f"Downloaded {file_size:.2f} MB")

        return temp_file


async def _parse_xmltv(
    file_path: Path,
    time_from: datetime,
    time_to: datetime
) -> tuple[list[tuple], list[dict]]:
    """Parse XMLTV file and return channels and programs"""
    logger.info("Parsing XMLTV file...")

    loop = asyncio.get_event_loop()
    channels, programs = await loop.run_in_executor(
        None,
        parse_xmltv_file,
        str(file_path),
        time_from,
        time_to
    )

    if not channels:
        raise ValueError("No channels found in XMLTV")

    if not programs:
        raise ValueError("No programs found in XMLTV")

    logger.info(f"Parsed {len(channels)} channels and {len(programs)} programs")

    return channels, programs


async def _store_data(
    channels: list[tuple],
    programs: list[dict],
    time_from: datetime
) -> dict:
    """Store channels and programs in database"""
    logger.info("="*60)
    logger.info("Storing data in database...")
    logger.info("="*60)

    async with aiosqlite.connect(settings.database_path) as db:
        deleted_count = await _delete_old_programs(db, time_from)
        await _store_channels(db, channels)
        inserted_count = await _store_programs(db, programs)
        await db.commit()

    return {
        "channels": len(channels),
        "programs_parsed": len(programs),
        "programs_inserted": inserted_count,
        "programs_deleted": deleted_count
    }


async def _delete_old_programs(db: aiosqlite.Connection, time_from: datetime) -> int:
    """Delete programs older than time_from"""
    cursor = await db.execute(
        "DELETE FROM programs WHERE start_time < ?",
        (time_from.isoformat(),)
    )
    deleted_count = cursor.rowcount
    logger.info(f"Deleted {deleted_count} old programs")
    return deleted_count


async def _store_channels(db: aiosqlite.Connection, channels: list[tuple]) -> None:
    """Store channels in database"""
    await db.executemany(
        "INSERT OR REPLACE INTO channels (xmltv_id, display_name, icon_url) VALUES (?, ?, ?)",
        channels
    )
    logger.info(f"Stored {len(channels)} channels")


async def _store_programs(db: aiosqlite.Connection, programs: list[dict]) -> int:
    """Store programs in database and return count of inserted programs"""
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
