from datetime import datetime, timedelta, timezone
from pathlib import Path
import logging
import asyncio
import tempfile

import aiosqlite
import httpx
import aiofiles

from app.config import settings
from app.xmltv_parser import parse_xmltv_file

logger = logging.getLogger("epg_service.fetcher")


async def fetch_and_process() -> dict:
    """
    Fetch EPG from all sources, parse and store in database

    Returns:
        Dictionary with fetch statistics or error
    """
    logger.info("="*60)
    logger.info(f"Starting EPG fetch at {datetime.now(timezone.utc).isoformat()}")
    logger.info(f"Processing {len(settings.epg_sources)} source(s)")
    logger.info("="*60)

    if not settings.epg_sources:
        logger.error("EPG_SOURCES not configured")
        return {"error": "EPG_SOURCES not configured"}

    now = datetime.now(timezone.utc)
    time_from = now - timedelta(days=14)
    time_to = now + timedelta(days=7)

    logger.info(f"Time window: {time_from.date()} to {time_to.date()}")

    try:
        # Delete old programs first (before fetching new data)
        async with aiosqlite.connect(settings.database_path) as db:
            deleted_count = await _delete_old_programs(db, time_from)
            await db.commit()

        all_channels: dict[str, tuple[str, str, str | None]] = {}  # xmltv_id -> (xmltv_id, display_name, icon_url)
        all_programs: dict[str, dict] = {}  # program_key -> program_dict

        source_stats = []

        # Process each source sequentially
        for idx, source_url in enumerate(settings.epg_sources, 1):
            logger.info("="*60)
            logger.info(f"Processing source {idx}/{len(settings.epg_sources)}")
            logger.info(f"URL: {source_url}")
            logger.info("="*60)

            temp_file = None
            try:
                temp_file = await _download_xmltv(source_url, idx)
                channels, programs = await _parse_xmltv(temp_file, time_from, time_to)

                # Merge channels (first source wins for duplicates)
                new_channels = 0
                for channel in channels:
                    xmltv_id = channel[0]
                    if xmltv_id not in all_channels:
                        all_channels[xmltv_id] = channel
                        new_channels += 1
                    else:
                        logger.debug(f"Skipping duplicate channel: {xmltv_id} (already exists from previous source)")

                # Merge programs (first source wins for duplicates)
                new_programs = 0
                for program in programs:
                    program_key = f"{program['xmltv_channel_id']}_{program['start_time']}_{program['title']}"
                    if program_key not in all_programs:
                        all_programs[program_key] = program
                        new_programs += 1
                    else:
                        logger.debug(f"Skipping duplicate program: {program['title']} on {program['xmltv_channel_id']}")

                source_stats.append({
                    "source_index": idx,
                    "source_url": source_url,
                    "status": "success",
                    "channels_parsed": len(channels),
                    "channels_new": new_channels,
                    "programs_parsed": len(programs),
                    "programs_new": new_programs
                })

                logger.info(f"Source {idx}: {new_channels} new channels, {new_programs} new programs")

            except Exception as e:
                error_msg = f"Error processing source {idx}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                source_stats.append({
                    "source_index": idx,
                    "source_url": source_url,
                    "status": "failed",
                    "error": str(e)
                })
                # Continue with next source
                continue
            finally:
                if temp_file and temp_file.exists():
                    try:
                        temp_file.unlink()
                        logger.debug(f"Cleaned up temporary file for source {idx}")
                    except Exception as e:
                        logger.warning(f"Failed to delete temporary file for source {idx}: {e}")

        # Store merged data
        if not all_channels:
            logger.error("No channels found across all sources")
            return {
                "error": "No channels found across all sources",
                "sources": source_stats
            }

        if not all_programs:
            logger.error("No programs found across all sources")
            return {
                "error": "No programs found across all sources",
                "sources": source_stats
            }

        logger.info("="*60)
        logger.info("Storing merged data in database...")
        logger.info(f"Total unique channels: {len(all_channels)}")
        logger.info(f"Total unique programs: {len(all_programs)}")
        logger.info("="*60)

        async with aiosqlite.connect(settings.database_path) as db:
            await _store_channels(db, list(all_channels.values()))
            inserted_count = await _store_programs(db, list(all_programs.values()))
            await db.commit()

        logger.info("="*60)
        logger.info("EPG fetch completed successfully")
        logger.info("="*60)

        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sources_processed": len(settings.epg_sources),
            "sources_succeeded": sum(1 for s in source_stats if s["status"] == "success"),
            "sources_failed": sum(1 for s in source_stats if s["status"] == "failed"),
            "channels": len(all_channels),
            "programs_parsed": len(all_programs),
            "programs_inserted": inserted_count,
            "programs_deleted": deleted_count,
            "source_details": source_stats
        }

    except Exception as e:
        error_msg = f"Error during EPG fetch: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}


async def _download_xmltv(url: str, source_index: int) -> Path:
    """Download XMLTV file from URL"""
    logger.info(f"Downloading XMLTV file...")

    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.get(url)
        response.raise_for_status()

        # Use system temp directory (cross-platform)
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"epg_source_{source_index}.xml"

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
