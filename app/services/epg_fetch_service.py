"""
EPG Fetching orchestration

This module coordinates the EPG fetching process from multiple sources.
"""
import logging
import asyncio
import aiosqlite
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.services.xmltv_parser_service import parse_xmltv_file
from app.utils.file_operations import download_file, cleanup_temp_file
from app.utils.data_merging import merge_channels, merge_programs, ChannelTuple, ProgramDict
from app.services.db_service import delete_old_programs, store_channels, store_programs


logger = logging.getLogger("epg_service.fetcher")

# Global lock to prevent concurrent fetch operations
_fetch_lock = asyncio.Lock()

async def fetch_and_process() -> dict:
    """
    Main entry point for EPG fetching with concurrency protection

    Returns:
        Dictionary with fetch statistics or error/skip message
    """
    # Try to acquire lock without blocking
    if _fetch_lock.locked():
        logger.warning("EPG fetch already in progress, skipping this request")
        return {
            "status": "skipped",
            "message": "EPG fetch operation already in progress"
        }

    async with _fetch_lock:
        logger.info("="*60)
        logger.info(f"Starting EPG fetch at {datetime.now(timezone.utc).isoformat()}")

        if not settings.epg_sources:
            logger.error("EPG_SOURCES not configured")
            return {"error": "EPG_SOURCES not configured"}

        return await _orchestrate_fetch()


async def _orchestrate_fetch() -> dict:
    """
    Orchestrate the complete fetch process

    Returns:
        Dictionary with fetch statistics or error
    """
    # Type guard - already checked in fetch_and_process
    if not settings.epg_sources:
        return {"error": "EPG_SOURCES not configured"}

    logger.info(f"Processing {len(settings.epg_sources)} source(s)")
    logger.info("="*60)

    try:
        # Calculate time boundaries
        boundaries = _calculate_time_boundaries()

        # Clean up old data
        deleted_count = await _cleanup_old_data(boundaries['archive_boundary'])

        # Process all sources
        all_channels, all_programs, source_stats = await _process_all_sources(boundaries)

        # Validate results
        if not all_channels or not all_programs:
            return _create_error_response(all_channels, all_programs, source_stats)

        # Store results
        inserted_count = await _store_results(all_channels, all_programs)

        logger.info("="*60)
        logger.info("EPG fetch completed successfully")
        logger.info("="*60)

        return _create_success_response(
            all_channels,
            all_programs,
            source_stats,
            inserted_count,
            deleted_count
        )

    except Exception as e:
        error_msg = f"Error during EPG fetch: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"error": error_msg}


def _calculate_time_boundaries() -> dict:
    """
    Calculate time boundaries for fetching and archiving

    Returns:
        Dictionary with time boundary datetimes
    """
    now = datetime.now(timezone.utc)
    archive_boundary = now - timedelta(days=settings.max_epg_depth)
    fetch_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    fetch_end = now + timedelta(days=365)

    logger.info(f"Archive boundary: {archive_boundary.date()} (keeping programs from this date)")
    logger.info(f"Fetch boundary: {fetch_start.date()} 00:00 UTC (only accepting programs from today)")
    logger.info(f"Max EPG depth: {settings.max_epg_depth} days")

    return {
        'now': now,
        'archive_boundary': archive_boundary,
        'fetch_start': fetch_start,
        'fetch_end': fetch_end
    }


async def _cleanup_old_data(archive_boundary: datetime) -> int:
    """
    Delete old programs from database

    Args:
        archive_boundary: Delete programs before this time

    Returns:
        Number of deleted programs
    """
    async with aiosqlite.connect(settings.database_path) as db:
        deleted_count = await delete_old_programs(db, archive_boundary)
        await db.commit()
    return deleted_count


async def _process_all_sources(boundaries: dict) -> tuple[dict[str, ChannelTuple], dict[str, ProgramDict], list[dict]]:
    """
    Process all EPG sources and merge their data

    Args:
        boundaries: Time boundary configuration

    Returns:
        Tuple of (merged_channels, merged_programs, source_statistics)
    """
    all_channels: dict[str, ChannelTuple] = {}
    all_programs: dict[str, ProgramDict] = {}
    source_stats = []
    epg_sources = settings.epg_sources or []

    for idx, source_url in enumerate(epg_sources, 1):
        _log_source_header(idx, len(epg_sources), source_url)

        try:
            channels, programs = await _process_single_source(
                source_url,
                idx,
                boundaries['fetch_start'],
                boundaries['fetch_end']
            )

            new_channels_count, new_programs_count = _merge_source_data(
                all_channels,
                all_programs,
                channels,
                programs
            )

            source_stats.append(_create_source_stat_success(
                idx,
                source_url,
                len(channels),
                new_channels_count,
                len(programs),
                new_programs_count
            ))

            logger.info(f"Source {idx}: {new_channels_count} new channels, {new_programs_count} new programs")

        except Exception as e:
            error_msg = f"Error processing source {idx}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            source_stats.append(_create_source_stat_failure(idx, source_url, str(e)))

    return all_channels, all_programs, source_stats


async def _process_single_source(
    source_url: str,
    source_index: int,
    time_from: datetime,
    time_to: datetime
) -> tuple[list[ChannelTuple], list[ProgramDict]]:
    """
    Download and parse a single EPG source

    Args:
        source_url: URL to download from
        source_index: Index of this source (for file naming)
        time_from: Start of time window
        time_to: End of time window

    Returns:
        Tuple of (channels, programs)
    """
    temp_file = None
    try:
        # Download
        temp_file = await download_file(source_url, f"epg_source_{source_index}.xml")

        # Parse
        channels, programs = await _parse_xmltv_async(temp_file, time_from, time_to)

        return channels, programs

    finally:
        if temp_file:
            cleanup_temp_file(temp_file)


async def _parse_xmltv_async(file_path, time_from: datetime, time_to: datetime) -> tuple[list[ChannelTuple], list[ProgramDict]]:
    """
    Parse XMLTV file asynchronously (offloaded to thread pool)

    Args:
        file_path: Path to XMLTV file
        time_from: Start of time window
        time_to: End of time window

    Returns:
        Tuple of (channels, programs)

    Raises:
        ValueError: If no channels or programs found
    """
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


def _merge_source_data(
    all_channels: dict[str, ChannelTuple],
    all_programs: dict[str, ProgramDict],
    new_channels: list[ChannelTuple],
    new_programs: list[ProgramDict]
) -> tuple[int, int]:
    """
    Merge data from a single source into aggregate collections

    Args:
        all_channels: Existing merged channels (modified in-place)
        all_programs: Existing merged programs (modified in-place)
        new_channels: New channels from source
        new_programs: New programs from source

    Returns:
        Tuple of (new_channels_count, new_programs_count)
    """
    _, new_channels_count = merge_channels(all_channels, new_channels)
    _, new_programs_count = merge_programs(all_programs, new_programs)
    return new_channels_count, new_programs_count


async def _store_results(
    all_channels: dict[str, ChannelTuple],
    all_programs: dict[str, ProgramDict]
) -> int:
    """
    Store merged channels and programs to database

    Args:
        all_channels: Merged channels dictionary
        all_programs: Merged programs dictionary

    Returns:
        Count of inserted programs
    """
    logger.info("="*60)
    logger.info("Storing merged data in database...")
    logger.info(f"Total unique channels: {len(all_channels)}")
    logger.info(f"Total unique programs: {len(all_programs)}")
    logger.info("="*60)

    async with aiosqlite.connect(settings.database_path) as db:
        await store_channels(db, list(all_channels.values()))
        inserted_count = await store_programs(db, list(all_programs.values()))
        await db.commit()

    return inserted_count


# Helper functions for logging and response creation

def _log_source_header(idx: int, total: int, url: str) -> None:
    """
    Log header for source processing

    Args:
        idx: Current source index (1-based)
        total: Total number of sources
        url: Source URL being processed
    """
    logger.info("="*60)
    logger.info(f"Processing source {idx}/{total}")
    logger.info(f"URL: {url}")
    logger.info("="*60)


def _create_source_stat_success(
    idx: int,
    url: str,
    channels_parsed: int,
    channels_new: int,
    programs_parsed: int,
    programs_new: int
) -> dict:
    """
    Create success statistics for a source

    Args:
        idx: Source index
        url: Source URL
        channels_parsed: Total channels found in this source
        channels_new: New channels added from this source
        programs_parsed: Total programs found in this source
        programs_new: New programs added from this source

    Returns:
        Dictionary with success statistics
    """
    return {
        "source_index": idx,
        "source_url": url,
        "status": "success",
        "channels_parsed": channels_parsed,
        "channels_new": channels_new,
        "programs_parsed": programs_parsed,
        "programs_new": programs_new
    }


def _create_source_stat_failure(idx: int, url: str, error: str) -> dict:
    """
    Create failure statistics for a source

    Args:
        idx: Source index
        url: Source URL
        error: Error message

    Returns:
        Dictionary with failure statistics
    """
    return {
        "source_index": idx,
        "source_url": url,
        "status": "failed",
        "error": error
    }


def _create_error_response(
    all_channels: dict,
    all_programs: dict,  # noqa: ARG001
    source_stats: list[dict]
) -> dict:
    """
    Create error response when no data found

    Args:
        all_channels: Merged channels dictionary
        all_programs: Merged programs dictionary (unused, kept for consistency)
        source_stats: Statistics from all sources

    Returns:
        Dictionary with error message and source details
    """
    if not all_channels:
        logger.error("No channels found across all sources")
        return {
            "error": "No channels found across all sources",
            "sources": source_stats
        }

    # If we get here, it means no programs found (all_programs is empty)
    logger.error("No programs found across all sources")
    return {
        "error": "No programs found across all sources",
        "sources": source_stats
    }


def _create_success_response(
    all_channels: dict,
    all_programs: dict,
    source_stats: list[dict],
    inserted_count: int,
    deleted_count: int
) -> dict:
    """
    Create success response with statistics

    Args:
        all_channels: Merged channels dictionary
        all_programs: Merged programs dictionary
        source_stats: Statistics from all sources
        inserted_count: Number of programs inserted into database
        deleted_count: Number of old programs deleted

    Returns:
        Dictionary with success status and detailed statistics
    """
    epg_sources = settings.epg_sources or []
    return {
        "status": "success",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sources_processed": len(epg_sources),
        "sources_succeeded": sum(1 for s in source_stats if s["status"] == "success"),
        "sources_failed": sum(1 for s in source_stats if s["status"] == "failed"),
        "channels": len(all_channels),
        "programs_parsed": len(all_programs),
        "programs_inserted": inserted_count,
        "programs_deleted": deleted_count,
        "source_details": source_stats
    }
