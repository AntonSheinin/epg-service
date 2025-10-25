"""
EPG Fetching Orchestration

Main orchestration service for EPG fetching process.
Coordinates downloading, parsing, merging, and storage of EPG data from multiple sources.
"""
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.database import _create_session_factory
from app.utils.data_merging import ChannelTuple, ProgramDict
from app.services.db_service import delete_old_programs, store_channels, store_programs
from app.services.epg_downloader_service import (
    process_single_source,
    merge_source_data,
)


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

        # Validate configuration
        if not settings.epg_sources:
            error_msg = "EPG_SOURCES not configured"
            logger.error(error_msg)
            return {"error": error_msg}

        if not settings.database_path:
            error_msg = "DATABASE_PATH not configured"
            logger.error(error_msg)
            return {"error": error_msg}

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

    Raises:
        RuntimeError: If database session cannot be created
    """
    try:
        session_factory = _create_session_factory()
        if session_factory is None:
            raise RuntimeError("Database session factory not initialized")

        async with session_factory() as db:
            deleted_count = await delete_old_programs(db, archive_boundary)
        return deleted_count
    except Exception as e:
        logger.error(f"Error cleaning up old data: {e}", exc_info=True)
        raise


async def _process_all_sources(
    boundaries: dict
) -> tuple[dict[str, ChannelTuple], dict[str, ProgramDict], list[dict]]:
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
            channels, programs = await process_single_source(
                source_url,
                idx,
                boundaries['fetch_start'],
                boundaries['fetch_end']
            )

            new_channels_count, new_programs_count = merge_source_data(
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

    Raises:
        RuntimeError: If database session cannot be created
    """
    logger.info("="*60)
    logger.info("Storing merged data in database...")
    logger.info(f"Total unique channels: {len(all_channels)}")
    logger.info(f"Total unique programs: {len(all_programs)}")
    logger.info("="*60)

    try:
        session_factory = _create_session_factory()
        if session_factory is None:
            raise RuntimeError("Database session factory not initialized")

        async with session_factory() as db:
            await store_channels(db, list(all_channels.values()))
            inserted_count = await store_programs(db, list(all_programs.values()))

        return inserted_count
    except Exception as e:
        logger.error(f"Error storing results: {e}", exc_info=True)
        raise


# ============================================================================
# Helper functions for logging and response creation
# ============================================================================

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
