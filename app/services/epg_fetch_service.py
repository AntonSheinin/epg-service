"""
EPG Fetching Service

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


logger = logging.getLogger(__name__)

# Global lock to prevent concurrent fetch operations
_fetch_lock = asyncio.Lock()


async def fetch_and_process() -> dict:
    """
    Main entry point for EPG fetching with concurrency protection.

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
        logger.info(f"EPG fetch started at {datetime.now(timezone.utc).isoformat()}")

        # Validate configuration
        if not settings.epg_sources:
            return {"error": "EPG_SOURCES not configured"}

        if not settings.database_path:
            return {"error": "DATABASE_PATH not configured"}

        return await _do_fetch()


async def _do_fetch() -> dict:
    """
    Execute the complete EPG fetch and storage workflow.

    Returns:
        Dictionary with fetch results and statistics
    """
    try:
        # Calculate time boundaries
        now = datetime.now(timezone.utc)
        archive_boundary = now - timedelta(days=settings.max_epg_depth)
        fetch_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        fetch_end = now + timedelta(days=365)

        logger.info(f"Archive boundary: {archive_boundary.date()}")
        logger.info(f"Fetch boundary: {fetch_start.date()} 00:00 UTC")
        logger.info(f"Max EPG depth: {settings.max_epg_depth} days")

        # Clean up old data
        session_factory = _create_session_factory()
        if session_factory is None:
            return {"error": "Database session factory not initialized"}

        async with session_factory() as db:
            deleted_count = await delete_old_programs(db, archive_boundary)

        # Process all sources
        all_channels: dict[str, ChannelTuple] = {}
        all_programs: dict[str, ProgramDict] = {}
        source_stats = []

        epg_sources = settings.epg_sources or []
        logger.info(f"Processing {len(epg_sources)} source(s)")

        for idx, source_url in enumerate(epg_sources, 1):
            logger.info(f"Processing source {idx}/{len(epg_sources)}: {source_url}")

            try:
                channels, programs = await process_single_source(
                    source_url, idx, fetch_start, fetch_end
                )

                new_channels_count, new_programs_count = merge_source_data(
                    all_channels, all_programs, channels, programs
                )

                logger.info(f"Source {idx}: {new_channels_count} new channels, {new_programs_count} new programs")

                source_stats.append({
                    "source_index": idx,
                    "source_url": source_url,
                    "status": "success",
                    "channels_parsed": len(channels),
                    "channels_new": new_channels_count,
                    "programs_parsed": len(programs),
                    "programs_new": new_programs_count
                })

            except Exception as e:
                logger.error(f"Error processing source {idx}: {e}", exc_info=True)
                source_stats.append({
                    "source_index": idx,
                    "source_url": source_url,
                    "status": "failed",
                    "error": str(e)
                })

        # Validate results
        if not all_channels or not all_programs:
            if not all_channels:
                logger.error("No channels found across all sources")
            else:
                logger.error("No programs found across all sources")
            return {
                "error": f"{'No channels' if not all_channels else 'No programs'} found across all sources",
                "sources": source_stats
            }

        # Store results
        logger.info(f"Storing: {len(all_channels)} channels, {len(all_programs)} programs")

        async with session_factory() as db:
            await store_channels(db, list(all_channels.values()))
            inserted_count = await store_programs(db, list(all_programs.values()))

        logger.info(f"EPG fetch completed at {datetime.now(timezone.utc).isoformat()}")

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

    except Exception as e:
        logger.error(f"Error during EPG fetch: {e}", exc_info=True)
        return {"error": str(e)}
