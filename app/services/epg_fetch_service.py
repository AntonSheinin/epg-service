"""
EPG Fetching Service

Coordinates downloading, parsing, merging, and storage of EPG data from multiple sources.
"""
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.database import _get_session_factory
from app.utils.data_merging import ChannelTuple, ProgramDict
from app.services.db_service import delete_old_programs, store_channels, store_programs
from app.services.epg_downloader_service import process_single_source, merge_source_data


logger = logging.getLogger(__name__)

# Global lock to prevent concurrent fetch operations
_fetch_lock = asyncio.Lock()


def _sanitize_url_for_logging(url: str) -> str:
    """Remove credentials from URL for safe logging"""
    if '://' not in url:
        return url
    try:
        protocol, rest = url.split('://', 1)
        if '@' in rest:
            # URL contains credentials, truncate them
            rest = rest.split('@')[1]
            return f"{protocol}://***:***@{rest}"
        return url
    except (ValueError, IndexError):
        return url

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
        fetch_end = now + timedelta(days=settings.max_future_epg_limit)

        logger.info(f"Time boundaries - Archive: {archive_boundary.date()}, Fetch: {fetch_start.date()} to {fetch_end.date()}")
        logger.info(f"Configuration - Max depth: {settings.max_epg_depth} days, Max future: {settings.max_future_epg_limit} days")

        # Clean up old data
        logger.info(f"Deleting programs before {archive_boundary.date()}...")
        session_factory = _get_session_factory()

        async with session_factory() as db:
            deleted_count = await delete_old_programs(db, archive_boundary)
            logger.info(f"Deleted {deleted_count} old programs")

        # Process all sources
        all_channels: dict[str, ChannelTuple] = {}
        all_programs: dict[str, ProgramDict] = {}
        source_stats = []

        epg_sources = settings.epg_sources or []
        logger.info(f"Starting to process {len(epg_sources)} EPG source(s)")

        for idx, source_url in enumerate(epg_sources, 1):
            sanitized_url = _sanitize_url_for_logging(source_url)
            logger.info(f"[Source {idx}/{len(epg_sources)}] Starting download: {sanitized_url}")

            try:
                logger.debug(f"[Source {idx}] Downloading and parsing XMLTV from {sanitized_url}")
                channels, programs = await process_single_source(
                    source_url, idx, fetch_start, fetch_end
                )

                logger.debug(f"[Source {idx}] Downloaded content contains {len(channels)} channels and {len(programs)} programs")

                logger.info(f"[Source {idx}] Merging data into aggregate collections...")
                new_channels_count, new_programs_count = merge_source_data(
                    all_channels, all_programs, channels, programs
                )

                logger.info(f"[Source {idx}] Successfully merged: {new_channels_count} new channels, {new_programs_count} new programs")
                logger.info(f"[Source {idx}] Total merged so far: {len(all_channels)} channels, {len(all_programs)} programs")

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
                logger.error(f"[Source {idx}] Failed to process: {e}", exc_info=True)
                source_stats.append({
                    "source_index": idx,
                    "source_url": source_url,
                    "status": "failed",
                    "error": str(e)
                })

        # Validate results
        logger.info(f"Processing complete. Aggregated data: {len(all_channels)} channels, {len(all_programs)} programs")

        if not all_channels or not all_programs:
            error_msg = f"{'No channels' if not all_channels else 'No programs'} found across all sources"
            logger.error(error_msg)
            return {
                "error": error_msg,
                "sources": source_stats
            }

        # Store results to database
        logger.info(f"Starting database transaction for storing {len(all_channels)} channels and {len(all_programs)} programs...")

        async with session_factory() as db:
            async with db.begin():
                logger.info(f"Storing channels...")
                await store_channels(db, list(all_channels.values()))
                logger.debug(f"Stored {len(all_channels)} channels")

                logger.info(f"Storing programs...")
                inserted_count = await store_programs(db, list(all_programs.values()))
                logger.debug(f"Inserted {inserted_count} programs")

        logger.info(f"Database transaction committed successfully")
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
