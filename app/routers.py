from datetime import datetime, timedelta, timezone
from typing import Annotated
from zoneinfo import ZoneInfo
import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query
from app.config import setup_logging
from app.database import get_db
from app.scheduler import epg_scheduler
from app.config import setup_logging
import logging
from app.epg_fetcher import fetch_and_process
from app.schemas import EPGRequest, EPGResponse, ProgramResponse

setup_logging()
logger = logging.getLogger(__name__)


main_router = APIRouter()

def convert_to_timezone(utc_time_str: str, target_tz: str) -> str:
    """
    Convert UTC timestamp to target timezone

    Args:
        utc_time_str: ISO8601 UTC timestamp
        target_tz: Target timezone (IANA format or 'UTC')

    Returns:
        ISO8601 timestamp in target timezone
    """
    dt = datetime.fromisoformat(utc_time_str)

    if target_tz == "UTC":
        return dt.isoformat()

    # Convert to target timezone
    target_zone = ZoneInfo(target_tz)
    dt_target = dt.astimezone(target_zone)

    return dt_target.isoformat()

@main_router.get("/")
async def root() -> dict:
    """Root endpoint with service information"""
    next_run = epg_scheduler.get_next_run_time()

    return {
        "service": "EPG Service",
        "version": "0.1.0",
        "next_scheduled_fetch": next_run.isoformat() if next_run else None,
        "endpoints": {
            "fetch": "/fetch - Manually trigger EPG fetch",
            "channels": "/channels - Get all channels",
            "programs": "/programs - Get programs (query params: start_from, start_to)",
            "epg": "/epg - Get EPG for multiple channels with individual time windows (POST)",
            "health": "/health - Health check"
        }
    }


@main_router.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    next_run = epg_scheduler.get_next_run_time()
    return {
        "status": "ok",
        "scheduler_running": epg_scheduler.scheduler.running,
        "next_fetch": next_run.isoformat() if next_run else None
    }


@main_router.get("/fetch")
async def trigger_fetch() -> dict:
    """
    Manually trigger EPG fetch from source

    This will download, parse and store EPG data
    """
    logger.info("Manual EPG fetch triggered via API")
    result = await fetch_and_process()

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@main_router.get("/channels")
async def get_channels(
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> dict:
    """Get all channels"""
    cursor = await db.execute(
        "SELECT xmltv_id, display_name, icon_url FROM channels ORDER BY display_name"
    )
    rows = list(await cursor.fetchall())  # Convert to list

    logger.debug(f"Retrieved {len(rows)} channels")

    return {
        "count": len(rows),
        "channels": [dict(row) for row in rows]
    }


@main_router.get("/programs")
async def get_programs(
    start_from: Annotated[str, Query(description="ISO8601 datetime, e.g. 2025-10-09T00:00:00Z")],
    start_to: Annotated[str, Query(description="ISO8601 datetime, e.g. 2025-10-10T00:00:00Z")],
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> dict:
    """
    Get all programs in time range

    Args:
        start_from: ISO8601 datetime (e.g. 2025-10-09T00:00:00Z)
        start_to: ISO8601 datetime (e.g. 2025-10-10T00:00:00Z)

    Returns:
        Dictionary with count and list of programs
    """
    logger.debug(f"Fetching programs from {start_from} to {start_to}")

    query = """
        SELECT
            id,
            xmltv_channel_id,
            start_time,
            stop_time,
            title,
            description
        FROM programs
        WHERE start_time >= ? AND start_time < ?
        ORDER BY xmltv_channel_id, start_time
    """

    cursor = await db.execute(query, (start_from, start_to))
    rows = list(await cursor.fetchall())  # Convert to list

    logger.debug(f"Retrieved {len(rows)} programs")

    return {
        "count": len(rows),
        "start_from": start_from,
        "start_to": start_to,
        "programs": [dict(row) for row in rows]
    }

@main_router.post("/epg", response_model=EPGResponse)
async def get_epg(
    request: EPGRequest,
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    Args:
        request: EPG request with channels, update mode, and timezone

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
    # Log the received request body
    logger.info(f"Received EPG request body: {request.model_dump_json()}")
    logger.info(f"EPG request: {len(request.channels)} channels, mode={request.update}, timezone={request.timezone}")

    now = datetime.now(timezone.utc)
    future_limit = now + timedelta(days=7)

    epg_data: dict[str, list[ProgramResponse]] = {}
    channels_found_set: set[str] = set()
    total_programs = 0

    # Process each channel request (may have duplicates)
    for channel in request.channels:
        # Calculate time window based on update mode
        if request.update == "force":
            start_time = now - timedelta(days=channel.epg_depth)
        else:  # delta
            start_time = now

        end_time = future_limit

        logger.debug(
            f"Fetching EPG for {channel.xmltv_id}: "
            f"{start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Query programs for this channel (stored in UTC)
        query = """
            SELECT
                id,
                start_time,
                stop_time,
                title,
                description
            FROM programs
            WHERE xmltv_channel_id = ?
              AND start_time >= ?
              AND start_time < ?
            ORDER BY start_time
        """

        cursor = await db.execute(
            query,
            (channel.xmltv_id, start_time.isoformat(), end_time.isoformat())
        )
        rows = await cursor.fetchall()

        if rows:
            channels_found_set.add(channel.xmltv_id)

            # If this xmltv_id already exists in epg_data, merge the programs
            if channel.xmltv_id in epg_data:
                # Create a set of existing program IDs to avoid duplicates
                existing_ids = {p.id for p in epg_data[channel.xmltv_id]}

                # Add only new programs with timezone conversion
                for row in rows:
                    if row["id"] not in existing_ids:
                        program = ProgramResponse(
                            id=row["id"],
                            start_time=convert_to_timezone(row["start_time"], request.timezone),
                            stop_time=convert_to_timezone(row["stop_time"], request.timezone),
                            title=row["title"],
                            description=row["description"]
                        )
                        epg_data[channel.xmltv_id].append(program)
                        existing_ids.add(row["id"])
                        total_programs += 1

                # Re-sort by start_time after merging
                epg_data[channel.xmltv_id].sort(key=lambda p: p.start_time)
            else:
                # First time seeing this xmltv_id - convert all timestamps
                programs = [
                    ProgramResponse(
                        id=row["id"],
                        start_time=convert_to_timezone(row["start_time"], request.timezone),
                        stop_time=convert_to_timezone(row["stop_time"], request.timezone),
                        title=row["title"],
                        description=row["description"]
                    )
                    for row in rows
                ]
                epg_data[channel.xmltv_id] = programs
                total_programs += len(programs)
        else:
            # Include channel in response even if no programs found
            if channel.xmltv_id not in epg_data:
                epg_data[channel.xmltv_id] = []

    channels_found = len(channels_found_set)

    logger.info(
        f"EPG response: {channels_found} unique channels found, "
        f"{total_programs} total programs, timezone={request.timezone}"
    )

    # Convert response timestamp to requested timezone
    response_timestamp = convert_to_timezone(now.isoformat(), request.timezone)

    return EPGResponse(
        update_mode=request.update,
        timestamp=response_timestamp,
        timezone=request.timezone,
        channels_requested=len(request.channels),
        channels_found=channels_found,
        total_programs=total_programs,
        epg=epg_data
    )
