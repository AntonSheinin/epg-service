"""
EPG Query Service

Business logic for querying and retrieving EPG data from database.
This service handles all read operations for channels and programs.
"""
from datetime import datetime, timedelta, timezone
import aiosqlite
import logging

from app.schemas import EPGRequest, EPGResponse, ProgramResponse
from app.utils.timezone import convert_to_timezone


logger = logging.getLogger(__name__)


async def get_all_channels(db: aiosqlite.Connection) -> dict:
    """
    Retrieve all channels from database

    Args:
        db: Database connection

    Returns:
        Dictionary with count and list of channels
    """
    cursor = await db.execute(
        "SELECT xmltv_id, display_name, icon_url FROM channels ORDER BY display_name"
    )
    rows = list(await cursor.fetchall())

    logger.debug(f"Retrieved {len(rows)} channels")

    return {
        "count": len(rows),
        "channels": [dict(row) for row in rows]
    }


async def get_programs_in_range(
    db: aiosqlite.Connection,
    start_from: str,
    start_to: str
) -> dict:
    """
    Get all programs within a time range

    Args:
        db: Database connection
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
    rows = list(await cursor.fetchall())

    logger.debug(f"Retrieved {len(rows)} programs")

    return {
        "count": len(rows),
        "start_from": start_from,
        "start_to": start_to,
        "programs": [dict(row) for row in rows]
    }


async def get_epg_data(
    db: aiosqlite.Connection,
    request: EPGRequest
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    This is the main business logic for retrieving EPG data based on
    channel requests, update mode, and timezone preferences.

    Args:
        db: Database connection
        request: EPG request with channels, update mode, and timezone

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
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
        programs_for_channel = await _query_programs_for_channel(
            db,
            channel.xmltv_id,
            start_time,
            end_time
        )

        if programs_for_channel:
            channels_found_set.add(channel.xmltv_id)
            total_programs += _merge_channel_programs(
                epg_data,
                channel.xmltv_id,
                programs_for_channel,
                request.timezone
            )
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


async def _query_programs_for_channel(
    db: aiosqlite.Connection,
    channel_id: str,
    start_time: datetime,
    end_time: datetime
) -> list:
    """
    Query programs for a specific channel and time window

    Args:
        db: Database connection
        channel_id: Channel XMLTV ID
        start_time: Start of time window
        end_time: End of time window

    Returns:
        List of program rows
    """
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
        (channel_id, start_time.isoformat(), end_time.isoformat())
    )
    return await cursor.fetchall()


def _merge_channel_programs(
    epg_data: dict[str, list[ProgramResponse]],
    channel_id: str,
    rows: list,
    timezone_str: str
) -> int:
    """
    Merge programs for a channel into the EPG data structure

    Args:
        epg_data: Existing EPG data dictionary (modified in place)
        channel_id: Channel XMLTV ID
        rows: Program rows from database
        timezone_str: Target timezone for conversion

    Returns:
        Number of programs added
    """
    programs_added = 0

    if channel_id in epg_data:
        # Merge with existing programs
        existing_ids = {p.id for p in epg_data[channel_id]}

        for row in rows:
            if row["id"] not in existing_ids:
                program = ProgramResponse(
                    id=row["id"],
                    start_time=convert_to_timezone(row["start_time"], timezone_str),
                    stop_time=convert_to_timezone(row["stop_time"], timezone_str),
                    title=row["title"],
                    description=row["description"]
                )
                epg_data[channel_id].append(program)
                existing_ids.add(row["id"])
                programs_added += 1

        # Re-sort by start_time after merging
        epg_data[channel_id].sort(key=lambda p: p.start_time)
    else:
        # First time seeing this channel - convert all timestamps
        programs = [
            ProgramResponse(
                id=row["id"],
                start_time=convert_to_timezone(row["start_time"], timezone_str),
                stop_time=convert_to_timezone(row["stop_time"], timezone_str),
                title=row["title"],
                description=row["description"]
            )
            for row in rows
        ]
        epg_data[channel_id] = programs
        programs_added = len(programs)

    return programs_added
