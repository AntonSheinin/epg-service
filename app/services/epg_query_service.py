"""
EPG Query Service

Business logic for querying and retrieving EPG data from database.
This service handles all read operations for channels and programs.
"""
from datetime import datetime, timezone
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Program
from app.schemas import EPGRequest, EPGResponse, ProgramResponse
from app.utils.timezone import convert_to_timezone, calculate_time_window

logger = logging.getLogger(__name__)


async def get_epg_data(db: AsyncSession, request: EPGRequest) -> EPGResponse:
    """
    Get EPG data for multiple channels

    Args:
        db: Database session
        request: EPG request with channels, from_date, to_date, and timezone

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
    logger.info(f"Received EPG request: {len(request.channels)} channels, timezone={request.timezone}")
    logger.info(f"Date range: from_date={request.from_date}, to_date={request.to_date}")

    epg_data: dict[str, list[ProgramResponse]] = {}
    channels_found_set: set[str] = set()
    total_programs = 0
    now = datetime.now(timezone.utc)

    # Calculate time window once for all channels
    start_time, end_time = calculate_time_window(request)

    # Process each channel request
    for channel in request.channels:

        logger.info(f"Fetching EPG for {channel.xmltv_id}: {start_time.isoformat()} to {end_time.isoformat()}")

        programs = await _query_programs_for_channel(db, channel.xmltv_id, start_time, end_time)

        if programs:
            channels_found_set.add(channel.xmltv_id)
            total_programs += _merge_channel_programs(epg_data, channel.xmltv_id, programs, request.timezone)
        elif channel.xmltv_id not in epg_data:
            epg_data[channel.xmltv_id] = []

    logger.info(f"EPG response: {len(channels_found_set)} channels found, {total_programs} programs, timezone={request.timezone}")

    return EPGResponse(
        timestamp=convert_to_timezone(now.isoformat(), request.timezone),
        timezone=request.timezone,
        channels_requested=len(request.channels),
        channels_found=len(channels_found_set),
        total_programs=total_programs,
        epg=epg_data
    )


async def _query_programs_for_channel(
    db: AsyncSession,
    channel_id: str,
    start_time: datetime,
    end_time: datetime
) -> list[Program]:
    """
    Query programs for a specific channel and time window

    Args:
        db: Database session
        channel_id: Channel XMLTV ID
        start_time: Start of time window
        end_time: End of time window

    Returns:
        List of program rows
    """
    stmt = (
        select(Program)
        .where(
            Program.xmltv_channel_id == channel_id,
            Program.start_time >= start_time.isoformat(),
            Program.start_time < end_time.isoformat()
        )
        .order_by(Program.start_time)
    )

    result = await db.execute(stmt)
    return list(result.scalars().all())


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
        rows: Program ORM objects from database
        timezone_str: Target timezone for conversion

    Returns:
        Number of programs added
    """
    programs_added = 0

    if channel_id in epg_data:
        # Merge with existing programs
        existing_ids = {p.id for p in epg_data[channel_id]}

        for row in rows:
            if row.id not in existing_ids:
                program = ProgramResponse(
                    id=row.id,
                    start_time=convert_to_timezone(row.start_time, timezone_str),
                    stop_time=convert_to_timezone(row.stop_time, timezone_str),
                    title=row.title,
                    description=row.description
                )
                epg_data[channel_id].append(program)
                existing_ids.add(row.id)
                programs_added += 1

        # Re-sort by start_time after merging
        epg_data[channel_id].sort(key=lambda p: p.start_time)
    else:
        # First time seeing this channel - convert all timestamps
        programs = [
            ProgramResponse(
                id=row.id,
                start_time=convert_to_timezone(row.start_time, timezone_str),
                stop_time=convert_to_timezone(row.stop_time, timezone_str),
                title=row.title,
                description=row.description
            )
            for row in rows
        ]
        epg_data[channel_id] = programs
        programs_added = len(programs)

    return programs_added
