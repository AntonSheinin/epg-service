"""
EPG Query Service.

Business logic for querying and retrieving EPG data from database.
This service handles all read operations for channels and programs.
"""
from __future__ import annotations

from datetime import datetime, timezone
import logging

from app.db.repository import SqlAlchemyEpgRepository
from app.schemas import EPGRequest, EPGResponse, ProgramResponse
from app.utils.timezone import convert_to_timezone, calculate_time_window

logger = logging.getLogger(__name__)


async def get_epg_data(repo: SqlAlchemyEpgRepository, request: EPGRequest) -> EPGResponse:
    """
    Get EPG data for multiple channels.
    """
    logger.info(
        "Received EPG request: %s channels, timezone=%s",
        len(request.channels),
        request.timezone,
    )
    logger.info("Date range: from_date=%s, to_date=%s", request.from_date, request.to_date)

    epg_data: dict[str, list[ProgramResponse]] = {}
    channels_found_set: set[str] = set()
    total_programs = 0
    now = datetime.now(timezone.utc)

    start_time, end_time = calculate_time_window(request)

    for channel in request.channels:
        programs = await repo.list_programs_for_channel(channel.xmltv_id, start_time, end_time)

        if programs:
            channels_found_set.add(channel.xmltv_id)
            epg_data[channel.xmltv_id] = [
                ProgramResponse(
                    id=p.id,
                    start_time=convert_to_timezone(p.start_time, request.timezone),
                    stop_time=convert_to_timezone(p.stop_time, request.timezone),
                    title=p.title,
                    description=p.description,
                )
                for p in programs
            ]
            total_programs += len(programs)
        else:
            epg_data[channel.xmltv_id] = []

    logger.info(
        "EPG response: %s channels found, %s programs, timezone=%s",
        len(channels_found_set),
        total_programs,
        request.timezone,
    )

    return EPGResponse(
        timestamp=convert_to_timezone(now, request.timezone),
        timezone=request.timezone,
        channels_requested=len(request.channels),
        channels_found=len(channels_found_set),
        total_programs=total_programs,
        epg=epg_data,
    )
