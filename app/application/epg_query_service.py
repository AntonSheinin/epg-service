"""
EPG Query Service.

Business logic for querying and retrieving EPG data from database.
This service handles all read operations for channels and programs.
"""
from __future__ import annotations

from datetime import datetime, timezone
import logging

from app.domain.entities import Program
from app.domain.repositories import EpgRepository
from app.schemas import EPGRequest, EPGResponse, ProgramResponse
from app.utils.timezone import convert_to_timezone, calculate_time_window

logger = logging.getLogger(__name__)


async def get_epg_data(repo: EpgRepository, request: EPGRequest) -> EPGResponse:
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
            total_programs += _merge_channel_programs(
                epg_data,
                channel.xmltv_id,
                programs,
                request.timezone,
            )
        elif channel.xmltv_id not in epg_data:
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


def _merge_channel_programs(
    epg_data: dict[str, list[ProgramResponse]],
    channel_id: str,
    rows: list[Program],
    timezone_str: str,
) -> int:
    """
    Merge programs for a channel into the EPG data structure.
    """
    programs_added = 0

    if channel_id in epg_data:
        existing_ids = {p.id for p in epg_data[channel_id]}

        for row in rows:
            if row.id not in existing_ids:
                program = ProgramResponse(
                    id=row.id,
                    start_time=convert_to_timezone(row.start_time, timezone_str),
                    stop_time=convert_to_timezone(row.stop_time, timezone_str),
                    title=row.title,
                    description=row.description,
                )
                epg_data[channel_id].append(program)
                existing_ids.add(row.id)
                programs_added += 1

        epg_data[channel_id].sort(key=lambda p: p.start_time)
    else:
        programs = [
            ProgramResponse(
                id=row.id,
                start_time=convert_to_timezone(row.start_time, timezone_str),
                stop_time=convert_to_timezone(row.stop_time, timezone_str),
                title=row.title,
                description=row.description,
            )
            for row in rows
        ]
        epg_data[channel_id] = programs
        programs_added = len(programs)

    return programs_added


__all__ = ["get_epg_data"]
