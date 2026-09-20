"""
EPG Query Service.

Business logic for querying and retrieving EPG data from database.
This service handles all read operations for channels and programs.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
import logging

from app.db.repository import SqlAlchemyEpgRepository
from app.models import LocalizedDescription
from app.schemas import EPGRequest, EPGResponse, ProgramResponse
from app.utils.timezone import convert_to_timezone, calculate_time_window

logger = logging.getLogger(__name__)


def select_description(
    descriptions: Sequence[LocalizedDescription],
    preferred_language: str | None,
) -> str | None:
    """Select one usable description using exact language matching."""
    usable_descriptions = [
        (description.language, description.text.strip())
        for description in descriptions
        if description.text.strip()
    ]

    if preferred_language is not None:
        for language, text in usable_descriptions:
            if language == preferred_language:
                return text

    if usable_descriptions:
        return usable_descriptions[0][1]
    return None


async def get_epg_data(repo: SqlAlchemyEpgRepository, request: EPGRequest) -> EPGResponse:
    """
    Get EPG data for multiple channels.
    """
    logger.info(
        "Received EPG request: %s channels, timezone=%s, preferred_description_language=%s",
        len(request.channels),
        request.timezone,
        request.preferred_description_language,
    )
    logger.info("Date range: from_date=%s, to_date=%s", request.from_date, request.to_date)

    epg_data: dict[str, list[ProgramResponse]] = {}
    channels_found_set: set[str] = set()
    total_programs = 0
    now = datetime.now(timezone.utc)
    last_epg_update_at = await repo.get_last_epg_update_at()

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
                    description=select_description(
                        p.descriptions,
                        request.preferred_description_language,
                    ),
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
        response_generated_at=convert_to_timezone(now, request.timezone),
        last_epg_update_at=(
            convert_to_timezone(last_epg_update_at, request.timezone)
            if last_epg_update_at
            else None
        ),
        timezone=request.timezone,
        channels_requested=len(request.channels),
        channels_found=len(channels_found_set),
        total_programs=total_programs,
        epg=epg_data,
    )
