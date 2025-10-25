"""
EPG Downloader Service

Handles downloading, parsing, and merging EPG data from multiple sources.
Separated from orchestration logic for better testability.
"""
import logging
import asyncio
from datetime import datetime

from app.services.xmltv_parser_service import parse_xmltv_file
from app.utils.file_operations import download_file, cleanup_temp_file
from app.utils.data_merging import merge_channels, merge_programs, ChannelTuple, ProgramDict


logger = logging.getLogger(__name__)


async def process_single_source(
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
        # Download with retry logic
        temp_file = await download_file(source_url, f"epg_source_{source_index}.xml")

        # Parse
        channels, programs = await parse_xmltv_async(temp_file, time_from, time_to)

        return channels, programs

    finally:
        if temp_file:
            cleanup_temp_file(temp_file)


async def parse_xmltv_async(
    file_path,
    time_from: datetime,
    time_to: datetime
) -> tuple[list[ChannelTuple], list[ProgramDict]]:
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


def merge_source_data(
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
