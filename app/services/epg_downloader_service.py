"""
EPG Downloader Service

Handles downloading, parsing, and merging EPG data from multiple sources.
Separated from orchestration logic for better testability.
"""
import logging
import asyncio
from datetime import datetime
from pathlib import Path

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
        logger.info(f"  [Source {source_index}] Starting download (time window: {time_from.date()} to {time_to.date()})...")
        logger.debug(f"  [Source {source_index}] Download URL: {source_url}")
        temp_file = await download_file(source_url, f"epg_source_{source_index}.xml")
        logger.info(f"  [Source {source_index}] Download successful, file saved to {temp_file}")
        logger.debug(f"  [Source {source_index}] File size: {temp_file.stat().st_size / 1024 / 1024:.2f} MB")

        # Parse
        logger.info(f"  [Source {source_index}] Parsing XMLTV content...")
        logger.debug(f"  [Source {source_index}] Starting async parsing with timeout protection...")
        channels, programs = await parse_xmltv_async(temp_file, time_from, time_to)
        logger.info(f"  [Source {source_index}] Parsing complete: {len(channels)} channels, {len(programs)} programs")
        logger.debug(f"  [Source {source_index}] Channel IDs (first 5): {[ch[0] for ch in channels[:5]]}")

        return channels, programs

    finally:
        if temp_file:
            logger.debug(f"  [Source {source_index}] Cleaning up temporary file...")
            if cleanup_temp_file(temp_file):
                logger.debug(f"  [Source {source_index}] Cleanup successful")
            else:
                logger.debug(f"  [Source {source_index}] Cleanup skipped (file not found)")


async def parse_xmltv_async(
    file_path: Path | str,
    time_from: datetime,
    time_to: datetime
) -> tuple[list[ChannelTuple], list[ProgramDict]]:
    """
    Parse XMLTV file asynchronously with timeout protection.

    File parsing is offloaded to thread pool to avoid blocking event loop.
    A 5-minute timeout prevents malformed or massive files from hanging.

    Args:
        file_path: Path to XMLTV file (Path or str)
        time_from: Start of time window
        time_to: End of time window

    Returns:
        Tuple of (channels, programs)

    Raises:
        ValueError: If no channels/programs found or parsing times out
        asyncio.TimeoutError: If parsing exceeds timeout
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    logger.info(f"Parsing XMLTV file: {file_path}")
    logger.debug(f"  File size: {file_path.stat().st_size / 1024 / 1024:.2f} MB")
    logger.debug(f"  Time window filter: {time_from.isoformat()} to {time_to.isoformat()}")

    try:
        loop = asyncio.get_event_loop()
        logger.debug("Offloading XML parsing to thread pool executor (timeout: 5 minutes)...")
        channels, programs = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                parse_xmltv_file,
                str(file_path),
                time_from,
                time_to
            ),
            timeout=300  # 5 minutes max for XML parsing
        )
        logger.info(f"XML parsing completed successfully: {len(channels)} channels, {len(programs)} programs")
        logger.debug(f"  Parsed channels: {', '.join([ch[0] for ch in channels[:3]])}{'...' if len(channels) > 3 else ''}")
    except asyncio.TimeoutError:
        logger.error(f"XML parsing timed out after 5 minutes for {file_path}")
        raise ValueError("XML parsing timed out - file may be too large or malformed")

    if not channels:
        logger.warning("No channels found in XMLTV file")
        raise ValueError("No channels found in XMLTV")

    if not programs:
        logger.warning("No programs found in XMLTV file (possibly outside time window)")
        raise ValueError("No programs found in XMLTV")

    logger.info(f"XMLTV parsing validation passed: {len(channels)} channels, {len(programs)} programs")

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
    logger.info(f"Starting merge operation...")
    logger.debug(f"  Channels before: {len(all_channels)}, Programs before: {len(all_programs)}")
    logger.debug(f"  New channels to merge: {len(new_channels)}, New programs to merge: {len(new_programs)}")

    logger.info(f"Merging {len(new_channels)} channels into aggregate...")
    _, new_channels_count = merge_channels(all_channels, new_channels)
    logger.info(f"  Channel merge complete: {new_channels_count} new/updated, total now: {len(all_channels)}")

    logger.info(f"Merging {len(new_programs)} programs into aggregate...")
    _, new_programs_count = merge_programs(all_programs, new_programs)
    logger.info(f"  Program merge complete: {new_programs_count} new/updated, total now: {len(all_programs)}")

    return new_channels_count, new_programs_count
