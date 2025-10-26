"""
EPG Downloader Service

Handles downloading and parsing EPG data from multiple sources.
Separated from orchestration logic for better testability.
"""
import logging
import asyncio
from datetime import datetime
from pathlib import Path

from app.services.xmltv_parser_service import parse_xmltv_file
from app.utils.file_operations import download_file, cleanup_temp_file
from app.services.fetch_types import ChannelPayload, ProgramPayload


logger = logging.getLogger(__name__)


async def process_single_source(
    source_url: str,
    source_index: int,
    time_from: datetime,
    time_to: datetime,
    parse_timeout_seconds: int | None = None
) -> tuple[list[ChannelPayload], list[ProgramPayload]]:
    """
    Download and parse a single EPG source

    Args:
        source_url: URL to download from
        source_index: Index of this source (for file naming)
        time_from: Start of time window
        time_to: End of time window

    Returns:
        Tuple of (channels, programs)

    Keyword Args:
        parse_timeout_seconds: Timeout in seconds for parsing (0/None disables timeout)
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
        channels, programs = await parse_xmltv_async(
            temp_file,
            time_from,
            time_to,
            parse_timeout_seconds=parse_timeout_seconds
        )
        logger.info(f"  [Source {source_index}] Parsing complete: {len(channels)} channels, {len(programs)} programs")
        logger.debug(f"  [Source {source_index}] Channel IDs (first 5): {[ch.xmltv_id for ch in channels[:5]]}")

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
    time_to: datetime,
    *,
        parse_timeout_seconds: int | None = None
) -> tuple[list[ChannelPayload], list[ProgramPayload]]:
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

    Keyword Args:
        parse_timeout_seconds: Timeout in seconds for parsing (0/None disables timeout)

    Raises:
        ValueError: If no channels/programs found or parsing times out
        asyncio.TimeoutError: If parsing exceeds timeout
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    logger.info(f"Parsing XMLTV file: {file_path}")
    logger.debug(f"  File size: {file_path.stat().st_size / 1024 / 1024:.2f} MB")
    logger.debug(f"  Time window filter: {time_from.isoformat()} to {time_to.isoformat()}")

    effective_timeout = parse_timeout_seconds if parse_timeout_seconds and parse_timeout_seconds > 0 else None

    try:
        loop = asyncio.get_running_loop()
        timeout_display = f"{effective_timeout}s" if effective_timeout else "disabled"
        logger.debug("Offloading XML parsing to thread pool executor (timeout: %s)...", timeout_display)
        parse_task = loop.run_in_executor(
            None,
            parse_xmltv_file,
            str(file_path),
            time_from,
            time_to
        )
        if effective_timeout:
            channels, programs = await asyncio.wait_for(
                parse_task,
                timeout=effective_timeout
            )
        else:
            channels, programs = await parse_task
        logger.info(f"XML parsing completed successfully: {len(channels)} channels, {len(programs)} programs")
        logger.debug(
            "  Parsed channels: %s%s",
            ", ".join([ch.xmltv_id for ch in channels[:3]]),
            "..." if len(channels) > 3 else "",
        )
    except asyncio.TimeoutError:
        logger.error(
            "XML parsing timed out after %s for %s",
            timeout_display,
            file_path
        )
        raise ValueError("XML parsing timed out - file may be too large or malformed")

    if not channels:
        logger.warning("No channels found in XMLTV file")
        raise ValueError("No channels found in XMLTV")

    if not programs:
        logger.warning("No programs found in XMLTV file (possibly outside time window)")
        raise ValueError("No programs found in XMLTV")

    logger.info(f"XMLTV parsing validation passed: {len(channels)} channels, {len(programs)} programs")

    return channels, programs
