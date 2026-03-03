"""
EPG Downloader Service.

Handles downloading and parsing EPG data from multiple sources.
Separated from orchestration logic for better testability.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from pathlib import Path

from app.models import Channel, Program
from app.services.xmltv_parser import parse_xmltv_file
from app.utils.file_operations import cleanup_temp_file, download_file

logger = logging.getLogger(__name__)


async def process_single_source(
    source_url: str,
    source_index: int,
    time_from: datetime,
    time_to: datetime,
    parse_timeout_seconds: int | None = None,
) -> tuple[list[Channel], list[Program]]:
    """
    Download and parse a single EPG source.
    """
    temp_file = None
    try:
        logger.info(
            "  [Source %s] Starting download (time window: %s to %s)...",
            source_index,
            time_from.date(),
            time_to.date(),
        )
        logger.debug("  [Source %s] Download URL: %s", source_index, source_url)
        temp_file = await download_file(source_url, f"epg_source_{source_index}.xml")
        logger.info("  [Source %s] Download successful, file saved to %s", source_index, temp_file)
        logger.debug(
            "  [Source %s] File size: %.2f MB",
            source_index,
            temp_file.stat().st_size / 1024 / 1024,
        )

        logger.info("  [Source %s] Parsing XMLTV content...", source_index)
        logger.debug("  [Source %s] Starting async parsing with timeout protection...", source_index)
        channels, programs = await parse_xmltv_async(
            temp_file,
            time_from,
            time_to,
            parse_timeout_seconds=parse_timeout_seconds,
        )
        logger.info(
            "  [Source %s] Parsing complete: %s channels, %s programs",
            source_index,
            len(channels),
            len(programs),
        )
        logger.debug(
            "  [Source %s] Channel IDs (first 5): %s",
            source_index,
            [ch.xmltv_id for ch in channels[:5]],
        )

        return channels, programs
    finally:
        if temp_file:
            logger.debug("  [Source %s] Cleaning up temporary file...", source_index)
            if cleanup_temp_file(temp_file):
                logger.debug("  [Source %s] Cleanup successful", source_index)
            else:
                logger.debug("  [Source %s] Cleanup skipped (file not found)", source_index)


async def parse_xmltv_async(
    file_path: Path | str,
    time_from: datetime,
    time_to: datetime,
    *,
    parse_timeout_seconds: int | None = None,
) -> tuple[list[Channel], list[Program]]:
    """
    Parse XMLTV file asynchronously with timeout protection.
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    logger.info("Parsing XMLTV file: %s", file_path)
    logger.debug("  File size: %.2f MB", file_path.stat().st_size / 1024 / 1024)
    logger.debug("  Time window filter: %s to %s", time_from.isoformat(), time_to.isoformat())

    effective_timeout = parse_timeout_seconds if parse_timeout_seconds and parse_timeout_seconds > 0 else None

    loop = asyncio.get_running_loop()
    timeout_display = f"{effective_timeout}s" if effective_timeout else "disabled"
    logger.debug(
        "Offloading XML parsing to thread pool executor (timeout: %s)...",
        timeout_display,
    )
    parse_task = loop.run_in_executor(
        None,
        parse_xmltv_file,
        str(file_path),
        time_from,
        time_to,
    )

    try:
        if effective_timeout:
            channels, programs = await asyncio.wait_for(parse_task, timeout=effective_timeout)
        else:
            channels, programs = await parse_task
    except asyncio.CancelledError:
        # Preserve cooperative cancellation for the whole fetch pipeline.
        parse_task.cancel()
        raise
    except TimeoutError as exc:
        parse_task.cancel()
        logger.error("XML parsing timed out after %s for %s", timeout_display, file_path)
        raise ValueError("XML parsing timed out - file may be too large or malformed") from exc

    logger.info(
        "XML parsing completed successfully: %s channels, %s programs",
        len(channels),
        len(programs),
    )
    logger.debug(
        "  Parsed channels: %s%s",
        ", ".join([ch.xmltv_id for ch in channels[:3]]),
        "..." if len(channels) > 3 else "",
    )

    if not channels:
        logger.warning("No channels found in XMLTV file")
        raise ValueError("No channels found in XMLTV")

    if not programs:
        logger.warning("No programs found in XMLTV file (possibly outside time window)")
        raise ValueError("No programs found in XMLTV")

    logger.info(
        "XMLTV parsing validation passed: %s channels, %s programs",
        len(channels),
        len(programs),
    )

    return channels, programs


__all__ = ["process_single_source"]
