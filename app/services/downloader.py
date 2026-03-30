"""
EPG Downloader Service.

Handles downloading EPG data sources to temporary files.
"""
from __future__ import annotations

import logging
from pathlib import Path

from app.utils.file_operations import download_file

logger = logging.getLogger(__name__)


async def process_single_source(source_url: str, source_index: int) -> Path:
    """Download a single EPG source and return the temporary file path."""
    logger.debug("  [Source %s] Download URL: %s", source_index, source_url)
    temp_file = await download_file(source_url, f"epg_source_{source_index}.xml")
    logger.info("  [Source %s] Download successful, file saved to %s", source_index, temp_file)
    logger.debug(
        "  [Source %s] File size: %.2f MB",
        source_index,
        temp_file.stat().st_size / 1024 / 1024,
    )
    return temp_file


__all__ = ["process_single_source"]
