"""
File operation utilities

This module handles file download and cleanup operations.
"""
import logging
import tempfile
from pathlib import Path

import aiofiles
import httpx


logger = logging.getLogger(__name__)


async def download_file(url: str, filename: str, timeout: float = 120.0) -> Path:
    """
    Download a file from URL to temporary directory

    Args:
        url: URL to download from
        filename: Name for the temporary file
        timeout: HTTP timeout in seconds

    Returns:
        Path to downloaded temporary file

    Raises:
        httpx.HTTPError: If download fails
    """
    logger.info(f"Downloading file from {url}...")

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.get(url)
        response.raise_for_status()

        # Use system temp directory (cross-platform)
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / filename

        async with aiofiles.open(temp_file, 'wb') as f:
            await f.write(response.content)

        file_size = len(response.content) / (1024 * 1024)
        logger.info(f"Downloaded {file_size:.2f} MB to {temp_file}")

        return temp_file


def cleanup_temp_file(file_path: Path) -> bool:
    """
    Safely delete a temporary file

    Args:
        file_path: Path to file to delete

    Returns:
        True if deleted successfully, False otherwise
    """
    if not file_path or not file_path.exists():
        return False

    try:
        file_path.unlink()
        logger.debug(f"Cleaned up temporary file: {file_path}")
        return True
    except (OSError, PermissionError) as e:
        logger.warning(f"Failed to delete temporary file {file_path}: {e}")
        return False
