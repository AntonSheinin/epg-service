"""
File operation utilities

This module handles file download and cleanup operations with retry logic.
"""
import logging
import tempfile
from pathlib import Path
import asyncio

import aiofiles
import httpx


logger = logging.getLogger(__name__)

# Module-level HTTP client for connection pooling and reuse
_http_client: httpx.AsyncClient | None = None


def get_http_client() -> httpx.AsyncClient:
    """Get or create the module-level HTTP client for connection pooling"""
    global _http_client
    if _http_client is None:
        _http_client = httpx.AsyncClient(timeout=120)
    return _http_client


async def close_http_client() -> None:
    """Close the module-level HTTP client"""
    global _http_client
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


async def download_file(url: str, filename: str) -> Path:
    """
    Download a file from URL with basic retry logic (3 attempts)

    Uses module-level HTTP client for connection pooling and reuse.

    Args:
        url: URL to download from
        filename: Name for the temporary file

    Returns:
        Path to downloaded temporary file

    Raises:
        httpx.HTTPError: If download fails after retries
    """
    logger.info(f"Downloading from {url}...")

    client = get_http_client()

    for attempt in range(3):
        try:
            response = await client.get(url)
            response.raise_for_status()

            temp_dir = Path(tempfile.gettempdir())
            temp_file = temp_dir / filename

            async with aiofiles.open(temp_file, 'wb') as f:
                await f.write(response.content)

            file_size = len(response.content) / (1024 * 1024)
            logger.info(f"Downloaded {file_size:.2f} MB")
            return temp_file

        except (httpx.TimeoutException, httpx.ConnectError, httpx.HTTPStatusError) as e:
            if attempt < 2:
                logger.warning(f"Download attempt {attempt + 1}/3 failed: {type(e).__name__}. Retrying...")
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error(f"Download failed after 3 attempts: {type(e).__name__}")
                raise


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
