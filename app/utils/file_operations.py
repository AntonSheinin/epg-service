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
_http_client_lock = asyncio.Lock()


async def get_http_client() -> httpx.AsyncClient:
    """
    Get or create the module-level HTTP client for connection pooling.

    Thread-safe initialization with asyncio lock to prevent race conditions.
    """
    global _http_client
    if _http_client is None:
        async with _http_client_lock:
            # Double-check pattern: another coroutine might have created it while we waited
            if _http_client is None:
                _http_client = httpx.AsyncClient(timeout=120)
                logger.debug("HTTP client initialized for connection pooling")
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
    logger.debug(f"Starting download from URL: {url}")
    logger.debug(f"  Temporary filename: {filename}")

    client = await get_http_client()

    for attempt in range(3):
        try:
            logger.debug(f"  Download attempt {attempt + 1}/3")
            response = await client.get(url)
            response.raise_for_status()

            logger.debug(f"  HTTP {response.status_code}: Download completed")

            temp_dir = Path(tempfile.gettempdir())
            temp_file = temp_dir / filename

            logger.debug(f"  Writing to temporary file: {temp_file}")
            async with aiofiles.open(temp_file, 'wb') as f:
                await f.write(response.content)

            file_size = len(response.content) / (1024 * 1024)
            logger.info(f"Successfully downloaded {file_size:.2f} MB from EPG source")
            logger.debug(f"  Saved to: {temp_file}")
            return temp_file

        except httpx.TimeoutException as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                logger.warning(f"Download attempt {attempt + 1}/3 timed out after 120s. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Download failed after 3 attempts due to timeout")
                raise
        except httpx.ConnectError as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                logger.warning(f"Download attempt {attempt + 1}/3 failed: connection error ({e}). Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Download failed after 3 attempts: unable to connect to server")
                raise
        except httpx.HTTPStatusError as e:
            if attempt < 2:
                wait_time = 2 ** attempt
                logger.warning(f"Download attempt {attempt + 1}/3 failed: HTTP {e.response.status_code}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Download failed after 3 attempts: HTTP {e.response.status_code} {e.response.reason_phrase}")
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
