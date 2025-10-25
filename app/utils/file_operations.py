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


async def download_file(
    url: str,
    filename: str,
    timeout: float = 120.0,
    max_retries: int = 3,
    backoff_factor: float = 2.0
) -> Path:
    """
    Download a file from URL with exponential backoff retry logic

    Retries on transient network errors (timeouts, connection errors).
    Does NOT retry on 4xx HTTP errors (client errors).

    Args:
        url: URL to download from
        filename: Name for the temporary file
        timeout: HTTP timeout in seconds
        max_retries: Maximum number of retry attempts
        backoff_factor: Exponential backoff multiplier (wait = backoff_factor ^ attempt)

    Returns:
        Path to downloaded temporary file

    Raises:
        httpx.HTTPError: If download fails after all retries
        asyncio.TimeoutError: If operation times out
    """
    logger.info(f"Downloading file from {url}...")

    last_error: Exception | None = None

    for attempt in range(max_retries):
        try:
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

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            # Transient network errors - retry
            last_error = e
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                logger.warning(
                    f"Download attempt {attempt + 1}/{max_retries} failed (transient error): {type(e).__name__}. "
                    f"Retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Download failed after {max_retries} attempts (transient error)")

        except httpx.HTTPStatusError as e:
            # HTTP errors - don't retry on 4xx (client error), retry on 5xx (server error)
            if 400 <= e.response.status_code < 500:
                logger.error(f"HTTP {e.response.status_code} (client error): {e}")
                raise

            # 5xx server error - retry
            last_error = e
            if attempt < max_retries - 1:
                wait_time = backoff_factor ** attempt
                logger.warning(
                    f"Download attempt {attempt + 1}/{max_retries} failed "
                    f"(HTTP {e.response.status_code} server error). "
                    f"Retrying in {wait_time:.1f}s..."
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(f"Download failed after {max_retries} attempts (HTTP {e.response.status_code})")

    # If we exhausted all retries, raise the last error
    if last_error:
        raise last_error

    raise RuntimeError(f"Failed to download {url} after {max_retries} attempts")


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
