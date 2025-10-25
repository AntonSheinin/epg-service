"""
Fetch Coordination

Manages EPG fetch operation coordination with concurrency protection.
Replaces global mutable state with a cleaner, more testable class-based approach.
"""
import asyncio
import logging
from typing import Callable, Any


logger = logging.getLogger(__name__)


class FetchCoordinator:
    """
    Coordinates EPG fetch operations to prevent concurrent executions.

    Uses an internal asyncio.Lock to ensure only one fetch operation runs at a time.
    This prevents multiple simultaneous downloads/parsing/storage operations
    that could cause data integrity issues.
    """

    def __init__(self):
        """Initialize the fetch coordinator with a lock."""
        self._fetch_lock = asyncio.Lock()

    async def execute(self, fetch_func: Callable[[], Any]) -> Any:
        """
        Execute a fetch operation with concurrency protection.

        Args:
            fetch_func: Async function to execute (typically the fetch_and_process logic)

        Returns:
            Result from fetch_func or error/skip response if already running

        Raises:
            Any exception raised by fetch_func
        """
        # Try to acquire lock without blocking
        if self._fetch_lock.locked():
            logger.warning("EPG fetch already in progress, skipping this request")
            return {
                "status": "skipped",
                "message": "EPG fetch operation already in progress"
            }

        async with self._fetch_lock:
            return await fetch_func()

    def is_fetching(self) -> bool:
        """
        Check if a fetch operation is currently in progress.

        Returns:
            True if fetch is running, False otherwise
        """
        return self._fetch_lock.locked()


# Global singleton instance
_coordinator: FetchCoordinator | None = None


def get_fetch_coordinator() -> FetchCoordinator:
    """
    Get or create the global fetch coordinator singleton.

    Returns:
        The global FetchCoordinator instance
    """
    global _coordinator
    if _coordinator is None:
        _coordinator = FetchCoordinator()
    return _coordinator


def reset_fetch_coordinator() -> None:
    """
    Reset the fetch coordinator (mainly for testing).

    WARNING: Only use this in test environments!
    """
    global _coordinator
    _coordinator = None
