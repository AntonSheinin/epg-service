"""
Structured logging helpers for consistent log formatting.

Provides utilities for structured, clean logging without excessive decorative separators.
"""
import logging
from datetime import datetime, timezone


def log_section_start(logger: logging.Logger, section_name: str) -> None:
    """
    Log the start of a processing section.

    Args:
        logger: Logger instance
        section_name: Name of the section being started
    """
    logger.info(f"Starting: {section_name}")


def log_section_end(logger: logging.Logger, section_name: str) -> None:
    """
    Log the end of a processing section.

    Args:
        logger: Logger instance
        section_name: Name of the section being ended
    """
    logger.info(f"Completed: {section_name}")


def log_source_processing(logger: logging.Logger, idx: int, total: int, url: str) -> None:
    """
    Log source processing header.

    Args:
        logger: Logger instance
        idx: Current source index (1-based)
        total: Total number of sources
        url: Source URL being processed
    """
    logger.info(f"Processing source {idx}/{total}: {url}")


def log_fetch_start(logger: logging.Logger) -> None:
    """Log EPG fetch operation start."""
    logger.info(f"EPG fetch started at {datetime.now(timezone.utc).isoformat()}")


def log_fetch_end(logger: logging.Logger) -> None:
    """Log EPG fetch operation end."""
    logger.info(f"EPG fetch completed at {datetime.now(timezone.utc).isoformat()}")


def log_merge_summary(
    logger: logging.Logger,
    channels_count: int,
    programs_count: int
) -> None:
    """
    Log merge operation summary.

    Args:
        logger: Logger instance
        channels_count: Number of merged channels
        programs_count: Number of merged programs
    """
    logger.info(f"Merge summary - Channels: {channels_count}, Programs: {programs_count}")


def log_storage_stats(
    logger: logging.Logger,
    total_channels: int,
    total_programs: int
) -> None:
    """
    Log storage statistics.

    Args:
        logger: Logger instance
        total_channels: Total unique channels to store
        total_programs: Total unique programs to store
    """
    logger.info(
        f"Storing data: {total_channels} channels, {total_programs} programs"
    )
