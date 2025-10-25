"""
Date and Time utilities

This module handles all date/time conversions, parsing, and EPG time window calculations.
Centralizes all date parsing logic to maintain consistency across the application.
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.schemas import EPGRequest

logger = logging.getLogger(__name__)


class DateFormatError(ValueError):
    """Raised when date format is invalid"""
    pass


def _normalize_iso8601_string(date_str: str) -> str:
    """Normalize ISO8601 string by replacing 'Z' with '+00:00'

    Args:
        date_str: ISO8601 datetime string

    Returns:
        Normalized string with explicit timezone offset
    """
    return date_str.replace('Z', '+00:00') if date_str.endswith('Z') else date_str


def parse_iso8601_to_utc(date_str: str) -> datetime:
    """
    Parse ISO8601 date string and convert to UTC datetime

    This is the single source of truth for date parsing across the application.

    Args:
        date_str: ISO8601 datetime string (e.g., '2025-10-09T00:00:00Z' or '2025-10-09T00:00:00+01:00')

    Returns:
        Timezone-aware datetime in UTC

    Raises:
        DateFormatError: If the date string format is invalid
    """
    try:
        normalized = _normalize_iso8601_string(date_str)
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, AttributeError) as e:
        raise DateFormatError(f"Invalid ISO8601 datetime format: '{date_str}'") from e


def convert_to_timezone(utc_time_str: str, target_tz: str) -> str:
    """
    Convert UTC timestamp to target timezone

    Args:
        utc_time_str: ISO8601 UTC timestamp
        target_tz: Target timezone (IANA format or 'UTC')

    Returns:
        ISO8601 timestamp in target timezone
    """
    dt = datetime.fromisoformat(utc_time_str)

    if target_tz == "UTC":
        return dt.isoformat()

    # Convert to target timezone
    target_zone = ZoneInfo(target_tz)
    dt_target = dt.astimezone(target_zone)

    return dt_target.isoformat()


def calculate_time_window(request: "EPGRequest") -> tuple[datetime, datetime]:
    """
    Calculate start and end time for EPG query based on request parameters

    Args:
        request: EPG request with from_date and to_date

    Returns:
        Tuple of (start_time, end_time) in UTC
    """
    # Use provided from_date and to_date
    start_time = parse_iso8601_to_utc(request.from_date)
    end_time = parse_iso8601_to_utc(request.to_date)
    return start_time, end_time
