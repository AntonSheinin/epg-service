"""
Date and Time utilities

This module handles all date/time conversions, parsing, and EPG time window calculations.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.config import settings
from app.schemas import EPGRequest


def parse_iso8601_to_utc(date_str: str) -> datetime:
    """
    Parse ISO8601 date string and convert to UTC datetime

    Args:
        date_str: ISO8601 datetime string (e.g., '2025-10-09T00:00:00Z' or '2025-10-09T00:00:00+01:00')

    Returns:
        Timezone-aware datetime in UTC
    """
    parsed_str = date_str.replace('Z', '+00:00') if date_str.endswith('Z') else date_str
    dt = datetime.fromisoformat(parsed_str)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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


def calculate_time_window(
    request: EPGRequest,
    channel_epg_depth: int,
    now: datetime
) -> tuple[datetime, datetime]:
    """
    Calculate start and end time for EPG query based on request parameters

    Args:
        request: EPG request with optional from_date/to_date
        channel_epg_depth: Number of days to look back for this channel
        now: Current datetime in UTC

    Returns:
        Tuple of (start_time, end_time) in UTC
    """
    # Custom date range takes precedence
    if request.from_date or request.to_date:
        start_time = parse_iso8601_to_utc(request.from_date) if request.from_date else datetime.min.replace(tzinfo=timezone.utc)
        end_time = parse_iso8601_to_utc(request.to_date) if request.to_date else now + timedelta(days=365 * 10)
        return start_time, end_time

    # Default behavior based on update mode
    future_limit = now + timedelta(days=settings.max_future_epg_limit)
    if request.update == "force":
        return now - timedelta(days=channel_epg_depth), future_limit
    else:  # delta
        return now, future_limit
