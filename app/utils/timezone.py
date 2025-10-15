"""
Timezone conversion utilities

This module handles all timezone-related conversions and operations.
"""
from datetime import datetime
from zoneinfo import ZoneInfo


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
