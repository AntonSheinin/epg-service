from datetime import datetime, timezone, timedelta
from typing import Optional
from uuid import uuid4
import logging

from lxml import etree # type: ignore

from app.services.fetch_types import ChannelPayload, ProgramPayload

logger = logging.getLogger(__name__)

def parse_xmltv_file(file_path: str, time_from: Optional[datetime] = None, time_to: Optional[datetime] = None) -> tuple[list[ChannelPayload], list[ProgramPayload]]:
    """
    Parse XMLTV file and return channels and programs

    Args:
        file_path: Path to XMLTV file
        time_from: Optional start of time window (UTC)
        time_to: Optional end of time window (UTC)

    Returns:
        Tuple of (channels, programs)
        - channels: List of (xmltv_id, display_name, icon_url)
        - programs: List of dicts with program data

    Raises:
        etree.XMLSyntaxError: If XML is malformed
        FileNotFoundError: If file doesn't exist
        OSError: If file can't be read
    """
    logger.debug(f"Parsing XMLTV file: {file_path}")

    try:
        logger.debug("  Loading XML document...")
        tree = etree.parse(file_path)
        root = tree.getroot()
        logger.debug(f"  XML document loaded (root tag: {root.tag})")
    except etree.XMLSyntaxError as e:
        logger.error(f"  XML parsing error: {e}")
        raise

    logger.debug("  Extracting channels...")
    channels = _parse_channels(root)
    logger.debug(f"    Found {len(channels)} valid channels")

    logger.debug(f"  Extracting programs (time filter: {time_from} to {time_to})...")
    programs = _parse_programs(root, time_from, time_to)
    logger.debug(f"    Found {len(programs)} programs in time window")

    logger.info(f"XMLTV parsing complete: {len(channels)} channels, {len(programs)} programs")

    return channels, programs


def _parse_channels(root: etree._Element) -> list[ChannelPayload]:
    """Extract channels from XMLTV root element"""
    channels = []

    for channel in root.findall('channel'):
        xmltv_id = channel.get('id')
        if not xmltv_id:
            logger.debug("Skipping channel with missing ID attribute")
            continue

        # Get display name (first one or fallback to ID)
        display_name = _get_text(channel, 'display-name', default=xmltv_id)

        # Get icon URL
        icon_url = None
        icon_elem = channel.find('icon')
        if icon_elem is not None:
            try:
                icon_url = icon_elem.get('src')
            except (AttributeError, TypeError):
                logger.debug(f"Failed to extract icon URL from malformed icon element for channel {xmltv_id}")

        channels.append(ChannelPayload(
            xmltv_id=xmltv_id,
            display_name=display_name or xmltv_id,
            icon_url=icon_url
        ))

    return channels


def _parse_programs(root: etree._Element, time_from: Optional[datetime], time_to: Optional[datetime]) -> list[ProgramPayload]:
    """Extract programs from XMLTV root element"""
    programs = []

    for programme in root.findall('programme'):
        program = _parse_single_program(programme, time_from, time_to)
        if program:
            programs.append(program)

    return programs


def _parse_single_program(programme: etree._Element, time_from: Optional[datetime], time_to: Optional[datetime]) -> Optional[ProgramPayload]:
    """Parse single programme element"""
    # Required fields
    channel_id = programme.get('channel')
    start_str = programme.get('start')
    stop_str = programme.get('stop')
    title_text = _get_text(programme, 'title')

    # Skip if missing required fields
    if not channel_id or not start_str or not stop_str or title_text is None:
        return None

    title: str = title_text

    # Parse times (skip invalid formats)
    try:
        start_time = _parse_xmltv_time(start_str)
        stop_time = _parse_xmltv_time(stop_str)
    except (ValueError, IndexError, KeyError):
        return None

    # Filter by time window
    if not _is_in_time_window(start_time, time_from, time_to):
        return None

    # Optional fields
    description = _get_text(programme, 'desc')

    return ProgramPayload(
        id=str(uuid4()),
        xmltv_channel_id=channel_id,
        start_time=start_time,
        stop_time=stop_time,
        title=title,
        description=description
    )


def _parse_xmltv_time(time_str: str) -> datetime:
    """
    Convert XMLTV time format to ISO8601 UTC

    Args:
        time_str: XMLTV time like '20080715003000 -0600'

    Returns:
        Timezone-aware datetime in UTC
    """
    # Split time and timezone
    parts = time_str.strip().split()
    time_part = parts[0]  # YYYYMMDDHHMMSS
    tz_part = parts[1] if len(parts) > 1 else '+0000'

    # Parse datetime
    dt = datetime.strptime(time_part, '%Y%m%d%H%M%S')

    # Parse timezone offset (±HHMM)
    tz_sign = 1 if tz_part[0] == '+' else -1
    tz_hours = int(tz_part[1:3])
    tz_mins = int(tz_part[3:5])
    tz_offset_minutes = tz_sign * (tz_hours * 60 + tz_mins)

    # Convert to UTC
    dt_utc = dt - timedelta(minutes=tz_offset_minutes)

    return dt_utc.replace(tzinfo=timezone.utc)


def _is_in_time_window(time_value: datetime, time_from: Optional[datetime], time_to: Optional[datetime]) -> bool:
    """Check if time is within the specified window"""
    if not time_from and not time_to:
        return True

    if time_from and time_value < time_from:
        return False

    if time_to and time_value > time_to:
        return False

    return True


def _get_text(element: etree._Element, tag: str, default: Optional[str] = None) -> Optional[str]:
    """Safely extract text from XML element"""
    child = element.find(tag)
    if child is None or not child.text:
        return default
    return child.text.strip()
