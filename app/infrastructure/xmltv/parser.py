from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional
import hashlib
import logging

from lxml import etree  # type: ignore

from app.domain.entities import Channel, Program

logger = logging.getLogger(__name__)


def _generate_deterministic_program_id(
    channel_id: str,
    start_time: datetime,
    title: str
) -> str:
    """
    Generate a deterministic UUID for a program based on its unique attributes.

    This ensures the same program (same channel, start time, and title) always
    gets the same ID, preventing duplicates when the same XMLTV data is parsed
    multiple times.
    """
    normalized_title = title.strip().lower()
    signature = f"{channel_id}|{start_time.isoformat()}|{normalized_title}"
    hash_obj = hashlib.sha256(signature.encode("utf-8"))
    return hash_obj.hexdigest()[:32]


def parse_xmltv_file(
    file_path: str,
    time_from: Optional[datetime] = None,
    time_to: Optional[datetime] = None,
) -> tuple[list[Channel], list[Program]]:
    """
    Parse XMLTV file and return channels and programs.
    """
    logger.debug("Parsing XMLTV file: %s", file_path)

    try:
        logger.debug("  Loading XML document...")
        tree = etree.parse(file_path)
        root = tree.getroot()
        logger.debug("  XML document loaded (root tag: %s)", root.tag)
    except etree.XMLSyntaxError as exc:
        logger.error("  XML parsing error: %s", exc)
        raise

    logger.debug("  Extracting channels...")
    channels = _parse_channels(root)
    logger.debug("    Found %s valid channels", len(channels))

    logger.debug("  Extracting programs (time filter: %s to %s)...", time_from, time_to)
    programs = _parse_programs(root, time_from, time_to)
    logger.debug("    Found %s programs in time window", len(programs))

    logger.info("XMLTV parsing complete: %s channels, %s programs", len(channels), len(programs))

    return channels, programs


def _parse_channels(root: etree._Element) -> list[Channel]:
    """Extract channels from XMLTV root element."""
    channels = []

    for channel in root.findall("channel"):
        xmltv_id = channel.get("id")
        if not xmltv_id:
            logger.debug("Skipping channel with missing ID attribute")
            continue

        display_name = _get_text(channel, "display-name", default=xmltv_id)

        icon_url = None
        icon_elem = channel.find("icon")
        if icon_elem is not None:
            try:
                icon_url = icon_elem.get("src")
            except (AttributeError, TypeError):
                logger.debug(
                    "Failed to extract icon URL from malformed icon element for channel %s",
                    xmltv_id,
                )

        channels.append(
            Channel(
                xmltv_id=xmltv_id,
                display_name=display_name or xmltv_id,
                icon_url=icon_url,
            )
        )

    return channels


def _parse_programs(
    root: etree._Element,
    time_from: Optional[datetime],
    time_to: Optional[datetime],
) -> list[Program]:
    """Extract programs from XMLTV root element."""
    programs = []

    for programme in root.findall("programme"):
        program = _parse_single_program(programme, time_from, time_to)
        if program:
            programs.append(program)

    return programs


def _parse_single_program(
    programme: etree._Element,
    time_from: Optional[datetime],
    time_to: Optional[datetime],
) -> Optional[Program]:
    """Parse single programme element."""
    channel_id = programme.get("channel")
    start_str = programme.get("start")
    stop_str = programme.get("stop")
    title_text = _get_text(programme, "title")

    if not channel_id or not start_str or not stop_str or title_text is None:
        return None

    title: str = title_text

    try:
        start_time = _parse_xmltv_time(start_str)
        stop_time = _parse_xmltv_time(stop_str)
    except (ValueError, IndexError, KeyError):
        return None

    if not _is_in_time_window(start_time, time_from, time_to):
        return None

    description = _get_text(programme, "desc")
    program_id = _generate_deterministic_program_id(channel_id, start_time, title)

    return Program(
        id=program_id,
        xmltv_channel_id=channel_id,
        start_time=start_time,
        stop_time=stop_time,
        title=title,
        description=description,
    )


def _parse_xmltv_time(time_str: str) -> datetime:
    """
    Convert XMLTV time format to ISO8601 UTC.
    """
    parts = time_str.strip().split()
    time_part = parts[0]  # YYYYMMDDHHMMSS
    tz_part = parts[1] if len(parts) > 1 else "+0000"

    dt = datetime.strptime(time_part, "%Y%m%d%H%M%S")

    tz_sign = 1 if tz_part[0] == "+" else -1
    tz_hours = int(tz_part[1:3])
    tz_mins = int(tz_part[3:5])
    tz_offset_minutes = tz_sign * (tz_hours * 60 + tz_mins)

    dt_utc = dt - timedelta(minutes=tz_offset_minutes)

    return dt_utc.replace(tzinfo=timezone.utc)


def _is_in_time_window(
    time_value: datetime,
    time_from: Optional[datetime],
    time_to: Optional[datetime],
) -> bool:
    """Check if time is within the specified window."""
    if not time_from and not time_to:
        return True

    if time_from and time_value < time_from:
        return False

    if time_to and time_value > time_to:
        return False

    return True


def _get_text(
    element: etree._Element,
    tag: str,
    default: Optional[str] = None,
) -> Optional[str]:
    """Safely extract text from XML element."""
    child = element.find(tag)
    if child is None or not child.text:
        return default
    return child.text.strip()


__all__ = ["parse_xmltv_file"]
