from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import perf_counter
from typing import Optional
import hashlib
import logging
import xml.etree.ElementTree as ET

from app.models import Channel, Program

logger = logging.getLogger(__name__)


def _generate_deterministic_program_id(
    channel_id: str,
    start_time: datetime,
    title: str,
) -> str:
    """Generate a stable identifier for a program row."""
    normalized_title = title.strip().lower()
    signature = f"{channel_id}|{start_time.isoformat()}|{normalized_title}"
    hash_obj = hashlib.sha256(signature.encode("utf-8"))
    return hash_obj.hexdigest()[:32]


@dataclass(slots=True)
class ProgramBatch:
    programs: list[Program]
    reached_eof: bool
    skipped_unknown_channels: int = 0


def parse_xmltv_channels(
    file_path: str | Path,
    *,
    deadline_monotonic: float | None = None,
) -> list[Channel]:
    """Parse all channel rows from an XMLTV file using incremental parsing."""
    path = Path(file_path)
    logger.info("Parsing XMLTV channels from %s", path)
    channels_by_id: dict[str, Channel] = {}

    context = ET.iterparse(str(path), events=("start", "end"))
    _, root = next(context)
    for event, elem in context:
        if event != "end":
            continue

        _check_deadline(deadline_monotonic)
        if elem.tag == "channel":
            _check_deadline(deadline_monotonic)
            channel = _parse_channel_element(elem)
            if channel is not None:
                channels_by_id[channel.xmltv_id] = channel
            root.clear()
        elif elem.tag == "programme":
            root.clear()
            break

    channels = list(channels_by_id.values())
    logger.info("XMLTV channel parsing complete: %s channels", len(channels))
    return channels


class XMLTVProgramBatchReader:
    """Incrementally read program batches from an XMLTV file."""

    def __init__(
        self,
        file_path: str | Path,
        *,
        known_channel_ids: set[str],
        time_from: Optional[datetime],
        time_to: Optional[datetime],
        batch_size: int,
        deadline_monotonic: float | None = None,
    ) -> None:
        self._path = Path(file_path)
        self._known_channel_ids = known_channel_ids
        self._time_from = time_from
        self._time_to = time_to
        self._batch_size = max(1, batch_size)
        self._deadline_monotonic = deadline_monotonic
        self._context = ET.iterparse(
            str(self._path),
            events=("start", "end"),
        )
        _, self._root = next(self._context)
        self._reached_eof = False

    def read_next_batch(self) -> ProgramBatch:
        """Read the next batch of valid programs."""
        if self._reached_eof or self._context is None or self._root is None:
            return ProgramBatch(programs=[], reached_eof=True)

        programs: list[Program] = []
        skipped_unknown_channels = 0

        while len(programs) < self._batch_size:
            _check_deadline(self._deadline_monotonic)
            try:
                event, elem = next(self._context)
            except StopIteration:
                self._reached_eof = True
                return ProgramBatch(
                    programs=programs,
                    reached_eof=True,
                    skipped_unknown_channels=skipped_unknown_channels,
                )

            if event != "end" or elem.tag != "programme":
                if event == "end" and elem.tag == "channel":
                    self._root.clear()
                continue

            program = _parse_single_program(elem, self._time_from, self._time_to)
            if program is None:
                self._root.clear()
                continue
            if program.xmltv_channel_id not in self._known_channel_ids:
                skipped_unknown_channels += 1
                self._root.clear()
                continue

            programs.append(program)
            self._root.clear()

        return ProgramBatch(
            programs=programs,
            reached_eof=False,
            skipped_unknown_channels=skipped_unknown_channels,
        )

    def close(self) -> None:
        """Close the underlying file handle."""
        if self._context is None:
            return

        self._context = None
        self._root = None


def _parse_channel_element(channel: ET.Element) -> Channel | None:
    xmltv_id = channel.get("id")
    if not xmltv_id:
        logger.debug("Skipping channel with missing ID attribute")
        return None

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

    return Channel(
        xmltv_id=xmltv_id,
        display_name=display_name or xmltv_id,
        icon_url=icon_url,
    )


def _parse_single_program(
    programme: ET.Element,
    time_from: Optional[datetime],
    time_to: Optional[datetime],
) -> Program | None:
    channel_id = programme.get("channel")
    start_str = programme.get("start")
    stop_str = programme.get("stop")
    title_text = _get_text(programme, "title")

    if not channel_id or not start_str or not stop_str or title_text is None:
        return None

    try:
        start_time = _parse_xmltv_time(start_str)
        stop_time = _parse_xmltv_time(stop_str)
    except (ValueError, IndexError, KeyError):
        return None

    if not _is_in_time_window(start_time, time_from, time_to):
        return None

    title = title_text
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
    """Convert XMLTV time format to UTC."""
    parts = time_str.strip().split()
    time_part = parts[0]
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
    if not time_from and not time_to:
        return True
    if time_from and time_value < time_from:
        return False
    if time_to and time_value > time_to:
        return False
    return True


def _get_text(
    element: ET.Element,
    tag: str,
    default: Optional[str] = None,
) -> Optional[str]:
    child = element.find(tag)
    if child is None or not child.text:
        return default
    return child.text.strip()


def _check_deadline(deadline_monotonic: float | None) -> None:
    if deadline_monotonic is not None and perf_counter() > deadline_monotonic:
        raise TimeoutError("XML parsing timed out - file may be too large or malformed")


__all__ = ["ProgramBatch", "XMLTVProgramBatchReader", "parse_xmltv_channels"]
