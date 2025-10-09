from lxml import etree
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Dict, Optional
import uuid


def parse_xmltv_time(time_str: str) -> str:
    """
    Convert XMLTV time format to ISO8601 UTC

    Input: '20080715003000 -0600'
    Output: '2008-07-15T06:30:00+00:00'
    """
    # Split time and timezone
    parts = time_str.strip().split()
    time_part = parts[0]
    tz_part = parts[1] if len(parts) > 1 else '+0000'

    # Parse datetime: YYYYMMDDHHMMSS
    dt = datetime.strptime(time_part, '%Y%m%d%H%M%S')

    # Parse timezone offset
    tz_sign = 1 if tz_part[0] == '+' else -1
    tz_hours = int(tz_part[1:3])
    tz_mins = int(tz_part[3:5])
    tz_offset = tz_sign * (tz_hours * 60 + tz_mins)

    # Convert to UTC
    dt_utc = dt - timedelta(minutes=tz_offset)

    # Return ISO8601 format
    return dt_utc.replace(tzinfo=timezone.utc).isoformat()


def parse_xmltv_file(
    file_path: str,
    time_from: Optional[datetime] = None,
    time_to: Optional[datetime] = None
) -> Tuple[List[Tuple], List[Dict]]:
    """
    Parse XMLTV file and return channels and programs

    Args:
        file_path: Path to XMLTV file
        time_from: Optional start of time window
        time_to: Optional end of time window

    Returns:
        (channels, programs) tuple
    """
    try:
        tree = etree.parse(file_path)
    except Exception as e:
        print(f"Error parsing XML file: {e}")
        return [], []

    root = tree.getroot()

    channels = []
    programs = []

    # Parse channels
    for channel in root.findall('channel'):
        xmltv_id = channel.get('id')
        if not xmltv_id:
            continue

        # Get first display-name
        display_name_elem = channel.find('display-name')
        if display_name_elem is None or not display_name_elem.text:
            display_name = xmltv_id  # Fallback to ID
        else:
            display_name = display_name_elem.text.strip()

        # Get icon if available
        icon_elem = channel.find('icon')
        icon_url = icon_elem.get('src') if icon_elem is not None else None

        channels.append((xmltv_id, display_name, icon_url))

    print(f"  Parsed {len(channels)} channels")

    # Parse programs
    for programme in root.findall('programme'):
        try:
            channel_id = programme.get('channel')
            if not channel_id:
                continue

            # Parse times
            start_str = programme.get('start')
            stop_str = programme.get('stop')

            if not start_str or not stop_str:
                continue

            start_time = parse_xmltv_time(start_str)
            stop_time = parse_xmltv_time(stop_str)

            # Filter by time window if provided
            if time_from or time_to:
                start_dt = datetime.fromisoformat(start_time)
                if time_from and start_dt < time_from:
                    continue
                if time_to and start_dt > time_to:
                    continue

            # Get title
            title_elem = programme.find('title')
            if title_elem is None or not title_elem.text:
                continue
            title = title_elem.text.strip()

            # Get description
            desc_elem = programme.find('desc')
            description = desc_elem.text.strip() if desc_elem is not None and desc_elem.text else None

            programs.append({
                'id': str(uuid.uuid4()),
                'xmltv_channel_id': channel_id,
                'start_time': start_time,
                'stop_time': stop_time,
                'title': title,
                'description': description
            })
        except Exception as e:
            # Skip problematic programs
            continue

    print(f"  Parsed {len(programs)} programs")

    return channels, programs
