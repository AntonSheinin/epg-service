"""
Data merging utilities

This module handles merging of channels and programs from multiple sources.
"""
import logging
from collections.abc import MutableMapping, Sequence

from app.services.fetch_types import ChannelPayload, ProgramPayload

logger = logging.getLogger(__name__)


def merge_channels(
    existing_channels: MutableMapping[str, ChannelPayload],
    new_channels: Sequence[ChannelPayload]
) -> tuple[MutableMapping[str, ChannelPayload], int]:
    """
    Merge new channels into existing channel dictionary.

    Updates existing channel data if new source has display name or icon.
    Counts as 'new' only if channel was not previously seen.

    Args:
        existing_channels: Dictionary of existing channels (xmltv_id -> ChannelPayload)
        new_channels: Iterable of new channel payloads to merge

    Returns:
        Tuple of (updated_channels_dict, count_of_new_channels_added)
    """
    new_count = 0

    for channel in new_channels:
        current = existing_channels.get(channel.xmltv_id)
        if current is None:
            existing_channels[channel.xmltv_id] = channel
            new_count += 1
        else:
            updated_display = channel.display_name or current.display_name
            updated_icon = channel.icon_url or current.icon_url

            if (
                updated_display != current.display_name
                or updated_icon != current.icon_url
            ):
                existing_channels[channel.xmltv_id] = ChannelPayload(
                    xmltv_id=channel.xmltv_id,
                    display_name=updated_display,
                    icon_url=updated_icon,
                )
                logger.debug("Updated channel %s with merged data", channel.xmltv_id)

    return existing_channels, new_count


def merge_programs(
    existing_programs: MutableMapping[str, ProgramPayload],
    new_programs: Sequence[ProgramPayload]
) -> tuple[MutableMapping[str, ProgramPayload], int]:
    """
    Merge new programs into existing program dictionary.

    Args:
        existing_programs: Dictionary of existing programs (program_key -> ProgramPayload)
        new_programs: Iterable of new program payloads to merge

    Returns:
        Tuple of (updated_programs_dict, count_of_new_programs_added)
    """
    new_count = 0

    for program in new_programs:
        program_key = create_program_key(program)
        if program_key not in existing_programs:
            existing_programs[program_key] = program
            new_count += 1
        else:
            logger.debug(
                "Skipping duplicate program: %s on %s",
                program.title,
                program.xmltv_channel_id,
            )

    return existing_programs, new_count


def create_program_key(program: ProgramPayload) -> str:
    """
    Create a unique key for a program based on channel, time, and title.

    Args:
        program: ProgramPayload instance

    Returns:
        Unique program key string
    """
    return f"{program.xmltv_channel_id}_{program.start_time.isoformat()}_{program.title}"
