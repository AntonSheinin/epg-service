"""
Data merging utilities

This module handles merging of channels and programs from multiple sources.
"""
import logging
from typing import TypeAlias, TYPE_CHECKING

if TYPE_CHECKING:
    from app.schemas import Channel, Program

logger = logging.getLogger(__name__)

# Type aliases for clarity - these map to Pydantic models in schemas.py
ChannelTuple: TypeAlias = tuple[str, str, str | None]  # (xmltv_id, display_name, icon_url) - maps to Channel
ProgramDict: TypeAlias = dict[str, str | None]  # maps to Program model


def merge_channels(
    existing_channels: dict[str, ChannelTuple],
    new_channels: list[ChannelTuple]
) -> tuple[dict[str, ChannelTuple], int]:
    """
    Merge new channels into existing channel dictionary

    Updates existing channel data if new source has display name or icon.
    Counts as 'new' only if channel was not previously seen.

    Args:
        existing_channels: Dictionary of existing channels (xmltv_id -> channel_tuple)
        new_channels: List of new channel tuples to merge

    Returns:
        Tuple of (updated_channels_dict, count_of_new_channels_added)
    """
    new_count = 0

    for channel in new_channels:
        xmltv_id = channel[0]
        if xmltv_id not in existing_channels:
            existing_channels[xmltv_id] = channel
            new_count += 1
        else:
            # Update channel data if new source has better information
            existing_id, existing_display, existing_icon = existing_channels[xmltv_id]
            new_id, new_display, new_icon = channel

            # Prefer non-empty values from new source
            updated_display = new_display if new_display else existing_display
            updated_icon = new_icon if new_icon else existing_icon

            if updated_display != existing_display or updated_icon != existing_icon:
                existing_channels[xmltv_id] = (xmltv_id, updated_display, updated_icon)
                logger.debug(f"Updated channel {xmltv_id} with better data from new source")
            else:
                logger.debug(f"Duplicate channel (no new data): {xmltv_id}")

    return existing_channels, new_count


def merge_programs(
    existing_programs: dict[str, ProgramDict],
    new_programs: list[ProgramDict]
) -> tuple[dict[str, ProgramDict], int]:
    """
    Merge new programs into existing program dictionary

    Args:
        existing_programs: Dictionary of existing programs (program_key -> program_dict)
        new_programs: List of new program dicts to merge

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
                f"Skipping duplicate program: {program.get('title', 'Unknown')} "
                f"on {program.get('xmltv_channel_id', 'Unknown')}"
            )

    return existing_programs, new_count


def create_program_key(program: ProgramDict) -> str:
    """
    Create a unique key for a program based on channel, time, and title

    Args:
        program: Program dictionary

    Returns:
        Unique program key string
    """
    return f"{program['xmltv_channel_id']}_{program['start_time']}_{program['title']}"
