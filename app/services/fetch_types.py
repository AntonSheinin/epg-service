"""
Shared dataclasses used across the EPG fetching pipeline.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class ChannelPayload:
    """In-memory representation of a channel row before persistence."""
    xmltv_id: str
    display_name: str
    icon_url: str | None = None


@dataclass(slots=True)
class ProgramPayload:
    """In-memory representation of a program row before persistence."""
    id: str
    xmltv_channel_id: str
    start_time: datetime
    stop_time: datetime
    title: str
    description: str | None = None
    created_at: datetime | None = None


__all__ = ["ChannelPayload", "ProgramPayload"]
