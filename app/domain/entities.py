"""
Domain entities for the EPG service.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Channel:
    """Channel entity."""
    xmltv_id: str
    display_name: str
    icon_url: str | None = None


@dataclass(slots=True)
class Program:
    """Program entity."""
    id: str
    xmltv_channel_id: str
    start_time: datetime
    stop_time: datetime
    title: str
    description: str | None = None
    created_at: datetime | None = None


__all__ = ["Channel", "Program"]
