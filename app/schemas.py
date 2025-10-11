from pydantic import BaseModel, Field
from typing import Literal


class ChannelEPGRequest(BaseModel):
    """Single channel EPG request"""
    xmltv_id: str = Field(..., description="Channel XMLTV ID")
    epg_depth: int = Field(..., ge=0, le=365, description="Days to look back from now (0-365)")


class EPGRequest(BaseModel):
    """EPG data request"""
    channels: list[ChannelEPGRequest] = Field(..., min_length=1, description="List of channels with their EPG depth")
    update: Literal["force", "delta"] = Field(..., description="Update mode: 'force' (full) or 'delta' (from now)")
    # Removed the validate_unique_channels validator


class ProgramResponse(BaseModel):
    """Single program data"""
    id: str
    start_time: str
    stop_time: str
    title: str
    description: str | None


class EPGResponse(BaseModel):
    """EPG data response"""
    update_mode: Literal["force", "delta"]
    timestamp: str
    channels_requested: int
    channels_found: int
    total_programs: int
    epg: dict[str, list[ProgramResponse]] = Field(..., description="EPG data grouped by xmltv_id")
