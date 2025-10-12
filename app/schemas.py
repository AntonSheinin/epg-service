from pydantic import BaseModel, Field, field_validator
from typing import Literal
from datetime import timezone
from zoneinfo import ZoneInfo, available_timezones


class ChannelEPGRequest(BaseModel):
    """Single channel EPG request"""
    xmltv_id: str = Field(..., description="Channel XMLTV ID")
    epg_depth: int = Field(..., ge=0, le=365, description="Days to look back from now (0-365)")


class EPGRequest(BaseModel):
    """EPG data request"""
    channels: list[ChannelEPGRequest] = Field(..., min_length=1, description="List of channels with their EPG depth")
    update: Literal["force", "delta"] = Field(..., description="Update mode: 'force' (full) or 'delta' (from now)")
    timezone: str = Field(default="UTC", description="Timezone for response timestamps (e.g., 'UTC', 'Europe/London', 'America/New_York')")

    @field_validator('timezone')
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        """Validate timezone string"""
        if v == "UTC":
            return v
        try:
            # Check if timezone is valid
            ZoneInfo(v)
            return v
        except Exception:
            raise ValueError(f"Invalid timezone: {v}. Must be a valid IANA timezone (e.g., 'Europe/London', 'America/New_York') or 'UTC'")


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
    timezone: str = Field(..., description="Timezone used for all timestamps in response")
    channels_requested: int
    channels_found: int
    total_programs: int
    epg: dict[str, list[ProgramResponse]] = Field(..., description="EPG data grouped by xmltv_id")
