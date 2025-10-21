from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal
from zoneinfo import ZoneInfo
from datetime import datetime


class ChannelEPGRequest(BaseModel):
    """Single channel EPG request"""
    xmltv_id: str = Field(..., description="Channel XMLTV ID")
    epg_depth: int = Field(..., ge=0, le=365, description="Days to look back from now (0-365)")


class EPGRequest(BaseModel):
    """EPG data request"""
    channels: list[ChannelEPGRequest] = Field(..., min_length=1, description="List of channels with their EPG depth")
    update: Literal["force", "delta"] = Field(..., description="Update mode: 'force' (full) or 'delta' (from now)")
    timezone: str = Field(default="UTC", description="Timezone for response timestamps (e.g., 'UTC', 'Europe/London', 'America/New_York')")
    from_date: str | None = Field(default=None, description="Optional ISO8601 datetime for start of EPG range. If only from_date is provided, returns all EPG from this date to most recent. (e.g., '2025-10-09T00:00:00Z')")
    to_date: str | None = Field(default=None, description="Optional ISO8601 datetime for end of EPG range. If only to_date is provided, returns all EPG up to this date. If both dates provided, returns EPG between them. (e.g., '2025-10-10T00:00:00Z')")

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

    @field_validator('from_date', 'to_date')
    @classmethod
    def validate_date_format(cls, v: str | None) -> str | None:
        """Validate ISO8601 datetime format"""
        if v is None:
            return v
        try:
            # Try parsing as ISO8601 datetime
            # Replace 'Z' with '+00:00' for proper parsing
            date_str = v.replace('Z', '+00:00') if v.endswith('Z') else v
            datetime.fromisoformat(date_str)
            return v
        except (ValueError, AttributeError) as e:
            raise ValueError(f"Invalid datetime format: {v}. Must be valid ISO8601 format (e.g., '2025-10-09T00:00:00Z' or '2025-10-09T00:00:00+00:00')")

    @model_validator(mode='after')
    def validate_date_range(self):
        """Validate that from_date is before to_date when both are provided"""
        if self.from_date and self.to_date:
            # Parse dates for comparison
            from_dt = datetime.fromisoformat(self.from_date.replace('Z', '+00:00') if self.from_date.endswith('Z') else self.from_date)
            to_dt = datetime.fromisoformat(self.to_date.replace('Z', '+00:00') if self.to_date.endswith('Z') else self.to_date)

            if from_dt >= to_dt:
                raise ValueError(f"from_date ({self.from_date}) must be before to_date ({self.to_date})")

        return self


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
