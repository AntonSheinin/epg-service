from pydantic import BaseModel, Field, field_validator, model_validator
from zoneinfo import ZoneInfo
from datetime import datetime
from uuid import UUID

from app.utils.timezone import parse_iso8601_to_utc, DateFormatError


class ChannelEPGRequest(BaseModel):
    """Single channel EPG request"""
    xmltv_id: str = Field(..., description="Channel XMLTV ID")


class EPGRequest(BaseModel):
    """EPG data request"""
    channels: list[ChannelEPGRequest] = Field(..., min_length=1, description="List of channels")
    timezone: str = Field(default="UTC", description="Timezone for response timestamps (e.g., 'UTC', 'Europe/London', 'America/New_York')")
    from_date: str = Field(..., description="ISO8601 datetime for start of EPG range (e.g., '2025-10-09T00:00:00Z')")
    to_date: str = Field(..., description="ISO8601 datetime for end of EPG range (e.g., '2025-10-10T00:00:00Z')")

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
        except (KeyError, ValueError):
            raise ValueError(f"Invalid timezone: {v}. Must be a valid IANA timezone (e.g., 'Europe/London', 'America/New_York') or 'UTC'")

    @field_validator('from_date', 'to_date')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Validate ISO8601 datetime format using centralized parser"""
        try:
            parse_iso8601_to_utc(v)  # Validate using centralized parser
            return v
        except DateFormatError as e:
            raise ValueError(f"Invalid datetime format: {v}. Must be valid ISO8601 format (e.g., '2025-10-09T00:00:00Z' or '2025-10-09T00:00:00+00:00')")

    @model_validator(mode='after')
    def validate_date_range(self):
        """Validate that from_date is before to_date using centralized parser"""
        from_dt = parse_iso8601_to_utc(self.from_date)
        to_dt = parse_iso8601_to_utc(self.to_date)

        if from_dt >= to_dt:
            raise ValueError(f"from_date ({self.from_date}) must be before to_date ({self.to_date})")

        return self


class Channel(BaseModel):
    """Channel data model"""
    xmltv_id: str = Field(..., description="Unique XMLTV channel ID")
    display_name: str = Field(..., description="Display name of the channel")
    icon_url: str | None = Field(None, description="URL to channel icon")


class Program(BaseModel):
    """Program data model"""
    id: str = Field(..., description="Unique program ID (UUID)")
    xmltv_channel_id: str = Field(..., description="XMLTV channel ID this program belongs to")
    start_time: str = Field(..., description="ISO8601 UTC start time")
    stop_time: str = Field(..., description="ISO8601 UTC stop time")
    title: str = Field(..., description="Program title")
    description: str | None = Field(None, description="Program description")


class ProgramResponse(BaseModel):
    """Single program data"""
    id: str
    start_time: str
    stop_time: str
    title: str
    description: str | None


class ErrorDetail(BaseModel):
    """Standard error detail"""
    code: str = Field(..., description="Error code (e.g., 'FETCH_FAILED', 'VALIDATION_ERROR')")
    message: str = Field(..., description="Human-readable error message")
    context: dict | None = Field(None, description="Additional context about the error")


class StandardErrorResponse(BaseModel):
    """Standardized error response for all endpoints"""
    status: str = Field("error", description="Status indicator")
    timestamp: str = Field(..., description="ISO8601 timestamp of error")
    error: ErrorDetail = Field(..., description="Error details")


class EPGResponse(BaseModel):
    """EPG data response"""
    timestamp: str
    timezone: str = Field(..., description="Timezone used for all timestamps in response")
    channels_requested: int
    channels_found: int
    total_programs: int
    epg: dict[str, list[ProgramResponse]] = Field(..., description="EPG data grouped by xmltv_id")
