from pydantic import BaseModel, Field, field_validator, model_validator
from zoneinfo import ZoneInfo
from datetime import datetime


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
        except Exception:
            raise ValueError(f"Invalid timezone: {v}. Must be a valid IANA timezone (e.g., 'Europe/London', 'America/New_York') or 'UTC'")

    @field_validator('from_date', 'to_date')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Validate ISO8601 datetime format"""
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
        """Validate that from_date is before to_date"""
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
    timestamp: str
    timezone: str = Field(..., description="Timezone used for all timestamps in response")
    channels_requested: int
    channels_found: int
    total_programs: int
    epg: dict[str, list[ProgramResponse]] = Field(..., description="EPG data grouped by xmltv_id")
