from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from zoneinfo import ZoneInfo

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


class ProgramResponse(BaseModel):
    """Single program data"""
    id: str
    start_time: str
    stop_time: str
    title: str
    description: str | None


class EPGResponse(BaseModel):
    """EPG data response"""
    response_generated_at: str = Field(
        ...,
        description="Time when this response was generated, converted to the requested timezone.",
    )
    last_epg_update_at: str | None = Field(
        ...,
        description="Last successful EPG import/update time, converted to the requested timezone.",
    )
    timezone: str = Field(..., description="Timezone used for all timestamps in response")
    channels_requested: int
    channels_found: int
    total_programs: int
    epg: dict[str, list[ProgramResponse]] = Field(..., description="EPG data grouped by xmltv_id")


class HealthResponse(BaseModel):
    """Service health response."""

    status: Literal["up", "degraded", "down"] = Field(
        ...,
        description="Service health state.",
        examples=["up"],
    )
    service: str = Field(
        ...,
        description="Service identifier.",
        examples=["epg-service"],
    )
    time: str = Field(
        ...,
        description="Response timestamp in UTC ISO-8601 format.",
        examples=["2026-02-28T12:00:00Z"],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "status": "up",
                "service": "epg-service",
                "time": "2026-02-28T12:00:00Z",
            }
        }
    )


class StatsResponse(BaseModel):
    """Service stats response."""

    checked_at: str = Field(
        ...,
        description="Time when the stats payload was produced (UTC ISO-8601).",
        examples=["2026-02-28T12:00:00Z"],
    )
    next_epg_update_at: str | None = Field(
        ...,
        description="Next scheduled EPG fetch time (UTC ISO-8601), if scheduler is active.",
        examples=["2026-02-29T03:00:00Z"],
    )
    last_epg_update_at: str | None = Field(
        ...,
        description="Latest recorded global EPG import time (UTC ISO-8601).",
        examples=["2026-02-28T11:45:00Z"],
    )
    sources_total: int = Field(
        ...,
        description="Number of sources that recorded committed updates in the latest import cycle.",
        examples=[9],
    )
    last_updated_channels_count: int | None = Field(
        ...,
        description="Number of channels with actual EPG row inserts/updates in the latest recorded import cycle.",
        examples=[8432],
    )
    error: str | None = Field(
        ...,
        description="Stats warning/error details, if any.",
        examples=[None],
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "checked_at": "2026-02-28T12:00:00Z",
                "next_epg_update_at": "2026-02-29T03:00:00Z",
                "last_epg_update_at": "2026-02-28T11:45:00Z",
                "sources_total": 9,
                "last_updated_channels_count": 8432,
                "error": None,
            }
        }
    )


class ServiceInfoResponse(BaseModel):
    """Service root endpoint response."""

    service: str = Field(..., examples=["EPG Service"])
    version: str = Field(..., examples=["0.1.0"])
    next_scheduled_fetch: str | None = Field(
        ...,
        description="Next scheduler run time in ISO-8601 format, if available.",
        examples=["2026-02-29T03:00:00+00:00"],
    )
    endpoints: dict[str, str] = Field(
        ...,
        description="Available API endpoints.",
        examples=[
            {
                "fetch": "/fetch - Manually trigger EPG fetch",
                "epg": "/epg - Get EPG for multiple channels (POST)",
                "health": "/health - Health check",
                "stats": "/stats - Service stats",
            }
        ],
    )
