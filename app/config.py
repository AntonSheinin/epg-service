import logging
import tomllib
from pathlib import Path
from typing import Annotated

from apscheduler.triggers.cron import CronTrigger
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


class EpgSourceConfig(BaseModel):
    """Validated configuration for one XMLTV source."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        allow_inf_nan=False,
    )

    name: str = Field(min_length=1, max_length=100)
    url: AnyHttpUrl
    time_offset_hours: Annotated[StrictFloat, Field(ge=-24, le=24)] = 0.0

    @field_validator("name", mode="before")
    @classmethod
    def normalize_name(cls, value: object) -> object:
        """Trim source names before applying length validation."""
        return value.strip() if isinstance(value, str) else value


class EpgSourcesConfig(BaseModel):
    """Top-level EPG source file schema."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    sources: tuple[EpgSourceConfig, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_unique_sources(self) -> "EpgSourcesConfig":
        """Reject duplicate source names and normalized URLs."""
        names = [source.name.casefold() for source in self.sources]
        if len(names) != len(set(names)):
            raise ValueError("EPG source names must be unique")

        urls = [str(source.url) for source in self.sources]
        if len(urls) != len(set(urls)):
            raise ValueError("EPG source URLs must be unique")

        return self


def _resolve_epg_sources_path(configured_path: Path) -> Path:
    """Resolve relative source paths from the project root."""
    if configured_path.is_absolute():
        return configured_path
    return (PROJECT_ROOT / configured_path).resolve()


def _format_source_validation_error(exc: ValidationError) -> str:
    """Format validation errors without exposing URL input values."""
    messages = []
    for error in exc.errors(include_input=False, include_url=False):
        location = ".".join(str(part) for part in error["loc"])
        messages.append(f"{location}: {error['msg']}")
    return "; ".join(messages)


def load_epg_sources(configured_path: Path) -> EpgSourcesConfig:
    """Load and validate the TOML source configuration."""
    source_path = _resolve_epg_sources_path(configured_path)
    try:
        with source_path.open("rb") as config_file:
            payload = tomllib.load(config_file)
    except FileNotFoundError:
        raise ValueError(f"EPG source configuration not found: {source_path}") from None
    except OSError:
        raise ValueError(f"Unable to read EPG source configuration: {source_path}") from None
    except tomllib.TOMLDecodeError:
        raise ValueError("EPG source configuration contains invalid TOML") from None

    try:
        return EpgSourcesConfig.model_validate(payload)
    except ValidationError as exc:
        details = _format_source_validation_error(exc)
        raise ValueError(f"Invalid EPG source configuration: {details}") from None


class CustomSettings(BaseSettings):
    """Application settings loaded from environment variables.

    Validates configuration at startup to catch misconfiguration early.
    """

    database_url: str
    log_level: str = "INFO"
    epg_sources_file: Path = Path("config/epg_sources.toml")
    epg_fetch_cron: str = "0 3 * * *"  # Daily at 3 AM
    epg_fetch_misfire_grace_sec: int = 3600  # Allow 1 hour to run missed jobs
    epg_channels_chunk_size: int = 1000
    epg_programs_chunk_size: int = 4000
    max_epg_depth: int = 14  # Days to keep past programs (archive)
    max_future_epg_limit: int = 7  # Days to keep future epg
    epg_parse_timeout_sec: int = 600  # Cumulative XML parsing seconds; 0 disables timeout

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        """Ensure database URL is present."""
        if not value or not value.strip():
            raise ValueError("DATABASE_URL must be set")
        return value

    @field_validator("max_epg_depth", "max_future_epg_limit")
    @classmethod
    def validate_day_ranges(cls, value: int, info) -> int:
        """Validate day range values are positive and reasonable."""
        if value < 0:
            raise ValueError(f"{info.field_name} must be >= 0")
        if value > 365:
            raise ValueError(f"{info.field_name} must be <= 365 days")
        return value

    @field_validator("epg_parse_timeout_sec")
    @classmethod
    def validate_parse_timeout(cls, value: int) -> int:
        """Validate XML parsing timeout (seconds)."""
        if value < 0:
            raise ValueError("epg_parse_timeout_sec must be >= 0")
        return value

    @field_validator("epg_fetch_misfire_grace_sec")
    @classmethod
    def validate_misfire_grace(cls, value: int) -> int:
        """Validate scheduler misfire grace period (seconds)."""
        if value < 0:
            raise ValueError("epg_fetch_misfire_grace_sec must be >= 0")
        return value

    @field_validator("epg_channels_chunk_size", "epg_programs_chunk_size")
    @classmethod
    def validate_chunk_sizes(cls, value: int, info) -> int:
        """Ensure chunk sizes are positive integers."""
        if value <= 0:
            raise ValueError(f"{info.field_name} must be > 0")
        return value

    @field_validator("epg_fetch_cron")
    @classmethod
    def validate_cron_expression(cls, value: str) -> str:
        """Validate cron expression is valid for APScheduler."""
        try:
            CronTrigger.from_crontab(value)
            return value
        except (ValueError, KeyError) as exc:
            raise ValueError(f"Invalid cron expression '{value}': {exc}") from exc

    @model_validator(mode="after")
    def validate_epg_configuration(self):
        """Validate cross-field configuration."""
        if self.max_epg_depth == 0 and self.max_future_epg_limit == 0:
            raise ValueError(
                "At least one of max_epg_depth or max_future_epg_limit must be > 0"
            )

        return self

    def __init__(self, **data):
        """Initialize settings and log configuration."""
        super().__init__(**data)

        logger.info("Configuration loaded:")
        logger.info("  Database URL configured")
        logger.info("  EPG Sources File: %s", self.epg_sources_file)
        logger.info("  Fetch Schedule: %s", self.epg_fetch_cron)
        logger.info("  Fetch Misfire Grace: %ss", self.epg_fetch_misfire_grace_sec)
        logger.info("  Channel Batch Size: %s", self.epg_channels_chunk_size)
        logger.info("  Program Batch Size: %s", self.epg_programs_chunk_size)
        logger.info("  Archive Depth: %s days", self.max_epg_depth)
        logger.info("  Future Limit: %s days", self.max_future_epg_limit)
        logger.info(
            "  Parse Timeout: %s seconds",
            self.epg_parse_timeout_sec or "disabled",
        )


settings = CustomSettings()
epg_sources_config = load_epg_sources(settings.epg_sources_file)


def setup_logging() -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # HTTP client request logs can contain token-bearing EPG source URLs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
