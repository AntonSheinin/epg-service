import logging

from croniter import croniter
from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


logger = logging.getLogger(__name__)


class CustomSettings(BaseSettings):
    """Application settings loaded from environment variables.

    Validates configuration at startup to catch misconfiguration early.
    """

    database_url: str
    epg_sources: list[str] | None = None
    epg_fetch_cron: str = "0 3 * * *"  # Daily at 3 AM
    epg_fetch_misfire_grace_sec: int = 3600  # Allow 1 hour to run missed jobs
    epg_channels_chunk_size: int = 1000
    epg_programs_chunk_size: int = 4000
    max_epg_depth: int = 14  # Days to keep past programs (archive)
    max_future_epg_limit: int = 7  # Days to keep future epg
    epg_parse_timeout_sec: int = 600  # XML parsing timeout, 0 disables timeout

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

    @field_validator("epg_sources", mode="before")
    @classmethod
    def parse_epg_sources(cls, value):
        """Parse comma-separated URLs or list."""
        if value is None:
            return []
        if isinstance(value, str):
            if not value.strip():
                return []
            return [url.strip() for url in value.split(",") if url.strip()]
        if isinstance(value, list):
            return value
        return []

    @field_validator("epg_sources", mode="after")
    @classmethod
    def validate_epg_sources(cls, value):
        """Validate EPG source URLs are HTTP/HTTPS."""
        if not value:
            return value

        for url in value:
            if not url.lower().startswith(("http://", "https://")):
                raise ValueError(f"EPG source URL must be HTTP/HTTPS: {url}")
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
        """Validate cron expression is valid."""
        try:
            croniter(value)
            return value
        except (ValueError, KeyError) as exc:
            raise ValueError(f"Invalid cron expression '{value}': {exc}") from exc

    @model_validator(mode="after")
    def validate_epg_configuration(self):
        """Validate cross-field configuration."""
        if not self.epg_sources:
            logger.warning(
                "No EPG sources configured - EPG fetch will not retrieve any data"
            )

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
        logger.info("  EPG Sources: %s configured", len(self.epg_sources or []))
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


def setup_logging() -> None:
    """Configure application logging."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
