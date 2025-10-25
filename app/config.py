from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path
import logging
from croniter import croniter

logger = logging.getLogger(__name__)


class CustomSettings(BaseSettings):
    """Application settings loaded from environment variables

    Validates configuration at startup to catch misconfiguration early.
    """

    database_path: str = "./data/epg.db"
    epg_sources: list[str] | None = None
    epg_fetch_cron: str = "0 3 * * *"  # Daily at 3 AM
    max_epg_depth: int = 14  # Days to keep past programs (archive)
    max_future_epg_limit: int = 7 # Days to keep future epg

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    @field_validator('epg_sources', mode='before')
    @classmethod
    def parse_epg_sources(cls, v):
        """Parse comma-separated URLs or list"""
        if v is None:
            return []
        if isinstance(v, str):
            if not v.strip():
                return []
            return [url.strip() for url in v.split(',') if url.strip()]
        if isinstance(v, list):
            return v
        return []

    @field_validator('epg_sources', mode='after')
    @classmethod
    def validate_epg_sources(cls, v):
        """Validate EPG source URLs are HTTP/HTTPS"""
        if not v:
            return v

        for url in v:
            if not url.lower().startswith(('http://', 'https://')):
                raise ValueError(f"EPG source URL must be HTTP/HTTPS: {url}")
        return v

    @field_validator('database_path')
    @classmethod
    def validate_database_path(cls, v: str) -> str:
        """Validate database path is accessible"""
        path = Path(v)
        try:
            # Ensure directory exists
            path.parent.mkdir(parents=True, exist_ok=True)
            return v
        except (OSError, PermissionError) as e:
            raise ValueError(f"Cannot access database path '{v}': {e}")

    @field_validator('max_epg_depth', 'max_future_epg_limit')
    @classmethod
    def validate_day_ranges(cls, v: int, info) -> int:
        """Validate day range values are positive and reasonable"""
        if v < 0:
            raise ValueError(f"{info.field_name} must be >= 0")
        if v > 365:
            raise ValueError(f"{info.field_name} must be <= 365 days")
        return v

    @field_validator('epg_fetch_cron')
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        """Validate cron expression is valid"""
        try:
            croniter(v)
            return v
        except (ValueError, KeyError) as e:
            raise ValueError(f"Invalid cron expression '{v}': {e}")

    @model_validator(mode='after')
    def validate_epg_configuration(self):
        """Validate cross-field configuration"""
        if not self.epg_sources:
            logger.warning("No EPG sources configured - EPG fetch will not retrieve any data")

        if self.max_epg_depth == 0 and self.max_future_epg_limit == 0:
            raise ValueError("At least one of max_epg_depth or max_future_epg_limit must be > 0")

        return self

    def __init__(self, **data):
        """Initialize settings and log configuration"""
        super().__init__(**data)

        # Log configuration at startup (without sensitive URLs)
        logger.info("Configuration loaded:")
        logger.info(f"  Database: {self.database_path}")
        logger.info(f"  EPG Sources: {len(self.epg_sources or [])} configured")
        logger.info(f"  Fetch Schedule: {self.epg_fetch_cron}")
        logger.info(f"  Archive Depth: {self.max_epg_depth} days")
        logger.info(f"  Future Limit: {self.max_future_epg_limit} days")


settings = CustomSettings()


def setup_logging() -> None:
    """Configure application logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
