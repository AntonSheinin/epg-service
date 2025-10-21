from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
import logging


class CustomSettings(BaseSettings):
    """Application settings loaded from environment variables"""

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


settings = CustomSettings()


def setup_logging() -> None:
    """Configure application logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
