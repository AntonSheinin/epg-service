from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    database_path: str = "./data/epg.db"
    epg_sources: list[str] = []
    log_level: str = "INFO"
    epg_fetch_cron: str = "0 3 * * *"  # Daily at 3 AM

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    @field_validator('epg_sources', mode='before')
    @classmethod
    def parse_epg_sources(cls, v):
        """Parse comma-separated URLs"""
        if isinstance(v, str):
            return [url.strip() for url in v.split(',') if url.strip()]
        return v


settings = Settings()
