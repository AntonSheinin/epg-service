from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    database_path: str = "./data/epg.db"
    epg_source_url: str | None = None
    log_level: str = "INFO"
    epg_fetch_cron: str = "0 3 * * *"  # Daily at 3 AM

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )


settings = Settings()
