import aiosqlite
import logging
from typing import AsyncGenerator

from app.config import settings, setup_logging

setup_logging()
logger = logging.getLogger(__name__)


async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """Database dependency for FastAPI"""
    async with aiosqlite.connect(settings.database_path) as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db() -> None:
    """Initialize database schema"""
    logger.info(f"Initializing database at {settings.database_path}")

    async with aiosqlite.connect(settings.database_path) as db:
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA cache_size = -64000")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                xmltv_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                icon_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS programs (
                id TEXT PRIMARY KEY,
                xmltv_channel_id TEXT NOT NULL,
                start_time TEXT NOT NULL,
                stop_time TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(xmltv_channel_id, start_time, title)
            )
        """)

        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_programs_channel_time
            ON programs(xmltv_channel_id, start_time)
        """)

        await db.commit()
        logger.info("Database initialized successfully")
