import aiosqlite
import os
from typing import AsyncGenerator
from dotenv import load_dotenv

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/epg.db")


async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """Database dependency for FastAPI"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        yield db


async def init_db() -> None:
    """Initialize database schema"""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        # Enable WAL mode for better concurrency
        await db.execute("PRAGMA journal_mode = WAL")
        await db.execute("PRAGMA cache_size = -64000")

        # Create channels table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                xmltv_id TEXT PRIMARY KEY,
                display_name TEXT NOT NULL,
                icon_url TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create programs table
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

        # Create index for fast queries
        await db.execute("""
            CREATE INDEX IF NOT EXISTS idx_programs_channel_time
            ON programs(xmltv_channel_id, start_time)
        """)

        await db.commit()
        print("✓ Database initialized")
