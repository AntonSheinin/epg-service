import logging
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import event

from app.config import settings
from app.models import Base

logger = logging.getLogger(__name__)

# Engine and session factory - initialized in init_db()
engine = None
AsyncSessionLocal = None


def _create_engine():
    """Create async engine for SQLite with aiosqlite driver"""
    global engine
    if engine is None:
        engine = create_async_engine(
            f"sqlite+aiosqlite:///{settings.database_path}",
            echo=False,
            future=True,
            pool_pre_ping=True,
            connect_args={"timeout": 30, "check_same_thread": False},
        )
    return engine


def _create_session_factory():
    """Create async session factory"""
    global AsyncSessionLocal
    if AsyncSessionLocal is None:
        eng = _create_engine()
        AsyncSessionLocal = async_sessionmaker(
            eng,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
            autocommit=False,
        )
    return AsyncSessionLocal


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database dependency for FastAPI"""
    factory = _create_session_factory()
    async with factory() as session:
        yield session


async def init_db() -> None:
    """Initialize database schema and engine"""
    logger.info(f"Initializing database at {settings.database_path}")

    # Create engine if not already created
    global engine
    eng = _create_engine()

    # Configure SQLite pragmas for better performance
    def configure_sqlite(dbapi_conn, _):
        """Configure SQLite connection parameters"""
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode = WAL")
        cursor.execute("PRAGMA cache_size = -64000")
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.close()

    # Register the event listener
    event.listen(eng.sync_engine, "connect", configure_sqlite)

    # Create all tables
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Ensure session factory is created
    _create_session_factory()

    logger.info("Database initialized successfully")
