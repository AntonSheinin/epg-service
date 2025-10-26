import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import event

from app.config import settings
from app.models import Base

logger = logging.getLogger(__name__)

# Engine and session factory - initialized in init_db() during startup
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database dependency for FastAPI"""
    session_factory = get_session_factory()

    async with session_factory() as session:
        yield session


def _create_session_factory(engine):
    """Create async session factory from engine"""
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get the session factory (initialized in init_db)"""
    global _session_factory
    if _session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() during startup.")
    return _session_factory


async def init_db() -> None:
    """Initialize database schema and engine"""
    global _engine, _session_factory

    logger.info(f"Initializing database at {settings.database_path}")

    # Create engine
    _engine = create_async_engine(
        f"sqlite+aiosqlite:///{settings.database_path}",
        echo=False,
        future=True,
        pool_pre_ping=True,
        connect_args={"timeout": 30, "check_same_thread": False},
    )

    # Configure SQLite pragmas for better performance
    def configure_sqlite(dbapi_conn, _):
        """Configure SQLite connection parameters"""
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA journal_mode = WAL")
        # Set cache size to 64MB for better performance on larger datasets
        cursor.execute("PRAGMA cache_size = -64000")
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.close()

    # Register the event listener
    event.listen(_engine.sync_engine, "connect", configure_sqlite)

    # Create all tables
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Create session factory
    _session_factory = _create_session_factory(_engine)

    logger.info("Database initialized successfully")


async def close_db() -> None:
    """Close database connections on shutdown"""
    global _engine
    if _engine:
        await _engine.dispose()
        logger.info("Database connections closed")


@asynccontextmanager
async def session_scope(*, begin: bool = True) -> AsyncIterator[AsyncSession]:
    """
    Provide an async session context manager with optional automatic transaction handling.

    Args:
        begin: When True (default), wrap the session in `session.begin()` for auto commit/rollback.
               When False, caller is responsible for transaction demarcation and commit/rollback.
    """
    session_factory = get_session_factory()

    async with session_factory() as session:
        if begin:
            async with session.begin():
                yield session
        else:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            else:
                await session.commit()
