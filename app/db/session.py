import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings

logger = logging.getLogger(__name__)

# Engine and session factory - initialized in init_db() during startup
_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def _resolve_async_database_url(raw_url: str) -> str:
    """Ensure the database URL points to an async-capable driver when needed."""
    try:
        url = make_url(raw_url)
    except ArgumentError:
        return raw_url

    drivername = url.drivername
    if "+" in drivername:
        return raw_url

    if drivername in {"postgresql", "postgres"}:
        url = url.set(drivername="postgresql+asyncpg")
        return url.render_as_string(hide_password=False)

    return raw_url


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Database dependency for FastAPI."""
    session_factory = get_session_factory()

    async with session_factory() as session:
        yield session


def _create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create async session factory from engine."""
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    """Get the session factory (initialized in init_db)."""
    global _session_factory
    if _session_factory is None:
        raise RuntimeError("Database not initialized. Call init_db() during startup.")
    return _session_factory


async def init_db() -> None:
    """Initialize database engine and session factory."""
    global _engine, _session_factory

    resolved_url = _resolve_async_database_url(settings.database_url)
    try:
        safe_url = make_url(resolved_url).render_as_string(hide_password=True)
    except ArgumentError:
        safe_url = "invalid database url"

    logger.info("Initializing database at %s", safe_url)

    _engine = create_async_engine(
        resolved_url,
        echo=False,
        future=True,
        pool_pre_ping=True,
    )

    _session_factory = _create_session_factory(_engine)

    logger.info("Database initialized successfully")


async def close_db() -> None:
    """Close database connections on shutdown."""
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
