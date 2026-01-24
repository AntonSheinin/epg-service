"""
FastAPI dependencies for database access.
"""
from __future__ import annotations

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.repositories import EpgRepository
from app.infrastructure.db.repository import SqlAlchemyEpgRepository
from app.infrastructure.db.session import get_db


async def get_epg_repository(
    db: AsyncSession = Depends(get_db),
) -> EpgRepository:
    """Provide a repository instance per request."""
    return SqlAlchemyEpgRepository(db)


__all__ = ["get_epg_repository"]
