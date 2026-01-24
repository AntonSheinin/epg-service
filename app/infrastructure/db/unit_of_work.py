"""
SQLAlchemy-backed unit of work.
"""
from __future__ import annotations

from app.domain.unit_of_work import EpgUnitOfWork
from app.infrastructure.db.repository import SqlAlchemyEpgRepository
from app.infrastructure.db.session import session_scope


class SqlAlchemyUnitOfWork(EpgUnitOfWork):
    """Unit of work implementation using SQLAlchemy sessions."""

    def __init__(self, *, begin: bool = True) -> None:
        self._begin = begin
        self._session_cm = None
        self.repo = None

    async def __aenter__(self) -> "SqlAlchemyUnitOfWork":
        self._session_cm = session_scope(begin=self._begin)
        session = await self._session_cm.__aenter__()
        self.repo = SqlAlchemyEpgRepository(session)
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        if self._session_cm is not None:
            await self._session_cm.__aexit__(exc_type, exc, traceback)
