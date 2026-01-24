"""
Unit of Work interface for coordinating persistence.
"""
from __future__ import annotations

from typing import Protocol

from app.domain.repositories import EpgRepository


class EpgUnitOfWork(Protocol):
    """Unit of Work boundary for EPG operations."""

    repo: EpgRepository

    async def __aenter__(self) -> "EpgUnitOfWork":
        ...

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        ...


__all__ = ["EpgUnitOfWork"]
