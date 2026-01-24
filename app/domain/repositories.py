"""
Repository interfaces for domain access.
"""
from __future__ import annotations

from datetime import datetime
from typing import Protocol, Sequence

from app.domain.entities import Channel, Program


class EpgRepository(Protocol):
    """Abstraction for EPG persistence."""

    async def delete_old_programs(self, cutoff_time: datetime) -> int:
        ...

    async def delete_future_programs(
        self,
        cutoff_time: datetime,
        *,
        inclusive: bool = False,
    ) -> int:
        ...

    async def upsert_channels(self, channels: Sequence[Channel]) -> None:
        ...

    async def upsert_programs(self, programs: Sequence[Program]) -> int:
        ...

    async def list_programs_for_channel(
        self,
        channel_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Program]:
        ...


__all__ = ["EpgRepository"]
