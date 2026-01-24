"""
SQLAlchemy repository implementations.
"""
from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime, timezone
from time import perf_counter

from sqlalchemy import delete, func, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.domain.entities import Channel, Program
from app.domain.repositories import EpgRepository
from app.infrastructure.db.models import ChannelRecord, ProgramRecord

logger = logging.getLogger(__name__)


_POSTGRES_MAX_PARAMS = 32767
_PROGRAM_INSERT_COLUMNS = (
    "id",
    "xmltv_channel_id",
    "start_time",
    "stop_time",
    "title",
    "description",
    "created_at",
)


def _chunked(items: Sequence, size: int) -> Sequence[Sequence]:
    """Yield list slices for batch processing."""
    for start_index in range(0, len(items), size):
        yield items[start_index:start_index + size]


class SqlAlchemyEpgRepository(EpgRepository):
    """SQLAlchemy-backed repository implementation."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def delete_old_programs(self, cutoff_time: datetime) -> int:
        result = await self._session.execute(
            select(func.count(ProgramRecord.id)).where(ProgramRecord.start_time < cutoff_time)
        )
        deleted_count = result.scalar_one_or_none() or 0

        stmt = delete(ProgramRecord).where(ProgramRecord.start_time < cutoff_time)
        await self._session.execute(stmt)

        logger.info(
            "Deleted %s old programs (start_time < %s)",
            deleted_count,
            cutoff_time.date(),
        )
        return deleted_count

    async def delete_future_programs(
        self,
        cutoff_time: datetime,
        *,
        inclusive: bool = False,
    ) -> int:
        if inclusive:
            condition = ProgramRecord.start_time >= cutoff_time
            operator_str = ">="
        else:
            condition = ProgramRecord.start_time > cutoff_time
            operator_str = ">"

        result = await self._session.execute(
            select(func.count(ProgramRecord.id)).where(condition)
        )
        deleted_count = result.scalar_one_or_none() or 0

        stmt = delete(ProgramRecord).where(condition)
        await self._session.execute(stmt)

        logger.info(
            "Deleted %s future programs (start_time %s %s)",
            deleted_count,
            operator_str,
            cutoff_time.date(),
        )
        return deleted_count

    async def upsert_channels(self, channels: Sequence[Channel]) -> None:
        channel_list = list(channels)
        if not channel_list:
            logger.debug("No channels to store")
            return

        deduped_channels: dict[str, Channel] = {
            channel.xmltv_id: channel for channel in channel_list
        }

        logger.info("Storing %s channels", len(deduped_channels))

        channel_ids = list(deduped_channels.keys())
        chunk_size = settings.epg_channels_chunk_size
        now = datetime.now(timezone.utc)

        for chunk_number, chunk_ids in enumerate(_chunked(channel_ids, chunk_size), start=1):
            result = await self._session.execute(
                select(ChannelRecord).where(ChannelRecord.xmltv_id.in_(chunk_ids))
            )
            existing_by_id = {row.xmltv_id: row for row in result.scalars()}

            new_rows: list[ChannelRecord] = []
            updated_count = 0

            for channel_id in chunk_ids:
                payload = deduped_channels[channel_id]
                existing = existing_by_id.get(channel_id)
                if existing:
                    updated = False
                    if payload.display_name != existing.display_name:
                        existing.display_name = payload.display_name
                        updated = True
                    if payload.icon_url is not None and payload.icon_url != existing.icon_url:
                        existing.icon_url = payload.icon_url
                        updated = True
                    if updated:
                        updated_count += 1
                    continue

                new_rows.append(
                    ChannelRecord(
                        xmltv_id=payload.xmltv_id,
                        display_name=payload.display_name,
                        icon_url=payload.icon_url,
                        created_at=now,
                    )
                )

            if new_rows:
                self._session.add_all(new_rows)

            logger.debug(
                "Channel chunk %s persisted: payload=%s, inserted=%s, updated=%s",
                chunk_number,
                len(chunk_ids),
                len(new_rows),
                updated_count,
            )

    async def upsert_programs(self, programs: Sequence[Program]) -> int:
        program_list = list(programs)
        if not program_list:
            logger.debug("No programs to store")
            return 0

        total_programs = len(program_list)
        deduped = {program.id: program for program in program_list}
        deduped_programs = list(deduped.values())
        duplicates = total_programs - len(deduped_programs)
        if duplicates:
            logger.info("Deduplicated %s duplicate program(s) in payload", duplicates)

        logger.info("Storing %s programs", len(deduped_programs))

        chunk_size = settings.epg_programs_chunk_size
        max_rows_per_stmt = max(1, _POSTGRES_MAX_PARAMS // len(_PROGRAM_INSERT_COLUMNS))
        effective_chunk_size = min(chunk_size, max_rows_per_stmt)
        if chunk_size > max_rows_per_stmt:
            logger.warning(
                "Program chunk size %s exceeds asyncpg parameter limit; using %s",
                chunk_size,
                max_rows_per_stmt,
            )
        upserted_count = 0

        await self._session.flush()

        for chunk_number, chunk in enumerate(
            _chunked(deduped_programs, effective_chunk_size),
            start=1,
        ):
            loop_start = perf_counter()
            now = datetime.now(timezone.utc)

            rows = []
            for program in chunk:
                created_at = program.created_at or now
                rows.append(
                    {
                        "id": program.id,
                        "xmltv_channel_id": program.xmltv_channel_id,
                        "start_time": program.start_time,
                        "stop_time": program.stop_time,
                        "title": program.title,
                        "description": program.description,
                        "created_at": created_at,
                    }
                )

            if not rows:
                continue

            stmt = pg_insert(ProgramRecord).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=[ProgramRecord.id],
                set_={
                    "stop_time": stmt.excluded.stop_time,
                    "description": stmt.excluded.description,
                },
                where=or_(
                    ProgramRecord.stop_time.is_distinct_from(stmt.excluded.stop_time),
                    ProgramRecord.description.is_distinct_from(stmt.excluded.description),
                ),
            )

            result = await self._session.execute(stmt)
            await self._session.commit()

            chunk_affected = result.rowcount
            if chunk_affected is None or chunk_affected < 0:
                chunk_affected = len(rows)
            upserted_count += chunk_affected

            total_duration = perf_counter() - loop_start
            logger.info(
                "Chunk %s upserted: payload=%s, affected=%s, total_time=%.2fs",
                chunk_number,
                len(chunk),
                chunk_affected,
                total_duration,
            )

        logger.info(
            "Program store complete: %s upserted",
            upserted_count,
        )

        return upserted_count

    async def list_programs_for_channel(
        self,
        channel_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Program]:
        stmt = (
            select(ProgramRecord)
            .where(
                ProgramRecord.xmltv_channel_id == channel_id,
                ProgramRecord.start_time < end_time,
                ProgramRecord.stop_time > start_time,
            )
            .order_by(ProgramRecord.start_time)
        )

        result = await self._session.execute(stmt)
        rows = list(result.scalars().all())
        return [
            Program(
                id=row.id,
                xmltv_channel_id=row.xmltv_channel_id,
                start_time=row.start_time,
                stop_time=row.stop_time,
                title=row.title,
                description=row.description,
                created_at=row.created_at,
            )
            for row in rows
        ]

__all__ = ["SqlAlchemyEpgRepository"]
