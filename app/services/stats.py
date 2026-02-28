"""
Service statistics collector.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.orm_models import ImportStatusRecord
from app.services.scheduler import epg_scheduler
from app.utils.timezone import to_utc_iso8601_z


async def collect_stats(session: AsyncSession) -> dict:
    """
    Collect service stats with a single lightweight query.
    """
    checked_at = datetime.now(timezone.utc)
    sources_total = len(settings.epg_sources or [])
    next_run = epg_scheduler.get_next_run_time()

    status_stmt = select(ImportStatusRecord).where(ImportStatusRecord.id == 1)
    status_row = (await session.execute(status_stmt)).scalar_one_or_none()

    last_epg_update_at = status_row.last_epg_update_at if status_row else None
    last_channels_update_at = status_row.last_channels_update_at if status_row else None
    last_updated_channels_count = status_row.last_updated_channels_count if status_row else None

    error: str | None = None
    if sources_total == 0:
        error = "No enabled EPG sources configured."
    elif last_epg_update_at is None:
        error = "No successful EPG import found yet."

    return {
        "checked_at": to_utc_iso8601_z(checked_at),
        "next_epg_update_at": to_utc_iso8601_z(next_run) if next_run else None,
        "last_epg_update_at": (
            to_utc_iso8601_z(last_epg_update_at) if last_epg_update_at else None
        ),
        "sources_total": sources_total,
        "last_channels_update_at": (
            to_utc_iso8601_z(last_channels_update_at) if last_channels_update_at else None
        ),
        "last_updated_channels_count": last_updated_channels_count,
        "error": error,
    }
