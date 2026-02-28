"""
Dashboard statistics service.
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.orm_models import ChannelRecord, ProgramRecord
from app.services.scheduler import epg_scheduler
from app.utils.timezone import to_utc_iso8601_z


def _max_datetime(*values: datetime | None) -> datetime | None:
    normalized = [value for value in values if value is not None]
    if not normalized:
        return None
    return max(normalized)


async def collect_stats(session: AsyncSession) -> dict:
    """
    Collect dashboard stats with a single aggregated SQL query.
    """
    checked_at = datetime.now(timezone.utc)
    sources_total = len(settings.epg_sources or [])

    stmt = select(
        select(func.max(ProgramRecord.created_at)).scalar_subquery().label("last_program_update_at"),
        select(func.max(ChannelRecord.created_at)).scalar_subquery().label("last_channel_update_at"),
        select(func.count(ChannelRecord.xmltv_id)).scalar_subquery().label("channels_updated_total"),
    )
    row = (await session.execute(stmt)).one()

    last_program_update_at: datetime | None = row.last_program_update_at
    last_channel_update_at: datetime | None = row.last_channel_update_at
    channels_updated_total_raw = row.channels_updated_total

    last_epg_update_at = _max_datetime(last_program_update_at, last_channel_update_at)
    last_channels_update_at = _max_datetime(last_program_update_at, last_channel_update_at)

    health = "up"
    error: str | None = None
    scheduler_running = bool(epg_scheduler.scheduler and epg_scheduler.scheduler.running)

    if not scheduler_running:
        health = "degraded"
        error = "Scheduler is not running."
    if sources_total == 0:
        health = "degraded"
        error = "No enabled EPG sources configured."
    elif last_epg_update_at is None:
        health = "degraded"
        error = "No successful EPG import found yet."

    return {
        "health": health,
        "checked_at": to_utc_iso8601_z(checked_at),
        "last_epg_update_at": (
            to_utc_iso8601_z(last_epg_update_at) if last_epg_update_at else None
        ),
        "sources_total": sources_total,
        "last_channels_update_at": (
            to_utc_iso8601_z(last_channels_update_at) if last_channels_update_at else None
        ),
        "channels_updated_total": int(channels_updated_total_raw or 0),
        "error": error,
    }
