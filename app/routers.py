from typing import Annotated
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.epg_fetch import fetch_and_process
from app.services.epg_query import get_epg_data
from app.services.stats import collect_stats
from app.services.scheduler import epg_scheduler
from app.db.repository import SqlAlchemyEpgRepository
from app.db.session import get_db, get_session_factory
from app.schemas import (
    EPGRequest,
    EPGResponse,
    HealthResponse,
    ServiceInfoResponse,
    StatsResponse,
)
from app.utils.timezone import to_utc_iso8601_z


logger = logging.getLogger(__name__)

main_router = APIRouter()


async def _get_repo(db: AsyncSession = Depends(get_db)) -> SqlAlchemyEpgRepository:
    return SqlAlchemyEpgRepository(db)

@main_router.get(
    "/",
    response_model=ServiceInfoResponse,
    summary="Service Info",
    description="Return service metadata and current endpoint map.",
)
async def root() -> ServiceInfoResponse:
    next_run = epg_scheduler.get_next_run_time()

    return ServiceInfoResponse(
        service="EPG Service",
        version="0.1.0",
        next_scheduled_fetch=next_run.isoformat() if next_run else None,
        endpoints={
            "fetch": "/fetch - Manually trigger EPG fetch",
            "epg": "/epg - Get EPG for multiple channels (POST)",
            "health": "/health - Health check",
            "stats": "/stats - Service stats",
        },
    )


@main_router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service Health",
    description="Return current service health for client-facing API availability.",
)
async def health_check() -> HealthResponse:
    """Service health endpoint."""
    status = "up"
    try:
        # Health reflects service ability to serve API requests.
        session_factory = get_session_factory()
        async with session_factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        logger.error("Failed to evaluate health status", exc_info=True)
        status = "down"

    return HealthResponse(
        status=status,
        service="epg-service",
        time=to_utc_iso8601_z(datetime.now(timezone.utc)),
    )


@main_router.get(
    "/stats",
    response_model=StatsResponse,
    summary="Service Stats",
    description="Return latest EPG ingestion statistics and next scheduled update time.",
    responses={
        500: {
            "description": "Stats computation failed.",
            "model": StatsResponse,
        }
    },
)
async def stats() -> StatsResponse | JSONResponse:
    """Service stats endpoint."""
    try:
        session_factory = get_session_factory()
        async with session_factory() as session:
            payload = await collect_stats(session)
        return StatsResponse(**payload)
    except Exception as exc:
        logger.error("Failed to compute stats: %s", exc, exc_info=True)
        next_run = epg_scheduler.get_next_run_time()
        error_payload = StatsResponse(
            checked_at=to_utc_iso8601_z(datetime.now(timezone.utc)),
            next_epg_update_at=to_utc_iso8601_z(next_run) if next_run else None,
            last_epg_update_at=None,
            sources_total=len(settings.epg_sources or []),
            last_updated_channels_count=None,
            error="Failed to compute stats.",
        )
        return JSONResponse(
            status_code=500,
            content=error_payload.model_dump(),
        )


@main_router.post(
    "/fetch",
    summary="Trigger Fetch",
    description="Manually trigger an EPG fetch/import cycle from configured sources.",
)
async def trigger_fetch() -> dict:
    """
    Manually trigger EPG fetch from source

    This will download, parse and store EPG data
    """
    logger.info("Manual EPG fetch triggered via API")
    result = await fetch_and_process()

    if "error" in result:
        raise HTTPException(status_code=500, detail=result["error"])

    return result


@main_router.post(
    "/epg",
    response_model=EPGResponse,
    summary="Get EPG",
    description="Return EPG programs for requested channels and date window.",
)
async def get_epg(
    request: EPGRequest,
    repo: Annotated[SqlAlchemyEpgRepository, Depends(_get_repo)]
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    Args:
        request: EPG request with channels, update mode, timezone, and optional date range

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
    return await get_epg_data(repo, request)
