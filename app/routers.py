from typing import Annotated
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services.epg_fetch import fetch_and_process
from app.services.epg_query import get_epg_data
from app.services.stats import collect_stats
from app.services.scheduler import epg_scheduler
from app.db.repository import SqlAlchemyEpgRepository
from app.db.session import get_db, get_session_factory
from app.schemas import (
    DashboardStatsResponse,
    EPGRequest,
    EPGResponse,
    HealthResponse,
)
from app.utils.timezone import to_utc_iso8601_z


logger = logging.getLogger(__name__)

main_router = APIRouter()
api_v1_router = APIRouter(prefix="/api/v1")


async def _get_repo(db: AsyncSession = Depends(get_db)) -> SqlAlchemyEpgRepository:
    return SqlAlchemyEpgRepository(db)

@main_router.get("/")
async def root() -> dict:
    """Root endpoint with service information"""
    next_run = epg_scheduler.get_next_run_time()

    return {
        "service": "EPG Service",
        "version": "0.1.0",
        "next_scheduled_fetch": next_run.isoformat() if next_run else None,
        "endpoints": {
            "fetch": "/fetch - Manually trigger EPG fetch",
            "epg": "/epg - Get EPG for multiple channels (POST)",
            "health": "/api/v1/health - Health check",
            "dashboard_stats": "/api/v1/dashboard/stats - Dashboard stats"
        }
    }


@api_v1_router.get("/health", response_model=HealthResponse)
async def health_check_v1() -> HealthResponse:
    """Versioned health endpoint for dashboard consumption."""
    status: str = "up"
    try:
        scheduler_running = bool(epg_scheduler.scheduler and epg_scheduler.scheduler.running)
        sources_configured = len(settings.epg_sources or []) > 0
        if not scheduler_running or not sources_configured:
            status = "degraded"
    except Exception:
        logger.error("Failed to evaluate health status", exc_info=True)
        status = "down"

    return HealthResponse(
        status=status,
        service="epg-service",
        time=to_utc_iso8601_z(datetime.now(timezone.utc)),
    )


@api_v1_router.get("/dashboard/stats", response_model=DashboardStatsResponse)
async def dashboard_stats() -> DashboardStatsResponse | JSONResponse:
    """Dashboard summary endpoint."""
    try:
        session_factory = get_session_factory()
        async with session_factory() as session:
            payload = await collect_stats(session)
        return DashboardStatsResponse(**payload)
    except Exception as exc:
        logger.error("Failed to compute dashboard stats: %s", exc, exc_info=True)
        error_payload = DashboardStatsResponse(
            health="down",
            checked_at=to_utc_iso8601_z(datetime.now(timezone.utc)),
            last_epg_update_at=None,
            sources_total=len(settings.epg_sources or []),
            last_channels_update_at=None,
            channels_updated_total=None,
            error="Failed to compute dashboard stats.",
        )
        return JSONResponse(
            status_code=500,
            content=error_payload.model_dump(),
        )


@main_router.post("/fetch")
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


@main_router.post("/epg", response_model=EPGResponse)
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
