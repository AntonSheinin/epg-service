from typing import Annotated
import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, Query
import logging

from app.database import get_db
from app.schemas import EPGRequest, EPGResponse
from app.services import (
    get_all_channels,
    get_programs_in_range,
    get_epg_data,
    fetch_and_process,
    epg_scheduler
)


logger = logging.getLogger(__name__)

main_router = APIRouter()

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
            "channels": "/channels - Get all channels",
            "programs": "/programs - Get programs (query params: start_from, start_to)",
            "epg": "/epg - Get EPG for multiple channels with individual time windows (POST)",
            "health": "/health - Health check"
        }
    }


@main_router.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    next_run = epg_scheduler.get_next_run_time()
    return {
        "status": "ok",
        "scheduler_running": epg_scheduler.scheduler.running,
        "next_fetch": next_run.isoformat() if next_run else None
    }


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


@main_router.get("/channels")
async def get_channels(
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> dict:
    """Get all channels"""
    return await get_all_channels(db)


@main_router.get("/programs")
async def get_programs(
    start_from: Annotated[str, Query(description="ISO8601 datetime, e.g. 2025-10-09T00:00:00Z")],
    start_to: Annotated[str, Query(description="ISO8601 datetime, e.g. 2025-10-10T00:00:00Z")],
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> dict:
    """
    Get all programs in time range

    Args:
        start_from: ISO8601 datetime (e.g. 2025-10-09T00:00:00Z)
        start_to: ISO8601 datetime (e.g. 2025-10-10T00:00:00Z)

    Returns:
        Dictionary with count and list of programs
    """
    return await get_programs_in_range(db, start_from, start_to)

@main_router.post("/epg", response_model=EPGResponse)
async def get_epg(
    request: EPGRequest,
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    Args:
        request: EPG request with channels, update mode, and timezone

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
    return await get_epg_data(db, request)
