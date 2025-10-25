from typing import Annotated
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import APIRouter, Depends, HTTPException
import logging

from app.database import get_db
from app.schemas import EPGRequest, EPGResponse
from app.services import (
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
            "epg": "/epg - Get EPG for multiple channels (POST)",
            "health": "/health - Health check"
        }
    }


@main_router.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    next_run = epg_scheduler.get_next_run_time()
    return {
        "status": "ok",
        "scheduler_running": epg_scheduler.scheduler.running if epg_scheduler.scheduler else False,
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


@main_router.post("/epg", response_model=EPGResponse)
async def get_epg(
    request: EPGRequest,
    db: Annotated[AsyncSession, Depends(get_db)]
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    Args:
        request: EPG request with channels, update mode, timezone, and optional date range

    Returns:
        EPG data grouped by channel xmltv_id with timestamps in requested timezone
    """
    return await get_epg_data(db, request)
