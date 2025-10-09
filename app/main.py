from contextlib import asynccontextmanager
from typing import Annotated
import logging

from fastapi import FastAPI, Depends, Query
from fastapi.responses import JSONResponse
import aiosqlite

from app.config import settings
from app.database import init_db, get_db
from app.epg_fetcher import fetch_and_process
from app.scheduler import epg_scheduler


def setup_logging() -> None:
    """Configure application logging"""
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


# Setup logging before anything else
setup_logging()

logger = logging.getLogger("epg_service.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    logger.info("Starting EPG Service...")
    await init_db()
    epg_scheduler.start()
    logger.info("EPG Service started successfully")

    yield

    logger.info("Shutting down EPG Service...")
    epg_scheduler.shutdown()
    logger.info("EPG Service stopped")


app = FastAPI(
    title="EPG Service",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/")
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
            "health": "/health - Health check"
        }
    }


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    next_run = epg_scheduler.get_next_run_time()
    return {
        "status": "healthy",
        "scheduler_running": epg_scheduler.scheduler.running,
        "next_fetch": next_run.isoformat() if next_run else None
    }


@app.post("/fetch")
async def trigger_fetch() -> dict | JSONResponse:
    """
    Manually trigger EPG fetch from source

    This will download, parse and store EPG data
    """
    logger.info("Manual EPG fetch triggered via API")
    result = await fetch_and_process()

    if "error" in result:
        return JSONResponse(status_code=500, content=result)

    return result


@app.get("/channels")
async def get_channels(
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> dict:
    """Get all channels"""
    cursor = await db.execute(
        "SELECT xmltv_id, display_name, icon_url FROM channels ORDER BY display_name"
    )
    rows = list(await cursor.fetchall())  # Convert to list

    logger.debug(f"Retrieved {len(rows)} channels")

    return {
        "count": len(rows),
        "channels": [dict(row) for row in rows]
    }


@app.get("/programs")
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
    logger.debug(f"Fetching programs from {start_from} to {start_to}")

    query = """
        SELECT
            id,
            xmltv_channel_id,
            start_time,
            stop_time,
            title,
            description
        FROM programs
        WHERE start_time >= ? AND start_time < ?
        ORDER BY xmltv_channel_id, start_time
    """

    cursor = await db.execute(query, (start_from, start_to))
    rows = list(await cursor.fetchall())  # Convert to list

    logger.debug(f"Retrieved {len(rows)} programs")

    return {
        "count": len(rows),
        "start_from": start_from,
        "start_to": start_to,
        "programs": [dict(row) for row in rows]
    }
