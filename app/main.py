from contextlib import asynccontextmanager
from typing import Annotated
import logging

from fastapi import FastAPI, Depends, Query, HTTPException, Request
import aiosqlite

from datetime import datetime, timezone, timedelta

from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from app.schemas import EPGRequest, EPGResponse, ProgramResponse

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

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Log validation errors with details"""
    logger.error(f"Validation error for {request.method} {request.url.path}")
    logger.error(f"Validation details: {exc.errors()}")

    try:
        body = await request.body()
        logger.error(f"Request body: {body.decode('utf-8')}")
    except:
        logger.error("Could not read request body")

    # Create a properly serializable error response
    errors = []
    for error in exc.errors():
        error_dict = {
            "type": error.get("type"),
            "loc": error.get("loc"),
            "msg": error.get("msg"),
            "input": str(error.get("input", ""))[:100]  # Limit input display
        }
        errors.append(error_dict)

    return JSONResponse(
        status_code=422,
        content={
            "detail": errors
        }
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
            "epg": "/epg - Get EPG for multiple channels with individual time windows (POST)",
            "health": "/health - Health check"
        }
    }


@app.get("/health")
async def health_check() -> dict:
    """Health check endpoint"""
    next_run = epg_scheduler.get_next_run_time()
    return {
        "status": "ok",
        "scheduler_running": epg_scheduler.scheduler.running,
        "next_fetch": next_run.isoformat() if next_run else None
    }


@app.post("/fetch")
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

@app.post("/epg", response_model=EPGResponse)
async def get_epg(
    request: EPGRequest,
    db: Annotated[aiosqlite.Connection, Depends(get_db)]
) -> EPGResponse:
    """
    Get EPG data for multiple channels with individual time windows

    Args:
        request: EPG request with channels and update mode

    Returns:
        EPG data grouped by channel xmltv_id
    """
    # Log the received request body
    logger.info(f"Received EPG request body: {request.model_dump_json()}")
    logger.info(f"EPG request: {len(request.channels)} channels, mode={request.update}")

    now = datetime.now(timezone.utc)
    future_limit = now + timedelta(days=7)

    epg_data: dict[str, list[ProgramResponse]] = {}
    channels_found_set: set[str] = set()
    total_programs = 0

    # Process each channel request (may have duplicates)
    for channel in request.channels:
        # Calculate time window based on update mode
        if request.update == "force":
            start_time = now - timedelta(days=channel.epg_depth)
        else:  # delta
            start_time = now

        end_time = future_limit

        logger.debug(
            f"Fetching EPG for {channel.xmltv_id}: "
            f"{start_time.isoformat()} to {end_time.isoformat()}"
        )

        # Query programs for this channel
        query = """
            SELECT
                id,
                start_time,
                stop_time,
                title,
                description
            FROM programs
            WHERE xmltv_channel_id = ?
              AND start_time >= ?
              AND start_time < ?
            ORDER BY start_time
        """

        cursor = await db.execute(
            query,
            (channel.xmltv_id, start_time.isoformat(), end_time.isoformat())
        )
        rows = await cursor.fetchall()

        if rows:
            channels_found_set.add(channel.xmltv_id)

            # If this xmltv_id already exists in epg_data, merge the programs
            if channel.xmltv_id in epg_data:
                # Create a set of existing program IDs to avoid duplicates
                existing_ids = {p.id for p in epg_data[channel.xmltv_id]}

                # Add only new programs
                for row in rows:
                    if row["id"] not in existing_ids:
                        program = ProgramResponse(
                            id=row["id"],
                            start_time=row["start_time"],
                            stop_time=row["stop_time"],
                            title=row["title"],
                            description=row["description"]
                        )
                        epg_data[channel.xmltv_id].append(program)
                        existing_ids.add(row["id"])
                        total_programs += 1

                # Re-sort by start_time after merging
                epg_data[channel.xmltv_id].sort(key=lambda p: p.start_time)
            else:
                # First time seeing this xmltv_id
                programs = [
                    ProgramResponse(
                        id=row["id"],
                        start_time=row["start_time"],
                        stop_time=row["stop_time"],
                        title=row["title"],
                        description=row["description"]
                    )
                    for row in rows
                ]
                epg_data[channel.xmltv_id] = programs
                total_programs += len(programs)
        else:
            # Include channel in response even if no programs found
            if channel.xmltv_id not in epg_data:
                epg_data[channel.xmltv_id] = []

    channels_found = len(channels_found_set)

    logger.info(
        f"EPG response: {channels_found} unique channels found, "
        f"{total_programs} total programs"
    )

    return EPGResponse(
        update_mode=request.update,
        timestamp=now.isoformat(),
        channels_requested=len(request.channels),
        channels_found=channels_found,
        total_programs=total_programs,
        epg=epg_data
    )
