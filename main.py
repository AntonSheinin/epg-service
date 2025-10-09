from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, Depends, Query
from fastapi.responses import JSONResponse
import aiosqlite

from app.config import settings
from app.database import init_db, get_db
from app.epg_fetcher import fetch_and_process


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    await init_db()
    print("✓ EPG Service started")
    yield
    # Shutdown (if needed)
    print("✓ EPG Service shutting down")


app = FastAPI(
    title="EPG Service",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/")
async def root() -> dict:
    """Root endpoint with service information"""
    return {
        "service": "EPG Service",
        "version": "0.1.0",
        "endpoints": {
            "fetch": "/fetch - Manually trigger EPG fetch",
            "channels": "/channels - Get all channels",
            "programs": "/programs - Get programs (query params: start_from, start_to)"
        }
    }


@app.post("/fetch")
async def trigger_fetch() -> dict | JSONResponse:
    """
    Manually trigger EPG fetch from source

    This will download, parse and store EPG data
    """
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
    rows = await cursor.fetchall()

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
    rows = await cursor.fetchall()

    return {
        "count": len(rows),
        "start_from": start_from,
        "start_to": start_to,
        "programs": [dict(row) for row in rows]
    }
