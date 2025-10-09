from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
import httpx

from app.config import settings
from app.xmltv_parser import parse_xmltv_file


async def fetch_and_process() -> dict:
    """
    Fetch EPG from source, parse and store in database

    Returns:
        Dictionary with fetch statistics or error
    """
    print(f"\n{'='*60}")
    print(f"Starting EPG fetch at {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}\n")

    if not settings.epg_source_url:
        return {"error": "EPG_SOURCE_URL not configured"}

    try:
        # Calculate time window
        now = datetime.now(timezone.utc)
        time_from = now - timedelta(days=14)
        time_to = now + timedelta(days=7)

        print(f"Time window: {time_from.date()} to {time_to.date()}")
        print(f"Source: {settings.epg_source_url}\n")

        # Download XMLTV file
        temp_file = await _download_xmltv(settings.epg_source_url)

        # Parse XMLTV
        channels, programs = await _parse_xmltv(temp_file, time_from, time_to)

        # Store in database
        stats = await _store_data(channels, programs, time_from)

        print(f"\n{'='*60}")
        print("✓ EPG fetch completed successfully")
        print(f"{'='*60}\n")

        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **stats
        }

    except Exception as e:
        error_msg = f"Error during EPG fetch: {str(e)}"
        print(f"\n✗ {error_msg}\n")
        return {"error": error_msg}


async def _download_xmltv(url: str) -> Path:
    """Download XMLTV file from URL"""
    print("Downloading XMLTV file...")

    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.get(url)
        response.raise_for_status()

        temp_file = Path("/tmp/epg.xml")
        temp_file.write_bytes(response.content)

        file_size = len(response.content) / (1024 * 1024)  # MB
        print(f"✓ Downloaded {file_size:.2f} MB\n")

        return temp_file


async def _parse_xmltv(
    file_path: Path,
    time_from: datetime,
    time_to: datetime
) -> tuple[list[tuple], list[dict]]:
    """Parse XMLTV file and return channels and programs"""
    print("Parsing XMLTV file...")

    channels, programs = parse_xmltv_file(str(file_path), time_from, time_to)

    if not channels:
        raise ValueError("No channels found in XMLTV")

    if not programs:
        raise ValueError("No programs found in XMLTV")

    return channels, programs


async def _store_data(
    channels: list[tuple],
    programs: list[dict],
    time_from: datetime
) -> dict:
    """Store channels and programs in database"""
    print(f"\n{'='*60}")
    print("Storing data in database...")
    print(f"{'='*60}\n")

    async with aiosqlite.connect(settings.database_path) as db:
        # Delete old programs
        deleted_count = await _delete_old_programs(db, time_from)

        # Store channels
        await _store_channels(db, channels)

        # Store programs and get count
        inserted_count = await _store_programs(db, programs)

        await db.commit()

    return {
        "channels": len(channels),
        "programs_parsed": len(programs),
        "programs_inserted": inserted_count,
        "programs_deleted": deleted_count
    }


async def _delete_old_programs(db: aiosqlite.Connection, time_from: datetime) -> int:
    """Delete programs older than time_from"""
    cursor = await db.execute(
        "DELETE FROM programs WHERE start_time < ?",
        (time_from.isoformat(),)
    )
    deleted_count = cursor.rowcount
    print(f"✓ Deleted {deleted_count} old programs")
    return deleted_count


async def _store_channels(db: aiosqlite.Connection, channels: list[tuple]) -> None:
    """Store channels in database"""
    await db.executemany(
        "INSERT OR REPLACE INTO channels (xmltv_id, display_name, icon_url) VALUES (?, ?, ?)",
        channels
    )
    print(f"✓ Stored {len(channels)} channels")


async def _store_programs(db: aiosqlite.Connection, programs: list[dict]) -> int:
    """Store programs in database and return count of inserted programs"""
    # Get count before insert
    cursor = await db.execute("SELECT COUNT(*) FROM programs")
    result = await cursor.fetchone()
    before_count = result[0] if result else 0

    # Prepare program tuples
    program_tuples = [
        (
            p['id'],
            p['xmltv_channel_id'],
            p['start_time'],
            p['stop_time'],
            p['title'],
            p['description']
        )
        for p in programs
    ]

    # Insert programs
    await db.executemany(
        """INSERT OR IGNORE INTO programs
           (id, xmltv_channel_id, start_time, stop_time, title, description)
           VALUES (?, ?, ?, ?, ?, ?)""",
        program_tuples
    )

    # Get count after insert
    cursor = await db.execute("SELECT COUNT(*) FROM programs")
    result = await cursor.fetchone()
    after_count = result[0] if result else 0

    inserted_count = after_count - before_count
    skipped_count = len(programs) - inserted_count

    print(f"✓ Stored {inserted_count} new programs (skipped {skipped_count} duplicates)")

    return inserted_count
