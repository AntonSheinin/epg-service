import httpx
import aiosqlite
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from xmltv_parser import parse_xmltv_file

load_dotenv()

DATABASE_PATH = os.getenv("DATABASE_PATH", "./data/epg.db")
EPG_SOURCE_URL = os.getenv("EPG_SOURCE_URL")


async def fetch_and_process() -> dict:
    """
    Fetch EPG from source, parse and store in database

    Returns:
        Dictionary with fetch statistics
    """
    print(f"\n{'='*60}")
    print(f"Starting EPG fetch at {datetime.utcnow().isoformat()}")
    print(f"{'='*60}\n")

    if not EPG_SOURCE_URL:
        return {"error": "EPG_SOURCE_URL not configured"}

    # Define time window (2 weeks back, 1 week forward)
    now = datetime.now(timezone.utc)
    time_from = now - timedelta(days=14)
    time_to = now + timedelta(days=7)

    print(f"Time window: {time_from.date()} to {time_to.date()}")
    print(f"Source: {EPG_SOURCE_URL}\n")

    try:
        # Download XMLTV file
        print("Downloading XMLTV file...")
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.get(EPG_SOURCE_URL)
            response.raise_for_status()

            # Save to temp file
            temp_file = "/tmp/epg.xml"
            with open(temp_file, 'wb') as f:
                f.write(response.content)

            file_size = len(response.content) / (1024 * 1024)  # MB
            print(f"✓ Downloaded {file_size:.2f} MB\n")

        # Parse XMLTV
        print("Parsing XMLTV file...")
        channels, programs = parse_xmltv_file(temp_file, time_from, time_to)

        if not channels:
            return {"error": "No channels found in XMLTV"}

        if not programs:
            return {"error": "No programs found in XMLTV"}

        print(f"\n{'='*60}")
        print("Storing data in database...")
        print(f"{'='*60}\n")

        # Store in database
        async with aiosqlite.connect(DATABASE_PATH) as db:
            # Delete old programs (older than time_from)
            result = await db.execute(
                "DELETE FROM programs WHERE start_time < ?",
                (time_from.isoformat(),)
            )
            deleted_count = result.rowcount
            print(f"✓ Deleted {deleted_count} old programs")

            # Insert/update channels
            await db.executemany(
                "INSERT OR REPLACE INTO channels (xmltv_id, display_name, icon_url) VALUES (?, ?, ?)",
                channels
            )
            print(f"✓ Stored {len(channels)} channels")

            # Insert programs (ignore duplicates)
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

            # Count how many were actually inserted
            before_count = await db.execute("SELECT COUNT(*) FROM programs")
            before_count = (await before_count.fetchone())[0]

            await db.executemany(
                """INSERT OR IGNORE INTO programs
                   (id, xmltv_channel_id, start_time, stop_time, title, description)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                program_tuples
            )

            after_count = await db.execute("SELECT COUNT(*) FROM programs")
            after_count = (await after_count.fetchone())[0]
            inserted_count = after_count - before_count

            print(f"✓ Stored {inserted_count} new programs (skipped {len(programs) - inserted_count} duplicates)")

            await db.commit()

        stats = {
            "status": "success",
            "timestamp": datetime.utcnow().isoformat(),
            "channels": len(channels),
            "programs_parsed": len(programs),
            "programs_inserted": inserted_count,
            "programs_deleted": deleted_count
        }

        print(f"\n{'='*60}")
        print("✓ EPG fetch completed successfully")
        print(f"{'='*60}\n")

        return stats

    except Exception as e:
        error_msg = f"Error during EPG fetch: {str(e)}"
        print(f"\n✗ {error_msg}\n")
        return {"error": error_msg}
