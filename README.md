# EPG Service

Minimal EPG (Electronic Program Guide) service that fetches XMLTV data, parses it, and exposes via REST API with automatic scheduled fetching.

## Features

- **Multi-source EPG fetching** - Fetch and merge data from multiple XMLTV sources
- **Concurrent fetching pipeline** - Parallelizes source downloads while keeping database writes safe
- **Date range filtering** - Query EPG with required from_date and to_date parameters
- **Timezone support** - Convert timestamps to any IANA timezone
- **Smart data merging** - Automatic deduplication across sources
- **SQLAlchemy ORM** - Type-safe database operations with PostgreSQL backend
- **PostgreSQL storage** - Reliable concurrent read/write operations
- **REST API** - FastAPI-based endpoints for channels and programs
- **Automatic scheduling** - APScheduler-based scheduled fetching with event loop integration
- **Concurrency protection** - Prevents overlapping fetch operations
- **Docker support** - Ready-to-deploy containerized setup

## Project Structure

```
epg-service/
|-- docker-compose.yml
|-- Dockerfile
|-- pyproject.toml
|-- alembic.ini
|-- alembic/
|   |-- env.py
|   `-- versions/
`-- app/
    |-- main.py              # FastAPI application
    |-- routers.py           # API routes
    |-- config.py            # Configuration
    |-- schemas.py           # Pydantic models
    |-- application/         # Use cases and orchestration
    |-- domain/              # Entities and interfaces
    |-- infrastructure/      # DB + external integrations
    `-- utils/               # Utilities
```

## Quick Start

### Local Development

```bash
# Install uv (Python package manager)
# Windows:
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
# macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e .

# Configure .env file (see Configuration section)
cp .env.example .env

# Run migrations
alembic upgrade head

# Run server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker

```bash
docker-compose up --build or
docker-compose up -d --build

# Run migrations
docker-compose exec epg-service alembic upgrade head
```

Server will be available at `http://localhost:8000`

## Configuration

Create `.env` file in project root:

```env
# Database
DATABASE_URL=postgresql://epg:epg@postgres:5432/epg
POSTGRES_DB=epg
POSTGRES_USER=epg
POSTGRES_PASSWORD=epg

# EPG Sources (comma-separated URLs)
EPG_SOURCES=https://source1.com/epg.xml,https://source2.com/epg.xml

# Logging
LOG_LEVEL=INFO

# Scheduler (cron format: minute hour day month day_of_week)
EPG_FETCH_CRON=0 3 * * *

# Archive depth (days to keep historical programs)
MAX_EPG_DEPTH=14

# Future EPG limit (days to keep future programs)
MAX_FUTURE_EPG_LIMIT=7

# XML parsing timeout in seconds (set 0 to disable timeout)
EPG_PARSE_TIMEOUT_SEC=600
```

When running the API outside Docker, use `DATABASE_URL=postgresql://epg:epg@localhost:5434/epg`.

**Cron Examples:**
- `0 3 * * *` - Daily at 3:00 AM
- `0 */6 * * *` - Every 6 hours
- `0 0,12 * * *` - Twice daily (midnight and noon)

## API Endpoints

### GET /
Service information and available endpoints

### GET /health
Service health status

**Response (200):**
```json
{
  "status": "up",
  "service": "epg-service",
  "time": "2026-02-28T12:00:00Z"
}
```

### GET /stats
Service stats summary

`last_updated_channels_count` is the number of channels that had actual EPG row inserts/updates in the last successful import cycle.

**Response (200):**
```json
{
  "checked_at": "2026-02-28T12:00:00Z",
  "next_epg_update_at": "2026-02-29T03:00:00Z",
  "last_epg_update_at": "2026-02-28T11:45:00Z",
  "sources_total": 12,
  "last_channels_update_at": "2026-02-28T11:45:00Z",
  "last_updated_channels_count": 8432,
  "error": null
}
```

### POST /fetch
Manually trigger EPG fetch from sources

```bash
curl -X POST http://localhost:8000/fetch
```

### POST /epg
Get EPG data for multiple channels with date range filtering and timezone conversion

**Request body:**
```json
{
  "channels": [
    {"xmltv_id": "channel1"},
    {"xmltv_id": "channel2"}
  ],
  "timezone": "Europe/London",
  "from_date": "2025-10-09T00:00:00Z",
  "to_date": "2025-10-15T23:59:59Z"
}
```

**Parameters:**
- `channels` (required): Array of channel objects with `xmltv_id`
- `from_date` (required): ISO8601 datetime for start of range
- `to_date` (required): ISO8601 datetime for end of range
- `timezone` (optional): IANA timezone for response (default: UTC)

**Examples:**

Basic query:
```bash
curl -X POST http://localhost:8000/epg \
  -H "Content-Type: application/json" \
  -d '{
    "channels": [{"xmltv_id": "channel1"}],
    "from_date": "2025-10-09T00:00:00Z",
    "to_date": "2025-10-15T23:59:59Z",
    "timezone": "Europe/London"
  }'
```

Multiple channels:
```bash
curl -X POST http://localhost:8000/epg \
  -H "Content-Type: application/json" \
  -d '{
    "channels": [
      {"xmltv_id": "channel1"},
      {"xmltv_id": "channel2"}
    ],
    "from_date": "2025-10-09T00:00:00Z",
    "to_date": "2025-10-15T23:59:59Z",
    "timezone": "UTC"
  }'
```

Different timezone:
```bash
curl -X POST http://localhost:8000/epg \
  -H "Content-Type: application/json" \
  -d '{
    "channels": [{"xmltv_id": "channel1"}],
    "from_date": "2025-10-09T00:00:00Z",
    "to_date": "2025-10-15T23:59:59Z",
    "timezone": "America/New_York"
  }'
```

## Database

PostgreSQL for concurrent read/write operations. Migrations are managed with Alembic.

**Tables:**
- `channels` - Channel metadata (xmltv_id, display_name, icon_url)
- `programs` - Program data (id, channel_id, start_time, stop_time, title, description)

**Data Retention:**
- Historical: Configurable via `MAX_EPG_DEPTH` (default 14 days)
- Future: Unlimited (accepts all future programs from sources)
- Auto-cleanup on each fetch

## Recent Improvements

### SQLAlchemy Migration (v0.3.0)
- Migrated from raw database access to SQLAlchemy ORM
- Type-safe database operations with proper async support
- Lazy engine initialization for proper event loop handling
- Enhanced error handling and logging

### Scheduler & Fetcher Fixes
- Fixed APScheduler event loop binding
- Proper async context detection for scheduler startup
- Improved error handling in startup/shutdown sequence
- Comprehensive logging for debugging

### API Simplification (v0.2.0)
- Changed to required `from_date` and `to_date` parameters
- Removed `update` (force/delta) mode parameters
- Removed per-channel `epg_depth` parameter
- Simplified request/response schemas
- ISO8601 format validation with clear error messages

### Code Quality Enhancements
- Refactored exception handling with specific error types
- Added comprehensive docstrings to all functions
- Improved type safety throughout the codebase
- Extracted reusable utilities to dedicated modules
- Simplified complex functions for better maintainability

### Architecture Improvements
- Consolidated date/time utilities into single module
- Better separation of concerns across services
- Type-safe helper functions with proper annotations
- Clean, documented codebase ready for production

## Development

```bash
# Run with hot-reload
uvicorn app.main:app --reload --log-level debug

# Add dependency
uv pip install <package>
# Then update pyproject.toml
```

## Docker

```bash
# Build and run
docker-compose up --build

# View logs
docker-compose logs -f epg-service

# Stop
docker-compose down
```

## Requirements

- Python 3.12+
- FastAPI 0.115+
- asyncpg 0.29+
- alembic 1.13+
- httpx 0.28+
- lxml 5.3+
- pydantic 2.10+
- aiofiles 24.1+
- apscheduler 3.10+

See [pyproject.toml](pyproject.toml) for complete list.

## Troubleshooting

**EPG_SOURCES not configured:**
```env
EPG_SOURCES=https://your-xmltv-source.com/epg.xml
```

**No programs found:**
- Check XMLTV source accessibility
- Verify XMLTV format validity
- Check logs for parsing errors

**Can't access from network:**
- Ensure server binds to `0.0.0.0` not `127.0.0.1`
- Check firewall settings
- Verify port forwarding if needed

## License

MIT


