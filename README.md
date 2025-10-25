# EPG Service

Minimal EPG (Electronic Program Guide) service that fetches XMLTV data, parses it, and exposes via REST API with automatic scheduled fetching.

## Features

- **Multi-source EPG fetching** - Fetch and merge data from multiple XMLTV sources
- **Date range filtering** - Query EPG with required from_date and to_date parameters
- **Timezone support** - Convert timestamps to any IANA timezone
- **Smart data merging** - Automatic deduplication across sources
- **SQLAlchemy ORM** - Type-safe database operations with SQLite backend
- **SQLite storage** - WAL mode for concurrent read/write operations
- **REST API** - FastAPI-based endpoints for channels and programs
- **Automatic scheduling** - APScheduler-based scheduled fetching with event loop integration
- **Concurrency protection** - Prevents overlapping fetch operations
- **Docker support** - Ready-to-deploy containerized setup

## Project Structure

```
epg-service/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env
├── data/                    # SQLite database
└── app/
    ├── main.py              # FastAPI application
    ├── routers.py           # API routes
    ├── config.py            # Configuration
    ├── database.py          # Database setup
    ├── schemas.py           # Pydantic models
    ├── services/            # Business logic
    │   ├── epg_query_service.py
    │   ├── epg_fetch_service.py
    │   ├── db_service.py
    │   ├── scheduler_service.py
    │   └── xmltv_parser_service.py
    └── utils/               # Utilities
        ├── timezone.py
        ├── file_operations.py
        └── data_merging.py
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

# Create data directory
mkdir -p data

# Configure .env file (see Configuration section)
cp .env.example .env

# Run server
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker

```bash
docker-compose up --build or
docker-compose up -d --build
```

Server will be available at `http://localhost:8000`

## Configuration

Create `.env` file in project root:

```env
# Database
DATABASE_PATH=./data/epg.db

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
```

**Cron Examples:**
- `0 3 * * *` - Daily at 3:00 AM
- `0 */6 * * *` - Every 6 hours
- `0 0,12 * * *` - Twice daily (midnight and noon)

## API Endpoints

### GET /
Service information and available endpoints

### GET /health
Health check and scheduler status

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

SQLite with WAL mode for concurrent read/write operations.

**Tables:**
- `channels` - Channel metadata (xmltv_id, display_name, icon_url)
- `programs` - Program data (id, channel_id, start_time, stop_time, title, description)

**Data Retention:**
- Historical: Configurable via `MAX_EPG_DEPTH` (default 14 days)
- Future: Unlimited (accepts all future programs from sources)
- Auto-cleanup on each fetch

## Recent Improvements

### SQLAlchemy Migration (v0.3.0)
- Migrated from raw aiosqlite to SQLAlchemy ORM
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
- aiosqlite 0.20+
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
