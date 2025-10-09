EPG Service
Minimal EPG (Electronic Program Guide) service that fetches XMLTV data, parses it, and exposes it via REST API with automatic scheduled fetching.
Features

✅ Fetch EPG from XMLTV sources (manual + scheduled)
✅ Parse XMLTV format with timezone conversion
✅ SQLite storage with WAL mode
✅ REST API for channels and programs
✅ Automatic scheduled fetching (cron-based)
✅ Docker support
✅ Fast dependency management with uv

Project Structure
epg-service/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml     # Project dependencies
├── .env
├── data/              # SQLite database location
└── app/
    ├── __init__.py
    ├── main.py        # FastAPI app + endpoints + logging setup
    ├── database.py    # Database init & connection
    ├── xmltv_parser.py # XMLTV parser
    ├── epg_fetcher.py # Fetch & process logic
    ├── scheduler.py   # Automatic scheduled fetching
    └── config.py      # Configuration management
Quick Start
Prerequisites
Install uv - ultra-fast Python package manager:
bash# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
Option 1: Local Development with uv
bash# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -e .

# Create data directory
mkdir -p data

# Configure .env file
cp .env.example .env
# Edit .env and set EPG_SOURCE_URL

# Run server
uvicorn app.main:app --reload
Or use uv directly without activating venv:
bash# Run server with uv
uv run uvicorn app.main:app --reload
Option 2: Docker
bash# Build and run
docker-compose up --build

# Server will be available at http://localhost:8000
Configuration
Create .env file in project root:
env# Database
DATABASE_PATH=./data/epg.db

# EPG Source (required)
EPG_SOURCE_URL=https://your-xmltv-source.com/epg.xml

# Logging
LOG_LEVEL=INFO

# Scheduler (cron format: minute hour day month day_of_week)
# Default: daily at 3 AM
EPG_FETCH_CRON=0 3 * * *
Cron Expression Examples
bashEPG_FETCH_CRON=0 3 * * *      # Daily at 3:00 AM
EPG_FETCH_CRON=0 */6 * * *    # Every 6 hours
EPG_FETCH_CRON=0 0,12 * * *   # Twice daily (midnight and noon)
EPG_FETCH_CRON=0 2 * * 0      # Weekly on Sunday at 2:00 AM
API Endpoints
Root
httpGET /
Service information and available endpoints.
Response:
json{
  "service": "EPG Service",
  "version": "0.1.0",
  "next_scheduled_fetch": "2025-10-10T03:00:00+00:00",
  "endpoints": {
    "fetch": "/fetch - Manually trigger EPG fetch",
    "channels": "/channels - Get all channels",
    "programs": "/programs - Get programs (query params: start_from, start_to)",
    "health": "/health - Health check"
  }
}
Health Check
httpGET /health
Check service health and scheduler status.
Response:
json{
  "status": "healthy",
  "scheduler_running": true,
  "next_fetch": "2025-10-10T03:00:00+00:00"
}
Manual Fetch
httpPOST /fetch
Manually trigger EPG fetch from source. This will:

Download XMLTV file from configured URL
Parse channels and programs
Store in SQLite database
Clean up temporary files
Return statistics

Example:
bashcurl -X POST http://localhost:8000/fetch
Response:
json{
  "status": "success",
  "timestamp": "2025-10-09T12:00:00+00:00",
  "channels": 150,
  "programs_parsed": 5000,
  "programs_inserted": 4500,
  "programs_deleted": 1200
}
Get Channels
httpGET /channels
Get all available channels.
Example:
bashcurl http://localhost:8000/channels
Response:
json{
  "count": 150,
  "channels": [
    {
      "xmltv_id": "channel.id",
      "display_name": "Channel Name",
      "icon_url": "https://example.com/icon.png"
    }
  ]
}
Get Programs
httpGET /programs?start_from=<ISO8601>&start_to=<ISO8601>
Get all programs within a time range.
Query Parameters:

start_from (required): ISO8601 datetime (e.g., 2025-10-09T00:00:00Z)
start_to (required): ISO8601 datetime (e.g., 2025-10-10T00:00:00Z)

Example:
bashcurl "http://localhost:8000/programs?start_from=2025-10-09T00:00:00Z&start_to=2025-10-10T00:00:00Z"
Response:
json{
  "count": 5000,
  "start_from": "2025-10-09T00:00:00Z",
  "start_to": "2025-10-10T00:00:00Z",
  "programs": [
    {
      "id": "uuid-here",
      "xmltv_channel_id": "channel.id",
      "start_time": "2025-10-09T14:00:00+00:00",
      "stop_time": "2025-10-09T15:00:00+00:00",
      "title": "Program Title",
      "description": "Program description..."
    }
  ]
}
Database
SQLite database with 2 main tables:
Channels Table

xmltv_id (PRIMARY KEY): Channel identifier from XMLTV
display_name: Human-readable channel name
icon_url: Channel logo/icon URL
created_at: Timestamp

Programs Table

id (PRIMARY KEY): UUID for each program
xmltv_channel_id: Foreign key to channels
start_time: Program start time (ISO8601 UTC)
stop_time: Program end time (ISO8601 UTC)
title: Program title
description: Program description
created_at: Timestamp
UNIQUE constraint on (channel, start_time, title)

Database features:

WAL (Write-Ahead Logging) mode for better concurrency
Optimized cache size
Index on (channel_id, start_time) for fast queries

Data Retention

Historical: 14 days (programs older than 14 days are deleted)
Future: 7 days (programs up to 7 days in the future)
Old programs are automatically deleted on each fetch

Scheduler
The service includes an automatic scheduler using APScheduler:

Configured via EPG_FETCH_CRON environment variable
Default: daily at 3:00 AM
Runs in the background
Logs all fetch attempts and results
Misfire grace time: 1 hour

View next scheduled fetch:
bashcurl http://localhost:8000/health
Logging
Simple console-based logging:

Configured via LOG_LEVEL environment variable (DEBUG, INFO, WARNING, ERROR)
Default: INFO
Logs to stdout (perfect for Docker/containerized deployments)
Structured format with timestamps

Log levels:

DEBUG: Detailed information for debugging
INFO: General informational messages
WARNING: Warning messages
ERROR: Error messages with stack traces

Development
Running locally
bash# Install dependencies
uv pip install -e .

# Run with auto-reload
uvicorn app.main:app --reload --log-level debug

# Run tests (when added)
pytest
Development with uv
bash# Add new dependency
uv pip install <package>
uv pip freeze > requirements.txt

# Update pyproject.toml manually with new dependency

# Run without venv activation
uv run uvicorn app.main:app --reload
Docker
Build and run
bashdocker-compose up --build
View logs
bashdocker-compose logs -f epg-service
Stop service
bashdocker-compose down
Testing
bash# Check service health
curl http://localhost:8000/health

# Trigger manual fetch
curl -X POST http://localhost:8000/fetch

# Get all channels
curl http://localhost:8000/channels

# Get today's programs
curl "http://localhost:8000/programs?start_from=$(date -u +%Y-%m-%dT00:00:00Z)&start_to=$(date -u -d '+1 day' +%Y-%m-%dT00:00:00Z)"

# Get programs for specific date
curl "http://localhost:8000/programs?start_from=2025-10-09T00:00:00Z&start_to=2025-10-10T00:00:00Z"
Requirements

Python 3.12+
FastAPI 0.115+
aiosqlite 0.20+
httpx 0.28+
lxml 5.3+
pydantic 2.10+
pydantic-settings 2.6+
aiofiles 24.1+
apscheduler 3.10+

See pyproject.toml for complete dependency list.
Troubleshooting
EPG_SOURCE_URL not configured
Make sure .env file exists and contains:
envEPG_SOURCE_URL=https://your-xmltv-source.com/epg.xml
No programs found

Check if XMLTV source is accessible
Verify XMLTV format is valid
Check logs for parsing errors
Ensure time window includes programs (14 days past to 7 days future)

Scheduler not running

Check logs on startup
Verify cron expression syntax
Check /health endpoint for scheduler status

Database locked

SQLite WAL mode should prevent most locks
Ensure only one instance is writing to the database
Check file permissions on data/ directory

Future Enhancements

 Input validation for datetime parameters
 Channel filtering (external service integration)
 Authentication (token validation)
 Multiple EPG sources with deduplication
 Rate limiting
 Caching layer
 Metrics and monitoring
 Unit and integration tests

License
MIT
