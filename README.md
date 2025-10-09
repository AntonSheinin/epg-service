# EPG Service - Phase 1

Minimal EPG (Electronic Program Guide) service that fetches XMLTV data, parses it, and exposes it via REST API.

## Features

- ✅ Fetch EPG from XMLTV sources
- ✅ Parse XMLTV format with timezone conversion
- ✅ SQLite storage with WAL mode
- ✅ REST API for channels and programs
- ✅ Docker support
- ✅ Fast dependency management with uv

## Project Structure

```
epg-service/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml     # Project dependencies
├── .env
├── data/              # SQLite database location
└── app/
    ├── main.py        # FastAPI app + endpoints
    ├── database.py    # Database init & connection
    ├── xmltv_parser.py # XMLTV parser
    └── epg_fetcher.py # Fetch & process logic
```

## Quick Start

### Prerequisites

Install [uv](https://github.com/astral-sh/uv) - ultra-fast Python package manager:

```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Option 1: Local Development with uv

```bash
# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
uv pip install -r pyproject.toml

# Create data directory
mkdir -p data

# Configure .env file
cp .env.example .env
# Edit .env and set EPG_SOURCE_URL

# Run server
uvicorn app.main:app --reload
```

Or use uv directly without activating venv:

```bash
# Run server with uv
uv run uvicorn app.main:app --reload
```

### Option 2: Docker

```bash
# Build and run
docker-compose up --build

# Server will be available at http://localhost:8000
```

## Configuration

Edit `.env` file:

```bash
DATABASE_PATH=./data/epg.db
EPG_SOURCE_URL=https://your-xmltv-source.com/epg.xml
```

## API Endpoints

### Root
```
GET /
```
Service information and available endpoints.

### Fetch EPG
```
POST /fetch
```
Manually trigger EPG fetch from source. This will:
1. Download XMLTV file
2. Parse channels and programs
3. Store in SQLite database
4. Return statistics

Example:
```bash
curl -X POST http://localhost:8000/fetch
```

### Get Channels
```
GET /channels
```
Get all channels.

Response:
```json
{
  "count": 150,
  "channels": [
    {
      "xmltv_id": "channel.id",
      "display_name": "Channel Name",
      "icon_url": "https://..."
    }
  ]
}
```

### Get Programs
```
GET /programs?start_from=<ISO8601>&start_to=<ISO8601>
```
Get all programs in time range.

Example:
```bash
curl "http://localhost:8000/programs?start_from=2025-10-09T00:00:00Z&start_to=2025-10-10T00:00:00Z"
```

Response:
```json
{
  "count": 5000,
  "start_from": "2025-10-09T00:00:00Z",
  "start_to": "2025-10-10T00:00:00Z",
  "programs": [
    {
      "id": "uuid",
      "xmltv_channel_id": "channel.id",
      "start_time": "2025-10-09T14:00:00+00:00",
      "stop_time": "2025-10-09T15:00:00+00:00",
      "title": "Program Title",
      "description": "Program description..."
    }
  ]
}
```

## Database

SQLite database with 2 main tables:

- **channels**: Stores channel information
- **programs**: Stores program schedule (2 weeks historical + 1 week future)

Database uses WAL (Write-Ahead Logging) mode for better concurrency.

## Data Retention

- Historical: 14 days
- Future: 7 days
- Old programs are automatically deleted on each fetch

## Testing

```bash
# Check service
curl http://localhost:8000/

# Trigger fetch
curl -X POST http://localhost:8000/fetch

# Get channels
curl http://localhost:8000/channels

# Get today's programs
curl "http://localhost:8000/programs?start_from=$(date -u +%Y-%m-%dT00:00:00Z)&start_to=$(date -u -d '+1 day' +%Y-%m-%dT00:00:00Z)"
```

## Development with uv

```bash
# Add new dependency
uv pip install <package>

# Update dependencies
uv pip install --upgrade -r pyproject.toml

# Run with uv (no venv activation needed)
uv run uvicorn app.main:app --reload
```

## Next Steps (Future Phases)

- [ ] Scheduler (automatic daily fetch)
- [ ] Channel filtering (external service integration)
- [ ] Authentication (token validation)
- [ ] Multiple EPG sources with deduplication
- [ ] Rate limiting
- [ ] Caching

## Requirements

- Python 3.12+
- uv (or pip)
- FastAPI 0.115+
- aiosqlite 0.20+
- httpx 0.27+
- lxml 5.3+

## License

MIT
