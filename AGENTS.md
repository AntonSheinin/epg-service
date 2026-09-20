# AGENTS.md

## Purpose

This file defines how AI agents should understand, navigate, and modify this repository.
It is the primary instruction source for automated code agents working in this project.

Agents should behave as careful maintainers, not aggressive refactorers. Prefer safe,
minimal, well-validated changes that preserve the existing architecture and API behavior.

## Project Overview

This repository contains `epg-service`, a FastAPI service that imports XMLTV EPG data,
stores it in PostgreSQL, and exposes REST endpoints for service health, import stats,
manual fetching, and EPG queries.

The service:

- downloads one or more XMLTV sources
- parses channels and programmes with bounded-memory streaming
- stores channels, programmes, and import status in PostgreSQL
- serves EPG data with timezone conversion
- schedules automatic imports using APScheduler
- runs locally or in Docker Compose
- deploys from `master` through GitHub Actions over SSH

## Stack

- Language: Python 3.12+
- Web framework: FastAPI
- ASGI server: Uvicorn
- Database: PostgreSQL
- Database driver: asyncpg
- ORM: SQLAlchemy async ORM
- Migrations: Alembic
- Settings: pydantic-settings / Pydantic v2
- HTTP client: httpx
- XML parsing: lxml
- Async file I/O: aiofiles
- Scheduler: APScheduler
- Packaging/build backend: hatchling
- Dependency workflow: uv
- Runtime/containerization: Docker and Docker Compose
- Deployment: GitHub Actions plus `appleboy/ssh-action`

## Repository Structure

```text
.
|-- app/
|   |-- main.py                 # FastAPI app, lifespan startup/shutdown, validation handler
|   |-- routers.py              # API routes: /, /health, /stats, /fetch, /epg
|   |-- config.py               # Environment settings and logging setup
|   |-- schemas.py              # Pydantic request/response models
|   |-- models.py               # Domain dataclasses
|   |-- db/
|   |   |-- orm_models.py        # SQLAlchemy ORM tables
|   |   |-- repository.py        # Database read/write repository methods
|   |   `-- session.py           # Async engine/session lifecycle helpers
|   |-- services/
|   |   |-- epg_fetch.py         # Fetch/import pipeline and import status updates
|   |   |-- epg_query.py         # /epg query orchestration and response assembly
|   |   |-- xmltv_parser.py      # XMLTV channel/program parsing
|   |   |-- downloader.py        # Source download handling
|   |   |-- scheduler.py         # APScheduler integration
|   |   `-- stats.py             # /stats payload assembly
|   `-- utils/
|       |-- timezone.py          # Date parsing, timezone conversion, range calculation
|       `-- file_operations.py   # File download/cleanup utilities
|-- alembic/
|   |-- env.py                   # Async Alembic environment
|   `-- versions/                # Database migration history
|-- .github/workflows/deploy.yml # Production deployment workflow
|-- Dockerfile                   # Service image
|-- docker-compose.yml           # Service plus PostgreSQL
|-- docker-entrypoint.sh         # Migration retry loop then service command
|-- pyproject.toml               # Project metadata and dependencies
|-- uv.lock                      # Locked dependency state
`-- README.md                    # User-facing project documentation
```

## Runtime Entry Points

- Application: `app.main:app`
- API routes: `app.routers.main_router`
- Startup sequence: `app.main.lifespan`
- Database initialization: `app.db.session.init_db`
- Scheduler startup: `app.services.scheduler.epg_scheduler.start`
- Manual fetch endpoint: `POST /fetch`
- Main EPG query endpoint: `POST /epg`
- Stats endpoint: `GET /stats`
- Health endpoint: `GET /health`

## Configuration

Configuration is loaded from environment variables and `.env` via `app.config.CustomSettings`.

Important variables:

- `DATABASE_URL`: required PostgreSQL connection string
- `POSTGRES_DB`: Docker Compose PostgreSQL database name
- `POSTGRES_USER`: Docker Compose PostgreSQL user
- `POSTGRES_PASSWORD`: Docker Compose PostgreSQL password
- `EPG_SOURCES`: comma-separated HTTP/HTTPS XMLTV source URLs
- `LOG_LEVEL`: logging verbosity
- `EPG_FETCH_CRON`: APScheduler cron expression
- `EPG_FETCH_MISFIRE_GRACE_SEC`: scheduler misfire grace period
- `EPG_CHANNELS_CHUNK_SIZE`: channel batch size
- `EPG_PROGRAMS_CHUNK_SIZE`: programme batch size
- `MAX_EPG_DEPTH`: historical programme retention window in days
- `MAX_FUTURE_EPG_LIMIT`: future programme retention window in days
- `EPG_PARSE_TIMEOUT_SEC`: per-source XML parsing timeout; `0` disables timeout
- `DB_STARTUP_MAX_ATTEMPTS`: migration retry attempts used by `docker-entrypoint.sh`

Never hardcode secrets, credentials, source URLs, server addresses, or SSH material.

## Deployment

Production deployment is defined in `.github/workflows/deploy.yml`.

The workflow:

- is named `Deploy`
- runs on pushes to the `master` branch
- uses `appleboy/ssh-action@v1`
- connects with these GitHub secrets:
  - `SERVER_HOST`
  - `SERVER_USER`
  - `SERVER_SSH_KEY`
- executes the following on the remote server:

```sh
cd ~/epg-service
git fetch origin master
git reset --hard origin/master
docker compose up -d --build --force-recreate
```

Deployment implications:

- A push to `master` deploys automatically.
- Remote deployment forcibly resets the server checkout to `origin/master`.
- Docker Compose rebuilds and recreates the service.
- `docker-entrypoint.sh` applies Alembic migrations before starting Uvicorn.
- Database state is stored in the `postgres_data` Docker volume.

Agents must not modify deployment configuration blindly. Treat changes to
`.github/workflows/deploy.yml`, `Dockerfile`, `docker-compose.yml`, and
`docker-entrypoint.sh` as infrastructure changes that require extra inspection and
clear justification.

## Local Development Commands

Use the existing toolchain and avoid introducing new project tooling without need.

Common commands:

```powershell
uv venv
uv pip install -e .
alembic upgrade head
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
docker compose up -d --build
docker compose logs -f epg-service
```

On Windows in this repository, the virtualenv Python is usually:

```powershell
.\.venv\Scripts\python.exe
```

Basic syntax validation:

```powershell
.\.venv\Scripts\python.exe -m compileall app
```

## 1. General Principles

- Make minimal, safe, and reversible changes.
- Prefer clarity over cleverness.
- Do not introduce unnecessary dependencies.
- Do not modify unrelated code.
- Preserve existing architecture and patterns.
- When uncertain, inspect before changing.
- Keep public API behavior stable unless the requested change explicitly requires a break.
- Respect the async SQLAlchemy and FastAPI patterns already used in the project.

## 2. Understanding the Codebase

Before making changes:

- Identify the entry point and route involved.
- Locate relevant schemas, services, repository methods, and ORM models.
- Locate configuration files and environment variables.
- Understand module boundaries and responsibilities.
- Search for existing implementations before adding new ones.
- Check whether a behavior is user-facing API behavior, persistence behavior, or internal pipeline behavior.

Avoid:

- duplicating logic
- creating parallel patterns
- guessing behavior without reading code
- mixing HTTP routing, business logic, and persistence in the same layer
- bypassing existing timezone helpers or database repository methods

## 3. Code Changes

When modifying code:

- Follow existing style and conventions.
- Keep changes small and focused.
- Maintain backward compatibility unless explicitly required.
- Do not rename or move files unless necessary.
- Do not break public interfaces without clear reason.
- Prefer adding narrow repository methods over embedding SQL in route handlers.
- Keep route handlers thin; put business behavior in `app/services/`.
- Keep database persistence concerns in `app/db/`.
- Keep request and response contracts in `app/schemas.py`.
- Keep timezone parsing/formatting behavior centralized in `app/utils/timezone.py`.

## 4. File Operations

- Prefer editing existing files over creating new ones.
- Only create new files when functionality clearly requires it and the file matches the project structure.
- Do not delete files unless certain they are unused.
- Do not move files or reorganize packages as part of a small feature or bug fix.
- Avoid generated-file churn and formatting-only changes outside the requested scope.

## 5. Dependencies

- Do not add new dependencies unless necessary.
- Prefer Python standard library functionality when it is adequate.
- If adding a dependency, choose a widely adopted, stable library.
- Update `pyproject.toml` and the lockfile consistently when dependencies change.
- Justify the dependency in the change summary or relevant code comment.
- Do not add overlapping libraries for HTTP, scheduling, database access, parsing, or settings when the project already has one.

## 6. Testing and Validation

There is no dedicated test suite in this repository at the time of writing. When making
changes, use the strongest available validation for the scope.

Recommended validation:

- Run `.\.venv\Scripts\python.exe -m compileall app` for Python syntax/import-level checks.
- For schema or service changes, create a small local smoke test where practical.
- For database changes, add an Alembic migration and verify `alembic upgrade head`.
- For Docker/runtime changes, run `docker compose up -d --build` where practical.
- For endpoint changes, validate request/response behavior with curl, FastAPI docs, or a small script.

Add tests only if the project already has a test structure or the change justifies creating one.
If tests cannot be run, clearly state what was and was not validated.

## 7. Documentation

- Update documentation when behavior changes.
- Keep documentation concise and accurate.
- Do not add excessive comments.
- Comments should explain why, not what.
- Update `README.md` for user-facing API, configuration, deployment, or operational changes.
- Update this file when repository conventions, deployment behavior, or agent expectations change.

## 8. Error Handling

- Do not suppress errors silently.
- Use existing error handling patterns.
- Provide meaningful error messages.
- Avoid overly broad exception handling.
- Preserve logging context for download, parsing, database, scheduler, and API validation failures.
- In async session code, respect commit/rollback boundaries and existing `session_scope` behavior.

## 9. Security

- Never expose secrets or credentials.
- Do not hardcode sensitive data.
- Respect authentication and authorization boundaries if they are added later.
- Validate external inputs where applicable.
- Treat XMLTV source URLs and downloaded XML as external input.
- Do not log full secret-bearing URLs, credentials, request tokens, or SSH material.
- Keep GitHub Actions secrets referenced through `${{ secrets.* }}` only.

## 10. Performance

- Avoid premature optimization.
- Do not introduce significant inefficiencies.
- Be mindful of:
  - unnecessary loops over large programme lists
  - excessive database round trips
  - loading full XML files when streaming is available
  - blocking operations in async request paths
  - large in-memory batches beyond configured chunk sizes
- Preserve the sequential, bounded-memory fetch pipeline unless a change explicitly targets it.
- Use database indexes and existing query shapes for channel/time lookups.

## 11. Git and Version Control

- Do not rewrite history unless explicitly required.
- Keep changes atomic and logically grouped.
- Do not modify unrelated files.
- Inspect `git status` before committing.
- Preserve user changes already present in the working tree.
- Do not use destructive commands such as `git reset --hard` or `git checkout --` unless explicitly requested.
- Be aware that pushes to `master` trigger deployment.

## 12. Clean Code Principles

Apply clean code principles pragmatically:

- Use clear names for functions, variables, models, and response fields.
- Keep functions focused on one responsibility.
- Prefer simple data flow over clever abstractions.
- Make side effects explicit.
- Keep domain logic readable without requiring route-handler context.
- Avoid hidden coupling between services, repositories, and schemas.
- Favor small, composable helpers when they remove real duplication.
- Do not add abstraction for hypothetical future use.

## 13. SOLID, YAGNI, and KISS

Use these principles as guardrails, not ceremony.

SOLID:

- Single Responsibility: keep routes, services, repositories, schemas, and utilities distinct.
- Open/Closed: extend behavior through narrow additions instead of risky rewrites.
- Liskov Substitution: keep replacement implementations compatible with existing contracts.
- Interface Segregation: expose focused methods instead of large catch-all APIs.
- Dependency Inversion: route and service code should depend on clear repository/service boundaries rather than concrete infrastructure details when practical.

YAGNI:

- Do not build features, abstractions, configuration knobs, or extension points before they are needed.
- Do not add generic frameworks around the fetch pipeline, scheduler, or repositories for one-off changes.

KISS:

- Prefer direct, obvious code.
- Keep API responses and persistence logic easy to trace.
- Avoid clever metaprogramming or dynamic behavior unless the existing code already uses it.

## 14. Database and Migrations

- ORM models live in `app/db/orm_models.py`.
- Repository methods live in `app/db/repository.py`.
- Alembic migrations live in `alembic/versions/`.
- Add a migration for schema changes.
- Do not mutate production data shape only through ORM changes.
- Keep migrations forward-compatible and explicit.
- Preserve existing data where possible.
- Be careful with destructive deletes and retention logic in the fetch pipeline.

## 15. API Contracts

- Public response and request models are defined in `app/schemas.py`.
- Changing response fields can break clients; do so only when requested.
- Document user-facing API changes in `README.md`.
- Preserve timezone behavior through `app/utils/timezone.py`.
- `/epg` returns programme data grouped by requested `xmltv_id`.
- `/stats` exposes service import status and scheduler timing.
- `/health` should remain lightweight and reflect API/database availability.

## 16. When Requirements Are Unclear

- Infer intent from code and context.
- Choose the safest, most conservative approach.
- Do not invent features or assumptions.
- Prefer inspecting local code over guessing.
- Ask for clarification only when a reasonable conservative choice would be risky.

## 17. Forbidden Actions

Agents must not:

- introduce breaking changes without clear reason
- modify infrastructure/configuration blindly
- add large refactors without necessity
- remove code that may be in use
- ignore existing conventions
- hardcode secrets or deployment credentials
- bypass Alembic for database schema changes
- replace established libraries without explicit need
- turn small fixes into broad rewrites

## 18. Recommended Workflow

1. Explore the repository.
2. Locate relevant code.
3. Understand current behavior.
4. Identify public API and persistence impact.
5. Apply the minimal change.
6. Validate with compile, smoke tests, migrations, or Docker as appropriate.
7. Update documentation if needed.
8. Summarize changed files, validation performed, and remaining risk.

## 19. Extensibility

This file may be extended with:

- project-specific rules
- service-specific instructions
- environment-specific constraints
- testing conventions if a test suite is added
- deployment safeguards if the release process changes

More specific `AGENTS.md` files in subdirectories, if added later, take precedence over
this file for files under those directories.

## Summary

This file enforces:

- safety
- consistency
- minimalism
- respect for existing code
- clear API boundaries
- careful deployment behavior

Agents should work like maintainers: inspect first, change narrowly, validate honestly,
and preserve the operational behavior of the service.
