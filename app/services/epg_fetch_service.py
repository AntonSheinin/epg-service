"""
EPG Fetching Service

Coordinates downloading, parsing, and persistence of EPG data from multiple sources.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal, Sequence

from app.config import settings
from app.database import session_scope
from app.services.db_service import delete_old_programs, store_channels, store_programs
from app.services.epg_downloader_service import process_single_source
from app.services.fetch_types import ChannelPayload, ProgramPayload


logger = logging.getLogger(__name__)

# Global lock to prevent concurrent fetch operations
_fetch_lock = asyncio.Lock()


@dataclass(slots=True)
class FetchContext:
    started_at: datetime
    archive_cutoff: datetime
    fetch_start: datetime
    fetch_end: datetime


@dataclass(slots=True)
class SourceSummary:
    index: int
    source_url: str
    sanitized_url: str
    started_at: datetime
    completed_at: datetime
    status: Literal["success", "failed"]
    channels_parsed: int = 0
    programs_parsed: int = 0
    programs_inserted: int = 0
    error: str | None = None
    channels: list[ChannelPayload] = field(default_factory=list)
    programs: list[ProgramPayload] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.completed_at - self.started_at).total_seconds())

    def to_dict(self) -> dict:
        payload = {
            "source_index": self.index,
            "source_url": self.source_url,
            "sanitized_url": self.sanitized_url,
            "status": self.status,
            "channels_parsed": self.channels_parsed,
            "programs_parsed": self.programs_parsed,
            "programs_inserted": self.programs_inserted,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat(),
            "duration_seconds": self.duration_seconds,
        }
        if self.error:
            payload["error"] = self.error
        return payload


class EPGFetchPipeline:
    """Coordinates download, merge, and persistence stages for a fetch cycle."""

    def __init__(self, sources: Sequence[str], *, max_concurrency: int | None = None) -> None:
        self.sources = [source for source in sources if source]
        self.total_sources = len(self.sources)
        self._concurrency = max(1, max_concurrency or min(4, self.total_sources or 1))
        self._semaphore = asyncio.Semaphore(self._concurrency)

    async def run(self) -> dict:
        context = self._build_context()
        logger.info(
            "Time boundaries - Archive cutoff: %s, Fetch window: %s -> %s",
            context.archive_cutoff.date(),
            context.fetch_start.isoformat(),
            context.fetch_end.isoformat(),
        )

        deleted_count = await self._cleanup_old_programs(context.archive_cutoff)
        summaries = await self._collect_sources(context)
        programs_inserted = await self._persist_sources(summaries)

        return self._build_result(context, deleted_count, programs_inserted, summaries)

    def _build_context(self) -> FetchContext:
        started_at = datetime.now(timezone.utc)
        archive_cutoff = started_at - timedelta(days=settings.max_epg_depth)
        fetch_start = started_at.replace(hour=0, minute=0, second=0, microsecond=0)
        fetch_end = started_at + timedelta(days=settings.max_future_epg_limit)
        return FetchContext(
            started_at=started_at,
            archive_cutoff=archive_cutoff,
            fetch_start=fetch_start,
            fetch_end=fetch_end,
        )

    async def _cleanup_old_programs(self, cutoff_time: datetime) -> int:
        logger.info("Deleting programs before %s...", cutoff_time.date())
        try:
            async with session_scope() as session:
                deleted = await delete_old_programs(session, cutoff_time)
        except RuntimeError as exc:
            logger.error("Database not initialized: %s", exc)
            raise
        logger.info("Deleted %s old programs", deleted)
        return deleted

    async def _collect_sources(self, context: FetchContext) -> list[SourceSummary]:
        if not self.sources:
            logger.warning("No EPG sources configured - skipping fetch cycle")
            return []

        tasks = [
            asyncio.create_task(self._process_source(index, source_url, context))
            for index, source_url in enumerate(self.sources, start=1)
        ]

        summaries = await asyncio.gather(*tasks)
        summaries.sort(key=lambda summary: summary.index)
        return summaries

    async def _process_source(
        self,
        index: int,
        source_url: str,
        context: FetchContext
    ) -> SourceSummary:
        sanitized_url = _sanitize_url_for_logging(source_url)
        started_at = datetime.now(timezone.utc)
        logger.info(
            "[Source %s/%s] Queued for download: %s",
            index,
            self.total_sources or len(self.sources),
            sanitized_url,
        )

        async with self._semaphore:
            logger.info(
                "[Source %s/%s] Starting download: %s",
                index,
                self.total_sources or len(self.sources),
                sanitized_url,
            )
            try:
                channels, programs = await process_single_source(
                    source_url,
                    index,
                    context.fetch_start,
                    context.fetch_end,
                )
            except Exception as exc:
                completed_at = datetime.now(timezone.utc)
                logger.error(
                    "[Source %s] Failed to process %s: %s",
                    index,
                    sanitized_url,
                    exc,
                    exc_info=True,
                )
                return SourceSummary(
                    index=index,
                    source_url=source_url,
                    sanitized_url=sanitized_url,
                    started_at=started_at,
                    completed_at=completed_at,
                    status="failed",
                    error=str(exc),
                )

        completed_at = datetime.now(timezone.utc)
        logger.info(
            "[Source %s/%s] Completed download: %s (%s channels, %s programs)",
            index,
            self.total_sources or len(self.sources),
            sanitized_url,
            len(channels),
            len(programs),
        )

        return SourceSummary(
            index=index,
            source_url=source_url,
            sanitized_url=sanitized_url,
            started_at=started_at,
            completed_at=completed_at,
            status="success",
            channels_parsed=len(channels),
            programs_parsed=len(programs),
            channels=list(channels),
            programs=list(programs),
        )

    async def _persist_sources(self, summaries: list[SourceSummary]) -> int:
        total_inserted = 0
        for summary in summaries:
            if summary.status != "success":
                continue

            logger.info(
                "[Source %s] Persisting %s channels and %s programs",
                summary.index,
                summary.channels_parsed,
                summary.programs_parsed,
            )

            try:
                async with session_scope() as session:
                    await store_channels(session, summary.channels)
                    inserted = await store_programs(session, summary.programs)
            except Exception as exc:
                logger.error(
                    "[Source %s] Failed to store data: %s",
                    summary.index,
                    exc,
                    exc_info=True,
                )
                summary.status = "failed"
                summary.error = str(exc)
                summary.programs_inserted = 0
                continue

            summary.programs_inserted = inserted
            total_inserted += inserted

            logger.info(
                "[Source %s] Stored %s programs (duplicates ignored automatically)",
                summary.index,
                inserted,
            )

            summary.channels.clear()
            summary.programs.clear()

        return total_inserted

    def _build_result(
        self,
        context: FetchContext,
        deleted_count: int,
        inserted_count: int,
        summaries: list[SourceSummary],
    ) -> dict:
        successes = sum(1 for summary in summaries if summary.status == "success")
        failures = sum(1 for summary in summaries if summary.status == "failed")

        return {
            "status": "success",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sources_processed": len(self.sources),
            "sources_succeeded": successes,
            "sources_failed": failures,
            "programs_inserted": inserted_count,
            "programs_deleted": deleted_count,
            "source_details": [summary.to_dict() for summary in summaries],
            "started_at": context.started_at.isoformat(),
        }


def _sanitize_url_for_logging(url: str) -> str:
    """Remove credentials from URL for safe logging."""
    if "://" not in url:
        return url
    try:
        protocol, rest = url.split("://", 1)
        if "@" in rest:
            rest = rest.split("@", 1)[1]
            return f"{protocol}://***:***@{rest}"
        return url
    except (ValueError, IndexError):
        return url


async def fetch_and_process() -> dict:
    """
    Main entry point for EPG fetching with concurrency protection.

    Returns:
        Dictionary with fetch statistics or error/skip message.
    """
    if _fetch_lock.locked():
        logger.warning("EPG fetch already in progress, skipping this request")
        return {
            "status": "skipped",
            "message": "EPG fetch operation already in progress",
        }

    async with _fetch_lock:
        logger.info("EPG fetch started at %s", datetime.now(timezone.utc).isoformat())

        if not settings.epg_sources:
            logger.warning("EPG_SOURCES not configured - fetch aborted")
            return {"error": "EPG_SOURCES not configured"}

        if not settings.database_path:
            logger.error("DATABASE_PATH not configured - fetch aborted")
            return {"error": "DATABASE_PATH not configured"}

        pipeline = EPGFetchPipeline(settings.epg_sources)
        try:
            result = await pipeline.run()
            logger.info("EPG fetch completed successfully")
            return result
        except RuntimeError as exc:
            logger.error("EPG fetch failed: %s", exc, exc_info=True)
            return {"error": str(exc)}
        except Exception as exc:  # Catch-all to ensure API stability
            logger.error("Unexpected error during EPG fetch: %s", exc, exc_info=True)
            return {"error": str(exc)}
