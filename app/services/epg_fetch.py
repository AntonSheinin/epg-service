"""
EPG Fetching Service.

Coordinates downloading, parsing, and persistence of EPG data from multiple sources.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Literal
from xml.etree.ElementTree import ParseError

import httpx
from sqlalchemy.exc import SQLAlchemyError

from app.config import EpgSourceConfig, epg_sources_config, settings
from app.db.repository import SqlAlchemyEpgRepository
from app.db.session import session_scope
from app.services.downloader import process_single_source
from app.services.xmltv_parser import XMLTVProgramBatchReader, parse_xmltv_channels
from app.utils.file_operations import cleanup_temp_file

logger = logging.getLogger(__name__)

# Global lock to prevent concurrent fetch operations
_fetch_lock = asyncio.Lock()


@contextmanager
def _time_source_stage(durations: dict[str, float], stage: str) -> Iterator[None]:
    started = perf_counter()
    try:
        yield
    finally:
        durations[stage] += perf_counter() - started


@dataclass(slots=True)
class FetchContext:
    started_at: datetime
    window_start: datetime
    window_end: datetime


@dataclass(slots=True)
class SourceSummary:
    index: int
    source_name: str
    time_offset_hours: float
    started_at: datetime
    completed_at: datetime
    status: Literal["success", "failed"]
    channels_parsed: int = 0
    programs_parsed: int = 0
    programs_inserted: int = 0
    committed_changes: bool = False
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        return max(0.0, (self.completed_at - self.started_at).total_seconds())

    def to_dict(self) -> dict:
        payload = {
            "source_index": self.index,
            "source_name": self.source_name,
            "time_offset_hours": self.time_offset_hours,
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


@dataclass(slots=True)
class CollectionResult:
    summaries: list[SourceSummary]
    programs_inserted: int
    last_updated_channels_count: int
    last_updated_sources_count: int
    last_recorded_update_at: datetime | None


class EPGFetchPipeline:
    """Coordinates download, parse, and persistence stages for a fetch cycle."""

    def __init__(self, sources: Sequence[EpgSourceConfig]) -> None:
        self.sources = tuple(sources)
        self.total_sources = len(self.sources)
        self._parse_timeout = settings.epg_parse_timeout_sec

    async def run(self) -> dict:
        context = self._build_context()
        logger.info(
            "Target window: %s -> %s (archive depth: %s days, future limit: %s days)",
            context.window_start.isoformat(),
            context.window_end.isoformat(),
            settings.max_epg_depth,
            settings.max_future_epg_limit,
        )
        logger.info(
            "Cumulative XML parsing timeout per source (download/storage excluded): %s",
            f"{self._parse_timeout}s" if self._parse_timeout else "disabled",
        )
        logger.info("Source processing mode: sequential")

        deleted_past, deleted_future = await self._trim_program_window(context)
        collection = await self._collect_sources(context)

        if collection.last_recorded_update_at is not None:
            await self._record_import_status(
                completed_at=collection.last_recorded_update_at,
                last_updated_channels_count=collection.last_updated_channels_count,
                last_updated_sources_count=collection.last_updated_sources_count,
            )

        return self._build_result(
            context,
            deleted_past,
            deleted_future,
            collection.programs_inserted,
            collection.last_updated_channels_count,
            collection.summaries,
        )

    def _build_context(self) -> FetchContext:
        started_at = datetime.now(timezone.utc)
        past_window_start = started_at - timedelta(days=settings.max_epg_depth)
        window_start = past_window_start.replace(hour=0, minute=0, second=0, microsecond=0)
        future_window_end = started_at + timedelta(days=settings.max_future_epg_limit)
        window_end = future_window_end.replace(hour=23, minute=59, second=59, microsecond=999999)
        return FetchContext(
            started_at=started_at,
            window_start=window_start,
            window_end=window_end,
        )

    async def _trim_program_window(self, context: FetchContext) -> tuple[int, int]:
        today_start = context.started_at.replace(hour=0, minute=0, second=0, microsecond=0)

        logger.info(
            "Trimming programs: deleting old programs (< %s) and all future programs (>= %s)",
            context.window_start.isoformat(),
            today_start.isoformat(),
        )
        try:
            async with session_scope(begin=False) as session:
                repo = SqlAlchemyEpgRepository(session)
                deleted_past = await repo.delete_old_programs(context.window_start)
                deleted_future = await repo.delete_future_programs(today_start, inclusive=True)
        except RuntimeError as exc:
            logger.error("Database not initialized: %s", exc)
            raise
        logger.info(
            "Deleted %s old programs and %s future programs (from today onwards)",
            deleted_past,
            deleted_future,
        )
        return deleted_past, deleted_future

    async def _collect_sources(self, context: FetchContext) -> CollectionResult:
        summaries: list[SourceSummary] = []
        total_inserted = 0
        updated_channel_ids: set[str] = set()
        updated_sources_count = 0
        last_recorded_update_at: datetime | None = None

        for index, source in enumerate(self.sources, start=1):
            summary, changed_channel_ids = await self._process_source(index, source, context)
            summaries.append(summary)

            total_inserted += summary.programs_inserted
            updated_channel_ids.update(changed_channel_ids)

            if summary.status == "success" or summary.committed_changes:
                updated_sources_count += 1
                if last_recorded_update_at is None or summary.completed_at > last_recorded_update_at:
                    last_recorded_update_at = summary.completed_at

        return CollectionResult(
            summaries=summaries,
            programs_inserted=total_inserted,
            last_updated_channels_count=len(updated_channel_ids),
            last_updated_sources_count=updated_sources_count,
            last_recorded_update_at=last_recorded_update_at,
        )

    async def _process_source(
        self,
        index: int,
        source: EpgSourceConfig,
        context: FetchContext,
    ) -> tuple[SourceSummary, set[str]]:
        started_at = datetime.now(timezone.utc)
        source_started = perf_counter()
        stage = "download"
        outcome = "interrupted"
        durations = dict.fromkeys(
            ("download", "channels_parse", "channels_store", "programs_parse", "programs_store"),
            0.0,
        )
        file_size_bytes = 0
        batch_number = 0
        changed_channel_ids: set[str] = set()
        temp_file = None
        program_reader: XMLTVProgramBatchReader | None = None
        skipped_unknown_channels = 0
        channels_parsed = 0
        programs_parsed = 0
        programs_inserted = 0
        committed_changes = False
        parse_remaining = float(self._parse_timeout) if self._parse_timeout else None

        logger.info(
            "[Source %s/%s: %s] Queued for download (time correction: %s hours)",
            index,
            self.total_sources,
            source.name,
            source.time_offset_hours,
        )
        logger.info(
            "[Source %s/%s: %s] Starting download",
            index,
            self.total_sources,
            source.name,
        )

        try:
            with _time_source_stage(durations, stage):
                temp_file = await process_single_source(str(source.url), index, source.name)
            file_size_bytes = temp_file.stat().st_size
            logger.info(
                "[Source %s/%s: %s] Download complete: elapsed=%.2fs, xml_bytes=%s",
                index,
                self.total_sources,
                source.name,
                durations["download"],
                file_size_bytes,
            )

            logger.info("[Source %s: %s] Parsing channel rows", index, source.name)
            stage = "channels_parse"
            with _time_source_stage(durations, stage):
                channels = await asyncio.to_thread(
                    parse_xmltv_channels,
                    temp_file,
                    deadline_monotonic=perf_counter() + parse_remaining
                    if parse_remaining is not None else None,
                )
            if parse_remaining is not None:
                parse_remaining -= durations[stage]
                if parse_remaining <= 0:
                    raise TimeoutError("Cumulative XML parsing budget exhausted")
            if not channels:
                raise ValueError("No channels found in XMLTV")

            known_channel_ids = {channel.xmltv_id for channel in channels}
            channels_parsed = len(channels)

            async with session_scope(begin=False) as session:
                repo = SqlAlchemyEpgRepository(session)

                logger.info(
                    "[Source %s: %s] Persisting %s channels",
                    index,
                    source.name,
                    len(channels),
                )
                stage = "channels_store"
                with _time_source_stage(durations, stage):
                    await repo.upsert_channels(channels)
                    await session.commit()
                committed_changes = True

                logger.info(
                    "[Source %s: %s] Starting program batch parsing",
                    index,
                    source.name,
                )
                stage = "programs_parse"
                with _time_source_stage(durations, stage):
                    program_reader = XMLTVProgramBatchReader(
                        temp_file,
                        known_channel_ids=known_channel_ids,
                        time_from=context.window_start,
                        time_to=context.window_end,
                        time_offset=timedelta(hours=source.time_offset_hours),
                        batch_size=settings.epg_programs_chunk_size,
                    )
                if parse_remaining is not None:
                    parse_remaining -= durations[stage]
                    if parse_remaining <= 0:
                        raise TimeoutError("Cumulative XML parsing budget exhausted")

                while True:
                    stage = "programs_parse"
                    parse_before = durations[stage]
                    logger.debug(
                        "[Source %s: %s] Reading program batch %s: parse_budget_remaining=%s",
                        index, source.name, batch_number + 1, parse_remaining,
                    )
                    with _time_source_stage(durations, stage):
                        batch = await asyncio.to_thread(
                            program_reader.read_next_batch,
                            deadline_monotonic=perf_counter() + parse_remaining
                            if parse_remaining is not None else None,
                        )
                    batch_parse_seconds = durations[stage] - parse_before
                    if parse_remaining is not None:
                        parse_remaining -= batch_parse_seconds
                        if parse_remaining <= 0:
                            raise TimeoutError("Cumulative XML parsing budget exhausted")
                    skipped_unknown_channels += batch.skipped_unknown_channels

                    if batch.programs:
                        batch_number += 1
                        programs_parsed += len(batch.programs)
                        stage = "programs_store"
                        store_before = durations[stage]
                        logger.debug(
                            "[Source %s: %s] Storing program batch %s: payload=%s",
                            index, source.name, batch_number, len(batch.programs),
                        )
                        with _time_source_stage(durations, stage):
                            upserted, batch_changed_channel_ids = await repo.upsert_programs(batch.programs)
                        batch_store_seconds = durations[stage] - store_before
                        programs_inserted += upserted
                        changed_channel_ids.update(batch_changed_channel_ids)
                        logger.info(
                            "[Source %s: %s] Stored program batch %s: payload=%s, affected=%s, "
                            "parse=%.2fs, store=%.2fs, source_elapsed=%.2fs, parse_budget_remaining=%s",
                            index,
                            source.name,
                            batch_number,
                            len(batch.programs),
                            upserted,
                            batch_parse_seconds,
                            batch_store_seconds,
                            perf_counter() - source_started,
                            f"{max(0.0, parse_remaining):.2f}s"
                            if parse_remaining is not None else "disabled",
                        )

                    if batch.reached_eof:
                        break

                if programs_parsed == 0:
                    raise ValueError("No programs found in XMLTV")

            completed_at = datetime.now(timezone.utc)
            outcome = "success"
            if skipped_unknown_channels:
                logger.warning(
                    "[Source %s] Skipped %s program(s) with unknown channel IDs",
                    index,
                    skipped_unknown_channels,
                )
            logger.info(
                "[Source %s/%s: %s] Completed source import (%s channels, %s programs, %s affected rows)",
                index,
                self.total_sources,
                source.name,
                channels_parsed,
                programs_parsed,
                programs_inserted,
            )
            return (
                SourceSummary(
                    index=index,
                    source_name=source.name,
                    time_offset_hours=source.time_offset_hours,
                    started_at=started_at,
                    completed_at=completed_at,
                    status="success",
                    channels_parsed=channels_parsed,
                    programs_parsed=programs_parsed,
                    programs_inserted=programs_inserted,
                    committed_changes=committed_changes,
                ),
                changed_channel_ids,
            )
        except (
            httpx.HTTPError,
            TimeoutError,
            ValueError,
            ParseError,
            SQLAlchemyError,
        ) as exc:
            outcome = "failed"
            completed_at = datetime.now(timezone.utc)
            safe_error = _safe_source_error(exc)
            logger.error(
                "[Source %s: %s] Failed to process source: %s (stage=%s)",
                index,
                source.name,
                safe_error,
                stage,
                exc_info=not isinstance(exc, httpx.HTTPError),
            )
            return (
                SourceSummary(
                    index=index,
                    source_name=source.name,
                    time_offset_hours=source.time_offset_hours,
                    started_at=started_at,
                    completed_at=completed_at,
                    status="failed",
                    channels_parsed=channels_parsed,
                    programs_parsed=programs_parsed,
                    programs_inserted=programs_inserted,
                    committed_changes=committed_changes,
                    error=safe_error,
                ),
                changed_channel_ids,
            )
        finally:
            logger.info(
                "[Source %s: %s] Source timing summary: outcome=%s, stage=%s, elapsed=%.2fs, "
                "timeout_budget=%ss, download=%.2fs, channels_parse=%.2fs, channels_store=%.2fs, "
                "programs_parse=%.2fs, programs_store=%.2fs, xml_bytes=%s, "
                "batches=%s, programs_parsed=%s, affected=%s, changed_channels=%s, "
                "skipped_unknown_channels=%s, committed_changes=%s",
                index, source.name, outcome, stage, perf_counter() - source_started,
                self._parse_timeout, durations["download"], durations["channels_parse"],
                durations["channels_store"], durations["programs_parse"], durations["programs_store"],
                file_size_bytes, batch_number, programs_parsed, programs_inserted,
                len(changed_channel_ids), skipped_unknown_channels, committed_changes,
            )
            if program_reader is not None:
                program_reader.close()
            if temp_file:
                cleanup_temp_file(temp_file)

    async def _record_import_status(
        self,
        *,
        completed_at: datetime,
        last_updated_channels_count: int,
        last_updated_sources_count: int,
    ) -> None:
        try:
            async with session_scope(begin=False) as session:
                repo = SqlAlchemyEpgRepository(session)
                await repo.upsert_import_status(
                    last_epg_update_at=completed_at,
                    last_updated_channels_count=last_updated_channels_count,
                    last_updated_sources_count=last_updated_sources_count,
                )
        except (SQLAlchemyError, RuntimeError) as exc:
            logger.error("Failed to persist import status: %s", exc, exc_info=True)

    def _build_result(
        self,
        context: FetchContext,
        deleted_past_count: int,
        deleted_future_count: int,
        inserted_count: int,
        last_updated_channels_count: int,
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
            "last_updated_channels_count": last_updated_channels_count,
            "programs_deleted": deleted_past_count + deleted_future_count,
            "programs_deleted_past": deleted_past_count,
            "programs_deleted_future": deleted_future_count,
            "source_details": [summary.to_dict() for summary in summaries],
            "started_at": context.started_at.isoformat(),
            "window_start": context.window_start.isoformat(),
            "window_end": context.window_end.isoformat(),
        }


def _safe_source_error(exc: Exception) -> str:
    """Return an operational error message that cannot expose a source URL."""
    if isinstance(exc, httpx.TimeoutException):
        return "Source download timed out"
    if isinstance(exc, httpx.HTTPError):
        return f"Source download failed ({type(exc).__name__})"
    if isinstance(exc, TimeoutError):
        return "Source processing timed out"
    if isinstance(exc, ParseError):
        return "Source returned invalid XML"
    if isinstance(exc, SQLAlchemyError):
        return "Database operation failed"
    return str(exc)


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
        pipeline = EPGFetchPipeline(epg_sources_config.sources)
        result = await pipeline.run()
        logger.info("EPG fetch completed successfully")
        return result
