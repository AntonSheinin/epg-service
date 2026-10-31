from __future__ import annotations

import tempfile
import unittest
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx

from app.config import EpgSourceConfig
from app.services import xmltv_parser
from app.services.epg_fetch import EPGFetchPipeline, SourceSummary


class SourceSummaryTests(unittest.TestCase):
    def test_public_payload_contains_name_and_offset_but_no_url(self) -> None:
        timestamp = datetime(2026, 9, 19, tzinfo=timezone.utc)
        payload = SourceSummary(
            index=1,
            source_name="Primary",
            time_offset_hours=-3,
            started_at=timestamp,
            completed_at=timestamp,
            status="success",
        ).to_dict()

        self.assertEqual(payload["source_name"], "Primary")
        self.assertEqual(payload["time_offset_hours"], -3)
        self.assertNotIn("source_url", payload)
        self.assertNotIn("sanitized_url", payload)


class ParseBudgetTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_download_and_storage_do_not_consume_parsing_budget(self) -> None:
        summary, stored, logs = await self._run_import(timeout=10, programme_seconds=2)
        self.assertEqual(summary.status, "success")
        self.assertEqual(stored, 3)
        self.assertIn("parse_budget_remaining=2.00s", logs)

    async def test_parsing_budget_is_shared_across_channels_and_batches(self) -> None:
        summary, stored, logs = await self._run_import(timeout=10, programme_seconds=4)
        # Channel parsing takes 2s; two 4s batches exhaust the budget.
        self.assertEqual(summary.status, "failed")
        self.assertEqual(stored, 1)
        self.assertIn("outcome=failed, stage=programs_parse", logs)
        self.assertIn("programs_parse=8.00s", logs)
        self.assertIn("affected=1", logs)
        self.assertIn("committed_changes=True", logs)

    async def test_zero_disables_parsing_budget(self) -> None:
        summary, stored, logs = await self._run_import(timeout=0, programme_seconds=1000)
        self.assertEqual(summary.status, "success")
        self.assertEqual(stored, 3)
        self.assertIn("parse_budget_remaining=disabled", logs)

    async def _run_import(self, *, timeout: int, programme_seconds: float):
        clock = 0.0

        async def download(*args):
            nonlocal clock
            clock += 1000
            return path

        def parse_channels(*args, **kwargs):
            nonlocal clock
            channels = xmltv_parser.parse_xmltv_channels(*args, **kwargs)
            clock += 2
            return channels

        parse_program = xmltv_parser._parse_single_program

        def parse_slow_program(*args, **kwargs):
            nonlocal clock
            program = parse_program(*args, **kwargs)
            clock += programme_seconds
            return program

        async def store_channels(*args):
            nonlocal clock
            clock += 2000

        async def store_programs(programs):
            nonlocal clock
            clock += 3000
            return len(programs), {"channel-1"}

        session = SimpleNamespace(commit=AsyncMock())

        @asynccontextmanager
        async def session_scope(**kwargs):
            yield session

        repo = SimpleNamespace(
            upsert_channels=AsyncMock(side_effect=store_channels),
            upsert_programs=AsyncMock(side_effect=store_programs),
        )
        source = EpgSourceConfig(name="Primary", url="https://example.com/epg.xml")
        pipeline = EPGFetchPipeline([source])
        pipeline._parse_timeout = timeout
        context = pipeline._build_context()
        module = "app.services.epg_fetch"
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "epg.xml"
            start = context.started_at.strftime("%Y%m%d%H%M%S +0000")
            programme = (
                f'<programme channel="channel-1" start="{start}" stop="{start}">'
                '<title>Test</title></programme>'
            )
            path.write_text(
                '<tv><channel id="channel-1"><display-name>One</display-name></channel>'
                + programme * 3 + '</tv>', encoding="utf-8",
            )
            with (
                patch(f"{module}.perf_counter", side_effect=lambda: clock),
                patch("app.services.xmltv_parser.perf_counter", side_effect=lambda: clock),
                patch(f"{module}.process_single_source", side_effect=download),
                patch(f"{module}.parse_xmltv_channels", side_effect=parse_channels),
                patch("app.services.xmltv_parser._parse_single_program", side_effect=parse_slow_program),
                patch(f"{module}.session_scope", session_scope),
                patch(f"{module}.SqlAlchemyEpgRepository", return_value=repo),
                patch(f"{module}.settings.epg_programs_chunk_size", 1),
                self.assertLogs(module, level="INFO") as captured,
            ):
                summary, changed = await pipeline._process_source(1, source, context)
            self.assertEqual(changed, {"channel-1"})
            self.assertFalse(path.exists())
        return summary, repo.upsert_programs.await_count, "\n".join(captured.output)


class SourceLoggingTests(unittest.IsolatedAsyncioTestCase):
    async def test_download_failure_logs_stage_without_source_url(self) -> None:
        source = EpgSourceConfig(name="Primary", url="https://example.com/private-token")
        pipeline = EPGFetchPipeline([source])
        module = "app.services.epg_fetch"
        with (
            patch(f"{module}.process_single_source", AsyncMock(side_effect=httpx.ReadTimeout(str(source.url)))),
            self.assertLogs(module, level="INFO") as captured,
        ):
            summary, _ = await pipeline._process_source(1, source, pipeline._build_context())
        logs = "\n".join(captured.output)
        self.assertEqual(summary.status, "failed")
        self.assertIn("Source download timed out (stage=download)", logs)
        self.assertIn("outcome=failed, stage=download", logs)
        self.assertIn("batches=0", logs)
        self.assertNotIn("private-token", logs)


if __name__ == "__main__":
    unittest.main()
