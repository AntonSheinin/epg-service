from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.models import LocalizedDescription, Program
from app.schemas import EPGRequest
from app.services.epg_query import get_epg_data, select_description


class DescriptionSelectorTests(unittest.TestCase):
    def test_selects_each_supported_language_exactly(self) -> None:
        descriptions = (
            LocalizedDescription("ru", "Russian"),
            LocalizedDescription("en", "English"),
            LocalizedDescription("he", "Hebrew"),
        )

        for language, expected in (("ru", "Russian"), ("en", "English"), ("he", "Hebrew")):
            with self.subTest(language=language):
                self.assertEqual(select_description(descriptions, language), expected)

    def test_uses_first_duplicate_exact_match(self) -> None:
        descriptions = (
            LocalizedDescription("ru", "First"),
            LocalizedDescription("ru", "Second"),
        )

        self.assertEqual(select_description(descriptions, "ru"), "First")

    def test_aliases_and_regional_tags_do_not_match(self) -> None:
        descriptions = (
            LocalizedDescription("en", "Fallback"),
            LocalizedDescription("rus", "Alias"),
            LocalizedDescription("ru-ru", "Regional"),
        )

        self.assertEqual(select_description(descriptions, "ru"), "Fallback")

    def test_missing_or_unavailable_preference_uses_first_usable(self) -> None:
        descriptions = (
            LocalizedDescription(None, "  First fallback  "),
            LocalizedDescription("en", "English"),
        )

        self.assertEqual(select_description(descriptions, None), "First fallback")
        self.assertEqual(select_description(descriptions, "he"), "First fallback")

    def test_ignores_empty_descriptions(self) -> None:
        descriptions = (
            LocalizedDescription("ru", "   "),
            LocalizedDescription("en", "English"),
        )

        self.assertEqual(select_description(descriptions, "ru"), "English")
        self.assertIsNone(
            select_description((LocalizedDescription("ru", "\n\t"),), "ru")
        )


class _FakeRepository:
    def __init__(self, programs: list[Program]) -> None:
        self.programs = programs

    async def get_last_epg_update_at(self) -> datetime | None:
        return None

    async def list_programs_for_channel(
        self,
        channel_id: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[Program]:
        return [program for program in self.programs if program.xmltv_channel_id == channel_id]


class EPGQueryDescriptionTests(unittest.IsolatedAsyncioTestCase):
    async def test_response_contains_selected_singular_description(self) -> None:
        program = Program(
            id="program-1",
            xmltv_channel_id="channel-1",
            start_time=datetime(2026, 9, 20, 10, tzinfo=timezone.utc),
            stop_time=datetime(2026, 9, 20, 11, tzinfo=timezone.utc),
            title="Programme",
            descriptions=(
                LocalizedDescription("ru", "Описание"),
                LocalizedDescription("en", "Description"),
            ),
        )
        request = EPGRequest.model_validate(
            {
                "channels": [{"xmltv_id": "channel-1"}],
                "timezone": "UTC",
                "from_date": "2026-09-20T00:00:00Z",
                "to_date": "2026-09-21T00:00:00Z",
                "preferred_description_language": "en",
            }
        )

        response = await get_epg_data(_FakeRepository([program]), request)  # type: ignore[arg-type]

        self.assertEqual(response.epg["channel-1"][0].description, "Description")


if __name__ == "__main__":
    unittest.main()
