from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.services.xmltv_parser import XMLTVProgramBatchReader


XMLTV = """\
<tv>
  <channel id="channel-1"><display-name>Channel One</display-name></channel>
  <programme channel="channel-1" start="20260919100000 +0000" stop="20260919110000 +0000">
    <title>Programme One</title>
  </programme>
</tv>
"""

MULTILINGUAL_XMLTV = """\
<tv>
  <channel id="channel-1"><display-name>Channel One</display-name></channel>
  <programme channel="channel-1" start="20260919100000 +0000" stop="20260919110000 +0000">
    <title>Programme One</title>
    <desc lang=" RU ">Первое описание</desc>
    <desc lang="en_US">English <b>nested</b> text</desc>
    <desc>Unlabelled fallback</desc>
    <desc lang=" ">Blank-language fallback</desc>
    <desc lang="ru">   </desc>
    <desc lang="ru">Второе описание</desc>
  </programme>
</tv>
"""


class XMLTVTimeCorrectionTests(unittest.TestCase):
    def test_zero_offset_preserves_parsed_time(self) -> None:
        program = self._read_one(timedelta(0))

        self.assertEqual(
            program.start_time,
            datetime(2026, 9, 19, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(
            program.stop_time,
            datetime(2026, 9, 19, 11, tzinfo=timezone.utc),
        )

    def test_positive_and_negative_offsets_shift_both_times(self) -> None:
        positive = self._read_one(timedelta(hours=2))
        negative = self._read_one(timedelta(hours=-3))

        self.assertEqual(positive.start_time.hour, 12)
        self.assertEqual(positive.stop_time.hour, 13)
        self.assertEqual(negative.start_time.hour, 7)
        self.assertEqual(negative.stop_time.hour, 8)
        self.assertEqual(
            positive.stop_time - positive.start_time,
            timedelta(hours=1),
        )

    def test_correction_is_applied_before_window_filtering(self) -> None:
        program = self._read_one(
            timedelta(hours=2),
            time_from=datetime(2026, 9, 19, 11, tzinfo=timezone.utc),
            time_to=datetime(2026, 9, 19, 13, tzinfo=timezone.utc),
        )

        self.assertEqual(program.start_time.hour, 12)

    def test_corrected_start_time_changes_deterministic_id(self) -> None:
        original = self._read_one(timedelta(0))
        corrected = self._read_one(timedelta(hours=1))

        self.assertNotEqual(original.id, corrected.id)

    def _read_one(
        self,
        offset: timedelta,
        *,
        time_from: datetime | None = None,
        time_to: datetime | None = None,
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "epg.xml"
            path.write_text(XMLTV, encoding="utf-8")
            reader = XMLTVProgramBatchReader(
                path,
                known_channel_ids={"channel-1"},
                time_from=time_from,
                time_to=time_to,
                time_offset=offset,
                batch_size=10,
            )
            try:
                batch = reader.read_next_batch()
            finally:
                reader.close()

        self.assertEqual(len(batch.programs), 1)
        return batch.programs[0]


class XMLTVDescriptionTests(unittest.TestCase):
    def test_preserves_usable_descriptions_and_language_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "epg.xml"
            path.write_text(MULTILINGUAL_XMLTV, encoding="utf-8")
            reader = XMLTVProgramBatchReader(
                path,
                known_channel_ids={"channel-1"},
                time_from=None,
                time_to=None,
                time_offset=timedelta(0),
                batch_size=10,
            )
            try:
                batch = reader.read_next_batch()
            finally:
                reader.close()

        self.assertEqual(len(batch.programs), 1)
        descriptions = batch.programs[0].descriptions
        self.assertEqual(
            [(item.language, item.text) for item in descriptions],
            [
                ("ru", "Первое описание"),
                ("en-us", "English nested text"),
                (None, "Unlabelled fallback"),
                (None, "Blank-language fallback"),
                ("ru", "Второе описание"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
