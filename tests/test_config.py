from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from pydantic import ValidationError

from app.config import EpgSourcesConfig, load_epg_sources


class EpgSourcesConfigTests(unittest.TestCase):
    def test_loads_valid_file_and_defaults_offset(self) -> None:
        config = self._load(
            """
[[sources]]
name = " Primary "
url = "https://example.com/epg.xml.gz"
"""
        )

        self.assertEqual(config.sources[0].name, "Primary")
        self.assertEqual(config.sources[0].time_offset_hours, 0.0)

    def test_rejects_missing_file(self) -> None:
        missing = Path(tempfile.gettempdir()) / "missing-epg-sources.toml"
        with self.assertRaisesRegex(ValueError, "configuration not found"):
            load_epg_sources(missing)

    def test_rejects_invalid_toml_without_echoing_content(self) -> None:
        marker = "private-source-token"
        with self.assertRaises(ValueError) as raised:
            self._load(f'url = "{marker}"\ninvalid = [')

        self.assertNotIn(marker, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_rejects_empty_sources(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 1 item"):
            self._load("sources = []")

    def test_rejects_unknown_fields(self) -> None:
        with self.assertRaisesRegex(ValueError, "Extra inputs are not permitted"):
            self._load(
                """
[[sources]]
name = "Primary"
url = "https://example.com/epg.xml.gz"
unknown = true
"""
            )

    def test_rejects_duplicate_names_case_insensitively(self) -> None:
        with self.assertRaisesRegex(ValueError, "names must be unique"):
            self._load(
                """
[[sources]]
name = "Primary"
url = "https://example.com/one.xml"

[[sources]]
name = "primary"
url = "https://example.com/two.xml"
"""
            )

    def test_rejects_duplicate_normalized_urls(self) -> None:
        with self.assertRaisesRegex(ValueError, "URLs must be unique"):
            self._load(
                """
[[sources]]
name = "One"
url = "https://example.com"

[[sources]]
name = "Two"
url = "https://example.com/"
"""
            )

    def test_rejects_non_http_url_without_echoing_it(self) -> None:
        marker = "private-source-token"
        with self.assertRaises(ValueError) as raised:
            self._load(
                f"""
[[sources]]
name = "Primary"
url = "ftp://example.com/{marker}"
"""
            )

        self.assertNotIn(marker, str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_rejects_non_strict_and_out_of_range_offsets(self) -> None:
        base = {
            "sources": [
                {
                    "name": "Primary",
                    "url": "https://example.com/epg.xml",
                }
            ]
        }
        for value in ("3", True, math.nan, math.inf, -25, 25):
            with self.subTest(value=value):
                base["sources"][0]["time_offset_hours"] = value
                with self.assertRaises(ValidationError):
                    EpgSourcesConfig.model_validate(base)

    def test_accepts_integer_fractional_and_negative_offsets(self) -> None:
        for value in (0, 5.5, -3):
            with self.subTest(value=value):
                config = EpgSourcesConfig.model_validate(
                    {
                        "sources": [
                            {
                                "name": "Primary",
                                "url": "https://example.com/epg.xml",
                                "time_offset_hours": value,
                            }
                        ]
                    }
                )
                self.assertEqual(config.sources[0].time_offset_hours, value)

    def _load(self, content: str) -> EpgSourcesConfig:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sources.toml"
            path.write_text(content, encoding="utf-8")
            return load_epg_sources(path)


if __name__ == "__main__":
    unittest.main()
