from __future__ import annotations

import unittest

from pydantic import ValidationError

from app.schemas import EPGRequest


BASE_REQUEST = {
    "channels": [{"xmltv_id": "channel-1"}],
    "timezone": "UTC",
    "from_date": "2026-09-20T00:00:00Z",
    "to_date": "2026-09-21T00:00:00Z",
}


class EPGRequestDescriptionLanguageTests(unittest.TestCase):
    def test_omitted_and_null_preference_are_accepted(self) -> None:
        self.assertIsNone(EPGRequest.model_validate(BASE_REQUEST).preferred_description_language)

        payload = {**BASE_REQUEST, "preferred_description_language": None}
        self.assertIsNone(EPGRequest.model_validate(payload).preferred_description_language)

    def test_exact_supported_values_are_accepted(self) -> None:
        for language in ("ru", "en", "he"):
            with self.subTest(language=language):
                payload = {**BASE_REQUEST, "preferred_description_language": language}
                request = EPGRequest.model_validate(payload)
                self.assertEqual(request.preferred_description_language, language)

    def test_unsupported_values_are_rejected(self) -> None:
        for value in ("rus", "eng", "iw", "en-US", "RU", " en", "", 1):
            with self.subTest(value=value):
                payload = {**BASE_REQUEST, "preferred_description_language": value}
                with self.assertRaises(ValidationError):
                    EPGRequest.model_validate(payload)


if __name__ == "__main__":
    unittest.main()
