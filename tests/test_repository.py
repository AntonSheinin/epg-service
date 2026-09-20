from __future__ import annotations

import unittest

from app.db.repository import _deserialize_descriptions, _serialize_descriptions
from app.models import LocalizedDescription


class DescriptionStorageBoundaryTests(unittest.TestCase):
    def test_serialization_preserves_order_and_shape(self) -> None:
        descriptions = (
            LocalizedDescription("ru", "Первое"),
            LocalizedDescription(None, "Fallback"),
        )

        self.assertEqual(
            _serialize_descriptions(descriptions),
            [
                {"language": "ru", "text": "Первое"},
                {"language": None, "text": "Fallback"},
            ],
        )

    def test_deserialization_normalizes_and_preserves_valid_order(self) -> None:
        value = [
            {"language": " RU ", "text": "  Первое  ", "extra": True},
            {"language": "en_US", "text": "English"},
            {"text": "Fallback"},
        ]

        self.assertEqual(
            _deserialize_descriptions(value),
            (
                LocalizedDescription("ru", "Первое"),
                LocalizedDescription("en-us", "English"),
                LocalizedDescription(None, "Fallback"),
            ),
        )

    def test_deserialization_tolerates_unexpected_data(self) -> None:
        self.assertEqual(_deserialize_descriptions({"text": "not-a-list"}), ())
        self.assertEqual(
            _deserialize_descriptions(
                [
                    "not-an-object",
                    {"language": "ru"},
                    {"language": "ru", "text": 123},
                    {"language": "ru", "text": "   "},
                    {"language": 123, "text": "Usable"},
                    {"language": " ", "text": "Also usable"},
                ]
            ),
            (
                LocalizedDescription(None, "Usable"),
                LocalizedDescription(None, "Also usable"),
            ),
        )


if __name__ == "__main__":
    unittest.main()
