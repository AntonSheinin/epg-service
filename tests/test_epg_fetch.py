from __future__ import annotations

import unittest
from datetime import datetime, timezone

from app.services.epg_fetch import SourceSummary


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


if __name__ == "__main__":
    unittest.main()
