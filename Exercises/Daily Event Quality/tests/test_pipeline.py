from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from run_demo import run_pipeline


class DailyEventQualityTests(unittest.TestCase):
    def test_deduplicates_validates_and_aggregates_events(self) -> None:
        result = run_pipeline()

        self.assertEqual(result["bronze_rows"], 7)
        self.assertEqual(result["silver_rows"], 3)
        self.assertEqual(
            result["gold_rows"],
            [
                {
                    "event_date": "2026-01-01",
                    "service": "checkout",
                    "event_count": 2,
                    "error_count": 0,
                    "error_rate": 0.0,
                    "avg_duration_ms": 115.0,
                },
                {
                    "event_date": "2026-01-02",
                    "service": "inventory",
                    "event_count": 1,
                    "error_count": 1,
                    "error_rate": 1.0,
                    "avg_duration_ms": 900.0,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
