from __future__ import annotations

import tempfile
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from run_demo import EXERCISE_DIR, run_pipeline


class DuckDBWarehouseTests(unittest.TestCase):
    def test_classifies_rows_and_rebuilds_outputs_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_dir = Path(directory)
            first = run_pipeline(EXERCISE_DIR / "events.csv", output_dir)
            first_csv = (output_dir / "gold_daily_service.csv").read_bytes()
            second = run_pipeline(EXERCISE_DIR / "events.csv", output_dir)

            self.assertEqual(first, second)
            self.assertEqual(first["bronze_events"], 7)
            self.assertEqual(first["silver_events"], 3)
            self.assertEqual(first["quarantine_events"], 4)
            self.assertEqual(
                [row["reason"] for row in first["quarantine_rows"]],
                [
                    "superseded_duplicate",
                    "invalid_timestamp",
                    "blank_service",
                    "invalid_duration",
                ],
            )
            self.assertEqual(
                (output_dir / "gold_daily_service.csv").read_bytes(),
                first_csv,
            )
            self.assertTrue((output_dir / "warehouse.duckdb").is_file())

    def test_missing_input_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(FileNotFoundError, "does not exist"):
                run_pipeline(Path(directory) / "missing.csv", Path(directory) / "output")


if __name__ == "__main__":
    unittest.main()
