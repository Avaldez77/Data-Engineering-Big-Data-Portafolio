from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any


EXERCISE_DIR = Path(__file__).parent


def run_pipeline() -> dict[str, Any]:
    connection = sqlite3.connect(":memory:")
    try:
        connection.executescript((EXERCISE_DIR / "schema.sql").read_text(encoding="utf-8"))
        connection.executescript((EXERCISE_DIR / "pipeline.sql").read_text(encoding="utf-8"))
        bronze_count = connection.execute("SELECT COUNT(*) FROM bronze_events").fetchone()[0]
        silver_count = connection.execute("SELECT COUNT(*) FROM silver_events").fetchone()[0]
        quarantine_rows = connection.execute(
            """
            SELECT event_id, rejection_reason
            FROM quarantine_events
            ORDER BY source_row
            """
        ).fetchall()
        gold_rows = connection.execute(
            """
            SELECT event_date, service, event_count, error_count, error_rate, avg_duration_ms
            FROM gold_daily_service
            ORDER BY event_date, service
            """
        ).fetchall()
        columns = ("event_date", "service", "event_count", "error_count", "error_rate", "avg_duration_ms")
        return {
            "bronze_rows": bronze_count,
            "silver_rows": silver_count,
            "quarantine_rows": [
                {"event_id": event_id, "reason": reason}
                for event_id, reason in quarantine_rows
            ],
            "gold_rows": [dict(zip(columns, row, strict=True)) for row in gold_rows],
        }
    finally:
        connection.close()


if __name__ == "__main__":
    print(json.dumps(run_pipeline(), indent=2))
