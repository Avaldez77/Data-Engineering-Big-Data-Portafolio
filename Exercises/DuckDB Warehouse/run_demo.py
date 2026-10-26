from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import duckdb


EXERCISE_DIR = Path(__file__).parent


def write_query_csv(connection: duckdb.DuckDBPyConnection, query: str, output: Path) -> None:
    cursor = connection.execute(query)
    with output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow([column[0] for column in cursor.description])
        writer.writerows(cursor.fetchall())


def run_pipeline(input_path: Path, output_dir: Path) -> dict[str, Any]:
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV does not exist: {input_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    database_path = output_dir / "warehouse.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute(
            """
            CREATE OR REPLACE TABLE bronze_events AS
            SELECT ROW_NUMBER() OVER () AS source_row, *
            FROM read_csv(?, header = true, all_varchar = true)
            """,
            [str(input_path.resolve())],
        )
        connection.execute((EXERCISE_DIR / "pipeline.sql").read_text(encoding="utf-8"))

        counts = {
            table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in ("bronze_events", "silver_events", "quarantine_events")
        }
        quarantine_rows = connection.execute(
            """
            SELECT source_row, event_id, rejection_reason
            FROM quarantine_events
            ORDER BY source_row
            """
        ).fetchall()
        write_query_csv(
            connection,
            "SELECT * FROM gold_daily_service ORDER BY event_date, service",
            output_dir / "gold_daily_service.csv",
        )
        summary = {
            **counts,
            "quarantine_rows": [
                {"source_row": row, "event_id": event_id, "reason": reason}
                for row, event_id, reason in quarantine_rows
            ],
        }
        (output_dir / "quality_summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
        return summary
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the local DuckDB event warehouse.")
    parser.add_argument("--input", type=Path, default=EXERCISE_DIR / "events.csv")
    parser.add_argument("--output-dir", type=Path, default=EXERCISE_DIR / "build")
    args = parser.parse_args(argv)
    summary = run_pipeline(args.input, args.output_dir)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
