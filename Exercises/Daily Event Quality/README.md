# Daily operational event quality

This self-contained exercise models a small Bronze/Silver/Gold pipeline using
fictional service events. It runs with Python's standard library and SQLite;
no cloud account or production data is required.

## Run

From this directory:

```bash
python run_demo.py
python -m unittest discover -s tests -v
```

`schema.sql` creates the immutable Bronze sample. `pipeline.sql` deduplicates
events by their latest ingestion, rejects invalid records from Silver, and
builds daily service-level metrics in Gold.

The expected output has seven Bronze rows, three accepted Silver rows, and two
daily service aggregates. A duplicate event, blank service, malformed
timestamp, and negative duration are intentionally included to exercise
quality controls.

## Extend the exercise

1. Add a quarantine table that records each rejected row and its rejection
   reason instead of dropping it from Silver.
2. Add a configurable error-rate alert threshold and test the exact boundary.
3. Make the deduplication tie-breaker explicit for records with equal
   `ingested_at` timestamps.
4. Add an incremental-run strategy and explain how retries avoid duplicate
   Gold records.

**Acceptance criteria:** Bronze remains unchanged, invalid input is auditable,
rerunning the pipeline produces the same result, and tests cover duplicate,
invalid, and boundary cases.
