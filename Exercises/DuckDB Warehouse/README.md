# DuckDB analytical warehouse

This local exercise demonstrates a compact analytical workflow using DuckDB:
CSV ingestion, typed staging, duplicate and data-quality classification,
quarantine, daily aggregation, and a persisted DuckDB database. Its bundled
event extract is illustrative and contains deliberately malformed and
superseded rows to exercise the quality rules.

From this directory, install the pinned dependency and run the pipeline:

```bash
python -m pip install -r ../../requirements.txt
python run_demo.py --output-dir ../../outputs/duckdb-demo
python -m unittest discover -s tests -v
```

The run creates `warehouse.duckdb`, `gold_daily_service.csv`, and
`quality_summary.json`. Re-running with the same input recreates the same
logical tables and outputs. DuckDB can query local files and Parquet directly;
this public exercise intentionally avoids cloud credentials and production
connectors.
