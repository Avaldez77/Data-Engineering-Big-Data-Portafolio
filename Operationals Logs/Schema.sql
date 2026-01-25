-- sql/bigquery/00_schema.sql
-- Minimal schema definitions (BigQuery). Adjust dataset names as needed.

CREATE SCHEMA IF NOT EXISTS observability;

-- Silver table (example). In practice this is written by Spark/ETL.
CREATE OR REPLACE TABLE observability.silver_ops_event (
  event_id STRING,
  operation_id INT64,
  layer STRING,
  status STRING,
  http_code INT64,
  error_type STRING,
  event_ts TIMESTAMP,
  raw_line STRING
);
