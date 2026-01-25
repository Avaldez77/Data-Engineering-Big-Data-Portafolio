-- sql/postgres/00_schema.sql

CREATE SCHEMA IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.silver_ops_event (
  event_id TEXT,
  operation_id INTEGER,
  layer TEXT,
  status TEXT,
  http_code INTEGER,
  error_type TEXT,
  event_ts TIMESTAMPTZ,
  raw_line TEXT
);
