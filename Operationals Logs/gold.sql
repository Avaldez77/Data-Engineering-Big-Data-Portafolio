-- sql/bigquery/30_gold_marts.sql

CREATE OR REPLACE TABLE observability.gold_kpi_daily AS
SELECT
  DATE(event_ts) AS day,
  layer,
  COUNT(*) AS total_events,
  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_events,
  SAFE_DIVIDE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END), COUNT(*)) AS error_rate,
  1 - SAFE_DIVIDE(SUM(CASE WHEN status='error' THEN 1 ELSE 0 END), COUNT(*)) AS availability
FROM observability.silver_ops_event
WHERE layer IN ('web','db')
GROUP BY 1,2;

CREATE OR REPLACE TABLE observability.gold_top_failing_ops AS
SELECT
  layer,
  operation_id,
  COUNT(*) AS total_events,
  SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_events
FROM observability.silver_ops_event
WHERE operation_id IS NOT NULL AND layer IN ('web','db')
GROUP BY 1,2
ORDER BY error_events DESC, total_events DESC;

CREATE OR REPLACE TABLE observability.gold_incidents AS
WITH per_min AS (
  SELECT
    TIMESTAMP_TRUNC(event_ts, MINUTE) AS t_min,
    layer,
    COUNT(*) AS total,
    SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS errors
  FROM observability.silver_ops_event
  WHERE layer IN ('web','db')
  GROUP BY 1,2
),
scored AS (
  SELECT
    *,
    SAFE_DIVIDE(errors, total) AS err_rate,
    AVG(SAFE_DIVIDE(errors, total)) OVER (
      PARTITION BY layer ORDER BY t_min
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS baseline_30m
  FROM per_min
)
SELECT
  t_min,
  layer,
  total,
  errors,
  err_rate,
  baseline_30m,
  CASE
    WHEN baseline_30m IS NOT NULL AND err_rate >= baseline_30m * 2 AND errors >= 5 THEN 1
    ELSE 0
  END AS is_incident
FROM scored;
