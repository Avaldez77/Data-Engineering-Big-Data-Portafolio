-- sql/postgres/30_gold_marts.sql

DROP TABLE IF EXISTS observability.gold_kpi_daily;
CREATE TABLE observability.gold_kpi_daily AS
SELECT
  (event_ts::date) AS day,
  layer,
  COUNT(*) AS total_events,
  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_events,
  (SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0)) AS error_rate,
  1 - (SUM(CASE WHEN status='error' THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0)) AS availability
FROM observability.silver_ops_event
WHERE layer IN ('web','db')
GROUP BY 1,2;

DROP TABLE IF EXISTS observability.gold_top_failing_ops;
CREATE TABLE observability.gold_top_failing_ops AS
SELECT
  layer,
  operation_id,
  COUNT(*) AS total_events,
  SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) AS error_events
FROM observability.silver_ops_event
WHERE operation_id IS NOT NULL AND layer IN ('web','db')
GROUP BY 1,2
ORDER BY error_events DESC, total_events DESC;

DROP TABLE IF EXISTS observability.gold_incidents;
CREATE TABLE observability.gold_incidents AS
WITH per_min AS (
  SELECT
    date_trunc('minute', event_ts) AS t_min,
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
    (errors::numeric / NULLIF(total,0)) AS err_rate,
    AVG(errors::numeric / NULLIF(total,0)) OVER (
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
