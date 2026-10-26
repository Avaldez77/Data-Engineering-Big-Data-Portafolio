DROP TABLE IF EXISTS silver_events;

CREATE TABLE silver_events AS
WITH ranked_events AS (
    SELECT
        event_id,
        TRIM(service) AS service,
        LOWER(TRIM(event_type)) AS event_type,
        event_timestamp,
        duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingested_at DESC, source_row DESC
        ) AS version_rank
    FROM bronze_events
    WHERE event_id IS NOT NULL
      AND TRIM(event_id) <> ''
),
valid_events AS (
    SELECT
        event_id,
        service,
        event_type,
        DATE(event_timestamp) AS event_date,
        duration_ms
    FROM ranked_events
    WHERE version_rank = 1
      AND service <> ''
      AND DATE(event_timestamp) IS NOT NULL
      AND duration_ms >= 0
)
SELECT event_id, service, event_type, event_date, duration_ms
FROM valid_events;

DROP TABLE IF EXISTS gold_daily_service;

CREATE TABLE gold_daily_service AS
SELECT
    event_date,
    service,
    COUNT(*) AS event_count,
    SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS error_count,
    ROUND(
        1.0 * SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) / COUNT(*),
        4
    ) AS error_rate,
    ROUND(AVG(duration_ms), 2) AS avg_duration_ms
FROM silver_events
GROUP BY event_date, service;
