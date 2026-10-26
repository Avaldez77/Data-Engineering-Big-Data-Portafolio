DROP TABLE IF EXISTS silver_events;

DROP TABLE IF EXISTS quarantine_events;
DROP TABLE IF EXISTS classified_events;

CREATE TABLE classified_events AS
WITH ranked_events AS (
    SELECT
        source_row,
        TRIM(event_id) AS event_id,
        TRIM(service) AS service,
        LOWER(TRIM(event_type)) AS event_type,
        event_timestamp,
        duration_ms,
        ingested_at,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(NULLIF(TRIM(event_id), ''), CAST(source_row AS TEXT))
            ORDER BY ingested_at DESC, source_row DESC
        ) AS version_rank
    FROM bronze_events
),
event_classification AS (
    SELECT
        source_row,
        event_id,
        service,
        event_type,
        event_timestamp,
        duration_ms,
        version_rank,
        CASE
            WHEN event_id IS NULL OR event_id = '' THEN 'missing_event_id'
            WHEN version_rank > 1 THEN 'superseded_duplicate'
            WHEN service IS NULL OR service = '' THEN 'blank_service'
            WHEN event_type IS NULL OR event_type NOT IN ('success', 'error')
                THEN 'invalid_event_type'
            WHEN DATE(event_timestamp) IS NULL THEN 'invalid_timestamp'
            WHEN duration_ms IS NULL OR duration_ms < 0 THEN 'invalid_duration'
            ELSE NULL
        END AS rejection_reason
    FROM ranked_events
)
SELECT * FROM event_classification;

CREATE TABLE silver_events AS
SELECT event_id, service, event_type, DATE(event_timestamp) AS event_date, duration_ms
FROM classified_events
WHERE rejection_reason IS NULL;

CREATE TABLE quarantine_events AS
SELECT source_row, event_id, rejection_reason
FROM classified_events
WHERE rejection_reason IS NOT NULL;

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
