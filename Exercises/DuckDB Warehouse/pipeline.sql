CREATE OR REPLACE TABLE classified_events AS
WITH typed AS (
    SELECT
        source_row,
        NULLIF(TRIM(event_id), '') AS event_id,
        TRY_CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
        NULLIF(TRIM(service), '') AS service,
        TRY_CAST(status_code AS INTEGER) AS status_code,
        TRY_CAST(duration_ms AS DOUBLE) AS duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY COALESCE(NULLIF(TRIM(event_id), ''), source_row::VARCHAR)
            ORDER BY source_row DESC
        ) AS duplicate_rank
    FROM bronze_events
)
SELECT
    source_row,
    event_id,
    event_timestamp,
    service,
    status_code,
    duration_ms,
    CASE
        WHEN duplicate_rank > 1 THEN 'superseded_duplicate'
        WHEN event_id IS NULL THEN 'blank_event_id'
        WHEN event_timestamp IS NULL THEN 'invalid_timestamp'
        WHEN service IS NULL THEN 'blank_service'
        WHEN status_code IS NULL THEN 'invalid_status_code'
        WHEN duration_ms IS NULL OR NOT ISFINITE(duration_ms) THEN 'invalid_duration'
        WHEN duration_ms < 0 THEN 'negative_duration'
        ELSE NULL
    END AS rejection_reason
FROM typed;

CREATE OR REPLACE TABLE quarantine_events AS
SELECT source_row, event_id, rejection_reason
FROM classified_events
WHERE rejection_reason IS NOT NULL;

CREATE OR REPLACE TABLE silver_events AS
SELECT event_id, event_timestamp, service, status_code, duration_ms
FROM classified_events
WHERE rejection_reason IS NULL;

CREATE OR REPLACE TABLE gold_daily_service AS
SELECT
    CAST(event_timestamp AS DATE) AS event_date,
    service,
    COUNT(*) AS event_count,
    COUNT(*) FILTER (WHERE status_code >= 500) AS error_count,
    COUNT(*) FILTER (WHERE status_code >= 500)::DOUBLE / COUNT(*) AS error_rate,
    AVG(duration_ms) AS avg_duration_ms
FROM silver_events
GROUP BY CAST(event_timestamp AS DATE), service;
