DROP TABLE IF EXISTS bronze_events;

CREATE TABLE bronze_events (
    source_row INTEGER PRIMARY KEY,
    event_id TEXT,
    service TEXT,
    event_type TEXT,
    event_timestamp TEXT,
    duration_ms INTEGER,
    ingested_at TEXT NOT NULL
);

INSERT INTO bronze_events
    (event_id, service, event_type, event_timestamp, duration_ms, ingested_at)
VALUES
    ('evt-001', 'checkout', 'success', '2026-01-01T10:00:00Z', 120, '2026-01-01T10:00:05Z'),
    ('evt-002', 'checkout', 'error', '2026-01-01T10:05:00Z', 480, '2026-01-01T10:05:05Z'),
    ('evt-002', 'checkout', 'success', '2026-01-01T10:06:00Z', 110, '2026-01-01T10:06:05Z'),
    ('evt-003', 'inventory', 'error', '2026-01-02T08:00:00Z', 900, '2026-01-02T08:00:05Z'),
    ('evt-004', '   ', 'error', '2026-01-02T08:05:00Z', 500, '2026-01-02T08:05:05Z'),
    ('evt-005', 'checkout', 'success', 'not-a-timestamp', 80, '2026-01-02T08:10:05Z'),
    ('evt-006', 'checkout', 'success', '2026-01-02T08:15:00Z', -1, '2026-01-02T08:15:05Z');
