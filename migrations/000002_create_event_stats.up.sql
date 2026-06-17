CREATE TABLE IF NOT EXISTS event_stats (
    event_type  TEXT PRIMARY KEY,
    count       BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);