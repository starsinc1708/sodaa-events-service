CREATE INDEX IF NOT EXISTS idx_events_user_created_id
    ON events (user_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_events_type_created_id
    ON events (event_type, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_events_created_id
    ON events (created_at DESC, id DESC);

DROP INDEX IF EXISTS idx_events_created_at;
