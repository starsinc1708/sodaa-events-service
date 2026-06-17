CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at DESC);

DROP INDEX IF EXISTS idx_events_created_id;
DROP INDEX IF EXISTS idx_events_type_created_id;
DROP INDEX IF EXISTS idx_events_user_created_id;
