-- Migration 005: linear_poll_state
-- Stores the last-polled watermark per Linear project so the listener
-- only processes state changes / comments since the previous poll.
--
-- Matches the post-004 convention: no FK constraints.

CREATE TABLE IF NOT EXISTS linear_poll_state (
    project_id      TEXT PRIMARY KEY,
    last_polled_at  TIMESTAMP
);

INSERT INTO schema_migrations (version, name, checksum)
VALUES ('005', 'linear_poll_state', NULL)
ON CONFLICT (version) DO NOTHING;
