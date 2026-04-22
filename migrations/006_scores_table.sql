-- Migration 006: scores table — add rank_position, component_scores columns
-- One row per (job, ranker_version) per rank run. Full audit trail of why
-- each job ranked where it did. Enables Phase I score-calibration analytics.
-- Matches post-004 convention: no FK constraints; app code owns integrity.
--
-- The scores table was first created in migration 001. This migration
-- adds the Phase F.2 columns that were not part of the original schema.
-- We reuse the existing scoring_detail JSON column for job metadata
-- (title/company/url); no schema duplication.

ALTER TABLE scores ADD COLUMN IF NOT EXISTS rank_position   INTEGER;
ALTER TABLE scores ADD COLUMN IF NOT EXISTS component_scores JSON;

INSERT INTO schema_migrations (version, name, checksum)
VALUES ('006', 'scores_table', NULL)
ON CONFLICT (version) DO NOTHING;
