-- ============================================================
-- Migration 003: canonical_key
-- Adds company_normalized and canonical_key columns to jobs.
-- Indexed on company_normalized for O(k) company-scoped pre-filter.
--
-- Backfill note: DuckDB SQL does not support calling Python functions,
-- so the column values for existing rows are left NULL here.
-- Run scripts/backfill_canonical.py after applying this migration to
-- populate both columns for pre-existing rows.
-- Subsequent inserts via upsert_job_with_source() populate both columns.
-- ============================================================

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS company_normalized TEXT;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS canonical_key TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_company_normalized ON jobs(company_normalized);

-- Record this migration
INSERT INTO schema_migrations (version, name, checksum)
VALUES ('003', 'canonical_key', NULL)
ON CONFLICT (version) DO NOTHING;
