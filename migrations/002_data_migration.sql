-- Phase B: Data Migration + Reference Data
-- Backfills job_sources, drops dead-weight applications table
-- JSON migration is handled by scripts/migrate_json_to_db.py
-- Idempotent: all statements use IF NOT EXISTS / ON CONFLICT

-- ============================================================
-- 11. Backfill existing jobs → job_sources (source_name='legacy')
-- ============================================================
-- Every pre-existing job row gets tagged 'legacy' because we genuinely
-- don't know where they came from. These are UNTRUSTED for multi-source
-- agreement scoring until re-observed by a real ingester.
INSERT INTO job_sources (job_id, source_name, source_url, raw_payload, first_seen, last_seen, source_trust)
SELECT
    id,
    'legacy',
    NULL,
    NULL,
    COALESCE(created_at, CURRENT_TIMESTAMP),
    COALESCE(created_at, CURRENT_TIMESTAMP),
    'untrusted'
FROM jobs
WHERE NOT EXISTS (
    SELECT 1 FROM job_sources js WHERE js.job_id = jobs.id AND js.source_name = 'legacy'
);

-- ============================================================
-- 14. Drop dead-weight applications table
-- ============================================================
-- Applications was designed but never populated. Events table supersedes it.
-- Kill it now before anyone starts depending on it.
DROP TABLE IF EXISTS applications;
