-- ============================================================
-- Migration 001: initial_schema
-- Creates the foundation tables for the opportunities-engine.
-- All statements are idempotent (IF NOT EXISTS).
-- DuckDB requires sequences BEFORE tables that reference them.
-- ============================================================

-- All sequences first (DuckDB strict ordering requirement)
CREATE SEQUENCE IF NOT EXISTS migration_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS job_source_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS event_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS score_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS company_attraction_id_seq START 1;

-- 1. schema_migrations — track applied migrations
CREATE TABLE IF NOT EXISTS schema_migrations (
    id         INTEGER PRIMARY KEY DEFAULT nextval('migration_id_seq'),
    version    TEXT UNIQUE NOT NULL,
    name       TEXT NOT NULL,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum   TEXT
);

-- 2. job_sources — multi-source agreement signal
CREATE TABLE IF NOT EXISTS job_sources (
    id             INTEGER PRIMARY KEY DEFAULT nextval('job_source_id_seq'),
    job_id         INTEGER NOT NULL REFERENCES jobs(id),
    source         TEXT NOT NULL,
    source_id      TEXT,
    found_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    discovery_path TEXT,
    UNIQUE(job_id, source)
);

-- 3. events — funnel log, append-only
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY DEFAULT nextval('event_id_seq'),
    job_id      INTEGER NOT NULL REFERENCES jobs(id),
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actor       TEXT DEFAULT 'system',
    detail      JSON
);

-- 4. scores — ranker audit trail
CREATE TABLE IF NOT EXISTS scores (
    id               INTEGER PRIMARY KEY DEFAULT nextval('score_id_seq'),
    job_id           INTEGER NOT NULL REFERENCES jobs(id),
    score            REAL NOT NULL,
    ranker_version   TEXT NOT NULL,
    scored_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scoring_detail   JSON,
    decision         TEXT
);

-- 5. company_attractions — dream taxonomy as queryable rows
CREATE TABLE IF NOT EXISTS company_attractions (
    id          INTEGER PRIMARY KEY DEFAULT nextval('company_attraction_id_seq'),
    company_id  INTEGER NOT NULL REFERENCES companies(id),
    attraction  TEXT NOT NULL,
    intensity   REAL DEFAULT 1.0,
    note        TEXT,
    UNIQUE(company_id, attraction)
);

-- 5b. Alter companies — add new columns (all nullable for safe ALTER)
ALTER TABLE companies ADD COLUMN IF NOT EXISTS canonical_name TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS hq_location TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS founded_year INTEGER;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_funding_date TIMESTAMP;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_funding_amount REAL;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_funding_stage TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS linkedin_url TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS twitter_handle TEXT;

-- 7. Indexes
CREATE INDEX IF NOT EXISTS idx_job_sources_job_id ON job_sources(job_id);
CREATE INDEX IF NOT EXISTS idx_events_job_id_occurred_at ON events(job_id, occurred_at);
CREATE INDEX IF NOT EXISTS idx_scores_job_id_scored_at ON scores(job_id, scored_at);
CREATE INDEX IF NOT EXISTS idx_companies_canonical_name ON companies(canonical_name);

-- 8-9. Archive schema and mirror tables
CREATE SCHEMA IF NOT EXISTS archive;

CREATE TABLE IF NOT EXISTS archive.jobs (
    id INTEGER,
    source TEXT,
    source_id TEXT,
    url TEXT,
    url_hash TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    description TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_currency TEXT DEFAULT 'USD',
    date_posted TIMESTAMP,
    date_first_seen TIMESTAMP,
    date_last_seen TIMESTAMP,
    is_remote BOOLEAN,
    job_type TEXT,
    seniority TEXT,
    department TEXT,
    company_industry TEXT,
    company_size TEXT,
    metadata JSON,
    status TEXT,
    linear_issue_id TEXT,
    notes TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS archive.events (
    id INTEGER,
    job_id INTEGER,
    event_type TEXT,
    occurred_at TIMESTAMP,
    actor TEXT,
    detail JSON,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Record this migration
INSERT INTO schema_migrations (version, name, checksum)
VALUES ('001', 'initial_schema', NULL)
ON CONFLICT (version) DO NOTHING;
