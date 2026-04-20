-- ============================================================
-- Migration 001: initial_schema
-- Creates ALL foundation tables for the opportunities-engine.
-- All statements are idempotent (IF NOT EXISTS).
-- DuckDB requires sequences BEFORE tables that reference them.
-- ============================================================

-- All sequences first (DuckDB strict ordering requirement)
CREATE SEQUENCE IF NOT EXISTS job_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS migration_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS job_source_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS event_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS score_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS company_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS company_attraction_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS skill_gap_id_seq START 1;

-- Base table: jobs (existed before migrations, now formally declared)
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY DEFAULT nextval('job_id_seq'),
    source           TEXT NOT NULL,
    source_id        TEXT,
    url              TEXT NOT NULL UNIQUE,
    url_hash         TEXT NOT NULL,
    title            TEXT NOT NULL,
    company          TEXT NOT NULL,
    location         TEXT,
    description      TEXT,
    salary_min       REAL,
    salary_max       REAL,
    salary_currency  TEXT DEFAULT 'USD',
    date_posted      TIMESTAMP,
    date_first_seen  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_last_seen   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_remote        BOOLEAN,
    job_type         TEXT,
    seniority        TEXT,
    department       TEXT,
    company_industry TEXT,
    company_size     TEXT,
    metadata         JSON,
    status           TEXT DEFAULT 'new',
    linear_issue_id  TEXT,
    notes            TEXT,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Base table: companies (formal replacement for JSON reference data)
CREATE TABLE IF NOT EXISTS companies (
    id                  INTEGER PRIMARY KEY DEFAULT nextval('company_id_seq'),
    canonical_name      TEXT UNIQUE,
    name                TEXT NOT NULL,
    website             TEXT,
    industry            TEXT,
    hq_location         TEXT,
    company_size        TEXT,
    funding_stage       TEXT,
    ats_platforms_json  JSON,
    is_dream            BOOLEAN DEFAULT FALSE,
    why_i_love          TEXT,
    priority            TEXT,
    status              TEXT,
    discovery_path      TEXT,
    active_role         TEXT,
    active_role_url     TEXT,
    notes               TEXT,
    source              TEXT,
    added_at            TEXT,
    last_funding_date   TIMESTAMP,
    last_funding_amount REAL,
    last_funding_stage  TEXT,
    linkedin_url        TEXT,
    twitter_handle      TEXT,
    founded_year        INTEGER,
    attraction_types    TEXT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Base table: skill_gaps (existed before migrations)
CREATE TABLE IF NOT EXISTS skill_gaps (
    id          INTEGER PRIMARY KEY DEFAULT nextval('skill_gap_id_seq'),
    job_id      INTEGER NOT NULL,
    skill       TEXT NOT NULL,
    priority    TEXT DEFAULT 'medium',
    status      TEXT DEFAULT 'identified',
    notes       TEXT,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

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
    id            INTEGER PRIMARY KEY DEFAULT nextval('job_source_id_seq'),
    job_id        INTEGER NOT NULL,
    source_name   TEXT NOT NULL,
    source_url    TEXT,
    raw_payload   JSON,
    first_seen    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_trust  TEXT DEFAULT 'trusted',
    UNIQUE(job_id, source_name)
);

-- 3. events — funnel log, append-only
CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY DEFAULT nextval('event_id_seq'),
    job_id      INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    actor       TEXT DEFAULT 'system',
    detail      JSON
);

-- 4. scores — ranker audit trail
CREATE TABLE IF NOT EXISTS scores (
    id               INTEGER PRIMARY KEY DEFAULT nextval('score_id_seq'),
    job_id           INTEGER NOT NULL,
    score            REAL NOT NULL,
    ranker_version   TEXT NOT NULL,
    scored_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scoring_detail   JSON,
    decision         TEXT
);

-- 5. company_attractions — dream taxonomy as queryable rows
CREATE TABLE IF NOT EXISTS company_attractions (
    id          INTEGER PRIMARY KEY DEFAULT nextval('company_attraction_id_seq'),
    company_id  INTEGER NOT NULL,
    attribute   TEXT NOT NULL,
    weight      REAL DEFAULT 1.0,
    source      TEXT,
    UNIQUE(company_id, attribute)
);

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
