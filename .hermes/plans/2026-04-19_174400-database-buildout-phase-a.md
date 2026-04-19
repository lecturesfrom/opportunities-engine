# Plan: Database Buildout Phase A — Schema Foundations

**Created:** 2026-04-19
**Status:** Planning — awaiting go-ahead

## Goal

Restructure the DuckDB schema from "four tables bootstrapped inline" to a migration-managed, normalized foundation with proper join tables, audit trails, and archival support. Nothing else (funnel, analytics, routing) works without this.

## Current State

```
jobs:        2,819 rows (working, source of truth for ranked_jobs.json)
companies:   0 rows   (table exists, never populated — all company data in JSON files)
applications: 0 rows  (table exists, never written to)
skill_gaps:   0 rows  (table exists, never written to)
```

**Problems with current schema:**
- No migration tracking — schema is bootstrapped via `_SCHEMA_SQL` in `db.py` on every connection. No way to evolve it safely.
- `companies.attraction_types` is a TEXT field (should be rows in a join table)
- `jobs.source` is a single value — a job found on both Greenhouse and JobSpy has no way to record both sources
- No funnel/event log — the `applications` table is a snapshot, not a timeline
- No scoring audit trail — when a job's rank changes, we lose the previous score
- No archive mechanism — expired jobs just sit in `jobs` forever
- No indexes beyond primary keys

## Proposed Schema Changes

### 1. `schema_migrations` table
```
id         INTEGER PRIMARY KEY
version    TEXT UNIQUE NOT NULL     -- e.g., "001"
name       TEXT NOT NULL            -- e.g., "initial_schema"
applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
checksum   TEXT                     -- SHA256 of the migration SQL
```
Tracks which migrations have been applied. Enables idempotent `migrate()` calls.

### 2. `job_sources` join table (multi-source agreement)
```
id         INTEGER PRIMARY KEY
job_id     INTEGER NOT NULL REFERENCES jobs(id)
source     TEXT NOT NULL            -- greenhouse, lever, ashby, jobspy, hn, yc_seed, manual, linkedin
source_id  TEXT                     -- platform-specific ID (e.g., Greenhouse job ID)
found_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
discovery_path TEXT                 -- e.g., "yc_seed→greenhouse→vercel" or "jobspy→GTM Engineer→indeed"
UNIQUE(job_id, source)
```
A job can appear in multiple sources. This table records *every* source that found it. The `discovery_path` field traces how we got there — critical for "Pipeline as a Product" (Phase 6).

### 3. `events` table (funnel log, append-only)
```
id           INTEGER PRIMARY KEY
job_id       INTEGER NOT NULL REFERENCES jobs(id)
event_type   TEXT NOT NULL           -- discovered, scored, pushed_linear, viewed, applied, phone_screen, interviewed, offered, rejected, ghosted, withdrawn
occurred_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
actor        TEXT DEFAULT 'system'   -- system, keegan, recruiter
detail       JSON                    -- arbitrary event payload (score, interviewer name, rejection reason)
```
This is the backbone of the post-application funnel. Every state transition is an event. No updates, no deletes — append only. The funnel analytics query this table.

### 4. `scores` table (ranker audit trail)
```
id             INTEGER PRIMARY KEY
job_id         INTEGER NOT NULL REFERENCES jobs(id)
score          REAL NOT NULL           -- the similarity score
ranker_version TEXT NOT NULL           -- e.g., "tfidf_v1", "embedding_v2"
scored_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
scoring_detail JSON                   -- breakdown: base_score, bonus, penalties, matched_terms
```
Every time the ranker runs, it writes a new row. We can see how a job's score changed over time, which ranker version produced it, and why (the `scoring_detail`).

### 5. `companies` table (already exists, needs migration)
Current table is fine structurally but `attraction_types TEXT` needs to become a proper join table. Add:
- `canonical_name TEXT` — normalized name for dedup (e.g., "Vercel" and "vercel inc" both map to "vercel")
- `hq_location TEXT`
- `founded_year INTEGER`
- `last_funding_date TIMESTAMP`
- `last_funding_amount REAL`
- `last_funding_stage TEXT`
- `linkedin_url TEXT`
- `twitter_handle TEXT`

### 6. `company_attractions` table (dream taxonomy as rows)
```
id           INTEGER PRIMARY KEY
company_id   INTEGER NOT NULL REFERENCES companies(id)
attraction   TEXT NOT NULL           -- innovation_tech, product_love, design_aesthetic, personal_connection, team_culture, mission_alignment
intensity    REAL DEFAULT 1.0        -- 0.0-1.0 how strong is this pull
note         TEXT                    -- free-text: "the branding is insane"
UNIQUE(company_id, attraction)
```
Replaces `companies.attraction_types TEXT`. Now queryable: "show me all companies where design_aesthetic > 0.8".

### 7. Indexes
```sql
CREATE INDEX idx_job_sources_job_id ON job_sources(job_id);
CREATE INDEX idx_events_job_id_occurred_at ON events(job_id, occurred_at);
CREATE INDEX idx_scores_job_id_scored_at ON scores(job_id, scored_at);
CREATE INDEX idx_companies_canonical_name ON companies(canonical_name);
```

### 8. Archive schema
```sql
CREATE SCHEMA IF NOT EXISTS archive;
CREATE TABLE archive.jobs AS SELECT * FROM jobs WHERE 1=0;    -- same structure, zero rows
CREATE TABLE archive.events AS SELECT * FROM events WHERE 1=0; -- same structure, zero rows
```
Expired or stale jobs get moved to `archive.*` instead of deleted. Keeps the main tables fast while preserving history.

## Migration Strategy

**Single file:** `migrations/001_initial_schema.sql`

This migration:
1. Creates `schema_migrations`
2. Inserts itself as the first row (`version='001', name='initial_schema'`)
3. Creates all new tables (`job_sources`, `events`, `scores`, `company_attractions`)
4. Alters `companies` to add new columns
5. Creates all indexes
6. Creates `archive` schema and mirror tables
7. Does NOT touch `jobs` — 2,819 rows stay intact

**Idempotency:** Every CREATE uses `IF NOT EXISTS`, every ALTER checks for column existence. Running it twice is safe.

**How it applies:** New `migrate()` method in `db.py` reads `migrations/` directory, checks `schema_migrations` for applied versions, runs unapplied ones in order, records them on success.

## Files to Change

| File | Action |
|---|---|
| `migrations/001_initial_schema.sql` | **CREATE** — the migration |
| `src/opportunities_engine/storage/db.py` | **MODIFY** — add `migrate()` method, update `_init_schema()` to call it, deprecate inline `_SCHEMA_SQL` |
| `src/opportunities_engine/storage/migrate.py` | **CREATE** — migration runner (read dir, check versions, execute, record) |
| `tests/test_migrations.py` | **CREATE** — verify idempotency, schema correctness, data preservation |
| `scripts/migrate.py` | **CREATE** — CLI entry point: `python scripts/migrate.py` |

## Data Preservation Plan

- The 2,819 rows in `jobs` are NOT touched by this migration
- The 0-row tables (`companies`, `applications`, `skill_gaps`) get new columns via ALTER
- The new tables (`job_sources`, `events`, `scores`, `company_attractions`) start empty
- After migration, we need a backfill step: populate `companies` from `seed_companies.json` + `dream_companies.json`, populate `company_attractions` from dream taxonomy, populate `job_sources` by parsing existing `jobs.source` + `jobs.metadata`

## Risks

1. **DuckDB ALTER TABLE limitations** — DuckDB supports ADD COLUMN but not all ALTER variants. The new columns on `companies` need to be nullable or have defaults. ✅ All planned columns are nullable.
2. **No rollback** — DuckDB doesn't support transactional DDL rollback the way Postgres does. If migration fails halfway, we need to handle partial state. Mitigation: each migration is designed to be idempotent.
3. **Archive schema parity** — `archive.jobs` and `archive.events` must stay in sync with their main-table counterparts. Any future column addition to `jobs` must also apply to `archive.jobs`. We should add a lint step for this.
4. **Backfill ordering** — `company_attractions` depends on `companies` being populated first. The backfill script must run in dependency order.

## Open Questions

1. **Should we migrate `attraction_types TEXT` out of `companies` in this migration, or leave the old column as a denormalized cache?** My recommendation: leave it for now as a cache, add `company_attractions` as the source of truth, and backfill from JSON. Remove the old column in migration 002.
2. **Should `archive` use the same DuckDB file or a separate one?** Same file, different schema — keeps things simple and queryable (`SELECT * FROM archive.jobs`).
3. **Sequence strategy for new tables** — The current schema uses named sequences (`job_id_seq`, etc.). DuckDB also supports `DEFAULT nextval('seq')`. New tables should use the same pattern for consistency.

## Verification Steps

1. Run migration on a copy of `jobs.duckdb`
2. Verify 2,819 rows still intact
3. Verify all new tables exist with correct columns
4. Verify indexes created
5. Verify `schema_migrations` has one row
6. Run migration again — verify idempotent (no errors, no duplicate rows)
7. Run `pytest` — all 69 tests still pass
8. Run backfill — companies from JSON populate correctly
