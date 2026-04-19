# Plan: Database Buildout — Full Schema (Phases A–J)

**Created:** 2026-04-19
**Status:** Planning — awaiting go-ahead
**Scope:** 59 items across 10 phases

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Goal

Transform the opportunities-engine from "four inline tables + JSON files + a bag of scripts" into a migration-managed, event-sourced, single-writer pipeline with audit trails, dedup guarantees, and analytics. This is the critical-path work that must land before breadth expansion (LinkedIn, X, YC Work at a Startup, HeyReach).

## Current State

```
jobs:        2,819 rows (working, source of truth)
companies:   0 rows   (table exists, never populated)
applications: 0 rows  (table exists, never written to)
skill_gaps:   0 rows  (table exists, never written to)

Schema: bootstrapped inline in db.py _SCHEMA_SQL
Migrations: none
Events: none
Scores: none (ranked_jobs.json is a flat file, no audit trail)
Dedup: url-hash only, no fuzzy matching, no multi-source tracking
Orchestration: separate scripts, no single writer
DB location: repo-root data/jobs.duckdb (committed to git!)
```

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase A: Schema Foundations (do first, nothing else works without these)

### New tables

| # | Table | Purpose |
|---|---|---|
| 1 | `schema_migrations` | Track applied migrations, enable idempotent `migrate()` |
| 2 | `job_sources` | Multi-source agreement signal (a job can be found by Greenhouse AND JobSpy) |
| 3 | `events` | Funnel log, append-only (every state transition is a row) |
| 4 | `scores` | Ranker audit trail (every score, every version, every breakdown) |
| 5 | `company_attractions` | Dream taxonomy as queryable rows (not a TEXT field) |

### Alter existing

| # | Change | Details |
|---|---|---|
| 5b | `companies` add columns | `canonical_name`, `hq_location`, `founded_year`, `last_funding_date`, `last_funding_amount`, `last_funding_stage`, `linkedin_url`, `twitter_handle` |

### Indexes

| # | Index | On |
|---|---|---|
| 7a | `idx_job_sources_job_id` | `job_sources(job_id)` |
| 7b | `idx_events_job_id_occurred_at` | `events(job_id, occurred_at)` |
| 7c | `idx_scores_job_id_scored_at` | `scores(job_id, scored_at)` |
| 7d | `idx_companies_canonical_name` | `companies(canonical_name)` |

### Archive

| # | Object | Details |
|---|---|---|
| 8 | `CREATE SCHEMA archive` | Separate namespace in same DuckDB file |
| 9a | `archive.jobs` | Mirror of `jobs` structure, zero rows |
| 9b | `archive.events` | Mirror of `events` structure, zero rows |

### Ship as

- `migrations/001_initial_schema.sql` — records itself in `schema_migrations` on apply

### Data preservation

- 2,819 `jobs` rows: **untouched**
- 0-row tables: ALTER to add columns (all nullable)
- New tables: start empty

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase B: Data Migration + Reference Data

| # | Task | Details |
|---|---|---|
| 10 | Write idempotent migration runner | `storage/migrate.py` — reads `migrations/`, checks `schema_migrations`, runs unapplied, records on success |
| 11 | Backfill jobs → job_sources | Parse existing `jobs.source` + `jobs.metadata`, write to `job_sources` with `source_name='legacy'` |
| 12 | seed_companies.json → companies | Upsert with `source='yc_seed'` |
| 13 | dream_companies.json → companies | Upsert with `source='dream_list'`, set `is_dream_company=True` |
| 14 | Attraction taxonomy → company_attractions | One row per attribute (design_aesthetic, product_love, etc.) with intensity |
| 15 | Freeze JSONs | Rename to `_snapshot_2026-04-19.json` — no longer authoritative, DB is source of truth |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase C: Relocate the DB File

| # | Task | Details |
|---|---|---|
| 16 | Move `data/jobs.duckdb` | → `~/Library/Application Support/opportunities-engine/jobs.duckdb` |
| 17 | Dev symlink | Create `data/jobs.duckdb` symlink OR read real path directly |
| 18 | config.py | Read `OPPORTUNITIES_DB_PATH` from env with sensible default |
| 19 | `.gitignore` | Exclude all `*.duckdb` — never commit the DB again |
| 20 | Git history check | If DB was ever committed, plan `git filter-repo` to purge it |

**Why:** The DB file should never have been in git. It's data, not code. macOS Application Support is the right home — survives app updates, doesn't pollute the repo.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase D: Dedup-on-Write Pipeline (PRECONDITION for breadth expansion)

| # | Task | Details |
|---|---|---|
| 21 | `canonical_job_key(company, title, location)` | Normalize everything: lowercase, strip punctuation, collapse whitespace |
| 22 | `upsert_job_with_source(job_data, source_name, url, raw_payload)` | Single chokepoint for ALL ingesters. Handles: new job, new source for existing job, pure dupe |
| 23 | Fuzzy-match fallback | `rapidfuzz` on canonical key, 90% threshold. Catches "GTM Engineer" vs "Growth Engineer" same company |
| 24 | Refactor `ingestion/ats.py` | → use upsert helper |
| 25 | Refactor `ingestion/jobspy_source.py` | → use upsert helper |
| 26 | Refactor `ingestion/hn_hiring.py` | → use upsert helper + wire into `daily_ingest.py` (closes audit gap #1) |
| 27 | Telemetry | Log counts: `new_job`, `new_source_of_existing`, `pure_dupe` per ingest run |

**Why this is the precondition:** Right now, adding LinkedIn or X on top of the existing sources would create floods of duplicates. The dedup gate must be airtight before we add breadth.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase E: Event Emission

| # | Task | Details |
|---|---|---|
| 28 | Emit `'scored'` from `ranker.py` | Every run, for every job evaluated |
| 29 | Emit `'pushed_to_linear'` from `push_top_to_linear.py` | When a card is created in Linear |
| 30 | Linear state listener | Webhook if supported, polling fallback. Detects state changes in Linear |
| 31 | Map Linear states → events | Todo=(none), In Progress=`'applied'`, In Review=`'phone_screen'`|`'interview'`, Done=`'offer'`|`'rejected'` (based on comment/label) |
| 32 | Parse Linear comments | Keywords: applied, screener, ghosted, rejected, offer |
| 33 | Manual CLI | `engine event add --job-id X --type applied --notes "..."` |

**Key design:** Events are append-only. No updates, no deletes. The funnel is reconstructed by querying the latest event per job.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase F: Ranker Integration

| # | Task | Details |
|---|---|---|
| 34 | Exclude terminal-event jobs | Rejected, withdrew, offer — don't rank them |
| 35 | Write to `scores` for EVERY job | Not just top N. Full audit trail. |
| 36 | `RANKER_VERSION` constant | In `config.py`, bump on any algorithm change |
| 37 | `component_scores` breakdown | JSON: `{base_score, bonus, penalties, matched_terms, dream_tier_bonus}` |
| 38 | `decision` column in `scores` | `promoted`, `shortlisted`, `rejected`, `freelance`, `dream` |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase G: Single-Writer Orchestrator

| # | Task | Details |
|---|---|---|
| 39 | `engine.py` | Single entry point for DB writes. No script writes directly. |
| 40 | Convert to callable modules | `daily_ingest`, `rank`, `push`, `digest` become importable functions |
| 41 | Structured logging | JSON lines → `~/Library/Logs/opportunities-engine/` |
| 42 | `engine run daily` | Orchestrates: ingest → rank → push → digest |
| 43 | Schedule via `launchd` | Once daily, morning (macOS native, no cron dependency) |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase H: Temperature Bands + Maintenance

| # | Task | Details |
|---|---|---|
| 44 | Define temperature bands | hot (≤30d, no terminal), warm (applied, pending), cold (terminal), archive (cold >90d) |
| 45 | hot → warm | On `'applied'` event |
| 46 | warm → cold | On rejected/offer/withdrew OR ghosted >45d |
| 47 | cold → archive | After 90d in cold |
| 48 | Purge | Raw listings unseen >90d AND never applied to |
| 49 | Schedule `maintenance.py` | Nightly |
| 50 | `--dry-run` flag | On every maintenance operation |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase I: Analytics Views (the system finally answers "how's it going")

| # | View | What it shows |
|---|---|---|
| 51 | `v_funnel_by_source` | source → found → applied → response → offer + conversion rates |
| 52 | `v_dream_company_hits` | Dream companies that posted, applied, outcome |
| 53 | `v_score_calibration` | Score bucket → app rate → response rate (is the ranker predictive?) |
| 54 | `v_weekly_activity` | Ingested/scored/pushed/applied by week |
| 55 | `engine report weekly` | Dumps views to markdown digest |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Phase J: Safety + Resilience

| # | Task | Details |
|---|---|---|
| 56 | Nightly DB snapshot | `snapshots/jobs-YYYY-MM-DD.duckdb`, keep 14 |
| 57 | `engine restore` | `--from snapshots/jobs-2026-04-15.duckdb` |
| 58 | `engine health` | Table counts, recent ingestion timestamp, last maintenance run |
| 59 | Schema integrity check | On startup — alert if expected tables missing |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Dependency Graph

```
A (Schema) ──→ B (Migration) ──→ C (Relocate DB)
     │              │
     ↓              ↓
D (Dedup) ────→ E (Events) ──→ F (Ranker Integration)
                                    │
                                    ↓
                         G (Orchestrator) ──→ H (Maintenance)
                                                    │
                                                    ↓
                                          I (Analytics) ──→ J (Safety)
```

**Critical path:** A → B → D → E → F → G
**A–F must land before breadth expansion.** G–J trickle in over the following two weeks.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Files to Create/Modify

### New files

| File | Phase | Purpose |
|---|---|---|
| `migrations/001_initial_schema.sql` | A | The migration |
| `src/opportunities_engine/storage/migrate.py` | A+B | Migration runner |
| `src/opportunities_engine/storage/engine.py` | G | Single-writer orchestrator |
| `src/opportunities_engine/storage/maintenance.py` | H | Temperature + purge |
| `src/opportunities_engine/storage/analytics.py` | I | View creation + reporting |
| `src/opportunities_engine/events/emitter.py` | E | Event emission helpers |
| `src/opportunities_engine/events/linear_listener.py` | E | Linear state → events |
| `scripts/engine_cli.py` | E+G+H+J | CLI: `engine run daily`, `engine event add`, `engine health`, etc. |
| `tests/test_migrations.py` | A | Idempotency, schema correctness |
| `tests/test_dedup.py` | D | Canonical key, fuzzy match, telemetry |
| `tests/test_events.py` | E | Event emission, append-only, queries |
| `tests/test_ranker_integration.py` | F | Score writing, exclusion, versioning |
| `tests/test_maintenance.py` | H | Temperature transitions, purge safety |
| `tests/test_analytics.py` | I | View correctness |

### Modified files

| File | Phase | Change |
|---|---|---|
| `src/opportunities_engine/storage/db.py` | A | Add `migrate()`, deprecate inline `_SCHEMA_SQL` |
| `src/opportunities_engine/storage/config.py` | C+F | `OPPORTUNITIES_DB_PATH` env, `RANKER_VERSION` |
| `src/opportunities_engine/ingestion/ats.py` | D | Refactor to use `upsert_job_with_source()` |
| `src/opportunities_engine/ingestion/jobspy_source.py` | D | Refactor to use `upsert_job_with_source()` |
| `src/opportunities_engine/ingestion/hn_hiring.py` | D | Refactor + wire into daily_ingest |
| `src/opportunities_engine/ranking/ranker.py` | F | Write to `scores`, exclude terminal, add version |
| `scripts/push_top_to_linear.py` | E | Emit `'pushed_to_linear'` event |
| `scripts/daily_ingest.py` | D+G | Use upsert, become callable module |
| `.gitignore` | C | Exclude `*.duckdb` |
| `data/seed_companies.json` | B | → `_snapshot_2026-04-19.json` |
| `data/dream_companies.json` | B | → `_snapshot_2026-04-19.json` |

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Risks

1. **DuckDB ALTER limitations** — Not all ALTER variants supported. Mitigation: all new columns nullable.
2. **No DDL rollback** — DuckDB doesn't transactional DDL rollback like Postgres. Mitigation: every migration idempotent.
3. **Backfill ordering** — `company_attractions` depends on `companies`. Script must respect dependency order.
4. **Archive drift** — `archive.jobs` must stay in sync with `jobs` schema. Need a lint step.
5. **Fuzzy dedup false positives** — 90% threshold on canonical key may merge distinct roles at same company ("SWE" vs "Senior SWE"). May need title-seniority awareness.
6. **launchd on macOS** — Requires a `.plist` file and `launchctl load`. More macOS-native than cron but less portable.
7. **DB in git history** — If `jobs.duckdb` was ever committed, even after `.gitignore`, the data persists in history. Need `git filter-repo` to purge.

## Open Questions

1. ~~Leave `attraction_types TEXT` as denormalized cache?~~ **Resolved: leave for now, remove in migration 002.**
2. **Archive: same DuckDB file or separate?** Recommendation: same file, `archive` schema — keeps things queryable.
3. **Linear webhook vs polling?** Need to check if Linear's API supports webhooks for state changes. If not, polling every 15 min.
4. **Fuzzy dedup threshold** — 90% is aggressive. May need to be configurable or per-field.
5. **Score storage volume** — Writing a row for every job every ranker run could be 2,800+ rows/day. At that rate, ~1M rows/year. DuckDB handles this fine, but we should verify.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

## Execution Strategy

**I'll use subagents (3 parallel) for independent phases, and sequence the dependent ones:**

```
Sprint 1 (sequential, must be exact):
  A → B → C  (schema, migration, relocate)

Sprint 2 (sequential, builds on Sprint 1):
  D → E  (dedup pipeline, event emission)

Sprint 3 (sequential, builds on Sprint 2):
  F → G  (ranker integration, orchestrator)

Sprint 4 (can parallelize):
  H (maintenance)  |  I (analytics)  |  J (safety)
  — these are independent of each other

Each sprint: implement → test → verify 69+ existing tests still pass → commit → push
```

**Estimated timeline:** 3–5 focused days for A–F, then 2 weeks trickle for G–J.
