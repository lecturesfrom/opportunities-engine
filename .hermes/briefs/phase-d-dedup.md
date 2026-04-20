# Phase D — Dedup-on-Write Pipeline (Brief)

Source of truth for every sub-agent working on Phase D. Do NOT improvise. If ambiguity arises, STOP and flag for the main thread.

## Why this exists

`upsert_job_with_source()` is the single chokepoint every current and every future ingester routes through. Today: Greenhouse, Lever, Ashby, JobSpy, HN Hiring. Tomorrow: X/Twitter, Substack, a16z Talent, Lenny's Jobs, 10+ more.

If the foundation is wrong, every source inherits the flaw. If right, adding a new source is one function call.

2,819 legacy rows in `job_sources` are `source_trust='untrusted'` because provenance is unknown. When a real ingester re-observes them, they flip to `'trusted'`. That flip is the success signal.

**Off-limits in Phase D:** `src/opportunities_engine/semantic/ranker.py`, `scripts/push_top_to_linear.py`. Those are Phase E/F.

**Only event emitted:** `possible_duplicate`. No others.

## Canonical key

```
canonical_job_key(title, company, location) -> str
  1. lowercase, strip accents
  2. strip punctuation except hyphens
  3. collapse internal whitespace
  4. apply TITLE_SYNONYMS word-by-word
     (sr→senior, jr→junior, swe→software engineer,
      pm→product manager, eng→engineer, mgr→manager, …)
  5. apply LOCATION_NORMALIZERS
     (sf / sf bay area / san francisco ca → san francisco;
      nyc / new york ny / new york city → new york;
      remote / wfh / anywhere → remote; …)
  6. company via small COMPANY_ALIASES + suffix stripper
     (Inc, LLC, Corp, Co, Ltd, GmbH)
  7. return "{title_normalized}|{company_normalized}|{location_normalized}"
```

Expose `normalize_company(name) -> str` and `normalize_location(loc) -> str` as public helpers from `dedup/canonical.py` — they're used for the SQL pre-filter.

## Match pipeline (company-scoped fuzzy)

```
1. Compute canonical key for incoming job.
2. SQL pre-filter: rows where company_normalized = incoming.company_normalized.
   (O(k) where k = jobs at that company. Prevents cross-company collisions.)
3. Identical canonical_key among those rows → outcome = duplicate (exact).
4. Otherwise rapidfuzz.WRatio(new_key, candidate_key) across that company's rows:
     ≥ DEDUP_THRESHOLD (95)         → outcome = duplicate
     ≥ DEDUP_REVIEW_FLOOR (93) < 95 → outcome = review_flagged
                                       + emit 'possible_duplicate' event
     < 93                           → outcome = new_job
```

Thresholds come from `config.dedup_threshold` / `config.dedup_review_floor`.

## `upsert_job_with_source` signature

```python
def upsert_job_with_source(
    store: JobStore,
    job: dict,
    source_name: str,
    *,
    now: datetime | None = None,
) -> UpsertResult
```

Per outcome:

- **new_job**: INSERT `jobs`, INSERT `job_sources` (source_trust='trusted'), telemetry.
- **new_source**: matched existing job_id via canonical/fuzzy; INSERT `job_sources` for this source (UNIQUE(job_id, source_name) idempotent); run trust-flip check.
- **duplicate**: `(job_id, source_name)` already present; UPDATE `job_sources.last_seen`; no trust change.
- **review_flagged**: DO NOT merge. INSERT as new_job AND emit `possible_duplicate` event referencing the near-match candidate job_id.

URL short-circuit: if `job['url']` exactly matches an existing `jobs.url`, skip canonical/fuzzy and treat as the existing `job_id` directly (reuse `_normalize_url`, `_url_hash` from `storage/db.py`).

## Trust auto-flip (single confirmation)

On `new_source`: if any `job_sources` row for this `job_id` has `source_trust='untrusted'`, UPDATE **all** rows for that `job_id` to `'trusted'`. One real ingester confirmation = job trusted. No partial states.

`trust_flipped` in `UpsertResult` is True only when this flip actually occurred.

## `UpsertResult` dataclass

```python
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class UpsertResult:
    outcome: Literal["new_job", "new_source", "duplicate", "review_flagged"]
    job_id: int
    matched_job_id: int | None
    fuzzy_score: float | None
    trust_flipped: bool
    source_name: str
```

## Telemetry (dual-write)

Every call writes a JSONL line to `get_default_logs_path() / f"dedup-{YYYY-MM-DD}.jsonl"`:

```json
{"ts": "2026-04-20T12:34:56Z",
 "outcome": "new_source",
 "job_id": 1234,
 "matched_job_id": 1234,
 "source_name": "greenhouse",
 "canonical_key": "senior software engineer|vercel|remote",
 "fuzzy_score": 96.4,
 "trust_flipped": true}
```

When (and ONLY when) outcome == `review_flagged`, also INSERT into `events`:

```
event_type = 'possible_duplicate'
detail     = {"new_job_id": N, "matched_job_id": M,
              "fuzzy_score": 94.1,
              "canonical_key_new": "...",
              "canonical_key_matched": "..."}
```

JSONL = grep-friendly debugging. `events` = queryable analytics. Both, always, no exceptions.

## Schema change

Add `jobs.company_normalized TEXT` and `jobs.canonical_key TEXT` columns via `migrations/003_canonical_key.sql`, indexed on `company_normalized`. Backfill existing rows by computing canonical on each row. Subsequent inserts populate both.

## Config additions

In `src/opportunities_engine/config.py`:
- `dedup_threshold: int = 95`
- `dedup_review_floor: int = 93`
- `get_default_logs_path()` mirror of existing `get_default_db_path()` — `~/Library/Logs/opportunities-engine` on macOS, `~/.opportunities-engine/logs` fallback.

## Event vocabulary

New module `src/opportunities_engine/events.py`:
```python
POSSIBLE_DUPLICATE = "possible_duplicate"
```
That is the only event Phase D emits.

## CLI

`src/opportunities_engine/cli.py` does not exist yet; `pyproject.toml:35-36` declares `oe = "opportunities_engine.cli:main"`. Create the module with a Click group. Add `engine` as a second console script alias resolving to the same `main`.

Subcommand: `engine dedup stats --last Nd`:
- Reads all JSONL lines from `~/Library/Logs/opportunities-engine/dedup-*.jsonl` within the window.
- Reports: outcome distribution (new_job / new_source / duplicate / review_flagged), total `trust_flipped` count, current review queue depth (unresolved `possible_duplicate` events in DB).

## Global rules

- `rapidfuzz` is the ONLY new dependency.
- All new code has type hints.
- Only `possible_duplicate` events emitted.
- Sub-agents report; main thread commits.
- If ambiguity arises: STOP, flag. Do not improvise.
