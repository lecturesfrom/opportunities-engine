# Phase D: Dedup-on-Write Pipeline

Implement the `upsert_job_with_source()` method — the single write path for all job ingestion. Strict TDD. Follow RED-GREEN-REFACTOR. Do NOT write implementation code without a failing test first.

## Architecture

### Canonical Key Generation

```
canonical_job_key(title, company, location) → str
  1. lowercase
  2. strip punctuation except hyphens
  3. collapse whitespace
  4. apply TITLE_SYNONYMS (sr→senior, jr→junior, ...)
  5. apply LOCATION_NORMALIZERS (sf bay area→san francisco, ...)
  6. apply COMPANY_ALIASES — strip corporate suffixes only: Inc., LLC, Corp., Co., Ltd., ", The"
  7. join as "{title}|{company}|{location}"
```

### Pre-filter by Company (CRITICAL for performance)

Do NOT fuzzy-match against all 2,819+ existing jobs. Instead:
1. Normalize the company name (strip corporate suffixes)
2. Query: `SELECT id, title, company, location FROM jobs WHERE lower(company) = ?`
3. Fuzzy-match canonical keys ONLY within that company's jobs

This is O(k) where k = jobs at that company (usually <50), not O(n). It also eliminates cross-company false positives ("Senior Engineer|Google|SF" vs "Senior Engineer|GoPro|SF").

### Fuzzy Matching

Use `rapidfuzz.WRatio(canonical_key_a, canonical_key_b)`:
- ≥95 → **duplicate** (outcome: `new_source` — add source to existing job)
- 93–94 → **review_flagged** (outcome: `possible_duplicate` — add source AND emit event)
- <93 → **new_job** (insert new job row)

The DEDUP_THRESHOLD env var defaults to 95 but can be overridden.

### Trust Flip

On `new_source` outcome: if ANY existing `job_sources` row for that `job_id` has `source_trust='untrusted'`, flip it to `'trusted'` (single confirmation from a real ingester is sufficient).

### UpsertResult (Pydantic model)

```python
class UpsertResult(BaseModel):
    outcome: Literal["new_job", "new_source", "possible_duplicate"]
    job_id: int
    matched_job_id: int | None = None
    fuzzy_score: float | None = None
    trust_flipped: bool = False
    source_name: str
```

### Main Function Signature

```python
def upsert_job_with_source(
    store: JobStore,
    job: dict,
    source_name: str,
    *,
    source_trust: str = "trusted",
    now: datetime | None = None,
) -> UpsertResult
```

This function:
1. Generates the canonical key from the job dict
2. Pre-filters existing jobs by normalized company name
3. Fuzzy-matches against candidates' canonical keys
4. If match ≥ threshold: adds a `job_sources` row, optionally flips trust, emits event
5. If no match: inserts new job + `job_sources` row, emits event
6. Writes a JSONL telemetry line

### Telemetry

Append one JSONL line per call to `data/dedup_telemetry.jsonl`:
```json
{"ts": "2026-04-19T...", "outcome": "new_job", "job_id": 42, "matched_job_id": null, "source_name": "greenhouse", "canonical_key": "software engineer|stripe|san francisco", "fuzzy_score": null, "trust_flipped": false}
```

### Events Table

On `possible_duplicate` outcome, INSERT into `events`:
- `event_type`: `'possible_duplicate'`
- `detail`: `{"fuzzy_score": 93.5, "matched_job_id": 42, "canonical_key": "...", "new_canonical_key": "..."}`

On `new_job` outcome, INSERT into `events`:
- `event_type`: `'job_created'`
- `detail`: `{"source_name": "greenhouse", "canonical_key": "..."}`

On `new_source` outcome, INSERT into `events`:
- `event_type`: `'job_source_added'`
- `detail`: `{"source_name": "...", "trust_flipped": true/false}`

## Existing Schema

The `jobs` table (from migration 001):
- `source TEXT NOT NULL` — the *original* source field, kept for backward compat
- `url TEXT NOT NULL UNIQUE` — still used for exact URL dedup
- `company TEXT NOT NULL` — used for pre-filtering
- All other fields as in migration 001

The `job_sources` table:
- `job_id INTEGER NOT NULL REFERENCES jobs(id)`
- `source_name TEXT NOT NULL`
- `source_url TEXT`
- `raw_payload JSON`
- `source_trust TEXT DEFAULT 'trusted'`
- `UNIQUE(job_id, source_name)`

The `events` table:
- `job_id INTEGER NOT NULL REFERENCES jobs(id)`
- `event_type TEXT NOT NULL`
- `detail JSON`

## Existing Code

The existing `upsert_job()` method in `src/opportunities_engine/storage/db.py` handles basic URL-based dedup. The new `upsert_job_with_source()` is a SEPARATE function (not a method on JobStore) that lives in a new module: `src/opportunities_engine/storage/dedup.py`. It should call `store.upsert_job()` internally for the actual insert/update, then handle the dedup + source logic on top.

## Dependencies

Add `rapidfuzz` to project dependencies. It's a C-extension fuzzy matching library, very fast.

## DuckDB Quirks

- `last_insert_rowid()` does NOT exist. Use `RETURNING id` or re-SELECT by the known key.
- All SQL parameters use `$1, $2, ...` positional syntax.
- JSON columns accept Python dicts directly (no need to json.dumps for DuckDB JSON columns).

## TDD Requirements

1. Write tests FIRST in `tests/test_phase_d.py`
2. Watch each test FAIL before writing implementation
3. Write minimal code to pass
4. Run full test suite after each cycle: `PYTHONPATH=src python -m pytest -v`
5. All 123+ existing tests must continue to pass

## Test Cases to Cover

1. **New job** — no existing match → inserts job + job_source, returns `new_job`
2. **Exact URL match** — same URL already exists → adds new source, returns `new_source`
3. **Fuzzy match ≥95** — different URL but canonical key matches → adds new source, returns `new_source`
4. **Fuzzy match 93-94** — near match → adds new source + emits `possible_duplicate` event, returns `possible_duplicate`
5. **Fuzzy match <93** — no match → inserts new job, returns `new_job`
6. **Trust flip** — legacy row with `source_trust='untrusted'` gets re-observed → trust flipped to `'trusted'`
7. **No trust flip** — all existing sources are already trusted → `trust_flipped=False`
8. **TITLE_SYNONYMS** — "Sr Engineer" and "Senior Engineer" at same company → match
9. **LOCATION_NORMALIZERS** — "SF" and "San Francisco" → match
10. **COMPANY_ALIASES** — "Stripe, Inc." and "Stripe" → same company bucket
11. **Cross-company safety** — same title at different companies → NOT a match
12. **Canonical key generation** — punctuation stripped, whitespace collapsed
13. **Telemetry** — JSONL line written for each call
14. **DEDUP_THRESHOLD override** — env var changes the threshold

## Files to Create/Modify

- **CREATE** `src/opportunities_engine/storage/dedup.py` — canonical key, fuzzy match, upsert_job_with_source, telemetry
- **CREATE** `tests/test_phase_d.py` — all test cases
- **MODIFY** `pyproject.toml` — add `rapidfuzz` dependency

## After All Tests Pass

1. Run `PYTHONPATH=src python -m pytest -v` — confirm 123+ tests pass
2. Do NOT commit — I will review and commit
