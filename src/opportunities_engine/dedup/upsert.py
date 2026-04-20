"""upsert_job_with_source — single chokepoint for all ingesters.

Implements the four outcome pipeline:
  new_job       — first time this job has been seen
  new_source    — job seen before but from a new source (trust flip possible)
  duplicate     — same source, same job; update last_seen only
  review_flagged — near-match that didn't cross DEDUP_THRESHOLD; inserted as
                   new_job AND emits possible_duplicate event

URL short-circuit: if job['url'] exactly matches an existing jobs.url, skip
canonical/fuzzy and treat as the existing job_id directly.
"""

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal

from opportunities_engine.config import settings
from opportunities_engine.dedup.canonical import (
    canonical_job_key,
    normalize_company,
)
from opportunities_engine.dedup.fuzzy import fuzzy_match
from opportunities_engine.storage.db import JobStore, _normalize_url, _url_hash


@dataclass(frozen=True)
class UpsertResult:
    outcome: Literal["new_job", "new_source", "duplicate", "review_flagged"]
    job_id: int
    matched_job_id: int | None
    fuzzy_score: float | None
    trust_flipped: bool
    source_name: str


def upsert_job_with_source(
    store: JobStore,
    job: dict,
    source_name: str,
    *,
    now: datetime | None = None,
) -> "UpsertResult":
    """Insert or update a job, tracking multi-source provenance.

    Returns a UpsertResult describing the outcome.
    """
    from opportunities_engine.dedup.telemetry import emit_dedup_event

    assert store.conn is not None
    conn = store.conn

    if now is None:
        now = datetime.now(timezone.utc)

    url: str = job.get("url", "")
    title: str = job.get("title", "")
    company: str = job.get("company", "")
    location: str = job.get("location", "") or ""

    # --- 0. Canonical key computation ---
    new_canonical_key = canonical_job_key(title, company, location)
    new_company_norm = normalize_company(company)

    # --- 1. URL short-circuit ---
    existing_by_url = None
    if url:
        norm_url = _normalize_url(url)
        url_h = _url_hash(url)
        row = conn.execute(
            "SELECT id FROM jobs WHERE url_hash = $1 AND url = $2",
            [url_h, norm_url],
        ).fetchone()
        if row is None:
            # Also try exact match on the raw URL
            row = conn.execute(
                "SELECT id FROM jobs WHERE url = $1",
                [url],
            ).fetchone()
        if row:
            existing_by_url = row[0]

    if existing_by_url is not None:
        matched_job_id = existing_by_url
        result = _handle_existing_job(
            conn=conn,
            job_id=matched_job_id,
            source_name=source_name,
            new_canonical_key=new_canonical_key,
            new_company_norm=new_company_norm,
            now=now,
            fuzzy_score=None,
            via_url=True,
        )
        emit_dedup_event(store, result, new_canonical_key, None)
        return result

    # --- 2. Canonical-key + company-scoped fuzzy match ---
    # SQL pre-filter: get all jobs at same company
    company_rows = conn.execute(
        "SELECT id, canonical_key FROM jobs WHERE company_normalized = $1",
        [new_company_norm],
    ).fetchall()

    # Step 3: Check for exact canonical_key match
    for (job_id, ckey) in company_rows:
        if ckey == new_canonical_key:
            result = _handle_existing_job(
                conn=conn,
                job_id=job_id,
                source_name=source_name,
                new_canonical_key=new_canonical_key,
                new_company_norm=new_company_norm,
                now=now,
                fuzzy_score=100.0,
                via_url=False,
            )
            emit_dedup_event(store, result, new_canonical_key, ckey)
            return result

    # Step 4: Fuzzy match across company rows
    candidates: list[tuple[int, str]] = [
        (jid, ckey) for (jid, ckey) in company_rows if ckey is not None
    ]
    fuzzy_result = fuzzy_match(new_canonical_key, candidates)

    threshold = settings.dedup_threshold
    review_floor = settings.dedup_review_floor

    if fuzzy_result is not None:
        best_id, best_score = fuzzy_result

        if best_score >= threshold:
            # Fuzzy duplicate
            matched_ckey = next((ck for jid, ck in candidates if jid == best_id), None)
            result = _handle_existing_job(
                conn=conn,
                job_id=best_id,
                source_name=source_name,
                new_canonical_key=new_canonical_key,
                new_company_norm=new_company_norm,
                now=now,
                fuzzy_score=best_score,
                via_url=False,
            )
            emit_dedup_event(store, result, new_canonical_key, matched_ckey)
            return result

        if best_score >= review_floor:
            # Review flagged: insert as new_job AND emit possible_duplicate event
            matched_ckey = next((ck for jid, ck in candidates if jid == best_id), None)
            new_job_id = _insert_new_job(
                conn=conn,
                job=job,
                source_name=source_name,
                canonical_key=new_canonical_key,
                company_norm=new_company_norm,
                now=now,
            )
            result = UpsertResult(
                outcome="review_flagged",
                job_id=new_job_id,
                matched_job_id=best_id,
                fuzzy_score=best_score,
                trust_flipped=False,
                source_name=source_name,
            )
            emit_dedup_event(store, result, new_canonical_key, matched_ckey)
            return result

    # --- 3. New job ---
    new_job_id = _insert_new_job(
        conn=conn,
        job=job,
        source_name=source_name,
        canonical_key=new_canonical_key,
        company_norm=new_company_norm,
        now=now,
    )
    result = UpsertResult(
        outcome="new_job",
        job_id=new_job_id,
        matched_job_id=None,
        fuzzy_score=None,
        trust_flipped=False,
        source_name=source_name,
    )
    emit_dedup_event(store, result, new_canonical_key, None)
    return result


def _insert_new_job(
    conn: object,
    job: dict,
    source_name: str,
    canonical_key: str,
    company_norm: str,
    now: datetime,
) -> int:
    """Insert a new job row + job_sources row. Returns the new job_id."""
    import json as _json

    url = job.get("url", "")
    url_h = _url_hash(url) if url else ""
    metadata_json = _json.dumps(job.get("metadata", {})) if job.get("metadata") is not None else "{}"

    row = conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO jobs (
            source, source_id, url, url_hash, title, company, location,
            description, salary_min, salary_max, salary_currency,
            date_posted, is_remote, job_type, seniority, department,
            company_industry, company_size, metadata, status, notes,
            date_first_seen, date_last_seen, created_at, updated_at,
            company_normalized, canonical_key
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14, $15, $16,
            $17, $18, $19, $20, $21,
            $22, $23, $24, $25,
            $26, $27
        ) RETURNING id
        """,
        [
            job.get("source", source_name),
            job.get("source_id"),
            url,
            url_h,
            job.get("title", ""),
            job.get("company", ""),
            job.get("location"),
            job.get("description"),
            job.get("salary_min"),
            job.get("salary_max"),
            job.get("salary_currency", "USD"),
            job.get("date_posted"),
            job.get("is_remote"),
            job.get("job_type"),
            job.get("seniority"),
            job.get("department"),
            job.get("company_industry"),
            job.get("company_size"),
            metadata_json,
            job.get("status", "new"),
            job.get("notes"),
            now,
            now,
            now,
            now,
            company_norm,
            canonical_key,
        ],
    ).fetchone()

    new_id: int = row[0]

    # Insert job_sources row
    conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO job_sources (job_id, source_name, source_url, raw_payload, first_seen, last_seen, source_trust)
        VALUES ($1, $2, $3, $4, $5, $6, 'trusted')
        ON CONFLICT (job_id, source_name) DO NOTHING
        """,
        [
            new_id,
            source_name,
            job.get("url"),
            json.dumps(job) if job else None,
            now,
            now,
        ],
    )

    return new_id


def _handle_existing_job(
    conn: object,
    job_id: int,
    source_name: str,
    new_canonical_key: str,
    new_company_norm: str,
    now: datetime,
    fuzzy_score: float | None,
    via_url: bool,
) -> "UpsertResult":
    """Handle an existing job: determine new_source, duplicate, or trust_flip logic."""
    # Check if this (job_id, source_name) pair already exists
    existing_source = conn.execute(  # type: ignore[union-attr]
        "SELECT id FROM job_sources WHERE job_id = $1 AND source_name = $2",
        [job_id, source_name],
    ).fetchone()

    if existing_source is not None:
        # Duplicate: same job_id + same source → update last_seen only
        conn.execute(  # type: ignore[union-attr]
            "UPDATE job_sources SET last_seen = $1 WHERE job_id = $2 AND source_name = $3",
            [now, job_id, source_name],
        )
        return UpsertResult(
            outcome="duplicate",
            job_id=job_id,
            matched_job_id=job_id,
            fuzzy_score=fuzzy_score,
            trust_flipped=False,
            source_name=source_name,
        )

    # New source for an existing job
    # Check if any existing job_sources row is untrusted
    untrusted_count = conn.execute(  # type: ignore[union-attr]
        "SELECT COUNT(*) FROM job_sources WHERE job_id = $1 AND source_trust = 'untrusted'",
        [job_id],
    ).fetchone()
    has_untrusted = untrusted_count is not None and untrusted_count[0] > 0

    # Insert the new job_sources row (trusted — this is a real ingester)
    conn.execute(  # type: ignore[union-attr]
        """
        INSERT INTO job_sources (job_id, source_name, first_seen, last_seen, source_trust)
        VALUES ($1, $2, $3, $4, 'trusted')
        ON CONFLICT (job_id, source_name) DO UPDATE SET last_seen = EXCLUDED.last_seen
        """,
        [job_id, source_name, now, now],
    )

    trust_flipped = False
    if has_untrusted:
        # Trust flip: update ALL rows for this job_id to trusted
        conn.execute(  # type: ignore[union-attr]
            "UPDATE job_sources SET source_trust = 'trusted' WHERE job_id = $1",
            [job_id],
        )
        trust_flipped = True

    return UpsertResult(
        outcome="new_source",
        job_id=job_id,
        matched_job_id=job_id,
        fuzzy_score=fuzzy_score,
        trust_flipped=trust_flipped,
        source_name=source_name,
    )
