"""Dedup telemetry: dual-write JSONL + events table for possible_duplicate.

Every upsert_job_with_source call writes one JSONL line.
When outcome == 'review_flagged', also INSERT into events table.
"""

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from opportunities_engine.config import get_default_logs_path

if TYPE_CHECKING:
    from opportunities_engine.dedup.upsert import UpsertResult
    from opportunities_engine.storage.db import JobStore


def emit_dedup_event(
    store: "JobStore",
    result: "UpsertResult",
    canonical_key: str,
    matched_canonical_key: str | None,
) -> None:
    """Write one JSONL telemetry line; also write events row for review_flagged."""
    now_utc = datetime.now(timezone.utc)
    ts = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    date_str = now_utc.strftime("%Y-%m-%d")

    record = {
        "ts": ts,
        "outcome": result.outcome,
        "job_id": result.job_id,
        "matched_job_id": result.matched_job_id,
        "source_name": result.source_name,
        "canonical_key": canonical_key,
        "fuzzy_score": result.fuzzy_score,
        "trust_flipped": result.trust_flipped,
    }

    # Write JSONL line
    logs_dir = get_default_logs_path()
    log_file = logs_dir / f"dedup-{date_str}.jsonl"
    with log_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record) + "\n")

    # For review_flagged: also INSERT into events table
    if result.outcome == "review_flagged":
        assert store.conn is not None
        detail = {
            "new_job_id": result.job_id,
            "matched_job_id": result.matched_job_id,
            "fuzzy_score": result.fuzzy_score,
            "canonical_key_new": canonical_key,
            "canonical_key_matched": matched_canonical_key,
        }
        store.conn.execute(
            """
            INSERT INTO events (job_id, event_type, occurred_at, actor, detail)
            VALUES ($1, 'possible_duplicate', $2, 'system', $3)
            """,
            [result.job_id, now_utc, json.dumps(detail)],
        )
