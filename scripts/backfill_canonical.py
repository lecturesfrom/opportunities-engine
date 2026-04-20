"""Backfill canonical_key and company_normalized for pre-existing job rows.

Run once after applying migration 003_canonical_key.sql:

    python scripts/backfill_canonical.py

Iterates all jobs where company_normalized IS NULL or canonical_key IS NULL
and updates them using the dedup functions.
"""

import sys
from pathlib import Path

# Ensure the src layout is importable when run as a script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from opportunities_engine.config import settings
from opportunities_engine.dedup.canonical import canonical_job_key, normalize_company
from opportunities_engine.storage.db import JobStore


def backfill(db_path: Path | str | None = None) -> int:
    """Backfill canonical columns. Returns count of rows updated."""
    path = db_path or settings.database_path

    updated = 0
    with JobStore(path) as store:
        assert store.conn is not None
        rows = store.conn.execute(
            """
            SELECT id, title, company, location
            FROM jobs
            WHERE company_normalized IS NULL OR canonical_key IS NULL
            """
        ).fetchall()

        for (job_id, title, company, location) in rows:
            ckey = canonical_job_key(title or "", company or "", location or "")
            cnorm = normalize_company(company or "")
            store.conn.execute(
                "UPDATE jobs SET company_normalized = $1, canonical_key = $2 WHERE id = $3",
                [cnorm, ckey, job_id],
            )
            updated += 1

    print(f"Backfilled {updated} rows.")
    return updated


if __name__ == "__main__":
    backfill()
