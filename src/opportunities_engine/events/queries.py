"""Event query helpers.

Read-side helpers that inspect the events table. Kept separate from
emitter.py (write-side) so callers can import just what they need.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opportunities_engine.storage.db import JobStore


def get_terminally_closed_job_ids(store: "JobStore") -> set[int]:
    """Return job_ids whose MOST RECENT event is a terminal (OFFER/REJECTED/WITHDREW).

    Uses a window function to pick the latest event per job, then filters by type.
    Caller: Phase F ranker exclusion; future Phase H temperature-band logic.
    """
    assert store.conn is not None
    rows = store.conn.execute(
        """
        WITH latest_events AS (
            SELECT job_id, event_type,
                   ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY occurred_at DESC) AS rn
            FROM events
        )
        SELECT job_id FROM latest_events
        WHERE rn = 1 AND event_type IN ('offer', 'rejected', 'withdrew')
        """
    ).fetchall()
    return {int(r[0]) for r in rows}
