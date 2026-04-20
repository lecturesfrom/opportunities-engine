"""Event emitter: insert rows into the events table."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from opportunities_engine.events.vocab import ALL_EVENT_TYPES

if TYPE_CHECKING:
    from opportunities_engine.storage.db import JobStore


def emit_event(
    store: "JobStore",
    job_id: int,
    event_type: str,
    *,
    actor: str = "system",
    detail: dict | None = None,
    occurred_at: datetime | None = None,
) -> None:
    """Insert an events row. Raises ValueError if event_type not in ALL_EVENT_TYPES."""
    if event_type not in ALL_EVENT_TYPES:
        allowed = sorted(ALL_EVENT_TYPES)
        raise ValueError(
            f"Unknown event_type {event_type!r}. Allowed types: {allowed}"
        )

    if occurred_at is None:
        occurred_at = datetime.now(timezone.utc)

    detail_json: str | None = json.dumps(detail) if detail is not None else None

    assert store.conn is not None
    store.conn.execute(
        """
        INSERT INTO events (job_id, event_type, occurred_at, actor, detail)
        VALUES ($1, $2, $3, $4, $5)
        """,
        [job_id, event_type, occurred_at, actor, detail_json],
    )
