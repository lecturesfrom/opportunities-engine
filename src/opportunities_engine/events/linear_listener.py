"""Linear listener: poll Linear for state changes and emit events.

Polls the configured Linear project for issue updates since the last watermark
and emits corresponding events into the events table.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from opportunities_engine.events.emitter import emit_event
from opportunities_engine.events.linear_comments import parse_comment
from opportunities_engine.events.vocab import (
    APPLIED,
    OFFER,
    PHONE_SCREEN,
    REJECTED,
    WITHDREW,
)
from opportunities_engine.integrations.linear import get_project_issues

if TYPE_CHECKING:
    from opportunities_engine.storage.db import JobStore


LINEAR_STATE_TO_EVENT: dict[str, str] = {
    "In Progress": APPLIED,
    "In Review": PHONE_SCREEN,   # default; comment parser may refine
    "Done": OFFER,               # default; comment parser may refine
    "Canceled": REJECTED,
    "Duplicate": REJECTED,
}

# Event types that can be used as terminal refinements from comments
_TERMINAL_REFINEMENTS: frozenset[str] = frozenset({OFFER, REJECTED, WITHDREW})


def get_job_id_for_linear_issue(store: "JobStore", linear_issue_id: str) -> int | None:
    """Lookup job_id from a PUSHED_TO_LINEAR event's detail.linear_issue_id.

    Args:
        store: Open JobStore connection.
        linear_issue_id: The Linear issue UUID to look up.

    Returns:
        The job_id if found, or None if no matching PUSHED_TO_LINEAR event exists.
    """
    assert store.conn is not None
    row = store.conn.execute(
        """
        SELECT job_id
        FROM events
        WHERE event_type = 'pushed_to_linear'
          AND json_extract_string(detail, '$.linear_issue_id') = $1
        LIMIT 1
        """,
        [linear_issue_id],
    ).fetchone()
    return int(row[0]) if row is not None else None


def _has_event_of_type(store: "JobStore", job_id: int, event_type: str) -> bool:
    """Return True if the job already has at least one event of this type."""
    assert store.conn is not None
    row = store.conn.execute(
        """
        SELECT COUNT(*) FROM events
        WHERE job_id = $1 AND event_type = $2
        """,
        [job_id, event_type],
    ).fetchone()
    return bool(row and row[0] > 0)


def poll_linear(
    store: "JobStore",
    project_id: str,
    *,
    now: datetime | None = None,
    dry_run: bool = False,
) -> dict:
    """Poll Linear for changes since the last watermark and emit events.

    Args:
        store: Open JobStore connection.
        project_id: Linear project UUID to poll.
        now: Override for the current time (used in tests). Defaults to UTC now.
        dry_run: If True, compute what would be emitted without writing any rows
                 or advancing the watermark.

    Returns:
        Summary dict with keys:
            issues_seen: int — total issues returned by Linear API.
            state_events_emitted: int — state-mapped events emitted.
            comment_events_emitted: int — comment-parsed events emitted.
            watermark_advanced_to: str — ISO timestamp the watermark was set to.
    """
    assert store.conn is not None

    if now is None:
        now = datetime.now(timezone.utc)

    # 1. Read watermark
    row = store.conn.execute(
        "SELECT last_polled_at FROM linear_poll_state WHERE project_id = $1",
        [project_id],
    ).fetchone()
    watermark: datetime | None = row[0] if row is not None else None

    # 2. Fetch issues since watermark
    issues = get_project_issues(project_id, since=watermark)

    state_events_emitted = 0
    comment_events_emitted = 0

    # 3. Process each issue
    for issue in issues:
        issue_id: str = issue["id"]
        state_name: str = (issue.get("state") or {}).get("name", "")
        comments: list[dict] = (issue.get("comments") or {}).get("nodes", [])

        # Resolve job_id
        job_id = get_job_id_for_linear_issue(store, issue_id)
        if job_id is None:
            continue

        # --- State mapping ---
        mapped_event = LINEAR_STATE_TO_EVENT.get(state_name)
        if mapped_event is not None:
            # Check for terminal refinement: use last comment if it matches
            # OFFER or REJECTED (or WITHDREW) override the default state map
            refined_event = mapped_event
            if comments:
                latest_comment = comments[-1]
                parsed = parse_comment(latest_comment.get("body", ""))
                if parsed is not None and parsed in _TERMINAL_REFINEMENTS:
                    refined_event = parsed

            if not _has_event_of_type(store, job_id, refined_event):
                if not dry_run:
                    emit_event(
                        store,
                        job_id,
                        refined_event,
                        actor="linear_listener",
                        detail={
                            "linear_issue_id": issue_id,
                            "linear_state": state_name,
                        },
                    )
                state_events_emitted += 1

        # --- Comment processing ---
        for comment in comments:
            comment_id: str = comment.get("id", "")
            body: str = comment.get("body", "")
            created_at_str: str = comment.get("createdAt", "")
            author_name: str = (comment.get("user") or {}).get("name", "")

            # Only process comments newer than the watermark
            if watermark is not None and created_at_str:
                try:
                    created_at = datetime.fromisoformat(
                        created_at_str.replace("Z", "+00:00")
                    )
                    # DuckDB returns naive datetimes; normalise watermark to UTC-aware
                    # before comparing against ISO-8601 comment timestamp.
                    wm_aware = (
                        watermark.replace(tzinfo=timezone.utc)
                        if watermark.tzinfo is None
                        else watermark
                    )
                    if created_at <= wm_aware:
                        continue
                except ValueError:
                    continue

            parsed = parse_comment(body)
            if parsed is None:
                continue

            # Check idempotency: don't re-emit if we already have this event type
            if not _has_event_of_type(store, job_id, parsed):
                if not dry_run:
                    emit_event(
                        store,
                        job_id,
                        parsed,
                        actor="linear_listener",
                        detail={
                            "linear_comment_id": comment_id,
                            "comment_excerpt": body[:240],
                            "matched_event_type": parsed,
                            "linear_author": author_name,
                        },
                    )
                comment_events_emitted += 1

    # 4. Advance watermark
    watermark_ts = now.isoformat()
    if not dry_run:
        store.conn.execute(
            """
            INSERT OR REPLACE INTO linear_poll_state (project_id, last_polled_at)
            VALUES ($1, $2)
            """,
            [project_id, now],
        )

    return {
        "issues_seen": len(issues),
        "state_events_emitted": state_events_emitted,
        "comment_events_emitted": comment_events_emitted,
        "watermark_advanced_to": watermark_ts,
    }
