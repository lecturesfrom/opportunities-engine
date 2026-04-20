"""Tests for events.linear_listener — poll_linear and helpers.

All tests use in-memory DuckDB and mock out the Linear API.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from opportunities_engine.events.linear_listener import (
    LINEAR_STATE_TO_EVENT,
    get_job_id_for_linear_issue,
    poll_linear,
)
from opportunities_engine.events.vocab import (
    APPLIED,
    OFFER,
    PHONE_SCREEN,
    PUSHED_TO_LINEAR,
    REJECTED,
)
from opportunities_engine.storage.db import JobStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
_PAST = datetime(2026, 4, 10, 0, 0, 0, tzinfo=timezone.utc)
_COMMENT_TS = "2026-04-15T08:00:00Z"  # between PAST and NOW


def _insert_job(store: JobStore, url: str = "https://example.com/job/1") -> int:
    """Insert a minimal job row and return its id."""
    assert store.conn is not None
    store.conn.execute(
        """
        INSERT INTO jobs (source, url, url_hash, title, company, location,
                          created_at, updated_at)
        VALUES ('test', $1, md5($1), 'Test Job', 'ACME Corp', 'Remote',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [url],
    )
    row = store.conn.execute("SELECT id FROM jobs WHERE url = $1", [url]).fetchone()
    assert row is not None
    return int(row[0])


def _emit_pushed_to_linear(store: JobStore, job_id: int, linear_issue_id: str) -> None:
    """Insert a PUSHED_TO_LINEAR event so get_job_id_for_linear_issue resolves."""
    assert store.conn is not None
    detail = json.dumps({"linear_issue_id": linear_issue_id, "linear_issue_url": "https://linear.app/issue/1"})
    store.conn.execute(
        """
        INSERT INTO events (job_id, event_type, actor, detail)
        VALUES ($1, 'pushed_to_linear', 'system', $2)
        """,
        [job_id, detail],
    )


def _make_issue(
    issue_id: str = "issue-001",
    state_name: str = "In Progress",
    comments: list[dict[str, Any]] | None = None,
    updated_at: str = "2026-04-20T10:00:00Z",
) -> dict[str, Any]:
    """Build a fake Linear issue dict."""
    return {
        "id": issue_id,
        "identifier": "APP-1",
        "title": "Test Job @ ACME Corp",
        "url": "https://linear.app/team/APP-1",
        "state": {"name": state_name},
        "comments": {"nodes": comments or []},
        "updatedAt": updated_at,
    }


def _make_comment(
    comment_id: str = "comment-001",
    body: str = "No match",
    created_at: str = _COMMENT_TS,
    author: str = "Alice",
) -> dict[str, Any]:
    """Build a fake Linear comment dict."""
    return {
        "id": comment_id,
        "body": body,
        "createdAt": created_at,
        "user": {"name": author},
    }


def _event_count(store: JobStore) -> int:
    """Count all rows in the events table."""
    assert store.conn is not None
    return store.conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]


def _event_types_for_job(store: JobStore, job_id: int) -> list[str]:
    """Return sorted list of event_type values for a job."""
    assert store.conn is not None
    rows = store.conn.execute(
        "SELECT event_type FROM events WHERE job_id = $1 ORDER BY occurred_at",
        [job_id],
    ).fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------------------
# Tests: get_job_id_for_linear_issue
# ---------------------------------------------------------------------------


class TestGetJobIdForLinearIssue:
    def test_resolves_when_pushed_to_linear_event_exists(self) -> None:
        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-abc")
            result = get_job_id_for_linear_issue(store, "issue-abc")
        assert result == job_id

    def test_returns_none_when_no_matching_event(self) -> None:
        with JobStore(":memory:") as store:
            _insert_job(store)
            result = get_job_id_for_linear_issue(store, "nonexistent-issue")
        assert result is None

    def test_returns_none_when_events_table_empty(self) -> None:
        with JobStore(":memory:") as store:
            result = get_job_id_for_linear_issue(store, "any-issue")
        assert result is None


# ---------------------------------------------------------------------------
# Tests: LINEAR_STATE_TO_EVENT mapping
# ---------------------------------------------------------------------------


class TestLinearStateMapping:
    def test_known_states_are_mapped(self) -> None:
        assert LINEAR_STATE_TO_EVENT["In Progress"] == APPLIED
        assert LINEAR_STATE_TO_EVENT["In Review"] == PHONE_SCREEN
        assert LINEAR_STATE_TO_EVENT["Done"] == OFFER
        assert LINEAR_STATE_TO_EVENT["Canceled"] == REJECTED
        assert LINEAR_STATE_TO_EVENT["Duplicate"] == REJECTED

    def test_unknown_state_not_in_mapping(self) -> None:
        assert "Todo" not in LINEAR_STATE_TO_EVENT
        assert "Backlog" not in LINEAR_STATE_TO_EVENT


# ---------------------------------------------------------------------------
# Tests: poll_linear — initial poll (no watermark)
# ---------------------------------------------------------------------------


class TestPollLinearInitial:
    def test_initial_poll_emits_state_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """First poll with an 'In Progress' issue emits APPLIED."""
        fake_issues = [_make_issue("issue-001", state_name="In Progress")]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")
            summary = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        assert summary["issues_seen"] == 1
        assert summary["state_events_emitted"] == 1
        assert summary["comment_events_emitted"] == 0
        assert APPLIED in event_types

    def test_watermark_row_created_after_initial_poll(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Watermark row is created after the first poll."""
        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: [],
        )

        with JobStore(":memory:") as store:
            poll_linear(store, "proj-123", now=_NOW)
            row = store.conn.execute(
                "SELECT last_polled_at FROM linear_poll_state WHERE project_id = 'proj-123'"
            ).fetchone()
        assert row is not None
        assert row[0] is not None

    def test_initial_poll_emits_comment_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Comment with parseable text emits an event on first poll."""
        comment = _make_comment(
            comment_id="c-001",
            body="Just applied to this position",
            created_at="2026-04-15T08:00:00Z",
        )
        fake_issues = [
            _make_issue("issue-001", state_name="Todo", comments=[comment])
        ]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")
            summary = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        # "Todo" is not in LINEAR_STATE_TO_EVENT, so 0 state events
        assert summary["state_events_emitted"] == 0
        assert summary["comment_events_emitted"] == 1
        assert APPLIED in event_types

    def test_unknown_state_emits_no_state_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An issue with an unmapped state name emits no state event."""
        fake_issues = [_make_issue("issue-001", state_name="Backlog")]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")
            summary = poll_linear(store, "proj-123", now=_NOW)

        assert summary["state_events_emitted"] == 0


# ---------------------------------------------------------------------------
# Tests: poll_linear — idempotency (re-poll with same data)
# ---------------------------------------------------------------------------


class TestPollLinearIdempotency:
    def test_repoll_same_data_emits_nothing_new(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Second poll with identical data emits zero additional events."""
        fake_issues = [_make_issue("issue-001", state_name="In Progress")]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            # First poll
            poll_linear(store, "proj-123", now=_PAST)
            count_after_first = _event_count(store)

            # Second poll
            summary2 = poll_linear(store, "proj-123", now=_NOW)
            count_after_second = _event_count(store)

        assert summary2["state_events_emitted"] == 0
        assert summary2["comment_events_emitted"] == 0
        # Events table unchanged (excluding watermark table)
        assert count_after_second == count_after_first

    def test_watermark_advances_on_repoll(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Watermark is updated to 'now' on each poll."""
        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: [],
        )

        with JobStore(":memory:") as store:
            poll_linear(store, "proj-123", now=_PAST)
            poll_linear(store, "proj-123", now=_NOW)
            row = store.conn.execute(
                "SELECT last_polled_at FROM linear_poll_state WHERE project_id = 'proj-123'"
            ).fetchone()

        assert row is not None
        # Watermark should be _NOW (or close to it)
        stored_ts = str(row[0])
        assert "2026-04-20" in stored_ts


# ---------------------------------------------------------------------------
# Tests: poll_linear — state change between polls
# ---------------------------------------------------------------------------


class TestPollLinearStateChange:
    def test_new_state_on_second_poll_emits_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Issue moves from 'In Progress' to 'Done' between polls → emit OFFER."""
        first_issues = [_make_issue("issue-001", state_name="In Progress")]
        second_issues = [_make_issue("issue-001", state_name="Done")]

        call_count = {"n": 0}

        def fake_get_project_issues(*a: Any, **kw: Any) -> list[dict]:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_issues
            return second_issues

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            fake_get_project_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            poll_linear(store, "proj-123", now=_PAST)
            summary2 = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        # First poll: PUSHED_TO_LINEAR + APPLIED; second: OFFER
        assert summary2["state_events_emitted"] == 1
        assert OFFER in event_types
        assert APPLIED in event_types


# ---------------------------------------------------------------------------
# Tests: poll_linear — new comment between polls
# ---------------------------------------------------------------------------


class TestPollLinearNewComment:
    def test_new_comment_on_second_poll_emits_event(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A new comment added after the first poll is emitted on the second poll."""
        early_comment = _make_comment(
            comment_id="c-old",
            body="applied weeks ago",
            created_at="2026-04-05T10:00:00Z",
        )
        new_comment = _make_comment(
            comment_id="c-new",
            body="phone screen scheduled",
            created_at="2026-04-18T10:00:00Z",  # after _PAST watermark
        )

        first_issues = [_make_issue("issue-001", state_name="In Progress", comments=[early_comment])]
        second_issues = [_make_issue("issue-001", state_name="In Progress", comments=[early_comment, new_comment])]

        call_count = {"n": 0}

        def fake_get_project_issues(*a: Any, **kw: Any) -> list[dict]:
            call_count["n"] += 1
            if call_count["n"] == 1:
                return first_issues
            return second_issues

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            fake_get_project_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            # First poll at _PAST: old comment (2026-04-05) is before watermark,
            # but on first poll watermark=None so all comments are processed.
            poll_linear(store, "proj-123", now=_PAST)

            # Second poll at _NOW: watermark is _PAST (2026-04-10)
            # new_comment (2026-04-18) is AFTER watermark → should be processed
            summary2 = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        assert summary2["comment_events_emitted"] == 1
        assert PHONE_SCREEN in event_types


# ---------------------------------------------------------------------------
# Tests: poll_linear — dry run
# ---------------------------------------------------------------------------


class TestPollLinearDryRun:
    def test_dry_run_reports_counts_but_writes_nothing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """dry_run=True reports emit counts but leaves events table and watermark unchanged."""
        fake_issues = [_make_issue("issue-001", state_name="In Progress")]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            events_before = _event_count(store)
            summary = poll_linear(store, "proj-123", now=_NOW, dry_run=True)
            events_after = _event_count(store)

            # Watermark should NOT be created
            row = store.conn.execute(
                "SELECT last_polled_at FROM linear_poll_state WHERE project_id = 'proj-123'"
            ).fetchone()

        # Summary says what WOULD have been emitted
        assert summary["state_events_emitted"] == 1
        # Events table unchanged
        assert events_after == events_before
        # Watermark NOT advanced
        assert row is None

    def test_dry_run_still_returns_summary_dict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """dry_run still returns the summary dict with all expected keys."""
        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: [],
        )

        with JobStore(":memory:") as store:
            summary = poll_linear(store, "proj-123", now=_NOW, dry_run=True)

        assert "issues_seen" in summary
        assert "state_events_emitted" in summary
        assert "comment_events_emitted" in summary
        assert "watermark_advanced_to" in summary


# ---------------------------------------------------------------------------
# Tests: poll_linear — Linear issue lookup miss
# ---------------------------------------------------------------------------


class TestPollLinearLookupMiss:
    def test_unknown_issue_is_skipped_silently(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Issue not in PUSHED_TO_LINEAR events is skipped; no error raised."""
        # Issue exists in Linear but no corresponding PUSHED_TO_LINEAR event
        fake_issues = [_make_issue("unknown-issue-id", state_name="In Progress")]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            # Deliberately do NOT emit PUSHED_TO_LINEAR for this issue_id

            summary = poll_linear(store, "proj-123", now=_NOW)

        assert summary["issues_seen"] == 1
        assert summary["state_events_emitted"] == 0
        assert summary["comment_events_emitted"] == 0


# ---------------------------------------------------------------------------
# Tests: poll_linear — terminal refinement
# ---------------------------------------------------------------------------


class TestPollLinearTerminalRefinement:
    def test_done_state_with_rejected_comment_emits_rejected(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue state 'Done' (default OFFER) but latest comment says 'rejected' → REJECTED."""
        comment = _make_comment(
            comment_id="c-001",
            body="They rejected my application",
            created_at=_COMMENT_TS,
        )
        fake_issues = [_make_issue("issue-001", state_name="Done", comments=[comment])]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            summary = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        assert summary["state_events_emitted"] == 1
        assert REJECTED in event_types
        assert OFFER not in event_types

    def test_done_state_with_no_terminal_comment_emits_offer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue state 'Done' with no terminal-matching comment → emit OFFER (default)."""
        comment = _make_comment(
            comment_id="c-001",
            body="Great conversation with the team",
            created_at=_COMMENT_TS,
        )
        fake_issues = [_make_issue("issue-001", state_name="Done", comments=[comment])]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            summary = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        assert summary["state_events_emitted"] == 1
        assert OFFER in event_types

    def test_in_review_state_with_offer_comment_emits_offer(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Issue state 'In Review' (default PHONE_SCREEN) but last comment says 'offer' → OFFER."""
        comment = _make_comment(
            comment_id="c-001",
            body="Got an offer from them",
            created_at=_COMMENT_TS,
        )
        fake_issues = [_make_issue("issue-001", state_name="In Review", comments=[comment])]

        monkeypatch.setattr(
            "opportunities_engine.events.linear_listener.get_project_issues",
            lambda *a, **kw: fake_issues,
        )

        with JobStore(":memory:") as store:
            job_id = _insert_job(store)
            _emit_pushed_to_linear(store, job_id, "issue-001")

            summary = poll_linear(store, "proj-123", now=_NOW)
            event_types = _event_types_for_job(store, job_id)

        assert summary["state_events_emitted"] == 1
        assert OFFER in event_types
        assert PHONE_SCREEN not in event_types
