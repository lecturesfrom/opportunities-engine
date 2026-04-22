"""Tests for events/queries.py — get_terminally_closed_job_ids."""
from __future__ import annotations

from pathlib import Path

import pytest

from opportunities_engine.events import get_terminally_closed_job_ids
from opportunities_engine.events.emitter import emit_event
from opportunities_engine.events.vocab import (
    APPLIED,
    INTERVIEW,
    OFFER,
    PHONE_SCREEN,
    POSSIBLE_DUPLICATE,
    PUSHED_TO_LINEAR,
    REJECTED,
    SCORED,
    WITHDREW,
)
from opportunities_engine.storage.db import JobStore


def _insert_job(store: JobStore, url: str, title: str = "Test Job") -> int:
    """Insert a minimal job and return its id."""
    assert store.conn is not None
    store.conn.execute(
        """
        INSERT INTO jobs (source, url, url_hash, title, company, location, created_at, updated_at)
        VALUES ('test', $1, md5($1), $2, 'Test Co', 'Remote',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [url, title],
    )
    row = store.conn.execute("SELECT id FROM jobs WHERE url = $1", [url]).fetchone()
    assert row is not None
    return int(row[0])


def _emit(store: JobStore, job_id: int, event_type: str, offset_seconds: int = 0) -> None:
    """Emit an event with an explicit occurred_at offset for ordering control."""
    from datetime import datetime, timedelta, timezone

    at = datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)
    emit_event(store, job_id, event_type, occurred_at=at)


class TestGetTerminallyClosedJobIds:
    """Unit tests for get_terminally_closed_job_ids."""

    def test_empty_events_table_returns_empty_set(self, tmp_path: Path) -> None:
        """No events → no terminal job_ids."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            _insert_job(store, "https://example.com/job/1")
            result = get_terminally_closed_job_ids(store)
        assert result == set()

    def test_job_with_no_events_not_included(self, tmp_path: Path) -> None:
        """A job with zero events is not returned."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            _insert_job(store, "https://example.com/job/1")
            result = get_terminally_closed_job_ids(store)
        assert result == set()

    def test_only_non_terminal_events_returns_empty_set(self, tmp_path: Path) -> None:
        """Jobs whose only events are scored/applied/interview are NOT terminal."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/1")
            _emit(store, job_id, SCORED, offset_seconds=0)
            _emit(store, job_id, APPLIED, offset_seconds=1)
            _emit(store, job_id, INTERVIEW, offset_seconds=2)
            result = get_terminally_closed_job_ids(store)
        assert result == set()

    def test_rejected_latest_event_is_terminal(self, tmp_path: Path) -> None:
        """Job whose latest event is REJECTED is returned."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/1")
            _emit(store, job_id, SCORED, offset_seconds=0)
            _emit(store, job_id, APPLIED, offset_seconds=1)
            _emit(store, job_id, REJECTED, offset_seconds=2)
            result = get_terminally_closed_job_ids(store)
        assert result == {job_id}

    def test_offer_latest_event_is_terminal(self, tmp_path: Path) -> None:
        """Job whose latest event is OFFER is returned."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/2")
            _emit(store, job_id, APPLIED, offset_seconds=0)
            _emit(store, job_id, OFFER, offset_seconds=1)
            result = get_terminally_closed_job_ids(store)
        assert result == {job_id}

    def test_withdrew_latest_event_is_terminal(self, tmp_path: Path) -> None:
        """Job whose latest event is WITHDREW is returned."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/3")
            _emit(store, job_id, APPLIED, offset_seconds=0)
            _emit(store, job_id, WITHDREW, offset_seconds=1)
            result = get_terminally_closed_job_ids(store)
        assert result == {job_id}

    def test_non_terminal_latest_even_if_earlier_was_terminal(
        self, tmp_path: Path
    ) -> None:
        """The most-recent rule: REJECTED then APPLIED → NOT terminal (re-opened).

        Even though REJECTED appeared earlier, the latest event is APPLIED
        which is non-terminal, so the job should NOT be excluded.
        """
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/4")
            _emit(store, job_id, REJECTED, offset_seconds=0)
            # Later event is non-terminal — job re-opened / updated
            _emit(store, job_id, APPLIED, offset_seconds=5)
            result = get_terminally_closed_job_ids(store)
        assert result == set()

    def test_mixed_jobs_returns_only_terminal_ones(self, tmp_path: Path) -> None:
        """Multiple jobs: only those whose last event is terminal are returned."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_active = _insert_job(store, "https://example.com/job/active")
            job_rejected = _insert_job(store, "https://example.com/job/rejected")
            job_offered = _insert_job(store, "https://example.com/job/offered")
            job_withdrew = _insert_job(store, "https://example.com/job/withdrew")
            job_no_events = _insert_job(store, "https://example.com/job/no-events")

            _emit(store, job_active, SCORED, offset_seconds=0)
            _emit(store, job_active, PHONE_SCREEN, offset_seconds=1)

            _emit(store, job_rejected, SCORED, offset_seconds=0)
            _emit(store, job_rejected, REJECTED, offset_seconds=1)

            _emit(store, job_offered, APPLIED, offset_seconds=0)
            _emit(store, job_offered, OFFER, offset_seconds=1)

            _emit(store, job_withdrew, APPLIED, offset_seconds=0)
            _emit(store, job_withdrew, WITHDREW, offset_seconds=1)

            # job_no_events: no events at all

            result = get_terminally_closed_job_ids(store)

        assert result == {job_rejected, job_offered, job_withdrew}
        assert job_active not in result
        assert job_no_events not in result

    def test_returns_set_of_ints(self, tmp_path: Path) -> None:
        """Returned values are Python ints, not some other numeric type."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/5")
            _emit(store, job_id, REJECTED, offset_seconds=0)
            result = get_terminally_closed_job_ids(store)
        assert isinstance(next(iter(result)), int)

    def test_possible_duplicate_is_not_terminal(self, tmp_path: Path) -> None:
        """POSSIBLE_DUPLICATE is not a terminal event type."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/6")
            _emit(store, job_id, POSSIBLE_DUPLICATE, offset_seconds=0)
            result = get_terminally_closed_job_ids(store)
        assert result == set()

    def test_pushed_to_linear_is_not_terminal(self, tmp_path: Path) -> None:
        """PUSHED_TO_LINEAR is not a terminal event type."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/7")
            _emit(store, job_id, PUSHED_TO_LINEAR, offset_seconds=0)
            result = get_terminally_closed_job_ids(store)
        assert result == set()
