"""Tests for events.emitter.emit_event and package layout."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from opportunities_engine.events import emit_event
from opportunities_engine.events.vocab import (
    ALL_EVENT_TYPES,
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


def _insert_job(store: JobStore, url: str) -> int:
    """Insert a minimal job row and return its id."""
    assert store.conn is not None
    store.conn.execute(
        """
        INSERT INTO jobs (source, url, url_hash, title, company, location, created_at, updated_at)
        VALUES ('test', $1, md5($1), 'Test Job', 'Test Co', 'Remote',
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [url],
    )
    row = store.conn.execute(
        "SELECT id FROM jobs WHERE url = $1", [url]
    ).fetchone()
    assert row is not None
    return int(row[0])


# ---------------------------------------------------------------------------
# Package layout / back-compat
# ---------------------------------------------------------------------------


class TestPackageLayout:
    """Verify back-compat re-exports."""

    def test_possible_duplicate_back_compat(self) -> None:
        """from opportunities_engine.events import POSSIBLE_DUPLICATE still resolves."""
        from opportunities_engine.events import POSSIBLE_DUPLICATE as PD

        assert PD == "possible_duplicate"

    def test_all_constants_importable_from_package(self) -> None:
        """All vocab constants are importable from the events package root."""
        from opportunities_engine.events import (
            ALL_EVENT_TYPES,
            APPLIED,
            INTERVIEW,
            OFFER,
            PHONE_SCREEN,
            POSSIBLE_DUPLICATE,
            PUSHED_TO_LINEAR,
            REJECTED,
            SCORED,
            TERMINAL_EVENT_TYPES,
            WITHDREW,
        )

        assert POSSIBLE_DUPLICATE == "possible_duplicate"
        assert SCORED == "scored"
        assert PUSHED_TO_LINEAR == "pushed_to_linear"
        assert APPLIED == "applied"
        assert PHONE_SCREEN == "phone_screen"
        assert INTERVIEW == "interview"
        assert OFFER == "offer"
        assert REJECTED == "rejected"
        assert WITHDREW == "withdrew"
        assert len(ALL_EVENT_TYPES) == 9
        assert len(TERMINAL_EVENT_TYPES) == 3

    def test_emit_event_importable_from_package(self) -> None:
        """emit_event is importable from the events package root."""
        from opportunities_engine.events import emit_event as ee

        assert callable(ee)


# ---------------------------------------------------------------------------
# emit_event core behaviour
# ---------------------------------------------------------------------------


class TestEmitEvent:
    """Tests for emit_event helper."""

    def test_basic_emit_creates_row(self) -> None:
        """A basic emit inserts one events row with correct fields."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/1")
            emit_event(store, job_id, SCORED)

            rows = store.conn.execute(
                "SELECT job_id, event_type, actor, detail FROM events WHERE event_type = $1",
                [SCORED],
            ).fetchall()

        assert len(rows) == 1
        row = rows[0]
        assert row[0] == job_id
        assert row[1] == SCORED
        assert row[2] == "system"
        assert row[3] is None  # no detail passed

    def test_detail_round_trips_through_json(self) -> None:
        """detail dict is JSON-serialized and can be deserialized back."""
        payload = {"score": 0.87, "rank_position": 3, "notes": "great fit"}
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/2")
            emit_event(store, job_id, APPLIED, detail=payload)

            row = store.conn.execute(
                "SELECT detail FROM events WHERE event_type = $1",
                [APPLIED],
            ).fetchone()

        assert row is not None
        recovered = json.loads(row[0])
        assert recovered == payload

    def test_occurred_at_override_is_respected(self) -> None:
        """Passing an explicit occurred_at stores that exact timestamp."""
        fixed_ts = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/3")
            emit_event(store, job_id, OFFER, occurred_at=fixed_ts)

            row = store.conn.execute(
                "SELECT occurred_at FROM events WHERE event_type = $1",
                [OFFER],
            ).fetchone()

        assert row is not None
        # DuckDB may return a datetime object; compare as string prefix
        stored = str(row[0])
        assert "2024-06-15" in stored
        assert "12:30:00" in stored

    def test_unknown_event_type_raises_value_error(self) -> None:
        """emit_event raises ValueError for an event_type not in ALL_EVENT_TYPES."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/4")
            with pytest.raises(ValueError) as exc_info:
                emit_event(store, job_id, "not_a_real_event")

        assert "not_a_real_event" in str(exc_info.value)
        # Should mention allowed types
        assert "scored" in str(exc_info.value)

    def test_actor_defaults_to_system(self) -> None:
        """actor defaults to 'system' when not provided."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/5")
            emit_event(store, job_id, REJECTED)

            row = store.conn.execute(
                "SELECT actor FROM events WHERE event_type = $1",
                [REJECTED],
            ).fetchone()

        assert row is not None
        assert row[0] == "system"

    def test_actor_override_is_stored(self) -> None:
        """Providing actor= stores the custom actor string."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/6")
            emit_event(store, job_id, WITHDREW, actor="keegan")

            row = store.conn.execute(
                "SELECT actor FROM events WHERE event_type = $1",
                [WITHDREW],
            ).fetchone()

        assert row is not None
        assert row[0] == "keegan"

    def test_multiple_events_per_job_all_land(self) -> None:
        """Multiple events for the same job_id all insert without constraint errors."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/7")
            emit_event(store, job_id, SCORED, detail={"score": 0.9, "rank_position": 0})
            emit_event(store, job_id, PUSHED_TO_LINEAR, detail={"linear_issue_id": "abc"})
            emit_event(store, job_id, APPLIED, actor="keegan")

            count = store.conn.execute(
                "SELECT COUNT(*) FROM events WHERE job_id = $1",
                [job_id],
            ).fetchone()[0]

        assert count == 3

    def test_detail_none_stored_as_sql_null(self) -> None:
        """Passing detail=None stores SQL NULL, not the string 'null'."""
        with JobStore(":memory:") as store:
            job_id = _insert_job(store, "https://example.com/job/8")
            emit_event(store, job_id, PHONE_SCREEN, detail=None)

            row = store.conn.execute(
                "SELECT detail FROM events WHERE event_type = $1",
                [PHONE_SCREEN],
            ).fetchone()

        assert row is not None
        assert row[0] is None
