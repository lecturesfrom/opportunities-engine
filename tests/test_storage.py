"""Tests for the DuckDB JobStore."""

from datetime import datetime, timedelta, timezone

import pytest

from opportunities_engine.storage.db import JobStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    """In-memory DuckDB JobStore via context manager."""
    with JobStore(":memory:") as s:
        yield s


def _make_job(n: int, **overrides) -> dict:
    """Build a sample job dict. *n* varies the URL / title so rows are unique."""
    base = {
        "source": "greenhouse",
        "source_id": str(n),
        "url": f"https://boards.greenhouse.io/testco/jobs/{n}",
        "title": f"Software Engineer {n}",
        "company": "TestCo",
        "location": "San Francisco, CA",
        "description": "Build great software.",
        "is_remote": False,
        "job_type": "full_time",
        "department": "Engineering",
        "metadata": {"test": True},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestJobStore:
    def test_insert_three_jobs(self, store: JobStore):
        for i in range(1, 4):
            assert store.upsert_job(_make_job(i)) is True

        jobs = store.get_jobs()
        assert len(jobs) == 3

    def test_upsert_dedup(self, store: JobStore):
        """Insert same URL twice — count stays at 3."""
        for i in range(1, 4):
            store.upsert_job(_make_job(i))

        # Re-insert job #2 (same URL) with a slightly different title
        was_new = store.upsert_job(_make_job(2, title="Senior Software Engineer 2"))
        assert was_new is False, "upsert should return False for an existing URL"

        jobs = store.get_jobs()
        assert len(jobs) == 3, "Re-inserting a duplicate URL must not create a new row"

        # The title should have been updated
        job2 = [j for j in jobs if j["source_id"] == "2"][0]
        assert job2["title"] == "Senior Software Engineer 2"

    def test_mark_seen(self, store: JobStore):
        store.upsert_job(_make_job(1))
        store.mark_seen(_make_job(1)["url"])

        jobs = store.get_jobs()
        assert len(jobs) == 1
        # date_last_seen should be >= date_first_seen (both are UTC now)
        assert jobs[0]["date_last_seen"] is not None

    def test_get_new_jobs(self, store: JobStore):
        for i in range(1, 4):
            store.upsert_job(_make_job(i))

        # All jobs are new and freshly inserted — should be returned
        new_jobs = store.get_new_jobs(since_hours=24)
        assert len(new_jobs) == 3

        # Mark one as 'interested' — it should no longer appear
        store.conn.execute(
            "UPDATE jobs SET status = 'interested' WHERE source_id = '1'"
        )
        new_jobs = store.get_new_jobs(since_hours=24)
        assert len(new_jobs) == 2

    def test_get_new_jobs_since_cutoff(self, store: JobStore):
        """A job first seen >24 h ago should not appear in the default query."""
        now = datetime.now(timezone.utc)
        store.upsert_job(_make_job(1))

        # Manually back-date date_first_seen
        store.conn.execute(
            "UPDATE jobs SET date_first_seen = $1 WHERE source_id = '1'",
            [now - timedelta(hours=48)],
        )

        new_jobs = store.get_new_jobs(since_hours=24)
        assert len(new_jobs) == 0

    def test_upsert_company(self, store: JobStore):
        assert store.upsert_company({"name": "TestCo", "ats_platform": "greenhouse", "ats_slug": "testco"}) is True
        assert store.upsert_company({"name": "TestCo", "industry": "SaaS"}) is False

    def test_get_jobs_by_status(self, store: JobStore):
        store.upsert_job(_make_job(1, status="new"))
        store.upsert_job(_make_job(2, status="interested"))

        new_only = store.get_jobs(status="new")
        assert len(new_only) == 1
        assert new_only[0]["status"] == "new"
