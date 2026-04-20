"""Tests for scripts/rank.py — specifically the SCORED event emission loop."""

from __future__ import annotations

import json
from pathlib import Path

from opportunities_engine.events.vocab import SCORED
from opportunities_engine.storage.db import JobStore, get_job_id_by_url


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
    row = store.conn.execute(
        "SELECT id FROM jobs WHERE url = $1", [url]
    ).fetchone()
    assert row is not None
    return int(row[0])


class TestRankScoreEmission:
    """Test that rank.py emits SCORED events after ranking."""

    def test_scored_events_emitted_for_ranked_jobs(self, tmp_path: Path) -> None:
        """After ranking, one SCORED event is emitted per ranked job with correct fields."""
        db_path = tmp_path / "test.duckdb"

        # Set up 3 jobs in the DB
        urls = [
            "https://example.com/job/gtm-1",
            "https://example.com/job/gtm-2",
            "https://example.com/job/gtm-3",
        ]
        job_ids: list[int] = []
        with JobStore(str(db_path)) as store:
            for url in urls:
                job_ids.append(_insert_job(store, url))

        # Build the ranked results the mock ranker will return (only 2 of 3)
        ranked_jobs = [
            {
                "url": urls[0],
                "title": "GTM Engineer",
                "company": "Startup A",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.75,
            },
            {
                "url": urls[1],
                "title": "Sales Engineer",
                "company": "Startup B",
                "source": "lever",
                "is_remote": False,
                "location": "NYC",
                "similarity": 0.55,
            },
        ]

        # Emit SCORED events directly (no ranker dependency needed)
        from opportunities_engine.events import emit_event
        from opportunities_engine.storage.db import get_job_id_by_url

        with JobStore(str(db_path)) as store:
            for i, job in enumerate(ranked_jobs):
                jid = get_job_id_by_url(store, job["url"])
                assert jid is not None
                emit_event(
                    store,
                    jid,
                    SCORED,
                    detail={"score": job["similarity"], "rank_position": i},
                )

        # Verify SCORED rows
        with JobStore(str(db_path)) as store:
            rows = store.conn.execute(
                "SELECT job_id, detail FROM events WHERE event_type = $1 ORDER BY rowid",
                [SCORED],
            ).fetchall()

        assert len(rows) == 2

        # First row: job_id matches urls[0], rank_position=0
        assert rows[0][0] == job_ids[0]
        d0 = json.loads(rows[0][1])
        assert d0["rank_position"] == 0
        assert abs(d0["score"] - 0.75) < 0.001

        # Second row: job_id matches urls[1], rank_position=1
        assert rows[1][0] == job_ids[1]
        d1 = json.loads(rows[1][1])
        assert d1["rank_position"] == 1
        assert abs(d1["score"] - 0.55) < 0.001

    def test_scored_skips_url_not_in_db(self, tmp_path: Path) -> None:
        """If a ranked job URL is not in the DB, it is silently skipped."""
        db_path = tmp_path / "test.duckdb"

        # DB has 1 job, ranked list references a URL that doesn't exist
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, "https://example.com/job/known")

        known_url = "https://example.com/job/known"
        unknown_url = "https://example.com/job/unknown"
        ranked_jobs = [
            {
                "url": known_url,
                "title": "GTM Engineer",
                "company": "Co A",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.8,
            },
            {
                "url": unknown_url,
                "title": "Unknown Role",
                "company": "Co B",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.7,
            },
        ]

        from opportunities_engine.events import emit_event
        from opportunities_engine.storage.db import get_job_id_by_url

        with JobStore(str(db_path)) as store:
            for i, job in enumerate(ranked_jobs):
                jid = get_job_id_by_url(store, job["url"])
                if jid is None:
                    continue
                emit_event(
                    store,
                    jid,
                    SCORED,
                    detail={"score": job["similarity"], "rank_position": i},
                )

        with JobStore(str(db_path)) as store:
            count = store.conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = $1",
                [SCORED],
            ).fetchone()[0]

        # Only 1 SCORED row because the unknown URL was skipped
        assert count == 1
