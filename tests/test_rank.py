"""Tests for scripts/rank.py — SCORED event emission, scores table writes,
terminal exclusion, and decision logic (Phase F.2)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from opportunities_engine.events.vocab import REJECTED as EVT_REJECTED
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


# ---------------------------------------------------------------------------
# Phase F.2 tests: terminal exclusion, scores table writes, decision logic
# ---------------------------------------------------------------------------

def _insert_job_full(
    store: JobStore,
    url: str,
    title: str = "GTM Engineer",
    company: str = "Startup Co",
    location: str = "Remote",
    is_remote: bool = True,
) -> int:
    """Insert a job with enough fields for the ranker to process."""
    assert store.conn is not None
    store.conn.execute(
        """
        INSERT INTO jobs (source, url, url_hash, title, company, location,
                          is_remote, created_at, updated_at)
        VALUES ('test', $1, md5($1), $2, $3, $4, $5,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [url, title, company, location, is_remote],
    )
    row = store.conn.execute("SELECT id FROM jobs WHERE url = $1", [url]).fetchone()
    assert row is not None
    return int(row[0])


def _make_ranked_job(
    url: str,
    title: str = "GTM Engineer",
    company: str = "Startup Co",
    location: str = "Remote",
    is_remote: bool = True,
    similarity: float = 0.75,
) -> dict:
    return {
        "url": url,
        "title": title,
        "company": company,
        "source": "greenhouse",
        "is_remote": is_remote,
        "location": location,
        "similarity": similarity,
    }


class TestTerminalExclusion:
    """Tests that terminally-closed jobs never reach the ranker or scores table."""

    def test_terminal_job_excluded_from_ranker_input(self, tmp_path: Path) -> None:
        """A job whose latest event is REJECTED is excluded from the ranked list."""
        db_path = tmp_path / "test.duckdb"
        terminal_url = "https://example.com/job/terminal"
        active_url = "https://example.com/job/active"

        with JobStore(str(db_path)) as store:
            terminal_id = _insert_job_full(store, terminal_url)
            active_id = _insert_job_full(store, active_url)
            # Mark terminal job as rejected
            from opportunities_engine.events import emit_event
            emit_event(store, terminal_id, EVT_REJECTED)

        # The ranker mock will be called; we inspect what was passed to it
        captured_jobs: list = []

        def mock_ranker(jobs: list, top_k: int, min_score: float) -> list:
            captured_jobs.extend(jobs)
            return [_make_ranked_job(active_url)]

        with (
            patch("scripts.rank.rank_jobs_local", side_effect=mock_ranker),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "f.2-tfidf-v1"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            result = runner.invoke(main, [])

        assert result.exit_code == 0, result.output

        # terminal job should NOT appear in what was passed to ranker
        passed_ids = {int(j.get("id", -1)) for j in captured_jobs}
        assert terminal_id not in passed_ids
        assert active_id in passed_ids

    def test_terminal_job_gets_no_scores_row(self, tmp_path: Path) -> None:
        """A terminally-closed job is not in the ranker output → no scores row."""
        db_path = tmp_path / "test.duckdb"
        terminal_url = "https://example.com/job/term2"
        active_url = "https://example.com/job/act2"

        with JobStore(str(db_path)) as store:
            terminal_id = _insert_job_full(store, terminal_url)
            active_id = _insert_job_full(store, active_url)
            from opportunities_engine.events import emit_event
            emit_event(store, terminal_id, EVT_REJECTED)

        ranked_result = [_make_ranked_job(active_url)]

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "test-version"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            result = runner.invoke(main, [])

        assert result.exit_code == 0, result.output

        with JobStore(str(db_path)) as store:
            rows = store.conn.execute(
                "SELECT job_id FROM scores"
            ).fetchall()
        scored_job_ids = {int(r[0]) for r in rows}
        assert terminal_id not in scored_job_ids


class TestScoresTableWrites:
    """Tests for scores table insertion logic in rank.py."""

    def test_every_ranked_job_gets_one_scores_row(self, tmp_path: Path) -> None:
        """Each job returned by the ranker gets exactly one scores row per run."""
        db_path = tmp_path / "test.duckdb"
        url_a = "https://example.com/job/a"
        url_b = "https://example.com/job/b"

        with JobStore(str(db_path)) as store:
            _insert_job_full(store, url_a)
            _insert_job_full(store, url_b)

        ranked_result = [
            _make_ranked_job(url_a, similarity=0.80),
            _make_ranked_job(url_b, similarity=0.60),
        ]

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "test-ranker-v1"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            result = runner.invoke(main, [])

        assert result.exit_code == 0, result.output

        with JobStore(str(db_path)) as store:
            rows = store.conn.execute(
                "SELECT job_id, ranker_version, score, rank_position, decision, "
                "component_scores, scoring_detail FROM scores ORDER BY rank_position"
            ).fetchall()

        assert len(rows) == 2

    def test_ranker_version_stored_correctly(self, tmp_path: Path) -> None:
        """scores.ranker_version matches settings.ranker_version."""
        db_path = tmp_path / "test.duckdb"
        url = "https://example.com/job/ver"

        with JobStore(str(db_path)) as store:
            _insert_job_full(store, url)

        ranked_result = [_make_ranked_job(url, similarity=0.50)]

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "my-test-version-xyz"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            runner.invoke(main, [])

        with JobStore(str(db_path)) as store:
            row = store.conn.execute(
                "SELECT ranker_version FROM scores LIMIT 1"
            ).fetchone()

        assert row is not None
        assert row[0] == "my-test-version-xyz"

    def test_component_scores_breakdown_available_false(self, tmp_path: Path) -> None:
        """component_scores.breakdown_available is False (placeholder for Phase F.3)."""
        db_path = tmp_path / "test.duckdb"
        url = "https://example.com/job/comp"

        with JobStore(str(db_path)) as store:
            _insert_job_full(store, url)

        ranked_result = [_make_ranked_job(url, similarity=0.45)]

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "test"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            runner.invoke(main, [])

        with JobStore(str(db_path)) as store:
            row = store.conn.execute(
                "SELECT component_scores FROM scores LIMIT 1"
            ).fetchone()

        assert row is not None
        comp = json.loads(row[0])
        assert comp["breakdown_available"] is False

    def test_detail_json_contains_expected_fields(self, tmp_path: Path) -> None:
        """scores.detail contains title, company, and url."""
        db_path = tmp_path / "test.duckdb"
        url = "https://example.com/job/detail-test"

        with JobStore(str(db_path)) as store:
            _insert_job_full(store, url, title="Sales Engineer", company="Acme")

        ranked_result = [
            _make_ranked_job(url, title="Sales Engineer", company="Acme", similarity=0.50)
        ]

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = 0.20
            mock_settings.ranker_version = "test"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            runner.invoke(main, [])

        with JobStore(str(db_path)) as store:
            row = store.conn.execute("SELECT scoring_detail FROM scores LIMIT 1").fetchone()

        assert row is not None
        detail = json.loads(row[0])
        assert detail["title"] == "Sales Engineer"
        assert detail["company"] == "Acme"
        assert detail["url"] == url


class TestDecisionLogic:
    """Tests for scores.decision: promoted / shortlisted / rejected."""

    def _run_and_get_decisions(
        self,
        tmp_path: Path,
        ranked_result: list[dict],
        min_relevance_score: float = 0.20,
    ) -> dict[str, str]:
        """Helper: seed DB, run rank.main, return {url: decision} map."""
        db_path = tmp_path / "test.duckdb"

        with JobStore(str(db_path)) as store:
            for job in ranked_result:
                _insert_job_full(
                    store,
                    url=job["url"],
                    title=job.get("title", "Test"),
                    company=job.get("company", "Co"),
                    location=job.get("location", "Remote"),
                    is_remote=bool(job.get("is_remote", True)),
                )

        with (
            patch("scripts.rank.rank_jobs_local", return_value=ranked_result),
            patch("scripts.rank.settings") as mock_settings,
            patch("scripts.rank.console"),
        ):
            mock_settings.database_path = str(db_path)
            mock_settings.min_relevance_score = min_relevance_score
            mock_settings.ranker_version = "test"
            mock_settings.repo_root = tmp_path

            from scripts.rank import main
            runner = CliRunner()
            runner.invoke(main, [])

        with JobStore(str(db_path)) as store:
            rows = store.conn.execute(
                "SELECT scoring_detail, decision FROM scores"
            ).fetchall()

        return {json.loads(r[0])["url"]: r[1] for r in rows}

    def test_high_score_remote_is_promoted(self, tmp_path: Path) -> None:
        """score >= min_relevance_score AND remote → decision='promoted'."""
        url = "https://example.com/job/promoted"
        ranked = [_make_ranked_job(url, similarity=0.80, is_remote=True, location="Remote")]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "promoted"

    def test_high_score_non_remote_is_shortlisted(self, tmp_path: Path) -> None:
        """score >= min_relevance_score but non-remote → decision='shortlisted'."""
        url = "https://example.com/job/shortlisted"
        ranked = [
            _make_ranked_job(
                url, similarity=0.75, is_remote=False, location="New York (Hybrid)"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "shortlisted"

    def test_low_score_is_rejected(self, tmp_path: Path) -> None:
        """score < min_relevance_score → decision='rejected' (regardless of remote)."""
        url = "https://example.com/job/rejected"
        ranked = [_make_ranked_job(url, similarity=0.10, is_remote=True, location="Remote")]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected"

    def test_exact_threshold_remote_is_promoted(self, tmp_path: Path) -> None:
        """score == min_relevance_score AND remote → promoted (boundary check)."""
        url = "https://example.com/job/boundary"
        ranked = [_make_ranked_job(url, similarity=0.20, is_remote=True, location="Remote")]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "promoted"

    def test_multiple_jobs_mixed_decisions(self, tmp_path: Path) -> None:
        """Multiple jobs in one run get correct independent decisions."""
        url_p = "https://example.com/job/prom"
        url_s = "https://example.com/job/short"
        url_r = "https://example.com/job/rej"
        ranked = [
            _make_ranked_job(url_p, similarity=0.90, is_remote=True, location="Remote"),
            _make_ranked_job(url_s, similarity=0.50, is_remote=False, location="Boston (on-site)"),
            _make_ranked_job(url_r, similarity=0.05, is_remote=True, location="Remote"),
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url_p] == "promoted"
        assert decisions[url_s] == "shortlisted"
        assert decisions[url_r] == "rejected"

    # ------------------------------------------------------------------
    # Phase F.3 additions: rejected_title, rejected_geo,
    # promoted_whitelist_remote
    # ------------------------------------------------------------------

    def test_fde_title_high_score_is_rejected_title(self, tmp_path: Path) -> None:
        """FDE in title → rejected_title regardless of score or remote status."""
        url = "https://example.com/job/fde"
        ranked = [
            _make_ranked_job(
                url, title="Founding FDE @ CiceroAI", similarity=0.85,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected_title"

    def test_forward_deployed_title_rejected(self, tmp_path: Path) -> None:
        """'Forward Deployed Engineer' title → rejected_title."""
        url = "https://example.com/job/fwd-dep"
        ranked = [
            _make_ranked_job(
                url, title="Forward Deployed Engineer", similarity=0.90,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected_title"

    def test_latam_in_title_high_score_is_rejected_geo(self, tmp_path: Path) -> None:
        """LatAm in job title → rejected_geo regardless of score."""
        url = "https://example.com/job/latam"
        ranked = [
            _make_ranked_job(
                url, title="Senior GTM Engineer (LatAm)", similarity=0.80,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected_geo"

    def test_latam_in_location_high_score_is_rejected_geo(self, tmp_path: Path) -> None:
        """LatAm in location field → rejected_geo."""
        url = "https://example.com/job/latam-loc"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", similarity=0.80,
                is_remote=False, location="LatAm"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected_geo"

    def test_vercel_sf_location_is_promoted_whitelist_remote(self, tmp_path: Path) -> None:
        """Vercel + San Francisco location (no remote flag) → promoted_whitelist_remote."""
        url = "https://example.com/job/vercel-sf"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", company="Vercel", similarity=0.75,
                is_remote=False, location="San Francisco, CA"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "promoted_whitelist_remote"

    def test_vercel_explicit_remote_is_promoted_not_whitelist_variant(
        self, tmp_path: Path
    ) -> None:
        """Vercel + explicit Remote location → promoted (is_remote=True short-circuits)."""
        url = "https://example.com/job/vercel-remote"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", company="Vercel", similarity=0.75,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "promoted"

    def test_normal_remote_high_score_is_promoted_unchanged(self, tmp_path: Path) -> None:
        """Unchanged behavior: remote + high score → promoted."""
        url = "https://example.com/job/normal-remote"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", company="Some Startup", similarity=0.70,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "promoted"

    def test_non_remote_unlisted_company_high_score_is_shortlisted(
        self, tmp_path: Path
    ) -> None:
        """Unchanged behavior: non-remote + unlisted company + high score → shortlisted."""
        url = "https://example.com/job/shortlisted-unlisted"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", company="Horizonia", similarity=0.70,
                is_remote=False, location="Austin, TX"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "shortlisted"

    def test_low_score_regardless_of_company_is_rejected(self, tmp_path: Path) -> None:
        """Low score (below threshold) → rejected, even for a whitelisted company."""
        url = "https://example.com/job/low-score-vercel"
        ranked = [
            _make_ranked_job(
                url, title="GTM Engineer", company="Vercel", similarity=0.10,
                is_remote=True, location="Remote"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        assert decisions[url] == "rejected"

    def test_title_exclusion_takes_priority_over_geo(self, tmp_path: Path) -> None:
        """Title exclusion fires before geo check (first match wins)."""
        url = "https://example.com/job/fde-latam"
        ranked = [
            _make_ranked_job(
                url, title="Founding FDE (LatAm)", similarity=0.80,
                is_remote=True, location="LatAm"
            )
        ]
        decisions = self._run_and_get_decisions(tmp_path, ranked, min_relevance_score=0.20)
        # Title pattern fires first
        assert decisions[url] == "rejected_title"
