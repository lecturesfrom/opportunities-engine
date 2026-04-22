"""Tests for scripts/push_top_to_linear.py — PUSHED_TO_LINEAR event emission."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from opportunities_engine.events.vocab import PUSHED_TO_LINEAR
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
    row = store.conn.execute(
        "SELECT id FROM jobs WHERE url = $1", [url]
    ).fetchone()
    assert row is not None
    return int(row[0])


def _make_gql_response(
    issue_id: str = "issue-abc-123",
    identifier: str = "APP-1",
    title: str = "GTM Engineer @ Startup A",
    url: str = "https://linear.app/team/issue/APP-1",
) -> dict:
    """Build a fake successful issueCreate GraphQL response."""
    return {
        "data": {
            "issueCreate": {
                "success": True,
                "issue": {
                    "id": issue_id,
                    "identifier": identifier,
                    "title": title,
                    "url": url,
                },
            }
        }
    }


class TestPushTopToLinear:
    """Test PUSHED_TO_LINEAR emission from push_top_to_linear.py."""

    def test_pushed_to_linear_emitted_on_success(self, tmp_path: Path) -> None:
        """After a successful issueCreate, one PUSHED_TO_LINEAR row is written."""
        db_path = tmp_path / "test.duckdb"
        job_url = "https://example.com/job/gtm-1"
        ranked_json = tmp_path / "ranked_jobs.json"

        # Seed one job in the DB
        with JobStore(str(db_path)) as store:
            job_id = _insert_job(store, job_url, "GTM Engineer")

        # Write a ranked_jobs.json with our job
        jobs_data = [
            {
                "url": job_url,
                "title": "GTM Engineer",
                "company": "Startup A",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.88,
                "description": "Some job description",
            }
        ]
        ranked_json.write_text(json.dumps(jobs_data))

        gql_response = _make_gql_response(
            issue_id="issue-abc-123",
            identifier="APP-1",
            title="GTM Engineer @ Startup A",
            url="https://linear.app/team/issue/APP-1",
        )

        with (
            patch("scripts.push_top_to_linear.RANKED", ranked_json),
            patch("scripts.push_top_to_linear.gql", return_value=gql_response),
            patch(
                "scripts.push_top_to_linear.existing_issue_titles", return_value=set()
            ),
            patch("scripts.push_top_to_linear.settings") as mock_settings,
            patch("scripts.push_top_to_linear.console"),
            patch("scripts.push_top_to_linear.make_description", return_value="desc"),
        ):
            mock_settings.database_path = str(db_path)
            from scripts.push_top_to_linear import main

            runner = CliRunner()
            result = runner.invoke(main, [])

        assert result.exit_code == 0, result.output

        # Check the PUSHED_TO_LINEAR event was written
        with JobStore(str(db_path)) as store:
            rows = store.conn.execute(
                "SELECT job_id, detail FROM events WHERE event_type = $1",
                [PUSHED_TO_LINEAR],
            ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == job_id
        detail = json.loads(rows[0][1])
        assert detail["linear_issue_id"] == "issue-abc-123"
        assert detail["linear_issue_url"] == "https://linear.app/team/issue/APP-1"

    def test_dry_run_emits_no_events(self, tmp_path: Path) -> None:
        """When --dry-run is passed, no PUSHED_TO_LINEAR rows are written."""
        db_path = tmp_path / "test.duckdb"
        job_url = "https://example.com/job/gtm-2"
        ranked_json = tmp_path / "ranked_jobs.json"

        with JobStore(str(db_path)) as store:
            _insert_job(store, job_url, "GTM Engineer")

        jobs_data = [
            {
                "url": job_url,
                "title": "GTM Engineer",
                "company": "Startup B",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.75,
                "description": "desc",
            }
        ]
        ranked_json.write_text(json.dumps(jobs_data))

        with (
            patch("scripts.push_top_to_linear.RANKED", ranked_json),
            patch("scripts.push_top_to_linear.gql") as mock_gql,
            patch(
                "scripts.push_top_to_linear.existing_issue_titles", return_value=set()
            ),
            patch("scripts.push_top_to_linear.settings") as mock_settings,
            patch("scripts.push_top_to_linear.console"),
        ):
            mock_settings.database_path = str(db_path)
            from scripts.push_top_to_linear import main

            runner = CliRunner()
            result = runner.invoke(main, ["--dry-run"])

        assert result.exit_code == 0, result.output

        # gql should NOT have been called in dry-run mode
        mock_gql.assert_not_called()

        # No PUSHED_TO_LINEAR events
        with JobStore(str(db_path)) as store:
            count = store.conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = $1",
                [PUSHED_TO_LINEAR],
            ).fetchone()[0]

        assert count == 0

    def test_job_url_not_in_db_emits_nothing(self, tmp_path: Path) -> None:
        """If the job URL is not in the DB, no event is emitted (no crash)."""
        db_path = tmp_path / "test.duckdb"
        ranked_json = tmp_path / "ranked_jobs.json"

        # DB is empty (no jobs)
        with JobStore(str(db_path)):
            pass

        jobs_data = [
            {
                "url": "https://example.com/job/unknown",
                "title": "Unknown Role",
                "company": "Co A",
                "source": "greenhouse",
                "is_remote": True,
                "location": "Remote",
                "similarity": 0.6,
                "description": "desc",
            }
        ]
        ranked_json.write_text(json.dumps(jobs_data))

        gql_response = _make_gql_response(
            issue_id="issue-xyz",
            identifier="APP-2",
            title="Unknown Role @ Co A",
            url="https://linear.app/team/issue/APP-2",
        )

        with (
            patch("scripts.push_top_to_linear.RANKED", ranked_json),
            patch("scripts.push_top_to_linear.gql", return_value=gql_response),
            patch(
                "scripts.push_top_to_linear.existing_issue_titles", return_value=set()
            ),
            patch("scripts.push_top_to_linear.settings") as mock_settings,
            patch("scripts.push_top_to_linear.console"),
            patch("scripts.push_top_to_linear.make_description", return_value="desc"),
        ):
            mock_settings.database_path = str(db_path)
            from scripts.push_top_to_linear import main

            runner = CliRunner()
            result = runner.invoke(main, [])

        assert result.exit_code == 0, result.output

        with JobStore(str(db_path)) as store:
            count = store.conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type = $1",
                [PUSHED_TO_LINEAR],
            ).fetchone()[0]

        assert count == 0


class TestRemoteGate:
    """Unit tests for the _is_remote hard gate in push_top_to_linear.py."""

    def _call(self, **kwargs: object) -> bool:
        from scripts.push_top_to_linear import _is_remote
        return _is_remote(kwargs)

    def test_is_remote_true_flag_passes(self) -> None:
        """Job with is_remote=True always passes, regardless of location."""
        assert self._call(is_remote=True, location="New York, NY") is True

    def test_hybrid_location_fails(self) -> None:
        """Job with 'hybrid' in location is rejected."""
        assert self._call(is_remote=None, location="San Francisco, CA (Hybrid)") is False

    def test_onsite_location_fails(self) -> None:
        """Job with 'onsite' in location is rejected."""
        assert self._call(is_remote=None, location="Austin TX — Onsite") is False

    def test_in_office_location_fails(self) -> None:
        """Job with 'in-office' in location is rejected."""
        assert self._call(is_remote=None, location="Seattle, WA — In-Office") is False

    def test_on_site_hyphenated_fails(self) -> None:
        """Job with 'on-site' in location is rejected."""
        assert self._call(is_remote=False, location="Chicago, IL — on-site") is False

    def test_remote_location_with_none_flag_passes(self) -> None:
        """Job with location='Remote' and is_remote=None passes."""
        assert self._call(is_remote=None, location="Remote") is True

    def test_anywhere_location_passes(self) -> None:
        """Job with 'anywhere' in location passes."""
        assert self._call(is_remote=None, location="Work from anywhere") is True

    def test_unknown_location_no_remote_markers_fails(self) -> None:
        """Job with is_remote=None and no remote signals is dropped (conservative default)."""
        assert self._call(is_remote=None, location="Boston, MA") is False

    def test_missing_location_field_fails(self) -> None:
        """Job with no location key and is_remote=None is dropped."""
        assert self._call(is_remote=None) is False

    def test_is_remote_false_with_non_remote_location_fails(self) -> None:
        """Job with is_remote=False and 'hybrid' location is rejected (non-remote marker wins)."""
        assert self._call(is_remote=False, location="hybrid") is False
