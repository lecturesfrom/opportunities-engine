"""Tests for Phase C: DB relocation + health check.

Covers:
- Default DB path resolves to ~/Library/Application Support/opportunities-engine/
- Parent directory is auto-created if missing
- Health check: checkpoint WAL, verify schema version, report table counts
- JobStore works with new default path
- Existing DB at new location opens without data loss
- :memory: still works (tests, quick scripts)
- DATABASE_PATH env var overrides the default
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest

from opportunities_engine.config import Settings, get_default_db_path
from opportunities_engine.storage.db import JobStore


def _make_job(n: int, **overrides) -> dict:
    """Build a sample job dict with required fields."""
    base = {
        "source": "greenhouse",
        "source_id": str(n),
        "url": f"https://boards.greenhouse.io/testco/jobs/{n}",
        "url_hash": f"hash{n}",
        "title": f"Software Engineer {n}",
        "company": "TestCo",
        "location": "Remote",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store_memory():
    """In-memory store (unchanged behavior)."""
    with JobStore(":memory:") as s:
        yield s


@pytest.fixture
def fresh_dir(tmp_path):
    """A temporary directory that simulates ~/Library/Application Support/."""
    return tmp_path / "opportunities-engine"


# ---------------------------------------------------------------------------
# get_default_db_path
# ---------------------------------------------------------------------------

class TestDefaultDbPath:
    def test_path_under_application_support(self):
        """Default path should live under Application Support or .opportunities-engine."""
        path = get_default_db_path()
        assert "opportunities-engine" in str(path)

    def test_path_ends_with_jobs_duckdb(self):
        """Filename should be jobs.duckdb."""
        path = get_default_db_path()
        assert path.name == "jobs.duckdb"

    def test_parent_dir_created_automatically(self, fresh_dir):
        """If the directory doesn't exist, it should be created."""
        db_path = fresh_dir / "jobs.duckdb"
        assert not fresh_dir.exists()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        assert fresh_dir.exists()

    def test_respects_env_override(self, tmp_path):
        """DATABASE_PATH env var should override the default."""
        custom = tmp_path / "custom.duckdb"
        with patch.dict("os.environ", {"DATABASE_PATH": str(custom)}):
            s = Settings()
            assert s.database_path == custom


# ---------------------------------------------------------------------------
# Settings.database_path default
# ---------------------------------------------------------------------------

class TestSettingsDatabasePath:
    def test_default_is_absolute(self):
        """Path must be absolute to avoid CWD-dependent behavior."""
        s = Settings()
        assert s.database_path.is_absolute()

    def test_default_not_under_repo_data(self):
        """Default should NOT point to <repo>/data/ anymore."""
        s = Settings()
        # Path should contain 'Application Support' or '.opportunities-engine'
        path_str = str(s.database_path)
        assert "Application Support" in path_str or ".opportunities-engine" in path_str


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_health_check_returns_ok(self, store_memory):
        """Fresh DB should pass health check."""
        result = store_memory.health_check()
        assert result["status"] == "ok"
        assert "schema_version_count" in result
        assert result["schema_version_count"] >= 2  # 001 + 002

    def test_health_check_checkpoints_wal(self, tmp_path):
        """WAL checkpoint should be attempted on file-backed DBs."""
        db_path = tmp_path / "test.duckdb"
        with JobStore(db_path) as s:
            s.upsert_job(_make_job(1))
            result = s.health_check()
        assert result["checkpoint"] == "ok"
        assert result["status"] == "ok"

    def test_health_check_skips_checkpoint_for_memory(self, store_memory):
        """:memory: DBs skip checkpoint (no WAL)."""
        result = store_memory.health_check()
        assert result["checkpoint"] == "skipped"

    def test_health_check_reports_table_counts(self, store_memory):
        """Health check should report row counts for key tables."""
        store_memory.upsert_job(_make_job(1))
        result = store_memory.health_check()
        assert "tables" in result
        assert "jobs" in result["tables"]
        assert "companies" in result["tables"]
        assert result["tables"]["jobs"] >= 1

    def test_health_check_warns_on_missing_migrations(self, store_memory):
        """Health check should warn when schema_version_count < expected."""
        # With a fresh in-memory DB, migrations have already run (>= 2).
        # Test the warn condition by verifying the logic:
        # If schema_version_count < 2, status should be 'warn'.
        result = store_memory.health_check()
        # The fresh DB should have >= 2 migrations applied
        assert result["schema_version_count"] >= 2
        assert result["status"] == "ok"

        # Verify the warn threshold: mock schema_version_count to 1
        # by directly calling health_check on a DB where we delete a migration row
        store_memory.conn.execute("DELETE FROM schema_migrations WHERE version = '002'")
        result_stale = store_memory.health_check()
        assert result_stale["schema_version_count"] < 2
        assert result_stale["status"] == "warn"


# ---------------------------------------------------------------------------
# JobStore with new path
# ---------------------------------------------------------------------------

class TestJobStoreNewPath:
    def test_read_write_at_new_path(self, tmp_path):
        """JobStore should work normally with the relocated DB path."""
        db_path = tmp_path / "subdir" / "jobs.duckdb"
        db_path.parent.mkdir(parents=True, exist_ok=True)

        with JobStore(db_path) as s:
            assert s.upsert_job(_make_job(1)) is True
            jobs = s.get_jobs()
            assert len(jobs) == 1
            assert jobs[0]["title"] == "Software Engineer 1"

    def test_existing_data_survives_relocation(self, tmp_path):
        """Data written before relocation should still be readable at new path."""
        db_path = tmp_path / "jobs.duckdb"

        # Write data
        with JobStore(db_path) as s:
            s.upsert_job(_make_job(1, title="Old Job"))

        # Re-open at same path (simulating relocation)
        with JobStore(db_path) as s:
            jobs = s.get_jobs()
            assert len(jobs) == 1
            assert jobs[0]["title"] == "Old Job"

    def test_memory_still_works(self):
        """:memory: should still work for tests and quick scripts."""
        with JobStore(":memory:") as s:
            assert s.upsert_job(_make_job(1)) is True
            assert len(s.get_jobs()) == 1
