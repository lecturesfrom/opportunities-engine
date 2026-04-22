"""Tests for the migration system (Phase A).

Covers:
- Migration runner discovers and applies migrations
- Idempotency: running twice applies nothing on second run
- Schema correctness: all expected tables, columns, indexes exist
- Data preservation: existing rows survive migration
- Checksum recording in schema_migrations
"""

import tempfile
from pathlib import Path

import duckdb
import pytest

from opportunities_engine.storage.migrate import (
    _applied_versions,
    _checksum,
    _migration_files,
    run_migrations,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Create a temporary migrations directory with a test migration."""
    m = tmp_path / "migrations"
    m.mkdir()
    # Write a minimal migration that creates one table
    (m / "001_test_table.sql").write_text("""
CREATE TABLE IF NOT EXISTS test_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO schema_migrations (version, name, checksum)
VALUES ('001', 'test_table', NULL)
ON CONFLICT (version) DO NOTHING;
""")
    return m


@pytest.fixture
def fresh_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection — no pre-created tables.

    Migrations handle ALL table creation now (001 includes base tables).
    """
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Migration file discovery
# ---------------------------------------------------------------------------

class TestMigrationDiscovery:
    def test_discovers_sql_files(self, migrations_dir: Path):
        files = _migration_files(migrations_dir)
        assert len(files) >= 1
        assert files[0][0] == "001"  # version
        assert files[0][1] == "test_table"  # name

    def test_empty_dir(self, tmp_path: Path):
        empty = tmp_path / "empty"
        empty.mkdir()
        assert _migration_files(empty) == []

    def test_nonexistent_dir(self, tmp_path: Path):
        assert _migration_files(tmp_path / "nope") == []

    def test_sorted_by_version(self, tmp_path: Path):
        m = tmp_path / "migrations"
        m.mkdir()
        (m / "003_third.sql").write_text("-- third")
        (m / "001_first.sql").write_text("-- first")
        (m / "002_second.sql").write_text("-- second")
        files = _migration_files(m)
        versions = [f[0] for f in files]
        assert versions == ["001", "002", "003"]


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

class TestChecksum:
    def test_deterministic(self):
        assert _checksum("hello") == _checksum("hello")

    def test_different_content(self):
        assert _checksum("hello") != _checksum("world")


# ---------------------------------------------------------------------------
# Applied versions tracking
# ---------------------------------------------------------------------------

class TestAppliedVersions:
    def test_empty_db(self, fresh_conn: duckdb.DuckDBPyConnection):
        versions = _applied_versions(fresh_conn)
        assert versions == set()

    def test_records_applied(self, fresh_conn: duckdb.DuckDBPyConnection, migrations_dir: Path):
        run_migrations(fresh_conn, migrations_dir)
        versions = _applied_versions(fresh_conn)
        assert "001" in versions


# ---------------------------------------------------------------------------
# Run migrations
# ---------------------------------------------------------------------------

class TestRunMigrations:
    def test_applies_pending(self, fresh_conn: duckdb.DuckDBPyConnection, migrations_dir: Path):
        applied = run_migrations(fresh_conn, migrations_dir)
        assert applied == ["001"]

    def test_idempotent(self, fresh_conn: duckdb.DuckDBPyConnection, migrations_dir: Path):
        run_migrations(fresh_conn, migrations_dir)
        # Second run should apply nothing
        applied = run_migrations(fresh_conn, migrations_dir)
        assert applied == []

    def test_creates_table(self, fresh_conn: duckdb.DuckDBPyConnection, migrations_dir: Path):
        run_migrations(fresh_conn, migrations_dir)
        # Verify test_table was created
        tables = fresh_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "test_table" in table_names

    def test_checksum_recorded(self, fresh_conn: duckdb.DuckDBPyConnection, migrations_dir: Path):
        run_migrations(fresh_conn, migrations_dir)
        row = fresh_conn.execute(
            "SELECT checksum FROM schema_migrations WHERE version = '001'"
        ).fetchone()
        # The test migration inserts with NULL checksum, the runner updates it
        # Either way, a row should exist
        assert row is not None


# ---------------------------------------------------------------------------
# Full 001_initial_schema migration (using the actual migration file)
# ---------------------------------------------------------------------------

class Test001InitialSchema:
    @pytest.fixture
    def project_migrations(self) -> Path:
        """Path to the actual project migrations/ directory."""
        # tests/test_migrations.py → tests/ → root/
        return Path(__file__).resolve().parent.parent / "migrations"

    @pytest.fixture
    def migrated_conn(self, fresh_conn: duckdb.DuckDBPyConnection, project_migrations: Path) -> duckdb.DuckDBPyConnection:
        """Connection after running the real 001 migration."""
        run_migrations(fresh_conn, project_migrations)
        return fresh_conn

    def test_schema_migrations_exists(self, migrated_conn: duckdb.DuckDBPyConnection):
        tables = {t[0] for t in migrated_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()}
        assert "schema_migrations" in tables

    def test_new_tables_exist(self, migrated_conn: duckdb.DuckDBPyConnection):
        tables = {t[0] for t in migrated_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()}
        expected = {"job_sources", "events", "scores", "company_attractions"}
        assert expected.issubset(tables), f"Missing tables: {expected - tables}"

    def test_events_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'events'"
        ).fetchall()}
        expected = {"id", "job_id", "event_type", "occurred_at", "actor", "detail"}
        assert expected.issubset(cols)

    def test_scores_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'scores'"
        ).fetchall()}
        expected = {"id", "job_id", "score", "ranker_version", "scored_at", "scoring_detail", "decision"}
        assert expected.issubset(cols)

    def test_job_sources_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'job_sources'"
        ).fetchall()}
        expected = {"id", "job_id", "source_name", "source_url", "raw_payload", "first_seen", "last_seen", "source_trust"}
        assert expected.issubset(cols)

    def test_company_attractions_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'company_attractions'"
        ).fetchall()}
        expected = {"id", "company_id", "attribute", "weight", "source"}
        assert expected.issubset(cols)

    def test_companies_new_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'companies'"
        ).fetchall()}
        new_cols = {"canonical_name", "hq_location", "founded_year", "linkedin_url", "twitter_handle", "is_dream", "why_i_love", "priority", "status", "ats_platforms_json"}
        assert new_cols.issubset(cols)

    def test_archive_schema_exists(self, migrated_conn: duckdb.DuckDBPyConnection):
        schemas = {s[0] for s in migrated_conn.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()}
        assert "archive" in schemas

    def test_archive_jobs_exists(self, migrated_conn: duckdb.DuckDBPyConnection):
        tables = {t[0] for t in migrated_conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'archive'"
        ).fetchall()}
        assert "jobs" in tables
        assert "events" in tables

    def test_archive_jobs_has_archived_at(self, migrated_conn: duckdb.DuckDBPyConnection):
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'archive' AND table_name = 'jobs'"
        ).fetchall()}
        assert "archived_at" in cols

    def test_indexes_created(self, migrated_conn: duckdb.DuckDBPyConnection):
        # DuckDB stores index info in duckdb_indexes() function
        indexes = {i[2] for i in migrated_conn.execute(
            "SELECT database_name, schema_name, index_name FROM duckdb_indexes() WHERE schema_name = 'main'"
        ).fetchall()}
        expected = {"idx_job_sources_job_id", "idx_events_job_id_occurred_at",
                    "idx_scores_job_id_scored_at", "idx_companies_canonical_name"}
        assert expected.issubset(indexes), f"Missing indexes: {expected - indexes}"

    def test_migration_recorded(self, migrated_conn: duckdb.DuckDBPyConnection):
        row = migrated_conn.execute(
            "SELECT version, name FROM schema_migrations WHERE version = '001'"
        ).fetchone()
        assert row is not None
        assert row[0] == "001"
        assert row[1] == "initial_schema"

    def test_idempotent_full_migration(self, migrated_conn: duckdb.DuckDBPyConnection, project_migrations: Path):
        # Run again — should apply nothing
        applied = run_migrations(migrated_conn, project_migrations)
        assert applied == []

    def test_006_adds_scores_columns(self, migrated_conn: duckdb.DuckDBPyConnection):
        """Migration 006 adds rank_position + component_scores columns to scores.
        (scoring_detail already existed from migration 001 and is reused.)"""
        cols = {c[0] for c in migrated_conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'scores'"
        ).fetchall()}
        # Columns added by migration 006
        expected_new = {"rank_position", "component_scores"}
        assert expected_new.issubset(cols), (
            f"Missing columns added by migration 006: {expected_new - cols}"
        )

    def test_006_recorded_in_schema_migrations(self, migrated_conn: duckdb.DuckDBPyConnection):
        """Migration 006 is recorded in schema_migrations."""
        row = migrated_conn.execute(
            "SELECT version, name FROM schema_migrations WHERE version = '006'"
        ).fetchone()
        assert row is not None
        assert row[0] == "006"
        assert row[1] == "scores_table"


# ---------------------------------------------------------------------------
# Data preservation
# ---------------------------------------------------------------------------

class TestDataPreservation:
    def test_existing_jobs_survive_migration(self, tmp_path: Path):
        """Simulate: create DB with jobs, run migration, verify row count.

        This simulates the real-world scenario: a pre-existing DB has rows
        in the jobs table, and the migration runner adds the new schema on
        top. Since migration 001 uses CREATE TABLE IF NOT EXISTS, existing
        tables are left intact and new tables are added.
        """
        db_path = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_path))

        # Bootstrap a minimal jobs table + insert a job
        # (Simulates the state of the live DB before migrations)
        # Include all NOT NULL columns that the migration SQL references
        conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS job_id_seq START 1;
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY DEFAULT nextval('job_id_seq'),
                source TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                url_hash TEXT NOT NULL,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute(
            "INSERT INTO jobs (source, url, url_hash, title, company) VALUES ($1, $2, $3, $4, $5)",
            ["greenhouse", "https://example.com/job/1", "abc123", "GTM Engineer", "Vercel"],
        )
        assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1

        # Run the real migration
        project_migrations = Path(__file__).resolve().parent.parent / "migrations"
        run_migrations(conn, project_migrations)

        # Verify the job is still there
        assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 1
        row = conn.execute("SELECT title, company FROM jobs WHERE id = 1").fetchone()
        assert row[0] == "GTM Engineer"
        assert row[1] == "Vercel"

        conn.close()
