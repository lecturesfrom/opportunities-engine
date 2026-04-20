"""Idempotent migration runner for DuckDB.

Reads SQL files from the migrations/ directory, checks which versions
have already been applied (via the schema_migrations table), runs
unapplied migrations in order, and records each on success.

Usage:
    from opportunities_engine.storage.migrate import run_migrations
    run_migrations(conn, migrations_dir=Path("migrations"))
"""

import hashlib
from pathlib import Path

import duckdb


def _ensure_migrations_table(conn: duckdb.DuckDBPyConnection) -> None:
    """Create the schema_migrations table if it doesn't exist."""
    conn.execute("CREATE SEQUENCE IF NOT EXISTS migration_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id         INTEGER PRIMARY KEY DEFAULT nextval('migration_id_seq'),
            version    TEXT UNIQUE NOT NULL,
            name       TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum   TEXT
        )
    """)


def _applied_versions(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return the set of already-applied migration versions."""
    _ensure_migrations_table(conn)
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {row[0] for row in rows}


def _migration_files(migrations_dir: Path) -> list[tuple[str, str, Path]]:
    """Discover migration SQL files sorted by version.

    Returns list of (version, name, path) tuples.
    Filename format: {version}_{name}.sql  e.g. 001_initial_schema.sql
    """
    if not migrations_dir.exists():
        return []

    migrations = []
    for f in sorted(migrations_dir.glob("*.sql")):
        stem = f.stem  # e.g. "001_initial_schema"
        parts = stem.split("_", 1)
        version = parts[0]
        name = parts[1] if len(parts) > 1 else version
        migrations.append((version, name, f))

    return migrations


def _drop_fk_constraints(conn: duckdb.DuckDBPyConnection) -> None:
    """Recreate child tables without FK constraints (DuckDB can't DROP CONSTRAINT).

    For each table that has a FOREIGN KEY, we: rename to _old, create without FK,
    copy data, drop _old, recreate indexes.
    """
    # Define each table's DDL without FK references
    tables = {
        "job_sources": """
            CREATE TABLE job_sources (
                id INTEGER PRIMARY KEY DEFAULT nextval('job_source_id_seq'),
                job_id INTEGER NOT NULL,
                source_name VARCHAR NOT NULL,
                source_url VARCHAR,
                raw_payload JSON,
                first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source_trust VARCHAR DEFAULT 'trusted',
                UNIQUE(job_id, source_name)
            )
        """,
        "events": """
            CREATE TABLE events (
                id INTEGER PRIMARY KEY DEFAULT nextval('event_id_seq'),
                job_id INTEGER NOT NULL,
                event_type VARCHAR NOT NULL,
                occurred_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                actor VARCHAR DEFAULT 'system',
                detail JSON
            )
        """,
        "scores": """
            CREATE TABLE scores (
                id INTEGER PRIMARY KEY DEFAULT nextval('score_id_seq'),
                job_id INTEGER NOT NULL,
                score FLOAT NOT NULL,
                ranker_version VARCHAR NOT NULL,
                scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                scoring_detail JSON,
                decision VARCHAR
            )
        """,
        "skill_gaps": """
            CREATE TABLE skill_gaps (
                id INTEGER PRIMARY KEY DEFAULT nextval('skill_gap_id_seq'),
                job_id INTEGER NOT NULL,
                skill VARCHAR NOT NULL,
                priority VARCHAR DEFAULT 'medium',
                status VARCHAR DEFAULT 'identified',
                notes VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        "company_attractions": """
            CREATE TABLE company_attractions (
                id INTEGER PRIMARY KEY DEFAULT nextval('company_attraction_id_seq'),
                company_id INTEGER NOT NULL,
                attribute VARCHAR NOT NULL,
                weight FLOAT DEFAULT 1.0,
                source VARCHAR,
                UNIQUE(company_id, attribute)
            )
        """,
    }

    indexes = {
        "job_sources": ["CREATE INDEX IF NOT EXISTS idx_job_sources_job_id ON job_sources(job_id)"],
        "events": ["CREATE INDEX IF NOT EXISTS idx_events_job_id_occurred_at ON events(job_id, occurred_at)"],
        "scores": ["CREATE INDEX IF NOT EXISTS idx_scores_job_id_scored_at ON scores(job_id, scored_at)"],
        "skill_gaps": [],
        "company_attractions": [],
    }

    for table_name, ddl in tables.items():
        # Check if this table actually has a FK
        fk_rows = conn.execute(
            "SELECT count(*) FROM duckdb_constraints() "
            f"WHERE table_name = '{table_name}' AND constraint_type = 'FOREIGN KEY'"
        ).fetchone()
        if fk_rows[0] == 0:
            continue

        # Drop dependent indexes before renaming (DuckDB blocks RENAME
        # when indexes exist on the table)
        existing_indexes = conn.execute(
            f"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table_name}'"
        ).fetchall()
        for (idx_name,) in existing_indexes:
            conn.execute(f"DROP INDEX IF EXISTS {idx_name}")

        # Rename -> create -> copy -> drop -> recreate indexes
        conn.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old")
        conn.execute(ddl)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_old")
        conn.execute(f"DROP TABLE {table_name}_old")

        for idx_sql in indexes.get(table_name, []):
            conn.execute(idx_sql)


def _checksum(content: str) -> str:
    """SHA256 hex digest of migration content."""
    return hashlib.sha256(content.encode()).hexdigest()


def run_migrations(
    conn: duckdb.DuckDBPyConnection,
    migrations_dir: Path | str | None = None,
) -> list[str]:
    """Run all pending migrations against the given connection.

    Args:
        conn: Active DuckDB connection.
        migrations_dir: Directory containing migration .sql files.
            Defaults to <project_root>/migrations/.

    Returns:
        List of version strings that were applied in this run.
    """
    if migrations_dir is None:
        # Default: look for migrations/ relative to the package root
        migrations_dir = Path(__file__).resolve().parent.parent.parent.parent / "migrations"
    elif isinstance(migrations_dir, str):
        migrations_dir = Path(migrations_dir)

    applied = _applied_versions(conn)
    pending = []

    for version, name, path in _migration_files(migrations_dir):
        if version not in applied:
            pending.append((version, name, path))

    if not pending:
        return []

    newly_applied = []

    for version, name, path in pending:
        content = path.read_text()
        cs = _checksum(content)

        # Special handling for migration 004 (drop FK constraints):
        # On fresh DBs (created with updated 001 that has no REFERENCES),
        # the ALTER TABLE RENAME will fail because index dependencies.
        # Detect whether any FK constraints exist and skip if none.
        if version == "004":
            fk_count = conn.execute(
                "SELECT count(*) FROM duckdb_constraints() "
                "WHERE constraint_type = 'FOREIGN KEY'"
            ).fetchone()[0]
            if fk_count == 0:
                # No FKs to drop — just record the migration as applied
                conn.execute(
                    """
                    INSERT INTO schema_migrations (version, name, checksum)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum
                    """,
                    [version, name, cs],
                )
                newly_applied.append(version)
                continue
            # FKs exist — run the table-swap DDL inline
            _drop_fk_constraints(conn)
            conn.execute(
                """
                INSERT INTO schema_migrations (version, name, checksum)
                VALUES ($1, $2, $3)
                ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum
                """,
                [version, name, cs],
            )
            newly_applied.append(version)
            continue

        # Execute the migration
        conn.execute(content)

        # Record it — the migration SQL itself may also INSERT into
        # schema_migrations, so we use ON CONFLICT to update checksum
        conn.execute(
            """
            INSERT INTO schema_migrations (version, name, checksum)
            VALUES ($1, $2, $3)
            ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum
            """,
            [version, name, cs],
        )

        newly_applied.append(version)

    return newly_applied
