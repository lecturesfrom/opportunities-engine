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
