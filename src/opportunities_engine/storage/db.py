"""DuckDB storage layer for the opportunities-engine."""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from opportunities_engine.storage.migrate import run_migrations


def _normalize_url(url: str) -> str:
    """Lowercase and strip trailing slashes / query fluff for hashing."""
    u = url.strip().lower()
    if u.endswith("/"):
        u = u[:-1]
    return u


def _url_hash(url: str) -> str:
    """MD5 of a normalized URL — used for cross-board dedup."""
    return hashlib.md5(_normalize_url(url).encode()).hexdigest()


def _row_to_dict(row: tuple, columns: list[str]) -> dict:
    """Convert a DuckDB row tuple to a dict, JSON-decoding metadata fields."""
    d = dict(zip(columns, row))
    for key in ("metadata", "follow_up_dates", "resources"):
        val = d.get(key)
        if isinstance(val, str):
            try:
                d[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass
    return d


class JobStore:
    """Context-manager wrapper around a DuckDB database."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self.conn: duckdb.DuckDBPyConnection | None = None  # type: ignore[name-defined]

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    def __enter__(self) -> "JobStore":
        self.conn = duckdb.connect(str(self.db_path))
        self._init_schema()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore[no-untyped-def]
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # ------------------------------------------------------------------
    # Schema bootstrap
    # ------------------------------------------------------------------
    def _init_schema(self) -> None:
        assert self.conn is not None
        # Migrations handle ALL table creation (001 includes base tables)
        run_migrations(self.conn)

    # ------------------------------------------------------------------
    # Job helpers
    # ------------------------------------------------------------------
    def upsert_job(self, job: dict) -> bool:
        """Insert a new job or update an existing one (matched by url).

        Returns ``True`` when a *new* row was inserted, ``False`` on update.
        """
        assert self.conn is not None

        url = job["url"]
        url_hash_val = _url_hash(url)
        now = datetime.now(timezone.utc)

        # Check if the job already exists
        existing = self.conn.execute(
            "SELECT id FROM jobs WHERE url = $1", [url]
        ).fetchone()

        if existing is not None:
            # Update existing row
            updates: dict[str, Any] = {
                "date_last_seen": now,
                "updated_at": now,
            }
            # Carry through any fields the caller provided (except id/url)
            for col in (
                "title", "company", "location", "description",
                "salary_min", "salary_max", "salary_currency",
                "date_posted", "is_remote", "job_type", "seniority",
                "department", "company_industry", "company_size",
                "source_id", "status", "notes",
            ):
                if col in job and job[col] is not None:
                    updates[col] = job[col]
            if "metadata" in job:
                updates["metadata"] = json.dumps(job["metadata"]) if isinstance(job["metadata"], dict) else job["metadata"]

            set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates.keys()))
            # WHERE param goes after all SET params
            where_idx = len(updates) + 1
            values = list(updates.values()) + [url]
            self.conn.execute(
                f"UPDATE jobs SET {set_clause} WHERE url = ${where_idx}",
                values,
            )
            return False

        # Insert new row
        metadata_json = json.dumps(job.get("metadata", {}))
        self.conn.execute(
            """
            INSERT INTO jobs (
                source, source_id, url, url_hash, title, company, location,
                description, salary_min, salary_max, salary_currency,
                date_posted, is_remote, job_type, seniority, department,
                company_industry, company_size, metadata, status, notes,
                date_first_seen, date_last_seen, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8, $9, $10, $11,
                $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21,
                $22, $23, $24, $25
            )
            """,
            [
                job.get("source"),
                job.get("source_id"),
                url,
                url_hash_val,
                job.get("title"),
                job.get("company"),
                job.get("location"),
                job.get("description"),
                job.get("salary_min"),
                job.get("salary_max"),
                job.get("salary_currency", "USD"),
                job.get("date_posted"),
                job.get("is_remote"),
                job.get("job_type"),
                job.get("seniority"),
                job.get("department"),
                job.get("company_industry"),
                job.get("company_size"),
                metadata_json,
                job.get("status", "new"),
                job.get("notes"),
                now,
                now,
                now,
                now,
            ],
        )
        return True

    def upsert_company(self, company: dict) -> bool:
        """Insert or update a company (matched by canonical_name). Returns True if new."""
        assert self.conn is not None

        name = company["name"]
        import re as _re
        canonical = _re.sub(r"\s+", " ", name.strip().lower())
        existing = self.conn.execute(
            "SELECT id FROM companies WHERE canonical_name = $1", [canonical]
        ).fetchone()

        now = datetime.now(timezone.utc)

        if existing is not None:
            updates: dict[str, Any] = {"updated_at": now}
            for col in (
                "website", "industry", "company_size", "funding_stage",
                "is_dream", "why_i_love", "priority", "status",
                "notes", "source", "ats_platforms_json",
            ):
                if col in company and company[col] is not None:
                    updates[col] = company[col]

            set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates.keys()))
            where_idx = len(updates) + 1
            values = list(updates.values()) + [canonical]
            self.conn.execute(
                f"UPDATE companies SET {set_clause} WHERE canonical_name = ${where_idx}",
                values,
            )
            return False

        ats_json = json.dumps(company.get("ats_platforms", [])) if company.get("ats_platforms") else None
        self.conn.execute(
            """
            INSERT INTO companies (
                canonical_name, name, website, industry, hq_location,
                company_size, funding_stage, ats_platforms_json,
                is_dream, why_i_love, priority, status,
                discovery_path, active_role, active_role_url,
                notes, source, added_at,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15,
                $16, $17, $18, $19, $20
            )
            """,
            [
                canonical,
                name,
                company.get("website"),
                company.get("industry"),
                company.get("hq_location"),
                company.get("company_size"),
                company.get("funding_stage"),
                ats_json,
                company.get("is_dream", False),
                company.get("why_i_love"),
                company.get("priority"),
                company.get("status"),
                company.get("discovery_path"),
                company.get("active_role"),
                company.get("active_role_url"),
                company.get("notes"),
                company.get("source"),
                company.get("added_at"),
                now,
                now,
            ],
        )
        return True

    def get_jobs(self, status: str | None = None, limit: int = 100) -> list[dict]:
        """Return jobs, optionally filtered by status."""
        assert self.conn is not None
        if status is not None:
            rows = self.conn.execute(
                "SELECT * FROM jobs WHERE status = $1 ORDER BY date_first_seen DESC LIMIT $2",
                [status, limit],
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM jobs ORDER BY date_first_seen DESC LIMIT $1",
                [limit],
            ).fetchall()

        columns = [desc[0] for desc in self.conn.description]
        return [_row_to_dict(r, columns) for r in rows]

    def mark_seen(self, url: str) -> None:
        """Update date_last_seen for an existing job."""
        assert self.conn is not None
        now = datetime.now(timezone.utc)
        self.conn.execute(
            "UPDATE jobs SET date_last_seen = $1, updated_at = $2 WHERE url = $3",
            [now, now, url],
        )

    def get_new_jobs(self, since_hours: int = 24) -> list[dict]:
        """Return jobs with status='new' first seen within the last *since_hours*."""
        assert self.conn is not None
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        rows = self.conn.execute(
            "SELECT * FROM jobs WHERE status = 'new' AND date_first_seen >= $1 ORDER BY date_first_seen DESC",
            [cutoff],
        ).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [_row_to_dict(r, columns) for r in rows]

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------
    def health_check(self) -> dict:
        """Check DB integrity: checkpoint WAL, verify schema version, report table counts.

        Returns a dict with:
            status: 'ok' or 'warn'
            checkpoint: result of WAL checkpoint
            schema_version_count: number of applied migrations
            tables: {table_name: row_count} for key tables
        """
        assert self.conn is not None

        # 1. Checkpoint WAL
        checkpoint_result = "skipped"
        if str(self.db_path) != ":memory:":
            try:
                self.conn.execute("CHECKPOINT")
                checkpoint_result = "ok"
            except Exception as e:
                checkpoint_result = f"error: {e}"

        # 2. Schema version count
        try:
            version_rows = self.conn.execute(
                "SELECT COUNT(*) FROM schema_migrations"
            ).fetchone()
            schema_version_count = version_rows[0] if version_rows else 0
        except Exception:
            schema_version_count = 0

        # 3. Table row counts
        tables = {}
        for table_name in ("jobs", "companies", "company_attractions", "job_sources", "events", "scores"):
            try:
                count_row = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                tables[table_name] = count_row[0] if count_row else 0
            except Exception:
                tables[table_name] = "missing"

        # 4. Determine status
        status = "ok"
        if checkpoint_result.startswith("error"):
            status = "warn"
        if schema_version_count < 2:
            status = "warn"

        return {
            "status": status,
            "checkpoint": checkpoint_result,
            "schema_version_count": schema_version_count,
            "tables": tables,
        }
