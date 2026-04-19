"""DuckDB storage layer for the opportunities-engine."""

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb


_SCHEMA_SQL = """
CREATE SEQUENCE IF NOT EXISTS job_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS company_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS app_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS skill_id_seq START 1;

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY DEFAULT nextval('job_id_seq'),
    source TEXT NOT NULL,
    source_id TEXT,
    url TEXT UNIQUE NOT NULL,
    url_hash TEXT NOT NULL,
    title TEXT NOT NULL,
    company TEXT NOT NULL,
    location TEXT,
    description TEXT,
    salary_min REAL,
    salary_max REAL,
    salary_currency TEXT DEFAULT 'USD',
    date_posted TIMESTAMP,
    date_first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_remote BOOLEAN,
    job_type TEXT,
    seniority TEXT,
    department TEXT,
    company_industry TEXT,
    company_size TEXT,
    metadata JSON,
    status TEXT DEFAULT 'new',
    linear_issue_id TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS companies (
    id INTEGER PRIMARY KEY DEFAULT nextval('company_id_seq'),
    name TEXT UNIQUE NOT NULL,
    website TEXT,
    ats_platform TEXT,
    ats_slug TEXT,
    industry TEXT,
    size TEXT,
    funding_stage TEXT,
    is_dream_company BOOLEAN DEFAULT FALSE,
    attraction_types TEXT,
    why_i_love_this TEXT,
    linear_project_id TEXT,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS applications (
    id INTEGER PRIMARY KEY DEFAULT nextval('app_id_seq'),
    job_id INTEGER REFERENCES jobs(id),
    applied_at TIMESTAMP,
    method TEXT,
    cover_letter_path TEXT,
    resume_version TEXT,
    follow_up_dates JSON,
    status TEXT DEFAULT 'applied',
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skill_gaps (
    id INTEGER PRIMARY KEY DEFAULT nextval('skill_id_seq'),
    skill TEXT NOT NULL,
    job_id INTEGER REFERENCES jobs(id),
    confidence REAL DEFAULT 0.5,
    status TEXT DEFAULT 'identified',
    resources JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


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
        self.conn.execute(_SCHEMA_SQL)

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
        """Insert or update a company (matched by name). Returns True if new."""
        assert self.conn is not None

        name = company["name"]
        existing = self.conn.execute(
            "SELECT id FROM companies WHERE name = $1", [name]
        ).fetchone()

        now = datetime.now(timezone.utc)

        if existing is not None:
            updates: dict[str, Any] = {"updated_at": now}
            for col in (
                "website", "ats_platform", "ats_slug", "industry", "size",
                "funding_stage", "is_dream_company", "attraction_types",
                "why_i_love_this", "linear_project_id",
            ):
                if col in company and company[col] is not None:
                    updates[col] = company[col]
            if "metadata" in company:
                updates["metadata"] = json.dumps(company["metadata"]) if isinstance(company["metadata"], dict) else company["metadata"]

            set_clause = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates.keys()))
            where_idx = len(updates) + 1
            values = list(updates.values()) + [name]
            self.conn.execute(
                f"UPDATE companies SET {set_clause} WHERE name = ${where_idx}",
                values,
            )
            return False

        metadata_json = json.dumps(company.get("metadata", {}))
        self.conn.execute(
            """
            INSERT INTO companies (
                name, website, ats_platform, ats_slug, industry, size,
                funding_stage, is_dream_company, attraction_types,
                why_i_love_this, linear_project_id, metadata,
                created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12,
                $13, $14
            )
            """,
            [
                name,
                company.get("website"),
                company.get("ats_platform"),
                company.get("ats_slug"),
                company.get("industry"),
                company.get("size"),
                company.get("funding_stage"),
                company.get("is_dream_company", False),
                company.get("attraction_types"),
                company.get("why_i_love_this"),
                company.get("linear_project_id"),
                metadata_json,
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
