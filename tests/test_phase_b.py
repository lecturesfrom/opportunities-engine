"""Tests for Phase B: data migration + reference data.

Covers:
- Legacy job_sources backfill
- JSON → companies migration (dedup, merge, ATS info)
- Attraction taxonomy → company_attractions rows
- JSON snapshot freezing
- applications table dropped
- Idempotency of the full migration
"""

import json
from pathlib import Path

import duckdb
import pytest

from opportunities_engine.storage.migrate import run_migrations


@pytest.fixture
def db(tmp_path):
    """Create a fresh DuckDB with 001 schema applied only.

    002 is NOT applied so that legacy backfill tests can exercise it.
    """
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    from opportunities_engine.storage.migrate import _ensure_migrations_table, _checksum
    from pathlib import Path as _P

    _ensure_migrations_table(conn)
    # Apply only 001
    sql = (_P(__file__).resolve().parent.parent / "migrations" / "001_initial_schema.sql").read_text()
    conn.execute(sql)
    cs = _checksum(sql)
    conn.execute(
        "UPDATE schema_migrations SET checksum = ? WHERE version = '001'",
        [cs],
    )
    yield conn
    conn.close()


@pytest.fixture
def db_with_jobs(db):
    """Add some job rows to test legacy backfill."""
    db.execute("""
        INSERT INTO jobs (id, title, company, location, url, url_hash, source, created_at)
        VALUES
            (9001, 'Growth Engineer', 'Greptile', 'Remote', 'https://greptile.com/jobs/1', 'h1', 'greenhouse', CURRENT_TIMESTAMP),
            (9002, 'SWE', 'Stripe', 'SF', 'https://stripe.com/jobs/1', 'h2', 'lever', CURRENT_TIMESTAMP),
            (9003, 'PM', 'PostHog', 'Remote', 'https://posthog.com/jobs/1', 'h3', 'unknown', CURRENT_TIMESTAMP)
    """)
    return db


@pytest.fixture
def data_dir(tmp_path):
    """Create a temporary data dir with sample JSON files."""
    d = tmp_path / "data"
    d.mkdir()

    seed = {
        "_meta": {"description": "test", "generated": "2026-04-19", "total": 2, "verified_count": 1},
        "companies": [
            {
                "name": "PostHog",
                "website": "https://posthog.com",
                "ats_platform": "greenhouse",
                "ats_slug": "posthog",
                "ats_slug_verified": False,
                "job_count": 0,
                "industry": "developer tools",
                "is_dream_company": True,
                "notes": "Test note"
            },
            {
                "name": "Vercel",
                "website": "https://vercel.com",
                "ats_platform": "greenhouse",
                "ats_slug": "vercel",
                "ats_slug_verified": True,
                "job_count": 82,
                "industry": "developer tools",
                "is_dream_company": True,
                "notes": "Verified"
            },
            {
                # Duplicate: Stripe appears under both greenhouse and lever
                "name": "Stripe",
                "website": "https://stripe.com",
                "ats_platform": "greenhouse",
                "ats_slug": "stripe",
                "ats_slug_verified": True,
                "job_count": 498,
                "industry": "fintech",
                "is_dream_company": False,
                "notes": "GH verified"
            },
            {
                "name": "Stripe",
                "website": "https://stripe.com",
                "ats_platform": "lever",
                "ats_slug": "stripe",
                "ats_slug_verified": False,
                "job_count": 0,
                "industry": "fintech",
                "is_dream_company": False,
                "notes": "Lever unverified"
            },
        ]
    }
    (d / "seed_companies.json").write_text(json.dumps(seed))

    dream = {
        "_meta": {"description": "test dream", "created": "2026-04-19"},
        "companies": [
            {
                "name": "PostHog",
                "url": "https://posthog.com",
                "why_i_love_this": "Coolest JDs ever",
                "attraction_types": ["team_culture", "product_love", "innovation_tech"],
                "priority": "tier_1",
                "status": "researching",
                "added": "2026-04-18",
                "notes": "THE dream"
            },
            {
                "name": "Suno",
                "url": "https://suno.com",
                "why_i_love_this": "AI music generation",
                "attraction_types": ["innovation_tech", "product_love", "personal_connection"],
                "priority": "tier_1",
                "status": "researching",
                "added": "2026-04-18"
            },
        ]
    }
    (d / "dream_companies.json").write_text(json.dumps(dream))

    return d


# ──────────────────────────────────────────────
# 11. Legacy job_sources backfill
# ──────────────────────────────────────────────

class TestLegacyBackfill:
    def test_backfills_all_existing_jobs(self, db_with_jobs):
        from scripts.migrate_json_to_db import DATA_DIR  # just to import runner
        # Run migration 002 SQL
        run_migrations(db_with_jobs)

        count = db_with_jobs.execute(
            "SELECT count(*) FROM job_sources WHERE source_name = 'legacy'"
        ).fetchone()[0]
        assert count == 3  # 3 jobs inserted in fixture

    def test_legacy_source_trust_is_untrusted(self, db_with_jobs):
        run_migrations(db_with_jobs)

        trust = db_with_jobs.execute(
            "SELECT source_trust FROM job_sources WHERE source_name = 'legacy' LIMIT 1"
        ).fetchone()[0]
        assert trust == "untrusted"

    def test_idempotent_backfill(self, db_with_jobs):
        run_migrations(db_with_jobs)
        run_migrations(db_with_jobs)  # second run

        count = db_with_jobs.execute(
            "SELECT count(*) FROM job_sources WHERE source_name = 'legacy'"
        ).fetchone()[0]
        assert count == 3  # no duplicates


# ──────────────────────────────────────────────
# 12-13. JSON → companies migration
# ──────────────────────────────────────────────

class TestJsonMigration:
    def test_merges_ats_info_for_duplicates(self, db, data_dir, monkeypatch):
        # Point script at our temp data dir
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        stats = m.migrate_companies(db)

        # Stripe should only appear once despite being in seed_companies twice
        stripe_rows = db.execute(
            "SELECT count(*) FROM companies WHERE canonical_name = 'stripe'"
        ).fetchone()[0]
        assert stripe_rows == 1

        # But should have BOTH ATS entries in ats_platforms_json
        ats_json = db.execute(
            "SELECT ats_platforms_json FROM companies WHERE canonical_name = 'stripe'"
        ).fetchone()[0]
        ats_info = json.loads(ats_json)
        platforms = {a["platform"] for a in ats_info}
        assert platforms == {"greenhouse", "lever"}

    def test_dream_company_data_merged(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        # PostHog is in BOTH seed and dream — should be merged
        row = db.execute(
            "SELECT is_dream, why_i_love, priority, status FROM companies WHERE canonical_name = 'posthog'"
        ).fetchone()
        assert row[0] is True  # is_dream
        assert row[1] == "Coolest JDs ever"  # why_i_love from dream
        assert row[2] == "tier_1"  # priority from dream
        assert row[3] == "researching"  # status from dream

    def test_dream_only_company_created(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        # Suno only exists in dream_companies
        row = db.execute(
            "SELECT is_dream, source FROM companies WHERE canonical_name = 'suno'"
        ).fetchone()
        assert row[0] is True
        assert row[1] == "dream_list"

    def test_merged_source_is_both(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        source = db.execute(
            "SELECT source FROM companies WHERE canonical_name = 'posthog'"
        ).fetchone()[0]
        assert source == "both"

    def test_canonical_name_is_lowercase(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        canon = db.execute(
            "SELECT canonical_name FROM companies WHERE canonical_name LIKE '%posthog%'"
        ).fetchone()
        assert canon[0] == "posthog"


# ──────────────────────────────────────────────
# 14. Attraction taxonomy → company_attractions
# ──────────────────────────────────────────────

class TestAttractionsMigration:
    def test_attractions_created_per_attribute(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        # PostHog has 3 attraction types
        posthog_id = db.execute(
            "SELECT id FROM companies WHERE canonical_name = 'posthog'"
        ).fetchone()[0]
        attrs = db.execute(
            "SELECT attribute FROM company_attractions WHERE company_id = ?",
            [posthog_id]
        ).fetchall()
        attr_names = {a[0] for a in attrs}
        assert attr_names == {"team_culture", "product_love", "innovation_tech"}

    def test_no_duplicate_attractions(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)
        m.migrate_companies(db)  # second run

        # Count should not double
        count = db.execute("SELECT count(*) FROM company_attractions").fetchone()[0]
        # PostHog: 3, Suno: 3 = 6 total
        assert count == 6

    def test_attraction_weight_defaults_to_1(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        weight = db.execute(
            "SELECT weight FROM company_attractions LIMIT 1"
        ).fetchone()[0]
        assert weight == 1.0

    def test_attraction_source_is_dream_list(self, db, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        m.migrate_companies(db)

        source = db.execute(
            "SELECT source FROM company_attractions LIMIT 1"
        ).fetchone()[0]
        assert source == "dream_list"


# ──────────────────────────────────────────────
# 15. Applications table dropped
# ──────────────────────────────────────────────

class TestApplicationsDropped:
    def test_applications_table_gone(self, db_with_jobs):
        run_migrations(db_with_jobs)

        tables = db_with_jobs.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "applications" not in table_names


# ──────────────────────────────────────────────
# Idempotency: full Phase B run twice
# ──────────────────────────────────────────────

class TestPhaseBIdempotency:
    def test_full_migration_idempotent(self, db_with_jobs, data_dir, monkeypatch):
        import scripts.migrate_json_to_db as m
        monkeypatch.setattr(m, "DATA_DIR", data_dir)

        # First run
        run_migrations(db_with_jobs)
        m.migrate_companies(db_with_jobs)

        job_sources_1 = db_with_jobs.execute("SELECT count(*) FROM job_sources").fetchone()[0]
        companies_1 = db_with_jobs.execute("SELECT count(*) FROM companies").fetchone()[0]
        attractions_1 = db_with_jobs.execute("SELECT count(*) FROM company_attractions").fetchone()[0]

        # Second run
        run_migrations(db_with_jobs)
        m.migrate_companies(db_with_jobs)

        job_sources_2 = db_with_jobs.execute("SELECT count(*) FROM job_sources").fetchone()[0]
        companies_2 = db_with_jobs.execute("SELECT count(*) FROM companies").fetchone()[0]
        attractions_2 = db_with_jobs.execute("SELECT count(*) FROM company_attractions").fetchone()[0]

        assert job_sources_1 == job_sources_2
        assert companies_1 == companies_2
        assert attractions_1 == attractions_2
