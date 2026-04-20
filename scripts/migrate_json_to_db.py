#!/usr/bin/env python3
"""Migrate seed_companies.json and dream_companies.json into DuckDB tables.

Handles:
- Deduplication by canonical_name (lowercase, trimmed)
- ATS info merge for companies appearing in both files
- Attraction taxonomy → company_attractions rows
- JSON snapshot freezing

Usage:
    PYTHONPATH=src python scripts/migrate_json_to_db.py
"""

import json
import re
import shutil
from datetime import datetime
from pathlib import Path

from opportunities_engine.storage.db import JobStore
from opportunities_engine.config import settings

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SNAPSHOT_DATE = "2026-04-19"


def canonicalize(name: str) -> str:
    """Normalize company name for dedup: lowercase, strip, collapse whitespace."""
    return re.sub(r"\s+", " ", name.strip().lower())


def migrate_companies(conn) -> dict:
    """Migrate both JSON files into companies table. Returns stats."""
    seed_path = DATA_DIR / "seed_companies.json"
    dream_path = DATA_DIR / "dream_companies.json"

    seed_data = json.loads(seed_path.read_text()) if seed_path.exists() else {"companies": []}
    dream_data = json.loads(dream_path.read_text()) if dream_path.exists() else {"companies": []}

    # Build merged company dict keyed by canonical_name
    merged: dict[str, dict] = {}

    # Process seed_companies first
    for co in seed_data.get("companies", []):
        canon = canonicalize(co["name"])
        if canon not in merged:
            merged[canon] = {
                "name": co["name"],  # preserve original casing from first encounter
                "canonical_name": canon,
                "website": co.get("website"),
                "industry": co.get("industry"),
                "ats_info": [],
                "is_dream": co.get("is_dream_company", False),
                "notes": co.get("notes"),
                "source": "yc_seed",
                "why_i_love_this": None,
                "attraction_types": [],
                "priority": None,
                "status": None,
                "discovery_path": None,
                "active_role": None,
                "active_role_url": None,
                "added": None,
            }
        # Always merge ATS info (a company can be in seed_companies under multiple ATS)
        ats_entry = {
            "platform": co.get("ats_platform"),
            "slug": co.get("ats_slug"),
            "verified": co.get("ats_slug_verified", False),
            "job_count": co.get("job_count", 0),
        }
        # Avoid duplicate ATS entries
        existing_platforms = {a["platform"] for a in merged[canon]["ats_info"]}
        if ats_entry["platform"] not in existing_platforms:
            merged[canon]["ats_info"].append(ats_entry)

        if co.get("is_dream_company"):
            merged[canon]["is_dream"] = True

    # Process dream_companies — merge or create
    for co in dream_data.get("companies", []):
        canon = canonicalize(co["name"])
        if canon not in merged:
            merged[canon] = {
                "name": co["name"],
                "canonical_name": canon,
                "website": co.get("url"),
                "industry": None,
                "ats_info": [],
                "is_dream": True,
                "notes": co.get("notes"),
                "source": "dream_list",
                "why_i_love_this": co.get("why_i_love_this"),
                "attraction_types": co.get("attraction_types", []),
                "priority": co.get("priority"),
                "status": co.get("status"),
                "discovery_path": co.get("discovery_path"),
                "active_role": co.get("active_role"),
                "active_role_url": co.get("active_role_url"),
                "added": co.get("added"),
            }
        else:
            # Merge dream data into existing seed entry
            merged[canon]["is_dream"] = True
            if co.get("url") and not merged[canon]["website"]:
                merged[canon]["website"] = co["url"]
            merged[canon]["why_i_love_this"] = co.get("why_i_love_this")
            merged[canon]["attraction_types"] = co.get("attraction_types", [])
            merged[canon]["priority"] = co.get("priority")
            merged[canon]["status"] = co.get("status")
            merged[canon]["discovery_path"] = co.get("discovery_path")
            merged[canon]["active_role"] = co.get("active_role")
            merged[canon]["active_role_url"] = co.get("active_role_url")
            merged[canon]["added"] = co.get("added")
            if co.get("notes") and not merged[canon]["notes"]:
                merged[canon]["notes"] = co["notes"]
            merged[canon]["source"] = "both"

    # Insert into companies table
    stats = {"companies_inserted": 0, "companies_skipped": 0, "attractions_inserted": 0}

    for canon, co in merged.items():
        # Check if already exists
        existing = conn.execute(
            "SELECT id FROM companies WHERE canonical_name = ?", [canon]
        ).fetchone()

        if existing:
            company_id = existing[0]
            stats["companies_skipped"] += 1
        else:
            conn.execute("""
                INSERT INTO companies (
                    canonical_name, name, website, industry, hq_location,
                    company_size, funding_stage, ats_platforms_json,
                    is_dream, why_i_love, priority, status,
                    discovery_path, active_role, active_role_url,
                    notes, source, added_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                co["canonical_name"],
                co["name"],
                co["website"],
                co["industry"],
                None,  # hq_location
                None,  # company_size
                None,  # funding_stage
                json.dumps(co["ats_info"]) if co["ats_info"] else None,
                co["is_dream"],
                co["why_i_love_this"],
                co.get("priority"),
                co.get("status"),
                co.get("discovery_path"),
                co.get("active_role"),
                co.get("active_role_url"),
                co.get("notes"),
                co.get("source"),
                co.get("added"),
            ])
            company_id = conn.execute(
                "SELECT id FROM companies WHERE canonical_name = ?", [canon]
            ).fetchone()[0]
            stats["companies_inserted"] += 1

        # Insert attraction types
        for attr in co.get("attraction_types", []):
            existing_attr = conn.execute(
                "SELECT id FROM company_attractions WHERE company_id = ? AND attribute = ?",
                [company_id, attr]
            ).fetchone()
            if not existing_attr:
                conn.execute("""
                    INSERT INTO company_attractions (company_id, attribute, weight, source)
                    VALUES (?, ?, 1.0, 'dream_list')
                """, [company_id, attr])
                stats["attractions_inserted"] += 1

    return stats


def freeze_jsons() -> list[str]:
    """Freeze JSON files as snapshots. Returns list of frozen filenames."""
    frozen = []
    for filename in ["seed_companies.json", "dream_companies.json"]:
        src = DATA_DIR / filename
        if src.exists():
            snapshot_name = filename.replace(".json", f"_snapshot_{SNAPSHOT_DATE}.json")
            dst = DATA_DIR / snapshot_name
            if not dst.exists():
                shutil.copy2(src, dst)
                frozen.append(snapshot_name)
    return frozen


def main():
    import duckdb
    conn = duckdb.connect(str(settings.database_path))

    print("Running SQL migration 002...")
    from opportunities_engine.storage.migrate import run_migrations
    applied = run_migrations(conn)
    print(f"  SQL migrations applied: {applied}")

    print("Migrating JSON reference data...")
    stats = migrate_companies(conn)
    print(f"  Companies inserted: {stats['companies_inserted']}")
    print(f"  Companies skipped (already exist): {stats['companies_skipped']}")
    print(f"  Attractions inserted: {stats['attractions_inserted']}")

    print("Freezing JSON files...")
    frozen = freeze_jsons()
    for f in frozen:
        print(f"  Frozen: {f}")

    conn.close()
    print("✅ Phase B migration complete.")


if __name__ == "__main__":
    main()
