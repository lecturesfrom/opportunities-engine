"""JobSpy-based scraper for Indeed and Google Jobs.

LinkedIn is intentionally excluded — it's rate-limited and GTM roles
are better captured via ATS APIs. This is the fallback/catch-all.
"""
from __future__ import annotations
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
from jobspy import scrape_jobs

from opportunities_engine.config import settings


SEARCH_TERMS = [
    "GTM Engineer",
    "Go-To-Market Engineer", 
    "Founding Growth Engineer",
    "Growth Engineer",
    "Forward Deployed Engineer",
    "Solutions Engineer",
    "Sales Engineer",
    "AI Solutions Engineer",
    "Customer Engineer",
    "RevOps Engineer",
    "Head of Growth",
    "Founding Growth",
    "Founding Product Engineer",
]

SITES = ["indeed", "google"]  # NOT linkedin — too rate-limited for automated use


def _normalize_row(row: pd.Series) -> dict:
    """Convert a JobSpy DataFrame row to our normalized job dict."""
    url = str(row.get("job_url", "") or row.get("url", "") or "")
    url_hash = hashlib.md5(url.lower().strip().encode()).hexdigest()
    
    return {
        "source": "jobspy_" + str(row.get("site", "unknown")),
        "source_id": str(row.get("job_id", "")),
        "url": url,
        "url_hash": url_hash,
        "title": str(row.get("title", "")),
        "company": str(row.get("company", "")),
        "location": str(row.get("location", "")),
        "description": str(row.get("description", "")),
        "salary_min": row.get("min_amount"),
        "salary_max": row.get("max_amount"),
        "salary_currency": str(row.get("currency", "USD")),
        "date_posted": row.get("date_posted"),
        "is_remote": bool(row.get("is_remote", False)),
        "job_type": str(row.get("job_type", "")),
        "department": "",
        "company_industry": str(row.get("company_industry", "")),
        "company_size": str(row.get("company_num_employees", "")),
        "metadata": {
            "company_logo": str(row.get("company_logo", "")),
            "company_url": str(row.get("company_url", "")),
            "company_url_direct": str(row.get("company_url_direct", "")),
            "company_rating": str(row.get("company_rating", "")),
            "company_revenue": str(row.get("company_revenue", "")),
            "emails": str(row.get("emails", "")),
        },
    }


def scrape_all(
    search_terms: list[str] | None = None,
    sites: list[str] | None = None,
    results_per_term: int = 30,
    hours_old: int = 72,
    location: str = "remote",
    country: str = "usa",
) -> Iterator[dict]:
    """Scrape all search terms across all sites, yielding normalized job dicts.
    
    Yields one dict at a time so the caller can upsert immediately
    without buffering the entire result set.
    """
    terms = search_terms or SEARCH_TERMS
    site_list = sites or SITES
    
    for term in terms:
        try:
            df = scrape_jobs(
                site_name=site_list,
                search_term=term,
                results_wanted=results_per_term,
                hours_old=hours_old,
                location=location,
                country_indeed=country,
            )
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    yield _normalize_row(row)
        except Exception as e:
            # Log but don't crash — one term failing shouldn't stop the whole run
            print(f"[jobspy] Error scraping '{term}': {e}")
            continue


if __name__ == "__main__":
    # Quick smoke test
    count = 0
    for job in scrape_all(results_per_term=5, hours_old=168):
        count += 1
        if count <= 5:
            print(f"  {job['title']} @ {job['company']}")
    print(f"\nTotal jobs scraped: {count}")
