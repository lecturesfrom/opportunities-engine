"""JobSpy-based scraper for non-ATS job boards.

Default mode uses Indeed + Google only.
LinkedIn is available in *manual capped* mode via `linkedin_lite=True`
for targeted easy-win sweeps.
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

SITES = ["indeed", "google"]  # default
LINKEDIN_LITE_TERMS = [
    "GTM Engineer",
    "Go-To-Market Engineer",
    "Forward Deployed Engineer",
    "Solutions Engineer",
    "Sales Engineer",
    "RevOps Engineer",
]


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
    linkedin_lite: bool = False,
    linkedin_terms_cap: int = 3,
    linkedin_results_cap: int = 8,
) -> Iterator[dict]:
    """Scrape search terms across selected sites, yielding normalized job dicts.

    Default: Indeed + Google only.
    LinkedIn-lite mode (manual): include LinkedIn with strict caps.
    """
    terms = search_terms or SEARCH_TERMS
    site_list = list(sites or SITES)

    if linkedin_lite:
        # manual capped mode for quick-win sweeps
        if "linkedin" not in site_list:
            site_list.append("linkedin")
        if search_terms is None:
            terms = LINKEDIN_LITE_TERMS[:linkedin_terms_cap]
        # keep linkedin runs small to reduce rate-limit fragility
        results_per_term = min(results_per_term, linkedin_results_cap)

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
