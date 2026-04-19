"""ATS API client — fetches public job boards from Greenhouse, Lever, and Ashby."""

from __future__ import annotations

import logging
import re
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_remote(location: str | None) -> bool | None:
    """Heuristic: if the location string contains 'remote' (case-insensitive)."""
    if location is None:
        return None
    return bool(re.search(r"\bremote\b", location, re.IGNORECASE))


def _infer_job_type(commitment: str | None) -> str | None:
    """Map common ATS commitment labels to our enum."""
    if not commitment:
        return None
    c = commitment.lower().strip()
    if "part" in c:
        return "part_time"
    if "contract" in c or "intern" in c:
        return "contract"
    return "full_time"


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    return text.strip() or None


# ---------------------------------------------------------------------------
# Greenhouse
# ---------------------------------------------------------------------------

_GREENHOUSE_URL = "https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"


def _normalize_greenhouse(slug: str, raw: dict) -> dict:
    """Turn a Greenhouse job payload into our common schema."""
    location_parts = [
        raw.get("location", {}).get("name") if isinstance(raw.get("location"), dict) else raw.get("location"),
    ]
    location = ", ".join(p for p in location_parts if p) or None

    # Build apply URL — prefer the first direct apply link
    url: str | None = None
    meta = raw.get("metadata", [])
    if isinstance(meta, list):
        for m in meta:
            if isinstance(m, dict) and m.get("name", "").lower() == "apply url":
                url = m.get("value")
                break
    if not url:
        url = raw.get("absolute_url")
    if not url:
        url = f"https://boards.greenhouse.io/{slug}/jobs/{raw.get('id', '')}"

    title = raw.get("title", "Unknown")
    return {
        "source": "greenhouse",
        "source_id": str(raw.get("id", "")),
        "url": url,
        "title": title,
        "company": raw.get("company_name") or slug,
        "location": location,
        "description": _clean(raw.get("content")) or _clean(raw.get("title")),
        "is_remote": _infer_remote(location),
        "job_type": _infer_job_type(None),  # greenhouse list endpoint doesn't include commitment
        "department": raw.get("departments", [{}])[0].get("name") if raw.get("departments") else None,
        "metadata": {
            "greenhouse_internal_id": raw.get("internal_job_id"),
            "greenhouse_updated_at": raw.get("updated_at"),
        },
    }


# ---------------------------------------------------------------------------
# Lever
# ---------------------------------------------------------------------------

_LEVER_URL = "https://api.lever.co/v0/postings/{slug}"


def _normalize_lever(slug: str, raw: dict) -> dict:
    """Turn a Lever posting into our common schema."""
    # Lever provides a flat location string in various shapes
    location = raw.get("categories", {}).get("location") or raw.get("categories", {}).get("team")
    if isinstance(location, list):
        location = ", ".join(location) if location else None

    url = raw.get("hostedUrl") or raw.get("applyUrl") or f"https://jobs.lever.co/{slug}/{raw.get('id', '')}"

    commitment = raw.get("categories", {}).get("commitment")
    team = raw.get("categories", {}).get("team")
    description_parts = []
    for desc in raw.get("description", []):
        if isinstance(desc, dict):
            description_parts.append(desc.get("content", ""))
        elif isinstance(desc, str):
            description_parts.append(desc)
    # Lever also has descriptionBulletList etc but text is usually enough
    full_desc = "\n".join(description_parts) or _clean(raw.get("descriptionPlain"))

    return {
        "source": "lever",
        "source_id": raw.get("id", ""),
        "url": url,
        "title": raw.get("text", "Unknown"),
        "company": raw.get("categories", {}).get("company") or slug,
        "location": _clean(location),
        "description": _clean(full_desc),
        "is_remote": _infer_remote(location),
        "job_type": _infer_job_type(commitment),
        "department": _clean(team) if isinstance(team, str) else None,
        "metadata": {
            "lever_id": raw.get("id"),
            "lever_created_at": raw.get("createdAt"),
        },
    }


# ---------------------------------------------------------------------------
# Ashby
# ---------------------------------------------------------------------------

_ASHBY_URL = "https://api.ashbyhq.com/posting-api/job-board/{slug}"


def _normalize_ashby(slug: str, raw: dict) -> dict:
    """Turn an Ashby job-board entry into our common schema."""
    location_parts = []
    if raw.get("location"):
        location_parts.append(raw["location"])
    if raw.get("locationType"):
        location_parts.append(raw["locationType"])
    location = ", ".join(location_parts) or None

    url = raw.get("url") or raw.get("externalUrl") or ""

    salary_min = raw.get("compensation", {}).get("min") if isinstance(raw.get("compensation"), dict) else None
    salary_max = raw.get("compensation", {}).get("max") if isinstance(raw.get("compensation"), dict) else None

    return {
        "source": "ashby",
        "source_id": str(raw.get("id", "")),
        "url": url,
        "title": raw.get("title", "Unknown"),
        "company": raw.get("companyName") or slug,
        "location": _clean(location),
        "description": _clean(raw.get("description")) or _clean(raw.get("descriptionPlain")),
        "salary_min": salary_min,
        "salary_max": salary_max,
        "is_remote": _infer_remote(location),
        "job_type": _infer_job_type(raw.get("employmentType")),
        "department": _clean(raw.get("department")),
        "metadata": {
            "ashby_id": raw.get("id"),
            "ashby_is_remote": raw.get("isRemote"),
        },
    }


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class ATSClient:
    """Unified client for public ATS job-board APIs."""

    def __init__(self, http_client: httpx.Client | None = None):
        self.http = http_client or httpx.Client(timeout=30, follow_redirects=True)

    # -- low-level helpers --------------------------------------------------

    def _get(self, url: str) -> httpx.Response | None:
        """GET with graceful error handling.

        * 404 → return None (company not on this ATS)
        * 429 → sleep 2 s, retry once, then return None
        * Other errors → log warning, return None
        """
        try:
            resp = self.http.get(url)
        except httpx.RequestError as exc:
            logger.warning("Request error fetching %s: %s", url, exc)
            return None

        if resp.status_code == 200:
            return resp

        if resp.status_code == 404:
            logger.debug("404 for %s — company not on this ATS", url)
            return None

        if resp.status_code == 429:
            logger.info("Rate-limited on %s — sleeping 2 s and retrying once", url)
            time.sleep(2)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp
            except httpx.RequestError as exc:
                logger.warning("Retry request error fetching %s: %s", url, exc)
                return None
            logger.warning("Still non-200 after retry on %s: %s", url, resp.status_code)
            return None

        logger.warning("Unexpected %s from %s", resp.status_code, url)
        return None

    # -- per-platform fetch --------------------------------------------------

    def fetch_greenhouse(self, company_slug: str) -> list[dict]:
        """GET Greenhouse job board API, return normalised job dicts."""
        url = _GREENHOUSE_URL.format(slug=company_slug)
        resp = self._get(url)
        if resp is None:
            return []
        try:
            data = resp.json()
        except Exception:
            logger.warning("Could not decode Greenhouse JSON from %s", url)
            return []

        jobs = data.get("jobs", data) if isinstance(data, dict) else data
        if not isinstance(jobs, list):
            return []

        # Greenhouse list endpoint gives minimal data; optionally we could
        # fetch each job's detail but that's N+1 — keep it light for now.
        results: list[dict] = []
        for raw in jobs:
            try:
                results.append(_normalize_greenhouse(company_slug, raw))
            except Exception as exc:
                logger.debug("Skipping malformed Greenhouse job: %s", exc)
        return results

    def fetch_lever(self, company_slug: str) -> list[dict]:
        """GET Lever postings API, return normalised job dicts."""
        url = _LEVER_URL.format(slug=company_slug)
        resp = self._get(url)
        if resp is None:
            return []
        try:
            data = resp.json()
        except Exception:
            logger.warning("Could not decode Lever JSON from %s", url)
            return []

        if not isinstance(data, list):
            return []

        results: list[dict] = []
        for raw in data:
            try:
                results.append(_normalize_lever(company_slug, raw))
            except Exception as exc:
                logger.debug("Skipping malformed Lever posting: %s", exc)
        return results

    def fetch_ashby(self, company_slug: str) -> list[dict]:
        """GET Ashby job-board API, return normalised job dicts."""
        url = _ASHBY_URL.format(slug=company_slug)
        resp = self._get(url)
        if resp is None:
            return []
        try:
            data = resp.json()
        except Exception:
            logger.warning("Could not decode Ashby JSON from %s", url)
            return []

        jobs = data.get("jobs", data) if isinstance(data, dict) else data
        if not isinstance(jobs, list):
            return []

        results: list[dict] = []
        for raw in jobs:
            try:
                results.append(_normalize_ashby(company_slug, raw))
            except Exception as exc:
                logger.debug("Skipping malformed Ashby job: %s", exc)
        return results

    # -- router --------------------------------------------------------------

    _PLATFORM_MAP: dict[str, str] = {
        "greenhouse": "fetch_greenhouse",
        "lever": "fetch_lever",
        "ashby": "fetch_ashby",
    }

    def fetch_company(self, company: dict) -> list[dict]:
        """Route to the correct fetch method based on ``company['ats_platform']``."""
        platform = company.get("ats_platform", "").lower()
        method_name = self._PLATFORM_MAP.get(platform)
        if method_name is None:
            logger.warning("Unknown or missing ats_platform for %s: %s", company.get("name"), platform)
            return []
        slug = company.get("ats_slug") or company.get("name", "").lower().replace(" ", "")
        method = getattr(self, method_name)
        return method(slug)
