"""Wellfound (AngelList Talent) public job-board scraper.

Hits the unauthenticated search pages, parses JSON-LD first,
falls back to structured HTML extraction.

No official API is available; this scraper is defensive by design:
- Never raises — all errors are logged and skipped.
- Returns empty list if Wellfound fully blocks the request.
- Rate-limited to 1 request per `sleep_seconds` (default 1.5 s).
- One retry on 429 with 3 s backoff.
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

import httpx

from opportunities_engine.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Regex to extract the numeric job ID from a Wellfound job path.
# e.g. /jobs/1234567-senior-gtm-engineer → "1234567"
_JOB_ID_RE = re.compile(r"/jobs/(\d+)")

# Regex for the DOM fallback: match job links of the form /jobs/NNNNN-slug
_JOB_LINK_RE = re.compile(r'href="(/jobs/(\d+)-[^"]+)"')

# Remote indicators in location or title strings.
_REMOTE_RE = re.compile(r"\bremote\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# WellfoundSource
# ---------------------------------------------------------------------------


class WellfoundSource:
    """Public-job-board scraper for Wellfound (wellfound.com).

    Hits the unauthenticated search pages, parses JSON-LD first,
    falls back to structured HTML extraction.
    """

    BASE_URL = "https://wellfound.com"
    JOBS_SEARCH = "/jobs"

    def __init__(
        self,
        http: httpx.Client | None = None,
        sleep_seconds: float = 1.5,
    ) -> None:
        self._http = http or httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={
                "User-Agent": _BROWSER_UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,*/*;q=0.8"
                ),
            },
        )
        self.sleep_seconds = sleep_seconds

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self, target_titles: list[str] | None = None) -> list[dict]:
        """Iterate over target_titles, fetch results per term, dedupe by URL.

        Defaults to ``settings.target_titles`` when target_titles is None.
        Returns a list of normalized job dicts; never raises.
        """
        titles = target_titles if target_titles is not None else settings.target_titles
        seen_urls: set[str] = set()
        results: list[dict] = []

        for i, query in enumerate(titles):
            if i > 0:
                time.sleep(self.sleep_seconds)
            try:
                html = self._search(query)
            except Exception as exc:
                logger.warning("Wellfound: network error for query %r: %s", query, exc)
                continue

            jobs: list[dict] = []
            try:
                jobs = self._parse_jsonld(html)
                logger.info(
                    "Wellfound: JSON-LD parse for %r → %d jobs", query, len(jobs)
                )
            except Exception as exc:
                logger.warning(
                    "Wellfound: JSON-LD parse failed for %r: %s", query, exc
                )

            if not jobs:
                try:
                    jobs = self._parse_dom(html)
                    logger.info(
                        "Wellfound: DOM parse for %r → %d jobs", query, len(jobs)
                    )
                except Exception as exc:
                    logger.warning(
                        "Wellfound: DOM parse failed for %r: %s", query, exc
                    )

            for raw in jobs:
                try:
                    norm = self._normalize(raw)
                except Exception as exc:
                    logger.debug("Wellfound: normalize error: %s", exc)
                    continue
                url = norm.get("url", "")
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                results.append(norm)

        return results

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _search(self, query: str) -> str:
        """GET the Wellfound jobs search page for a single query. Returns raw HTML."""
        # Wellfound's search accepts a `q` query param on the /jobs page.
        url = f"{self.BASE_URL}{self.JOBS_SEARCH}"
        params = {"q": query}
        try:
            resp = self._http.get(url, params=params)
        except httpx.RequestError as exc:
            logger.warning("Wellfound: request error for %r: %s", query, exc)
            raise

        logger.info(
            "Wellfound: GET %s?q=%s → HTTP %d", url, query, resp.status_code
        )

        if resp.status_code == 429:
            logger.info("Wellfound: rate-limited — sleeping 3 s and retrying")
            time.sleep(3)
            try:
                resp = self._http.get(url, params=params)
                logger.info(
                    "Wellfound: retry → HTTP %d", resp.status_code
                )
            except httpx.RequestError as exc:
                logger.warning("Wellfound: retry request error: %s", exc)
                raise

        if resp.status_code != 200:
            logger.warning(
                "Wellfound: non-200 response %d for query %r", resp.status_code, query
            )
            # Return empty string — callers handle gracefully.
            return ""

        return resp.text

    def _parse_jsonld(self, html: str) -> list[dict]:
        """Find <script type='application/ld+json'> blocks with JobPosting @type entries.

        Returns a list of raw dicts (one per JobPosting) ready for _normalize.
        """
        if not html:
            return []

        # Extract all JSON-LD script blocks.
        blocks = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html,
            re.DOTALL | re.IGNORECASE,
        )

        jobs: list[dict] = []
        for block in blocks:
            try:
                data = json.loads(block.strip())
            except json.JSONDecodeError:
                continue

            # data may be a single object or a list of objects
            entries: list[Any] = data if isinstance(data, list) else [data]
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                if entry.get("@type") != "JobPosting":
                    continue
                jobs.append(entry)

        return jobs

    def _parse_dom(self, html: str) -> list[dict]:
        """Fallback: scrape job cards from HTML via regex.

        Looks for <a href="/jobs/NNNNN-{slug}"> patterns and extracts
        title/company/location from nearby text. Avoids a BeautifulSoup
        dependency by using regex on the surrounding ~500-char window.
        """
        if not html:
            return []

        jobs: list[dict] = []
        for m in _JOB_LINK_RE.finditer(html):
            path = m.group(1)        # /jobs/9876543-some-slug
            job_id = m.group(2)      # 9876543
            url = f"{self.BASE_URL}{path}"

            # Grab a small window of text around the match to find title/company.
            start = max(0, m.start() - 50)
            end = min(len(html), m.end() + 600)
            window = html[start:end]

            # Strip tags from the window for text extraction.
            clean = re.sub(r"<[^>]+>", " ", window)
            clean = re.sub(r"\s+", " ", clean).strip()

            # Attempt to pull the link text as a title.
            # The link text is between the opening <a ...> and </a>.
            link_text_m = re.search(
                r'href="' + re.escape(path) + r'"[^>]*>(.*?)</a>',
                window,
                re.DOTALL,
            )
            if link_text_m:
                title_raw = re.sub(r"<[^>]+>", "", link_text_m.group(1)).strip()
                title = re.sub(r"\s+", " ", title_raw).strip() or None
            else:
                title = None

            # Look for explicit company/location markers in the window.
            # Wellfound uses CSS-module class names like styles_company__xxx
            # but they change. We rely on nearby text heuristics.
            company: str | None = None
            location: str | None = None

            # Strategy: grab first few non-empty text tokens after the link.
            tokens = [t.strip() for t in clean.split(" ") if t.strip()]
            # Skip the job title tokens (which appear first), then grab next
            # meaningful chunks. This is approximate.
            if len(tokens) > 2:
                company_candidate = tokens[1] if len(tokens) > 1 else None
                location_candidate = tokens[2] if len(tokens) > 2 else None
                # Simple heuristic: short strings that don't look like a title
                if company_candidate and len(company_candidate) < 60:
                    company = company_candidate
                if location_candidate and len(location_candidate) < 60:
                    location = location_candidate

            jobs.append(
                {
                    "_source": "dom",
                    "url": url,
                    "source_id": job_id,
                    "title": title,
                    "company": company,
                    "location": location,
                }
            )

        return jobs

    def _normalize(self, raw: dict) -> dict:
        """Build the canonical job dict from a raw parsed entry.

        Handles both JSON-LD dicts (from schema.org JobPosting) and the
        minimal dicts produced by _parse_dom.
        """
        # --- URL ---
        if raw.get("_source") == "dom":
            url: str = raw.get("url", "")
            source_id: str = raw.get("source_id", "")
            title: str | None = raw.get("title") or None
            company: str | None = raw.get("company") or None
            location: str | None = raw.get("location") or None
            description: str | None = None
            is_remote = _infer_remote(location, title)
            job_type: str | None = None
        else:
            # JSON-LD JobPosting
            url = raw.get("url", "")
            source_id = _extract_job_id(url)
            title = raw.get("title") or None
            description = raw.get("description") or None

            # Company
            org = raw.get("hiringOrganization")
            if isinstance(org, dict):
                company = org.get("name") or None
            else:
                company = org or None

            # Location
            job_location = raw.get("jobLocation")
            location = _extract_location(job_location)

            # Remote: JSON-LD jobLocationType=TELECOMMUTE takes priority.
            jlt = raw.get("jobLocationType", "")
            if isinstance(jlt, str) and jlt.upper() == "TELECOMMUTE":
                is_remote = True
            else:
                is_remote = _infer_remote(location, title)

            # Employment type
            emp_type = raw.get("employmentType")
            job_type = emp_type.lower() if isinstance(emp_type, str) else None

        if not source_id and url:
            source_id = _extract_job_id(url)

        return {
            "source": "wellfound",
            "source_id": source_id,
            "url": url,
            "title": title or "Unknown",
            "company": company or "Unknown",
            "location": location,
            "description": description,
            "is_remote": is_remote,
            "job_type": job_type,
            "metadata": {
                "wellfound_job_id": source_id,
            },
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _extract_job_id(url: str) -> str:
    """Extract the numeric Wellfound job ID from a URL path.

    /jobs/1234567-senior-gtm-engineer → "1234567"
    Returns "" if no match.
    """
    m = _JOB_ID_RE.search(url)
    return m.group(1) if m else ""


def _infer_remote(location: str | None, title: str | None = None) -> bool | None:
    """Return True if location or title contains 'remote', else None."""
    if location and _REMOTE_RE.search(location):
        return True
    if title and _REMOTE_RE.search(title):
        return True
    return None


def _extract_location(job_location: Any) -> str | None:
    """Extract a human-readable location string from a schema.org Place or string."""
    if not job_location:
        return None
    if isinstance(job_location, str):
        return job_location or None
    if isinstance(job_location, dict):
        addr = job_location.get("address")
        if isinstance(addr, dict):
            parts = [
                addr.get("addressLocality"),
                addr.get("addressRegion"),
                addr.get("addressCountry"),
            ]
            loc = ", ".join(p for p in parts if p)
            return loc or None
        # Sometimes jobLocation is a Place with a "name".
        name = job_location.get("name")
        return name or None
    if isinstance(job_location, list) and job_location:
        # Use the first entry.
        return _extract_location(job_location[0])
    return None
