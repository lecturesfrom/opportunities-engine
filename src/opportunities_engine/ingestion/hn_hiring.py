"""Hacker News 'Who is Hiring?' ingestion source.

Pipeline:
  1. Discover the monthly hiring thread via Algolia's search_by_date API
  2. Fetch comments for that thread via Algolia's items endpoint
  3. Parse each comment into a structured job record (company, title, location, remote, URL)
  4. Filter to GTM-adjacent roles only (matching our curated title universe)
  5. Dedup by (company, normalized_title)

Why HN?
- The monthly "Ask HN: Who is Hiring?" thread is the single best free signal
  for founding / early-stage roles at YC and tech startups.
- Algolia's HN API is free, no auth required, rate-limited but generous.
- Many roles posted here never appear on job boards.

Adapted from the last30days-skill hackernews.py (MIT License, mvanhorn).
"""

from __future__ import annotations

import html
import logging
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from .ats import ATSClient  # noqa: F401 — shared pattern reference

logger = logging.getLogger(__name__)

# Algolia HN API endpoints (free, no auth)
ALGOLIA_SEARCH_BY_DATE = "https://hn.algolia.com/api/v1/search_by_date"
ALGOLIA_ITEM = "https://hn.algolia.com/api/v1/items"

# GTM-adjacent title keywords — roles we want to surface
_GTM_TITLE_PATTERNS = re.compile(
    r"\b("
    r"founding\s+(gtm|ae|sales|sdr|bdr|account\s+exec|growth)"
    r"|forward\s+deployed"
    r"|solutions\s+engineer"
    r"|sales\s+engineer"
    r"|customer\s+engineer"
    r"|gtm\s+engineer"
    r"|growth\s+engineer"
    r"|revops\s+engineer"
    r"|commercial\s+engineer"
    r"|enterprise\s+(ae|account\s+exec)"
    r"|account\s+executive"
    r"|business\s+development"
    r"|partner\s+engineer"
    r"|developer\s+advocate"
    r"|developer\s+relations"
    r"|community\s+engineer"
    r")\b",
    re.IGNORECASE,
)

# Remote indicator patterns
_REMOTE_PATTERNS = re.compile(
    r"\b(remote|distributed|work\s+from\s+anywhere|wfh|work\s+from\s+home)\b",
    re.IGNORECASE,
)

# US location patterns (subset of config — used for initial gate)
_US_PATTERNS = re.compile(
    r"\b(us|usa|united\s+states|nyc|sf|san\s+francisco|bay\s+area"
    r"|la|los\s+angeles|chicago|boston|seattle|austin|denver|atlanta)\b",
    re.IGNORECASE,
)

# Non-US patterns to reject
_NON_US_PATTERNS = re.compile(
    r"\b(uk|london|berlin|germany|eu|europe|singapore|tokyo|india"
    r"|bangalore|mumbai|canada|toronto|vancouver|sydney|melbourne)\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Thread discovery
# ---------------------------------------------------------------------------

def find_hiring_thread(
    year: int,
    month: int,
    http: httpx.Client | None = None,
) -> dict | None:
    """Find the 'Ask HN: Who is Hiring?' thread for a given year/month.

    Uses Algolia's search_by_date endpoint. Returns the story dict with
    at least objectID, title, num_comments; or None if not found.
    """
    client = http or httpx.Client(timeout=15, follow_redirects=True)
    month_name = datetime(year, month, 1).strftime("%B")

    # Search for the hiring thread by title
    query = f'Ask HN: Who is Hiring? ({month_name} {year})'
    try:
        resp = client.get(
            ALGOLIA_SEARCH_BY_DATE,
            params={
                "query": query,
                "tags": "story",
                "numericFilters": f"created_at_i>{int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp())}",
            },
        )
    except httpx.RequestError as exc:
        logger.warning("HN Algolia network error: %s", exc)
        return None

    if resp.status_code != 200:
        logger.warning("HN Algolia returned %s", resp.status_code)
        return None

    try:
        data = resp.json()
    except Exception:
        return None

    hits = data.get("hits", [])
    for hit in hits:
        title = hit.get("title", "")
        if "who is hiring" in title.lower() and str(year) in title:
            return hit

    return None


# ---------------------------------------------------------------------------
# Comment parsing
# ---------------------------------------------------------------------------

def _strip_html(text: str) -> str:
    """Strip HTML tags and decode entities from HN comment text."""
    text = html.unescape(text)
    text = re.sub(r"<p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def _extract_links(text: str) -> list[str]:
    """Extract href URLs from HTML anchor tags."""
    return re.findall(r'<a\s+href="([^"]+)"', text)


def _extract_job_from_comment(comment: dict) -> dict | None:
    """Parse a single HN hiring comment into a job record dict.

    Expected comment format (convention):
        Company | Location | Role description with URL

    Returns None if the comment doesn't look like a hiring post or
    doesn't match GTM-adjacent patterns.
    """
    raw_text = comment.get("comment_text", "") or comment.get("text", "")
    if not raw_text:
        return None

    clean = _strip_html(raw_text)
    links = _extract_links(raw_text)

    # First line typically contains: Company | Location | ...
    first_line = clean.split("\n")[0].strip()
    parts = [p.strip() for p in first_line.split("|")]

    if len(parts) < 2:
        return None

    company = parts[0].strip()
    if not company or len(company) > 80:
        return None

    # Location from second part (if present)
    location = parts[1].strip() if len(parts) > 1 else ""

    # Title: try to find a GTM-adjacent role in the full comment text
    title_match = _GTM_TITLE_PATTERNS.search(clean)
    if not title_match:
        return None

    # Extract the role title — get the sentence containing the match
    title = title_match.group(0).strip()
    # Try to expand to the full role title (e.g., "Founding GTM Engineer")
    for line in clean.split("\n"):
        if title_match.group(0).lower() in line.lower():
            # Grab a reasonable title-length chunk
            snippet = line.strip()[:120]
            if len(snippet) > len(title):
                title = snippet
            break

    # Remote detection
    is_remote = bool(_REMOTE_PATTERNS.search(clean))

    # US gate: accept if US-pattern found, remote (assumed US-eligible),
    # or no location specified. Reject obvious non-US.
    if _NON_US_PATTERNS.search(clean) and not _US_PATTERNS.search(clean):
        if not is_remote:
            return None

    # URL from links or empty
    url = links[0] if links else ""

    return {
        "company": company,
        "title": title[:120],
        "location": location[:100],
        "is_remote": is_remote,
        "url": url,
        "source": "hn_hiring",
        "raw_text": clean[:500],
    }


def parse_hiring_comments(comments: list[dict]) -> list[dict]:
    """Parse a list of HN comments into deduped job records.

    Filters to GTM-adjacent roles only. Deduplicates by (company, title).
    """
    seen: set[str] = set()
    jobs: list[dict] = []

    for comment in comments:
        job = _extract_job_from_comment(comment)
        if job is None:
            continue

        # Dedup key: lowercase company + first 30 chars of title
        key = f"{job['company'].lower()}|{job['title'][:30].lower()}"
        if key in seen:
            continue
        seen.add(key)
        jobs.append(job)

    return jobs


# ---------------------------------------------------------------------------
# Source class (matches our ingestion pattern)
# ---------------------------------------------------------------------------

class HNHiringSource:
    """Hacker News 'Who is Hiring?' ingestion source."""

    name = "hn_hiring"

    def __init__(self, months_back: int = 1):
        """months_back: how many months to look back for hiring threads (default: current + 1 prior)."""
        self.months_back = months_back

    def fetch(self, http: httpx.Client | None = None) -> list[dict]:
        """Fetch and parse hiring thread comments for recent months.

        Returns a list of job record dicts compatible with our DuckDB schema.
        """
        client = http or httpx.Client(timeout=15, follow_redirects=True)
        all_jobs: list[dict] = []

        now = datetime.now(timezone.utc)
        for offset in range(self.months_back + 1):
            # Walk backwards from current month
            target_month = now.month - offset
            target_year = now.year
            while target_month <= 0:
                target_month += 12
                target_year -= 1

            thread = find_hiring_thread(target_year, target_month, http=client)
            if thread is None:
                logger.info("No HN hiring thread found for %s-%02d", target_year, target_month)
                continue

            object_id = thread.get("objectID")
            if not object_id:
                continue

            # Fetch comments for the thread
            try:
                resp = client.get(f"{ALGOLIA_ITEM}/{object_id}")
            except httpx.RequestError:
                continue

            if resp.status_code != 200:
                continue

            try:
                item_data = resp.json()
            except Exception:
                continue

            # Algolia returns children as a list of comment objects
            children = item_data.get("children", [])
            comments = []
            for child in children:
                text = child.get("text", "")
                if text:
                    comments.append({
                        "comment_text": text,
                        "story_title": thread.get("title", ""),
                    })

            month_jobs = parse_hiring_comments(comments)
            logger.info(
                "HN hiring %s-%02d: %d GTM roles from %d comments",
                target_year, target_month, len(month_jobs), len(comments),
            )
            all_jobs.extend(month_jobs)

        # Final dedup across months
        seen: set[str] = set()
        deduped: list[dict] = []
        for job in all_jobs:
            key = f"{job['company'].lower()}|{job['title'][:30].lower()}"
            if key not in seen:
                seen.add(key)
                deduped.append(job)

        return deduped
