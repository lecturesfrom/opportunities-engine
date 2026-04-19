"""YC-batch + website → ATS slug seed discovery.

Pipeline:
  1. Hit YC's public ``api.ycombinator.com/v0.1/companies?batch=W25`` endpoint
     for each target batch; collect ``(name, slug, website)`` triples.
  2. For each company, generate plausible ATS slug candidates from the name
     and the website domain stem.
  3. Probe Greenhouse / Lever / Ashby via the existing :class:`ATSClient`
     (which already does 404 / 429 graceful handling). First 200-with-jobs
     wins; the company is recorded as ATS-verified.
  4. Merge verified hits into ``data/seed_companies.json`` — case-insensitive
     dedup by company ``name``, idempotent across runs.

Design notes
------------
* We do *not* use HEAD requests — the three ATS APIs aren't all HEAD-friendly
  and we need the JSON payload to count jobs anyway. A cold GET on each
  platform is the minimum cost.
* We skip 0-job boards because those are usually stale / dead slug matches
  rather than real signal.
"""

from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import httpx

from .ats import ATSClient

logger = logging.getLogger(__name__)

_YC_API = "https://api.ycombinator.com/v0.1/companies"

# Batches we want to cover (YC W23 through S25).
DEFAULT_BATCHES = ("W23", "S23", "W24", "S24", "W25", "S25")


# ---------------------------------------------------------------------------
# Slug-candidate generation
# ---------------------------------------------------------------------------

_SLUG_STRIP = re.compile(r"[^a-z0-9]+")
_SLUG_HYPHEN = re.compile(r"[^a-z0-9-]+")


def _domain_stem(website: str) -> str | None:
    """Return the second-level domain label of *website*, lowercased.

    ``https://www.foo-bar.io`` → ``foo-bar``
    ``https://app.acme.com``   → ``acme``
    """
    if not website:
        return None
    try:
        parsed = urlparse(website if "://" in website else f"https://{website}")
    except Exception:
        return None
    host = (parsed.hostname or "").lower().strip()
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]
    parts = host.split(".")
    if len(parts) >= 2:
        return parts[-2]  # second-level domain
    return parts[0] or None


def candidate_slugs(name: str, website: str) -> list[str]:
    """Return a deduped, ordered list of plausible ATS slug candidates.

    Strategy:
      * Collapsed-alphanumeric form of name     (``Scale AI`` → ``scaleai``)
      * Hyphenated form of name                 (``Scale AI`` → ``scale-ai``)
      * First-word-only form                    (``Scale AI`` → ``scale``)
      * Domain stem                             (``scale.com`` → ``scale``)
    """
    cands: list[str] = []

    name_lc = (name or "").lower().strip()
    if name_lc:
        collapsed = _SLUG_STRIP.sub("", name_lc)
        if collapsed:
            cands.append(collapsed)

        # hyphenate — turn runs of non-alnum into a single '-'
        hyphenated = _SLUG_HYPHEN.sub("-", name_lc).strip("-")
        hyphenated = re.sub(r"-+", "-", hyphenated)
        if hyphenated and hyphenated not in cands:
            cands.append(hyphenated)

        first_word = re.split(r"\s+", name_lc)[0]
        first_word = _SLUG_STRIP.sub("", first_word)
        if first_word and first_word not in cands:
            cands.append(first_word)

    stem = _domain_stem(website)
    if stem and stem not in cands:
        cands.append(stem)

    # Final cleanup: drop empties, dedup preserving order
    seen: set[str] = set()
    out: list[str] = []
    for c in cands:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ---------------------------------------------------------------------------
# YC batch fetch
# ---------------------------------------------------------------------------

def fetch_yc_batch(batch: str, http: httpx.Client | None = None) -> list[dict]:
    """Fetch all companies in a given YC *batch* (e.g. ``"W25"``).

    Returns a list of dicts with at least ``name``, ``slug``, ``website``,
    ``batch``. Handles pagination automatically. 404/network errors return
    an empty list.
    """
    client = http or httpx.Client(timeout=20, follow_redirects=True)
    out: list[dict] = []
    page = 1
    while True:
        try:
            resp = client.get(_YC_API, params={"batch": batch, "page": page})
        except httpx.RequestError as exc:
            logger.warning("YC API network error for %s page %s: %s", batch, page, exc)
            return out

        if resp.status_code != 200:
            if resp.status_code != 404:
                logger.warning("YC API returned %s for batch=%s page=%s",
                               resp.status_code, batch, page)
            return out

        try:
            data = resp.json()
        except Exception:
            logger.warning("YC API returned non-JSON for batch=%s", batch)
            return out

        companies = data.get("companies") or []
        for c in companies:
            out.append({
                "name": c.get("name"),
                "slug": c.get("slug"),
                "website": c.get("website") or "",
                "batch": c.get("batch") or batch,
                "one_liner": c.get("oneLiner") or "",
                "industries": c.get("industries") or [],
                "status": c.get("status"),
            })

        total_pages = int(data.get("totalPages") or 1)
        if page >= total_pages:
            break
        page += 1

    return out


def fetch_yc_batches(
    batches: Iterable[str] = DEFAULT_BATCHES,
    http: httpx.Client | None = None,
    *,
    sleep_between: float = 0.2,
) -> list[dict]:
    """Fetch multiple YC batches, deduping by company name (case-insensitive)."""
    seen: set[str] = set()
    out: list[dict] = []
    for b in batches:
        for c in fetch_yc_batch(b, http=http):
            key = (c.get("name") or "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(c)
        if sleep_between:
            time.sleep(sleep_between)
    return out


# ---------------------------------------------------------------------------
# ATS probing
# ---------------------------------------------------------------------------

# Order matters: greenhouse is the most common for YC/seed/A stage, then lever,
# then ashby. We stop on first verified hit to limit request volume.
_PROBE_ORDER = ("greenhouse", "lever", "ashby")


def _count_jobs(platform: str, slug: str, ats: ATSClient) -> int:
    """Return the job count returned by *platform* for *slug*, or 0."""
    try:
        if platform == "greenhouse":
            return len(ats.fetch_greenhouse(slug))
        if platform == "lever":
            return len(ats.fetch_lever(slug))
        if platform == "ashby":
            return len(ats.fetch_ashby(slug))
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug("Error probing %s/%s: %s", platform, slug, exc)
    return 0


def probe_company_ats(
    name: str,
    website: str,
    http: httpx.Client | None = None,
    *,
    ats: ATSClient | None = None,
) -> dict | None:
    """Return a verified seed entry dict if the company's ATS is found.

    Tries each candidate slug on greenhouse → lever → ashby. First 200 with
    at least one job wins. Returns ``None`` if nothing matched (incl. all 404
    or all 429 responses).
    """
    client = ats or ATSClient(http_client=http or httpx.Client(timeout=15, follow_redirects=True))
    slugs = candidate_slugs(name, website)
    if not slugs:
        return None

    for platform in _PROBE_ORDER:
        for slug in slugs:
            n = _count_jobs(platform, slug, client)
            if n > 0:
                return {
                    "name": name,
                    "website": website,
                    "ats_platform": platform,
                    "ats_slug": slug,
                    "ats_slug_verified": True,
                    "job_count": n,
                    "industry": "",
                    "is_dream_company": False,
                    "notes": f"Auto-verified via YC seeder: {n} jobs on {platform}",
                }
    return None


# ---------------------------------------------------------------------------
# Seed-file merge (idempotent)
# ---------------------------------------------------------------------------

def _norm_name(s: str) -> str:
    return (s or "").strip().lower()


def merge_into_seed(seed_path: Path, new_entries: list[dict]) -> int:
    """Merge *new_entries* into the seed JSON at *seed_path*.

    * Creates the file if missing (with ``_meta`` scaffold + empty companies).
    * Dedup by ``name`` (case-insensitive). Existing rows win — we do not
      overwrite them, keeping user-curated notes / flags intact.
    * Returns the number of entries actually appended.
    """
    seed_path = Path(seed_path)

    if seed_path.exists():
        data = json.loads(seed_path.read_text())
    else:
        seed_path.parent.mkdir(parents=True, exist_ok=True)
        data = {"_meta": {"description": "Seed companies for ATS-based job ingestion"},
                "companies": []}

    existing_names = {_norm_name(c.get("name", "")) for c in data.get("companies", [])}
    added = 0
    for entry in new_entries:
        key = _norm_name(entry.get("name", ""))
        if not key or key in existing_names:
            continue
        data.setdefault("companies", []).append(entry)
        existing_names.add(key)
        added += 1

    # Refresh _meta counters
    companies = data.get("companies", [])
    meta = data.setdefault("_meta", {})
    meta["total"] = len(companies)
    meta["verified_count"] = sum(1 for c in companies if c.get("ats_slug_verified"))

    seed_path.write_text(json.dumps(data, indent=2))
    return added
