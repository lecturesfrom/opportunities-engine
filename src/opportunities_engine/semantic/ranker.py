"""Fast relevance ranking (token-efficient).

US/remote-only gate + curated title universe + TF-IDF scoring.
"""
from __future__ import annotations

import re
from typing import Iterable

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from opportunities_engine.config import (
    DEFAULT_TARGET_TITLES,
    US_LOCATION_PATTERNS,
    NON_US_LOCATION_PATTERNS,
)

INCLUDE_PATTERNS = [
    r"\bfounding gtm\b",
    r"\bgtm engineer\b",
    r"\bgo[- ]to[- ]market\b",
    r"\bgrowth engineer\b",
    r"\bforward deployed\b",
    r"\bsolutions? engineer\b",
    r"\bsales engineer\b",
    r"\bcustomer engineer\b",
    r"\brevops\b",
    r"\brevenue operations\b",
    r"\bhead of growth\b",
    r"\bgrowth lead\b",
    r"\btechnical product manager\b",
    r"\bproduct engineer\b",
    r"\baccount executive \(technical\)\b",
]

EXCLUDE_PATTERNS = [
    r"\bnurse\b", r"\bphysician\b", r"\bclinical\b", r"\btherapist\b",
    r"\bdentist\b", r"\bbehavior\b", r"\bcfo\b", r"\baccountant\b",
    r"\bteacher\b", r"\bprofessor\b", r"\bwarehouse\b", r"\bdriver\b",
    r"\bconstruction\b", r"\bbariatric\b", r"\bveterinary\b",
]


def _matches_any(patterns: Iterable[str], text: str) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def _title(job: dict) -> str:
    return str(job.get("title", "")).lower().strip()


def _location(job: dict) -> str:
    return str(job.get("location", "")).lower().strip()


def _text(job: dict) -> str:
    title = str(job.get("title", ""))
    company = str(job.get("company", ""))
    desc = str(job.get("description", ""))[:1200]
    loc = str(job.get("location", ""))
    return f"{title} {company} {loc} {desc}".lower()


def _is_us_or_remote(job: dict) -> bool:
    title = _title(job)
    loc = _location(job)
    desc = str(job.get("description", "")).lower()[:600]
    geo_blob = f"{title} {loc} {desc}"

    # hard reject known non-US geo hints even if 'remote'
    if any(p in geo_blob for p in NON_US_LOCATION_PATTERNS):
        return False

    # explicit US/remote signals
    if any(p in geo_blob for p in US_LOCATION_PATTERNS):
        return True

    # fallback to explicit remote flag
    if bool(job.get("is_remote", False)):
        return True

    return False


def _is_curated_title_hit(title: str) -> bool:
    curated = [t.lower() for t in DEFAULT_TARGET_TITLES]
    if title in curated:
        return True
    return any(title in c or c in title for c in curated)


def _source_priority(source: str) -> int:
    s = (source or "").lower()
    if any(x in s for x in ["greenhouse", "lever", "ashby"]):
        return 1
    if "jobspy" in s:
        return 2
    return 3


def _role_priority(title: str) -> int:
    t = title.lower()
    if any(x in t for x in ["founding gtm", "gtm engineer", "go-to-market engineer", "go to market engineer"]):
        return 1
    if "forward deployed" in t or "solutions engineer" in t:
        return 2
    if any(x in t for x in ["sales engineer", "customer engineer", "revops", "revenue operations"]):
        return 3
    if any(x in t for x in ["growth", "product manager", "product engineer", "technical product manager"]):
        return 4
    return 5


def _dedup_key(job: dict) -> tuple[str, str]:
    c = str(job.get("company", "")).strip().lower()
    t = re.sub(r"\s+", " ", str(job.get("title", "")).strip().lower())
    return c, t


def _dedup_prefer_ats(jobs: list[dict]) -> list[dict]:
    keep: dict[tuple[str, str], dict] = {}
    for j in jobs:
        k = _dedup_key(j)
        prev = keep.get(k)
        if not prev:
            keep[k] = j
            continue
        if _source_priority(str(j.get("source", ""))) < _source_priority(str(prev.get("source", ""))):
            keep[k] = j
    return list(keep.values())


def filter_relevant(jobs: list[dict]) -> list[dict]:
    out = []
    for job in jobs:
        title = _title(job)
        body = _text(job)

        if str(job.get("company", "")).strip().lower() in {"", "nan", "none", "null"}:
            continue
        if not _is_us_or_remote(job):
            continue
        if _matches_any(EXCLUDE_PATTERNS, f"{title} {body}"):
            continue
        if not (_is_curated_title_hit(title) or _matches_any(INCLUDE_PATTERNS, f"{title} {body}")):
            continue

        out.append(job)

    return _dedup_prefer_ats(out)


def rank_jobs_local(jobs: list[dict], top_k: int = 50, min_score: float = 0.16) -> list[dict]:
    candidates = filter_relevant(jobs)
    if not candidates:
        return []

    query = " | ".join(DEFAULT_TARGET_TITLES + [
        "founding gtm engineer",
        "go to market engineer",
        "growth engineer",
        "solutions engineer",
        "sales engineer",
        "customer engineer",
        "forward deployed engineer",
        "revops engineer",
    ])

    docs = [query] + [_text(j) for j in candidates]
    vec = TfidfVectorizer(ngram_range=(1, 2), min_df=1, max_features=40000)
    X = vec.fit_transform(docs)
    sims = cosine_similarity(X[0:1], X[1:]).flatten()

    ranked = []
    for job, sim in zip(candidates, sims):
        title = _title(job)
        bonus = 0.0
        if _matches_any(INCLUDE_PATTERNS[:8], title):
            bonus += 0.08
        if "founding" in title:
            bonus += 0.05
        if "engineer" in title:
            bonus += 0.03

        score = float(sim) + bonus
        if score < min_score:
            continue

        ranked.append(
            {
                "title": job.get("title", ""),
                "company": job.get("company", ""),
                "url": job.get("url", ""),
                "source": job.get("source", ""),
                "is_remote": bool(job.get("is_remote", False)),
                "location": job.get("location", ""),
                "similarity": round(score, 3),
            }
        )

    ranked.sort(
        key=lambda x: (
            -x["similarity"],
            _role_priority(str(x.get("title", ""))),
            _source_priority(str(x.get("source", ""))),
        )
    )
    return ranked[:top_k]
