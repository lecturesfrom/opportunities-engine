"""Fast relevance ranking (token-efficient, no heavy model deps).

Uses TF-IDF + rule gates to surface GTM-relevant jobs and suppress noise.
Designed for speed/cost over fancy embeddings.
"""
from __future__ import annotations

import re
from typing import Iterable

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from opportunities_engine.config import DEFAULT_TARGET_TITLES

# Strong include signals for your target role family
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
    r"\bsolutions? architect\b",
]

# Hard excludes to kill obvious irrelevant jobs
EXCLUDE_PATTERNS = [
    r"\bnurse\b", r"\bphysician\b", r"\bclinical\b", r"\btherapist\b",
    r"\bdentist\b", r"\bbehavior\b", r"\bcfo\b", r"\baccountant\b",
    r"\bteacher\b", r"\bprofessor\b", r"\bwarehouse\b", r"\bdriver\b",
    r"\bconstruction\b", r"\bbariatric\b", r"\bveterinary\b",
]


def _text(job: dict) -> str:
    title = str(job.get("title", ""))
    desc = str(job.get("description", ""))[:1200]
    company = str(job.get("company", ""))
    return f"{title} {company} {desc}".lower()


def _title(job: dict) -> str:
    return str(job.get("title", "")).lower()


def _matches_any(patterns: Iterable[str], text: str) -> bool:
    return any(re.search(p, text, re.IGNORECASE) for p in patterns)


def filter_relevant(jobs: list[dict]) -> list[dict]:
    out = []
    for job in jobs:
        title = _title(job)
        body = _text(job)

        if _matches_any(EXCLUDE_PATTERNS, title):
            continue

        # Require at least one strong signal in title/body
        if not _matches_any(INCLUDE_PATTERNS, f"{title} {body}"):
            continue

        out.append(job)
    return out


def rank_jobs_local(jobs: list[dict], top_k: int = 50, min_score: float = 0.14) -> list[dict]:
    """Return ranked relevant jobs using TF-IDF similarity + signal bonuses."""
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
        if _matches_any(INCLUDE_PATTERNS[:8], title):  # strongest patterns in title
            bonus += 0.08
        if "founding" in title:
            bonus += 0.05
        if "engineer" in title:
            bonus += 0.03

        score = float(sim) + bonus
        if score < min_score:
            continue

        ranked.append({
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "url": job.get("url", ""),
            "source": job.get("source", ""),
            "is_remote": bool(job.get("is_remote", False)),
            "similarity": round(score, 3),
        })

    ranked.sort(key=lambda x: x["similarity"], reverse=True)
    return ranked[:top_k]
