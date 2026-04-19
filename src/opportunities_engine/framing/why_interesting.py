"""Why Interesting — deterministic template-based framing module.

Generates a 2-3 sentence blurb explaining why a ranked job matches
Keegan's profile. Template-based (no LLM API call) for deterministic
and testable output. Can be upgraded to LLM-generated blurbs later.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# ── Public heading used in Linear descriptions ────────────────────────

WHY_INTERESTING_HEADING: str = "💡 Why Interesting"

# ── Keegan's profile constants ────────────────────────────────────────

_KEEGAN_PROFILE = {
    "ideal_role": "Founding GTM Engineer",
    "experience_map": {
        # Title patterns → experience framing
        "gtm engineer": "Your Mixmax founding GTME experience maps directly",
        "go-to-market engineer": "Your Mixmax founding GTME experience maps directly",
        "founding gtm": "Your Mixmax founding GTME experience maps directly",
        "founding sales": "Your Mixmax founding GTME background gives you a natural edge",
        "forward deployed": "Your GTM engineering background aligns well with forward-deployed customer work",
        "solutions engineer": "Your technical GTM experience at Mixmax and Mobb.AI translates well to solutions engineering",
        "sales engineer": "Your technical GTM experience at Mixmax and Mobb.AI translates well to sales engineering",
        "customer engineer": "Your customer-facing GTM experience maps well to this customer engineer role",
        "growth engineer": "Your GTM engineering experience bridges growth and technical execution",
        "growth": "Your GTM engineering background brings a growth-minded approach",
        "product engineer": "Your GTM engineering experience bridges product thinking and technical execution",
        "revops": "Your GTM systems experience maps directly to revenue operations",
        "revenue operations": "Your GTM systems experience maps directly to revenue operations",
        "head of growth": "Your founding GTM experience positions you well to lead growth",
        "account executive": "Your technical GTM background gives you an edge in technical sales",
    },
    "past_companies": ["Mixmax", "Mobb.AI", "TraceAir", "Biofourmis"],
    "core_experience": "founding GTME at Mixmax",
    "stack": ["Python", "ETL", "AI agents", "Claude Code", "Clay", "SmartLead"],
    "taste": "craft, design, and creative tooling (music/audio-adjacent)",
}

# Skills we scan for in job descriptions
_SKILL_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("Python", re.compile(r"\bpython\b", re.IGNORECASE)),
    ("ETL", re.compile(r"\betl\b", re.IGNORECASE)),
    ("AI agents", re.compile(r"\bai\s+agents?\b", re.IGNORECASE)),
    ("Claude Code", re.compile(r"\bclaude\s+code\b", re.IGNORECASE)),
    ("Clay", re.compile(r"\bclay\b", re.IGNORECASE)),
    ("SmartLead", re.compile(r"\bsmartlead\b", re.IGNORECASE)),
]


# ── Dream company loading ─────────────────────────────────────────────

def load_dream_companies(path: Path | str | None = None) -> list[dict[str, Any]]:
    """Load dream companies from JSON file. Returns [] on any error."""
    if path is None:
        from opportunities_engine.config import REPO_ROOT
        path = REPO_ROOT / "data" / "dream_companies.json"
    path = Path(path)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []
    return data.get("companies", [])


def detect_dream_company(
    company: str,
    dream_companies: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """Find matching dream company entry (case-insensitive). Returns None if no match."""
    target = company.strip().lower()
    for dc in dream_companies:
        if dc.get("name", "").strip().lower() == target:
            return dc
    return None


# ── Founding role detection ──────────────────────────────────────────

def detect_founding_role(title: str) -> bool:
    """Return True if the title indicates a founding/early-stage role."""
    return bool(re.search(r"\bfounding\b", title, re.IGNORECASE))


# ── Skill matching ────────────────────────────────────────────────────

def match_skills(text: str | None) -> list[str]:
    """Scan text for known skill keywords. Returns list of matched skill names."""
    if not text:
        return []
    matched = []
    for name, pattern in _SKILL_PATTERNS:
        if pattern.search(text):
            matched.append(name)
    return matched


# ── Role-to-experience mapping ────────────────────────────────────────

def _experience_mapping(title: str) -> str | None:
    """Map a job title to an experience framing sentence fragment."""
    title_lower = title.strip().lower()
    for pattern, framing in _KEEGAN_PROFILE["experience_map"].items():
        if pattern in title_lower:
            return framing
    return None


# ── Main blurb generator ─────────────────────────────────────────────

def generate_why_interesting(
    job: dict,
    dream_companies_path: Path | str | None = None,
) -> str:
    """Generate a 2-3 sentence 'Why Interesting' blurb for a ranked job.

    Args:
        job: A ranked job dict with at least 'title' and 'company'.
        dream_companies_path: Path to dream_companies.json. Defaults to
            the repo's data/dream_companies.json.

    Returns:
        A 2-3 sentence blurb string.
    """
    title = str(job.get("title", "")).strip()
    company = str(job.get("company", "")).strip()
    description = str(job.get("description", "")).strip() if job.get("description") else ""

    # Load dream companies
    dream_companies = load_dream_companies(dream_companies_path)
    dream_match = detect_dream_company(company, dream_companies)

    # Detect signals
    is_founding = detect_founding_role(title)
    skill_matches = match_skills(description) or match_skills(f"{title} {company}")
    exp_mapping = _experience_mapping(title)

    # ── Build sentences ───────────────────────────────────────────────

    sentences: list[str] = []

    # Sentence 1: Company + role intro, dream company callout
    s1_parts: list[str] = []
    s1_parts.append(f"{title} at {company}")

    if dream_match:
        tier = dream_match.get("priority", "")
        tier_label = f" ({tier} dream company)" if tier else " (dream company)"
        s1_parts.append(f"is a{tier_label} match")
    else:
        s1_parts.append("aligns with your GTM engineering trajectory")

    if is_founding:
        s1_parts.append("and this is a founding/early-stage role where you can shape the GTM function from zero")
    else:
        s1_parts.append("where your technical and commercial instincts are assets")

    sentences.append(" ".join(s1_parts) + ".")

    # Sentence 2: Experience mapping
    if exp_mapping:
        sentences.append(f"{exp_mapping}.")
    else:
        # Generic fallback referencing core experience
        sentences.append(
            f"Your {_KEEGAN_PROFILE['core_experience']} experience positions you well for this kind of technical-commercial hybrid."
        )

    # Sentence 3: Skill matches (if any)
    if skill_matches:
        skills_str = ", ".join(skill_matches)
        sentences.append(
            f"Direct stack overlap: {skills_str}."
        )

    blurb = " ".join(sentences)
    return blurb
