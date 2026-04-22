"""Quality filters for job ranking (Phase F.3).

Pure functions with injected pattern lists — no imports from config so
these stay independently testable.
"""
from __future__ import annotations

import re


def title_excluded(title: str, patterns: list[str]) -> str | None:
    """Return the first matching pattern (for audit) or None.

    Matches are case-insensitive. First match wins.
    """
    for pattern in patterns:
        if re.search(pattern, title, re.IGNORECASE):
            return pattern
    return None


def location_excluded(title: str, location: str, patterns: list[str]) -> str | None:
    """Return the first matching pattern (for audit) or None.

    Matches against title and location concatenated with a space,
    case-insensitive. First match wins.

    Note: only the exact geo restriction variants listed in patterns are
    caught (e.g. "Canada-based" and "Canada-only" but NOT "based in Canada"
    or plain "EU" without a qualifier).
    """
    combined = f"{title} {location}"
    for pattern in patterns:
        if re.search(pattern, combined, re.IGNORECASE):
            return pattern
    return None


def is_remote_first_company(company: str, whitelist: list[str]) -> bool:
    """Case-insensitive exact-match of company name against the whitelist.

    Uses str.casefold() for Unicode-safe comparison. Only exact matches
    qualify — "Vercel Inc" does NOT match "Vercel".
    """
    normalized = company.casefold()
    return any(normalized == entry.casefold() for entry in whitelist)
