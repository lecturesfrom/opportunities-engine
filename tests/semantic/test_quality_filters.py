"""Table-driven tests for semantic/quality_filters.py (Phase F.3)."""

from __future__ import annotations

import pytest

from opportunities_engine.config import EXCLUDE_LOCATION_MARKERS, EXCLUDE_TITLE_MARKERS
from opportunities_engine.semantic.quality_filters import (
    is_remote_first_company,
    location_excluded,
    title_excluded,
)

# ---------------------------------------------------------------------------
# title_excluded
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, expected_match",
    [
        # FDE patterns — exact word boundary matches
        ("Founding FDE @ CiceroAI", r"\bfde\b"),
        ("FDE at Startup", r"\bfde\b"),
        ("FDE", r"\bfde\b"),
        # word-boundary: "OFDE" should NOT match \bfde\b
        ("OFDE Engineer", None),
        # forward deployed
        ("Forward Deployed Engineer", r"\bforward deployed\b"),
        ("forward deployed engineer", r"\bforward deployed\b"),
        # solutions? engineer (singular and plural)
        ("Solutions Engineer", r"\bsolutions? engineer\b"),
        ("Solution Engineer", r"\bsolutions? engineer\b"),
        # sales / customer engineer
        ("Sales Engineer, West", r"\bsales engineer\b"),
        ("Customer Engineer", r"\bcustomer engineer\b"),
        # technical account manager
        ("Technical Account Manager", r"\btechnical account manager\b"),
        ("Senior Technical Account Manager", r"\btechnical account manager\b"),
        # TAM
        ("TAM — Strategic Accounts", r"\btam\b"),
        # founding engineer (not founding GTM)
        ("Founding Engineer", r"\bfounding engineer\b"),
        # full-stack / fullstack variants
        ("Full Stack Engineer", r"\bfull[- ]?stack engineer\b"),
        ("Full-Stack Engineer", r"\bfull[- ]?stack engineer\b"),
        ("Fullstack Engineer", r"\bfull[- ]?stack engineer\b"),
        # GTM titles that must NOT be excluded
        ("GTM Engineer", None),
        ("Senior GTM Engineer", None),
        ("RevOps Engineer", None),
        ("Growth Engineer", None),
        ("Founding GTM", None),
        # case-insensitive check
        ("SALES ENGINEER", r"\bsales engineer\b"),
    ],
)
def test_title_excluded(title: str, expected_match: str | None) -> None:
    result = title_excluded(title, EXCLUDE_TITLE_MARKERS)
    if expected_match is None:
        assert result is None, f"Expected no match for {title!r}, but got {result!r}"
    else:
        assert result == expected_match, (
            f"Expected pattern {expected_match!r} for {title!r}, got {result!r}"
        )


# ---------------------------------------------------------------------------
# location_excluded
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "title, location, expected_match",
    [
        # LatAm in title
        ("Senior GTM Engineer (LatAm)", "Remote", r"\blatam\b"),
        # LatAm in location
        ("GTM Engineer", "LatAm", r"\blatam\b"),
        # Latin America spelled out
        ("GTM Engineer", "Latin America", r"\blatin america\b"),
        # Europe only in location
        ("GTM Engineer", "Europe only", r"\beurope[- ]?only\b"),
        # EU-only
        ("GTM Engineer", "EU-only", r"\beu[- ]?only\b"),
        # UK-only
        ("GTM Engineer", "UK-only", r"\buk[- ]?only\b"),
        # Canada-only
        ("GTM Engineer", "Canada-only", r"\bcanada[- ]?only\b"),
        # Canada-based
        ("GTM Engineer", "Canada-based", r"\bcanada[- ]?based\b"),
        # Germany-only
        ("GTM Engineer", "Germany-only", r"\bgermany[- ]?only\b"),
        # UK & Ireland
        ("GTM Engineer", "UK & Ireland", r"\buk\s*&\s*ireland\b"),
        # EMEA only
        ("GTM Engineer", "EMEA only", r"\bemea\s*only\b"),
        # Normal remote — should NOT match
        ("GTM Engineer", "Remote, US", None),
        ("Senior GTM Engineer", "United States", None),
        # Plain "EU" without qualifier should NOT match (limitation documented)
        ("GTM Engineer", "EU", None),
        # "based in Canada" should NOT match (only Canada-based / Canada-only)
        ("GTM Engineer", "based in Canada", None),
    ],
)
def test_location_excluded(
    title: str, location: str, expected_match: str | None
) -> None:
    result = location_excluded(title, location, EXCLUDE_LOCATION_MARKERS)
    if expected_match is None:
        assert result is None, (
            f"Expected no match for title={title!r} location={location!r}, "
            f"but got {result!r}"
        )
    else:
        assert result == expected_match, (
            f"Expected pattern {expected_match!r} for title={title!r} "
            f"location={location!r}, got {result!r}"
        )


# ---------------------------------------------------------------------------
# is_remote_first_company
# ---------------------------------------------------------------------------

REMOTE_WHITELIST = [
    "Vercel",
    "GitLab",
    "Zapier",
    "Automattic",
    "Buffer",
    "HashiCorp",
    "Doist",
    "Basecamp",
    "Ghost",
    "Clerk",
    "PostHog",
    "Replicate",
]


@pytest.mark.parametrize(
    "company, expected",
    [
        # Exact match
        ("Vercel", True),
        ("GitLab", True),
        ("Zapier", True),
        # Case-insensitive exact match
        ("VERCEL", True),
        ("vercel", True),
        ("gitlab", True),
        ("ZAPIER", True),
        # Partial / extended names do NOT match (exact match only)
        ("Vercel Inc", False),
        ("vercel inc", False),
        ("GitLab Inc.", False),
        # Unknown company
        ("Horizonia", False),
        ("CiceroAI", False),
        ("", False),
    ],
)
def test_is_remote_first_company(company: str, expected: bool) -> None:
    assert is_remote_first_company(company, REMOTE_WHITELIST) is expected
