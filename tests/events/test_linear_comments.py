"""Table-driven tests for events.linear_comments.parse_comment."""

from __future__ import annotations

import pytest

from opportunities_engine.events.linear_comments import parse_comment
from opportunities_engine.events.vocab import (
    APPLIED,
    INTERVIEW,
    OFFER,
    PHONE_SCREEN,
    REJECTED,
    WITHDREW,
)


# ---------------------------------------------------------------------------
# Parametrize table: (comment_text, expected_event_type_or_None)
# ---------------------------------------------------------------------------

CASES: list[tuple[str, str | None]] = [
    # APPLIED — happy paths
    ("Just applied to this role", APPLIED),
    ("submitted application yesterday", APPLIED),
    ("Applied via LinkedIn", APPLIED),
    # PHONE_SCREEN — happy paths
    ("Had a screener with the HR person", PHONE_SCREEN),
    ("Scheduling a screening call next week", PHONE_SCREEN),
    ("recruiter call scheduled for Monday", PHONE_SCREEN),
    ("phone screen went well", PHONE_SCREEN),
    # INTERVIEW — happy paths
    ("Going for the onsite next Friday", INTERVIEW),
    ("Technical interview is tomorrow", INTERVIEW),
    ("Final round scheduled", INTERVIEW),
    ("interview loop starts Monday", INTERVIEW),
    # OFFER — happy paths
    ("Got an offer today!", OFFER),
    ("They offered me the position", OFFER),
    # REJECTED — happy paths
    ("Unfortunately they rejected me", REJECTED),
    ("reject — not moving forward", REJECTED),
    ("They passed on my candidacy", REJECTED),
    ("ghosted after the last round", REJECTED),
    # WITHDREW — happy paths
    ("I withdrew my application", WITHDREW),
    ("withdrawn from consideration", WITHDREW),
    # Should return None — unrelated comment text
    ("Moving card to the next column", None),
    ("", None),
    ("!!! ???", None),
    # Verify case-insensitivity
    ("APPLIED to this one", APPLIED),
    ("SCREENER tomorrow", PHONE_SCREEN),
    ("ONSITE at HQ", INTERVIEW),
    ("OFFER incoming", OFFER),
    ("REJECTED via email", REJECTED),
    ("WITHDREW from process", WITHDREW),
]


@pytest.mark.parametrize("text,expected", CASES)
def test_parse_comment(text: str, expected: str | None) -> None:
    """parse_comment returns the expected event type (or None) for the input text."""
    result = parse_comment(text)
    assert result == expected, f"parse_comment({text!r}) = {result!r}, expected {expected!r}"


# ---------------------------------------------------------------------------
# Additional targeted tests
# ---------------------------------------------------------------------------


def test_first_match_wins() -> None:
    """When multiple patterns could match, the first one in the ladder wins."""
    # "applied" comes before "offer" in the ladder
    text = "applied and later got an offer"
    result = parse_comment(text)
    assert result == APPLIED


def test_none_on_empty_string() -> None:
    """Empty string returns None."""
    assert parse_comment("") is None


def test_none_on_whitespace_only() -> None:
    """Whitespace-only string returns None."""
    assert parse_comment("   \t\n  ") is None


def test_word_boundary_prevents_partial_match() -> None:
    """'applied' inside a longer word should NOT match (word boundary check)."""
    # The regex uses \b, so 'reapplied' contains 'applied' without a leading \b
    # before the 'a', meaning it should NOT match APPLIED.
    # (This tests that our boundaries work correctly.)
    result = parse_comment("We reapplied the patch to production")
    # 'applied' has a \b before 'a' only when it's a standalone word.
    # In "reapplied", the 'a' of 'applied' is NOT at a word boundary.
    # So this should NOT match — return None.
    assert result is None
