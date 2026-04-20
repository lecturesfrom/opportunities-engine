"""Parse Linear comment text into event_type constants.

First regex match wins; unmatched comments return None (no event).
"""

from __future__ import annotations

import re

from opportunities_engine.events.vocab import (
    APPLIED,
    INTERVIEW,
    OFFER,
    PHONE_SCREEN,
    REJECTED,
    WITHDREW,
)

# Each entry is (compiled_pattern, event_type_constant).
# Order matters — first match wins.
_LADDER: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(applied|submitted application)\b", re.IGNORECASE), APPLIED),
    (
        re.compile(
            r"\b(screener|screening call|recruiter call|phone screen)\b",
            re.IGNORECASE,
        ),
        PHONE_SCREEN,
    ),
    (
        re.compile(
            r"\b(onsite|technical interview|final round|interview loop)\b",
            re.IGNORECASE,
        ),
        INTERVIEW,
    ),
    (re.compile(r"\b(offer|offered)\b", re.IGNORECASE), OFFER),
    (
        re.compile(
            r"\b(reject|rejected|passed on|ghosted)\b",
            re.IGNORECASE,
        ),
        REJECTED,
    ),
    (re.compile(r"\b(withdrew|withdrawn)\b", re.IGNORECASE), WITHDREW),
]


def parse_comment(text: str) -> str | None:
    """Return an event_type for this comment body, or None if no match.

    Case-insensitive.  First regex to match wins.

    Args:
        text: Raw comment body text.

    Returns:
        A vocab constant string (e.g. ``APPLIED``) or ``None``.
    """
    for pattern, event_type in _LADDER:
        if pattern.search(text):
            return event_type
    return None
