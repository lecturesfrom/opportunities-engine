"""Remote-work gate for job filtering.

Extracted from scripts/push_top_to_linear.py so both rank.py and
push_top_to_linear.py share a single authoritative implementation.
"""
from __future__ import annotations

_NON_REMOTE_MARKERS: tuple[str, ...] = (
    "hybrid",
    "in-office",
    "in office",
    "onsite",
    "on-site",
    "on site",
)


def is_remote(job: dict) -> bool:
    """Hard gate: a job must declare itself remote OR have no non-remote markers.

    Returns True if the job is considered remote-friendly, False otherwise.
    Conservative default: if is_remote is None and no remote location signals,
    returns False (the caller can override via engine event add if needed).
    """
    if job.get("is_remote") is True:
        return True
    loc = (job.get("location") or "").lower()
    if any(m in loc for m in _NON_REMOTE_MARKERS):
        return False
    # Conservative default: if is_remote is None and location mentions remote words, allow
    if "remote" in loc or "anywhere" in loc:
        return True
    # Unknown — drop.
    return False
