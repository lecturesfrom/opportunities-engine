"""Fuzzy matching for canonical job keys using rapidfuzz.WRatio."""

from rapidfuzz import fuzz


def fuzzy_match(
    new_key: str,
    candidates: list[tuple[int, str]],  # (job_id, canonical_key)
) -> tuple[int, float] | None:
    """Return (best_job_id, best_score) or None if candidates is empty.

    Uses rapidfuzz.WRatio for scoring. Returns the candidate with the
    highest score.
    """
    if not candidates:
        return None

    best_id: int = -1
    best_score: float = -1.0

    for job_id, candidate_key in candidates:
        score = fuzz.WRatio(new_key, candidate_key)
        if score > best_score:
            best_score = score
            best_id = job_id

    return (best_id, best_score)
