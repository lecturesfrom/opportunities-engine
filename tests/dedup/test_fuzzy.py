"""Tests for fuzzy_match boundary cases.

String pairs are verified to land in specific score bands:
  ~100 (exact duplicate, outcome = duplicate)
  ~97  (>=95, outcome = duplicate)
  ~94  (>=93 and <95, outcome = review_flagged)
  ~92  (<93, outcome = new_job)
  ~70  (<93, definitely new_job)

Thresholds: DEDUP_THRESHOLD=95, DEDUP_REVIEW_FLOOR=93.

The exact scores are locked in after verifying with rapidfuzz.WRatio.
"""

import pytest
from rapidfuzz import fuzz

from opportunities_engine.dedup.fuzzy import fuzzy_match

# Locked pairs with their verified WRatio scores
EXACT_PAIR = (
    "senior software engineer|stripe|remote",
    "senior software engineer|stripe|remote",
)  # 100.0

HIGH_DUP_PAIR = (
    "product manager|stripe|remote",
    "senior product manager|stripe|remote",
)  # 95.0

REVIEW_PAIR = (
    "software engineer|stripe|remote",
    "software engineer iii|stripe|remote",
)  # 93.9 → in [93, 95) → review_flagged

BELOW_FLOOR_PAIR = (
    "senior software engineer|acme|remote",
    "staff software engineer|acme|remote",
)  # 87.3 → < 93 → new_job

LOW_SCORE_PAIR = (
    "software engineer|acme|remote",
    "product designer|acme|remote",
)  # 70.2 → < 93 → new_job


DEDUP_THRESHOLD = 95
DEDUP_REVIEW_FLOOR = 93


class TestFuzzyMatchScores:
    """Verify that our locked pairs land in the expected score bands."""

    def test_exact_pair_is_100(self) -> None:
        score = fuzz.WRatio(EXACT_PAIR[0], EXACT_PAIR[1])
        assert score == 100.0

    def test_high_dup_pair_is_gte_95(self) -> None:
        score = fuzz.WRatio(HIGH_DUP_PAIR[0], HIGH_DUP_PAIR[1])
        assert score >= DEDUP_THRESHOLD, f"Expected >= {DEDUP_THRESHOLD}, got {score}"

    def test_review_pair_is_in_review_band(self) -> None:
        score = fuzz.WRatio(REVIEW_PAIR[0], REVIEW_PAIR[1])
        assert DEDUP_REVIEW_FLOOR <= score < DEDUP_THRESHOLD, (
            f"Expected [{DEDUP_REVIEW_FLOOR}, {DEDUP_THRESHOLD}), got {score}"
        )

    def test_below_floor_pair_is_lt_93(self) -> None:
        score = fuzz.WRatio(BELOW_FLOOR_PAIR[0], BELOW_FLOOR_PAIR[1])
        assert score < DEDUP_REVIEW_FLOOR, f"Expected < {DEDUP_REVIEW_FLOOR}, got {score}"

    def test_low_score_pair_is_lt_80(self) -> None:
        score = fuzz.WRatio(LOW_SCORE_PAIR[0], LOW_SCORE_PAIR[1])
        assert score < 80, f"Expected < 80, got {score}"


class TestFuzzyMatchFunction:
    def test_returns_none_for_empty_candidates(self) -> None:
        result = fuzzy_match("senior engineer|acme|remote", [])
        assert result is None

    def test_returns_best_match_from_single_candidate(self) -> None:
        candidates = [(1, EXACT_PAIR[1])]
        result = fuzzy_match(EXACT_PAIR[0], candidates)
        assert result is not None
        job_id, score = result
        assert job_id == 1
        assert score == 100.0

    def test_returns_highest_score_from_multiple_candidates(self) -> None:
        candidates = [
            (1, "product manager|stripe|remote"),
            (2, "data scientist|stripe|remote"),
            (3, "senior product manager|stripe|remote"),
        ]
        # new_key is "product manager|stripe|remote" — should match id=1 exactly
        result = fuzzy_match("product manager|stripe|remote", candidates)
        assert result is not None
        job_id, score = result
        assert job_id == 1
        assert score == 100.0

    def test_exact_match_score_100(self) -> None:
        candidates = [(42, EXACT_PAIR[1])]
        result = fuzzy_match(EXACT_PAIR[0], candidates)
        assert result is not None
        _, score = result
        assert score >= DEDUP_THRESHOLD  # 100 >= 95 → duplicate

    def test_high_dup_score_gte_threshold(self) -> None:
        candidates = [(7, HIGH_DUP_PAIR[1])]
        result = fuzzy_match(HIGH_DUP_PAIR[0], candidates)
        assert result is not None
        _, score = result
        assert score >= DEDUP_THRESHOLD  # duplicate outcome

    def test_review_score_in_review_band(self) -> None:
        candidates = [(3, REVIEW_PAIR[1])]
        result = fuzzy_match(REVIEW_PAIR[0], candidates)
        assert result is not None
        _, score = result
        assert DEDUP_REVIEW_FLOOR <= score < DEDUP_THRESHOLD  # review_flagged outcome

    def test_below_floor_score_lt_review_floor(self) -> None:
        candidates = [(5, BELOW_FLOOR_PAIR[1])]
        result = fuzzy_match(BELOW_FLOOR_PAIR[0], candidates)
        assert result is not None
        _, score = result
        assert score < DEDUP_REVIEW_FLOOR  # new_job outcome

    def test_low_score_clearly_new_job(self) -> None:
        candidates = [(9, LOW_SCORE_PAIR[1])]
        result = fuzzy_match(LOW_SCORE_PAIR[0], candidates)
        assert result is not None
        _, score = result
        assert score < DEDUP_REVIEW_FLOOR  # new_job outcome

    def test_picks_best_when_mixed_candidates(self) -> None:
        """When multiple candidates are present, returns the highest-scoring one."""
        candidates = [
            (10, LOW_SCORE_PAIR[1]),       # low score
            (11, BELOW_FLOOR_PAIR[1]),     # also low
            (12, HIGH_DUP_PAIR[1]),        # high score
        ]
        new_key = HIGH_DUP_PAIR[0]
        result = fuzzy_match(new_key, candidates)
        assert result is not None
        job_id, score = result
        assert job_id == 12
        assert score >= DEDUP_THRESHOLD
