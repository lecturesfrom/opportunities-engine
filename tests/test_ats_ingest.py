"""Tests for ATS ingestion normalizers — Greenhouse, Lever, Ashby."""
from __future__ import annotations

import pytest

from opportunities_engine.ingestion.ats import (
    _normalize_ashby,
    _infer_remote,
)


# ---------------------------------------------------------------------------
# _infer_remote helper
# ---------------------------------------------------------------------------


class TestInferRemote:
    def test_remote_in_location_returns_true(self) -> None:
        assert _infer_remote("Remote, US") is True

    def test_non_remote_location_returns_false(self) -> None:
        assert _infer_remote("San Francisco, CA") is False

    def test_none_location_returns_none(self) -> None:
        assert _infer_remote(None) is None

    def test_case_insensitive(self) -> None:
        assert _infer_remote("REMOTE") is True
        assert _infer_remote("remote") is True


# ---------------------------------------------------------------------------
# _normalize_ashby — isRemote flag takes precedence
# ---------------------------------------------------------------------------


class TestNormalizeAshbyIsRemote:
    """Verify that _normalize_ashby uses Ashby's own isRemote metadata first."""

    def _make_raw(
        self,
        *,
        is_remote: object = None,
        location: str | None = None,
        location_type: str | None = None,
    ) -> dict:
        """Build a minimal Ashby raw payload."""
        raw: dict = {
            "id": "abc123",
            "title": "GTM Engineer",
            "companyName": "TestCo",
        }
        if is_remote is not None:
            raw["isRemote"] = is_remote
        if location is not None:
            raw["location"] = location
        if location_type is not None:
            raw["locationType"] = location_type
        return raw

    def test_isremote_true_wins_over_non_remote_location(self) -> None:
        """Ashby isRemote=True should produce is_remote=True even when location says San Francisco."""
        raw = self._make_raw(is_remote=True, location="San Francisco, CA")
        result = _normalize_ashby("testco", raw)
        assert result["is_remote"] is True

    def test_isremote_false_wins_over_remote_location(self) -> None:
        """Ashby isRemote=False should produce is_remote=False even when location says Remote."""
        raw = self._make_raw(is_remote=False, location="Remote, US")
        result = _normalize_ashby("testco", raw)
        assert result["is_remote"] is False

    def test_isremote_none_falls_back_to_infer_remote_location(self) -> None:
        """When isRemote is absent, _infer_remote(location) is used — remote location → True."""
        raw = self._make_raw(location="Remote")
        # isRemote key not present at all
        assert "isRemote" not in raw
        result = _normalize_ashby("testco", raw)
        assert result["is_remote"] is True

    def test_isremote_none_falls_back_to_infer_non_remote_location(self) -> None:
        """When isRemote is absent, _infer_remote(location) is used — non-remote location → False."""
        raw = self._make_raw(location="San Francisco")
        assert "isRemote" not in raw
        result = _normalize_ashby("testco", raw)
        assert result["is_remote"] is False

    def test_isremote_explicit_none_value_falls_back_to_infer(self) -> None:
        """isRemote=None in the payload (JSON null) should fall back to location inference."""
        raw = self._make_raw(location="Remote")
        raw["isRemote"] = None  # explicitly set to null/None
        result = _normalize_ashby("testco", raw)
        # raw.get("isRemote") returns None → fall back → location "Remote" → True
        assert result["is_remote"] is True

    def test_metadata_stores_raw_isremote(self) -> None:
        """ashby_is_remote in metadata always reflects the raw payload value."""
        raw = self._make_raw(is_remote=True, location="San Francisco, CA")
        result = _normalize_ashby("testco", raw)
        assert result["metadata"]["ashby_is_remote"] is True

    def test_source_is_ashby(self) -> None:
        raw = self._make_raw(is_remote=True)
        result = _normalize_ashby("testco", raw)
        assert result["source"] == "ashby"
