"""Tests for semantic/remote_filter.py — is_remote_or_whitelisted (Phase F.3)."""

from __future__ import annotations

import pytest

from opportunities_engine.semantic.remote_filter import is_remote, is_remote_or_whitelisted

_WHITELIST = ["Vercel", "GitLab", "Zapier"]


class TestIsRemoteOrWhitelisted:
    """Unit tests for the whitelist-aware remote gate."""

    def test_whitelisted_company_non_remote_location_passes(self) -> None:
        """Vercel with a San Francisco location bypasses the remote gate."""
        job = {"company": "Vercel", "location": "San Francisco, CA", "is_remote": None}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_whitelisted_company_remote_location_passes(self) -> None:
        """Vercel with an explicit Remote location also passes (short-circuits)."""
        job = {"company": "Vercel", "location": "Remote", "is_remote": True}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_whitelisted_company_hybrid_location_still_passes(self) -> None:
        """Whitelisted company overrides even an explicit 'hybrid' marker."""
        job = {"company": "Vercel", "location": "San Francisco (Hybrid)", "is_remote": False}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_unknown_company_non_remote_location_fails(self) -> None:
        """Non-whitelisted company + non-remote location is rejected."""
        job = {"company": "Horizonia", "location": "New York, NY", "is_remote": None}
        assert is_remote_or_whitelisted(job, _WHITELIST) is False

    def test_unknown_company_remote_location_passes(self) -> None:
        """Non-whitelisted company BUT explicit remote location still passes."""
        job = {"company": "Horizonia", "location": "Remote", "is_remote": None}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_unknown_company_is_remote_true_passes(self) -> None:
        """Non-whitelisted company with is_remote=True passes via is_remote()."""
        job = {"company": "Some Corp", "location": "Anywhere", "is_remote": True}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_empty_whitelist_falls_back_to_is_remote(self) -> None:
        """With an empty whitelist, behavior is identical to is_remote()."""
        job = {"company": "Vercel", "location": "San Francisco", "is_remote": None}
        # Empty whitelist → falls back to is_remote() which returns False for SF
        assert is_remote_or_whitelisted(job, []) is False

    def test_case_insensitive_company_match(self) -> None:
        """Company matching is case-insensitive: 'VERCEL' == 'Vercel'."""
        job = {"company": "VERCEL", "location": "San Francisco", "is_remote": None}
        assert is_remote_or_whitelisted(job, _WHITELIST) is True

    def test_partial_company_name_does_not_match(self) -> None:
        """'Vercel Inc' does not match whitelisted 'Vercel' (exact match only)."""
        job = {"company": "Vercel Inc", "location": "San Francisco", "is_remote": None}
        assert is_remote_or_whitelisted(job, _WHITELIST) is False

    def test_missing_company_field_falls_back_to_is_remote(self) -> None:
        """Job without a company key falls back to is_remote() logic."""
        job = {"location": "Remote", "is_remote": None}
        # Falls back to is_remote() — 'remote' in location → True
        assert is_remote_or_whitelisted(job, _WHITELIST) is True
