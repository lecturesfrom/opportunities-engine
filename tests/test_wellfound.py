"""Tests for the Wellfound scraper (WellfoundSource).

Uses a synthetic HTML fixture at tests/fixtures/wellfound_sample.html.
All network calls are mocked — no real HTTP requests are made.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import httpx
import pytest

from opportunities_engine.ingestion.wellfound import (
    WellfoundSource,
    _extract_job_id,
    _infer_remote,
)

# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SAMPLE_HTML = (FIXTURES_DIR / "wellfound_sample.html").read_text()


def _make_http_mock(html: str = SAMPLE_HTML, status_code: int = 200) -> MagicMock:
    """Build a mocked httpx.Client that returns the given HTML."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    client = MagicMock(spec=httpx.Client)
    client.get.return_value = resp
    return client


# ---------------------------------------------------------------------------
# _parse_jsonld tests
# ---------------------------------------------------------------------------


class TestParseJsonld:
    def test_parse_jsonld_extracts_job(self) -> None:
        """JSON-LD with JobPosting type → normalized dict with expected fields."""
        source = WellfoundSource(http=_make_http_mock())
        jobs = source._parse_jsonld(SAMPLE_HTML)

        assert len(jobs) >= 1
        # First entry from the fixture
        titles = [j.get("title") for j in jobs]
        assert "Senior GTM Engineer" in titles

    def test_parse_jsonld_returns_only_job_postings(self) -> None:
        """Non-JobPosting @type entries are ignored."""
        html = """
        <script type="application/ld+json">
        [
          {"@type": "Organization", "name": "Acme"},
          {"@type": "JobPosting", "title": "GTM Engineer",
           "url": "https://wellfound.com/jobs/111-gtm-engineer",
           "hiringOrganization": {"@type": "Organization", "name": "Acme"}}
        ]
        </script>
        """
        source = WellfoundSource(http=_make_http_mock(html))
        jobs = source._parse_jsonld(html)
        assert len(jobs) == 1
        assert jobs[0]["title"] == "GTM Engineer"

    def test_parse_jsonld_empty_on_no_script(self) -> None:
        """HTML with no JSON-LD block returns empty list."""
        source = WellfoundSource(http=_make_http_mock("<html><body>nothing</body></html>"))
        jobs = source._parse_jsonld("<html><body>nothing</body></html>")
        assert jobs == []

    def test_parse_jsonld_skips_malformed_json(self) -> None:
        """Malformed JSON in a script block is skipped without raising."""
        html = '<script type="application/ld+json">{ broken json </script>'
        source = WellfoundSource(http=_make_http_mock(html))
        jobs = source._parse_jsonld(html)
        assert jobs == []


# ---------------------------------------------------------------------------
# _parse_dom tests
# ---------------------------------------------------------------------------


class TestParseDom:
    def test_parse_dom_extracts_job(self) -> None:
        """DOM parse on fixture HTML finds at least one job via link regex."""
        source = WellfoundSource(http=_make_http_mock())
        jobs = source._parse_dom(SAMPLE_HTML)

        assert len(jobs) >= 1
        # Verify job IDs are extracted
        source_ids = [j.get("source_id") for j in jobs]
        assert "9876543" in source_ids

    def test_parse_dom_builds_absolute_url(self) -> None:
        """DOM parser prefixes BASE_URL to the relative /jobs/NNN-slug path."""
        source = WellfoundSource(http=_make_http_mock())
        jobs = source._parse_dom(SAMPLE_HTML)
        for job in jobs:
            assert job["url"].startswith("https://wellfound.com/jobs/"), (
                f"Expected absolute URL, got: {job['url']}"
            )

    def test_parse_dom_empty_on_no_links(self) -> None:
        """HTML with no /jobs/NNN-slug links returns empty list."""
        html = "<html><body><p>No jobs here</p></body></html>"
        source = WellfoundSource(http=_make_http_mock(html))
        jobs = source._parse_dom(html)
        assert jobs == []


# ---------------------------------------------------------------------------
# _normalize tests
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_normalize_source_id_extraction(self) -> None:
        """url /jobs/1234567-senior-gtm → source_id='1234567'."""
        raw = {
            "@type": "JobPosting",
            "title": "Senior GTM Engineer",
            "url": "https://wellfound.com/jobs/1234567-senior-gtm-engineer",
            "hiringOrganization": {"@type": "Organization", "name": "Acme Corp"},
            "description": "Great role.",
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["source_id"] == "1234567"

    def test_normalize_sets_source_wellfound(self) -> None:
        """Normalized dict always has source='wellfound'."""
        raw = {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/9999-gtm-engineer",
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["source"] == "wellfound"

    def test_normalize_company_from_hiring_organization(self) -> None:
        """Company name extracted from hiringOrganization.name."""
        raw = {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/1-gtm",
            "hiringOrganization": {"@type": "Organization", "name": "Rocket Co"},
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["company"] == "Rocket Co"

    def test_normalize_location_from_job_location(self) -> None:
        """Location parsed from nested schema.org address."""
        raw = {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/1-gtm",
            "jobLocation": {
                "@type": "Place",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "San Francisco",
                    "addressRegion": "CA",
                    "addressCountry": "US",
                },
            },
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["location"] is not None
        assert "San Francisco" in norm["location"]


# ---------------------------------------------------------------------------
# is_remote tests
# ---------------------------------------------------------------------------


class TestIsRemote:
    def test_is_remote_from_jsonld_telecommute(self) -> None:
        """JSON-LD with jobLocationType=TELECOMMUTE → is_remote=True."""
        raw = {
            "@type": "JobPosting",
            "title": "Founding Growth Engineer",
            "url": "https://wellfound.com/jobs/7654321-founding-growth-engineer",
            "jobLocationType": "TELECOMMUTE",
            "hiringOrganization": {"@type": "Organization", "name": "Startup Inc"},
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["is_remote"] is True

    def test_is_remote_from_location_string(self) -> None:
        """Location string containing 'remote' → is_remote=True."""
        raw = {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/1-gtm",
            "jobLocation": {
                "@type": "Place",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "Remote",
                    "addressCountry": "US",
                },
            },
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["is_remote"] is True

    def test_is_remote_from_title(self) -> None:
        """Title 'Senior GTM Engineer (Remote)' → is_remote=True."""
        raw = {
            "@type": "JobPosting",
            "title": "Senior GTM Engineer (Remote)",
            "url": "https://wellfound.com/jobs/42-senior-gtm-engineer-remote",
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["is_remote"] is True

    def test_is_remote_none_when_no_remote_indicator(self) -> None:
        """No 'remote' in title or location → is_remote=None."""
        raw = {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/1-gtm",
            "jobLocation": {
                "@type": "Place",
                "address": {
                    "@type": "PostalAddress",
                    "addressLocality": "New York",
                    "addressRegion": "NY",
                    "addressCountry": "US",
                },
            },
        }
        source = WellfoundSource(http=_make_http_mock())
        norm = source._normalize(raw)
        assert norm["is_remote"] is None


# ---------------------------------------------------------------------------
# fetch tests
# ---------------------------------------------------------------------------


class TestFetch:
    def test_fetch_dedupes_by_url(self) -> None:
        """Two results with the same URL → returned once."""
        # Craft HTML with two identical job URLs in JSON-LD
        html = """
        <script type="application/ld+json">
        [
          {
            "@type": "JobPosting",
            "title": "GTM Engineer",
            "url": "https://wellfound.com/jobs/1111-gtm-engineer",
            "hiringOrganization": {"@type": "Organization", "name": "Acme"}
          },
          {
            "@type": "JobPosting",
            "title": "GTM Engineer (copy)",
            "url": "https://wellfound.com/jobs/1111-gtm-engineer",
            "hiringOrganization": {"@type": "Organization", "name": "Acme"}
          }
        ]
        </script>
        """
        client = _make_http_mock(html)
        source = WellfoundSource(http=client, sleep_seconds=0)
        jobs = source.fetch(target_titles=["GTM Engineer"])
        urls = [j["url"] for j in jobs]
        assert len(urls) == len(set(urls)), "Duplicate URLs found in fetch() result"

    def test_fetch_graceful_on_network_error(self) -> None:
        """httpx raises → returns [], no crash."""
        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = httpx.RequestError("connection refused")
        source = WellfoundSource(http=client, sleep_seconds=0)
        result = source.fetch(target_titles=["GTM Engineer"])
        assert result == []

    def test_fetch_graceful_on_non_200(self) -> None:
        """Non-200 response → returns [], no crash."""
        source = WellfoundSource(http=_make_http_mock("", status_code=403), sleep_seconds=0)
        result = source.fetch(target_titles=["GTM Engineer"])
        assert result == []

    def test_fetch_rate_limited_retry(self) -> None:
        """On 429, _search sleeps 3s and retries once."""
        resp_429 = MagicMock()
        resp_429.status_code = 429
        resp_429.text = ""

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.text = SAMPLE_HTML

        client = MagicMock(spec=httpx.Client)
        client.get.side_effect = [resp_429, resp_200]

        with patch("opportunities_engine.ingestion.wellfound.time.sleep") as mock_sleep:
            source = WellfoundSource(http=client, sleep_seconds=0)
            # _search is called once for one title; it will 429 then retry
            html = source._search("GTM Engineer")

        # Should have slept 3 s for the 429 backoff
        mock_sleep.assert_called_once_with(3)
        assert client.get.call_count == 2

    def test_fetch_sleep_seconds_respected_between_calls(self) -> None:
        """sleep_seconds is passed to time.sleep between successive queries."""
        client = _make_http_mock(SAMPLE_HTML)

        with patch("opportunities_engine.ingestion.wellfound.time.sleep") as mock_sleep:
            source = WellfoundSource(http=client, sleep_seconds=1.5)
            source.fetch(target_titles=["GTM Engineer", "Growth Engineer"])

        # sleep should be called once between the two queries (not before the first)
        mock_sleep.assert_called_once_with(1.5)

    def test_fetch_returns_normalized_dicts(self) -> None:
        """fetch() returns dicts with the required canonical fields."""
        source = WellfoundSource(http=_make_http_mock(SAMPLE_HTML), sleep_seconds=0)
        jobs = source.fetch(target_titles=["GTM Engineer"])

        required_keys = {"source", "source_id", "url", "title", "company"}
        for job in jobs:
            missing = required_keys - job.keys()
            assert not missing, f"Job missing keys {missing}: {job}"
            assert job["source"] == "wellfound"


# ---------------------------------------------------------------------------
# _extract_job_id unit tests
# ---------------------------------------------------------------------------


class TestExtractJobId:
    def test_numeric_id_from_path(self) -> None:
        assert _extract_job_id("https://wellfound.com/jobs/1234567-senior-gtm") == "1234567"

    def test_no_match_returns_empty_string(self) -> None:
        assert _extract_job_id("https://wellfound.com/company/acme") == ""

    def test_short_id(self) -> None:
        assert _extract_job_id("/jobs/42-role") == "42"


# ---------------------------------------------------------------------------
# _infer_remote unit tests
# ---------------------------------------------------------------------------


class TestInferRemote:
    def test_remote_in_location(self) -> None:
        assert _infer_remote("Remote, USA") is True

    def test_remote_in_title(self) -> None:
        assert _infer_remote(None, "GTM Engineer (Remote)") is True

    def test_no_remote_indicator(self) -> None:
        assert _infer_remote("San Francisco, CA", "GTM Engineer") is None

    def test_none_inputs(self) -> None:
        assert _infer_remote(None, None) is None
