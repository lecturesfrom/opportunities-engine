"""Tests for Hacker News 'Who is Hiring' ingestion source.

Covers:
- Monthly thread discovery via Algolia API
- Comment parsing into structured job records (title, company, location, remote, URL)
- US/remote gate filtering
- Dedup by URL + company+title
- Graceful handling of API errors / empty months
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from opportunities_engine.ingestion.hn_hiring import (
    HNHiringSource,
    find_hiring_thread,
    parse_hiring_comments,
    _extract_job_from_comment,
)


# ---------------------------------------------------------------------------
# Helpers — fake httpx transport
# ---------------------------------------------------------------------------

def _mock_transport(responses: dict[str, tuple[int, object]]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for needle, (status, payload) in responses.items():
            if needle in url:
                if isinstance(payload, (dict, list)):
                    return httpx.Response(status, json=payload)
                return httpx.Response(status, text=payload or "")
        return httpx.Response(404, text="not mocked")

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# find_hiring_thread
# ---------------------------------------------------------------------------

class TestFindHiringThread:
    def test_finds_monthly_thread(self):
        """Algolia returns a 'Ask HN: Who is Hiring?' story for the target month."""
        payload = {
            "hits": [
                {
                    "objectID": "12345",
                    "title": "Ask HN: Who is Hiring? (April 2026)",
                    "url": "",
                    "num_comments": 312,
                    "created_at": "2026-04-01T09:00:00Z",
                },
            ],
            "nbHits": 1,
        }
        transport = _mock_transport({"hn.algolia.com": (200, payload)})
        client = httpx.Client(transport=transport, timeout=5)

        result = find_hiring_thread(2026, 4, http=client)
        assert result is not None
        assert result["objectID"] == "12345"
        assert result["num_comments"] == 312

    def test_returns_none_when_no_thread(self):
        """No matching thread for a future/invalid month."""
        transport = _mock_transport({"hn.algolia.com": (200, {"hits": [], "nbHits": 0})})
        client = httpx.Client(transport=transport, timeout=5)

        result = find_hiring_thread(2099, 1, http=client)
        assert result is None

    def test_handles_api_error(self):
        """503 from Algolia returns None, no crash."""
        transport = _mock_transport({"hn.algolia.com": (503, "service unavailable")})
        client = httpx.Client(transport=transport, timeout=5)

        result = find_hiring_thread(2026, 4, http=client)
        assert result is None


# ---------------------------------------------------------------------------
# _extract_job_from_comment
# ---------------------------------------------------------------------------

class TestExtractJobFromComment:
    def test_parses_standard_format(self):
        """Typical HN hiring comment: company | location | role | URL."""
        comment = {
            "comment_text": "<p>Acme Corp | San Francisco / Remote | <p>We're hiring a Founding GTM Engineer. You'll own the entire sales pipeline. <a href=\"https://acme.com/jobs/gtm\">Apply here</a>.",
            "story_title": "Ask HN: Who is Hiring? (April 2026)",
        }
        job = _extract_job_from_comment(comment)
        assert job is not None
        assert job["company"] == "Acme Corp"
        assert "Founding GTM Engineer" in job["title"]
        assert job["url"] == "https://acme.com/jobs/gtm"
        assert job["is_remote"] is True or "Remote" in job.get("location", "")

    def test_skips_non_hiring_comments(self):
        """A top-level non-hiring comment (e.g. 'Ask HN: Freelancer?') returns None."""
        comment = {
            "comment_text": "<p>I'm looking for freelance work, not a job posting.",
            "story_title": "Ask HN: Who is Hiring? (April 2026)",
        }
        job = _extract_job_from_comment(comment)
        assert job is None

    def test_handles_no_url(self):
        """Comment with company + title but no link still extracts what it can."""
        comment = {
            "comment_text": "<p>ScaleAI | NYC | Hiring a Solutions Engineer to work on data pipelines.",
            "story_title": "Ask HN: Who is Hiring? (April 2026)",
        }
        job = _extract_job_from_comment(comment)
        assert job is not None
        assert job["company"] == "ScaleAI"
        assert "Solutions Engineer" in job["title"]
        assert job["url"] == ""

    def test_handles_remote_flag(self):
        """Remote keyword detected from location or body text."""
        comment = {
            "comment_text": "<p>PostHog | Remote (US/EU) | Forward Deployed Engineer. <a href=\"https://posthog.com/careers\">Apply</a>.",
            "story_title": "Ask HN: Who is Hiring? (April 2026)",
        }
        job = _extract_job_from_comment(comment)
        assert job is not None
        assert job["is_remote"] is True


# ---------------------------------------------------------------------------
# parse_hiring_comments
# ---------------------------------------------------------------------------

class TestParseHiringComments:
    def test_filters_to_relevant_roles(self):
        """Only comments matching GTM-adjacent title patterns are kept."""
        comments = [
            {
                "comment_text": "<p>Acme | SF | Founding GTM Engineer. <a href=\"https://a.com\">a</a>.",
                "story_title": "Ask HN: Who is Hiring? (April 2026)",
            },
            {
                "comment_text": "<p>BigCo | NYC | Senior Java Developer. <a href=\"https://b.com\">b</a>.",
                "story_title": "Ask HN: Who is Hiring? (April 2026)",
            },
            {
                "comment_text": "<p>Vercel | Remote | Solutions Engineer. <a href=\"https://v.com\">v</a>.",
                "story_title": "Ask HN: Who is Hiring? (April 2026)",
            },
        ]
        jobs = parse_hiring_comments(comments)
        companies = [j["company"] for j in jobs]
        assert "Acme" in companies
        assert "Vercel" in companies
        assert "BigCo" not in companies  # Java Developer = not GTM-adjacent

    def test_dedup_by_company_title(self):
        """Duplicate company+title combination only kept once."""
        comments = [
            {
                "comment_text": "<p>Acme | Remote | Founding GTM Engineer. <a href=\"https://a.com\">a</a>.",
                "story_title": "Ask HN: Who is Hiring? (April 2026)",
            },
            {
                "comment_text": "<p>Acme | Remote | Founding GTM Engineer. <a href=\"https://a.com\">a</a>.",
                "story_title": "Ask HN: Who is Hiring? (April 2026)",
            },
        ]
        jobs = parse_hiring_comments(comments)
        assert len(jobs) == 1


# ---------------------------------------------------------------------------
# HNHiringSource.fetch (integration-style)
# ---------------------------------------------------------------------------

class TestHNHiringSourceFetch:
    def test_fetch_returns_job_records(self):
        """End-to-end: thread discovery → comment fetch → parse → JobRecord list."""
        thread_payload = {
            "hits": [
                {
                    "objectID": "999",
                    "title": "Ask HN: Who is Hiring? (April 2026)",
                    "url": "",
                    "num_comments": 5,
                },
            ],
        }
        # Algolia item endpoint returns comments for the thread
        item_payload = {
            "children": [
                {
                    "id": 1,
                    "text": "<p>TestCo | Remote US | Sales Engineer. <a href=\"https://t.com/jobs\">Apply</a>.",
                    "author": "testuser",
                },
                {
                    "id": 2,
                    "text": "<p>IgnoreCo | NYC | Backend Developer.",
                    "author": "testuser2",
                },
            ]
        }

        transport = _mock_transport({
            "search_by_date": (200, thread_payload),
            "items/999": (200, item_payload),
        })
        client = httpx.Client(transport=transport, timeout=5)

        source = HNHiringSource()
        jobs = source.fetch(http=client)
        assert len(jobs) >= 1
        assert any(j["company"] == "TestCo" for j in jobs)

    def test_fetch_returns_empty_on_api_failure(self):
        """API failure returns empty list, no crash."""
        transport = _mock_transport({"hn.algolia.com": (503, "unavailable")})
        client = httpx.Client(transport=transport, timeout=5)

        source = HNHiringSource()
        jobs = source.fetch(http=client)
        assert jobs == []
