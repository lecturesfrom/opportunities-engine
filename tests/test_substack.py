"""Tests for the Substack hiring-roundup scraper (SubstackSource).

Uses mocked HTTP and Anthropic clients — no real network calls are made.
All test patterns match test_wellfound.py style.
"""

from __future__ import annotations

import base64
import hashlib
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from opportunities_engine.ingestion.substack import (
    SubstackPost,
    SubstackSource,
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


def _make_http_mock_from_feed(xml: str, status_code: int = 200) -> MagicMock:
    """Build a mocked httpx.Client that returns the given RSS XML."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = xml
    resp.raise_for_status = MagicMock()
    client = MagicMock(spec=httpx.Client)
    client.get.return_value = resp
    return client


def _make_minimal_rss_with_items(
    items: list[dict],
) -> str:
    """Build minimal RSS XML with the given items.

    Each item dict should have keys: post_url, title, published, html_body (optional).
    """
    items_xml = ""
    for item in items:
        body = item.get("html_body", "")
        items_xml += f"""
        <item>
            <link>{item.get("post_url", "")}</link>
            <title>{item.get("title", "")}</title>
            <pubDate>{item.get("published", "")}</pubDate>
            <content:encoded><![CDATA[{body}]]></content:encoded>
        </item>
        """

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">
    <channel>
        {items_xml}
    </channel>
</rss>
"""


# ---------------------------------------------------------------------------
# _fetch_feed tests
# ---------------------------------------------------------------------------


class TestFetchFeed:
    def test_fetch_feed_returns_substackpost_list(self) -> None:
        """_fetch_feed GET {feed_url}/feed → returns list[SubstackPost]."""
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring Roundup #1",
                "published": "2024-01-01T12:00:00Z",
                "html_body": "<p>Some companies hiring</p>",
            }
        ]
        rss = _make_minimal_rss_with_items(items)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=_make_http_mock_from_feed(rss),
        )
        posts = source._fetch_feed("https://newsletter.substack.com")

        assert len(posts) == 1
        assert posts[0].post_url == "https://newsletter.substack.com/p/hiring-1"
        assert posts[0].title == "Hiring Roundup #1"
        assert posts[0].published == "2024-01-01T12:00:00Z"
        assert posts[0].html_body == "<p>Some companies hiring</p>"

    def test_fetch_feed_caps_images_per_post(self) -> None:
        """An item with 20 <img> tags → only max_images_per_post (default 10) retained."""
        img_tags = "".join(
            [f'<img src="https://example.com/img{i}.png" />' for i in range(20)]
        )
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/images",
                "title": "Image-rich post",
                "published": "2024-01-01T12:00:00Z",
                "html_body": f"<div>{img_tags}</div>",
            }
        ]
        rss = _make_minimal_rss_with_items(items)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=_make_http_mock_from_feed(rss),
            max_images_per_post=10,
        )
        posts = source._fetch_feed("https://newsletter.substack.com")

        assert len(posts) == 1
        assert len(posts[0].image_urls) == 10
        # Verify the first 10 are present
        assert "https://example.com/img0.png" in posts[0].image_urls
        assert "https://example.com/img9.png" in posts[0].image_urls
        # And the 11th is not present
        assert "https://example.com/img10.png" not in posts[0].image_urls

    def test_fetch_feed_multiple_items(self) -> None:
        """_fetch_feed with 2 items → returns 2 SubstackPost objects."""
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring Roundup #1",
                "published": "2024-01-01T12:00:00Z",
                "html_body": "<p>Acme hiring</p>",
            },
            {
                "post_url": "https://newsletter.substack.com/p/hiring-2",
                "title": "Hiring Roundup #2",
                "published": "2024-01-08T12:00:00Z",
                "html_body": "<p>Bolt hiring</p>",
            },
        ]
        rss = _make_minimal_rss_with_items(items)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=_make_http_mock_from_feed(rss),
        )
        posts = source._fetch_feed("https://newsletter.substack.com")

        assert len(posts) == 2
        assert posts[0].title == "Hiring Roundup #1"
        assert posts[1].title == "Hiring Roundup #2"


# ---------------------------------------------------------------------------
# _parse_text_body tests
# ---------------------------------------------------------------------------


class TestParseTextBody:
    def test_em_dash_pattern_single_entry(self) -> None:
        """Pattern: 'Acme — Senior GTM Engineer — https://boards.greenhouse.io/acme/jobs/123'
        → one extracted entry."""
        post = SubstackPost(
            post_url="https://newsletter.substack.com/p/hiring",
            title="Hiring",
            published="2024-01-01T12:00:00Z",
            html_body="Acme — Senior GTM Engineer — https://boards.greenhouse.io/acme/jobs/123",
        )
        source = SubstackSource(feeds=[])
        results = source._parse_text_body(post)

        assert len(results) == 1
        assert results[0]["company"] == "Acme"
        assert results[0]["title"] == "Senior GTM Engineer"
        assert results[0]["url"] == "https://boards.greenhouse.io/acme/jobs/123"

    def test_markdown_link_pattern(self) -> None:
        """Pattern: '[Acme — GTM Engineer](https://boards.greenhouse.io/acme/jobs/123)'
        → one extracted entry."""
        post = SubstackPost(
            post_url="https://newsletter.substack.com/p/hiring",
            title="Hiring",
            published="2024-01-01T12:00:00Z",
            html_body="[Acme — GTM Engineer](https://boards.greenhouse.io/acme/jobs/123)",
        )
        source = SubstackSource(feeds=[])
        results = source._parse_text_body(post)

        assert len(results) == 1
        assert results[0]["company"] == "Acme"
        assert results[0]["title"] == "GTM Engineer"
        assert results[0]["url"] == "https://boards.greenhouse.io/acme/jobs/123"

    def test_dedupe_within_post_by_url(self) -> None:
        """Same URL appears twice in a post → dedupe, only one extracted."""
        post = SubstackPost(
            post_url="https://newsletter.substack.com/p/hiring",
            title="Hiring",
            published="2024-01-01T12:00:00Z",
            html_body=(
                "Acme — Senior GTM Engineer — https://boards.greenhouse.io/acme/jobs/123\n"
                "Acme — Another Role — https://boards.greenhouse.io/acme/jobs/123"
            ),
        )
        source = SubstackSource(feeds=[])
        results = source._parse_text_body(post)

        assert len(results) == 1

    def test_non_job_urls_skipped(self) -> None:
        """Non-ATS domain URLs are skipped; ATS domain URLs are extracted via Pattern 3."""
        post = SubstackPost(
            post_url="https://newsletter.substack.com/p/hiring",
            title="Hiring",
            published="2024-01-01T12:00:00Z",
            html_body=(
                "Check out https://twitter.com/someone\n"
                "Or read https://another.substack.com/p/article\n"
                "Acme — GTM Engineer — https://boards.greenhouse.io/acme/jobs/123"
            ),
        )
        source = SubstackSource(feeds=[])
        results = source._parse_text_body(post)

        # Only the line with ATS domain URL should produce an entry
        assert len(results) == 1
        assert results[0]["company"] == "Acme"
        assert results[0]["title"] == "GTM Engineer"


# ---------------------------------------------------------------------------
# _call_vision tests
# ---------------------------------------------------------------------------


class TestCallVision:
    def test_call_vision_parses_json_array(self) -> None:
        """_call_vision mocks Anthropic → returns parsed list of dicts."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps(
            [
                {
                    "company": "Acme",
                    "title": "Senior GTM Engineer",
                    "url": "https://boards.greenhouse.io/acme/jobs/123",
                    "location": "Remote",
                }
            ]
        )
        mock_client.messages.create.return_value = mock_response

        source = SubstackSource(feeds=[], anthropic_client=mock_client)
        result = source._call_vision(b"fake image bytes", "hash123", "https://newsletter.substack.com/p/hiring")

        assert len(result) == 1
        assert result[0]["company"] == "Acme"
        assert result[0]["title"] == "Senior GTM Engineer"

    def test_call_vision_handles_garbage_json(self) -> None:
        """_call_vision returns garbage non-JSON → returns [] without raising."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = "This is not valid JSON!!!"
        mock_client.messages.create.return_value = mock_response

        source = SubstackSource(feeds=[], anthropic_client=mock_client)
        result = source._call_vision(b"fake image bytes", "hash123", "https://newsletter.substack.com/p/hiring")

        assert result == []

    def test_call_vision_empty_feeds_no_client_instantiation(self) -> None:
        """With empty feeds=[] and no anthropic_client, Anthropic client is never used."""
        source = SubstackSource(feeds=[])
        # _call_vision should only check if client is available and api_key is set
        # Since we didn't inject a client and settings.anthropic_api_key is likely None,
        # this should return [] without trying to instantiate
        result = source._call_vision(b"fake image bytes", "hash123", "https://newsletter.substack.com/p/hiring")

        # Verify no exception is raised
        assert isinstance(result, list)

    def test_call_vision_returns_empty_on_non_list_response(self) -> None:
        """Vision returns a dict instead of list → returns [] without raising."""
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps({"company": "Acme"})  # dict, not list
        mock_client.messages.create.return_value = mock_response

        source = SubstackSource(feeds=[], anthropic_client=mock_client)
        result = source._call_vision(b"fake image bytes", "hash123", "https://newsletter.substack.com/p/hiring")

        assert result == []


# ---------------------------------------------------------------------------
# Cache tests
# ---------------------------------------------------------------------------


class TestCache:
    def test_cache_round_trip(self, tmp_path: Path) -> None:
        """First fetch() populates cache; second fetch() with same post skips it."""
        cache_path = tmp_path / "substack-cache.json"

        # Create a minimal RSS with one item
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring #1",
                "published": "2024-01-01T12:00:00Z",
                "html_body": "Acme — Senior GTM Engineer — https://boards.greenhouse.io/acme/jobs/123",
            }
        ]
        rss = _make_minimal_rss_with_items(items)
        http_mock = _make_http_mock_from_feed(rss)

        # First fetch
        source1 = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=http_mock,
            cache_path=cache_path,
        )
        jobs1 = source1.fetch()
        assert len(jobs1) == 1

        # Verify cache was written
        assert cache_path.exists()
        cache_content = json.loads(cache_path.read_text())
        assert "https://newsletter.substack.com/p/hiring-1" in cache_content.get("posts", {})

        # Second fetch with same HTTP mock
        http_mock2 = _make_http_mock_from_feed(rss)
        source2 = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=http_mock2,
            cache_path=cache_path,
        )
        # Mock _parse_text_body to track if it's called — it shouldn't be for cached posts
        original_parse = source2._parse_text_body
        call_count = {"text": 0}

        def mock_parse(post: SubstackPost) -> list[dict]:
            call_count["text"] += 1
            return original_parse(post)

        source2._parse_text_body = mock_parse

        jobs2 = source2.fetch()
        # The post is cached, so _parse_text_body should not be called
        assert call_count["text"] == 0
        assert jobs2 == []

    def test_cache_image_hash_dedup(self, tmp_path: Path) -> None:
        """Two posts with the same image URL → vision only called once."""
        cache_path = tmp_path / "substack-cache.json"

        # Create two items with same image URL
        img_url = "https://example.com/hiring-list.png"
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring #1",
                "published": "2024-01-01T12:00:00Z",
                "html_body": f'<img src="{img_url}" />',
            },
            {
                "post_url": "https://newsletter.substack.com/p/hiring-2",
                "title": "Hiring #2",
                "published": "2024-01-08T12:00:00Z",
                "html_body": f'<img src="{img_url}" />',
            },
        ]
        rss = _make_minimal_rss_with_items(items)

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock()]
        mock_response.content[0].text = json.dumps(
            [{"company": "Acme", "title": "GTM Engineer", "url": "https://greenhouse.io/acme/123", "location": None}]
        )
        mock_client.messages.create.return_value = mock_response

        # Mock image download
        def mock_download(url: str) -> bytes | None:
            return b"fake image data"

        http_mock = _make_http_mock_from_feed(rss)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=http_mock,
            cache_path=cache_path,
            anthropic_client=mock_client,
        )
        source._download_image = mock_download

        jobs = source.fetch()

        # Vision should be called only once (second item uses cache)
        assert mock_client.messages.create.call_count == 1

    def test_cache_malformed_json_starts_fresh(self, tmp_path: Path) -> None:
        """Malformed cache JSON on disk → _load_cache starts fresh."""
        cache_path = tmp_path / "substack-cache.json"
        cache_path.write_text("{ broken json")

        source = SubstackSource(
            feeds=[],
            cache_path=cache_path,
        )

        # Should load successfully with empty cache
        assert source._cache == {}


# ---------------------------------------------------------------------------
# _normalize tests
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_normalize_requires_company(self) -> None:
        """Entry without company → _normalize returns None."""
        source = SubstackSource(feeds=[])
        entry = {
            "title": "Senior GTM Engineer",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
            "location": "Remote",
        }
        result = source._normalize(entry, "https://newsletter.substack.com/p/hiring")
        assert result is None

    def test_normalize_sets_source_substack(self) -> None:
        """Normalized dict always has source='substack'."""
        source = SubstackSource(feeds=[])
        entry = {
            "company": "Acme",
            "title": "Senior GTM Engineer",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
        }
        result = source._normalize(entry, "https://newsletter.substack.com/p/hiring")
        assert result is not None
        assert result["source"] == "substack"

    def test_normalize_sets_post_url_metadata(self) -> None:
        """Normalized dict has metadata.post_url set."""
        source = SubstackSource(feeds=[])
        entry = {
            "company": "Acme",
            "title": "Senior GTM Engineer",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
        }
        post_url = "https://newsletter.substack.com/p/hiring"
        result = source._normalize(entry, post_url)
        assert result is not None
        assert result["metadata"]["post_url"] == post_url

    def test_normalize_infers_is_remote_from_location(self) -> None:
        """Location contains 'remote' → is_remote=True."""
        source = SubstackSource(feeds=[])
        entry = {
            "company": "Acme",
            "title": "Senior GTM Engineer",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
            "location": "Remote, USA",
        }
        result = source._normalize(entry, "https://newsletter.substack.com/p/hiring")
        assert result is not None
        assert result["is_remote"] is True

    def test_normalize_infers_is_remote_from_title(self) -> None:
        """Title contains 'remote' → is_remote=True."""
        source = SubstackSource(feeds=[])
        entry = {
            "company": "Acme",
            "title": "Senior GTM Engineer (Remote)",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
        }
        result = source._normalize(entry, "https://newsletter.substack.com/p/hiring")
        assert result is not None
        assert result["is_remote"] is True

    def test_normalize_is_remote_none_when_not_found(self) -> None:
        """No 'remote' indicator → is_remote=None."""
        source = SubstackSource(feeds=[])
        entry = {
            "company": "Acme",
            "title": "Senior GTM Engineer",
            "url": "https://boards.greenhouse.io/acme/jobs/123",
            "location": "San Francisco, CA",
        }
        result = source._normalize(entry, "https://newsletter.substack.com/p/hiring")
        assert result is not None
        assert result["is_remote"] is None


# ---------------------------------------------------------------------------
# _dedupe tests
# ---------------------------------------------------------------------------


class TestDedupe:
    def test_dedupe_via_canonical_helpers(self) -> None:
        """Two entries with different case/format → dedupe returns one."""
        source = SubstackSource(feeds=[])
        jobs = [
            {
                "source": "substack",
                "company": "Acme, Inc.",
                "title": "Senior Software Engineer",
                "url": "https://boards.greenhouse.io/acme/jobs/123",
            },
            {
                "source": "substack",
                "company": "Acme",
                "title": "Sr SWE",
                "url": "https://boards.greenhouse.io/acme/jobs/456",
            },
        ]
        result = source._dedupe(jobs)

        # Should dedupe to one entry (canonical normalization)
        assert len(result) == 1

    def test_dedupe_preserves_first_occurrence(self) -> None:
        """When deduping, first occurrence is kept."""
        source = SubstackSource(feeds=[])
        jobs = [
            {
                "source": "substack",
                "company": "Acme",
                "title": "Senior Software Engineer",
                "url": "https://boards.greenhouse.io/acme/jobs/123",
                "location": "Remote",
            },
            {
                "source": "substack",
                "company": "Acme",
                "title": "Senior Software Engineer",
                "url": "https://boards.greenhouse.io/acme/jobs/456",
                "location": "San Francisco",
            },
        ]
        result = source._dedupe(jobs)

        assert len(result) == 1
        assert result[0]["location"] == "Remote"


# ---------------------------------------------------------------------------
# Graceful failure modes
# ---------------------------------------------------------------------------


class TestGracefulFailures:
    def test_no_api_key_returns_empty(self, tmp_path: Path) -> None:
        """anthropic_api_key is None → image parsing skips, returns []."""
        # Patch settings to ensure api_key is None
        with patch("opportunities_engine.ingestion.substack.settings") as mock_settings:
            mock_settings.anthropic_api_key = None

            source = SubstackSource(feeds=[], cache_path=tmp_path / "cache.json")
            result = source._call_vision(b"fake image", "hash123", "https://newsletter.substack.com/p/hiring")

            assert result == []

    def test_network_error_graceful_fetch(self) -> None:
        """httpx.Client.get raises → _fetch_feed returns [] without raising."""
        http_mock = MagicMock(spec=httpx.Client)
        http_mock.get.side_effect = httpx.RequestError("connection refused")

        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=http_mock,
        )

        # Should not raise
        result = source.fetch()
        assert result == []

    def test_fetch_malformed_xml_graceful(self) -> None:
        """Malformed XML in feed → _fetch_feed returns [] without raising."""
        http_mock = _make_http_mock_from_feed("<broken>xml>")
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=http_mock,
        )

        result = source._fetch_feed("https://newsletter.substack.com")
        assert result == []


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


class TestFetchIntegration:
    def test_fetch_end_to_end_text_parsing(self, tmp_path: Path) -> None:
        """Full fetch() with text body → normalized jobs returned."""
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring Roundup #1",
                "published": "2024-01-01T12:00:00Z",
                "html_body": "Acme — Senior GTM Engineer — https://boards.greenhouse.io/acme/jobs/123",
            }
        ]
        rss = _make_minimal_rss_with_items(items)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=_make_http_mock_from_feed(rss),
            cache_path=tmp_path / "cache.json",
        )

        jobs = source.fetch()

        assert len(jobs) == 1
        assert jobs[0]["source"] == "substack"
        assert jobs[0]["company"] == "Acme"
        assert jobs[0]["title"] == "Senior GTM Engineer"
        assert jobs[0]["metadata"]["post_url"] == "https://newsletter.substack.com/p/hiring-1"

    def test_fetch_normalizes_output(self, tmp_path: Path) -> None:
        """fetch() returns normalized dicts with required fields."""
        items = [
            {
                "post_url": "https://newsletter.substack.com/p/hiring-1",
                "title": "Hiring",
                "published": "2024-01-01T12:00:00Z",
                "html_body": "TechCo — Backend Engineer — https://lever.co/techco/jobs/123",
            }
        ]
        rss = _make_minimal_rss_with_items(items)
        source = SubstackSource(
            feeds=["https://newsletter.substack.com"],
            http=_make_http_mock_from_feed(rss),
            cache_path=tmp_path / "cache.json",
        )

        jobs = source.fetch()

        required_keys = {"source", "source_id", "url", "title", "company", "metadata"}
        for job in jobs:
            missing = required_keys - job.keys()
            assert not missing, f"Job missing keys {missing}: {job}"
            assert job["source"] == "substack"
