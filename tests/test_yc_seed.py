"""Tests for the YC-batch company seeding module.

These verify:
* Slug-candidate generation heuristics (name + website → list of plausible ATS slugs)
* YC public API parsing (fake httpx transport)
* ATS probe handles 200 / 404 / 429 gracefully and returns verified hits
* Merge-into-seed is additive, case-insensitive dedup, idempotent across runs
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from opportunities_engine.ingestion.yc_seed import (
    candidate_slugs,
    fetch_yc_batch,
    merge_into_seed,
    probe_company_ats,
)


# ---------------------------------------------------------------------------
# Helpers — fake httpx transport
# ---------------------------------------------------------------------------

def _mock_transport(responses: dict[str, tuple[int, Any]]) -> httpx.MockTransport:
    """Build a MockTransport from a dict of {url_substring: (status, json_payload_or_text)}."""

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
# candidate_slugs
# ---------------------------------------------------------------------------

class TestCandidateSlugs:
    def test_simple_name(self):
        slugs = candidate_slugs("Acme", "https://acme.com")
        assert "acme" in slugs

    def test_multiword_collapsed_and_hyphenated(self):
        slugs = candidate_slugs("Scale AI", "https://scale.com")
        # should produce both collapsed ("scaleai") and hyphenated ("scale-ai") plus domain stem ("scale")
        assert "scaleai" in slugs
        assert "scale-ai" in slugs
        assert "scale" in slugs

    def test_strips_punctuation_and_lowercases(self):
        slugs = candidate_slugs("Foo.bar, Inc!", "https://foo-bar.io")
        assert "foobar" in slugs or "foo-bar" in slugs
        assert "foo-bar" in slugs  # from domain

    def test_domain_stem_from_www_and_subdomain(self):
        slugs = candidate_slugs("Example", "https://www.example.ai")
        assert "example" in slugs

    def test_empty_website_still_returns_name_variants(self):
        slugs = candidate_slugs("PostHog", "")
        assert "posthog" in slugs

    def test_deduped_and_nonempty(self):
        slugs = candidate_slugs("Acme", "https://acme.com")
        assert len(slugs) == len(set(slugs))
        assert all(s for s in slugs)


# ---------------------------------------------------------------------------
# fetch_yc_batch
# ---------------------------------------------------------------------------

class TestFetchYcBatch:
    def test_parses_single_page(self):
        payload = {
            "companies": [
                {"name": "Trim", "slug": "trim", "website": "https://trimresearch.com", "batch": "W25"},
                {"name": "Foo AI", "slug": "foo-ai", "website": "https://foo.ai", "batch": "W25"},
            ],
            "page": 1,
            "totalPages": 1,
        }
        transport = _mock_transport({"api.ycombinator.com": (200, payload)})
        client = httpx.Client(transport=transport, timeout=5)

        companies = fetch_yc_batch("W25", http=client)
        assert len(companies) == 2
        assert companies[0]["name"] == "Trim"
        assert companies[0]["batch"] == "W25"
        assert companies[1]["website"] == "https://foo.ai"

    def test_handles_404_gracefully(self):
        transport = _mock_transport({"api.ycombinator.com": (404, "")})
        client = httpx.Client(transport=transport, timeout=5)

        companies = fetch_yc_batch("X99", http=client)
        assert companies == []

    def test_paginates(self):
        """When totalPages>1, the fetcher should request subsequent pages."""
        call_count = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            call_count["n"] += 1
            page = int(request.url.params.get("page", "1"))
            total = 2
            return httpx.Response(200, json={
                "companies": [
                    {"name": f"Co{page}", "slug": f"co{page}", "website": f"https://co{page}.com", "batch": "W25"}
                ],
                "page": page,
                "totalPages": total,
            })

        transport = httpx.MockTransport(handler)
        client = httpx.Client(transport=transport, timeout=5)

        companies = fetch_yc_batch("W25", http=client)
        assert len(companies) == 2
        assert call_count["n"] == 2
        assert {c["name"] for c in companies} == {"Co1", "Co2"}


# ---------------------------------------------------------------------------
# probe_company_ats
# ---------------------------------------------------------------------------

class TestProbeCompanyAts:
    def test_finds_greenhouse_hit(self):
        """A 200 on greenhouse with jobs returns a verified entry."""
        responses = {
            "boards-api.greenhouse.io/v1/boards/acme/jobs": (200, {"jobs": [{"id": 1, "title": "SE"}]}),
        }
        transport = _mock_transport(responses)
        client = httpx.Client(transport=transport, timeout=5)

        hit = probe_company_ats("Acme", "https://acme.com", http=client)
        assert hit is not None
        assert hit["ats_platform"] == "greenhouse"
        assert hit["ats_slug"] == "acme"
        assert hit["job_count"] >= 1
        assert hit["ats_slug_verified"] is True

    def test_falls_through_to_lever(self):
        """Greenhouse 404 → then Lever 200 → lever hit returned."""
        responses = {
            "boards-api.greenhouse.io": (404, ""),
            "api.lever.co/v0/postings/acme": (200, [{"id": "x", "text": "SE", "categories": {}}]),
        }
        transport = _mock_transport(responses)
        client = httpx.Client(transport=transport, timeout=5)

        hit = probe_company_ats("Acme", "https://acme.com", http=client)
        assert hit is not None
        assert hit["ats_platform"] == "lever"

    def test_handles_429_then_no_hit(self):
        """429 on every platform → no hit, no crash."""
        responses = {
            "boards-api.greenhouse.io": (429, ""),
            "api.lever.co": (429, ""),
            "api.ashbyhq.com": (429, ""),
        }
        transport = _mock_transport(responses)
        client = httpx.Client(transport=transport, timeout=5)

        hit = probe_company_ats("Acme", "https://acme.com", http=client)
        assert hit is None

    def test_skips_zero_job_boards(self):
        """A 200 with zero jobs is not a strong signal — skip it."""
        responses = {
            "boards-api.greenhouse.io/v1/boards/acme/jobs": (200, {"jobs": []}),
            "api.lever.co": (404, ""),
            "api.ashbyhq.com": (404, ""),
        }
        transport = _mock_transport(responses)
        client = httpx.Client(transport=transport, timeout=5)

        hit = probe_company_ats("Acme", "https://acme.com", http=client)
        assert hit is None


# ---------------------------------------------------------------------------
# merge_into_seed
# ---------------------------------------------------------------------------

class TestMergeIntoSeed:
    def test_appends_new_entries(self, tmp_path: Path):
        seed = tmp_path / "seed.json"
        seed.write_text(json.dumps({
            "_meta": {"total": 1, "verified_count": 1},
            "companies": [
                {"name": "Existing", "website": "https://existing.com",
                 "ats_platform": "greenhouse", "ats_slug": "existing",
                 "ats_slug_verified": True, "job_count": 5,
                 "industry": "unknown", "is_dream_company": False, "notes": ""}
            ],
        }, indent=2))

        new = [{
            "name": "NewCo", "website": "https://newco.com",
            "ats_platform": "lever", "ats_slug": "newco",
            "ats_slug_verified": True, "job_count": 3,
            "industry": "yc-w25", "is_dream_company": False,
            "notes": "Discovered via YC W25 seeder",
        }]

        added = merge_into_seed(seed, new)
        assert added == 1

        data = json.loads(seed.read_text())
        names = [c["name"] for c in data["companies"]]
        assert "NewCo" in names
        assert "Existing" in names
        assert data["_meta"]["total"] == 2
        assert data["_meta"]["verified_count"] == 2

    def test_idempotent(self, tmp_path: Path):
        """Running merge twice with the same input adds nothing the second time."""
        seed = tmp_path / "seed.json"
        seed.write_text(json.dumps({"_meta": {}, "companies": []}))

        new = [{
            "name": "NewCo", "website": "https://newco.com",
            "ats_platform": "lever", "ats_slug": "newco",
            "ats_slug_verified": True, "job_count": 3,
            "industry": "yc-w25", "is_dream_company": False, "notes": "",
        }]

        first = merge_into_seed(seed, new)
        second = merge_into_seed(seed, new)
        assert first == 1
        assert second == 0

        data = json.loads(seed.read_text())
        assert len(data["companies"]) == 1

    def test_dedup_case_insensitive(self, tmp_path: Path):
        seed = tmp_path / "seed.json"
        seed.write_text(json.dumps({
            "_meta": {},
            "companies": [{"name": "Vercel", "ats_platform": "greenhouse",
                           "ats_slug": "vercel", "ats_slug_verified": True, "job_count": 80}],
        }))
        new = [{
            "name": "vercel", "website": "https://vercel.com",
            "ats_platform": "greenhouse", "ats_slug": "vercel",
            "ats_slug_verified": True, "job_count": 80, "notes": "",
        }]
        added = merge_into_seed(seed, new)
        assert added == 0

    def test_creates_file_if_missing(self, tmp_path: Path):
        seed = tmp_path / "seed.json"
        assert not seed.exists()

        new = [{
            "name": "NewCo", "website": "https://newco.com",
            "ats_platform": "lever", "ats_slug": "newco",
            "ats_slug_verified": True, "job_count": 3, "notes": "",
        }]
        added = merge_into_seed(seed, new)
        assert added == 1
        assert seed.exists()
        data = json.loads(seed.read_text())
        assert len(data["companies"]) == 1
