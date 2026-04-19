"""Tests for the 'Why Interesting' framing module — TDD GREEN phase."""
import json
import pytest

from opportunities_engine.framing.why_interesting import (
    generate_why_interesting,
    load_dream_companies,
    detect_dream_company,
    detect_founding_role,
    match_skills,
)


# ── Fixtures ──────────────────────────────────────────────

@pytest.fixture
def dream_data(tmp_path):
    """Write a minimal dream_companies.json for testing."""
    data = {
        "_meta": {},
        "companies": [
            {"name": "Vercel", "priority": "tier_1", "why_i_love_this": "Dev-first, incredible design.",
             "attraction_types": ["design_aesthetic", "product_love"]},
            {"name": "PostHog", "priority": "tier_1", "why_i_love_this": "Weird in the best way.",
             "attraction_types": ["team_culture"]},
            {"name": "Stripe", "priority": "tier_2", "why_i_love_this": "API gold standard.",
             "attraction_types": ["innovation_tech"]},
        ],
    }
    p = tmp_path / "dream_companies.json"
    p.write_text(json.dumps(data))
    return str(p)


@pytest.fixture
def job_gtm_vercel():
    """A GTM Engineer role at a dream company."""
    return {
        "title": "GTM Engineer",
        "company": "Vercel",
        "url": "https://vercel.com/careers/123",
        "source": "greenhouse",
        "is_remote": True,
        "location": "Remote - US",
        "similarity": 0.39,
    }


@pytest.fixture
def job_founding_solutions():
    """A founding solutions engineer role at a non-dream company."""
    return {
        "title": "Founding Solutions Engineer",
        "company": "Acme Corp",
        "url": "https://acme.com/jobs/456",
        "source": "lever",
        "is_remote": False,
        "location": "San Francisco, CA",
        "similarity": 0.28,
    }


@pytest.fixture
def job_revops():
    """A RevOps role — adjacent but not core GTM."""
    return {
        "title": "RevOps Engineer",
        "company": "BoringCo",
        "url": "https://boringco.com/jobs/789",
        "source": "jobspy",
        "is_remote": True,
        "location": "Remote",
        "similarity": 0.19,
    }


@pytest.fixture
def job_with_python_description():
    """A job with Python/ETL in the description for skill matching."""
    return {
        "title": "Growth Engineer",
        "company": "StartupXYZ",
        "url": "https://startupxyz.com/jobs/1",
        "source": "greenhouse",
        "is_remote": True,
        "location": "Remote",
        "similarity": 0.30,
        "description": "We need someone proficient in Python, ETL pipelines, and AI agents to build our growth engine.",
    }


# ── load_dream_companies ─────────────────────────────────

def test_load_dream_companies_parses_file(dream_data):
    result = load_dream_companies(dream_data)
    assert len(result) == 3
    assert result[0]["name"] == "Vercel"


def test_load_dream_companies_missing_file(tmp_path):
    result = load_dream_companies(str(tmp_path / "nonexistent.json"))
    assert result == []


# ── detect_dream_company ─────────────────────────────────

def test_detect_dream_company_true(dream_data):
    companies = load_dream_companies(dream_data)
    match = detect_dream_company("Vercel", companies)
    assert match is not None
    assert match["name"] == "Vercel"
    assert match["priority"] == "tier_1"


def test_detect_dream_company_false(dream_data):
    companies = load_dream_companies(dream_data)
    assert detect_dream_company("UnknownCo", companies) is None


def test_detect_dream_company_case_insensitive(dream_data):
    companies = load_dream_companies(dream_data)
    assert detect_dream_company("vercel", companies) is not None


# ── detect_founding_role ─────────────────────────────────

def test_is_founding_explicit():
    assert detect_founding_role("Founding GTM Engineer") is True


def test_is_founding_false():
    assert detect_founding_role("Senior Solutions Engineer") is False


def test_is_founding_founding_sales():
    assert detect_founding_role("Founding Sales Engineer") is True


# ── match_skills ─────────────────────────────────────────

def test_match_skills_python():
    result = match_skills("Must know Python and ETL pipelines")
    assert "Python" in result
    assert "ETL" in result


def test_match_skills_ai_agents():
    result = match_skills("Experience with AI agents and Claude Code")
    assert "AI agents" in result
    assert "Claude Code" in result


def test_match_skills_none():
    assert match_skills(None) == []


def test_match_skills_no_match():
    assert match_skills("Must know Java and Spring Boot") == []


# ── generate_why_interesting — integration ───────────────

def test_generate_dream_company_blurb(dream_data, job_gtm_vercel):
    blurb = generate_why_interesting(job_gtm_vercel, dream_data)
    assert "Vercel" in blurb
    assert "dream" in blurb.lower()
    assert "Mixmax" in blurb  # Experience mapping


def test_generate_founding_blurb(dream_data, job_founding_solutions):
    blurb = generate_why_interesting(job_founding_solutions, dream_data)
    assert "founding" in blurb.lower() or "early" in blurb.lower()
    assert "Mixmax" in blurb


def test_generate_revops_blurb(dream_data, job_revops):
    blurb = generate_why_interesting(job_revops, dream_data)
    assert "BoringCo" in blurb
    assert len(blurb) > 20


def test_generate_without_dream_data(job_gtm_vercel, tmp_path):
    empty = tmp_path / "empty_dreams.json"
    empty.write_text('{"_meta": {}, "companies": []}')
    blurb = generate_why_interesting(job_gtm_vercel, str(empty))
    assert "Vercel" in blurb
    assert "Mixmax" in blurb


def test_blurb_not_too_long(dream_data, job_gtm_vercel):
    blurb = generate_why_interesting(job_gtm_vercel, dream_data)
    assert len(blurb) <= 500  # Concise — Linear card, not an essay


def test_skill_matches_in_description(dream_data, job_with_python_description):
    blurb = generate_why_interesting(job_with_python_description, dream_data)
    assert "Python" in blurb
    assert "ETL" in blurb or "AI agents" in blurb


def test_tier_1_dream_label(dream_data, job_gtm_vercel):
    blurb = generate_why_interesting(job_gtm_vercel, dream_data)
    assert "tier_1" in blurb  # Should show the tier


def test_tier_2_dream_company(dream_data):
    job = {
        "title": "Technical Solutions Engineer",
        "company": "Stripe",
        "url": "https://stripe.com/jobs/1",
        "source": "greenhouse",
        "similarity": 0.25,
    }
    blurb = generate_why_interesting(job, dream_data)
    assert "dream" in blurb.lower()
    assert "tier_2" in blurb
