"""Tests for Discord digest delivery — TDD RED then GREEN phase."""
import json
import pytest
from unittest.mock import patch, MagicMock

from opportunities_engine.alerts.discord_digest import (
    format_digest,
    post_digest,
    DISCORD_MAX_LEN,
)


# ── Fixtures ──────────────────────────────────────────────

@pytest.fixture
def ranked_jobs():
    """Sample ranked jobs list for testing."""
    return [
        {
            "title": "GTM Engineer",
            "company": "Vercel",
            "url": "https://vercel.com/careers/123",
            "source": "greenhouse",
            "is_remote": True,
            "location": "Remote - US",
            "similarity": 0.39,
        },
        {
            "title": "Forward Deployed Engineer",
            "company": "Databricks",
            "url": "https://databricks.com/jobs/456",
            "source": "greenhouse",
            "is_remote": False,
            "location": "United States",
            "similarity": 0.31,
        },
        {
            "title": "RevOps Engineer",
            "company": "BoringCo",
            "url": "https://boringco.com/jobs/789",
            "source": "jobspy",
            "is_remote": True,
            "location": "Remote",
            "similarity": 0.19,
        },
    ]


@pytest.fixture
def ranked_jobs_file(tmp_path, ranked_jobs):
    """Write ranked jobs to a temp JSON file."""
    p = tmp_path / "ranked_jobs.json"
    p.write_text(json.dumps(ranked_jobs))
    return str(p)


# ── format_digest ────────────────────────────────────────

def test_format_digest_includes_count(ranked_jobs):
    msg = format_digest(ranked_jobs)
    assert "3" in msg  # Total job count
    assert "opportunit" in msg.lower()


def test_format_digest_includes_top_picks(ranked_jobs):
    msg = format_digest(ranked_jobs)
    assert "Vercel" in msg
    assert "GTM Engineer" in msg


def test_format_digest_includes_score(ranked_jobs):
    msg = format_digest(ranked_jobs)
    assert "0.39" in msg  # Top job score


def test_format_digest_includes_remote_indicator(ranked_jobs):
    msg = format_digest(ranked_jobs)
    # Remote jobs get the 🌐 emoji badge
    assert "🌐" in msg


def test_format_digest_under_discord_limit(ranked_jobs):
    msg = format_digest(ranked_jobs)
    assert len(msg) <= DISCORD_MAX_LEN


def test_format_digest_empty_jobs():
    msg = format_digest([])
    assert "no new" in msg.lower() or "0" in msg


def test_format_digest_many_jobs():
    """Ensure digest stays under Discord limit even with 50 jobs."""
    jobs = []
    for i in range(50):
        jobs.append({
            "title": f"Engineer Role {i}",
            "company": f"Company {i}",
            "url": f"https://example.com/jobs/{i}",
            "source": "greenhouse",
            "is_remote": True,
            "location": "Remote",
            "similarity": round(0.40 - i * 0.005, 3),
        })
    msg = format_digest(jobs)
    assert len(msg) <= DISCORD_MAX_LEN
    # Should still include the top 3
    assert "Company 0" in msg


def test_format_digest_uses_emojis(ranked_jobs):
    msg = format_digest(ranked_jobs)
    # Should have at least one emoji character
    has_emoji = any(ord(c) > 0x1F000 for c in msg)
    assert has_emoji


# ── post_digest ──────────────────────────────────────────

def test_post_digest_dry_run(ranked_jobs, capsys):
    post_digest(ranked_jobs, webhook_url=None, dry_run=True)
    captured = capsys.readouterr()
    assert "Vercel" in captured.out


def test_post_digest_calls_webhook(ranked_jobs):
    with patch("opportunities_engine.alerts.discord_digest._send_webhook") as mock_send:
        mock_send.return_value = {"status": "ok"}
        post_digest(ranked_jobs, webhook_url="https://discord.com/webhook/test")
        mock_send.assert_called_once()


def test_post_digest_no_webhook_no_dry_run(ranked_jobs, capsys):
    """Should warn if no webhook URL and not dry-run."""
    post_digest(ranked_jobs, webhook_url=None, dry_run=False)
    captured = capsys.readouterr()
    assert "no webhook" in captured.out.lower() or "warning" in captured.out.lower()


# ── source + URL in digest ────────────────────────────────

def test_format_digest_includes_source_name(ranked_jobs):
    """Digest message must contain the source name (e.g. 'greenhouse') for the top job."""
    msg = format_digest(ranked_jobs)
    assert "greenhouse" in msg


def test_format_digest_includes_source_url(ranked_jobs):
    """Digest message must contain the job URL for the top job."""
    msg = format_digest(ranked_jobs)
    assert "https://vercel.com/careers/123" in msg


def test_format_digest_includes_source_emoji_prefix(ranked_jobs):
    """Source line should start with the 📍 emoji."""
    msg = format_digest(ranked_jobs)
    assert "📍" in msg


def test_format_digest_includes_score_line(ranked_jobs):
    """Score should appear in the meta line (e.g. 'score 0.39')."""
    msg = format_digest(ranked_jobs)
    assert "score 0.39" in msg


def test_format_digest_includes_decision_when_present():
    """Decision field is rendered when present."""
    jobs = [
        {
            "title": "GTM Engineer",
            "company": "Vercel",
            "url": "https://vercel.com/careers/99",
            "source": "greenhouse",
            "is_remote": True,
            "similarity": 0.38,
            "decision": "promoted_whitelist_remote",
        }
    ]
    msg = format_digest(jobs)
    assert "decision: promoted_whitelist_remote" in msg


def test_format_digest_omits_score_line_when_missing():
    """When similarity is absent, the score meta line must not appear."""
    jobs = [
        {
            "title": "GTM Engineer",
            "company": "Vercel",
            "url": "https://vercel.com/careers/99",
            "source": "greenhouse",
            "is_remote": True,
            # no similarity key
        }
    ]
    msg = format_digest(jobs)
    # "score" string should not appear in the meta line area
    assert "score" not in msg


def test_format_digest_omits_decision_when_missing():
    """When decision is absent, 'decision:' must not appear in the output."""
    jobs = [
        {
            "title": "GTM Engineer",
            "company": "Vercel",
            "url": "https://vercel.com/careers/99",
            "source": "greenhouse",
            "is_remote": True,
            "similarity": 0.30,
            # no decision key
        }
    ]
    msg = format_digest(jobs)
    assert "decision:" not in msg


def test_format_digest_defaults_source_to_unknown_when_missing():
    """When source is absent, the source line falls back to 'unknown'."""
    jobs = [
        {
            "title": "GTM Engineer",
            "company": "SomeCo",
            "url": "https://someco.com/jobs/1",
            "is_remote": True,
            "similarity": 0.25,
            # no source key
        }
    ]
    msg = format_digest(jobs)
    assert "unknown" in msg


def test_format_digest_url_present_when_job_has_url(ranked_jobs):
    """All top-3 job URLs should appear in the digest."""
    msg = format_digest(ranked_jobs)
    assert "https://databricks.com/jobs/456" in msg
    assert "https://boringco.com/jobs/789" in msg
