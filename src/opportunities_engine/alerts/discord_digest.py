"""Discord digest delivery — posts daily opportunity summary to a webhook.

Reads ranked_jobs.json, formats a concise Discord-friendly message
(under 2000 chars), and POSTs it via webhook. Supports --dry-run for
preview without sending.
"""
from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path
from typing import Any

DISCORD_MAX_LEN = 2000


def _load_dream_names() -> set[str]:
    """Load dream company names for highlighting in digest."""
    try:
        from opportunities_engine.config import REPO_ROOT
        path = REPO_ROOT / "data" / "dream_companies.json"
        if not path.exists():
            return set()
        data = json.loads(path.read_text())
        return {c["name"].strip().lower() for c in data.get("companies", [])}
    except Exception:
        return set()


def _score_emoji(score: float) -> str:
    """Pick an emoji based on score tier."""
    if score >= 0.35:
        return "🔥"
    if score >= 0.25:
        return "💪"
    if score >= 0.18:
        return "👀"
    return "☑️"


def _remote_badge(is_remote: bool | None) -> str:
    """Short remote indicator."""
    if is_remote:
        return " 🌐"
    return ""


def format_digest(jobs: list[dict[str, Any]]) -> str:
    """Format ranked jobs into a Discord-friendly digest message.

    Shows: total count, top 3 picks with score + remote flag,
    dream company callouts, and a closing line.
    Stays under DISCORD_MAX_LEN.
    """
    if not jobs:
        return "📭 No new opportunities today. The pipeline is quiet."

    dream_names = _load_dream_names()
    total = len(jobs)

    # Header
    lines: list[str] = [
        f"📡 **Opportunities Digest** — {total} role{'s' if total != 1 else ''} in pipeline",
        "",
    ]

    # Top 3 picks
    top = jobs[:3]
    lines.append("**Top picks:**")
    for i, job in enumerate(top, 1):
        title = str(job.get("title", "")).strip()
        company = str(job.get("company", "")).strip()
        score = float(job.get("similarity", 0))
        is_remote = job.get("is_remote")
        emoji = _score_emoji(score)
        remote = _remote_badge(is_remote)
        dream_marker = " ⭐" if company.lower() in dream_names else ""
        lines.append(
            f"{i}. {emoji} **{title}** @ {company}{dream_marker}{remote} ({score:.2f})"
        )

    # Dream company hits
    dream_hits = [
        j for j in jobs
        if str(j.get("company", "")).strip().lower() in dream_names
    ]
    if dream_hits:
        names = [str(j.get("company", "")).strip() for j in dream_hits]
        lines.append("")
        lines.append(f"⭐ Dream company hits: {', '.join(names[:5])}")

    # Closing
    lines.append("")
    lines.append("_Next digest auto-delivered tomorrow. Reply `apply` on any card to move it forward._")

    msg = "\n".join(lines)

    # Truncate to Discord limit if needed
    if len(msg) > DISCORD_MAX_LEN:
        # Aggressive truncation: keep header + top 3, cut the rest
        truncated = "\n".join(lines[:8])  # header + blank + top3 header + 3 picks + blank
        if len(truncated) > DISCORD_MAX_LEN:
            truncated = truncated[: DISCORD_MAX_LEN - 3] + "..."
        msg = truncated

    return msg


def _send_webhook(webhook_url: str, content: str) -> dict:
    """POST a message to a Discord webhook."""
    body = json.dumps({"content": content}).encode()
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return {"status": "ok", "code": resp.status}


def post_digest(
    jobs: list[dict[str, Any]],
    webhook_url: str | None = None,
    dry_run: bool = False,
) -> None:
    """Format and post the digest. Prints to stdout on dry_run or missing webhook."""
    msg = format_digest(jobs)

    if dry_run:
        print(msg)
        return

    if not webhook_url:
        print(f"⚠️ No webhook URL provided. Set DISCORD_WEBHOOK_URL or use --dry-run.\n\n{msg}")
        return

    try:
        _send_webhook(webhook_url, msg)
        print("✅ Digest posted to Discord.")
    except Exception as e:
        print(f"❌ Failed to post digest: {e}\n\n{msg}")
