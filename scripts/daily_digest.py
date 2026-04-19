"""CLI entry point: post a daily digest of ranked jobs to Discord.

Usage:
    python scripts/daily_digest.py              # send to webhook
    python scripts/daily_digest.py --dry-run     # preview only
    python scripts/daily_digest.py --top 5       # top 5 instead of default 3
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click

REPO = Path(__file__).resolve().parents[1]
RANKED = REPO / "data" / "ranked_jobs.json"


@click.command()
@click.option("--dry-run", is_flag=True, help="Print digest without posting to Discord")
@click.option("--top", default=3, help="Number of top picks to feature")
def main(dry_run: bool, top: int) -> None:
    if not RANKED.exists():
        click.echo("❌ ranked_jobs.json not found. Run `python scripts/rank.py --save` first.")
        sys.exit(1)

    jobs = json.loads(RANKED.read_text())
    # Only feature the top N, but report total count
    featured = jobs[:top]

    from opportunities_engine.alerts.discord_digest import post_digest

    webhook_url = os.environ.get("DISCORD_WEBHOOK_URL")
    post_digest(jobs=featured, webhook_url=webhook_url, dry_run=dry_run)


if __name__ == "__main__":
    main()
