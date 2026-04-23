"""Refresh the YC seed — fetch latest batches and probe their ATS endpoints.

Idempotent: existing entries in seed_companies.json are not overwritten;
only new (by name, case-insensitive) companies are appended. New companies
are probed against Greenhouse, Lever, and Ashby; the first platform that
returns ≥1 job wins. Verified companies get `ats_slug_verified=True` so
the daily ingest picks them up automatically.

Usage
-----
    python scripts/refresh_yc_seed.py                   # default batches
    python scripts/refresh_yc_seed.py --batches W25 S25 # specific batches
    python scripts/refresh_yc_seed.py --dry-run         # fetch + probe, no write

Hitting ~hundreds of companies × 3 platforms = a lot of outbound requests.
Rate-limited (0.5s between probes) to stay polite.
"""
from __future__ import annotations

import time
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.table import Table

from opportunities_engine.config import settings
from opportunities_engine.ingestion.ats import ATSClient
from opportunities_engine.ingestion.yc_seed import (
    DEFAULT_BATCHES,
    fetch_yc_batches,
    merge_into_seed,
    probe_company_ats,
)

console = Console()


@click.command()
@click.option(
    "--batches",
    multiple=True,
    default=DEFAULT_BATCHES,
    help="YC batches to probe (default: W23 S23 W24 S24 W25 S25).",
)
@click.option(
    "--seed-path",
    default=None,
    type=click.Path(),
    help="Override path to seed_companies.json (defaults to settings.seed_companies_path).",
)
@click.option("--dry-run", is_flag=True, help="Fetch + probe but do not write the seed file.")
@click.option("--sleep", default=0.5, help="Seconds between probe requests (default: 0.5).")
def main(batches: tuple[str, ...], seed_path: str | None, dry_run: bool, sleep: float) -> None:
    """Refresh YC seed — fetch latest batches, probe their ATS, merge into seed JSON."""
    seed_file = Path(seed_path) if seed_path else settings.seed_companies_path
    console.print(f"[bold cyan]🌱 YC Seed Refresh[/bold cyan]")
    console.print(f"  Batches: {', '.join(batches)}")
    console.print(f"  Seed file: {seed_file}")
    console.print(f"  Dry run: {dry_run}")
    console.print()

    # 1. Fetch YC companies for each batch
    console.print("[bold]Step 1: Fetching YC batches[/bold]")
    with httpx.Client(timeout=15, follow_redirects=True) as http:
        companies = fetch_yc_batches(batches, http=http)
    console.print(f"  Fetched {len(companies)} companies across {len(batches)} batches\n")

    # 2. Probe each unverified candidate
    console.print("[bold]Step 2: Probing ATS endpoints[/bold]")
    verified: list[dict] = []
    skipped = 0
    ats = ATSClient()
    for i, comp in enumerate(companies):
        name = comp.get("name", "")
        website = comp.get("website", "")
        if not name or not website:
            skipped += 1
            continue
        result = probe_company_ats(name, website, ats=ats)
        if result:
            verified.append(result)
            console.print(
                f"  [green]✓[/] {name} → {result['ats_platform']}/{result['ats_slug']} "
                f"({result['job_count']} jobs)"
            )
        if sleep:
            time.sleep(sleep)
    console.print(f"\n  Verified {len(verified)} companies, skipped {skipped}\n")

    # 3. Merge into seed file
    if dry_run:
        console.print("[yellow]Dry run — not writing seed file.[/yellow]")
    else:
        console.print("[bold]Step 3: Merging into seed_companies.json[/bold]")
        added = merge_into_seed(seed_file, verified)
        console.print(f"  Appended {added} new entries (existing entries preserved)")

    # 4. Summary table
    table = Table(title="Summary", show_lines=False)
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold")
    table.add_row("Companies fetched", str(len(companies)))
    table.add_row("Probed & verified", str(len(verified)))
    table.add_row("Skipped (no website)", str(skipped))
    console.print(table)


if __name__ == "__main__":
    main()
