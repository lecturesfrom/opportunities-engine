"""Daily ingestion pipeline.

1. Hit verified ATS APIs (Greenhouse, Lever, Ashby) for seed companies
2. Hit JobSpy (Indeed, Google Jobs) for catch-all keyword search
3. Dedup and upsert everything into DuckDB
4. Print a summary of new jobs found
"""
from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from opportunities_engine.config import settings
from opportunities_engine.ingestion.ats import ATSClient
from opportunities_engine.ingestion.jobspy_source import scrape_all
from opportunities_engine.storage.db import JobStore

console = Console()


def _load_seed_companies() -> list[dict]:
    """Load the seed company list, return only verified entries."""
    path = settings.repo_root / "data" / "seed_companies.json"
    if not path.exists():
        return []
    data = json.loads(path.read_text())
    return [c for c in data.get("companies", []) if c.get("ats_slug_verified")]


def ingest_ats(store: JobStore, client: ATSClient | None = None) -> int:
    """Scrape all verified ATS companies. Returns count of NEW jobs."""
    ats = client or ATSClient()
    companies = _load_seed_companies()
    new_count = 0
    seen_count = 0
    error_count = 0

    for company in companies:
        slug = company["ats_slug"]
        platform = company["ats_platform"]
        name = company["name"]

        try:
            jobs = ats.fetch_company(company)
            for job in jobs:
                is_new = store.upsert_job(job)
                if is_new:
                    new_count += 1
                else:
                    seen_count += 1
            console.print(f"  [green]✓[/] {name} ({platform}): {len(jobs)} jobs ({new_count} new so far)")
        except Exception as e:
            error_count += 1
            console.print(f"  [red]✗[/] {name} ({platform}): {e}")

        time.sleep(0.5)  # be polite

    console.print(f"\n  ATS total: {new_count} new, {seen_count} seen, {error_count} errors")
    return new_count


def ingest_jobspy(store: JobStore, results_per_term: int = 30, hours_old: int = 72) -> int:
    """Scrape JobSpy sources (Indeed, Google). Returns count of NEW jobs."""
    new_count = 0
    seen_count = 0

    for job in scrape_all(results_per_term=results_per_term, hours_old=hours_old):
        is_new = store.upsert_job(job)
        if is_new:
            new_count += 1
        else:
            seen_count += 1

    console.print(f"  JobSpy: {new_count} new, {seen_count} seen")
    return new_count


def print_new_jobs_summary(store: JobStore, limit: int = 20) -> None:
    """Print a rich table of the newest jobs."""
    jobs = store.get_new_jobs(since_hours=168)[:limit]
    if not jobs:
        console.print("[yellow]No new jobs found.[/yellow]")
        return

    table = Table(title=f"📋 Newest Jobs (showing {len(jobs)} of recent)", show_lines=True)
    table.add_column("Company", style="cyan", max_width=20)
    table.add_column("Title", style="bold", max_width=40)
    table.add_column("Source", style="dim", max_width=15)
    table.add_column("Remote", max_width=6)

    for job in jobs:
        remote = "🌍" if job.get("is_remote") else ""
        table.add_row(
            str(job.get("company", "")),
            str(job.get("title", "")),
            str(job.get("source", "")),
            remote,
        )

    console.print(table)


@click.command()
@click.option("--skip-ats", is_flag=True, help="Skip ATS ingestion (JobSpy only)")
@click.option("--skip-jobspy", is_flag=True, help="Skip JobSpy ingestion (ATS only)")
@click.option("--hours", default=72, help="Hours old for JobSpy search")
@click.option("--results", default=30, help="Results per search term for JobSpy")
def main(skip_ats: bool, skip_jobspy: bool, hours: int, results: int) -> None:
    """Run the daily ingestion pipeline."""
    console.print("[bold cyan]🔄 Opportunities Engine — Daily Ingest[/bold cyan]")
    console.print(f"  DB: {settings.database_path}")
    console.print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    console.print()

    with JobStore(settings.database_path) as store:
        if not skip_ats:
            console.print("[bold]Phase 1: ATS APIs[/bold]")
            ats_new = ingest_ats(store)
            console.print()

        if not skip_jobspy:
            console.print("[bold]Phase 2: JobSpy (Indeed + Google)[/bold]")
            spy_new = ingest_jobspy(store, results_per_term=results, hours_old=hours)
            console.print()

        # Summary
        console.print("[bold]Summary[/bold]")
        print_new_jobs_summary(store)

        total_jobs = len(store.get_jobs(limit=99999))
        console.print(f"\n  Total jobs in DB: {total_jobs}")


if __name__ == "__main__":
    main()
