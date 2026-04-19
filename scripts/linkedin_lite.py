"""Manual LinkedIn-lite sweep.

Small, capped run for quick wins. Does NOT run on schedule by default.
"""
from __future__ import annotations

import click
from rich.console import Console

from opportunities_engine.config import settings
from opportunities_engine.storage.db import JobStore
from opportunities_engine.ingestion.jobspy_source import scrape_all

console = Console()


@click.command()
@click.option("--terms-cap", default=3, help="How many high-signal terms to run")
@click.option("--results-cap", default=8, help="Max results per term")
@click.option("--hours", default=168, help="Lookback window in hours")
def main(terms_cap: int, results_cap: int, hours: int) -> None:
    console.print("[bold cyan]🔎 LinkedIn-lite manual sweep[/bold cyan]")
    console.print("[dim]Mode: capped, manual, non-scheduled[/dim]")

    new_count = 0
    seen_count = 0

    with JobStore(settings.database_path) as store:
        for job in scrape_all(
            linkedin_lite=True,
            linkedin_terms_cap=terms_cap,
            linkedin_results_cap=results_cap,
            results_per_term=results_cap,
            hours_old=hours,
        ):
            is_new = store.upsert_job(job)
            if is_new:
                new_count += 1
            else:
                seen_count += 1

    console.print(f"[green]Done[/green] — new: {new_count}, seen: {seen_count}")


if __name__ == "__main__":
    main()
