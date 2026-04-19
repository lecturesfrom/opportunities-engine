"""Daily ingestion entry point.

Scrapes configured job boards + dream-company pages and writes
normalized rows into the DuckDB store.

TODO(phase-1): wire up ingestion.jobspy_source and dream-company crawlers.
"""
from __future__ import annotations

import click
from rich.console import Console

from opportunities_engine.config import settings

console = Console()


@click.command()
def main() -> None:
    """Run the daily ingestion pipeline."""
    console.print("[bold cyan]Opportunities Engine — daily_ingest[/bold cyan]")
    console.print(f"DB: {settings.database_path}")
    console.print(f"Target titles: {len(settings.target_titles)} configured")
    console.print(f"Linear team: {settings.linear_team_name}")
    console.print("[yellow]stub — Phase 1 will wire up JobSpy ingestion[/yellow]")


if __name__ == "__main__":
    main()
