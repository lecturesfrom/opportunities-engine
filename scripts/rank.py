"""Ranking entry point.

Embeds new postings, scores them against the profile, writes ranks
back to DuckDB.

TODO(phase-2): wire up semantic.embed + semantic.rank.
"""
from __future__ import annotations

import click
from rich.console import Console

from opportunities_engine.config import settings

console = Console()


@click.command()
def main() -> None:
    """Embed + rank postings against the target-title profile."""
    console.print("[bold cyan]Opportunities Engine — rank[/bold cyan]")
    console.print(f"Chroma: {settings.chroma_path}")
    console.print(f"Profile titles: {len(settings.target_titles)}")
    console.print("[yellow]stub — Phase 2 will wire up ChromaDB ranking[/yellow]")


if __name__ == "__main__":
    main()
