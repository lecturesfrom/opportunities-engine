"""Rank jobs by relevance to GTM profile (fast mode)."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from opportunities_engine.config import settings
from opportunities_engine.events import emit_event, SCORED
from opportunities_engine.semantic.ranker import rank_jobs_local
from opportunities_engine.storage.db import JobStore, get_job_id_by_url

logger = logging.getLogger(__name__)

console = Console()


@click.command()
@click.option("--top", default=50, help="Number of top results to show")
@click.option("--threshold", default=0.14, help="Minimum relevance score")
@click.option("--save", is_flag=True, help="Save ranked results to data/ranked_jobs.json")
def main(top: int, threshold: float, save: bool) -> None:
    console.print("[bold cyan]🎯 Opportunities Engine — Fast Rank[/bold cyan]")

    with JobStore(settings.database_path) as store:
        jobs = store.get_jobs(limit=99999)

    if not jobs:
        console.print("[red]No jobs in DB. Run daily_ingest first.[/red]")
        return

    ranked = rank_jobs_local(jobs, top_k=top, min_score=threshold)

    if not ranked:
        console.print("[yellow]No jobs above threshold. Try lowering --threshold[/yellow]")
        return

    # Emit SCORED events for each ranked job
    with JobStore(settings.database_path) as store:
        for i, job in enumerate(ranked):
            job_id = get_job_id_by_url(store, job["url"])
            if job_id is None:
                logger.debug("No job_id found for URL %s — skipping SCORED emit", job["url"])
                continue
            emit_event(
                store,
                job_id,
                SCORED,
                detail={"score": job["similarity"], "rank_position": i},
            )

    table = Table(title=f"🔥 Top {len(ranked)} Relevant GTM Roles", show_lines=True)
    table.add_column("#", style="dim", width=3)
    table.add_column("Score", style="bold green", width=6)
    table.add_column("Title", max_width=42)
    table.add_column("Company", style="cyan", max_width=20)
    table.add_column("Source", style="dim", max_width=14)
    table.add_column("Remote", width=6)

    for i, job in enumerate(ranked, 1):
        table.add_row(
            str(i),
            f"{job['similarity']:.2f}",
            str(job["title"]),
            str(job["company"]),
            str(job["source"]),
            "🌍" if job.get("is_remote") else "",
        )

    console.print(table)
    console.print(f"\n[dim]DB: {len(jobs)} total → {len(ranked)} relevant[/dim]")

    if save:
        out = settings.repo_root / "data" / "ranked_jobs.json"
        out.write_text(json.dumps(ranked, indent=2))
        console.print(f"[green]Saved:[/green] {out}")


if __name__ == "__main__":
    main()
