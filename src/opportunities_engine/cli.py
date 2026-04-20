"""Opportunities Engine CLI — main entry point."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from opportunities_engine.config import get_default_logs_path, settings
from opportunities_engine.events import POSSIBLE_DUPLICATE, emit_event
from opportunities_engine.events.vocab import ALL_EVENT_TYPES
from opportunities_engine.storage.db import JobStore

console = Console()


def parse_time_window(last_str: str) -> timedelta:
    """Parse time window string like '1d', '7d', '30d', '0d', '1h', or bare int.

    Args:
        last_str: String like '1d', '7d', '1h', '0d', or just '7' (treated as days).

    Returns:
        timedelta object representing the window.

    Raises:
        ValueError: If the string format is unrecognized.
    """
    last_str = last_str.strip()

    # Try to parse as int only (days)
    try:
        days = int(last_str)
        return timedelta(days=days)
    except ValueError:
        pass

    # Parse with suffix
    match = re.match(r"^(\d+)([dhm])$", last_str, re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid time window format: {last_str}. Use '1d', '7d', '1h', '30', etc.")

    amount = int(match.group(1))
    unit = match.group(2).lower()

    if unit == "d":
        return timedelta(days=amount)
    elif unit == "h":
        return timedelta(hours=amount)
    elif unit == "m":
        return timedelta(minutes=amount)
    else:
        raise ValueError(f"Invalid time unit: {unit}")


def read_dedup_jsonl(
    logs_dir: Path, since: datetime
) -> list[dict]:
    """Read JSONL records from dedup-*.jsonl files within the time window.

    Args:
        logs_dir: Directory containing dedup-*.jsonl files.
        since: Only include records with ts >= this datetime (UTC).

    Returns:
        List of parsed JSONL records.
    """
    records = []

    if not logs_dir.exists():
        return records

    # Find all dedup-YYYY-MM-DD.jsonl files
    for jsonl_file in sorted(logs_dir.glob("dedup-*.jsonl")):
        # Extract date from filename dedup-YYYY-MM-DD.jsonl
        match = re.search(r"dedup-(\d{4}-\d{2}-\d{2})\.jsonl", jsonl_file.name)
        if not match:
            continue

        file_date_str = match.group(1)
        try:
            file_date = datetime.strptime(file_date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            continue

        # Check if this file's date is within the window
        # file_date is midnight UTC; since could be any time that day
        # Include the file if its date is >= since's date
        if file_date.date() < since.date():
            continue

        # Read and parse lines
        try:
            with jsonl_file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                        # Check if ts is within window
                        if "ts" in record:
                            try:
                                record_ts = datetime.fromisoformat(
                                    record["ts"].replace("Z", "+00:00")
                                )
                                if record_ts >= since:
                                    records.append(record)
                            except (ValueError, AttributeError):
                                # Skip records with invalid ts
                                pass
                    except json.JSONDecodeError:
                        # Skip malformed JSON lines
                        pass
        except (IOError, OSError):
            # Skip files that can't be read
            pass

    return records


@click.group()
def main() -> None:
    """Opportunities Engine CLI."""


@main.group()
def dedup() -> None:
    """Dedup telemetry and diagnostics."""


@dedup.command("stats")
@click.option(
    "--last",
    default="1d",
    help="Time window: '1d', '7d', '30d', '0d' (today only), '1h', or bare int (days). Default: 1d",
)
def stats(last: str) -> None:
    """Report dedup pipeline statistics over a time window.

    Reads JSONL telemetry and queries review queue depth from events table.
    """
    # Parse the time window
    try:
        window = parse_time_window(last)
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        raise SystemExit(1)

    # Calculate the 'since' cutoff (UTC)
    now_utc = datetime.now(timezone.utc)
    since = now_utc - window

    # Read JSONL records
    logs_dir = get_default_logs_path()
    records = read_dedup_jsonl(logs_dir, since)

    if not records:
        console.print(
            f"[yellow]No dedup telemetry found in the last {last}.[/yellow]"
        )
        return

    # Tally outcome distribution
    outcome_counts: dict[str, int] = {
        "new_job": 0,
        "new_source": 0,
        "duplicate": 0,
        "review_flagged": 0,
    }
    trust_flipped_count = 0

    for record in records:
        outcome = record.get("outcome")
        if outcome in outcome_counts:
            outcome_counts[outcome] += 1
        if record.get("trust_flipped"):
            trust_flipped_count += 1

    # Query review queue depth from events table
    review_queue_depth = 0
    try:
        with JobStore(settings.database_path) as store:
            result = store.conn.execute(
                f"""
                SELECT COUNT(*) as count FROM events
                WHERE event_type = ?
                """,
                [POSSIBLE_DUPLICATE],
            ).fetchall()
            if result:
                review_queue_depth = result[0][0]
    except Exception as e:
        console.print(f"[yellow]Warning: Could not query review queue: {e}[/yellow]")

    # Build and print summary table
    table = Table(title="Dedup Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Count", style="bold")

    # Outcomes
    table.add_row("Outcome: new_job", str(outcome_counts["new_job"]))
    table.add_row("Outcome: new_source", str(outcome_counts["new_source"]))
    table.add_row("Outcome: duplicate", str(outcome_counts["duplicate"]))
    table.add_row("Outcome: review_flagged", str(outcome_counts["review_flagged"]))

    # Summary
    total_records = sum(outcome_counts.values())
    table.add_row("Total records", str(total_records))
    table.add_row("Trust flipped", str(trust_flipped_count))
    table.add_row("Review queue depth", str(review_queue_depth))

    console.print(table)


@main.group()
def event() -> None:
    """Event emission and diagnostics."""


@event.command("add")
@click.option("--job-id", type=int, required=True)
@click.option("--type", "event_type", required=True)
@click.option("--notes", default=None)
@click.option("--actor", default="keegan")
def event_add(job_id: int, event_type: str, notes: str | None, actor: str) -> None:
    """Append a manual event to a job's timeline."""
    if event_type not in ALL_EVENT_TYPES:
        allowed = sorted(ALL_EVENT_TYPES)
        console.print(
            f"[red]Error:[/red] Unknown event type {event_type!r}. "
            f"Allowed types: {allowed}"
        )
        raise SystemExit(1)

    detail: dict | None = {"notes": notes} if notes else None

    with JobStore(settings.database_path) as store:
        emit_event(store, job_id, event_type, actor=actor, detail=detail)

    console.print(
        f"[green]Event recorded:[/green] job_id={job_id} type={event_type!r} actor={actor!r}"
    )


@event.command("poll-linear")
@click.option(
    "--project-id",
    default=None,
    help="Linear project id. Defaults to settings.linear_project_id if set.",
)
@click.option("--dry-run", is_flag=True, help="Show what would emit without writing.")
def poll_linear_cmd(project_id: str | None, dry_run: bool) -> None:
    """Poll Linear for issue state changes and emit events."""
    from opportunities_engine.events.linear_listener import poll_linear

    resolved_project_id = project_id or settings.linear_project_id
    if resolved_project_id is None:
        console.print(
            "[red]Error:[/red] No --project-id provided and settings.linear_project_id is not set. "
            "Set LINEAR_PROJECT_ID in your .env or pass --project-id."
        )
        raise SystemExit(1)

    with JobStore(settings.database_path) as store:
        summary = poll_linear(store, resolved_project_id, dry_run=dry_run)

    from rich import print as rprint

    rprint(summary)


if __name__ == "__main__":
    main()
