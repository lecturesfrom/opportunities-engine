"""Push top ranked roles into Linear Active Applications.

Dedups by URL and (company,title) to avoid noisy duplicates.
"""
from __future__ import annotations

import json
import os
import urllib.request
from pathlib import Path

import click
from rich.console import Console

console = Console()
REPO = Path(__file__).resolve().parents[1]
RANKED = REPO / "data" / "ranked_jobs.json"


def _env(key: str, default: str = "") -> str:
    # simple .env loader fallback
    env_path = REPO / ".env"
    if key in os.environ:
        return os.environ[key]
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith(f"{key}="):
                return line.split("=", 1)[1].strip()
    return default


def gql(query: str, variables: dict | None = None) -> dict:
    api_key = _env("LINEAR_API_KEY")
    body = {"query": query}
    if variables:
        body["variables"] = variables
    req = urllib.request.Request(
        "https://api.linear.app/graphql",
        data=json.dumps(body).encode(),
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_team_and_project() -> tuple[str, str, str]:
    # team key/name fixed for this project
    team_id = "95c2e237-88e1-42eb-a733-fd6b75ae737f"
    project_id = "600dd458-ac07-46e9-9567-d108e7d3c6bd"  # 🎯 Active Applications
    state_todo = "efb95eb0-9e68-4d48-a6a5-29fc765cb9a2"
    return team_id, project_id, state_todo


def existing_issue_titles(project_id: str) -> set[str]:
    q = """
    query($projectId: String!) {
      project(id: $projectId) {
        issues(first: 250) { nodes { title } }
      }
    }
    """
    data = gql(q, {"projectId": project_id})
    nodes = data.get("data", {}).get("project", {}).get("issues", {}).get("nodes", [])
    return {n["title"].strip().lower() for n in nodes}


def make_title(job: dict) -> str:
    return f"{job.get('title','').strip()} @ {job.get('company','').strip()}"


def make_description(job: dict) -> str:
    from opportunities_engine.framing.why_interesting import (
        generate_why_interesting,
        WHY_INTERESTING_HEADING,
    )
    blurb = generate_why_interesting(job)
    return "\n".join(
        [
            f"**{WHY_INTERESTING_HEADING}**",
            "",
            blurb,
            "",
            "---",
            f"**Score:** {job.get('similarity','')}",
            f"**Source:** {job.get('source','')}",
            f"**Location:** {job.get('location','')}",
            f"**Remote:** {'yes' if job.get('is_remote') else 'no'}",
            "",
            f"**URL:** {job.get('url','')}",
            "",
            "_Auto-pushed from ranked_jobs.json_",
        ]
    )


@click.command()
@click.option("--top", default=15, help="How many ranked jobs to push")
@click.option("--dry-run", is_flag=True, help="Preview only")
def main(top: int, dry_run: bool) -> None:
    if not RANKED.exists():
        console.print("[red]ranked_jobs.json not found[/red]")
        raise SystemExit(1)

    jobs = json.loads(RANKED.read_text())[:top]
    team_id, project_id, state_todo = get_team_and_project()
    existing = existing_issue_titles(project_id)

    mut = """
    mutation($input: IssueCreateInput!) {
      issueCreate(input: $input) {
        success
        issue { identifier title url }
      }
    }
    """

    created = 0
    skipped = 0

    for job in jobs:
        title = make_title(job)
        key = title.strip().lower()
        if key in existing:
            skipped += 1
            continue

        if dry_run:
            console.print(f"[yellow]DRY[/yellow] {title}")
            continue

        inp = {
            "teamId": team_id,
            "projectId": project_id,
            "stateId": state_todo,
            "title": title,
            "description": make_description(job),
            "priority": 2,
        }
        data = gql(mut, {"input": inp})
        if data.get("errors"):
            console.print(f"[red]error[/red] {title}: {data['errors'][0].get('message','unknown')}" )
            continue
        issue = data["data"]["issueCreate"]["issue"]
        console.print(f"[green]✓[/green] {issue['identifier']} {issue['title']}")
        created += 1
        existing.add(key)

    console.print(f"\nDone: created={created}, skipped_existing={skipped}, scanned={len(jobs)}")


if __name__ == "__main__":
    main()
