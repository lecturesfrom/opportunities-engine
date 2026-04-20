"""Linear GraphQL client shared between push_top_to_linear and the listener."""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime
from typing import Any

from opportunities_engine.config import settings


def gql(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    """Execute a GraphQL query/mutation against the Linear API.

    Uses ``settings.linear_api_key`` for authentication.  Payload is a JSON
    POST to ``https://api.linear.app/graphql``.

    Args:
        query: GraphQL query or mutation string.
        variables: Optional variables dict to include in the request body.

    Returns:
        Parsed JSON response as a dict.
    """
    api_key = settings.linear_api_key or ""
    body: dict[str, Any] = {"query": query}
    if variables:
        body["variables"] = variables
    req = urllib.request.Request(
        "https://api.linear.app/graphql",
        data=json.dumps(body).encode(),
        headers={"Authorization": api_key, "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())  # type: ignore[no-any-return]


def get_project_issues(
    project_id: str,
    since: datetime | None = None,
) -> list[dict[str, Any]]:
    """Fetch issues for a Linear project, optionally filtered by updatedAt.

    Args:
        project_id: Linear project UUID.
        since: If provided, only return issues with ``updatedAt >= since``.

    Returns:
        List of issue node dicts with fields:
        id, identifier, title, url, state.name,
        comments.nodes (id, body, createdAt, user.name), updatedAt.
    """
    filter_arg = ""
    variables: dict[str, Any] = {"projectId": project_id}
    if since is not None:
        # Linear expects ISO-8601 with timezone
        since_str = since.isoformat()
        filter_arg = ", filter: {updatedAt: {gte: $since}}"
        variables["since"] = since_str

    if since is not None:
        query = """
        query($projectId: String!, $since: DateComparator) {
          project(id: $projectId) {
            issues(first: 250, filter: {updatedAt: {gte: $since}}) {
              nodes {
                id
                identifier
                title
                url
                state { name }
                comments {
                  nodes {
                    id
                    body
                    createdAt
                    user { name }
                  }
                }
                updatedAt
              }
            }
          }
        }
        """
    else:
        query = """
        query($projectId: String!) {
          project(id: $projectId) {
            issues(first: 250) {
              nodes {
                id
                identifier
                title
                url
                state { name }
                comments {
                  nodes {
                    id
                    body
                    createdAt
                    user { name }
                  }
                }
                updatedAt
              }
            }
          }
        }
        """

    data = gql(query, variables)
    nodes: list[dict[str, Any]] = (
        data.get("data", {})
        .get("project", {})
        .get("issues", {})
        .get("nodes", [])
    )
    return nodes
