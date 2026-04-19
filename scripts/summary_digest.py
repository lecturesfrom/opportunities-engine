"""Generate a concise digest from ranked_jobs.json."""
from __future__ import annotations

import json
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
ranked = BASE / "data" / "ranked_jobs.json"

rows = json.loads(ranked.read_text()) if ranked.exists() else []

top = rows[:20]
lines = ["🔥 **Top GTM-Relevant Roles (today)**", ""]
for i, r in enumerate(top, 1):
    remote = " 🌍" if r.get("is_remote") else ""
    lines.append(f"{i}. **{r.get('title','')}** — {r.get('company','')} [{r.get('similarity','')}] {remote}")

out = "\n".join(lines)
print(out)

out_path = BASE / "data" / "latest_digest.md"
out_path.write_text(out)
print(f"\n\nSaved: {out_path}")
