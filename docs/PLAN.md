# Opportunities Engine — 6-Piece Plan (Pinned)

Owner: Keegan Moody  
Team: regripping4L (RGL)  
Workspace: lecturesfrom  
Repo: lecturesfrom/opportunities-engine

## 1) Ingestion (done + tighten)
- ATS-first ingestion: Greenhouse, Lever, Ashby
- JobSpy fallback: Indeed + Google
- LinkedIn via JobSpy is currently disabled in code (intentional for stability/rate-limit)
- Next tighten: optional LinkedIn capture mode for quick-win searches (manual runs, low-volume)

## 2) Relevance filtering (done)
- US/remote gate is ON
- Curated GTME + adjacent role universe is applied
- No seniority gate
- Dedup + ATS preference over board duplicates

## 3) Linear-first execution (in progress)
- Move from “big shipments” to small ticket flow
- Each change maps to a single RGL issue
- Active project has phased tracker issues RGL-10 through RGL-16

## 4) Daily shortlist + digest (next)
- Generate top 10–15 roles daily
- Post to this Discord channel
- Keep links + score + source compact

## 5) Auto-capture + tracking loops (next)
- Gmail parser: “Thanks for applying” -> Active Applications updates
- Discord commands: /applied, /interested, /dream
- changedetection for dream companies with custom pages

## 6) Skill/learning loop (next)
- Skill gaps from job descriptions
- Curated prep resources (Substack + others)
- Feedback loop to improve recommendations over time

---

## LinkedIn answer (explicit)
Yes, we can and should catch LinkedIn quick wins.

Current state:
- We ingest ATS + Indeed + Google reliably now.
- LinkedIn automated scraping is not enabled by default to avoid brittle/rate-limited runs.

How we catch easy wins now:
1. Add an optional `--linkedin-lite` mode for manual on-demand runs (small results per term)
2. Keep ATS as primary high-signal source
3. If you drop specific LinkedIn job URLs in Discord, we can ingest them immediately into DuckDB + Linear

## Working rule
Ship smaller. Commit smaller. Track in Linear first.
