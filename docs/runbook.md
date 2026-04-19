# Opportunities Engine — Runbook (minimal config)

## What happens when commands run

### 1) Daily ingest
Command:
```bash
.venv/bin/python scripts/daily_ingest.py
```
What it does:
1. Pulls jobs from verified ATS APIs (Greenhouse/Lever/Ashby)
2. Pulls fallback jobs from JobSpy (Indeed + Google)
3. Upserts into DuckDB (`data/jobs.duckdb`) with dedup
4. Prints summary + recent jobs table

### 2) LinkedIn-lite sweep (manual)
Command:
```bash
.venv/bin/python scripts/linkedin_lite.py --terms-cap 3 --results-cap 8 --hours 168
```
What it does:
- Runs a capped LinkedIn pass for easy wins
- Adds/updates jobs in DuckDB
- Never runs by default on schedules

### 3) Ranking
Command:
```bash
.venv/bin/python scripts/rank.py --top 30 --threshold 0.16 --save
```
What it does:
- Applies US/remote gate
- Applies curated GTME+adjacent role universe
- Removes obvious noise and dedups
- Writes shortlist to `data/ranked_jobs.json`

### 4) Digest
Command:
```bash
.venv/bin/python scripts/summary_digest.py
```
What it does:
- Creates compact digest from ranked jobs
- Writes `data/latest_digest.md`

---

## Required config
Only `.env` is required (already set):
- `LINEAR_API_KEY`
- `LINEAR_WORKSPACE_SLUG`
- `LINEAR_TEAM_NAME`

No extra config needed to run the commands.

---

## Recommended daily cadence (token-efficient)

### 2x/day baseline
- 8:00 AM ET: ATS + JobSpy + rank + digest
- 6:00 PM ET: ATS + JobSpy + rank + digest

### Optional 3rd pass (if market is hot)
- 1:00 PM ET: ATS + rank (skip JobSpy for lower noise/cost)

### LinkedIn-lite
- Manual 1–2x/day when you want quick wins.

---

## Why 2,794 jobs is OK
That is raw + deduped storage over time, not your daily action list.
Action list is `ranked_jobs.json` (high-signal shortlist), currently much smaller.

Raw DB = coverage
Ranked list = decisions

---

## One-command local flow (manual)
```bash
cd ~/code/opportunities-engine
.venv/bin/python scripts/daily_ingest.py
.venv/bin/python scripts/linkedin_lite.py --terms-cap 2 --results-cap 5 --hours 168
.venv/bin/python scripts/rank.py --top 30 --threshold 0.16 --save
.venv/bin/python scripts/summary_digest.py
```

