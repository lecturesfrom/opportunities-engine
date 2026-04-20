# Data Snapshots

These JSON files are **frozen reference points**, not live configuration.

The authoritative source of truth for company and attraction data is now the **DuckDB `companies` and `company_attractions` tables**.

These snapshots exist so you can:
- Audit what the JSON files looked like at the time of migration
- Roll back if the migration introduced errors
- Compare current DB state against the original inputs

**Do not edit these files.** If you need to update company data, modify it in the database.

| File | Frozen | Source |
|---|---|---|
| `seed_companies_snapshot_2026-04-19.json` | 2026-04-19 | YC seed company ATS discovery |
| `dream_companies_snapshot_2026-04-19.json` | 2026-04-19 | Keegan's dream company taxonomy |
