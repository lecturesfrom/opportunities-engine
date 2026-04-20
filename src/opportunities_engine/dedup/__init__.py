"""Dedup package — re-exports public API."""

from opportunities_engine.dedup.upsert import UpsertResult, upsert_job_with_source

__all__ = ["UpsertResult", "upsert_job_with_source"]
