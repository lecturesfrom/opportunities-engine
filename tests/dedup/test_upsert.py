"""Integration tests for upsert_job_with_source.

Uses JobStore(':memory:') throughout. Covers all four outcomes plus trust_flip.
"""

import json

import pytest

from opportunities_engine.dedup.upsert import UpsertResult, upsert_job_with_source
from opportunities_engine.storage.db import JobStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(
    n: int,
    title: str = "Senior Software Engineer",
    company: str = "Stripe",
    location: str = "Remote",
    source: str = "greenhouse",
    **overrides: object,
) -> dict:
    """Build a minimal job dict with a unique URL per `n`."""
    base = {
        "url": f"https://boards.greenhouse.io/stripe/jobs/{n}",
        "title": title,
        "company": company,
        "location": location,
        "source": source,
        "source_id": str(n),
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestNewJob:
    def test_empty_store_returns_new_job(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1)
            result = upsert_job_with_source(store, job, "greenhouse")

            assert result.outcome == "new_job"
            assert result.job_id is not None
            assert result.matched_job_id is None
            assert result.fuzzy_score is None
            assert result.trust_flipped is False
            assert result.source_name == "greenhouse"

    def test_new_job_creates_one_jobs_row(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            count = store.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            assert count == 1

    def test_new_job_creates_one_job_sources_row(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            count = store.conn.execute("SELECT COUNT(*) FROM job_sources").fetchone()[0]
            assert count == 1

    def test_new_job_source_is_trusted(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            row = store.conn.execute(
                "SELECT source_trust FROM job_sources WHERE source_name = 'greenhouse'"
            ).fetchone()
            assert row[0] == "trusted"

    def test_new_job_sets_canonical_key(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            row = store.conn.execute(
                "SELECT canonical_key, company_normalized FROM jobs"
            ).fetchone()
            assert row[0] is not None
            assert row[1] is not None
            assert "stripe" in row[0]


class TestNewSource:
    """Same canonical job seen from a second source."""

    def test_new_source_outcome(self) -> None:
        with JobStore(":memory:") as store:
            # Source A inserts the job
            r1 = upsert_job_with_source(store, _make_job(1), "greenhouse")
            assert r1.outcome == "new_job"

            # Source B inserts a different URL, same canonical
            job_b = _make_job(2)  # different n → different URL, same title/company/location
            r2 = upsert_job_with_source(store, job_b, "lever")
            assert r2.outcome == "new_source"

    def test_new_source_still_one_job_row(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            upsert_job_with_source(store, _make_job(2), "lever")
            count = store.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            assert count == 1

    def test_new_source_creates_second_job_sources_row(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            upsert_job_with_source(store, _make_job(2), "lever")
            count = store.conn.execute("SELECT COUNT(*) FROM job_sources").fetchone()[0]
            assert count == 2

    def test_new_source_matched_job_id_equals_original(self) -> None:
        with JobStore(":memory:") as store:
            r1 = upsert_job_with_source(store, _make_job(1), "greenhouse")
            r2 = upsert_job_with_source(store, _make_job(2), "lever")
            assert r2.job_id == r1.job_id
            assert r2.matched_job_id == r1.job_id


class TestDuplicate:
    """Same canonical job, same source, inserted twice."""

    def test_duplicate_outcome(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1)
            upsert_job_with_source(store, job, "greenhouse")
            r2 = upsert_job_with_source(store, job, "greenhouse")
            assert r2.outcome == "duplicate"

    def test_duplicate_does_not_create_new_job_row(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1)
            upsert_job_with_source(store, job, "greenhouse")
            upsert_job_with_source(store, job, "greenhouse")
            count = store.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            assert count == 1

    def test_duplicate_advances_last_seen(self) -> None:
        from datetime import datetime, timedelta, timezone

        with JobStore(":memory:") as store:
            job = _make_job(1)
            r1 = upsert_job_with_source(store, job, "greenhouse")

            # Back-date last_seen (use naive local datetime to match DuckDB storage)
            past_naive = datetime.now() - timedelta(hours=2)
            store.conn.execute(
                "UPDATE job_sources SET last_seen = $1 WHERE job_id = $2",
                [past_naive, r1.job_id],
            )

            r2 = upsert_job_with_source(store, job, "greenhouse")
            assert r2.outcome == "duplicate"

            row = store.conn.execute(
                "SELECT last_seen FROM job_sources WHERE job_id = $1 AND source_name = 'greenhouse'",
                [r1.job_id],
            ).fetchone()
            # DuckDB may return naive datetime; compare naively
            last_seen = row[0]
            if hasattr(last_seen, "tzinfo") and last_seen.tzinfo is not None:
                last_seen = last_seen.replace(tzinfo=None)
            assert last_seen > past_naive

    def test_duplicate_no_trust_flip(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1)
            upsert_job_with_source(store, job, "greenhouse")
            r2 = upsert_job_with_source(store, job, "greenhouse")
            assert r2.trust_flipped is False


class TestReviewFlagged:
    """Near-miss that does NOT cross DEDUP_THRESHOLD but is >= DEDUP_REVIEW_FLOOR."""

    # Verified pair: WRatio ≈ 93.9, which lands in [93, 95)
    JOB_A = {
        "url": "https://stripe.com/jobs/swe-1",
        "title": "Software Engineer",
        "company": "Stripe",
        "location": "Remote",
        "source": "greenhouse",
    }
    JOB_B = {
        "url": "https://stripe.com/jobs/swe-3",
        "title": "Software Engineer III",
        "company": "Stripe",
        "location": "Remote",
        "source": "greenhouse",
    }

    def test_review_flagged_outcome(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, self.JOB_A, "greenhouse")
            r2 = upsert_job_with_source(store, self.JOB_B, "greenhouse")
            assert r2.outcome == "review_flagged"

    def test_review_flagged_creates_two_job_rows(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, self.JOB_A, "greenhouse")
            upsert_job_with_source(store, self.JOB_B, "greenhouse")
            count = store.conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            assert count == 2

    def test_review_flagged_emits_possible_duplicate_event(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, self.JOB_A, "greenhouse")
            r2 = upsert_job_with_source(store, self.JOB_B, "greenhouse")
            events = store.conn.execute(
                "SELECT event_type, detail FROM events WHERE event_type = 'possible_duplicate'"
            ).fetchall()
            assert len(events) == 1
            event_type, detail_raw = events[0]
            assert event_type == "possible_duplicate"
            detail = json.loads(detail_raw) if isinstance(detail_raw, str) else detail_raw
            assert detail["new_job_id"] == r2.job_id
            assert detail["matched_job_id"] == r2.matched_job_id

    def test_review_flagged_matched_job_id_set(self) -> None:
        with JobStore(":memory:") as store:
            r1 = upsert_job_with_source(store, self.JOB_A, "greenhouse")
            r2 = upsert_job_with_source(store, self.JOB_B, "greenhouse")
            assert r2.matched_job_id == r1.job_id

    def test_review_flagged_fuzzy_score_in_review_band(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, self.JOB_A, "greenhouse")
            r2 = upsert_job_with_source(store, self.JOB_B, "greenhouse")
            assert r2.fuzzy_score is not None
            assert 93 <= r2.fuzzy_score < 95


class TestTrustFlip:
    """Trust flip: untrusted legacy rows flip to trusted when a real ingester re-observes."""

    def test_trust_flip_occurs_when_untrusted_source_present(self) -> None:
        with JobStore(":memory:") as store:
            # Insert job via greenhouse
            r1 = upsert_job_with_source(store, _make_job(1), "greenhouse")
            # Simulate legacy: mark existing source as untrusted
            store.conn.execute(
                "UPDATE job_sources SET source_trust = 'untrusted' WHERE job_id = $1",
                [r1.job_id],
            )

            # Re-observe via lever (different URL, same canonical)
            r2 = upsert_job_with_source(store, _make_job(2), "lever")

            assert r2.outcome == "new_source"
            assert r2.trust_flipped is True

    def test_trust_flip_updates_all_rows(self) -> None:
        with JobStore(":memory:") as store:
            r1 = upsert_job_with_source(store, _make_job(1), "greenhouse")
            # Mark greenhouse row untrusted
            store.conn.execute(
                "UPDATE job_sources SET source_trust = 'untrusted' WHERE job_id = $1",
                [r1.job_id],
            )

            upsert_job_with_source(store, _make_job(2), "lever")

            # All job_sources rows for this job_id should now be trusted
            rows = store.conn.execute(
                "SELECT source_trust FROM job_sources WHERE job_id = $1",
                [r1.job_id],
            ).fetchall()
            trusts = [row[0] for row in rows]
            assert all(t == "trusted" for t in trusts), f"Found untrusted rows: {trusts}"

    def test_no_trust_flip_when_all_already_trusted(self) -> None:
        with JobStore(":memory:") as store:
            upsert_job_with_source(store, _make_job(1), "greenhouse")
            # Do NOT mark as untrusted — all rows stay trusted
            r2 = upsert_job_with_source(store, _make_job(2), "lever")
            assert r2.trust_flipped is False

    def test_trust_flip_false_for_duplicate_outcome(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1)
            upsert_job_with_source(store, job, "greenhouse")
            r2 = upsert_job_with_source(store, job, "greenhouse")
            assert r2.outcome == "duplicate"
            assert r2.trust_flipped is False


class TestUrlShortCircuit:
    """URL short-circuit: exact URL match bypasses canonical/fuzzy."""

    def test_same_url_same_source_is_duplicate(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1, title="Software Engineer", company="Acme")
            upsert_job_with_source(store, job, "greenhouse")
            r2 = upsert_job_with_source(store, job, "greenhouse")
            assert r2.outcome == "duplicate"

    def test_same_url_different_source_is_new_source(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(1, title="Software Engineer", company="Acme")
            r1 = upsert_job_with_source(store, job, "greenhouse")
            r2 = upsert_job_with_source(store, job, "lever")
            assert r2.outcome == "new_source"
            assert r2.job_id == r1.job_id


class TestEmptyUrl:
    """Ashby (and occasionally other ingesters) may return jobs with no URL.
    jobs.url is NOT NULL UNIQUE, so blanks must be synthesized at the
    chokepoint. Canonical-key dedup still handles re-observation.
    """

    def test_two_empty_url_jobs_different_canonical_both_insert(self) -> None:
        with JobStore(":memory:") as store:
            a = _make_job(1, title="GTM Engineer", company="Acme", url="", source="ashby")
            b = _make_job(2, title="Forward Deployed Engineer", company="Acme", url="", source="ashby")
            r1 = upsert_job_with_source(store, a, "ashby")
            r2 = upsert_job_with_source(store, b, "ashby")
            assert r1.outcome == "new_job"
            assert r2.outcome == "new_job"
            assert r1.job_id != r2.job_id

    def test_empty_url_is_synthesized_as_urn(self) -> None:
        with JobStore(":memory:") as store:
            job = _make_job(42, title="GTM Engineer", company="Acme", url="", source="ashby")
            result = upsert_job_with_source(store, job, "ashby")
            row = store.conn.execute(
                "SELECT url FROM jobs WHERE id = $1", [result.job_id]
            ).fetchone()
            assert row[0].startswith("urn:opp:ashby:")
            assert "42" in row[0]  # source_id threads through

    def test_empty_url_whitespace_treated_as_empty(self) -> None:
        with JobStore(":memory:") as store:
            a = _make_job(1, title="A", company="Co", url="   ", source="ashby")
            b = _make_job(2, title="B", company="Co", url="\t\n", source="ashby")
            r1 = upsert_job_with_source(store, a, "ashby")
            r2 = upsert_job_with_source(store, b, "ashby")
            assert r1.outcome == "new_job"
            assert r2.outcome == "new_job"

    def test_empty_url_same_canonical_still_dedups(self) -> None:
        """Same title/company/location + empty URL from two Ashby pulls → second is duplicate."""
        with JobStore(":memory:") as store:
            job = _make_job(1, title="GTM Engineer", company="Acme", url="", source="ashby")
            r1 = upsert_job_with_source(store, job, "ashby")
            r2 = upsert_job_with_source(store, job, "ashby")
            assert r1.outcome == "new_job"
            assert r2.outcome == "duplicate"

    def test_empty_url_no_source_id_falls_back_to_canonical_hash(self) -> None:
        """If both url and source_id are empty, URN derives from canonical_key hash."""
        with JobStore(":memory:") as store:
            a = _make_job(1, title="Ops Lead", company="Acme", url="", source="ashby")
            a["source_id"] = ""
            b = _make_job(2, title="Finance Lead", company="Acme", url="", source="ashby")
            b["source_id"] = ""
            r1 = upsert_job_with_source(store, a, "ashby")
            r2 = upsert_job_with_source(store, b, "ashby")
            assert r1.outcome == "new_job"
            assert r2.outcome == "new_job"
            assert r1.job_id != r2.job_id
