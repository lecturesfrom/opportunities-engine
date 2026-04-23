"""Tests for daily ingest pipeline rewiring.

Covers:
- ingest_ats routes through upsert_job_with_source
- ingest_jobspy routes through upsert_job_with_source
- ingest_hn_hiring wired in and working
- CLI --skip-hn flag works
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from opportunities_engine.dedup.upsert import UpsertResult
from opportunities_engine.ingestion.ats import ATSClient
from opportunities_engine.storage.db import JobStore


# ---------------------------------------------------------------------------
# Helper: mock ATS client
# ---------------------------------------------------------------------------


def _make_ats_job(n: int, platform: str = "greenhouse") -> dict:
    """Build a minimal normalized job dict from ATS."""
    return {
        "source": platform,
        "source_id": str(n),
        "url": f"https://boards.{platform}.io/test/jobs/{n}",
        "title": f"GTM Engineer {n}",
        "company": "TestCo",
        "location": "Remote",
    }


# ---------------------------------------------------------------------------
# Tests: ingest_ats
# ---------------------------------------------------------------------------


class TestIngestATS:
    def test_ingest_ats_routes_through_upsert_job_with_source(self) -> None:
        """ingest_ats calls upsert_job_with_source for each job, not store.upsert_job."""
        from scripts.daily_ingest import ingest_ats

        with JobStore(":memory:") as store:
            # Mock ATSClient to return a single job
            mock_client = MagicMock(spec=ATSClient)
            job = _make_ats_job(1)
            mock_client.fetch_company.return_value = [job]

            # Mock _load_seed_companies to return a single company
            with patch("scripts.daily_ingest._load_seed_companies") as mock_load:
                mock_load.return_value = [
                    {
                        "name": "TestCo",
                        "ats_slug": "testco",
                        "ats_platform": "greenhouse",
                    }
                ]

                # Patch upsert_job_with_source to track calls
                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="greenhouse",
                    )

                    new_count = ingest_ats(store, client=mock_client)

                    # Verify upsert_job_with_source was called
                    assert mock_upsert.called
                    call_args = mock_upsert.call_args
                    assert call_args[1]["source_name"] == "greenhouse"
                    assert new_count == 1

    def test_ingest_ats_counts_new_job_outcome(self) -> None:
        """ingest_ats counts new_job outcome as new."""
        from scripts.daily_ingest import ingest_ats

        with JobStore(":memory:") as store:
            mock_client = MagicMock(spec=ATSClient)
            job1 = _make_ats_job(1)
            job2 = _make_ats_job(2)
            mock_client.fetch_company.return_value = [job1, job2]

            with patch("scripts.daily_ingest._load_seed_companies") as mock_load:
                mock_load.return_value = [
                    {
                        "name": "TestCo",
                        "ats_slug": "testco",
                        "ats_platform": "greenhouse",
                    }
                ]

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    # First job: new_job, second job: duplicate
                    mock_upsert.side_effect = [
                        UpsertResult(
                            outcome="new_job",
                            job_id=1,
                            matched_job_id=None,
                            fuzzy_score=None,
                            trust_flipped=False,
                            source_name="greenhouse",
                        ),
                        UpsertResult(
                            outcome="duplicate",
                            job_id=1,
                            matched_job_id=1,
                            fuzzy_score=None,
                            trust_flipped=False,
                            source_name="greenhouse",
                        ),
                    ]

                    new_count = ingest_ats(store, client=mock_client)
                    assert new_count == 1

    def test_ingest_ats_counts_review_flagged_as_new(self) -> None:
        """ingest_ats counts review_flagged outcome as new."""
        from scripts.daily_ingest import ingest_ats

        with JobStore(":memory:") as store:
            mock_client = MagicMock(spec=ATSClient)
            job = _make_ats_job(1)
            mock_client.fetch_company.return_value = [job]

            with patch("scripts.daily_ingest._load_seed_companies") as mock_load:
                mock_load.return_value = [
                    {
                        "name": "TestCo",
                        "ats_slug": "testco",
                        "ats_platform": "greenhouse",
                    }
                ]

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="review_flagged",
                        job_id=1,
                        matched_job_id=2,
                        fuzzy_score=94.0,
                        trust_flipped=False,
                        source_name="greenhouse",
                    )

                    new_count = ingest_ats(store, client=mock_client)
                    assert new_count == 1


# ---------------------------------------------------------------------------
# Tests: ingest_jobspy
# ---------------------------------------------------------------------------


class TestIngestJobSpy:
    def test_ingest_jobspy_routes_through_upsert_job_with_source(self) -> None:
        """ingest_jobspy calls upsert_job_with_source for each job."""
        from scripts.daily_ingest import ingest_jobspy

        with JobStore(":memory:") as store:
            job = {
                "source": "indeed",
                "source_id": "123",
                "url": "https://indeed.com/jobs/123",
                "title": "GTM Engineer",
                "company": "TestCo",
                "location": "Remote",
            }

            with patch("scripts.daily_ingest.scrape_all") as mock_scrape:
                mock_scrape.return_value = [job]

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="indeed",
                    )

                    new_count = ingest_jobspy(store)

                    # Verify upsert_job_with_source was called with source_name
                    assert mock_upsert.called
                    call_args = mock_upsert.call_args
                    assert call_args[1]["source_name"] == "indeed"
                    assert new_count == 1

    def test_ingest_jobspy_counts_outcomes_correctly(self) -> None:
        """ingest_jobspy counts new_job and review_flagged as new, others separately."""
        from scripts.daily_ingest import ingest_jobspy

        with JobStore(":memory:") as store:
            jobs = [{"source": "indeed", "url": f"https://indeed.com/jobs/{i}"} for i in range(3)]

            with patch("scripts.daily_ingest.scrape_all") as mock_scrape:
                mock_scrape.return_value = jobs

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.side_effect = [
                        UpsertResult(
                            outcome="new_job",
                            job_id=1,
                            matched_job_id=None,
                            fuzzy_score=None,
                            trust_flipped=False,
                            source_name="indeed",
                        ),
                        UpsertResult(
                            outcome="new_source",
                            job_id=1,
                            matched_job_id=1,
                            fuzzy_score=None,
                            trust_flipped=False,
                            source_name="indeed",
                        ),
                        UpsertResult(
                            outcome="duplicate",
                            job_id=1,
                            matched_job_id=1,
                            fuzzy_score=None,
                            trust_flipped=False,
                            source_name="indeed",
                        ),
                    ]

                    new_count = ingest_jobspy(store)
                    assert new_count == 1  # Only new_job counts as new


# ---------------------------------------------------------------------------
# Tests: ingest_hn_hiring
# ---------------------------------------------------------------------------


class TestIngestHNHiring:
    def test_ingest_hn_hiring_routes_through_upsert_job_with_source(self) -> None:
        """ingest_hn_hiring calls upsert_job_with_source for each job."""
        from scripts.daily_ingest import ingest_hn_hiring

        with JobStore(":memory:") as store:
            hn_job = {
                "company": "TestCo",
                "title": "Founding GTM Engineer",
                "location": "Remote",
                "is_remote": True,
                "url": "https://testco.com/jobs",
                "source": "hn_hiring",
            }

            with patch("scripts.daily_ingest.HNHiringSource") as mock_source_class:
                mock_source = MagicMock()
                mock_source.fetch.return_value = [hn_job]
                mock_source_class.return_value = mock_source

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="hn_hiring",
                    )

                    new_count = ingest_hn_hiring(store)

                    # Verify upsert_job_with_source was called with source_name="hn_hiring"
                    assert mock_upsert.called
                    call_args = mock_upsert.call_args
                    assert call_args[1]["source_name"] == "hn_hiring"
                    assert new_count == 1

    def test_ingest_hn_hiring_sets_source_field(self) -> None:
        """ingest_hn_hiring ensures source='hn_hiring' in the job dict."""
        from scripts.daily_ingest import ingest_hn_hiring

        with JobStore(":memory:") as store:
            hn_job = {
                "company": "TestCo",
                "title": "Founding GTM Engineer",
                "location": "Remote",
                "is_remote": True,
                "url": "https://testco.com/jobs",
                # Note: no 'source' field
            }

            with patch("scripts.daily_ingest.HNHiringSource") as mock_source_class:
                mock_source = MagicMock()
                mock_source.fetch.return_value = [hn_job]
                mock_source_class.return_value = mock_source

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="hn_hiring",
                    )

                    ingest_hn_hiring(store)

                    # Check the job dict passed to upsert_job_with_source
                    call_args = mock_upsert.call_args
                    job_dict = call_args[0][1]
                    assert job_dict["source"] == "hn_hiring"

    def test_ingest_hn_hiring_handles_api_failure(self) -> None:
        """ingest_hn_hiring returns 0 and logs error if HN API fails."""
        from scripts.daily_ingest import ingest_hn_hiring

        with JobStore(":memory:") as store:
            with patch("scripts.daily_ingest.HNHiringSource") as mock_source_class:
                mock_source = MagicMock()
                mock_source.fetch.side_effect = Exception("API error")
                mock_source_class.return_value = mock_source

                new_count = ingest_hn_hiring(store)
                assert new_count == 0


# ---------------------------------------------------------------------------
# Tests: CLI flags
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: ingest_wellfound
# ---------------------------------------------------------------------------


class TestIngestWellfound:
    def test_ingest_wellfound_routes_through_upsert_job_with_source(self) -> None:
        """Mock WellfoundSource.fetch → one upsert_job_with_source call with source_name='wellfound'."""
        from scripts.daily_ingest import ingest_wellfound

        with JobStore(":memory:") as store:
            wf_job = {
                "source": "wellfound",
                "source_id": "1234567",
                "url": "https://wellfound.com/jobs/1234567-senior-gtm-engineer",
                "title": "Senior GTM Engineer",
                "company": "Acme Corp",
                "location": "Remote",
                "is_remote": True,
            }

            with patch("scripts.daily_ingest.WellfoundSource") as mock_class:
                mock_instance = MagicMock()
                mock_instance.fetch.return_value = [wf_job]
                mock_class.return_value = mock_instance

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="wellfound",
                    )

                    new_count = ingest_wellfound(store)

                    assert mock_upsert.called
                    call_args = mock_upsert.call_args
                    assert call_args[1]["source_name"] == "wellfound"
                    assert new_count == 1

    def test_ingest_wellfound_sets_source_field(self) -> None:
        """ingest_wellfound overwrites source='wellfound' even if fetch returned something else."""
        from scripts.daily_ingest import ingest_wellfound

        with JobStore(":memory:") as store:
            wf_job = {
                # No source field — ingest_wellfound should set it
                "source_id": "999",
                "url": "https://wellfound.com/jobs/999-gtm",
                "title": "GTM Engineer",
                "company": "TestCo",
            }

            with patch("scripts.daily_ingest.WellfoundSource") as mock_class:
                mock_instance = MagicMock()
                mock_instance.fetch.return_value = [wf_job]
                mock_class.return_value = mock_instance

                with patch("scripts.daily_ingest.upsert_job_with_source") as mock_upsert:
                    mock_upsert.return_value = UpsertResult(
                        outcome="new_job",
                        job_id=1,
                        matched_job_id=None,
                        fuzzy_score=None,
                        trust_flipped=False,
                        source_name="wellfound",
                    )

                    ingest_wellfound(store)

                    call_args = mock_upsert.call_args
                    job_passed = call_args[0][1]
                    assert job_passed["source"] == "wellfound"

    def test_ingest_wellfound_returns_zero_on_exception(self) -> None:
        """If WellfoundSource raises, ingest_wellfound returns 0 without propagating."""
        from scripts.daily_ingest import ingest_wellfound

        with JobStore(":memory:") as store:
            with patch("scripts.daily_ingest.WellfoundSource") as mock_class:
                mock_class.side_effect = RuntimeError("scraper broken")

                new_count = ingest_wellfound(store)
                assert new_count == 0


class TestCLIFlags:
    def test_skip_hn_flag_skips_hn_ingest(self) -> None:
        """--skip-hn flag should skip the HN Hiring phase."""
        from scripts.daily_ingest import main

        runner = CliRunner()

        with patch("scripts.daily_ingest.JobStore"):
            with patch("scripts.daily_ingest.ingest_ats"):
                with patch("scripts.daily_ingest.ingest_jobspy"):
                    with patch("scripts.daily_ingest.ingest_wellfound"):
                        with patch("scripts.daily_ingest.ingest_hn_hiring") as mock_hn:
                            with patch("scripts.daily_ingest.print_new_jobs_summary"):
                                result = runner.invoke(
                                    main, ["--skip-ats", "--skip-jobspy", "--skip-hn"]
                                )

                                # HN should not be called
                                assert not mock_hn.called

    def test_no_skip_hn_flag_runs_hn_ingest(self) -> None:
        """Without --skip-hn flag, ingest_hn_hiring should be called."""
        from scripts.daily_ingest import main

        runner = CliRunner()

        with patch("scripts.daily_ingest.JobStore"):
            with patch("scripts.daily_ingest.ingest_ats"):
                with patch("scripts.daily_ingest.ingest_jobspy"):
                    with patch("scripts.daily_ingest.ingest_wellfound"):
                        with patch("scripts.daily_ingest.ingest_hn_hiring") as mock_hn:
                            mock_hn.return_value = 0
                            with patch("scripts.daily_ingest.print_new_jobs_summary"):
                                result = runner.invoke(main, ["--skip-ats", "--skip-jobspy"])

                                # HN should be called
                                assert mock_hn.called

    def test_skip_wellfound_flag_skips_wellfound_ingest(self) -> None:
        """--skip-wellfound flag should skip the Wellfound phase."""
        from scripts.daily_ingest import main

        runner = CliRunner()

        with patch("scripts.daily_ingest.JobStore"):
            with patch("scripts.daily_ingest.ingest_ats"):
                with patch("scripts.daily_ingest.ingest_jobspy"):
                    with patch("scripts.daily_ingest.ingest_wellfound") as mock_wf:
                        with patch("scripts.daily_ingest.ingest_hn_hiring"):
                            with patch("scripts.daily_ingest.print_new_jobs_summary"):
                                result = runner.invoke(
                                    main,
                                    ["--skip-ats", "--skip-jobspy", "--skip-wellfound", "--skip-hn"],
                                )
                                assert result.exit_code == 0
                                assert not mock_wf.called

    def test_no_skip_wellfound_flag_runs_wellfound_ingest(self) -> None:
        """Without --skip-wellfound, ingest_wellfound should be called."""
        from scripts.daily_ingest import main

        runner = CliRunner()

        with patch("scripts.daily_ingest.JobStore"):
            with patch("scripts.daily_ingest.ingest_ats"):
                with patch("scripts.daily_ingest.ingest_jobspy"):
                    with patch("scripts.daily_ingest.ingest_wellfound") as mock_wf:
                        mock_wf.return_value = 0
                        with patch("scripts.daily_ingest.ingest_hn_hiring"):
                            with patch("scripts.daily_ingest.print_new_jobs_summary"):
                                result = runner.invoke(
                                    main,
                                    ["--skip-ats", "--skip-jobspy", "--skip-hn"],
                                )
                                assert result.exit_code == 0
                                assert mock_wf.called


# ---------------------------------------------------------------------------
# Tests: LinkedIn default-on + --no-linkedin / --linkedin-lite CLI flags
# ---------------------------------------------------------------------------


class TestLinkedInCLIFlags:
    """Verify LinkedIn-lite is on by default and --no-linkedin disables it."""

    def _run_main(self, extra_args: list[str]) -> tuple:
        """Helper: invoke main with skip-ats, skip-hn, capture ingest_jobspy call args."""
        from scripts.daily_ingest import main

        runner = CliRunner()

        with patch("scripts.daily_ingest.JobStore"):
            with patch("scripts.daily_ingest.ingest_ats"):
                with patch("scripts.daily_ingest.ingest_jobspy") as mock_jobspy:
                    mock_jobspy.return_value = 0
                    with patch("scripts.daily_ingest.ingest_wellfound"):
                        with patch("scripts.daily_ingest.ingest_hn_hiring"):
                            with patch("scripts.daily_ingest.print_new_jobs_summary"):
                                result = runner.invoke(
                                    main,
                                    ["--skip-ats", "--skip-hn"] + extra_args,
                                )
                                return result, mock_jobspy

    def test_default_invocation_enables_linkedin_lite(self) -> None:
        """Default invocation → linkedin_lite=True propagates to ingest_jobspy."""
        result, mock_jobspy = self._run_main([])

        assert result.exit_code == 0
        assert mock_jobspy.called
        _, kwargs = mock_jobspy.call_args
        assert kwargs.get("linkedin_lite") is True

    def test_no_linkedin_flag_disables_linkedin_lite(self) -> None:
        """--no-linkedin → linkedin_lite=False propagates to ingest_jobspy."""
        result, mock_jobspy = self._run_main(["--no-linkedin"])

        assert result.exit_code == 0
        assert mock_jobspy.called
        _, kwargs = mock_jobspy.call_args
        assert kwargs.get("linkedin_lite") is False

    def test_linkedin_lite_legacy_flag_is_noop(self) -> None:
        """--linkedin-lite (legacy) is accepted and keeps linkedin_lite=True (same as default)."""
        result, mock_jobspy = self._run_main(["--linkedin-lite"])

        assert result.exit_code == 0
        assert mock_jobspy.called
        _, kwargs = mock_jobspy.call_args
        # Legacy flag is a no-op; linkedin_lite should still be True (the default)
        assert kwargs.get("linkedin_lite") is True
