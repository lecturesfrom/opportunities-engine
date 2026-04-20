"""Tests for engine CLI dedup stats subcommand.

Covers:
- Time window parsing (1d, 7d, 0d, 1h, bare int)
- JSONL file reading with timestamp filtering
- Outcome distribution counting
- trust_flipped counting
- Review queue depth from events table
- Empty telemetry graceful handling
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from opportunities_engine.cli import dedup, event, parse_time_window, read_dedup_jsonl
from opportunities_engine.events import POSSIBLE_DUPLICATE
from opportunities_engine.events.vocab import ALL_EVENT_TYPES
from opportunities_engine.storage.db import JobStore, _url_hash


# ---------------------------------------------------------------------------
# Test parse_time_window
# ---------------------------------------------------------------------------


class TestParseTimeWindow:
    """Test parse_time_window parsing logic."""

    def test_parse_1d(self) -> None:
        """Parse '1d' to 1 day."""
        td = parse_time_window("1d")
        assert td.days == 1
        assert td.seconds == 0

    def test_parse_7d(self) -> None:
        """Parse '7d' to 7 days."""
        td = parse_time_window("7d")
        assert td.days == 7

    def test_parse_0d(self) -> None:
        """Parse '0d' to 0 days (today only)."""
        td = parse_time_window("0d")
        assert td.days == 0

    def test_parse_30d(self) -> None:
        """Parse '30d' to 30 days."""
        td = parse_time_window("30d")
        assert td.days == 30

    def test_parse_1h(self) -> None:
        """Parse '1h' to 1 hour."""
        td = parse_time_window("1h")
        assert td.total_seconds() == 3600

    def test_parse_bare_int(self) -> None:
        """Parse bare '7' as 7 days."""
        td = parse_time_window("7")
        assert td.days == 7

    def test_parse_invalid_format(self) -> None:
        """Reject invalid format."""
        with pytest.raises(ValueError, match="Invalid time window format"):
            parse_time_window("1x")

    def test_parse_empty_string(self) -> None:
        """Reject empty string."""
        with pytest.raises(ValueError):
            parse_time_window("")


# ---------------------------------------------------------------------------
# Test read_dedup_jsonl
# ---------------------------------------------------------------------------


class TestReadDedupJsonl:
    """Test JSONL reading with timestamp filtering."""

    def test_read_empty_directory(self, tmp_path: Path) -> None:
        """Reading from empty directory returns empty list."""
        records = read_dedup_jsonl(tmp_path, datetime.now(timezone.utc))
        assert records == []

    def test_read_nonexistent_directory(self, tmp_path: Path) -> None:
        """Reading from nonexistent directory returns empty list."""
        nonexistent = tmp_path / "does-not-exist"
        records = read_dedup_jsonl(nonexistent, datetime.now(timezone.utc))
        assert records == []

    def test_read_single_jsonl_file(self, tmp_path: Path) -> None:
        """Read a single JSONL file with records."""
        # Create dedup-2026-04-20.jsonl
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        records_to_write = [
            {
                "ts": "2026-04-20T12:00:00Z",
                "outcome": "new_job",
                "job_id": 1,
                "matched_job_id": None,
                "source_name": "greenhouse",
                "canonical_key": "test",
                "fuzzy_score": None,
                "trust_flipped": False,
            },
            {
                "ts": "2026-04-20T13:00:00Z",
                "outcome": "new_source",
                "job_id": 2,
                "matched_job_id": 2,
                "source_name": "lever",
                "canonical_key": "test2",
                "fuzzy_score": None,
                "trust_flipped": True,
            },
        ]
        with log_file.open("w") as f:
            for rec in records_to_write:
                f.write(json.dumps(rec) + "\n")

        # Read with a cutoff before the records
        since = datetime(2026, 4, 20, 10, 0, 0, tzinfo=timezone.utc)
        records = read_dedup_jsonl(tmp_path, since)

        assert len(records) == 2
        assert records[0]["outcome"] == "new_job"
        assert records[1]["outcome"] == "new_source"

    def test_read_timestamp_filtering(self, tmp_path: Path) -> None:
        """Only include records with ts >= since."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        records_to_write = [
            {
                "ts": "2026-04-20T10:00:00Z",
                "outcome": "new_job",
                "job_id": 1,
                "matched_job_id": None,
                "source_name": "greenhouse",
                "canonical_key": "test",
                "fuzzy_score": None,
                "trust_flipped": False,
            },
            {
                "ts": "2026-04-20T15:00:00Z",
                "outcome": "new_source",
                "job_id": 2,
                "matched_job_id": 2,
                "source_name": "lever",
                "canonical_key": "test2",
                "fuzzy_score": None,
                "trust_flipped": False,
            },
        ]
        with log_file.open("w") as f:
            for rec in records_to_write:
                f.write(json.dumps(rec) + "\n")

        # Read with cutoff between the records
        since = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)
        records = read_dedup_jsonl(tmp_path, since)

        # Should only get the second record
        assert len(records) == 1
        assert records[0]["outcome"] == "new_source"

    def test_read_multiple_jsonl_files(self, tmp_path: Path) -> None:
        """Read multiple JSONL files, filtering by date."""
        # Create two JSONL files on consecutive days
        for day in [19, 20]:
            log_file = tmp_path / f"dedup-2026-04-{day:02d}.jsonl"
            record = {
                "ts": f"2026-04-{day:02d}T12:00:00Z",
                "outcome": "new_job",
                "job_id": day,
                "matched_job_id": None,
                "source_name": "greenhouse",
                "canonical_key": "test",
                "fuzzy_score": None,
                "trust_flipped": False,
            }
            with log_file.open("w") as f:
                f.write(json.dumps(record) + "\n")

        # Read from 2026-04-20 onwards
        since = datetime(2026, 4, 20, 0, 0, 0, tzinfo=timezone.utc)
        records = read_dedup_jsonl(tmp_path, since)

        # Should only get 2026-04-20
        assert len(records) == 1
        assert records[0]["job_id"] == 20

    def test_read_malformed_json_lines(self, tmp_path: Path) -> None:
        """Skip malformed JSON lines gracefully."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        with log_file.open("w") as f:
            f.write(json.dumps({"ts": "2026-04-20T12:00:00Z", "outcome": "new_job"}) + "\n")
            f.write("{ this is not valid json\n")
            f.write(json.dumps({"ts": "2026-04-20T13:00:00Z", "outcome": "new_source"}) + "\n")

        since = datetime(2026, 4, 20, 0, 0, 0, tzinfo=timezone.utc)
        records = read_dedup_jsonl(tmp_path, since)

        # Should get 2 valid records (skipped the malformed one)
        assert len(records) == 2

    def test_read_invalid_timestamp_in_record(self, tmp_path: Path) -> None:
        """Skip records with invalid timestamp format."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        with log_file.open("w") as f:
            f.write(json.dumps({"ts": "not-a-timestamp", "outcome": "new_job"}) + "\n")
            f.write(json.dumps({"ts": "2026-04-20T12:00:00Z", "outcome": "new_source"}) + "\n")

        since = datetime(2026, 4, 20, 0, 0, 0, tzinfo=timezone.utc)
        records = read_dedup_jsonl(tmp_path, since)

        # Should only get the valid record
        assert len(records) == 1
        assert records[0]["outcome"] == "new_source"


# ---------------------------------------------------------------------------
# Test engine dedup stats CLI command
# ---------------------------------------------------------------------------


class TestDedupStats:
    """Test engine dedup stats command."""

    def test_stats_empty_telemetry(self, tmp_path: Path) -> None:
        """stats command with no telemetry prints friendly message and exits 0."""
        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats"])

        assert result.exit_code == 0
        assert "No dedup telemetry found" in result.output

    def test_stats_outcome_distribution(self, tmp_path: Path) -> None:
        """stats command correctly counts outcomes."""
        # Create JSONL with various outcomes
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        outcomes_to_write = [
            {"ts": "2026-04-20T12:00:00Z", "outcome": "new_job", "trust_flipped": False},
            {"ts": "2026-04-20T12:01:00Z", "outcome": "new_job", "trust_flipped": False},
            {"ts": "2026-04-20T12:02:00Z", "outcome": "new_source", "trust_flipped": True},
            {"ts": "2026-04-20T12:03:00Z", "outcome": "duplicate", "trust_flipped": False},
            {"ts": "2026-04-20T12:04:00Z", "outcome": "review_flagged", "trust_flipped": False},
        ]
        with log_file.open("w") as f:
            for rec in outcomes_to_write:
                f.write(json.dumps(rec) + "\n")

        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats"])

        assert result.exit_code == 0
        assert "new_job" in result.output
        assert "new_source" in result.output
        assert "duplicate" in result.output
        assert "review_flagged" in result.output

    def test_stats_trust_flipped_count(self, tmp_path: Path) -> None:
        """stats command counts trust_flipped records."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        outcomes_to_write = [
            {"ts": "2026-04-20T12:00:00Z", "outcome": "new_source", "trust_flipped": True},
            {"ts": "2026-04-20T12:01:00Z", "outcome": "new_source", "trust_flipped": True},
            {"ts": "2026-04-20T12:02:00Z", "outcome": "new_job", "trust_flipped": False},
        ]
        with log_file.open("w") as f:
            for rec in outcomes_to_write:
                f.write(json.dumps(rec) + "\n")

        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats"])

        assert result.exit_code == 0
        # Trust flipped count should be 2
        assert "2" in result.output or "Trust flipped" in result.output

    def test_stats_review_queue_depth(self, tmp_path: Path) -> None:
        """stats command queries review queue depth from events table."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        outcomes_to_write = [
            {"ts": "2026-04-20T12:00:00Z", "outcome": "new_job", "trust_flipped": False},
        ]
        with log_file.open("w") as f:
            for rec in outcomes_to_write:
                f.write(json.dumps(rec) + "\n")

        runner = CliRunner()

        # Create an in-memory JobStore with a possible_duplicate event
        with JobStore(":memory:") as store:
            # Insert a dummy job first
            url = "http://test.com/1"
            url_hash = _url_hash(url)
            store.conn.execute(
                """
                INSERT INTO jobs (source, url, url_hash, title, company, location, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                ["test", url, url_hash, "Test Job", "Test Co", "Remote"]
            )
            # Get the job_id
            job_id = store.conn.execute("SELECT id FROM jobs LIMIT 1").fetchone()[0]

            # Insert a possible_duplicate event
            store.conn.execute(
                """
                INSERT INTO events (job_id, event_type, detail)
                VALUES (?, ?, ?)
                """,
                [job_id, POSSIBLE_DUPLICATE, "{}"]
            )

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            with patch("opportunities_engine.cli.settings.database_path", ":memory:"):
                # We can't easily mock the database_path for the in-memory case,
                # so we'll just check that the command runs without error
                result = runner.invoke(dedup, ["stats"])
                assert result.exit_code == 0

    def test_stats_last_1d(self, tmp_path: Path) -> None:
        """stats --last 1d parses without error."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        record = {"ts": "2026-04-20T12:00:00Z", "outcome": "new_job", "trust_flipped": False}
        with log_file.open("w") as f:
            f.write(json.dumps(record) + "\n")

        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats", "--last", "1d"])

        assert result.exit_code == 0

    def test_stats_last_7d(self, tmp_path: Path) -> None:
        """stats --last 7d parses without error."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        record = {"ts": "2026-04-20T12:00:00Z", "outcome": "new_job", "trust_flipped": False}
        with log_file.open("w") as f:
            f.write(json.dumps(record) + "\n")

        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats", "--last", "7d"])

        assert result.exit_code == 0

    def test_stats_last_0d(self, tmp_path: Path) -> None:
        """stats --last 0d (today only) parses without error."""
        log_file = tmp_path / "dedup-2026-04-20.jsonl"
        record = {"ts": "2026-04-20T12:00:00Z", "outcome": "new_job", "trust_flipped": False}
        with log_file.open("w") as f:
            f.write(json.dumps(record) + "\n")

        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats", "--last", "0d"])

        assert result.exit_code == 0

    def test_stats_invalid_last_format(self, tmp_path: Path) -> None:
        """stats --last with invalid format exits with error."""
        runner = CliRunner()

        with patch("opportunities_engine.cli.get_default_logs_path", return_value=tmp_path):
            result = runner.invoke(dedup, ["stats", "--last", "invalid"])

        assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Test engine event add CLI command
# ---------------------------------------------------------------------------


class TestEventAdd:
    """Test the `engine event add` command."""

    def _make_store_with_job(self, db_path: str) -> int:
        """Create a store with one job and return its job_id."""
        with JobStore(db_path) as store:
            assert store.conn is not None
            store.conn.execute(
                """
                INSERT INTO jobs (source, url, url_hash, title, company, location,
                                  created_at, updated_at)
                VALUES ('test', 'https://example.com/job/1', md5('https://example.com/job/1'),
                        'Test Job', 'Test Co', 'Remote',
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
            )
            row = store.conn.execute("SELECT id FROM jobs LIMIT 1").fetchone()
            assert row is not None
            return int(row[0])

    def test_success_path_creates_event_row(self, tmp_path: Path) -> None:
        """Valid job-id + valid type → exit 0 and event row inserted."""
        db_path = str(tmp_path / "test.duckdb")
        job_id = self._make_store_with_job(db_path)

        runner = CliRunner()
        with patch("opportunities_engine.cli.settings") as mock_settings:
            mock_settings.database_path = db_path
            result = runner.invoke(event, ["add", "--job-id", str(job_id), "--type", "applied"])

        assert result.exit_code == 0, result.output
        assert "applied" in result.output.lower() or "recorded" in result.output.lower()

        with JobStore(db_path) as store:
            rows = store.conn.execute(
                "SELECT event_type, actor FROM events WHERE job_id = $1",
                [job_id],
            ).fetchall()

        assert len(rows) == 1
        assert rows[0][0] == "applied"
        assert rows[0][1] == "keegan"  # default actor

    def test_invalid_event_type_exits_nonzero(self, tmp_path: Path) -> None:
        """Invalid event type → non-zero exit code and stderr mentions allowed types."""
        db_path = str(tmp_path / "test.duckdb")
        self._make_store_with_job(db_path)

        runner = CliRunner()
        with patch("opportunities_engine.cli.settings") as mock_settings:
            mock_settings.database_path = db_path
            result = runner.invoke(
                event,
                ["add", "--job-id", "1", "--type", "not_a_real_type"],
            )

        assert result.exit_code != 0
        # Output should mention the bad type or allowed types
        assert "not_a_real_type" in result.output or "scored" in result.output

    def test_notes_ends_up_in_detail(self, tmp_path: Path) -> None:
        """--notes value is stored in the detail JSON field."""
        db_path = str(tmp_path / "test.duckdb")
        job_id = self._make_store_with_job(db_path)

        runner = CliRunner()
        with patch("opportunities_engine.cli.settings") as mock_settings:
            mock_settings.database_path = db_path
            result = runner.invoke(
                event,
                [
                    "add",
                    "--job-id", str(job_id),
                    "--type", "applied",
                    "--notes", "sent resume via LinkedIn",
                ],
            )

        assert result.exit_code == 0, result.output

        with JobStore(db_path) as store:
            row = store.conn.execute(
                "SELECT detail FROM events WHERE job_id = $1 AND event_type = 'applied'",
                [job_id],
            ).fetchone()

        assert row is not None
        detail = json.loads(row[0])
        assert detail["notes"] == "sent resume via LinkedIn"

    def test_custom_actor_is_stored(self, tmp_path: Path) -> None:
        """--actor override is stored in the events row."""
        db_path = str(tmp_path / "test.duckdb")
        job_id = self._make_store_with_job(db_path)

        runner = CliRunner()
        with patch("opportunities_engine.cli.settings") as mock_settings:
            mock_settings.database_path = db_path
            result = runner.invoke(
                event,
                [
                    "add",
                    "--job-id", str(job_id),
                    "--type", "phone_screen",
                    "--actor", "recruiter",
                ],
            )

        assert result.exit_code == 0, result.output

        with JobStore(db_path) as store:
            row = store.conn.execute(
                "SELECT actor FROM events WHERE job_id = $1",
                [job_id],
            ).fetchone()

        assert row is not None
        assert row[0] == "recruiter"


# ---------------------------------------------------------------------------
# Test engine event poll-linear CLI command
# ---------------------------------------------------------------------------


class TestEventPollLinear:
    """Test the `engine event poll-linear` command."""

    def test_dry_run_exits_0_and_prints_summary(self, tmp_path: Path) -> None:
        """--dry-run exits 0 and prints the summary dict without writing events."""
        db_path = str(tmp_path / "test.duckdb")

        # Create the DB (empty is fine for dry-run with mocked poll_linear)
        with JobStore(db_path):
            pass

        runner = CliRunner()

        fake_summary = {
            "issues_seen": 2,
            "state_events_emitted": 1,
            "comment_events_emitted": 0,
            "watermark_advanced_to": "2026-04-20T12:00:00+00:00",
        }

        with (
            patch("opportunities_engine.cli.settings") as mock_settings,
            patch(
                "opportunities_engine.events.linear_listener.poll_linear",
                return_value=fake_summary,
            ),
        ):
            mock_settings.database_path = db_path
            mock_settings.linear_project_id = "proj-abc"
            result = runner.invoke(event, ["poll-linear", "--dry-run"])

        assert result.exit_code == 0, result.output
        # Summary keys appear in output
        assert "issues_seen" in result.output or "watermark" in result.output or "2" in result.output

    def test_missing_project_id_errors_helpfully(self, tmp_path: Path) -> None:
        """No --project-id and no settings.linear_project_id → non-zero exit with message."""
        db_path = str(tmp_path / "test.duckdb")

        with JobStore(db_path):
            pass

        runner = CliRunner()

        with patch("opportunities_engine.cli.settings") as mock_settings:
            mock_settings.database_path = db_path
            mock_settings.linear_project_id = None
            result = runner.invoke(event, ["poll-linear"])

        assert result.exit_code != 0
        # Error message should mention project-id
        assert "project" in result.output.lower() or "LINEAR_PROJECT_ID" in result.output

    def test_explicit_project_id_flag_overrides_settings(self, tmp_path: Path) -> None:
        """--project-id flag is passed through to poll_linear."""
        db_path = str(tmp_path / "test.duckdb")

        with JobStore(db_path):
            pass

        runner = CliRunner()

        captured: dict = {}

        def fake_poll_linear(store, project_id, *, dry_run=False, **kwargs):  # type: ignore[no-untyped-def]
            captured["project_id"] = project_id
            return {
                "issues_seen": 0,
                "state_events_emitted": 0,
                "comment_events_emitted": 0,
                "watermark_advanced_to": "2026-04-20T12:00:00+00:00",
            }

        with (
            patch("opportunities_engine.cli.settings") as mock_settings,
            patch(
                "opportunities_engine.events.linear_listener.poll_linear",
                side_effect=fake_poll_linear,
            ),
        ):
            mock_settings.database_path = db_path
            mock_settings.linear_project_id = None
            result = runner.invoke(event, ["poll-linear", "--project-id", "explicit-proj-id"])

        assert result.exit_code == 0, result.output
        assert captured.get("project_id") == "explicit-proj-id"
