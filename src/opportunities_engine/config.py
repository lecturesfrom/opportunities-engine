"""Configuration — loads .env, exposes typed settings."""
import os
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


def get_default_db_path() -> Path:
    """Return the default DB path under ~/Library/Application Support/.

    Creates the parent directory if it doesn't exist.
    On non-macOS systems, falls back to ~/.opportunities-engine/.
    """
    if os.name == "posix" and Path.home().joinpath("Library", "Application Support").exists():
        base = Path.home() / "Library" / "Application Support" / "opportunities-engine"
    else:
        base = Path.home() / ".opportunities-engine"

    base.mkdir(parents=True, exist_ok=True)
    return base / "jobs.duckdb"


def get_default_logs_path() -> Path:
    """Return the default logs directory.

    Creates the directory if it doesn't exist.
    macOS: ~/Library/Logs/opportunities-engine
    Linux fallback: ~/.opportunities-engine/logs
    """
    if os.name == "posix" and Path.home().joinpath("Library", "Logs").exists():
        base = Path.home() / "Library" / "Logs" / "opportunities-engine"
    else:
        base = Path.home() / ".opportunities-engine" / "logs"

    base.mkdir(parents=True, exist_ok=True)
    return base

# User-curated GTME + adjacent role universe (flat, no weights)
DEFAULT_TARGET_TITLES: list[str] = [
    # Core GTM / Revenue
    "GTM Engineer",
    "Go-To-Market Engineer",
    "Founding GTM",
    "GTM Systems Engineer",
    "Go-To-Market Systems Engineer",
    "GTM Operations Engineer",
    "GTM Data Engineer",
    "GTM Automation Specialist",
    "Revenue Architect",
    "Revenue Systems Engineer",
    "Director of Revenue Systems",
    "RevOps Engineer",
    "Revenue Operations Consultant",
    "Principal GTM Engineer",
    "Staff GTM Engineer",
    "VP Revenue Systems",
    "AI Transformation Lead",
    "Revenue Solution Architect",
    # Growth
    "Growth Engineer",
    "Founding Growth",
    "Head of Growth",
    "Growth Lead",
    "Growth Hacker",
    "Growth Product Manager",
    # Growth-adjacent engineering
    "Software Engineer (Growth)",
    "Engineer, Growth",
    "Full Stack Engineer (GTM-adjacent startup)",
    # Product — entry-level only
    "Associate Product Manager",
    "APM",
    "Product Management Intern",
    "Product Intern",
    "PM Intern",
    "Junior Product Manager",
    "Product Analyst",
    "Rotational Product Manager",
    "Product Operations Analyst",
    # Sales / BDR — GTM-coded
    "AI BDR",
    "BDR",
    "SDR",
    "Sales Development Representative",
    "Business Development Engineer",
    "GTM Lead",
    "Revenue Lead",
]

# US/Remote gate
US_LOCATION_PATTERNS: list[str] = [
    "remote", "united states", "united states only", "usa", "u.s.", "us", "u.s.a"
]
NON_US_LOCATION_PATTERNS: list[str] = [
    "emea", "apac", "europe", "uk", "united kingdom", "london", "paris", "berlin",
    "poland", "brazil", "india", "canada", "australia", "singapore", "tokyo", "france", "germany",
    "italy", "spain", "netherlands", "sweden", "norway", "denmark", "ireland", "portugal",
    "japan", "korea", "china", "taiwan", "hong kong", "mexico", "argentina", "chile",
    "new zealand", "south africa", "uae", "dubai"
]


class Settings(BaseSettings):
    """Top-level settings. Reads from .env at repo root."""

    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Secrets / integrations
    linear_api_key: Optional[str] = None
    linear_workspace_slug: Optional[str] = None
    linear_team_name: Optional[str] = None
    linear_project_id: Optional[str] = None
    discord_webhook_url: Optional[str] = None
    heyreach_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    openrouter_api_key: Optional[str] = None

    # Behavior
    target_titles: list[str] = Field(default_factory=lambda: list(DEFAULT_TARGET_TITLES))
    database_path: Path = Field(default_factory=get_default_db_path)
    chroma_path: Path = REPO_ROOT / "data" / "chroma"
    dream_companies_path: Path = REPO_ROOT / "data" / "dream_companies.json"
    seed_companies_path: Path = REPO_ROOT / "data" / "seed_companies.json"
    us_remote_only: bool = True
    min_relevance_score: float = 0.20  # Bumped 0.16 → 0.20 with F.1 curated title list.
    # Bump on any algorithm change (Phase F consumer: scores.ranker_version audit).
    ranker_version: str = "f.2-tfidf-v1"
    max_daily_shortlist: int = 25

    # Dedup pipeline thresholds
    dedup_threshold: int = 95
    dedup_review_floor: int = 93

    @property
    def repo_root(self) -> Path:
        return REPO_ROOT


settings = Settings()
