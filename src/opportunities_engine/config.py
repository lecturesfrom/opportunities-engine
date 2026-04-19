"""Configuration — loads .env, exposes typed settings."""
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]

# User-curated GTME + adjacent role universe (flat, no weights)
DEFAULT_TARGET_TITLES: list[str] = [
    # Core GTME
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
    # Technical / Deployed
    "Forward Deployed Engineer",
    "Solutions Engineer",
    "Sales Engineer",
    "Founding Sales Engineer",
    "Customer Engineer",
    "AI Solutions Engineer",
    "AI Sales Engineer",
    "Technical Account Manager",
    # Growth
    "Growth Engineer",
    "Founding Growth",
    "Head of Growth",
    "Growth Lead",
    "Growth Hacker",
    # Builder / Product Path
    "Product Engineer",
    "Product Manager",
    "Associate Product Manager",
    "Technical Product Manager",
    "Product Lead",
    "Growth Product Manager",
    "Founding Engineer",
    "Software Engineer (Growth)",
    "Engineer, Growth",
    "Full Stack Engineer (GTM-adjacent startup)",
    # Sales & Commercial
    "Sales Lead",
    "Account Executive (Technical)",
    "Founding AE",
    "Revenue Lead",
    "Commercial Lead",
    "Business Development Engineer",
    # Emerging / Watch List
    "Principal GTM Engineer",
    "Staff GTM Engineer",
    "VP Revenue Systems",
    "AI Transformation Lead",
    "Revenue Solution Architect",
    "AI BDR",
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
    discord_webhook_url: Optional[str] = None
    heyreach_api_key: Optional[str] = None
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None
    openrouter_api_key: Optional[str] = None

    # Behavior
    target_titles: list[str] = Field(default_factory=lambda: list(DEFAULT_TARGET_TITLES))
    database_path: Path = REPO_ROOT / "data" / "jobs.duckdb"
    chroma_path: Path = REPO_ROOT / "data" / "chroma"
    dream_companies_path: Path = REPO_ROOT / "data" / "dream_companies.json"
    seed_companies_path: Path = REPO_ROOT / "data" / "seed_companies.json"
    us_remote_only: bool = True
    min_relevance_score: float = 0.16
    max_daily_shortlist: int = 25

    @property
    def repo_root(self) -> Path:
        return REPO_ROOT


settings = Settings()
