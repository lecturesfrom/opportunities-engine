"""Configuration — loads .env, exposes typed settings."""
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]

# Primary + adjacent title aliases. Used as the semantic query profile.
DEFAULT_TARGET_TITLES: list[str] = [
    # Primary sweet spot
    "Founding GTM Engineer",
    "GTM Engineer",
    "Go-To-Market Engineer",
    "Founding Growth Engineer",
    # Adjacent (same role, different naming)
    "Growth Engineer",
    "Forward Deployed Engineer",
    "Solutions Engineer",
    "Sales Engineer",
    "AI Solutions Engineer",
    "Customer Engineer",
    "RevOps Engineer",
    "Founding Sales Engineer",
    "Head of Growth",
    "Founding Growth",
    # Product lean (only GTM-adjacent early stage)
    "Founding Product Engineer",
    "Product Engineer",
]


class Settings(BaseSettings):
    """Top-level settings. Reads from .env at the repo root."""

    model_config = SettingsConfigDict(
        env_file=str(REPO_ROOT / ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Linear
    linear_api_key: Optional[str] = None
    linear_workspace_slug: Optional[str] = None
    linear_team_name: Optional[str] = None

    # Alerts
    discord_webhook_url: Optional[str] = None

    # Outreach
    heyreach_api_key: Optional[str] = None

    # LLMs
    openai_api_key: Optional[str] = None
    anthropic_api_key: Optional[str] = None

    # Behavior
    target_titles: list[str] = Field(default_factory=lambda: list(DEFAULT_TARGET_TITLES))
    database_path: Path = REPO_ROOT / "data" / "jobs.duckdb"
    chroma_path: Path = REPO_ROOT / "data" / "chroma"
    dream_companies_path: Path = REPO_ROOT / "data" / "dream_companies.json"
    linear_ids_path: Path = REPO_ROOT / "data" / "linear_ids.json"

    @property
    def repo_root(self) -> Path:
        return REPO_ROOT


settings = Settings()
