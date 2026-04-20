"""Sanity tests for config loading."""
from opportunities_engine.config import DEFAULT_TARGET_TITLES, settings


def test_target_titles_loaded():
    assert len(settings.target_titles) >= 10
    assert "GTM Engineer" in settings.target_titles
    # user-curated title universe now uses 'Founding GTM' (not 'Founding GTM Engineer')
    assert "Founding GTM" in settings.target_titles
    assert "Forward Deployed Engineer" in settings.target_titles
    assert "Technical Product Manager" in settings.target_titles


def test_paths_anchored_to_repo():
    assert settings.repo_root.name == "opportunities-engine"
    # DB path now lives under ~/Library/Application Support/ (Phase C)
    assert "opportunities-engine" in str(settings.database_path)
    assert settings.database_path.name == "jobs.duckdb"
    assert settings.chroma_path.parent.name == "data"


def test_default_titles_constant():
    assert len(DEFAULT_TARGET_TITLES) == len(settings.target_titles)
