"""Sanity tests for config loading."""
from opportunities_engine.config import DEFAULT_TARGET_TITLES, settings


def test_target_titles_loaded():
    assert len(settings.target_titles) >= 10
    assert "GTM Engineer" in settings.target_titles
    # F.1 curated list uses 'Founding GTM' (not 'Founding GTM Engineer')
    assert "Founding GTM" in settings.target_titles


def test_paths_anchored_to_repo():
    assert settings.repo_root.name == "opportunities-engine"
    # DB path now lives under ~/Library/Application Support/ (Phase C)
    assert "opportunities-engine" in str(settings.database_path)
    assert settings.database_path.name == "jobs.duckdb"
    assert settings.chroma_path.parent.name == "data"


def test_default_titles_constant():
    assert len(DEFAULT_TARGET_TITLES) == len(settings.target_titles)


def test_f1_curated_title_list_exact_count():
    """F.1: curated list must have exactly 43 entries."""
    assert len(DEFAULT_TARGET_TITLES) == 43


def test_f1_curated_list_contains_gtm_engineer():
    """F.1: GTM Engineer must be present."""
    assert "GTM Engineer" in DEFAULT_TARGET_TITLES


def test_f1_curated_list_excludes_forward_deployed_engineer():
    """F.1: Forward Deployed Engineer was removed from the curated list."""
    assert "Forward Deployed Engineer" not in DEFAULT_TARGET_TITLES


def test_min_relevance_score_bumped():
    """F.1: threshold bumped from 0.16 to 0.20."""
    assert settings.min_relevance_score == 0.20
