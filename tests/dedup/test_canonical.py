"""Tests for canonical_job_key and normalization helpers.

Covers at least 20 cases:
- Title synonym expansion
- Whitespace collapsing
- Punctuation stripping (keep hyphens)
- Accent stripping
- Company suffix stripping
- Location aliasing
- Remote aliases
- Edge cases: empty strings, extra punctuation, mixed case
"""

import pytest

from opportunities_engine.dedup.canonical import (
    canonical_job_key,
    normalize_company,
    normalize_location,
    normalize_title,
)


class TestNormalizeTitle:
    def test_lowercase(self) -> None:
        assert normalize_title("Senior Engineer") == "senior engineer"

    def test_synonym_sr_to_senior(self) -> None:
        assert normalize_title("Sr Engineer") == "senior engineer"

    def test_synonym_jr_to_junior(self) -> None:
        assert normalize_title("Jr Engineer") == "junior engineer"

    def test_synonym_swe_to_software_engineer(self) -> None:
        assert normalize_title("SWE") == "software engineer"

    def test_synonym_pm_to_product_manager(self) -> None:
        assert normalize_title("PM") == "product manager"

    def test_synonym_eng_to_engineer(self) -> None:
        assert normalize_title("Eng Manager") == "engineer manager"

    def test_synonym_mgr_to_manager(self) -> None:
        assert normalize_title("Sr Mgr") == "senior manager"

    def test_synonym_sre_to_site_reliability_engineer(self) -> None:
        assert normalize_title("SRE") == "site reliability engineer"

    def test_whitespace_collapsing(self) -> None:
        assert normalize_title("  Senior   Engineer  ") == "senior engineer"

    def test_punctuation_stripped(self) -> None:
        assert normalize_title("Software Engineer (Backend)") == "software engineer backend"

    def test_hyphens_kept(self) -> None:
        """Hyphens should be preserved."""
        result = normalize_title("Full-Stack Engineer")
        assert "full-stack" in result

    def test_accent_stripping(self) -> None:
        """Accented characters should be reduced to ASCII equivalents."""
        result = normalize_title("Engenheiro Sênior")
        assert "senior" in result
        assert "ê" not in result

    def test_sr_engineer_same_as_senior_engineer(self) -> None:
        """sr engineer and senior engineer should normalize identically."""
        assert normalize_title("Sr Engineer") == normalize_title("Senior Engineer")

    def test_mixed_case(self) -> None:
        assert normalize_title("SENIOR SOFTWARE ENGINEER") == "senior software engineer"

    def test_empty_string(self) -> None:
        assert normalize_title("") == ""


class TestNormalizeCompany:
    def test_strips_inc(self) -> None:
        assert normalize_company("Stripe Inc") == "stripe"

    def test_strips_inc_with_dot(self) -> None:
        assert normalize_company("Stripe Inc.") == "stripe"

    def test_strips_llc(self) -> None:
        assert normalize_company("OpenAI LLC") == "openai"

    def test_strips_corp(self) -> None:
        assert normalize_company("Acme Corp") == "acme"

    def test_strips_ltd(self) -> None:
        assert normalize_company("Example Ltd") == "example"

    def test_strips_comma_inc(self) -> None:
        assert normalize_company("Stripe, Inc.") == "stripe"

    def test_strips_gmbh(self) -> None:
        assert normalize_company("Example GmbH") == "example"

    def test_lowercase(self) -> None:
        assert normalize_company("GOOGLE") == "google"

    def test_whitespace_collapse(self) -> None:
        assert normalize_company("  Acme  Corp  ") == "acme"

    def test_empty_string(self) -> None:
        assert normalize_company("") == ""


class TestNormalizeLocation:
    def test_sf_alias(self) -> None:
        assert normalize_location("SF") == "san francisco"

    def test_san_francisco_ca(self) -> None:
        assert normalize_location("San Francisco CA") == "san francisco"

    def test_sf_bay_area(self) -> None:
        assert normalize_location("SF Bay Area") == "san francisco"

    def test_bay_area(self) -> None:
        assert normalize_location("Bay Area") == "san francisco"

    def test_nyc_alias(self) -> None:
        assert normalize_location("NYC") == "new york"

    def test_new_york_ny(self) -> None:
        assert normalize_location("New York NY") == "new york"

    def test_new_york_city(self) -> None:
        assert normalize_location("New York City") == "new york"

    def test_la_alias(self) -> None:
        assert normalize_location("LA") == "los angeles"

    def test_los_angeles_ca(self) -> None:
        assert normalize_location("Los Angeles CA") == "los angeles"

    def test_dc_alias(self) -> None:
        assert normalize_location("DC") == "washington"

    def test_washington_dc(self) -> None:
        assert normalize_location("Washington DC") == "washington"

    def test_remote_passthrough(self) -> None:
        assert normalize_location("Remote") == "remote"

    def test_wfh_alias(self) -> None:
        assert normalize_location("WFH") == "remote"

    def test_anywhere_alias(self) -> None:
        assert normalize_location("Anywhere") == "remote"

    def test_work_from_home_alias(self) -> None:
        assert normalize_location("Work From Home") == "remote"

    def test_seattle_wa(self) -> None:
        assert normalize_location("Seattle WA") == "seattle"

    def test_austin_tx(self) -> None:
        assert normalize_location("Austin TX") == "austin"

    def test_boston_ma(self) -> None:
        assert normalize_location("Boston MA") == "boston"

    def test_chicago_il(self) -> None:
        assert normalize_location("Chicago IL") == "chicago"

    def test_empty_string(self) -> None:
        assert normalize_location("") == ""

    def test_passthrough_unknown(self) -> None:
        """Unknown locations should be returned normalized but not aliased."""
        result = normalize_location("Portland OR")
        assert result == "portland or"


class TestCanonicalJobKey:
    def test_basic_key_format(self) -> None:
        key = canonical_job_key("Senior Engineer", "Stripe Inc", "Remote")
        assert key == "senior engineer|stripe|remote"

    def test_two_pipes_in_result(self) -> None:
        key = canonical_job_key("Engineer", "Acme", "SF")
        assert key.count("|") == 2

    def test_synonym_expansion_in_key(self) -> None:
        key1 = canonical_job_key("Sr Engineer", "Stripe", "Remote")
        key2 = canonical_job_key("Senior Engineer", "Stripe", "Remote")
        assert key1 == key2

    def test_location_alias_in_key(self) -> None:
        key1 = canonical_job_key("Engineer", "Acme", "SF")
        key2 = canonical_job_key("Engineer", "Acme", "San Francisco CA")
        assert key1 == key2

    def test_company_suffix_in_key(self) -> None:
        key1 = canonical_job_key("Engineer", "Stripe Inc", "Remote")
        key2 = canonical_job_key("Engineer", "Stripe LLC", "Remote")
        assert key1 == key2

    def test_accent_stripping_in_key(self) -> None:
        key = canonical_job_key("Engenheiro", "Empresa", "São Paulo")
        assert "ã" not in key
        assert "ê" not in key

    def test_sao_paulo_accent_stripped(self) -> None:
        key = canonical_job_key("Engineer", "Acme", "São Paulo")
        assert "sao paulo" in key

    def test_wfh_normalizes_to_remote(self) -> None:
        key1 = canonical_job_key("Engineer", "Acme", "WFH")
        key2 = canonical_job_key("Engineer", "Acme", "Remote")
        assert key1 == key2

    def test_empty_fields(self) -> None:
        key = canonical_job_key("", "", "")
        assert key == "||"

    def test_all_spaces_fields(self) -> None:
        key = canonical_job_key("  ", "  ", "  ")
        assert key == "||"

    def test_extra_punctuation_stripped(self) -> None:
        key = canonical_job_key("Engineer!", "Acme!!!", "Remote???")
        assert "!" not in key
        assert "?" not in key

    def test_mixed_case_all_lowercase(self) -> None:
        key = canonical_job_key("SENIOR ENGINEER", "STRIPE INC", "REMOTE")
        assert key == key.lower()
