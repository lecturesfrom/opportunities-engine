"""Canonical key generation for job deduplication.

canonical_job_key(title, company, location) -> str
Steps (per brief):
  1. lowercase, strip accents
  2. strip punctuation except hyphens
  3. collapse internal whitespace
  4. apply TITLE_SYNONYMS word-by-word
  5. apply LOCATION_NORMALIZERS
  6. company via suffix stripper + COMPANY_ALIASES
  7. return "{title_normalized}|{company_normalized}|{location_normalized}"
"""

import re
import unicodedata

from opportunities_engine.dedup.locations import LOCATION_NORMALIZERS
from opportunities_engine.dedup.title_synonyms import TITLE_SYNONYMS

# Company legal suffixes to strip (order matters: longer before shorter)
_COMPANY_SUFFIXES = (
    "incorporated",
    "corporation",
    "company",
    "limited",
    "gmbh",
    "inc",
    "llc",
    "corp",
    "ltd",
    "co",
)

# Small set of well-known company aliases (brand → canonical)
_COMPANY_ALIASES: dict[str, str] = {
    # Add entries here as they become useful; keep it minimal
}


def _strip_accents(text: str) -> str:
    """Decompose accented characters and drop combining marks."""
    nfd = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")


def _base_normalize(text: str) -> str:
    """Step 1–3: lowercase → strip accents → strip punctuation (keep hyphens) → collapse whitespace."""
    text = text.lower()
    text = _strip_accents(text)
    # Strip punctuation except hyphens (keep a-z, 0-9, space, hyphen)
    text = re.sub(r"[^a-z0-9 \-]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_title(title: str) -> str:
    """Normalize a job title: base normalization + synonym expansion word-by-word."""
    text = _base_normalize(title)
    words = text.split()
    expanded: list[str] = []
    for word in words:
        replacement = TITLE_SYNONYMS.get(word)
        if replacement is not None:
            # The replacement may itself be multiple words
            expanded.extend(replacement.split())
        else:
            expanded.append(word)
    return " ".join(expanded)


def normalize_company(name: str) -> str:
    """Normalize a company name: base normalization → alias lookup → suffix stripping.

    Public helper — used by the SQL pre-filter.
    """
    text = _base_normalize(name)

    # Alias lookup (pre-suffix)
    if text in _COMPANY_ALIASES:
        return _COMPANY_ALIASES[text]

    # Strip trailing legal suffixes (allow a trailing comma/space before the suffix)
    for suffix in _COMPANY_SUFFIXES:
        # Match suffix at end, optionally preceded by comma or space
        pattern = r"[\s,]+\b" + re.escape(suffix) + r"\b\.?$"
        stripped = re.sub(pattern, "", text).strip()
        if stripped and stripped != text:
            text = stripped
            break  # Only strip one suffix (outermost)

    # Post-strip alias lookup
    if text in _COMPANY_ALIASES:
        return _COMPANY_ALIASES[text]

    return text


def normalize_location(loc: str) -> str:
    """Normalize a location string: base normalization → alias lookup.

    Public helper — used by the SQL pre-filter.
    """
    text = _base_normalize(loc)

    # Direct lookup in normalizer map
    if text in LOCATION_NORMALIZERS:
        return LOCATION_NORMALIZERS[text]

    # Try stripping trailing country-level noise (e.g. "san francisco, ca, us" → "san francisco ca")
    # and looking up again — covers multi-part strings
    text_no_comma = re.sub(r",\s*", " ", text)
    text_no_comma = re.sub(r"\s+", " ", text_no_comma).strip()
    if text_no_comma in LOCATION_NORMALIZERS:
        return LOCATION_NORMALIZERS[text_no_comma]

    return text


def canonical_job_key(title: str, company: str, location: str) -> str:
    """Return a canonical dedup key: '{title_normalized}|{company_normalized}|{location_normalized}'.

    Steps follow the brief exactly (1–7).
    """
    t = normalize_title(title)
    c = normalize_company(company)
    l = normalize_location(location)
    return f"{t}|{c}|{l}"
