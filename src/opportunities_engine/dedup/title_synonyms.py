"""Title abbreviation → expansion dictionary for canonical key normalization.

Every entry must be an unambiguous abbreviation. Keep this dict small and defensible.
Applied word-by-word after lowercasing and accent stripping.
"""

TITLE_SYNONYMS: dict[str, str] = {
    # Seniority
    "sr": "senior",
    "jr": "junior",
    # Role abbreviations
    "swe": "software engineer",
    "pm": "product manager",
    "eng": "engineer",
    "mgr": "manager",
    "dir": "director",
    "vp": "vice president",
    "ceo": "chief executive officer",
    "cto": "chief technology officer",
    "cfo": "chief financial officer",
    "coo": "chief operating officer",
    # Function abbreviations
    "hr": "human resources",
    "qa": "quality assurance",
    "ui": "user interface",
    "ux": "user experience",
    "ml": "machine learning",
    "ai": "artificial intelligence",
    "sre": "site reliability engineer",
}
