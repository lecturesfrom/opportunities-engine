"""Location alias → canonical location name dictionary.

Keys are lowercased/stripped variants; values are the canonical form.
Applied after the general normalization pass (lowercase, accent strip, punct strip).
"""

LOCATION_NORMALIZERS: dict[str, str] = {
    # San Francisco / Bay Area
    "sf": "san francisco",
    "san francisco ca": "san francisco",
    "san francisco california": "san francisco",
    "sf bay area": "san francisco",
    "bay area": "san francisco",
    # New York
    "nyc": "new york",
    "new york ny": "new york",
    "new york city": "new york",
    "ny": "new york",
    "new york new york": "new york",
    # Los Angeles
    "la": "los angeles",
    "los angeles ca": "los angeles",
    "los angeles california": "los angeles",
    # Washington DC
    "dc": "washington",
    "washington dc": "washington",
    "washington d c": "washington",
    # Remote / Distributed
    "remote": "remote",
    "wfh": "remote",
    "anywhere": "remote",
    "work from home": "remote",
    "fully remote": "remote",
    "distributed": "remote",
    # Seattle
    "seattle wa": "seattle",
    "seattle washington": "seattle",
    # Austin
    "austin tx": "austin",
    "austin texas": "austin",
    # Boston
    "boston ma": "boston",
    "boston massachusetts": "boston",
    # Chicago
    "chicago il": "chicago",
    "chicago illinois": "chicago",
    # Denver
    "denver co": "denver",
    "denver colorado": "denver",
    # Atlanta
    "atlanta ga": "atlanta",
    "atlanta georgia": "atlanta",
}
