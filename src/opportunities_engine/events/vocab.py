"""Event type vocabulary. Every event_type written to the events table
MUST be one of these constants.

Terminal events (consumed by Phase F to exclude jobs from re-ranking):
  OFFER, REJECTED, WITHDREW
"""

POSSIBLE_DUPLICATE = "possible_duplicate"
SCORED = "scored"
PUSHED_TO_LINEAR = "pushed_to_linear"
APPLIED = "applied"
PHONE_SCREEN = "phone_screen"
INTERVIEW = "interview"
OFFER = "offer"
REJECTED = "rejected"
WITHDREW = "withdrew"

ALL_EVENT_TYPES: frozenset[str] = frozenset({
    POSSIBLE_DUPLICATE, SCORED, PUSHED_TO_LINEAR,
    APPLIED, PHONE_SCREEN, INTERVIEW,
    OFFER, REJECTED, WITHDREW,
})

TERMINAL_EVENT_TYPES: frozenset[str] = frozenset({
    OFFER, REJECTED, WITHDREW,
})
