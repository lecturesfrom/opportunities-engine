"""Events package for opportunities-engine.

Re-exports emit_event and all vocabulary constants so that existing imports
such as `from opportunities_engine.events import POSSIBLE_DUPLICATE` continue
to work unchanged.
"""

from opportunities_engine.events.emitter import emit_event
from opportunities_engine.events.vocab import (
    ALL_EVENT_TYPES,
    APPLIED,
    INTERVIEW,
    OFFER,
    PHONE_SCREEN,
    POSSIBLE_DUPLICATE,
    PUSHED_TO_LINEAR,
    REJECTED,
    SCORED,
    TERMINAL_EVENT_TYPES,
    WITHDREW,
)

__all__ = [
    "emit_event",
    "ALL_EVENT_TYPES",
    "APPLIED",
    "INTERVIEW",
    "OFFER",
    "PHONE_SCREEN",
    "POSSIBLE_DUPLICATE",
    "PUSHED_TO_LINEAR",
    "REJECTED",
    "SCORED",
    "TERMINAL_EVENT_TYPES",
    "WITHDREW",
]
