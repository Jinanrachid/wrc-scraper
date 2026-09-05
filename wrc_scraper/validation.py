"""Client-side input validation for the WRC spider (Phase 2, Decision 7).

Every case here is decided from evidence gathered directly against the live
site (docs/SCRAPY_EXPERIMENTS.md) or from what a caller can know for free
without a request -- see the plan's Decision 7 table for the reasoning behind
each one.
"""

from __future__ import annotations

from datetime import date

# Single source of truth for known bodies (id -> slug + display name).
from wrc_scraper.bodies import KNOWN_BODY_IDS


def parse_iso_date(raw: str) -> date:
    """Parse a YYYY-MM-DD string, raising a ValueError that names the bad
    input for unparseable or impossible (e.g. "2024-02-30") dates, rather
    than letting date.fromisoformat's own message stand alone (Decision 7).
    """
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"invalid date {raw!r}, expected YYYY-MM-DD: {exc}") from exc


def validate_date_range(start_date: date, end_date: date) -> None:
    """Raise ValueError for a reversed range. Future dates are allowed --
    Phase 1 verified the site returns an empty result set gracefully (HTTP
    200, 0 rows) rather than erroring, so there's nothing to reject; the
    caller is responsible for logging a clear "0 records" note rather than a
    silent success (Decision 7).
    """
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) must not be after end_date ({end_date})")


def validate_bodies(bodies: list[str]) -> None:
    """Raise ValueError for an empty list or any id outside the known set.

    Both an unknown numeric id (e.g. "999") and a non-numeric one (e.g.
    "abc") were observed live to return HTTP 200 with 0 rows, but only after
    ~10s (vs. sub-2s for a valid query) -- "abc" additionally returns an
    ~800KB unfiltered fallback page rather than a normal result page. Both
    are a measured cost worth avoiding client-side, not just style.
    """
    if not bodies:
        raise ValueError(
            "bodies must not be empty -- an empty list is very likely a caller mistake"
        )

    unknown = [b for b in bodies if b not in KNOWN_BODY_IDS]
    if unknown:
        raise ValueError(f"unknown body id(s) {unknown!r}; known ids are {sorted(KNOWN_BODY_IDS)}")
