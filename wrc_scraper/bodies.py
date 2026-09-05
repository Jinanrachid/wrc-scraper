"""Deciding-body registry: the site's numeric ids mapped to a stable internal
slug and a human-readable display name.

The numeric ids (1/2/3/15376) are the *site's* search query parameter -- an
external identifier we don't control. Storage identity (Mongo ``_id`` and MinIO
object keys) is keyed on the ``slug`` here instead, so:

* if the site ever renumbers a body, updating the id below keeps stored records
  stable (they stay under the same slug) and dedup still works across the change;
* object keys and ids are human-readable (``wrc/...`` not ``15376/...``);
* the display ``name`` is stored in each metadata record (as the assessment's
  screenshot shows the body by name, not by id).

The id remains the crawl query parameter and is kept in each record as
provenance (which body filter produced it).
"""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(frozen=True)
class BodyInfo:
    id: str  # the site's ?body=<id> query parameter
    slug: str  # stable internal key used for storage identity
    name: str  # human-readable display name (stored in metadata)


# Verified live (docs/SCRAPY_EXPERIMENTS.md Sec 3).
BODIES: dict[str, BodyInfo] = {
    "1": BodyInfo("1", "equality", "Equality Tribunal"),
    "2": BodyInfo("2", "eat", "Employment Appeals Tribunal"),
    "3": BodyInfo("3", "labour_court", "Labour Court"),
    "15376": BodyInfo("15376", "wrc", "Workplace Relations Commission"),
}

KNOWN_BODY_IDS: frozenset[str] = frozenset(BODIES)


def body_info(body_id: str) -> BodyInfo:
    """Return the registry entry for ``body_id``, or raise a clear ValueError.

    Callers past client-side validation should never hit the error, but storage
    code must not silently build a key from an unknown body.
    """
    try:
        return BODIES[body_id]
    except KeyError as exc:
        raise ValueError(f"unknown body id {body_id!r}; known ids are {sorted(BODIES)}") from exc


def body_slug(body_id: str) -> str:
    return body_info(body_id).slug


def body_name(body_id: str) -> str:
    return body_info(body_id).name
