"""Shared (month x body_slug) partitioning for both orchestration assets.

`landing_documents` and `processed_documents` are partitioned on the *same*
grain, so `processed_documents` at a given partition reads exactly the slice
`landing_documents` at the matching partition just wrote -- one Landing Zone
crawl x transform pair per (calendar month, deciding body).

Two invariants make this safe (tested in
`tests/orchestration/test_partitions.py`):

* **Alignment** -- a Dagster month partition key ("YYYY-MM-01") names exactly
  the same calendar-month window the spider's own `partition_date`
  (`wrc_scraper.partitioning.iter_date_partitions`) assigns to every record
  scraped in that window. `month_window` below reconstructs that window from
  the key without re-deriving the spider's month math -- both sides describe
  the same calendar month, computed independently but agreeing by construction.
* **Cluster-in-one-partition** -- a variant cluster (several `detail_url`s
  sharing one `identifier`, docs/SCRAPY_EXPERIMENTS.md Sec 19) is always a set
  of *consecutive re-publications of one decision*, so every member carries
  the same `partition_date` and therefore lands in one partition. This is
  what makes it safe to partition the transform stage at all: canonical
  selection for a `(body_slug, identifier)` group never needs to look outside
  its own partition.
"""

from __future__ import annotations

import calendar
import dataclasses
from datetime import date

from dagster import (
    MonthlyPartitionsDefinition,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

from wrc_scraper.bodies import BODIES

MONTH_DIMENSION = "month"
BODY_DIMENSION = "body_slug"

# 1989-01-01: the earliest date the assessment's target range (1989-2026) needs.
# Dagster's MonthlyPartitionsDefinition needs a concrete start; unlike the
# spider's own PartitionGranularity (generic, unit-agnostic), Dagster's
# partitions are declared once at definition time, not per-run.
MONTH_PARTITIONS = MonthlyPartitionsDefinition(start_date="1989-01-01")

# The slug, not the site's numeric id: renumbering-safe (wrc_scraper.bodies)
# and what every stored record actually carries. Numeric ids are resolved
# only at the point of invoking the spider (SLUG_TO_BODY_ID below).
BODY_SLUGS: list[str] = sorted(info.slug for info in BODIES.values())
BODY_PARTITIONS = StaticPartitionsDefinition(BODY_SLUGS)

SLUG_TO_BODY_ID: dict[str, str] = {info.slug: info.id for info in BODIES.values()}

partitions_def = MultiPartitionsDefinition(
    {MONTH_DIMENSION: MONTH_PARTITIONS, BODY_DIMENSION: BODY_PARTITIONS}
)


@dataclasses.dataclass(frozen=True)
class PartitionWindow:
    month_key: str
    body_slug: str
    body_id: str
    start: date
    end: date


def month_window(month_key: str) -> tuple[date, date]:
    """The inclusive calendar-month `[start, end]` window for a Dagster month
    partition key ("YYYY-MM-01") -- the same window
    `wrc_scraper.partitioning.iter_date_partitions` assigns that
    `partition_date` when run at monthly granularity (see the alignment
    invariant above).
    """
    start = date.fromisoformat(month_key)
    _, last_day = calendar.monthrange(start.year, start.month)
    return start, date(start.year, start.month, last_day)


def resolve_partition(partition_key: MultiPartitionKey) -> PartitionWindow:
    """Decode a Dagster `MultiPartitionKey` into the (body id/slug, date
    window) both assets need.
    """
    keys_by_dimension = partition_key.keys_by_dimension
    month_key = keys_by_dimension[MONTH_DIMENSION]
    slug = keys_by_dimension[BODY_DIMENSION]
    start, end = month_window(month_key)
    return PartitionWindow(
        month_key=month_key,
        body_slug=slug,
        body_id=SLUG_TO_BODY_ID[slug],
        start=start,
        end=end,
    )
