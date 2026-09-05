"""Date-range partitioning for the WRC crawl.

Generic by design (Phase 2 plan, Decision 4 / Step 2.1): the granularity that
performs best (monthly, biweekly, weekly, or something else) is an open
question the Step 2.0 experiments answer, not something this module should
bake in. It supports whichever granularity is chosen, as configuration.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum


class PartitionUnit(StrEnum):
    DAYS = "days"
    MONTHS = "months"


@dataclass(frozen=True)
class PartitionGranularity:
    unit: PartitionUnit
    count: int

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError(f"granularity count must be positive, got {self.count}")


@dataclass(frozen=True)
class DatePartition:
    start: date
    end: date
    partition_date: date


def _add_months(d: date, months: int) -> date:
    month_index = d.month - 1 + months
    year = d.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, 1)


def iter_date_partitions(
    start_date: date, end_date: date, granularity: PartitionGranularity
) -> Iterator[DatePartition]:
    """Yield consecutive, non-overlapping, gap-free partitions covering
    the inclusive [start_date, end_date] range at the given granularity.
    """
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) must not be after end_date ({end_date})")

    if granularity.unit is PartitionUnit.DAYS:
        window = timedelta(days=granularity.count)
        cursor = start_date
        while cursor <= end_date:
            window_end = min(cursor + window - timedelta(days=1), end_date)
            yield DatePartition(start=cursor, end=window_end, partition_date=cursor)
            cursor = window_end + timedelta(days=1)
    else:
        # Calendar-month-aligned windows: each partition's *label* and
        # right-edge follow real month boundaries (not a fixed day count),
        # matching the brief's own "monthly" example.
        cursor = start_date
        while cursor <= end_date:
            partition_label = date(cursor.year, cursor.month, 1)
            window_end_exclusive = _add_months(partition_label, granularity.count)
            window_end = min(window_end_exclusive - timedelta(days=1), end_date)
            yield DatePartition(start=cursor, end=window_end, partition_date=partition_label)
            cursor = window_end + timedelta(days=1)


def format_site_date(d: date) -> str:
    """Convert a date to the WRC site's DD/MM/YYYY query format (verified Phase 1)."""
    return d.strftime("%d/%m/%Y")
