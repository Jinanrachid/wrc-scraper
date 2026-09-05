"""Tests for wrc_scraper.partitioning (Phase 2, Step 2.1).

Per the approved plan: cover normal ranges, the brief's own monthly example,
boundaries, invalid ranges, coarser granularities, and -- critically -- the
partitioning invariant (no gaps, no overlaps, full coverage) for every
granularity, since that's what makes the Step 2.0 partition experiment's
"same logical records regardless of granularity" result trustworthy.
"""

from __future__ import annotations

from datetime import date, timedelta
from itertools import pairwise

import pytest

from wrc_scraper.partitioning import (
    DatePartition,
    PartitionGranularity,
    PartitionUnit,
    format_site_date,
    iter_date_partitions,
)


def assert_full_coverage_no_gaps_no_overlaps(
    partitions: list[DatePartition], start: date, end: date
) -> None:
    assert partitions, "expected at least one partition"
    assert partitions[0].start == start
    assert partitions[-1].end == end
    for earlier, later in pairwise(partitions):
        assert later.start == earlier.end + timedelta(days=1), (
            f"gap or overlap between {earlier} and {later}"
        )


@pytest.mark.parametrize(
    "granularity",
    [
        PartitionGranularity(PartitionUnit.DAYS, 7),
        PartitionGranularity(PartitionUnit.DAYS, 14),
        PartitionGranularity(PartitionUnit.MONTHS, 1),
        PartitionGranularity(PartitionUnit.MONTHS, 2),
    ],
)
def test_invariant_holds_for_a_normal_multi_window_range(granularity: PartitionGranularity) -> None:
    start, end = date(2024, 1, 1), date(2024, 6, 30)
    partitions = list(iter_date_partitions(start, end, granularity))
    assert_full_coverage_no_gaps_no_overlaps(partitions, start, end)


def test_monthly_partitioning_matches_the_briefs_own_example() -> None:
    # "example monthly partitions between 01-01-2024 and 01-01-2025"
    start, end = date(2024, 1, 1), date(2025, 1, 1)
    monthly = PartitionGranularity(PartitionUnit.MONTHS, 1)
    partitions = list(iter_date_partitions(start, end, monthly))
    assert_full_coverage_no_gaps_no_overlaps(partitions, start, end)
    assert len(partitions) == 13  # Jan 2024 .. Jan 2025 inclusive
    assert partitions[0].partition_date == date(2024, 1, 1)
    assert partitions[0].end == date(2024, 1, 31)
    assert partitions[-1].partition_date == date(2025, 1, 1)
    assert partitions[-1].end == date(2025, 1, 1)  # clipped to the requested end


def test_weekly_partitioning_over_two_months() -> None:
    start, end = date(2024, 1, 1), date(2024, 2, 29)
    partitions = list(iter_date_partitions(start, end, PartitionGranularity(PartitionUnit.DAYS, 7)))
    assert_full_coverage_no_gaps_no_overlaps(partitions, start, end)
    assert len(partitions) == 9
    assert all((p.end - p.start).days <= 6 for p in partitions)


@pytest.mark.parametrize(
    "granularity",
    [PartitionGranularity(PartitionUnit.DAYS, 7), PartitionGranularity(PartitionUnit.MONTHS, 1)],
)
def test_single_day_range_boundary(granularity: PartitionGranularity) -> None:
    start = end = date(2024, 3, 15)
    partitions = list(iter_date_partitions(start, end, granularity))
    expected = DatePartition(start=start, end=end, partition_date=partitions[0].partition_date)
    assert partitions == [expected]


def test_reversed_range_raises() -> None:
    with pytest.raises(ValueError, match="must not be after"):
        list(
            iter_date_partitions(
                date(2024, 2, 1), date(2024, 1, 1), PartitionGranularity(PartitionUnit.MONTHS, 1)
            )
        )


def test_non_positive_granularity_count_raises() -> None:
    with pytest.raises(ValueError, match="positive"):
        PartitionGranularity(PartitionUnit.DAYS, 0)
    with pytest.raises(ValueError, match="positive"):
        PartitionGranularity(PartitionUnit.MONTHS, -1)


def test_coarser_granularity_yields_fewer_partitions() -> None:
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    weekly_g = PartitionGranularity(PartitionUnit.DAYS, 7)
    biweekly_g = PartitionGranularity(PartitionUnit.DAYS, 14)
    monthly_g = PartitionGranularity(PartitionUnit.MONTHS, 1)
    two_monthly_g = PartitionGranularity(PartitionUnit.MONTHS, 2)

    weekly = list(iter_date_partitions(start, end, weekly_g))
    biweekly = list(iter_date_partitions(start, end, biweekly_g))
    monthly = list(iter_date_partitions(start, end, monthly_g))
    two_monthly = list(iter_date_partitions(start, end, two_monthly_g))

    assert len(weekly) > len(biweekly) > len(monthly) > len(two_monthly)
    for partitions in (weekly, biweekly, monthly, two_monthly):
        assert_full_coverage_no_gaps_no_overlaps(partitions, start, end)


def test_format_site_date_matches_verified_ddmmyyyy_format() -> None:
    assert format_site_date(date(2024, 1, 5)) == "05/01/2024"
    assert format_site_date(date(2024, 12, 31)) == "31/12/2024"
