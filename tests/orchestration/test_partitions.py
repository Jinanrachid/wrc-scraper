"""Tests for wrc_scraper.orchestration.partitions (Phase 5).

Covers the partitioning scheme itself (dimensions, body slugs, month-window
math) and the two invariants the approved design's partitioned-transform
safety argument rests on:

* **Alignment** -- a Dagster month partition key names exactly the calendar
  month window `wrc_scraper.partitioning.iter_date_partitions` assigns as
  `partition_date` at monthly granularity (the spider's own partitioning).
* **Cluster-in-one-partition** -- every row discovered on one listing page
  (one `(body, partition)` search request) is stamped with the *same*
  `partition` object, hence the same `partition_date` -- the structural
  guarantee that, combined with the documented fact that variant-cluster
  siblings are discovered together on one listing page
  (docs/SCRAPY_EXPERIMENTS.md Sec 19), keeps a whole cluster inside one
  Dagster partition.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from scrapy.http import HtmlResponse, Request

from wrc_scraper.bodies import BODIES
from wrc_scraper.orchestration.partitions import (
    BODY_DIMENSION,
    BODY_SLUGS,
    MONTH_DIMENSION,
    SLUG_TO_BODY_ID,
    month_window,
    partitions_def,
    resolve_partition,
)
from wrc_scraper.partitioning import PartitionGranularity, PartitionUnit, iter_date_partitions
from wrc_scraper.spiders.wrc_spider import WrcSpider

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def test_partitions_def_has_month_and_body_slug_dimensions() -> None:
    dimension_names = {d.name for d in partitions_def.partitions_defs}
    assert dimension_names == {MONTH_DIMENSION, BODY_DIMENSION}


def test_body_slugs_match_the_body_registry() -> None:
    assert BODY_SLUGS == sorted(info.slug for info in BODIES.values())
    assert set(SLUG_TO_BODY_ID) == set(BODY_SLUGS)
    for info in BODIES.values():
        assert SLUG_TO_BODY_ID[info.slug] == info.id


@pytest.mark.parametrize(
    ("month_key", "expected_end"),
    [
        ("2024-01-01", date(2024, 1, 31)),
        ("2024-02-01", date(2024, 2, 29)),  # 2024 is a leap year
        ("2023-02-01", date(2023, 2, 28)),
        ("2024-12-01", date(2024, 12, 31)),
        ("1989-01-01", date(1989, 1, 31)),  # the earliest supported month
    ],
)
def test_month_window_matches_calendar_month(month_key: str, expected_end: date) -> None:
    start, end = month_window(month_key)
    assert start == date.fromisoformat(month_key)
    assert end == expected_end


@pytest.mark.parametrize("month_key", ["1989-01-01", "2000-02-01", "2024-06-01", "2026-12-01"])
def test_alignment_invariant_matches_spiders_own_monthly_partitioning(month_key: str) -> None:
    """A Dagster month partition key must describe the exact same window the
    spider's own `iter_date_partitions` assigns that `partition_date` when run
    at monthly granularity -- otherwise `processed_documents` could read a
    window `landing_documents` never actually wrote.
    """
    start, end = month_window(month_key)
    monthly = PartitionGranularity(PartitionUnit.MONTHS, 1)
    spider_partitions = list(iter_date_partitions(start, end, monthly))

    assert len(spider_partitions) == 1
    assert spider_partitions[0].partition_date == date.fromisoformat(month_key)
    assert spider_partitions[0].start == start
    assert spider_partitions[0].end == end


def test_resolve_partition_decodes_month_and_body(monkeypatch: pytest.MonkeyPatch) -> None:
    from dagster import MultiPartitionKey

    key = MultiPartitionKey({MONTH_DIMENSION: "2024-03-01", BODY_DIMENSION: "wrc"})
    window = resolve_partition(key)

    assert window.month_key == "2024-03-01"
    assert window.body_slug == "wrc"
    assert window.body_id == "15376"
    assert window.start == date(2024, 3, 1)
    assert window.end == date(2024, 3, 31)


# -- cluster-in-one-partition invariant ---------------------------------------


def test_cluster_invariant_all_rows_on_one_listing_page_share_one_partition() -> None:
    """Every row `parse_listing` extracts from a single response is stamped
    with the identical `partition` object (hence identical `partition_date`)
    -- the mechanism that keeps a variant cluster (several `detail_url`s
    sharing one `identifier`, always discovered together on one listing page
    per docs/SCRAPY_EXPERIMENTS.md Sec 19) inside a single Dagster partition.
    """
    from wrc_scraper.partitioning import DatePartition

    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    partition = DatePartition(
        start=date(2024, 1, 1), end=date(2024, 1, 31), partition_date=date(2024, 1, 1)
    )
    key = ("15376", partition.partition_date.isoformat())
    spider._tracker.partition_state[key] = {
        "records_found": 0,
        "records_scraped": 0,
        "records_failed": 0,
        "records_expected": None,
        "pagination_done": False,
        "pending": {},
        "incomplete": False,
        "reason": None,
    }
    body = (FIXTURES_DIR / "listing_wrc.html").read_bytes()
    request = Request(
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    response = HtmlResponse(url=request.url, body=body, encoding="utf-8", request=request)

    results = list(spider.parse_listing(response))
    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]

    assert len(follow_requests) >= 2
    partition_dates = {r.cb_kwargs["partition"].partition_date for r in follow_requests}
    assert partition_dates == {partition.partition_date}
