"""Unit tests for wrc_scraper.spiders.partition_tracker.PartitionTracker.

No Scrapy crawl, no fixtures -- just the tracker against synthetic
(body, DatePartition) inputs and a real `wrc.events` logger captured via
caplog, mirroring the behavioral assertions that used to live directly on
WrcSpider in tests/test_wrc_spider_parsing.py.
"""

from __future__ import annotations

import json
import logging
from datetime import date

import pytest

from wrc_scraper.partitioning import DatePartition
from wrc_scraper.spiders.partition_tracker import PartitionTracker

BODY = "15376"


@pytest.fixture
def partition() -> DatePartition:
    return DatePartition(
        start=date(2024, 1, 1), end=date(2024, 1, 31), partition_date=date(2024, 1, 1)
    )


@pytest.fixture
def tracker() -> PartitionTracker:
    return PartitionTracker(logging.getLogger("wrc.events"))


def _events(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [json.loads(r.message) for r in caplog.records if r.name == "wrc.events"]


def test_start_partition_initializes_state_and_logs(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.start_partition(BODY, partition)

    key = (BODY, "2024-01-01")
    assert tracker.partition_state[key]["records_found"] == 0
    assert tracker.partition_state[key]["pending"] == {}
    started = [e for e in _events(caplog) if e["event"] == "partition_started"]
    assert len(started) == 1
    assert started[0]["body"] == BODY
    assert started[0]["partition_date"] == "2024-01-01"


def test_record_found_increments_state_and_totals(
    tracker: PartitionTracker, partition: DatePartition
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.record_found(BODY, partition)
    tracker.record_found(BODY, partition)

    key = (BODY, "2024-01-01")
    assert tracker.partition_state[key]["records_found"] == 2
    assert tracker.totals["records_found"] == 2


def test_mark_pending_then_record_scraped_clears_it(
    tracker: PartitionTracker, partition: DatePartition
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.mark_pending(BODY, partition, "https://example/1.html", "ADJ-1")

    key = (BODY, "2024-01-01")
    assert tracker.partition_state[key]["pending"] == {"https://example/1.html": "ADJ-1"}

    tracker.record_scraped(BODY, partition, "https://example/1.html")

    assert tracker.partition_state[key]["pending"] == {}
    assert tracker.partition_state[key]["records_scraped"] == 1
    assert tracker.totals["records_scraped"] == 1


def test_record_failed_clears_pending_and_logs(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.mark_pending(BODY, partition, "https://example/1.html", "ADJ-1")

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.record_failed(
            BODY,
            partition,
            detail_url="https://example/1.html",
            reason="boom",
            url="https://example/1.html",
            http_status=500,
        )

    key = (BODY, "2024-01-01")
    assert tracker.partition_state[key]["pending"] == {}
    assert tracker.partition_state[key]["records_failed"] == 1
    assert tracker.totals["records_failed"] == 1

    failed = [e for e in _events(caplog) if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert failed[0]["reason"] == "boom"
    assert failed[0]["http_status"] == 500


def test_record_immediate_failure_does_not_touch_pending(
    tracker: PartitionTracker, partition: DatePartition
) -> None:
    """A row that never got a detail request sent must not decrement
    `pending` -- it was never incremented for it in the first place.
    """
    tracker.start_partition(BODY, partition)
    tracker.mark_pending(BODY, partition, "https://example/1.html", "ADJ-1")

    tracker.record_immediate_failure(
        BODY, partition, reason="missing identifier", listing_url="https://example/search"
    )

    key = (BODY, "2024-01-01")
    assert tracker.partition_state[key]["pending"] == {"https://example/1.html": "ADJ-1"}
    assert tracker.partition_state[key]["records_failed"] == 1


def test_finish_pagination_normally_matching_expected_completes_without_mismatch(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.set_records_expected(BODY, partition, 2)
    tracker.record_found(BODY, partition)
    tracker.record_found(BODY, partition)
    tracker.record_scraped(BODY, partition, "u1")
    tracker.record_scraped(BODY, partition, "u2")

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.finish_pagination_normally(BODY, partition)

    key = (BODY, "2024-01-01")
    assert key not in tracker.partition_state  # completed and removed
    assert tracker.totals["partitions_completed"] == 1
    assert tracker.totals["partitions_incomplete"] == 0

    events = _events(caplog)
    assert not [e for e in events if e["event"] == "partition_count_mismatch"]
    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["incomplete"] is False
    assert completed[0]["records_unaccounted"] == 0


def test_finish_pagination_normally_mismatch_marks_incomplete_and_logs(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.set_records_expected(BODY, partition, 5)
    tracker.record_found(BODY, partition)
    tracker.record_found(BODY, partition)
    tracker.record_scraped(BODY, partition, "u1")
    tracker.record_scraped(BODY, partition, "u2")

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.finish_pagination_normally(BODY, partition)

    events = _events(caplog)
    mismatches = [e for e in events if e["event"] == "partition_count_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["records_expected"] == 5
    assert mismatches[0]["records_found"] == 2

    completed = [e for e in events if e["event"] == "partition_completed"]
    assert completed[0]["incomplete"] is True
    assert completed[0]["records_unaccounted"] == 3
    assert tracker.totals["partitions_incomplete"] == 1


def test_finish_pagination_at_max_pages_marks_incomplete_and_logs(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.record_found(BODY, partition)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.finish_pagination_at_max_pages(BODY, partition, max_pages=500)

    events = _events(caplog)
    reached = [e for e in events if e["event"] == "partition_max_pages_reached"]
    assert len(reached) == 1
    assert reached[0]["max_pages"] == 500
    assert reached[0]["records_found"] == 1

    completed = [e for e in events if e["event"] == "partition_completed"]
    assert completed[0]["incomplete"] is True
    assert "max page limit reached" in completed[0]["reason"]


def test_mark_pagination_failed_marks_incomplete_with_given_reason(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.mark_pagination_failed(BODY, partition, reason="listing page failed: boom")

    completed = [e for e in _events(caplog) if e["event"] == "partition_completed"]
    assert completed[0]["incomplete"] is True
    assert completed[0]["reason"] == "listing page failed: boom"


def test_maybe_complete_waits_for_pagination_and_pending(
    tracker: PartitionTracker, partition: DatePartition
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.mark_pending(BODY, partition, "u1", "ADJ-1")
    key = (BODY, "2024-01-01")

    # pagination not done yet -- no-op.
    tracker.maybe_complete(BODY, partition)
    assert key in tracker.partition_state

    tracker.partition_state[key]["pagination_done"] = True
    # still pending -- still no-op.
    tracker.maybe_complete(BODY, partition)
    assert key in tracker.partition_state

    tracker.record_scraped(BODY, partition, "u1")
    tracker.maybe_complete(BODY, partition)
    assert key not in tracker.partition_state


def test_reconcile_dangling_logs_each_pending_record_and_completes(
    tracker: PartitionTracker, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.start_partition(BODY, partition)
    tracker.record_found(BODY, partition)
    tracker.record_found(BODY, partition)
    tracker.mark_pending(BODY, partition, "u1", "ADJ-1")
    tracker.mark_pending(BODY, partition, "u2", "ADJ-2")
    key = (BODY, "2024-01-01")
    tracker.partition_state[key]["pagination_done"] = True

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.reconcile_dangling()

    assert tracker.partition_state == {}
    assert tracker.totals["records_failed"] == 2
    assert tracker.totals["partitions_incomplete"] == 1

    events = _events(caplog)
    failed = [e for e in events if e["event"] == "record_failed"]
    assert len(failed) == 2
    assert {e["identifier"] for e in failed} == {"ADJ-1", "ADJ-2"}
    assert all("resolved" in e["reason"] for e in failed)

    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["incomplete"] is True
    assert "pending" in completed[0]["reason"]


def test_log_run_summary_emits_totals(
    tracker: PartitionTracker, caplog: pytest.LogCaptureFixture
) -> None:
    tracker.totals["records_found"] = 3
    tracker.totals["records_scraped"] = 2
    tracker.totals["records_failed"] = 1

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        tracker.log_run_summary("finished")

    summary = [e for e in _events(caplog) if e["event"] == "run_summary"]
    assert len(summary) == 1
    assert summary[0]["finish_reason"] == "finished"
    assert summary[0]["records_found"] == 3
    assert summary[0]["records_scraped"] == 2
    assert summary[0]["records_failed"] == 1
