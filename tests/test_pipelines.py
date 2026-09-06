"""Tests for wrc_scraper.pipelines.MongoMinioPipeline error classification.

Only the process_item error-handling path is exercised here (robustness pass,
Fix 3): a systemic storage outage must abort the whole crawl via CloseSpider,
while an unclassified per-item exception must propagate unchanged (NOT be
relabelled as an intentional DropItem). Both paths must emit a structured log
event first.

These bypass open_spider by setting pipeline._service / pipeline._events_logger
directly -- no Docker, no real Mongo/MinIO -- following the _RecordingLogger
convention already used in tests/transform/test_cli.py. get_events_logger() is a
process-wide singleton, so a recorder is injected rather than asserting on a
shared stream.
"""

from __future__ import annotations

import json
import logging

import pymongo.errors
import pytest
from scrapy.exceptions import CloseSpider

from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.pipelines import MongoMinioPipeline


class _RecordingLogger:
    def __init__(self) -> None:
        self.records: list[tuple[int, str]] = []

    def error(self, message: str) -> None:
        self.records.append((logging.ERROR, message))

    def warning(self, message: str) -> None:
        self.records.append((logging.WARNING, message))

    def info(self, message: str) -> None:
        self.records.append((logging.INFO, message))


class _RaisingService:
    """Stand-in IngestService whose ingest() always raises the given error."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def ingest(self, record: WrcDecisionRecord) -> object:
        raise self._exc


def _record() -> WrcDecisionRecord:
    return WrcDecisionRecord(
        identifier="ADJ-00047352",
        description="Jessica Davis V St. Vincent's Private Hospital",
        published_date="2024-01-31",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        document_type="html_inline",
        document_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        partition_date="2024-01-01",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        scraped_at="2024-02-01T00:00:00+00:00",
        raw_html="<html></html>",
    )


def _pipeline_with(service: _RaisingService) -> tuple[MongoMinioPipeline, _RecordingLogger]:
    pipeline = MongoMinioPipeline()
    logger = _RecordingLogger()
    pipeline._service = service
    pipeline._events_logger = logger
    return pipeline, logger


def _events(logger: _RecordingLogger) -> list[dict]:
    return [json.loads(message) for _level, message in logger.records]


def test_storage_outage_aborts_the_crawl_with_closespider_and_logs_context() -> None:
    """A systemic Mongo/MinIO outage (here a ServerSelectionTimeoutError) must
    stop the whole run fast via Scrapy's CloseSpider rather than limp on
    producing thousands of slow, uncategorized per-item failures.
    """
    service = _RaisingService(pymongo.errors.ServerSelectionTimeoutError("mongo unreachable"))
    pipeline, logger = _pipeline_with(service)
    record = _record()

    with pytest.raises(CloseSpider):
        pipeline.process_item(record)

    unavailable = [e for e in _events(logger) if e["event"] == "storage_unavailable"]
    assert len(unavailable) == 1
    event = unavailable[0]
    assert event["body_slug"] == "wrc"
    assert event["identifier"] == "ADJ-00047352"
    assert event["detail_url"] == record.detail_url
    assert "mongo unreachable" in event["reason"]


def test_unclassified_error_is_reraised_unchanged_not_dropped_and_logs_context() -> None:
    """An arbitrary, unclassified exception could be a systemic code bug that
    fails every item identically. It must propagate unchanged (so it surfaces
    in Scrapy's own exception stats) -- never be wrapped in DropItem, which
    would relabel it as a routine, expected drop.
    """
    boom = RuntimeError("boom")
    service = _RaisingService(boom)
    pipeline, logger = _pipeline_with(service)
    record = _record()

    with pytest.raises(RuntimeError, match="boom") as exc_info:
        pipeline.process_item(record)
    # The very same exception object propagates -- not re-wrapped as DropItem.
    assert exc_info.value is boom

    ingest_errors = [e for e in _events(logger) if e["event"] == "record_ingest_error"]
    assert len(ingest_errors) == 1
    event = ingest_errors[0]
    assert event["body_slug"] == "wrc"
    assert event["identifier"] == "ADJ-00047352"
    assert event["detail_url"] == record.detail_url
    assert "boom" in event["reason"]
