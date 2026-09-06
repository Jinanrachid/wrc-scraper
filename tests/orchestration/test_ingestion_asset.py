"""Tests for wrc_scraper.orchestration.assets.landing_documents.

Uses Dagster's direct-invocation testing pattern (calling the decorated asset
function with a `build_asset_context(...)`) rather than a full `materialize()`
run: it exercises the asset's own compute logic without Dagster's op-retry
machinery, which -- correctly -- sleeps between attempts for a real
`RetryPolicy` (see test_dependency.py for a `materialize()`-based test with
retries disabled).

`run_scrapy_crawl` is monkeypatched on the assets module with a fake that
writes a synthetic JSONL events log to the given path instead of actually
spawning Scrapy -- the same seam a real subprocess failure would report
through.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest
from dagster import MultiPartitionKey, build_asset_context

from wrc_scraper.orchestration import assets as assets_mod

_PARTITION_KEY = MultiPartitionKey({"month": "2024-01-01", "body_slug": "wrc"})


def _completed(returncode: int = 0, stderr: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout="", stderr=stderr)


def _write_run_summary(events_log_file: Path, **fields: object) -> None:
    payload = {"event": "run_summary", "finish_reason": "finished", **fields}
    events_log_file.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_successful_crawl_returns_metadata_and_passing_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_runner(*, start, end, body_id, events_log_file: Path):
        assert body_id == "15376"  # wrc slug -> numeric id
        _write_run_summary(
            events_log_file,
            records_found=10,
            records_scraped=10,
            records_failed=0,
            records_unaccounted=0,
            partitions_completed=1,
            partitions_incomplete=0,
        )
        return _completed()

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.landing_documents(context)

    assert result.metadata["month"].value == "2024-01-01"
    assert result.metadata["body_slug"].value == "wrc"
    assert result.metadata["records_found"].value == 10
    assert result.metadata["finish_reason"].value == "finished"
    (check_result,) = result.check_results
    assert check_result.passed is True


def test_nonzero_exit_code_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_runner(*, start, end, body_id, events_log_file: Path):
        return _completed(returncode=1, stderr="connection refused")

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    with pytest.raises(RuntimeError, match="exit 1"):
        assets_mod.landing_documents(context)


def test_missing_run_summary_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_runner(*, start, end, body_id, events_log_file: Path):
        events_log_file.write_text(
            json.dumps({"event": "partition_started"}) + "\n", encoding="utf-8"
        )
        return _completed()

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    with pytest.raises(RuntimeError, match="no run_summary event"):
        assets_mod.landing_documents(context)


def test_bad_finish_reason_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_runner(*, start, end, body_id, events_log_file: Path):
        _write_run_summary(events_log_file)
        # Overwrite with a non-"finished" reason (e.g. the spider was killed).
        events_log_file.write_text(
            json.dumps({"event": "run_summary", "finish_reason": "shutdown"}) + "\n",
            encoding="utf-8",
        )
        return _completed()

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    with pytest.raises(RuntimeError, match="finish_reason='shutdown'"):
        assets_mod.landing_documents(context)


def test_per_record_failures_do_not_raise_but_are_reported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A clean crawl (finish_reason == 'finished') with some per-record
    failures is a successful materialization -- failures are surfaced as
    metadata, never as an exception (they don't mean the crawl is broken).
    """

    def fake_runner(*, start, end, body_id, events_log_file: Path):
        _write_run_summary(events_log_file, records_found=10, records_scraped=8, records_failed=2)
        return _completed()

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.landing_documents(context)

    assert result.metadata["records_failed"].value == 2


def test_quality_check_warns_when_failed_ratio_exceeds_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WRC_INGEST_FAILED_RATIO_THRESHOLD", "0.05")

    def fake_runner(*, start, end, body_id, events_log_file: Path):
        # 3/10 = 30% failed, well over the 5% threshold.
        _write_run_summary(events_log_file, records_found=10, records_scraped=7, records_failed=3)
        return _completed()

    monkeypatch.setattr(assets_mod, "run_scrapy_crawl", fake_runner)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.landing_documents(context)

    (check_result,) = result.check_results
    assert check_result.passed is False
