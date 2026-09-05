"""Tests for wrc_scraper.transform.cli argument handling.

Only the argument-validation path is exercised here: it returns before any
Mongo/MinIO client is constructed, so it needs no Docker and no fakes for
storage. The events logger is swapped for a recorder (rather than asserting on
captured stdout/stderr) because `get_events_logger()` is a process-wide
singleton shared across the whole test session -- reading its real stream
would be order-dependent and flaky. The Mongo/MinIO-touching paths are
covered by TransformService's own tests (test_transform_service.py) and the
Docker-backed integration tests.
"""

from __future__ import annotations

import logging

import pytest

from wrc_scraper.transform import cli


class _RecordingLogger:
    def __init__(self) -> None:
        self.records: list[tuple[int, str]] = []

    def error(self, message: str) -> None:
        self.records.append((logging.ERROR, message))

    def info(self, message: str) -> None:
        self.records.append((logging.INFO, message))


@pytest.fixture
def recording_logger(monkeypatch: pytest.MonkeyPatch) -> _RecordingLogger:
    logger = _RecordingLogger()
    monkeypatch.setattr(cli, "get_events_logger", lambda: logger)
    return logger


def test_reversed_date_range_fails_clearly_with_usage_exit_code(
    recording_logger: _RecordingLogger,
) -> None:
    exit_code = cli.main(["--start-date", "2024-02-01", "--end-date", "2024-01-01"])

    assert exit_code == 2
    assert any("invalid_arguments" in message for _level, message in recording_logger.records)


def test_invalid_date_format_fails_clearly_with_usage_exit_code(
    recording_logger: _RecordingLogger,
) -> None:
    exit_code = cli.main(["--start-date", "not-a-date", "--end-date", "2024-01-31"])

    assert exit_code == 2
    assert any("invalid_arguments" in message for _level, message in recording_logger.records)
