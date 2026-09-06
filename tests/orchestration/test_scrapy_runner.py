"""Tests for wrc_scraper.orchestration.scrapy_runner.run_scrapy_crawl.

`subprocess.Popen` is monkeypatched with a fake process so these tests don't
spawn a real Scrapy crawl. The behavior under test is the bounded-timeout
guard: a hung subprocess must raise `ScrapyCrawlTimeoutError` (so the calling
Dagster partition fails/retries normally) instead of blocking forever, and its
whole process group must be killed rather than leaking the child.
"""

from __future__ import annotations

import subprocess
from datetime import date
from pathlib import Path

import pytest

from wrc_scraper.orchestration import scrapy_runner

_START = date(2024, 1, 1)
_END = date(2024, 1, 31)


class _FakeProcess:
    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self.wait_called = False

    def communicate(self, timeout: float | None = None) -> tuple[str, str]:
        raise subprocess.TimeoutExpired(cmd="scrapy", timeout=timeout)

    def wait(self) -> None:
        self.wait_called = True
        self.returncode = -9


class _FakeCompletedProcess:
    def __init__(self, pid: int = 4242) -> None:
        self.pid = pid
        self.returncode = 0

    def communicate(self, timeout: float | None = None) -> tuple[str, str]:
        return "stdout text", "stderr text"


def test_hung_subprocess_raises_timeout_error_and_kills_process_group(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_process = _FakeProcess()
    killed: list[tuple[int, int]] = []
    captured_args: list[list[str]] = []

    def fake_popen(args, **kwargs):
        captured_args.append(args)
        assert kwargs["start_new_session"] is True
        return fake_process

    monkeypatch.setattr(scrapy_runner.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(scrapy_runner.os, "killpg", lambda pid, sig: killed.append((pid, sig)))
    monkeypatch.setenv("WRC_SCRAPY_SUBPROCESS_TIMEOUT_SECONDS", "0.01")

    with pytest.raises(scrapy_runner.ScrapyCrawlTimeoutError, match="15376"):
        scrapy_runner.run_scrapy_crawl(
            start=_START, end=_END, body_id="15376", events_log_file=tmp_path / "events.jsonl"
        )

    assert killed == [(fake_process.pid, scrapy_runner.signal.SIGKILL)]
    assert fake_process.wait_called
    assert captured_args  # Popen was actually invoked


def test_normal_completion_returns_completed_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_process = _FakeCompletedProcess()
    monkeypatch.setattr(scrapy_runner.subprocess, "Popen", lambda *a, **k: fake_process)
    monkeypatch.delenv("WRC_SCRAPY_SUBPROCESS_TIMEOUT_SECONDS", raising=False)

    result = scrapy_runner.run_scrapy_crawl(
        start=_START, end=_END, body_id="15376", events_log_file=tmp_path / "events.jsonl"
    )

    assert result.returncode == 0
    assert result.stdout == "stdout text"
    assert result.stderr == "stderr text"
