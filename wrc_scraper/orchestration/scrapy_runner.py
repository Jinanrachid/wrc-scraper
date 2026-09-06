"""Subprocess invocation of `scrapy crawl wrc` -- one fresh process per
Dagster partition.

Scrapy's Twisted reactor cannot be restarted once stopped within a single
process, so materializing many `(month, body_slug)` partitions inside one
Dagster run rules out driving the crawler in-process.
A subprocess per partition sidesteps this entirely, at the cost of process
start-up overhead -- acceptable at the assessment's 500-1,000 doc scale and
the ~1,776-partition (month x body) scale the design targets. Dagster Pipes
is the natural next step if scraping ever moves to separate
containers/Kubernetes, deferred here since nothing about this assessment run
needs it.

Kept as one plain function, independent of any Dagster machinery, so
`wrc_scraper.orchestration.assets` can monkeypatch it in tests with a fake
that writes a synthetic events log instead of actually spawning Scrapy.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from datetime import date
from pathlib import Path

from wrc_scraper.config import OrchestrationSettings

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class ScrapyCrawlTimeoutError(RuntimeError):
    """Raised when a `scrapy crawl` subprocess exceeds its bounded timeout."""


def run_scrapy_crawl(
    *, start: date, end: date, body_id: str, events_log_file: Path
) -> subprocess.CompletedProcess[str]:
    """Run one `scrapy crawl wrc -a start_date=... -a end_date=... -a bodies=...`
    invocation for `[start, end] x body_id`, with structured JSONL events
    routed to `events_log_file` (`WRC_EVENTS_LOG_FILE`) so the caller can read
    the run's `run_summary` once the process exits.

    Uses `sys.executable -m scrapy` (never a bare `"scrapy"` on `PATH`, and
    never `shell=True`) so the subprocess runs under the exact same Python
    environment as the Dagster process invoking it.

    Bounded by `WRC_SCRAPY_SUBPROCESS_TIMEOUT_SECONDS` (`OrchestrationSettings.
    scrapy_subprocess_timeout_seconds`): Scrapy's own DOWNLOAD_TIMEOUT/RETRY_TIMES
    only bound individual HTTP requests, not the process itself, so a reactor
    deadlock or a hang before any per-request timeout applies would otherwise
    block the calling Dagster step forever. On timeout, the whole process
    group is killed (the child runs in its own session via
    `start_new_session=True`, so this also reaps any of its descendants) and
    `ScrapyCrawlTimeoutError` is raised so the partition fails/retries like any
    other systemic crawl failure.
    """
    env = {**os.environ, "WRC_EVENTS_LOG_FILE": str(events_log_file)}
    args = [
        sys.executable,
        "-m",
        "scrapy",
        "crawl",
        "wrc",
        "-a",
        f"start_date={start.isoformat()}",
        "-a",
        f"end_date={end.isoformat()}",
        "-a",
        f"bodies={body_id}",
    ]
    timeout = OrchestrationSettings.from_env().scrapy_subprocess_timeout_seconds
    process = subprocess.Popen(
        args,
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait()
        raise ScrapyCrawlTimeoutError(
            f"scrapy crawl for body={body_id} start={start.isoformat()} "
            f"end={end.isoformat()} exceeded {timeout}s timeout; process group killed"
        ) from None
    return subprocess.CompletedProcess(args, process.returncode, stdout, stderr)
