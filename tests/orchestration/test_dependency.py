"""Dependency-enforcement test: a failing `landing_documents` must block
`processed_documents` for the same partition.

This is the one test in the suite that drives a real `materialize()` run
(rather than direct invocation, see test_ingestion_asset.py's docstring) --
it needs Dagster's actual step-skipping behavior, not just the compute
function's return value. `WRC_ORCHESTRATION_RETRY_MAX`/`_DELAY_SECONDS` are
set to 0 before importing the assets module so the run fails immediately
instead of waiting through the real (`30s` x exponential backoff) partition
retry policy -- exercising the same dependency wiring with a fast,
deterministic policy instead of a different code path.
"""

from __future__ import annotations

import importlib
import subprocess
from pathlib import Path

import pytest
from dagster import MultiPartitionKey, materialize

_PARTITION_KEY = MultiPartitionKey({"month": "2024-01-01", "body_slug": "wrc"})


@pytest.fixture
def zero_retry_assets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("WRC_ORCHESTRATION_RETRY_MAX", "0")
    monkeypatch.setenv("WRC_ORCHESTRATION_RETRY_DELAY_SECONDS", "0")
    import wrc_scraper.orchestration.assets as assets_mod

    importlib.reload(assets_mod)
    yield assets_mod
    # Restore the module to its normal (env-default) retry policy for any
    # test that runs after this one in the same process.
    monkeypatch.undo()
    importlib.reload(assets_mod)


def test_failing_ingestion_blocks_transformation(zero_retry_assets) -> None:
    assets_mod = zero_retry_assets

    def failing_runner(*, start, end, body_id, events_log_file: Path):
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="boom")

    assets_mod.run_scrapy_crawl = failing_runner

    result = materialize(
        [assets_mod.landing_documents, assets_mod.processed_documents],
        partition_key=_PARTITION_KEY,
        raise_on_error=False,
    )

    assert result.success is False
    assert result.asset_materializations_for_node("landing_documents") == []
    assert result.asset_materializations_for_node("processed_documents") == []
