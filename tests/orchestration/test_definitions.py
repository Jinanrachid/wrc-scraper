"""Tests for wrc_scraper.orchestration.definitions -- the Dagster entry point.

Confirms the definitions load without error, both assets are present, they
share the same (month x body_slug) partitioning, `processed_documents`
depends on `landing_documents`, and both assets' quality checks are wired up.
"""

from __future__ import annotations

from dagster import AssetKey

from wrc_scraper.orchestration.definitions import defs
from wrc_scraper.orchestration.partitions import partitions_def


def test_definitions_load_and_resolve() -> None:
    repo = defs.get_repository_def()
    assert repo is not None


def test_both_assets_present() -> None:
    graph = defs.get_repository_def().asset_graph
    keys = graph.get_all_asset_keys()
    assert AssetKey("landing_documents") in keys
    assert AssetKey("processed_documents") in keys


def test_processed_documents_depends_on_landing_documents() -> None:
    graph = defs.get_repository_def().asset_graph
    parents = graph.get(AssetKey("processed_documents")).parent_keys
    assert AssetKey("landing_documents") in parents


def test_both_assets_share_the_month_body_partitions_def() -> None:
    graph = defs.get_repository_def().asset_graph
    landing_partitions = graph.get(AssetKey("landing_documents")).partitions_def
    processed_partitions = graph.get(AssetKey("processed_documents")).partitions_def
    assert landing_partitions is partitions_def
    assert processed_partitions is partitions_def


def test_quality_checks_are_registered() -> None:
    graph = defs.get_repository_def().asset_graph
    check_names = {key.name for key in graph.asset_check_keys}
    assert "landing_documents_quality" in check_names
    assert "processed_documents_quality" in check_names
