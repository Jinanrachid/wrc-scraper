"""Dagster entry point: `dagster dev -f wrc_scraper/orchestration/definitions.py`
or via `workspace.yaml` (loads this module's `defs`).
"""

from __future__ import annotations

from dagster import AssetSelection, Definitions, define_asset_job

from wrc_scraper.orchestration.assets import landing_documents, processed_documents

# Named jobs so the Dagster UI/CLI "Target" column always says something
# distinct for what actually ran, instead of every run -- full pipeline,
# landing only, or transform only -- showing the same ambiguous label
# (or the framework-generated `__ephemeral_asset_job__`/`__ASSET_JOB` when no
# named job is targeted). Launch/backfill against the specific job that
# matches what you mean to run:
#   - `wrc_pipeline`  -- landing_documents -> processed_documents, both stages
#   - `landing`       -- landing_documents only
#   - `process`       -- processed_documents only (reads whatever landing
#                        already wrote for that partition; does not trigger
#                        landing itself)
wrc_pipeline = define_asset_job(
    "wrc_pipeline",
    selection=AssetSelection.assets(landing_documents, processed_documents),
)

landing = define_asset_job(
    "landing",
    selection=AssetSelection.assets(landing_documents),
)

process = define_asset_job(
    "process",
    selection=AssetSelection.assets(processed_documents),
)

defs = Definitions(
    assets=[landing_documents, processed_documents],
    jobs=[wrc_pipeline, landing, process],
)
