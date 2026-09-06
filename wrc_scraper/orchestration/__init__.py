"""Dagster orchestration over the existing Scrapy crawl and TransformService
transformation.

This package owns partitioning, dependency wiring, retries, and
observability only -- it does not reimplement crawling or transformation
logic (CLAUDE.md). See `wrc_scraper.orchestration.assets` for the two
assets and `wrc_scraper.orchestration.partitions` for the shared
(month x body_slug) partitioning scheme.
"""

from __future__ import annotations
