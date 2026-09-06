"""Thin CLI entry point for the transformation stage.

Usage::

    .venv/bin/python -m wrc_scraper.transform.cli --start-date 2024-01-01 --end-date 2024-01-31

Builds the source (Landing Zone) and destination (transformed) repositories
from environment configuration and runs `TransformService.transform_range`
once. Deliberately thin -- all the actual logic lives in `service.py`, so the
same service can later be wrapped as a Dagster asset without change.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys

from wrc_scraper.config import TransformSettings
from wrc_scraper.logging_utils import get_events_logger
from wrc_scraper.storage.factory import (
    StorageSettings,
    build_minio,
    build_mongo,
    build_transformed_minio,
    build_transformed_mongo,
)
from wrc_scraper.transform.service import TransformService
from wrc_scraper.validation import parse_iso_date, validate_date_range


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transform Landing Zone documents.")
    parser.add_argument("--start-date", required=True, help="ISO YYYY-MM-DD, inclusive")
    parser.add_argument("--end-date", required=True, help="ISO YYYY-MM-DD, inclusive")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logger = get_events_logger()

    try:
        start_date = parse_iso_date(args.start_date)
        end_date = parse_iso_date(args.end_date)
        validate_date_range(start_date, end_date)
    except ValueError as exc:
        # Bad input must fail clearly and predictably, not with a bare traceback:
        # invalid/reversed dates are a usage error, distinct from a run-level
        # infrastructure failure or a partial-data failure (exit codes 2 vs 3 vs 1).
        logger.error(json.dumps({"event": "invalid_arguments", "reason": str(exc)}))
        return 2

    storage_settings = StorageSettings.from_env()
    transform_settings = TransformSettings.from_env()

    source_mongo_client = dest_mongo_client = None
    try:
        # Client construction (Mongo/MinIO) lives inside this try too: a
        # connection/config error at construction time is a startup failure
        # for this run just as much as one raised later by transform_range,
        # and must be logged with the same date-range context rather than
        # propagating as a bare, unlogged traceback.
        source_mongo_client, source_mongo = build_mongo(storage_settings)
        source_minio = build_minio(storage_settings)
        dest_mongo_client, dest_mongo = build_transformed_mongo(
            storage_settings, transform_settings
        )
        dest_minio = build_transformed_minio(storage_settings, transform_settings)

        dest_mongo.ensure_indexes()
        dest_minio.ensure_bucket()

        service = TransformService(
            source_mongo,
            source_minio,
            dest_mongo,
            dest_minio,
            keep_images=transform_settings.keep_images,
            near_tie_chars=transform_settings.near_tie_chars,
            max_workers=transform_settings.concurrency,
            events_logger=logger,
        )
        summary = service.transform_range(start_date.isoformat(), end_date.isoformat())
    except Exception as exc:  # noqa: BLE001 -- a run-level failure (client
        # construction, Mongo/MinIO unreachable, auth/config error, etc.) must be
        # surfaced clearly as a structured event and a distinct exit code, not a
        # bare traceback -- individual document failures never reach here, they're
        # already caught and counted inside TransformService.
        logger.error(
            json.dumps(
                {
                    "event": "run_failed",
                    "reason": repr(exc),
                    "start_date": args.start_date,
                    "end_date": args.end_date,
                }
            )
        )
        return 3
    finally:
        if source_mongo_client is not None:
            source_mongo_client.close()
        if dest_mongo_client is not None:
            dest_mongo_client.close()

    print(json.dumps({"event": "cli_summary", **dataclasses.asdict(summary)}))
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
