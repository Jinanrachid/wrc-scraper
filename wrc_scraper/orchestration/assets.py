"""The two orchestration assets: `landing_documents` (ingestion) and
`processed_documents` (transformation), partitioned on `(month, body_slug)`
(see `wrc_scraper.orchestration.partitions`).

Neither asset reimplements crawling or transformation logic (CLAUDE.md):
`landing_documents` invokes the existing `wrc` spider as a subprocess
(`scrapy_runner.run_scrapy_crawl`) and `processed_documents` calls the
existing `TransformService.transform_range` in-process. Dagster's job here is
partitioning, dependency ordering, retries, and observability only.

Two module-level seams (`run_scrapy_crawl`, `_build_transform_repos`) are
deliberately plain functions, not Dagster resources, so tests can monkeypatch
them with fakes -- matching this codebase's existing test style
(`tests/transform/test_cli.py` monkeypatches `get_events_logger` the same
way) rather than introducing a second, Dagster-specific injection mechanism.

No `from __future__ import annotations` here (unlike the rest of this
codebase): Dagster's `@asset` decorator inspects the `context` parameter's
raw annotation at import time and requires it to literally be the
`AssetExecutionContext` class, which postponed evaluation turns into an
unresolved string and breaks.
"""

import json
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    Backoff,
    Jitter,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from wrc_scraper.config import OrchestrationSettings, TransformSettings
from wrc_scraper.orchestration.partitions import partitions_def, resolve_partition
from wrc_scraper.orchestration.scrapy_runner import run_scrapy_crawl
from wrc_scraper.storage.factory import (
    StorageSettings,
    build_minio,
    build_mongo,
    build_transformed_minio,
    build_transformed_mongo,
)
from wrc_scraper.transform.service import TransformService

# Partition-level retries (retry layer 2 -- see ARCHITECTURE.md): a transient failure
# (a subprocess crash, a momentary Mongo/MinIO outage) retries this one
# partition without affecting the other ~1,775. Safe because storage is
# idempotent -- deterministic keys, MinIO written before Mongo confirms.
# Read once at import time (WRC_ORCHESTRATION_RETRY_MAX/_DELAY_SECONDS) so
# tests can drop max_retries to 0 to exercise a failure path without waiting
# through a real backoff delay.
_orchestration_settings = OrchestrationSettings.from_env()
_RETRY_POLICY = RetryPolicy(
    max_retries=_orchestration_settings.retry_max,
    delay=_orchestration_settings.retry_delay_seconds,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

_LANDING_QUALITY_CHECK = "landing_documents_quality"
_PROCESSED_QUALITY_CHECK = "processed_documents_quality"


def _read_events(events_log_file: Path) -> list[dict[str, Any]]:
    if not events_log_file.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in events_log_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return events


def _last_run_summary(events: list[dict[str, Any]]) -> dict[str, Any] | None:
    for event in reversed(events):
        if event.get("event") == "run_summary":
            return event
    return None


@asset(
    partitions_def=partitions_def,
    retry_policy=_RETRY_POLICY,
    check_specs=[AssetCheckSpec(_LANDING_QUALITY_CHECK, asset="landing_documents")],
)
def landing_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Crawl one `(month, body_slug)` partition into the Landing Zone.

    Storage writes happen exactly as in a manual `scrapy crawl` (the spider's
    own `MongoMinioPipeline`); this asset's job is to invoke that crawl,
    verify it finished cleanly, and surface its run summary as metadata.
    """
    window = resolve_partition(context.partition_key)
    partition_label = f"{window.body_slug}/{window.month_key}"

    with TemporaryDirectory() as tmp_dir:
        events_log_file = Path(tmp_dir) / "events.jsonl"
        started = time.monotonic()
        result = run_scrapy_crawl(
            start=window.start,
            end=window.end,
            body_id=window.body_id,
            events_log_file=events_log_file,
        )
        elapsed = time.monotonic() - started
        events = _read_events(events_log_file)

    if result.stderr:
        context.log.info(result.stderr)

    # Systemic failures -- the crawl process itself failed, or ended in a
    # state its own run_summary doesn't vouch for -- are raised so Dagster
    # retries/fails the partition. Per-record failures inside an otherwise
    # clean run are surfaced as metadata below instead: they don't mean the
    # crawl is broken, and shouldn't block downstream transformation.
    if result.returncode != 0:
        raise RuntimeError(
            f"scrapy crawl failed for {partition_label} (exit {result.returncode}): "
            f"{result.stderr[-2000:]}"
        )

    summary = _last_run_summary(events)
    if summary is None:
        raise RuntimeError(f"scrapy crawl for {partition_label} produced no run_summary event")

    finish_reason = summary.get("finish_reason")
    if finish_reason != "finished":
        raise RuntimeError(
            f"scrapy crawl for {partition_label} closed with finish_reason="
            f"{finish_reason!r}, not 'finished'"
        )

    records_found = summary.get("records_found", 0)
    records_failed = summary.get("records_failed", 0)
    failed_ratio = records_failed / records_found if records_found else 0.0
    threshold = OrchestrationSettings.from_env().ingest_failed_ratio_threshold

    return MaterializeResult(
        metadata={
            "month": MetadataValue.text(window.month_key),
            "body_slug": MetadataValue.text(window.body_slug),
            "records_found": MetadataValue.int(records_found),
            "records_scraped": MetadataValue.int(summary.get("records_scraped", 0)),
            "records_failed": MetadataValue.int(records_failed),
            "records_unaccounted": MetadataValue.int(summary.get("records_unaccounted", 0)),
            "partitions_completed": MetadataValue.int(summary.get("partitions_completed", 0)),
            "partitions_incomplete": MetadataValue.int(summary.get("partitions_incomplete", 0)),
            "finish_reason": MetadataValue.text(finish_reason),
            "elapsed_seconds": MetadataValue.float(round(elapsed, 2)),
        },
        check_results=[
            AssetCheckResult(
                check_name=_LANDING_QUALITY_CHECK,
                passed=finish_reason == "finished" and failed_ratio <= threshold,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "records_failed": records_failed,
                    "records_found": records_found,
                    "failed_ratio": round(failed_ratio, 4),
                    "threshold": threshold,
                },
            )
        ],
    )


def _build_transform_repos(
    storage_settings: StorageSettings, transform_settings: TransformSettings
) -> tuple[list[Any], Any, Any, Any, Any]:
    """Construct the four repositories `processed_documents` needs, plus the
    underlying clients (for closing). Factored out to one seam so tests can
    monkeypatch it with in-memory fakes instead of a real Mongo/MinIO
    connection.
    """
    source_mongo_client, source_mongo = build_mongo(storage_settings)
    source_minio = build_minio(storage_settings)
    dest_mongo_client, dest_mongo = build_transformed_mongo(storage_settings, transform_settings)
    dest_minio = build_transformed_minio(storage_settings, transform_settings)
    return (
        [source_mongo_client, dest_mongo_client],
        source_mongo,
        source_minio,
        dest_mongo,
        dest_minio,
    )


@asset(
    partitions_def=partitions_def,
    deps=[landing_documents],
    retry_policy=_RETRY_POLICY,
    check_specs=[AssetCheckSpec(_PROCESSED_QUALITY_CHECK, asset="processed_documents")],
)
def processed_documents(context: AssetExecutionContext) -> MaterializeResult:
    """Transform one `(month, body_slug)` partition's Landing Zone slice.

    In-process (no non-restartable reactor to sidestep here): calls
    `TransformService.transform_range` scoped to this partition's calendar
    month window *and* body_slug, so it only ever reads/writes the slice
    `landing_documents` at the matching partition just wrote.
    """
    window = resolve_partition(context.partition_key)
    partition_label = f"{window.body_slug}/{window.month_key}"
    storage_settings = StorageSettings.from_env()
    transform_settings = TransformSettings.from_env()

    started = time.monotonic()
    clients: list[Any] = []
    try:
        # Client construction (Mongo/MinIO) lives inside this try too: a
        # connection/config error at construction time is just as much a
        # partition-scoped startup failure as one raised later by
        # transform_range, and must carry the same body/partition context
        # rather than propagating as a bare, unwrapped exception.
        clients, source_mongo, source_minio, dest_mongo, dest_minio = _build_transform_repos(
            storage_settings, transform_settings
        )
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
        )
        summary = service.transform_range(
            window.start.isoformat(), window.end.isoformat(), body_slug=window.body_slug
        )
    except Exception as exc:  # noqa: BLE001 -- a run-level failure (client
        # construction, Mongo/MinIO unreachable, auth/config error) must fail the
        # partition clearly with its own body/date context; per-group failures
        # never reach here, TransformService already caught and counted them.
        raise RuntimeError(
            f"transform run failed for {partition_label} "
            f"(start={window.start.isoformat()} end={window.end.isoformat()}): {exc!r}"
        ) from exc
    finally:
        for client in clients:
            client.close()
    elapsed = time.monotonic() - started

    # `failed` counts actual per-group processing failures (a candidate cluster
    # with no viable member, a Mongo/MinIO write failure, an unexpected
    # exception); `dropped` counts individual variant-cluster candidates that
    # were never meant to become canonical (a losing sibling, or one candidate
    # in an otherwise-successful cluster that failed to resolve) -- expected,
    # already visible via `variant_dropped` log events and the `dropped`
    # metadata below. Only genuine failures should move this quality ratio.
    denominator = summary.found
    failed_ratio = summary.failed / denominator if denominator else 0.0
    threshold = OrchestrationSettings.from_env().transform_failed_ratio_threshold

    return MaterializeResult(
        metadata={
            "month": MetadataValue.text(window.month_key),
            "body_slug": MetadataValue.text(window.body_slug),
            "found": MetadataValue.int(summary.found),
            "transformed": MetadataValue.int(summary.transformed),
            "skipped": MetadataValue.int(summary.skipped),
            "failed": MetadataValue.int(summary.failed),
            "dropped": MetadataValue.int(summary.dropped),
            "elapsed_seconds": MetadataValue.float(round(elapsed, 2)),
        },
        check_results=[
            AssetCheckResult(
                check_name=_PROCESSED_QUALITY_CHECK,
                passed=failed_ratio <= threshold,
                severity=AssetCheckSeverity.WARN,
                metadata={
                    "failed": summary.failed,
                    "dropped": summary.dropped,
                    "found": summary.found,
                    "failed_ratio": round(failed_ratio, 4),
                    "threshold": threshold,
                },
            )
        ],
    )
