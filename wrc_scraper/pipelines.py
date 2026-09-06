"""Thin Scrapy item pipeline adapter.

Deliberately thin: all state-machine/idempotency logic lives in
IngestService (framework-agnostic, independently unit-tested). This class's
only job is Scrapy lifecycle wiring (open/close real Mongo+MinIO clients) and
turning IngestOutcome into a structured log event -- consistent with
CLAUDE.md's "keep storage/business logic separated from crawling logic."
"""

from __future__ import annotations

import json

import pymongo.errors
from minio.error import MinioException
from scrapy.exceptions import CloseSpider
from urllib3.exceptions import HTTPError as Urllib3HTTPError

from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.logging_utils import get_events_logger
from wrc_scraper.storage.factory import StorageSettings, build_minio, build_mongo
from wrc_scraper.storage.ingest_service import IngestService


class MongoMinioPipeline:
    def __init__(self) -> None:
        self._events_logger = get_events_logger()
        self._service: IngestService | None = None
        self._mongo_client = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self) -> None:
        settings = StorageSettings.from_env()

        self._mongo_client, mongo_repo = build_mongo(settings)
        mongo_repo.ensure_indexes()

        minio_repo = build_minio(settings)
        minio_repo.ensure_bucket()

        self._service = IngestService(mongo_repo, minio_repo)

    def close_spider(self) -> None:
        if self._mongo_client is not None:
            self._mongo_client.close()

    def process_item(self, item):
        assert self._service is not None, "open_spider must run before process_item"
        record = None
        try:
            record = item if isinstance(item, WrcDecisionRecord) else WrcDecisionRecord(**item)
            outcome = self._service.ingest(record)

            self._events_logger.info(
                json.dumps(
                    {
                        "event": "ingest_outcome",
                        "doc_id": outcome.doc_id,
                        "status": outcome.status,
                        "content_changed": outcome.content_changed,
                        "reason": outcome.reason,
                        "variant_cluster": outcome.variant_cluster,
                    }
                )
            )
            if outcome.variant_cluster:
                self._events_logger.warning(
                    json.dumps(
                        {
                            "event": "variant_cluster_detected",
                            "doc_id": outcome.doc_id,
                            # body_slug, not body: WrcDecisionRecord has no
                            # `body` attribute, so the old code raised an
                            # uncaught AttributeError on every variant-cluster
                            # item -- silently dropped by Scrapy, the exact
                            # invisible-failure mode this pass eliminates.
                            "body": record.body_slug,
                            "identifier": record.identifier,
                            "detail_url": record.detail_url,
                        }
                    )
                )

            return item
        except (pymongo.errors.PyMongoError, MinioException, Urllib3HTTPError) as exc:
            # Systemic store outage (Mongo/MinIO unreachable, connection pool
            # errors, S3 protocol errors). Every subsequent item would fail the
            # same slow way, so don't limp on producing a misleadingly
            # "mostly-successful"-looking run full of uncategorized per-item
            # timeouts -- fail the whole crawl fast and visibly via Scrapy's
            # own built-in CloseSpider. IngestService.ingest() already converts
            # its *guarded* put_object/mark_stored failures into
            # IngestOutcome(status="failed"); this only catches what it leaves
            # unguarded (upsert_pending, count_by_identifier, mark_unchanged,
            # mark_failed).
            self._events_logger.error(
                json.dumps(
                    {
                        "event": "storage_unavailable",
                        "reason": repr(exc),
                        "body_slug": getattr(record, "body_slug", None),
                        "identifier": getattr(record, "identifier", None),
                        "detail_url": getattr(record, "detail_url", None),
                    }
                )
            )
            raise CloseSpider(reason=f"storage unavailable: {exc!r}") from exc
        except Exception as exc:
            # An unclassified exception could be a one-off data quirk, but it
            # could equally be a systemic code bug that fails *every* item
            # identically. Wrapping it in DropItem would relabel it as an
            # intentional, routine drop -- indistinguishable in Scrapy's stats
            # from a benign drop, exactly the "successful-looking run" failure
            # mode this review removes. Instead log it as a genuine anomaly and
            # re-raise unchanged: Scrapy's per-item isolation still applies (the
            # item is dropped, the crawl continues), but the error surfaces in
            # Scrapy's own exception stats and is never silently swallowed.
            self._events_logger.error(
                json.dumps(
                    {
                        "event": "record_ingest_error",
                        "reason": repr(exc),
                        "body_slug": getattr(record, "body_slug", None),
                        "identifier": getattr(record, "identifier", None),
                        "detail_url": getattr(record, "detail_url", None),
                    }
                )
            )
            raise
