"""Thin Scrapy item pipeline adapter (Phase 3, Step 2.3 architecture).

Deliberately thin: all state-machine/idempotency logic lives in
IngestService (framework-agnostic, independently unit-tested). This class's
only job is Scrapy lifecycle wiring (open/close real Mongo+MinIO clients) and
turning IngestOutcome into a structured log event -- consistent with
CLAUDE.md's "keep storage/business logic separated from crawling logic."
"""

from __future__ import annotations

import json

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
                        "body": record.body,
                        "identifier": record.identifier,
                        "detail_url": record.detail_url,
                    }
                )
            )

        return item
