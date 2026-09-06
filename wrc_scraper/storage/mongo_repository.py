"""Thin pymongo wrapper.

Deliberately thin: every method maps to one MongoDB operation. The actual
idempotency *decisions* live in IngestService, which depends only on this
class's public interface (duck-typed -- see storage/ingest_service.py's
MongoPort) so it can be swapped for a fake in unit tests without pymongo or a
live server.
"""

from __future__ import annotations

from typing import Any


class MongoRepository:
    def __init__(self, client: Any, database: str, collection: str) -> None:
        self._collection = client[database][collection]

    def ensure_indexes(self) -> None:
        self._collection.create_index([("body_slug", 1), ("status", 1)])
        self._collection.create_index([("body_slug", 1), ("partition_date", 1)])
        # Variant clusters: several distinct detail_urls can share one
        # identifier, and the transformation stage groups by exactly this to
        # pick a canonical copy.
        self._collection.create_index([("body_slug", 1), ("identifier", 1)])
        # Covers find_stored's query shape when called with no body_slug (the
        # wrc-transform CLI's default: whole date range, all bodies) -- none of
        # the body_slug-prefixed indexes above serve that filter, so without
        # this it's a collection scan. Dagster's own processed_documents always
        # supplies body_slug and hits the compound index instead.
        self._collection.create_index([("status", 1), ("partition_date", 1)])

    def get(self, doc_id: str) -> dict | None:
        return self._collection.find_one({"_id": doc_id})

    def count_by_identifier(self, body_slug: str, identifier: str) -> int:
        return self._collection.count_documents({"body_slug": body_slug, "identifier": identifier})

    def find_stored(
        self, start_date: str, end_date: str, *, body_slug: str | None = None
    ) -> list[dict]:
        """Every `status == "stored"` record whose `partition_date` falls in
        `[start_date, end_date]` (inclusive, ISO `YYYY-MM-DD`) -- the landing
        query the transformation stage runs over. Sorted deterministically so
        repeated runs group `(body_slug, identifier)` clusters the same way.

        `body_slug`, when given, scopes the query to one deciding body -- used
        by the Dagster `processed_documents` asset, which is partitioned on
        `(month, body_slug)` and must only touch its own partition's slice.
        """
        query: dict[str, object] = {
            "status": "stored",
            "partition_date": {"$gte": start_date, "$lte": end_date},
        }
        if body_slug is not None:
            query["body_slug"] = body_slug
        cursor = self._collection.find(query).sort(
            [("body_slug", 1), ("identifier", 1), ("detail_url", 1)]
        )
        return list(cursor)

    def find_stored_by_identifier(self, body_slug: str, identifier: str) -> list[dict]:
        """Every stored landing record sharing `(body_slug, identifier)` --
        the full variant cluster (docs/SCRAPY_EXPERIMENTS.md Sec 19), including
        siblings outside the date range a transform run was invoked with, so
        canonical selection never misses a candidate because of the query window.
        """
        cursor = self._collection.find(
            {"body_slug": body_slug, "identifier": identifier, "status": "stored"}
        ).sort("detail_url", 1)
        return list(cursor)

    def upsert_pending(self, doc_id: str, *, now: str, **fields: Any) -> dict:
        """Atomic create-if-absent -- concurrent callers race
        safely on MongoDB's own `_id` uniqueness; only one insert wins, the
        rest just update. Always refreshes the descriptive metadata fields
        (identifier/description/etc.) even on an existing doc, since those can
        legitimately change on rerun; never touches status/file_hash/file_path
        here.
        """
        self._collection.update_one(
            {"_id": doc_id},
            {
                "$setOnInsert": {
                    "_id": doc_id,
                    "status": "pending",
                    "file_path": None,
                    "file_hash": None,
                    "file_size_bytes": None,
                    "remote_etag": None,
                    "error": None,
                    "first_scraped_at": now,
                    "last_changed_at": now,
                },
                "$set": {**fields, "last_checked_at": now},
            },
            upsert=True,
        )
        doc = self.get(doc_id)
        assert doc is not None
        return doc

    def mark_stored(
        self,
        doc_id: str,
        *,
        file_path: str,
        file_hash: str,
        file_size_bytes: int,
        remote_etag: str | None,
        now: str,
        content_changed: bool,
        extra: dict[str, Any] | None = None,
    ) -> None:
        """`extra` sets additional fields verbatim (e.g. the transformation
        stage's `source_file_hash`/`source_candidates` provenance) -- unused by
        the landing pipeline, which never passes it.
        """
        update: dict[str, Any] = {
            "status": "stored",
            "file_path": file_path,
            "file_hash": file_hash,
            "file_size_bytes": file_size_bytes,
            "remote_etag": remote_etag,
            "last_checked_at": now,
            "error": None,
        }
        if content_changed:
            update["last_changed_at"] = now
        if extra:
            update.update(extra)
        self._collection.update_one({"_id": doc_id}, {"$set": update})

    def mark_unchanged(self, doc_id: str, *, remote_etag: str | None, now: str) -> None:
        update: dict[str, Any] = {"last_checked_at": now, "status": "stored", "error": None}
        if remote_etag is not None:
            update["remote_etag"] = remote_etag
        self._collection.update_one({"_id": doc_id}, {"$set": update})

    def mark_failed(
        self,
        doc_id: str,
        *,
        stage: str,
        reason: str,
        now: str,
        candidate_errors: list[dict] | None = None,
    ) -> None:
        error: dict[str, object] = {
            "stage": stage,
            "reason": reason,
            "occurred_at": now,
        }
        if candidate_errors:
            error["candidate_errors"] = candidate_errors
        self._collection.update_one(
            {"_id": doc_id},
            {
                "$set": {
                    "status": "failed",
                    "error": error,
                    "last_checked_at": now,
                }
            },
        )
