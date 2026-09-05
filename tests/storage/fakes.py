"""In-memory fakes for MongoPort/MinioPort -- no Docker required.

Mirror the real repositories' contracts closely enough (including the
atomic-upsert-never-duplicates semantics MongoDB gives for free on `_id`) that
IngestService's tests exercise real state-machine behavior, not a trivial
stub.
"""

from __future__ import annotations


class FakeMongoRepository:
    def __init__(self) -> None:
        self.docs: dict[str, dict] = {}
        self.upsert_calls = 0
        self.mark_stored_calls = 0
        self.mark_unchanged_calls = 0
        self.mark_failed_calls = 0
        self.fail_on_mark_stored = False

    def get(self, doc_id: str) -> dict | None:
        doc = self.docs.get(doc_id)
        return dict(doc) if doc is not None else None

    def count_by_identifier(self, body_slug: str, identifier: str) -> int:
        return sum(
            1
            for doc in self.docs.values()
            if doc.get("body_slug") == body_slug and doc.get("identifier") == identifier
        )

    def upsert_pending(self, doc_id: str, *, now: str, **fields: object) -> dict:
        self.upsert_calls += 1
        if doc_id not in self.docs:
            self.docs[doc_id] = {
                "_id": doc_id,
                "status": "pending",
                "file_path": None,
                "file_hash": None,
                "file_size_bytes": None,
                "remote_etag": None,
                "error": None,
                "first_scraped_at": now,
                "last_changed_at": now,
            }
        self.docs[doc_id].update(fields)
        self.docs[doc_id]["last_checked_at"] = now
        return dict(self.docs[doc_id])

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
    ) -> None:
        if self.fail_on_mark_stored:
            raise RuntimeError("simulated mongo confirmation failure")
        self.mark_stored_calls += 1
        doc = self.docs[doc_id]
        doc.update(
            status="stored",
            file_path=file_path,
            file_hash=file_hash,
            file_size_bytes=file_size_bytes,
            remote_etag=remote_etag,
            last_checked_at=now,
            error=None,
        )
        if content_changed:
            doc["last_changed_at"] = now

    def mark_unchanged(self, doc_id: str, *, remote_etag: str | None, now: str) -> None:
        self.mark_unchanged_calls += 1
        doc = self.docs[doc_id]
        doc["last_checked_at"] = now
        doc["status"] = "stored"
        doc["error"] = None
        if remote_etag is not None:
            doc["remote_etag"] = remote_etag

    def mark_failed(self, doc_id: str, *, stage: str, reason: str, now: str) -> None:
        self.mark_failed_calls += 1
        doc = self.docs[doc_id]
        doc.update(
            status="failed",
            error={"stage": stage, "reason": reason, "occurred_at": now},
            last_checked_at=now,
        )


class FakeMinioRepository:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_calls = 0
        self.fail_on_put = False

    def object_exists(self, key: str) -> bool:
        return key in self.objects

    def put_object(self, key: str, data: bytes, content_type: str) -> None:
        if self.fail_on_put:
            raise RuntimeError("simulated minio failure")
        self.put_calls += 1
        self.objects[key] = data
