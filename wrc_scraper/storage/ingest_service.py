"""Idempotent ingest state machine (Phase 3, Decisions 6/7/8).

Framework-agnostic: depends only on the small Protocols below (MongoPort,
MinioPort), never on Scrapy, pymongo, or the minio client directly -- so it's
fully unit-testable against hand-rolled in-memory fakes, no Docker required.

Governing rule (Decision 8): MinIO is written *before* Mongo is ever marked
"stored". A crash between the two leaves an orphaned MinIO object (harmless,
reconcilable) rather than a Mongo record pointing at nothing -- never the
reverse. Deterministic keys make a blind retry after any failure safe (PUT to
the same key with the same bytes is a no-op).

Identity is `(body_slug, detail_url)` (storage/keys.py), so two distinct pages
can never overwrite each other -- the collision problem documented in
docs/SCRAPY_EXPERIMENTS.md Sec 19 is structurally resolved, not just detected.

What remains is *variant clusters*: several distinct URLs legitimately sharing
one `identifier` (e.g. DEC-E2003-057's complete and truncated copies, or the
RP74/RP75/RP76 trio pointing at one joint decision). All of them are kept as
separate Landing records -- the Landing Zone's job is faithful capture, and
picking a winner here would risk permanently keeping the truncated copy of an
immutable store. `IngestOutcome.variant_cluster` flags them so the
transformation stage can select a canonical one per cluster.

Binary documents can also arrive already known to be unchanged: the spider
makes a conditional GET on the stored ETag (storage/conditional_get.py) and
the server may answer 304, in which case the record carries `not_modified`
and no bytes at all. That path re-confirms the prior version really is intact
before trusting the 304 -- see `_ingest_not_modified`. HTML pages expose no
validators, so they are always downloaded in full and only the *write* is
skipped, via the hash comparison below.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from typing import Protocol

from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.storage.hashing import hash_binary, hash_html
from wrc_scraper.storage.keys import minio_object_key, mongo_document_id

CONTENT_TYPES = {
    "html_inline": "text/html; charset=utf-8",
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
}


class MongoPort(Protocol):
    def get(self, doc_id: str) -> dict | None: ...
    def upsert_pending(self, doc_id: str, *, now: str, **fields: object) -> dict: ...
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
        extra: dict[str, object] | None = None,
    ) -> None: ...
    def mark_unchanged(self, doc_id: str, *, remote_etag: str | None, now: str) -> None: ...
    def mark_failed(self, doc_id: str, *, stage: str, reason: str, now: str) -> None: ...
    def count_by_identifier(self, body_slug: str, identifier: str) -> int: ...


class MinioPort(Protocol):
    def object_exists(self, key: str) -> bool: ...
    def put_object(self, key: str, data: bytes, content_type: str) -> None: ...


@dataclasses.dataclass(frozen=True)
class IngestOutcome:
    doc_id: str
    status: str  # "stored" | "failed"
    content_changed: bool
    reason: str | None = None
    variant_cluster: bool = False


def _now() -> str:
    return datetime.now(UTC).isoformat()


class IngestService:
    def __init__(self, mongo: MongoPort, minio: MinioPort) -> None:
        self._mongo = mongo
        self._minio = minio

    def ingest(self, record: WrcDecisionRecord) -> IngestOutcome:
        doc_id = mongo_document_id(record.body_slug, record.detail_url)
        key = minio_object_key(record.body_slug, record.detail_url, record.document_type)
        now = _now()

        existing = self._mongo.upsert_pending(
            doc_id,
            now=now,
            body_slug=record.body_slug,
            body_name=record.body_name,
            identifier=record.identifier,
            description=record.description,
            published_date=record.published_date,
            partition_date=record.partition_date,
            detail_url=record.detail_url,
            document_type=record.document_type,
            document_url=record.document_url,
        )

        # Identity is (body_slug, detail_url), so two distinct pages can no
        # longer overwrite each other. What remains worth surfacing is a *variant
        # cluster*: several distinct URLs sharing one identifier (confirmed
        # real -- e.g. DEC-E2003-057's complete and truncated copies). These
        # are legitimate separate Landing records; the transformation stage
        # picks a canonical one per cluster, so flag them for it.
        variant_siblings = self._mongo.count_by_identifier(record.body_slug, record.identifier)
        variant_cluster = variant_siblings > 1

        if record.not_modified:
            return self._ingest_not_modified(record, doc_id, key, existing, now, variant_cluster)

        try:
            data, content_type = self._extract_bytes(record)
        except ValueError as exc:
            self._mongo.mark_failed(doc_id, stage="extract", reason=str(exc), now=now)
            return IngestOutcome(doc_id, "failed", False, str(exc), variant_cluster)

        new_hash = hash_html(record.raw_html) if record.raw_html is not None else hash_binary(data)

        object_present = self._minio.object_exists(key)
        unchanged = (
            existing.get("status") == "stored"
            and existing.get("file_hash") == new_hash
            and object_present  # never trust a matching hash if the object is actually gone
        )

        if unchanged:
            self._mongo.mark_unchanged(doc_id, remote_etag=record.remote_etag, now=now)
            return IngestOutcome(doc_id, "stored", False, None, variant_cluster)

        try:
            self._minio.put_object(key, data, content_type)
        except Exception as exc:  # noqa: BLE001 -- any storage failure must be recorded, not raised
            self._mongo.mark_failed(doc_id, stage="minio_upload", reason=repr(exc), now=now)
            return IngestOutcome(doc_id, "failed", False, repr(exc), variant_cluster)

        content_changed = existing.get("file_hash") not in (None, new_hash)
        try:
            self._mongo.mark_stored(
                doc_id,
                file_path=key,
                file_hash=new_hash,
                file_size_bytes=len(data),
                remote_etag=record.remote_etag,
                now=now,
                content_changed=content_changed,
            )
        except Exception as exc:  # noqa: BLE001
            # MinIO write already succeeded -- the object is *not* orphaned by
            # accident, it's an accepted, recoverable side effect (Decision 8).
            # Mongo is left at "pending" (from upsert_pending above) rather
            # than "stored", so a rerun retries the confirmation safely.
            return IngestOutcome(doc_id, "failed", content_changed, repr(exc), variant_cluster)

        return IngestOutcome(doc_id, "stored", content_changed, None, variant_cluster)

    def _ingest_not_modified(
        self,
        record: WrcDecisionRecord,
        doc_id: str,
        key: str,
        existing: dict,
        now: str,
        variant_cluster: bool,
    ) -> IngestOutcome:
        """A 304 from the spider's conditional GET: the server states the
        bytes already in the Landing Zone are current, so there is nothing to
        download, hash or upload -- only `last_checked_at` to bump.

        The 304 is only *acted* on after re-confirming the prior version is
        genuinely intact and is the one this record would have written. The
        advisor checks the same things before ever sending If-None-Match, so
        reaching a failure branch here means the store changed underneath a
        live run. That is recorded as a failure -- loud and retried next run --
        rather than quietly reported as stored, since we hold no bytes to fall
        back on.
        """
        if record.document_type == "html_inline":
            reason = "not_modified is only valid for binary documents"
        elif existing.get("status") != "stored" or not existing.get("file_hash"):
            reason = "304 received but there is no stored prior version to confirm"
        elif existing.get("file_path") != key:
            reason = f"304 received but stored file_path {existing.get('file_path')!r} != {key!r}"
        elif not self._minio.object_exists(key):
            reason = "304 received but the stored object is missing from object storage"
        else:
            self._mongo.mark_unchanged(doc_id, remote_etag=record.remote_etag, now=now)
            return IngestOutcome(doc_id, "stored", False, "not_modified", variant_cluster)

        self._mongo.mark_failed(doc_id, stage="conditional_get", reason=reason, now=now)
        return IngestOutcome(doc_id, "failed", False, reason, variant_cluster)

    @staticmethod
    def _extract_bytes(record: WrcDecisionRecord) -> tuple[bytes, str]:
        content_type = CONTENT_TYPES.get(record.document_type)
        if content_type is None:
            raise ValueError(f"unknown document_type {record.document_type!r}")

        if record.document_type == "html_inline":
            if record.raw_html is None:
                raise ValueError("html_inline record has no raw_html")
            return record.raw_html.encode("utf-8"), content_type

        if record.raw_binary is None:
            raise ValueError(f"{record.document_type} record has no raw_binary")
        return record.raw_binary, content_type
