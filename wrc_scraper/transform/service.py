"""Transformation state machine.

Mirrors `wrc_scraper.storage.ingest_service.IngestService`: framework-agnostic
(depends only on the Protocols below, never on pymongo/minio directly), so it
is fully unit-testable against the same in-memory fakes used for ingestion
(`tests/storage/fakes.py`).

Reads landing metadata + Landing Zone bytes, never writes back to either (the
Landing Zone is immutable -- CLAUDE.md). For each distinct `(body_slug,
identifier)` group found in the requested date range:

1. Resolve the full variant cluster (`SourceMongoPort.find_stored_by_identifier`)
   -- not just the members inside the requested range, so a sibling scraped
   under a different partition can never be missed (docs/SCRAPY_EXPERIMENTS.md
   Sec 19).
2. Idempotency fast path: if the transformed record already reflects this exact
   set of `(detail_url, file_hash)` pairs and its object is still present, skip
   entirely -- no download, no re-cleaning (`source_candidates` is the
   "reference" the assessment's idempotency requirement calls for).
3. Otherwise fetch + clean (html) or fetch as-is (pdf/doc/docx) every candidate,
   and pick the canonical one. Document-type precedence is applied first: a
   non-empty HTML variant always wins over PDF/DOC/DOCX (an HTML variant that
   resolved empty never does -- and never can, since `clean_html` already
   drops empty/missing div.content before it reaches candidate selection).
   Within the winning document type, longest cleaned/raw content wins, a
   detected signature block is the secondary tiebreaker, and `detail_url`
   breaks any remaining tie deterministically. HTML char counts and binary
   byte sizes are never compared against each other. Near-ties (within the
   winning type) are logged as ambiguous rather than decided silently.
4. Write the canonical bytes to the transformed bucket under
   `{body_slug}/{sanitize_identifier(identifier)}.{ext}`, and the transformed
   metadata (new `file_path`, new `file_hash`, `source_file_hash` /
   `source_candidates` provenance) to the transformed collection. MinIO is
   written before Mongo is marked "stored" -- same ordering rule as
   `IngestService`.

Every candidate that is not the canonical copy is logged (`variant_dropped`)
with its reason -- a fetch/clean failure or simply losing the selection.

Groups are independent units of failure: a malformed landing record, an
unsupported `document_type`, a fetch/decode error, or any other unexpected
exception while resolving one `(body_slug, identifier)` group is caught and
logged as a single failed record (`_process_group_safely`), never aborting
the run or affecting unrelated groups. Groups may be processed with bounded
thread-pool concurrency (`max_workers`) since each touches only its own Mongo
document and MinIO key.
"""

from __future__ import annotations

import dataclasses
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Protocol

from wrc_scraper.logging_utils import get_events_logger
from wrc_scraper.storage.hashing import hash_binary
from wrc_scraper.storage.ingest_service import CONTENT_TYPES
from wrc_scraper.storage.keys import transformed_minio_object_key, transformed_mongo_document_id
from wrc_scraper.transform.html_cleaner import clean_html

# document_type values the transformation stage knows how to handle (a straight
# copy for pdf/doc/docx, a BeautifulSoup clean for html_inline). Anything else
# -- a malformed record or a format the site introduces later -- must be
# rejected explicitly rather than silently copied through as if it were a
# known binary type (assessment: "do not silently corrupt an unknown file type").
_KNOWN_DOCUMENT_TYPES = frozenset(CONTENT_TYPES)


class SourceMongoPort(Protocol):
    def find_stored(
        self, start_date: str, end_date: str, *, body_slug: str | None = None
    ) -> list[dict]: ...
    def find_stored_by_identifier(self, body_slug: str, identifier: str) -> list[dict]: ...


class SourceMinioPort(Protocol):
    def get_object(self, key: str) -> bytes: ...


class DestMongoPort(Protocol):
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
    def mark_failed(
        self,
        doc_id: str,
        *,
        stage: str,
        reason: str,
        now: str,
        candidate_errors: list[dict] | None = None,
    ) -> None: ...


class DestMinioPort(Protocol):
    def object_exists(self, key: str) -> bool: ...
    def put_object(self, key: str, data: bytes, content_type: str) -> None: ...


@dataclasses.dataclass(frozen=True)
class RunSummary:
    found: int = 0
    transformed: int = 0
    skipped: int = 0
    failed: int = 0
    dropped: int = 0


@dataclasses.dataclass(frozen=True)
class _Resolved:
    candidate: dict
    data: bytes
    text_length: int
    has_signature_block: bool


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _source_candidates(candidates: list[dict]) -> list[dict]:
    return sorted(
        (
            {"detail_url": candidate["detail_url"], "file_hash": candidate["file_hash"]}
            for candidate in candidates
        ),
        key=lambda entry: entry["detail_url"],
    )


class TransformService:
    def __init__(
        self,
        source_mongo: SourceMongoPort,
        source_minio: SourceMinioPort,
        dest_mongo: DestMongoPort,
        dest_minio: DestMinioPort,
        *,
        keep_images: bool = False,
        near_tie_chars: int = 50,
        max_workers: int = 1,
        events_logger: object | None = None,
    ) -> None:
        self._source_mongo = source_mongo
        self._source_minio = source_minio
        self._dest_mongo = dest_mongo
        self._dest_minio = dest_minio
        self._keep_images = keep_images
        self._near_tie_chars = near_tie_chars
        self._max_workers = max(1, max_workers)
        self._events_logger = events_logger or get_events_logger()

    def transform_range(
        self, start_date: str, end_date: str, *, body_slug: str | None = None
    ) -> RunSummary:
        """Transform every landing record in `[start_date, end_date]`.

        `body_slug`, when given, scopes the run to one deciding body -- used
        by the Dagster `processed_documents` asset, which is partitioned on
        `(month, body_slug)` and must only read/write its own partition's
        slice. Left `None` (the default) for standalone CLI use, which keeps
        processing every body in the date range -- a wider scope, still
        correct.
        """
        landing_records = self._source_mongo.find_stored(start_date, end_date, body_slug=body_slug)
        self._log(
            "transform_started",
            start_date=start_date,
            end_date=end_date,
            body_slug=body_slug,
            landing_records_found=len(landing_records),
        )

        found = failed = 0
        seen_groups: set[tuple[str, str]] = set()
        group_keys: list[tuple[str, str]] = []

        for record in landing_records:
            try:
                group_key = (record["body_slug"], record["identifier"])
            except KeyError as exc:
                # A landing record missing a field its own identity depends on
                # can't be grouped at all -- this is a data-integrity problem
                # with that one record, not the whole run (assessment: "malformed
                # /missing metadata" must fail clearly without derailing the rest).
                failed += 1
                self._log(
                    "record_failed",
                    landing_doc_id=record.get("_id"),
                    reason=f"landing record missing required field: {exc}",
                )
                continue
            if group_key in seen_groups:
                continue
            seen_groups.add(group_key)
            found += 1
            group_keys.append(group_key)

        # Each group is independent (own Mongo doc, own MinIO key), so bounded
        # concurrency is safe: pymongo's MongoClient and the minio-py client are
        # both documented safe for concurrent use from multiple threads, and
        # `pool.map` still returns results in submission order, so aggregation
        # and tests stay deterministic regardless of thread interleaving.
        # `max_workers=1` (the default) runs fully sequentially with no thread
        # pool at all -- the original, single-threaded behavior.
        if self._max_workers <= 1 or len(group_keys) <= 1:
            results = [self._process_group_safely(group_key) for group_key in group_keys]
        else:
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                results = list(pool.map(self._process_group_safely, group_keys))

        transformed = skipped = dropped = 0
        for kind, dropped_count in results:
            dropped += dropped_count
            if kind == "transformed":
                transformed += 1
            elif kind == "skipped":
                skipped += 1
            else:
                failed += 1

        summary = RunSummary(
            found=found, transformed=transformed, skipped=skipped, failed=failed, dropped=dropped
        )
        self._log("run_summary", **dataclasses.asdict(summary))
        return summary

    def _process_group_safely(self, group_key: tuple[str, str]) -> tuple[str, int]:
        """Resolve and process one `(body_slug, identifier)` group, converting
        any unexpected failure -- a Mongo query interruption, a candidate
        missing a field `_process_group` assumes is present, or anything else
        not already handled inline -- into a single failed record rather than
        letting it abort the rest of the run (assessment: one document's
        failure must not affect unrelated documents).
        """
        try:
            candidates = self._source_mongo.find_stored_by_identifier(*group_key)
            return self._process_group(group_key, candidates)
        except Exception as exc:  # noqa: BLE001 -- last-resort per-group safety net
            doc_id = transformed_mongo_document_id(*group_key)
            self._log("record_failed", doc_id=doc_id, reason=repr(exc))
            return "failed", 0

    # -- one (body_slug, identifier) group ----------------------------------

    def _process_group(self, group_key: tuple[str, str], candidates: list[dict]) -> tuple[str, int]:
        body_slug, identifier = group_key
        doc_id = transformed_mongo_document_id(body_slug, identifier)
        now = _now()

        if not candidates:
            # A race between `find_stored` (built the group list) and
            # `find_stored_by_identifier` (resolved it): the record no longer
            # matches `status == "stored"`. No candidate means no metadata to
            # write anywhere -- log and fail the group rather than indexing
            # into an empty list.
            self._log(
                "record_failed",
                doc_id=doc_id,
                reason="no stored landing records found for this identifier",
            )
            return "failed", 0

        source_candidates = _source_candidates(candidates)
        by_detail_url = {candidate["detail_url"]: candidate for candidate in candidates}

        existing = self._dest_mongo.get(doc_id)
        if (
            existing is not None
            and existing.get("status") == "stored"
            and existing.get("source_candidates") == source_candidates
            and existing.get("file_path")
            and existing.get("detail_url") in by_detail_url
            and self._dest_minio.object_exists(existing["file_path"])
        ):
            canonical_landing = by_detail_url[existing["detail_url"]]
            self._dest_mongo.upsert_pending(
                doc_id, now=now, **self._metadata_fields(canonical_landing)
            )
            self._dest_mongo.mark_unchanged(doc_id, remote_etag=None, now=now)
            self._log("record_skipped", doc_id=doc_id, reason="unchanged")
            dropped = 0
            for candidate in candidates:
                if candidate["detail_url"] == canonical_landing["detail_url"]:
                    continue
                dropped += 1
                self._log(
                    "variant_dropped",
                    doc_id=doc_id,
                    detail_url=candidate["detail_url"],
                    reason="unchanged_cluster",
                )
            return "skipped", dropped

        resolved, unresolved_count, candidate_errors = self._resolve_candidates(doc_id, candidates)
        if not resolved:
            # upsert_pending first: mark_failed only updates an existing doc, and
            # every candidate having failed to resolve means no other branch has
            # created one yet this run.
            self._dest_mongo.upsert_pending(doc_id, now=now, **self._metadata_fields(candidates[0]))
            self._dest_mongo.mark_failed(
                doc_id,
                stage="transform",
                reason="no viable candidate in cluster",
                now=now,
                candidate_errors=candidate_errors or None,
            )
            self._log("record_failed", doc_id=doc_id, reason="no viable candidate in cluster")
            return "failed", 0

        # Document-type precedence: a non-empty HTML variant always outranks
        # PDF/DOC/DOCX (never the reverse), and an HTML variant that resolved
        # empty never outranks a valid binary -- but `clean_html` already
        # drops empty/missing div.content candidates before they reach
        # `resolved` (see _resolve_candidates), so `text_length > 0` here is a
        # defensive restatement of that guarantee, not new behavior. Within
        # one document type, `text_length` is a comparable unit (chars for
        # HTML, bytes for binary); across types it deliberately never is, so
        # HTML and binary candidates are never sorted against each other.
        html_variants = [
            r
            for r in resolved
            if r.candidate["document_type"] == "html_inline" and r.text_length > 0
        ]
        selection_pool = html_variants or resolved
        selection_pool.sort(
            key=lambda r: (-r.text_length, not r.has_signature_block, r.candidate["detail_url"])
        )
        canonical = selection_pool[0]

        if len(selection_pool) > 1:
            gap = selection_pool[0].text_length - selection_pool[1].text_length
            if gap <= self._near_tie_chars:
                self._log(
                    "variant_selection_ambiguous",
                    doc_id=doc_id,
                    top=selection_pool[0].candidate["detail_url"],
                    runner_up=selection_pool[1].candidate["detail_url"],
                    text_length_gap=gap,
                )

        if len(candidates) > 1:
            self._log(
                "variant_canonical_selected",
                doc_id=doc_id,
                chosen_detail_url=canonical.candidate["detail_url"],
                cluster_size=len(candidates),
            )

        document_type = canonical.candidate["document_type"]
        key = transformed_minio_object_key(body_slug, identifier, document_type)
        new_hash = hash_binary(canonical.data)
        content_type = CONTENT_TYPES[document_type]

        upserted = self._dest_mongo.upsert_pending(
            doc_id, now=now, **self._metadata_fields(canonical.candidate)
        )

        try:
            self._dest_minio.put_object(key, canonical.data, content_type)
        except Exception as exc:  # noqa: BLE001 -- any storage failure must be recorded, not raised
            self._dest_mongo.mark_failed(doc_id, stage="minio_upload", reason=repr(exc), now=now)
            self._log("record_failed", doc_id=doc_id, reason=repr(exc))
            return "failed", 0

        content_changed = upserted.get("file_hash") not in (None, new_hash)
        try:
            self._dest_mongo.mark_stored(
                doc_id,
                file_path=key,
                file_hash=new_hash,
                file_size_bytes=len(canonical.data),
                remote_etag=None,
                now=now,
                content_changed=content_changed,
                extra={
                    "source_candidates": source_candidates,
                    "source_file_hash": canonical.candidate["file_hash"],
                    "source_doc_id": canonical.candidate["_id"],
                },
            )
        except Exception as exc:  # noqa: BLE001 -- mirrors IngestService's ordering rule:
            # MinIO already succeeded; the object becomes a harmless, retryable
            # orphan rather than Mongo claiming a "stored" it can't confirm.
            self._log("record_failed", doc_id=doc_id, reason=repr(exc))
            return "failed", 0

        self._log(
            "record_transformed",
            doc_id=doc_id,
            source_doc_id=canonical.candidate["_id"],
            source_file_hash=canonical.candidate["file_hash"],
            file_hash=new_hash,
            file_path=key,
        )
        # `unresolved_count` candidates were already logged (with their own fetch/
        # clean-failure reason) inside _resolve_candidates -- only the candidates
        # that resolved but lost the selection are logged here, so no candidate is
        # ever counted or logged as dropped twice.
        dropped = unresolved_count
        for candidate_result in resolved:
            if candidate_result.candidate["detail_url"] == canonical.candidate["detail_url"]:
                continue
            dropped += 1
            self._log(
                "variant_dropped",
                doc_id=doc_id,
                detail_url=candidate_result.candidate["detail_url"],
                reason="not_canonical",
            )
        return "transformed", dropped

    def _resolve_candidates(
        self, doc_id: str, candidates: list[dict]
    ) -> tuple[list[_Resolved], int, list[dict]]:
        """Resolve candidates into `_Resolved` objects.

        Returns ``(resolved, unresolved_count, candidate_errors)`` where
        ``candidate_errors`` is a list of ``{detail_url, reason}`` dicts for
        every candidate that could not be resolved -- so callers can surface
        the root cause in Mongo rather than only the aggregate outcome.
        """
        resolved: list[_Resolved] = []
        unresolved_count = 0
        candidate_errors: list[dict] = []
        for candidate in candidates:
            detail_url = candidate.get("detail_url")
            document_type = candidate.get("document_type")

            if document_type not in _KNOWN_DOCUMENT_TYPES:
                # An unknown document_type is never guessed at as HTML or a
                # passthrough binary -- explicit rejection (assessment: a new
                # file format must not be silently corrupted or mistreated).
                unresolved_count += 1
                reason = f"unsupported document_type {document_type!r}"
                self._log(
                    "variant_dropped",
                    doc_id=doc_id,
                    detail_url=detail_url,
                    reason=reason,
                )
                candidate_errors.append({"detail_url": detail_url, "reason": reason})
                continue

            try:
                data = self._source_minio.get_object(candidate["file_path"])
            except Exception as exc:  # noqa: BLE001 -- an unreachable source object fails this
                # one candidate, not the whole cluster; the remaining candidates are
                # still eligible to become canonical.
                unresolved_count += 1
                reason = f"source fetch failed: {exc!r}"
                self._log(
                    "variant_dropped",
                    doc_id=doc_id,
                    detail_url=detail_url,
                    reason=reason,
                )
                candidate_errors.append({"detail_url": detail_url, "reason": reason})
                continue

            if document_type == "html_inline":
                try:
                    cleaned = clean_html(
                        data.decode("utf-8"), candidate["identifier"], keep_images=self._keep_images
                    )
                except Exception as exc:  # noqa: BLE001 -- a decode/parse failure fails only
                    # this one candidate (e.g. a page served with a non-UTF-8 encoding);
                    # siblings in the cluster are still eligible to become canonical.
                    unresolved_count += 1
                    reason = f"decode/clean failed: {exc!r}"
                    self._log(
                        "variant_dropped",
                        doc_id=doc_id,
                        detail_url=detail_url,
                        reason=reason,
                    )
                    candidate_errors.append({"detail_url": detail_url, "reason": reason})
                    continue
                if cleaned is None:
                    unresolved_count += 1
                    reason = "empty or missing div.content"
                    self._log(
                        "variant_dropped",
                        doc_id=doc_id,
                        detail_url=detail_url,
                        reason=reason,
                    )
                    candidate_errors.append({"detail_url": detail_url, "reason": reason})
                    continue
                resolved.append(
                    _Resolved(
                        candidate, cleaned.html, cleaned.text_length, cleaned.has_signature_block
                    )
                )
            else:
                resolved.append(_Resolved(candidate, data, len(data), False))
        return resolved, unresolved_count, candidate_errors

    @staticmethod
    def _metadata_fields(candidate: dict) -> dict[str, object]:
        return {
            "body_slug": candidate["body_slug"],
            "body_name": candidate["body_name"],
            "identifier": candidate["identifier"],
            "description": candidate["description"],
            "published_date": candidate["published_date"],
            "partition_date": candidate["partition_date"],
            "detail_url": candidate["detail_url"],
            "source_document_type": candidate["document_type"],
        }

    def _log(self, event: str, **fields: object) -> None:
        self._events_logger.info(json.dumps({"event": event, **fields}))
