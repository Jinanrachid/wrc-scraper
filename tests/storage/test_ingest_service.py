"""Tests for wrc_scraper.storage.ingest_service (Phase 3, Decisions 6/7/8).

Against hand-rolled in-memory fakes -- no Docker required (CLAUDE.md: use
mocks/fakes for external systems in ordinary unit tests). Covers every
scenario from the approved Phase 3 test list except the ETag pre-download
skip (not implemented at this layer -- see the module docstring in
ingest_service.py and the accompanying report) and true multi-process
concurrency (simulated instead via repeated idempotent application against
the same fakes, which is what MongoDB's own atomic `_id` upsert reduces
concurrent-writer safety to).
"""

from __future__ import annotations

from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.storage.ingest_service import IngestService

from .fakes import FakeMinioRepository, FakeMongoRepository


def make_html_record(**overrides: object) -> WrcDecisionRecord:
    defaults = dict(
        identifier="ADJ-00047352",
        description="Jessica Davis V St. Vincent's Private Hospital",
        published_date="2024-01-31",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        document_type="html_inline",
        document_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        partition_date="2024-01-01",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        scraped_at="2026-01-01T00:00:00+00:00",
        raw_html="<html><body>decision text</body></html>",
    )
    defaults.update(overrides)
    return WrcDecisionRecord(**defaults)


def make_pdf_record(**overrides: object) -> WrcDecisionRecord:
    defaults = dict(
        identifier="RP2147/2009, MN1794/2009, WT796/2009",
        description="RP2147/2009, MN1794/2009, WT796/2009",
        published_date="2010-12-31",
        detail_url="https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009.html",
        document_type="pdf",
        document_url="https://www.workplacerelations.ie/en/eat_import/2010/12/x.pdf",
        partition_date="2010-12-01",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
        scraped_at="2026-01-01T00:00:00+00:00",
        raw_binary=b"%PDF-1.4 fake pdf bytes",
        remote_etag="635084654748830000",
    )
    defaults.update(overrides)
    return WrcDecisionRecord(**defaults)


def make_service() -> tuple[IngestService, FakeMongoRepository, FakeMinioRepository]:
    mongo = FakeMongoRepository()
    minio = FakeMinioRepository()
    return IngestService(mongo, minio), mongo, minio


# -- first ingestion / basic flow ----------------------------------------------


def test_first_ingestion_html_stores_and_creates_metadata() -> None:
    service, mongo, minio = make_service()
    record = make_html_record()

    outcome = service.ingest(record)

    assert outcome.status == "stored"
    assert minio.put_calls == 1
    assert minio.objects["wrc/en/cases/2024/january/adj-00047352.html"] == record.raw_html.encode(
        "utf-8"
    )
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["status"] == "stored"
    assert doc["file_path"] == "wrc/en/cases/2024/january/adj-00047352.html"
    assert doc["file_hash"] is not None
    assert doc["identifier"] == record.identifier


def test_first_ingestion_pdf_stores_raw_bytes_unmodified() -> None:
    service, mongo, minio = make_service()
    record = make_pdf_record()

    outcome = service.ingest(record)

    assert outcome.status == "stored"
    assert minio.objects["eat/en/cases/2010/december/rp2147_2009.pdf"] == record.raw_binary
    doc = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")
    assert doc["file_path"] == "eat/en/cases/2010/december/rp2147_2009.pdf"
    assert doc["remote_etag"] == "635084654748830000"


# -- duplicate rerun, unchanged --------------------------------------------------


def test_duplicate_rerun_unchanged_html_skips_minio_write() -> None:
    service, mongo, minio = make_service()
    record = make_html_record()
    service.ingest(record)

    outcome = service.ingest(record)  # identical content, rerun

    assert outcome.status == "stored"
    assert outcome.content_changed is False
    assert minio.put_calls == 1  # not called again
    assert mongo.mark_unchanged_calls == 1


def test_duplicate_rerun_unchanged_binary_after_download_skips_minio_write() -> None:
    service, mongo, minio = make_service()
    record = make_pdf_record()
    service.ingest(record)

    outcome = service.ingest(record)

    assert outcome.status == "stored"
    assert outcome.content_changed is False
    assert minio.put_calls == 1
    assert mongo.mark_unchanged_calls == 1


def test_rerun_updates_metadata_even_when_content_unchanged() -> None:
    """Requirement: metadata (identifier/description/etc.) must stay current even
    when the file content itself hasn't changed.
    """
    service, mongo, minio = make_service()
    service.ingest(make_html_record(description="Old description"))

    service.ingest(make_html_record(description="Corrected description"))

    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["description"] == "Corrected description"
    assert minio.put_calls == 1  # content itself never changed


# -- changed content ------------------------------------------------------------


def test_changed_html_content_reuploads_and_updates_hash() -> None:
    service, mongo, minio = make_service()
    service.ingest(make_html_record(raw_html="<html>version 1</html>"))

    outcome = service.ingest(make_html_record(raw_html="<html>version 2</html>"))

    assert outcome.status == "stored"
    assert outcome.content_changed is True
    assert minio.put_calls == 2
    assert minio.objects["wrc/en/cases/2024/january/adj-00047352.html"] == b"<html>version 2</html>"
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["last_changed_at"] == doc["last_checked_at"]  # bumped on this run


def test_changed_binary_content_reuploads_to_same_key() -> None:
    service, mongo, minio = make_service()
    service.ingest(make_pdf_record(raw_binary=b"%PDF version 1"))

    outcome = service.ingest(make_pdf_record(raw_binary=b"%PDF version 2", remote_etag="new-etag"))

    assert outcome.content_changed is True
    assert minio.put_calls == 2
    assert minio.objects["eat/en/cases/2010/december/rp2147_2009.pdf"] == b"%PDF version 2"
    doc = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")
    assert doc["remote_etag"] == "new-etag"


def test_first_ingestion_is_not_flagged_as_content_changed() -> None:
    service, _mongo, _minio = make_service()
    outcome = service.ingest(make_html_record())
    assert outcome.content_changed is False


# -- failure handling -------------------------------------------------------------


def test_failed_extraction_when_no_bytes_present_marks_failed_and_skips_minio() -> None:
    service, mongo, minio = make_service()
    record = make_html_record(raw_html=None)  # simulates a download that never populated bytes

    outcome = service.ingest(record)

    assert outcome.status == "failed"
    assert minio.put_calls == 0
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["status"] == "failed"
    assert doc["error"]["stage"] == "extract"


def test_minio_failure_marks_failed_and_does_not_report_stored() -> None:
    service, mongo, minio = make_service()
    minio.fail_on_put = True

    outcome = service.ingest(make_html_record())

    assert outcome.status == "failed"
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["status"] == "failed"
    assert doc["error"]["stage"] == "minio_upload"
    assert doc["file_path"] is None  # never claims a path that was never written


def test_mongo_confirmation_failure_leaves_orphaned_but_present_minio_object() -> None:
    """Decision 8: MinIO succeeding but the Mongo confirmation write failing
    must never leave Mongo saying "stored" -- the object becomes an orphan
    (harmless, recoverable), not a dangling reference.
    """
    service, mongo, minio = make_service()
    mongo.fail_on_mark_stored = True

    outcome = service.ingest(make_html_record())

    assert outcome.status == "failed"
    assert minio.put_calls == 1
    assert (
        "wrc/en/cases/2024/january/adj-00047352.html" in minio.objects
    )  # the object really is there
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["status"] == "pending"  # never "stored" -- safely retryable


def test_retry_after_mongo_confirmation_failure_recovers_to_stored() -> None:
    service, mongo, minio = make_service()
    mongo.fail_on_mark_stored = True
    service.ingest(make_html_record())
    mongo.fail_on_mark_stored = False

    outcome = service.ingest(make_html_record())  # same content, retried

    assert outcome.status == "stored"
    assert outcome.content_changed is False  # this is recovery, not a real content change
    doc = mongo.get("wrc:en/cases/2024/january/adj-00047352.html")
    assert doc["status"] == "stored"


def test_missing_minio_object_despite_matching_hash_forces_reupload() -> None:
    """A hash match alone must not be trusted if the object it refers to
    doesn't actually exist (e.g. manually deleted).
    """
    service, mongo, minio = make_service()
    record = make_html_record()
    service.ingest(record)
    del minio.objects["wrc/en/cases/2024/january/adj-00047352.html"]  # simulate manual deletion

    outcome = service.ingest(record)

    assert outcome.status == "stored"
    assert minio.put_calls == 2  # re-uploaded despite the "unchanged" hash
    assert "wrc/en/cases/2024/january/adj-00047352.html" in minio.objects


# -- concurrency / duplicate prevention -------------------------------------------


def test_repeated_processing_of_the_same_record_never_creates_a_duplicate_document() -> None:
    service, mongo, _minio = make_service()
    record = make_html_record()

    for _ in range(5):
        service.ingest(record)

    assert len(mongo.docs) == 1
    assert mongo.upsert_calls == 5  # every call reaches Mongo, but never duplicates


def test_deterministic_keys_mean_concurrent_uploads_of_identical_content_are_safe() -> None:
    """Two "concurrent" writers processing the same unchanged record both
    target the same key with the same bytes -- last-write-wins is a no-op,
    not data loss.
    """
    service, _mongo, minio = make_service()
    record = make_html_record()

    service.ingest(record)
    service.ingest(record)

    assert minio.objects["wrc/en/cases/2024/january/adj-00047352.html"] == record.raw_html.encode(
        "utf-8"
    )


# -- variant clusters (real cases, docs/SCRAPY_EXPERIMENTS.md Sec 19) -----------


def test_two_urls_sharing_one_identifier_are_kept_as_separate_records() -> None:
    """The pw18_2007 case: one real page + one empty stub sharing an
    identifier. Under the old (body, identifier) key the second overwrote the
    first; under (body, detail_url) both must survive independently.
    """
    service, mongo, minio = make_service()
    first = make_pdf_record(
        detail_url="https://www.workplacerelations.ie/en/cases/2008/january/pw18_2007.html",
        raw_binary=b"%PDF complete document",
    )
    second = make_pdf_record(
        detail_url="https://www.workplacerelations.ie/en/cases/2008/january/pw18_20071.html",
        raw_binary=b"%PDF different underlying document",
    )

    service.ingest(first)
    outcome = service.ingest(second)

    # Two distinct Mongo docs and two distinct MinIO objects -- nothing lost.
    assert len(mongo.docs) == 2
    assert len(minio.objects) == 2
    assert mongo.get("eat:en/cases/2008/january/pw18_2007.html")["detail_url"] == first.detail_url
    assert mongo.get("eat:en/cases/2008/january/pw18_20071.html")["detail_url"] == second.detail_url
    assert minio.objects["eat/en/cases/2008/january/pw18_2007.pdf"] == b"%PDF complete document"

    # ...but they're flagged as a variant cluster so the transformation stage
    # knows to pick a canonical copy.
    assert outcome.variant_cluster is True


def test_no_variant_cluster_flagged_for_a_genuine_unchanged_rerun() -> None:
    service, _mongo, _minio = make_service()
    record = make_pdf_record()
    service.ingest(record)

    outcome = service.ingest(record)

    assert outcome.variant_cluster is False


# -- conditional GET / 304 Not Modified ------------------------------------------


def test_not_modified_confirms_the_stored_copy_without_reuploading() -> None:
    """The whole point of the optimization: a 304 carries no bytes, and none
    are needed -- no re-hash, no re-upload, just a fresh last_checked_at.
    """
    service, mongo, minio = make_service()
    service.ingest(make_pdf_record())
    hash_before = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")["file_hash"]

    outcome = service.ingest(make_pdf_record(raw_binary=None, not_modified=True))

    assert outcome.status == "stored"
    assert outcome.content_changed is False
    assert outcome.reason == "not_modified"
    assert minio.put_calls == 1  # never written a second time
    assert mongo.mark_unchanged_calls == 1
    doc = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")
    assert doc["file_hash"] == hash_before  # the hash of the bytes we still hold
    assert doc["last_changed_at"] != doc["last_checked_at"]  # checked, not changed


def test_not_modified_still_refreshes_metadata() -> None:
    service, mongo, _minio = make_service()
    service.ingest(make_pdf_record(description="Old description"))

    service.ingest(
        make_pdf_record(raw_binary=None, not_modified=True, description="Corrected description")
    )

    doc = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")
    assert doc["description"] == "Corrected description"


def test_not_modified_without_a_stored_prior_version_fails_loudly() -> None:
    """A 304 we can't corroborate must never be reported as stored -- we hold
    no bytes to fall back on, so the record is failed and retried next run.
    """
    service, mongo, minio = make_service()

    outcome = service.ingest(make_pdf_record(raw_binary=None, not_modified=True))

    assert outcome.status == "failed"
    assert minio.put_calls == 0
    doc = mongo.get("eat:en/cases/2010/december/rp2147_2009.html")
    assert doc["status"] == "failed"
    assert doc["error"]["stage"] == "conditional_get"


def test_not_modified_with_a_deleted_object_fails_rather_than_claiming_stored() -> None:
    service, mongo, minio = make_service()
    service.ingest(make_pdf_record())
    del minio.objects["eat/en/cases/2010/december/rp2147_2009.pdf"]  # deleted mid-run

    outcome = service.ingest(make_pdf_record(raw_binary=None, not_modified=True))

    assert outcome.status == "failed"
    assert "missing from object storage" in outcome.reason
    assert mongo.get("eat:en/cases/2010/december/rp2147_2009.html")["status"] == "failed"


def test_not_modified_is_rejected_for_html_records() -> None:
    """HTML pages expose no validators, so a 304 could only come from a bug."""
    service, _mongo, _minio = make_service()
    service.ingest(make_html_record())

    outcome = service.ingest(make_html_record(raw_html=None, not_modified=True))

    assert outcome.status == "failed"
    assert "only valid for binary documents" in outcome.reason
