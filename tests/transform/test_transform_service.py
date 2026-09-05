"""Tests for wrc_scraper.transform.service.TransformService (Phase 4).

Against the same in-memory fakes used for ingestion (tests/storage/fakes.py,
CLAUDE.md: reuse existing abstractions) -- no Docker required. Covers: first
run creates transformed records, a rerun with unchanged content is skipped
without re-downloading, changed content is re-transformed, variant-cluster
canonical selection (longest wins) with dropped siblings logged, and the
empty-div.content guard failing a record.
"""

from __future__ import annotations

import json

from tests.storage.fakes import FakeMinioRepository, FakeMongoRepository
from wrc_scraper.storage.keys import transformed_minio_object_key, transformed_mongo_document_id
from wrc_scraper.transform.service import TransformService


class _RecordingLogger:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def info(self, message: str) -> None:
        self.events.append(json.loads(message))

    warning = info

    def events_named(self, name: str) -> list[dict]:
        return [event for event in self.events if event["event"] == name]


def _landing_doc(**overrides: object) -> dict:
    defaults = dict(
        _id="wrc:en/cases/2024/january/adj-00047352.html",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        identifier="ADJ-00047352",
        description="Jessica Davis V St. Vincent's Private Hospital",
        published_date="2024-01-31",
        partition_date="2024-01-01",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        document_type="html_inline",
        document_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html",
        file_path="wrc/en/cases/2024/january/adj-00047352.html",
        file_hash="hash-v1",
        status="stored",
    )
    defaults.update(overrides)
    return defaults


_CLEAN_HTML = (
    b'<div class="content"><h1>Decision</h1><p>Full decision text goes here in reasonable '
    b"length so the extracted content is long enough to be meaningfully compared.</p></div>"
)


def make_service():
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    dest_mongo = FakeMongoRepository()
    dest_minio = FakeMinioRepository()
    logger = _RecordingLogger()
    service = TransformService(
        source_mongo, source_minio, dest_mongo, dest_minio, events_logger=logger
    )
    return service, source_mongo, source_minio, dest_mongo, dest_minio, logger


def _seed(
    source_mongo: FakeMongoRepository, source_minio: FakeMinioRepository, doc: dict, html: bytes
) -> None:
    source_mongo.docs[doc["_id"]] = doc
    source_minio.objects[doc["file_path"]] = html


# -- first run / basic flow ------------------------------------------------


def test_first_run_transforms_and_renames_to_identifier_ext() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, _CLEAN_HTML)

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 1
    assert summary.transformed == 1
    assert summary.failed == 0
    dest_id = transformed_mongo_document_id("wrc", "ADJ-00047352")
    dest_key = transformed_minio_object_key("wrc", "ADJ-00047352", "html_inline")
    assert dest_key == "wrc/ADJ-00047352.html"
    assert dest_key in dest_minio.objects
    dest_doc = dest_mongo.get(dest_id)
    assert dest_doc["status"] == "stored"
    assert dest_doc["file_path"] == dest_key
    assert dest_doc["source_file_hash"] == "hash-v1"
    assert b"Decision" in dest_minio.objects[dest_key]
    assert b'class="content"' not in dest_minio.objects[dest_key]  # cleaned, not raw


def test_html_content_is_actually_cleaned_before_storage() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    raw = b'<div class="content"><span class="c1">Decision text long enough to compare</span></div>'
    _seed(source_mongo, source_minio, doc, raw)

    service.transform_range("2024-01-01", "2024-01-31")

    dest_key = transformed_minio_object_key("wrc", "ADJ-00047352", "html_inline")
    stored = dest_minio.objects[dest_key]
    assert b"class=" not in stored
    assert b"<span" not in stored


def test_pdf_record_is_stored_unmodified() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc(
        _id="eat:en/cases/2010/december/rp2147_2009.html",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
        identifier="RP2147/2009",
        document_type="pdf",
        file_path="eat/en/cases/2010/december/rp2147_2009.pdf",
        file_hash="pdf-hash-v1",
    )
    _seed(source_mongo, source_minio, doc, b"%PDF fake bytes")

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.transformed == 1
    dest_key = transformed_minio_object_key("eat", "RP2147/2009", "pdf")
    assert dest_key == "eat/RP2147-2009.pdf"
    assert dest_minio.objects[dest_key] == b"%PDF fake bytes"


# -- idempotency -------------------------------------------------------------


def test_rerun_with_unchanged_content_skips_without_redownloading() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, _CLEAN_HTML)
    service.transform_range("2024-01-01", "2024-01-31")
    assert dest_minio.put_calls == 1

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.skipped == 1
    assert summary.transformed == 0
    assert dest_minio.put_calls == 1  # never re-uploaded


def test_changed_source_content_is_retransformed() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, _CLEAN_HTML)
    service.transform_range("2024-01-01", "2024-01-31")

    source_mongo.docs[doc["_id"]]["file_hash"] = "hash-v2"
    source_minio.objects[doc["file_path"]] = (
        b'<div class="content"><h1>Updated decision</h1><p>New text long enough to compare'
        b"meaningfully against the prior version stored before.</p></div>"
    )

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.transformed == 1
    assert dest_minio.put_calls == 2
    dest_key = transformed_minio_object_key("wrc", "ADJ-00047352", "html_inline")
    assert b"Updated decision" in dest_minio.objects[dest_key]
    dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", "ADJ-00047352"))
    assert dest_doc["source_file_hash"] == "hash-v2"


def test_deleted_transformed_object_forces_reupload_despite_unchanged_source() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, _CLEAN_HTML)
    service.transform_range("2024-01-01", "2024-01-31")
    dest_key = transformed_minio_object_key("wrc", "ADJ-00047352", "html_inline")
    del dest_minio.objects[dest_key]

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.transformed == 1
    assert dest_key in dest_minio.objects


# -- variant clusters ---------------------------------------------------------


def test_variant_cluster_picks_longest_content_and_logs_dropped_sibling() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    complete = _landing_doc(
        _id="equality:en/cases/2003/dec-e2003-057-full.html",
        identifier="DEC-E2003-057",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/dec-e2003-057-full.html",
        file_path="equality/en/cases/2003/dec-e2003-057-full.html",
        file_hash="hash-complete",
        body_slug="equality",
        body_name="Equality Tribunal",
    )
    truncated = _landing_doc(
        _id="equality:en/cases/2003/dec-e2003-057-trunc.html",
        identifier="DEC-E2003-057",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/dec-e2003-057-trunc.html",
        file_path="equality/en/cases/2003/dec-e2003-057-trunc.html",
        file_hash="hash-truncated",
        body_slug="equality",
        body_name="Equality Tribunal",
    )
    _seed(
        source_mongo,
        source_minio,
        complete,
        b'<div class="content"><p>'
        + b"Full decision text. " * 40
        + b"Signed on behalf of the tribunal.</p></div>",
    )
    _seed(
        source_mongo,
        source_minio,
        truncated,
        b'<div class="content"><p>Full decision text cuts off mid-sent</p></div>',
    )

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 1
    assert summary.transformed == 1
    assert summary.dropped == 1

    dest_doc = dest_mongo.get(transformed_mongo_document_id("equality", "DEC-E2003-057"))
    assert dest_doc["detail_url"] == complete["detail_url"]
    assert dest_doc["source_file_hash"] == "hash-complete"

    dropped_events = logger.events_named("variant_dropped")
    assert len(dropped_events) == 1
    assert dropped_events[0]["detail_url"] == truncated["detail_url"]

    selected_events = logger.events_named("variant_canonical_selected")
    assert len(selected_events) == 1
    assert selected_events[0]["chosen_detail_url"] == complete["detail_url"]


def test_variant_cluster_with_one_unresolvable_sibling_is_not_double_counted() -> None:
    """A sibling whose source object can't be fetched is logged once (with its
    fetch-failure reason) inside candidate resolution -- it must not also be
    logged/counted a second time as "not_canonical" once a winner is picked.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    good = _landing_doc(
        _id="eat:en/cases/2010/december/rp2147_2009.html",
        identifier="RP2147/2009",
        detail_url="https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009.html",
        file_path="eat/en/cases/2010/december/rp2147_2009.pdf",
        file_hash="hash-good",
        document_type="pdf",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
    )
    unreachable = _landing_doc(
        _id="eat:en/cases/2010/december/rp2147_2009b.html",
        identifier="RP2147/2009",
        detail_url="https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009b.html",
        file_path="eat/en/cases/2010/december/rp2147_2009b.pdf",
        file_hash="hash-missing",
        document_type="pdf",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
    )
    _seed(source_mongo, source_minio, good, b"%PDF good bytes")
    source_mongo.docs[unreachable["_id"]] = unreachable  # object deliberately never seeded

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.transformed == 1
    assert summary.dropped == 1  # the unreachable sibling, counted exactly once
    dropped_events = logger.events_named("variant_dropped")
    assert len(dropped_events) == 1
    assert dropped_events[0]["detail_url"] == unreachable["detail_url"]
    assert "source fetch failed" in dropped_events[0]["reason"]


def test_unchanged_variant_cluster_rerun_is_skipped_without_redownload() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    first = _landing_doc(
        _id="eat:en/cases/2008/january/rp74_2007.html",
        identifier="RP74/2007",
        detail_url="https://www.workplacerelations.ie/en/cases/2008/january/rp74_2007.html",
        file_path="eat/en/cases/2008/january/rp74_2007.pdf",
        file_hash="pdf-hash-a",
        document_type="pdf",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
    )
    second = _landing_doc(
        _id="eat:en/cases/2008/january/rp75_2007.html",
        identifier="RP74/2007",
        detail_url="https://www.workplacerelations.ie/en/cases/2008/january/rp75_2007.html",
        file_path="eat/en/cases/2008/january/rp75_2007.pdf",
        file_hash="pdf-hash-a",  # byte-identical joint decision
        document_type="pdf",
        body_slug="eat",
        body_name="Employment Appeals Tribunal",
    )
    _seed(source_mongo, source_minio, first, b"%PDF joint decision")
    _seed(source_mongo, source_minio, second, b"%PDF joint decision")

    service.transform_range("2024-01-01", "2024-01-31")
    assert dest_minio.put_calls == 1

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.skipped == 1
    assert dest_minio.put_calls == 1


# -- failure handling ----------------------------------------------------------


def test_empty_div_content_fails_the_record() -> None:
    service, source_mongo, source_minio, dest_mongo, dest_minio, _logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, b'<div class="content"></div>')

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.failed == 1
    assert summary.transformed == 0
    dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", "ADJ-00047352"))
    assert dest_doc["status"] == "failed"


# -- robustness / partial-failure hardening ------------------------------------


def test_malformed_landing_record_does_not_abort_other_records() -> None:
    """A landing record missing a field its own grouping depends on (e.g. a
    corrupted write) must fail only itself -- the rest of the run must still
    complete normally.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    good = _landing_doc()
    _seed(source_mongo, source_minio, good, _CLEAN_HTML)
    malformed = _landing_doc(
        _id="wrc:en/cases/2024/january/malformed.html",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/malformed.html",
    )
    del malformed["identifier"]
    source_mongo.docs[malformed["_id"]] = malformed

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 1  # only the well-formed record could be grouped
    assert summary.transformed == 1
    assert summary.failed == 1
    dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", "ADJ-00047352"))
    assert dest_doc["status"] == "stored"
    failed_events = logger.events_named("record_failed")
    assert any(event.get("landing_doc_id") == malformed["_id"] for event in failed_events)


def test_group_with_no_resolvable_candidates_at_all_does_not_crash() -> None:
    """A race between the two Mongo queries (the record's status flips between
    `find_stored` and `find_stored_by_identifier`) can hand `_process_group` an
    empty candidate list -- it must fail cleanly rather than indexing into it.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    doc = _landing_doc()
    source_mongo.docs[doc["_id"]] = doc  # listed by find_stored...

    original_find_by_identifier = source_mongo.find_stored_by_identifier
    source_mongo.find_stored_by_identifier = lambda *a, **kw: []  # ...but gone by the time
    try:
        summary = service.transform_range("2024-01-01", "2024-01-31")
    finally:
        source_mongo.find_stored_by_identifier = original_find_by_identifier

    assert summary.found == 1
    assert summary.failed == 1
    assert summary.transformed == 0
    assert logger.events_named("record_failed")


def test_unexpected_exception_in_one_group_does_not_abort_other_groups() -> None:
    """An unhandled exception while resolving one group's candidates (here: a
    candidate missing `file_hash`, needed before any per-candidate try/except
    runs) must fail only that group.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    good = _landing_doc()
    _seed(source_mongo, source_minio, good, _CLEAN_HTML)

    broken = _landing_doc(
        _id="wrc:en/cases/2024/january/broken.html",
        identifier="ADJ-00099999",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/broken.html",
        file_path="wrc/en/cases/2024/january/broken.html",
    )
    del broken["file_hash"]
    source_mongo.docs[broken["_id"]] = broken
    source_minio.objects[broken["file_path"]] = _CLEAN_HTML

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 2
    assert summary.transformed == 1
    assert summary.failed == 1
    good_doc = dest_mongo.get(transformed_mongo_document_id("wrc", "ADJ-00047352"))
    assert good_doc["status"] == "stored"
    failed_events = logger.events_named("record_failed")
    assert any(
        event.get("doc_id") == transformed_mongo_document_id("wrc", "ADJ-00099999")
        for event in failed_events
    )


def test_unsupported_document_type_is_marked_failed_not_treated_as_binary() -> None:
    """If the site ever introduces a new file format, it must never be silently
    passed through as an opaque binary (or worse, mistaken for HTML) -- it must
    be rejected explicitly.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    doc = _landing_doc(document_type="xlsx", file_path="wrc/en/cases/2024/january/adj.xlsx")
    _seed(source_mongo, source_minio, doc, b"not a real xlsx, doesn't matter")

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.failed == 1
    assert summary.transformed == 0
    dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", "ADJ-00047352"))
    assert dest_doc["status"] == "failed"
    dropped_events = logger.events_named("variant_dropped")
    assert any("unsupported document_type" in event["reason"] for event in dropped_events)


def test_non_utf8_html_candidate_is_dropped_not_crashing_the_run() -> None:
    """A page served with a non-UTF-8 encoding must fail only that candidate --
    a decode error inside `_resolve_candidates` must never propagate and abort
    the whole cluster/run.
    """
    service, source_mongo, source_minio, dest_mongo, dest_minio, logger = make_service()
    doc = _landing_doc()
    _seed(source_mongo, source_minio, doc, b"\xff\xfe not valid utf-8 at all")

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.failed == 1
    assert summary.transformed == 0
    dropped_events = logger.events_named("variant_dropped")
    assert any("decode/clean failed" in event["reason"] for event in dropped_events)


def test_bounded_concurrency_processes_independent_groups_correctly() -> None:
    """With max_workers > 1, independent groups run on a thread pool but must
    still produce exactly the same end state as sequential processing -- no
    lost writes, no cross-group interference.
    """
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    dest_mongo = FakeMongoRepository()
    dest_minio = FakeMinioRepository()
    logger = _RecordingLogger()
    service = TransformService(
        source_mongo, source_minio, dest_mongo, dest_minio, max_workers=4, events_logger=logger
    )

    identifiers = [f"ADJ-{i:08d}" for i in range(10)]
    for i, identifier in enumerate(identifiers):
        doc = _landing_doc(
            _id=f"wrc:en/cases/2024/january/adj-{i}.html",
            identifier=identifier,
            detail_url=f"https://www.workplacerelations.ie/en/cases/2024/january/adj-{i}.html",
            file_path=f"wrc/en/cases/2024/january/adj-{i}.html",
        )
        _seed(source_mongo, source_minio, doc, _CLEAN_HTML)

    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 10
    assert summary.transformed == 10
    assert summary.failed == 0
    for identifier in identifiers:
        dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", identifier))
        assert dest_doc["status"] == "stored"
