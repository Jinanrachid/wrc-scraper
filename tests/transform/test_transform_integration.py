"""Docker-backed integration tests for the transformation stage (Phase 4).

Exercises `TransformService` against real MongoDB + MinIO (not the in-memory
fakes used elsewhere in tests/transform/) to validate what fakes structurally
cannot: real `pymongo.MongoClient` / minio-py client behavior under the bounded
thread-pool concurrency introduced in the hardening pass, genuine Landing Zone
immutability against a real object store, and the complete-vs-incomplete
duplicate-identifier resolution against real round-tripped documents.

Same convention as tests/storage/test_integration.py: auto-skips (not a hard
failure) if MongoDB/MinIO aren't reachable, so the default `pytest` run stays
fast and Docker-independent. Everything is written under throwaway
database/bucket names and torn down afterward -- nothing here ever touches
`wrc`/`wrc-landing`, the real Landing Zone.
"""

from __future__ import annotations

import os
import time
import uuid

import pytest

from wrc_scraper.config import TransformSettings
from wrc_scraper.storage.hashing import hash_binary
from wrc_scraper.storage.ingest_service import CONTENT_TYPES
from wrc_scraper.storage.keys import transformed_minio_object_key, transformed_mongo_document_id
from wrc_scraper.storage.minio_repository import MinioRepository
from wrc_scraper.storage.mongo_repository import MongoRepository
from wrc_scraper.transform.service import TransformService

MONGO_URI = os.environ.get("WRC_MONGO_URI", "mongodb://localhost:27017")
MINIO_ENDPOINT = os.environ.get("WRC_MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("WRC_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("WRC_MINIO_SECRET_KEY", "minioadmin")


@pytest.fixture
def mongo_client():
    pymongo = pytest.importorskip("pymongo")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable at {MONGO_URI}: {exc}")
    database = f"wrc_transform_test_{uuid.uuid4().hex[:8]}"
    yield client, database
    client.drop_database(database)
    client.close()


@pytest.fixture
def minio_client():
    minio_module = pytest.importorskip("minio")
    try:
        client = minio_module.Minio(
            MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False
        )
        client.list_buckets()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    yield client
    # Bucket cleanup happens per-bucket in the `stores` fixture, which knows
    # the exact bucket names it created.


@pytest.fixture
def stores(mongo_client, minio_client):
    """Four real repositories wired exactly like production
    (storage/factory.py): source + dest share one Mongo database (different
    collections), each side gets its own MinIO bucket -- all under throwaway
    names, torn down after the test.
    """
    client, database = mongo_client
    source_mongo = MongoRepository(client, database, "landing_metadata")
    dest_mongo = MongoRepository(client, database, "transformed_metadata")
    source_mongo.ensure_indexes()
    dest_mongo.ensure_indexes()

    source_bucket = f"wrc-transform-test-src-{uuid.uuid4().hex[:8]}"
    dest_bucket = f"wrc-transform-test-dest-{uuid.uuid4().hex[:8]}"
    source_minio = MinioRepository(minio_client, source_bucket)
    dest_minio = MinioRepository(minio_client, dest_bucket)
    source_minio.ensure_bucket()
    dest_minio.ensure_bucket()

    yield source_mongo, source_minio, dest_mongo, dest_minio

    for bucket in (source_bucket, dest_bucket):
        for obj in minio_client.list_objects(bucket, recursive=True):
            minio_client.remove_object(bucket, obj.object_name)
        minio_client.remove_bucket(bucket)


def _seed_landing(
    mongo_repo: MongoRepository,
    minio_repo: MinioRepository,
    *,
    doc_id: str,
    file_path: str,
    data: bytes,
    document_type: str = "html_inline",
    **fields: object,
) -> None:
    """Write one Landing Zone record the way IngestService would have left it
    (status "stored", file_path/file_hash set) -- directly through the real
    repositories, bypassing IngestService/Scrapy since only the transform
    stage's behavior is under test here.
    """
    now = "2026-01-01T00:00:00+00:00"
    defaults: dict[str, object] = {
        "body_slug": "wrc",
        "body_name": "Workplace Relations Commission",
        "description": "Test party v. Test respondent",
        "published_date": "2024-01-31",
        "partition_date": "2024-01-01",
        "document_type": document_type,
        "document_url": fields.get("detail_url"),
    }
    defaults.update(fields)
    mongo_repo.upsert_pending(doc_id, now=now, **defaults)
    minio_repo.put_object(file_path, data, CONTENT_TYPES[document_type])
    mongo_repo.mark_stored(
        doc_id,
        file_path=file_path,
        file_hash=hash_binary(data),
        file_size_bytes=len(data),
        remote_etag=None,
        now=now,
        content_changed=True,
    )


def _snapshot_landing(mongo_repo: MongoRepository, minio_repo: MinioRepository) -> dict:
    """A comparable snapshot of every stored landing record (metadata dict
    minus volatile bookkeeping timestamps, plus the raw object bytes) -- used
    to prove the Landing Zone is byte-for-byte and doc-for-doc unchanged
    across a transform run.
    """
    docs = mongo_repo.find_stored("0001-01-01", "9999-12-31")
    volatile = {"last_checked_at", "last_changed_at", "first_scraped_at"}
    return {
        doc["_id"]: (
            {k: v for k, v in doc.items() if k not in volatile},
            minio_repo.get_object(doc["file_path"]),
        )
        for doc in docs
    }


# -- basic real-service round trip --------------------------------------------


def test_transform_against_real_services_end_to_end(stores) -> None:
    source_mongo, source_minio, dest_mongo, dest_minio = stores
    _seed_landing(
        source_mongo,
        source_minio,
        doc_id="wrc:en/cases/2024/january/adj-1.html",
        file_path="wrc/en/cases/2024/january/adj-1.html",
        data=b'<div class="content"><h1>Decision</h1><p>'
        + b"Real integration test content. " * 10
        + b"</p></div>",
        identifier="ADJ-INTEGRATION-1",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
    )

    service = TransformService(source_mongo, source_minio, dest_mongo, dest_minio)
    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 1
    assert summary.transformed == 1
    assert summary.failed == 0

    dest_id = transformed_mongo_document_id("wrc", "ADJ-INTEGRATION-1")
    dest_key = transformed_minio_object_key("wrc", "ADJ-INTEGRATION-1", "html_inline")
    dest_doc = dest_mongo.get(dest_id)
    assert dest_doc["status"] == "stored"
    assert dest_doc["file_path"] == dest_key
    stored_bytes = dest_minio.get_object(dest_key)
    assert b"Real integration test content" in stored_bytes
    assert b'class="content"' not in stored_bytes  # actually cleaned, not a raw copy


# -- complete vs incomplete duplicate identifier -------------------------------


def test_duplicate_identifier_complete_record_wins_against_real_services(stores) -> None:
    source_mongo, source_minio, dest_mongo, dest_minio = stores
    complete_html = (
        b'<div class="content"><p>'
        + b"Full decision text with real substance. " * 30
        + b"Signed on behalf of the tribunal.</p></div>"
    )
    truncated_html = b'<div class="content"><p>Full decision text cuts off mid-sent</p></div>'

    _seed_landing(
        source_mongo,
        source_minio,
        doc_id="equality:en/cases/2003/dec-e2003-999-full.html",
        file_path="equality/en/cases/2003/dec-e2003-999-full.html",
        data=complete_html,
        identifier="DEC-E2003-999",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/dec-e2003-999-full.html",
        body_slug="equality",
        body_name="Equality Tribunal",
    )
    _seed_landing(
        source_mongo,
        source_minio,
        doc_id="equality:en/cases/2003/dec-e2003-999-trunc.html",
        file_path="equality/en/cases/2003/dec-e2003-999-trunc.html",
        data=truncated_html,
        identifier="DEC-E2003-999",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/dec-e2003-999-trunc.html",
        body_slug="equality",
        body_name="Equality Tribunal",
    )
    landing_before = _snapshot_landing(source_mongo, source_minio)

    service = TransformService(source_mongo, source_minio, dest_mongo, dest_minio)
    summary = service.transform_range("2024-01-01", "2024-01-31")

    assert summary.found == 1  # one identifier group, two candidates
    assert summary.transformed == 1
    assert summary.dropped == 1  # the truncated sibling, logged and dropped

    dest_doc = dest_mongo.get(transformed_mongo_document_id("equality", "DEC-E2003-999"))
    assert (
        dest_doc["detail_url"]
        == "https://www.workplacerelations.ie/en/cases/2003/dec-e2003-999-full.html"
    )
    stored_bytes = dest_minio.get_object(dest_doc["file_path"])
    assert b"Signed on behalf of the tribunal" in stored_bytes
    assert b"cuts off mid-sent" not in stored_bytes

    # Both original Landing Zone records -- complete AND truncated -- must
    # still exist, byte-for-byte, completely untouched by the transform run.
    landing_after = _snapshot_landing(source_mongo, source_minio)
    assert landing_after == landing_before
    assert len(landing_after) == 2


def test_duplicate_identifier_resolution_is_independent_of_processing_order(stores) -> None:
    """The transform stage resolves the full cluster via
    `find_stored_by_identifier` regardless of which landing record triggered
    it -- seeding the truncated copy under a lexicographically earlier
    detail_url (so it would be iterated first) must not change the outcome.
    """
    source_mongo, source_minio, dest_mongo, dest_minio = stores
    _seed_landing(
        source_mongo,
        source_minio,
        doc_id="equality:en/cases/2003/a-trunc.html",
        file_path="equality/en/cases/2003/a-trunc.html",
        data=b'<div class="content"><p>Cuts off mid-sent</p></div>',
        identifier="DEC-E2003-ORDER",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/a-trunc.html",
        body_slug="equality",
        body_name="Equality Tribunal",
    )
    _seed_landing(
        source_mongo,
        source_minio,
        doc_id="equality:en/cases/2003/z-full.html",
        file_path="equality/en/cases/2003/z-full.html",
        data=b'<div class="content"><p>'
        + b"Full decision text with real substance. " * 30
        + b"Signed on behalf of the tribunal.</p></div>",
        identifier="DEC-E2003-ORDER",
        detail_url="https://www.workplacerelations.ie/en/cases/2003/z-full.html",
        body_slug="equality",
        body_name="Equality Tribunal",
    )

    service = TransformService(source_mongo, source_minio, dest_mongo, dest_minio)
    service.transform_range("2024-01-01", "2024-01-31")

    dest_doc = dest_mongo.get(transformed_mongo_document_id("equality", "DEC-E2003-ORDER"))
    assert dest_doc["detail_url"].endswith("z-full.html")  # complete copy, despite sorting last


# -- bounded concurrency: load validation against real services ---------------


def test_configured_concurrency_transforms_a_batch_correctly(stores) -> None:
    """Load-validates the actually configured `WRC_TRANSFORM_CONCURRENCY`
    (or its default) against real MongoDB + MinIO: a batch of independent
    groups run through the bounded thread pool must produce exactly the same
    correct end state as sequential processing, with no lost writes, no
    crashes, and no cross-record interference from real concurrent client use.
    """
    source_mongo, source_minio, dest_mongo, dest_minio = stores
    configured_concurrency = TransformSettings.from_env().concurrency
    record_count = 80

    identifiers = [f"ADJ-LOAD-{i:04d}" for i in range(record_count)]
    for i, identifier in enumerate(identifiers):
        # Alternate html/pdf so the batch exercises both the BeautifulSoup
        # clean path and the binary passthrough path under concurrency.
        is_html = i % 3 != 0
        document_type = "html_inline" if is_html else "pdf"
        file_path = f"wrc/en/cases/2024/january/load-{i}.{'html' if is_html else 'pdf'}"
        data = (
            (
                b'<div class="content"><h1>Decision</h1><p>'
                + f"Load test content for record {i}. ".encode() * 5
                + b"</p></div>"
            )
            if is_html
            else f"%PDF fake load test bytes {i}".encode()
        )
        _seed_landing(
            source_mongo,
            source_minio,
            doc_id=f"wrc:en/cases/2024/january/load-{i}.html",
            file_path=file_path,
            data=data,
            document_type=document_type,
            identifier=identifier,
            detail_url=f"https://www.workplacerelations.ie/en/cases/2024/january/load-{i}.html",
        )

    landing_before = _snapshot_landing(source_mongo, source_minio)

    service = TransformService(
        source_mongo, source_minio, dest_mongo, dest_minio, max_workers=configured_concurrency
    )
    started = time.monotonic()
    summary = service.transform_range("2024-01-01", "2024-01-31")
    elapsed = time.monotonic() - started

    assert summary.found == record_count
    assert summary.transformed == record_count
    assert summary.failed == 0
    assert summary.dropped == 0

    for identifier in identifiers:
        dest_doc = dest_mongo.get(transformed_mongo_document_id("wrc", identifier))
        assert dest_doc is not None
        assert dest_doc["status"] == "stored"
        assert dest_minio.object_exists(dest_doc["file_path"])

    # The Landing Zone must remain untouched by a concurrent run, same as a
    # sequential one -- concurrency must never introduce a stray write.
    assert _snapshot_landing(source_mongo, source_minio) == landing_before

    # Rerun: idempotency must hold under concurrency too -- everything skips,
    # nothing is re-uploaded, no record flips to failed.
    rerun_started = time.monotonic()
    rerun_summary = service.transform_range("2024-01-01", "2024-01-31")
    rerun_elapsed = time.monotonic() - rerun_started

    assert rerun_summary.skipped == record_count
    assert rerun_summary.transformed == 0
    assert rerun_summary.failed == 0

    print(
        f"\n[load] {record_count} records, max_workers={configured_concurrency}: "
        f"first run {elapsed:.2f}s, unchanged rerun {rerun_elapsed:.2f}s"
    )


def test_concurrent_and_sequential_runs_produce_identical_results(stores) -> None:
    """The bounded thread pool must be a pure performance optimization --
    running the same batch through TransformService with max_workers=1 vs a
    real thread pool must produce byte-identical transformed output and
    metadata, proving no race condition changes the outcome.
    """
    source_mongo, source_minio, dest_mongo_sequential, dest_minio_sequential = stores

    # A second destination pair in the same throwaway database/MinIO client,
    # under different names, so the two runs can't interfere with each other.
    mongo_client_obj = source_mongo._collection.database.client  # noqa: SLF001 -- test-only introspection
    dest_mongo_concurrent = MongoRepository(
        mongo_client_obj, source_mongo._collection.database.name, "transformed_metadata_concurrent"
    )
    minio_client_obj = source_minio._client  # noqa: SLF001 -- test-only introspection
    concurrent_bucket = f"wrc-transform-test-dest-concurrent-{uuid.uuid4().hex[:8]}"
    dest_minio_concurrent = MinioRepository(minio_client_obj, concurrent_bucket)
    dest_minio_concurrent.ensure_bucket()

    identifiers = [f"ADJ-CMP-{i:04d}" for i in range(20)]
    for i, identifier in enumerate(identifiers):
        file_path = f"wrc/en/cases/2024/january/cmp-{i}.html"
        _seed_landing(
            source_mongo,
            source_minio,
            doc_id=f"wrc:en/cases/2024/january/cmp-{i}.html",
            file_path=file_path,
            data=b'<div class="content"><p>'
            + f"Comparison content {i}. ".encode() * 5
            + b"</p></div>",
            identifier=identifier,
            detail_url=f"https://www.workplacerelations.ie/en/cases/2024/january/cmp-{i}.html",
        )

    try:
        sequential = TransformService(
            source_mongo, source_minio, dest_mongo_sequential, dest_minio_sequential, max_workers=1
        )
        sequential.transform_range("2024-01-01", "2024-01-31")

        concurrent = TransformService(
            source_mongo, source_minio, dest_mongo_concurrent, dest_minio_concurrent, max_workers=8
        )
        concurrent.transform_range("2024-01-01", "2024-01-31")

        for identifier in identifiers:
            doc_id = transformed_mongo_document_id("wrc", identifier)
            seq_doc = dest_mongo_sequential.get(doc_id)
            conc_doc = dest_mongo_concurrent.get(doc_id)
            assert seq_doc["file_hash"] == conc_doc["file_hash"]
            key = transformed_minio_object_key("wrc", identifier, "html_inline")
            assert dest_minio_sequential.get_object(key) == dest_minio_concurrent.get_object(key)
    finally:
        for obj in minio_client_obj.list_objects(concurrent_bucket, recursive=True):
            minio_client_obj.remove_object(concurrent_bucket, obj.object_name)
        minio_client_obj.remove_bucket(concurrent_bucket)
