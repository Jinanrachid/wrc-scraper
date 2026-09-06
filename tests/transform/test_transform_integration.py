"""Docker-backed integration tests for the transformation stage (Phase 4).

Exercises `TransformService` against real MongoDB + MinIO (not the in-memory
fakes used elsewhere in tests/transform/) to validate what fakes structurally
cannot: real `pymongo.MongoClient` / minio-py client wiring end to end. The
transformation business logic itself (variant-cluster selection, ordering
independence, concurrency correctness) is storage-agnostic pure Python and is
already covered against fakes in test_transform_service.py -- it does not
need to be re-proven against real services here.

Same convention as tests/storage/test_integration.py: auto-skips (not a hard
failure) if MongoDB/MinIO aren't reachable, so the default `pytest` run stays
fast and Docker-independent. Everything is written under throwaway
database/bucket names and torn down afterward -- nothing here ever touches
`wrc`/`wrc-landing`, the real Landing Zone.
"""

from __future__ import annotations

import os
import uuid

import pytest

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
