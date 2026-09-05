"""Integration tests against real MongoDB + MinIO (Phase 3, Step 10).

Requires `docker compose up -d` (docker-compose.yml at repo root). Auto-skips
(not a hard failure) if either service isn't reachable, so the default
`pytest` run stays fast and Docker-independent -- per the approved design,
"keep the majority of logic unit-testable," with these as the smaller set
validating the *real* client wiring that the fakes in test_ingest_service.py
can't.
"""

from __future__ import annotations

import os
import uuid

import pytest

from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.storage.ingest_service import IngestService
from wrc_scraper.storage.minio_repository import MinioRepository
from wrc_scraper.storage.mongo_repository import MongoRepository

MONGO_URI = os.environ.get("WRC_MONGO_URI", "mongodb://localhost:27017")
MINIO_ENDPOINT = os.environ.get("WRC_MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("WRC_MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("WRC_MINIO_SECRET_KEY", "minioadmin")


@pytest.fixture
def mongo_repo():
    pymongo = pytest.importorskip("pymongo")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500)
        client.admin.command("ping")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MongoDB not reachable at {MONGO_URI}: {exc}")
    database = f"wrc_test_{uuid.uuid4().hex[:8]}"
    repo = MongoRepository(client, database, "landing_metadata")
    repo.ensure_indexes()
    yield repo
    client.drop_database(database)
    client.close()


@pytest.fixture
def minio_repo():
    minio_module = pytest.importorskip("minio")
    try:
        client = minio_module.Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        client.list_buckets()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MinIO not reachable at {MINIO_ENDPOINT}: {exc}")
    bucket = f"wrc-test-{uuid.uuid4().hex[:8]}"
    repo = MinioRepository(client, bucket)
    repo.ensure_bucket()
    yield repo
    for obj in client.list_objects(bucket, recursive=True):
        client.remove_object(bucket, obj.object_name)
    client.remove_bucket(bucket)


def test_mongo_repository_round_trip(mongo_repo) -> None:
    doc_id = "15376:ADJ-INTEGRATION-TEST"
    mongo_repo.upsert_pending(
        doc_id,
        now="2026-01-01T00:00:00+00:00",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        identifier="ADJ-INTEGRATION-TEST",
        description="d",
        published_date="2024-01-01",
        partition_date="2024-01-01",
        detail_url="https://example.test/x.html",
        document_type="html_inline",
        document_url="https://example.test/x.html",
    )
    assert mongo_repo.get(doc_id)["status"] == "pending"

    mongo_repo.mark_stored(
        doc_id,
        file_path="15376/ADJ-INTEGRATION-TEST.html",
        file_hash="abc123",
        file_size_bytes=42,
        remote_etag=None,
        now="2026-01-01T00:01:00+00:00",
        content_changed=True,
    )
    doc = mongo_repo.get(doc_id)
    assert doc["status"] == "stored"
    assert doc["file_hash"] == "abc123"


def test_minio_repository_round_trip(minio_repo) -> None:
    key = "15376/ADJ-INTEGRATION-TEST.html"
    assert minio_repo.object_exists(key) is False

    minio_repo.put_object(key, b"<html>hello</html>", "text/html")

    assert minio_repo.object_exists(key) is True


def test_ingest_service_full_round_trip_against_real_services(mongo_repo, minio_repo) -> None:
    service = IngestService(mongo_repo, minio_repo)
    record = WrcDecisionRecord(
        identifier="ADJ-INTEGRATION-TEST",
        description="Integration test record",
        published_date="2024-01-01",
        detail_url="https://example.test/adj-integration-test.html",
        document_type="html_inline",
        document_url="https://example.test/adj-integration-test.html",
        partition_date="2024-01-01",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        scraped_at="2026-01-01T00:00:00+00:00",
        raw_html="<html><body>integration test content</body></html>",
    )

    first = service.ingest(record)
    assert first.status == "stored"
    assert minio_repo.object_exists("wrc/adj-integration-test.html")

    second = service.ingest(record)  # unchanged rerun
    assert second.status == "stored"
    assert second.content_changed is False
