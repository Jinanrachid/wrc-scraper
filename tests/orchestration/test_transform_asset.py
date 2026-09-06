"""Tests for wrc_scraper.orchestration.assets.processed_documents.

Uses Dagster's direct-invocation pattern (see test_ingestion_asset.py's module
docstring) plus a monkeypatched `_build_transform_repos` that returns
`FakeMongoRepository`/`FakeMinioRepository` (tests/storage/fakes.py, the same
fakes TransformService's own unit tests use) instead of real Mongo/MinIO
clients.
"""

from __future__ import annotations

import pytest
from dagster import MultiPartitionKey, build_asset_context

from tests.storage.fakes import FakeMinioRepository, FakeMongoRepository
from wrc_scraper.orchestration import assets as assets_mod

_PARTITION_KEY = MultiPartitionKey({"month": "2024-01-01", "body_slug": "wrc"})


def _landing_doc(**overrides: object) -> dict:
    defaults = dict(
        _id="wrc:en/cases/2024/january/adj-00047352.html",
        body_slug="wrc",
        body_name="Workplace Relations Commission",
        identifier="ADJ-00047352",
        description="A decision",
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


def _fake_repos_factory(source_mongo: FakeMongoRepository, source_minio: FakeMinioRepository):
    def _build(storage_settings, transform_settings):
        return [], source_mongo, source_minio, FakeMongoRepository(), FakeMinioRepository()

    return _build


def test_success_transforms_and_returns_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    doc = _landing_doc()
    source_mongo.docs[doc["_id"]] = doc
    source_minio.objects[doc["file_path"]] = _CLEAN_HTML

    monkeypatch.setattr(
        assets_mod, "_build_transform_repos", _fake_repos_factory(source_mongo, source_minio)
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.processed_documents(context)

    assert result.metadata["found"].value == 1
    assert result.metadata["transformed"].value == 1
    assert result.metadata["failed"].value == 0
    (check_result,) = result.check_results
    assert check_result.passed is True


def test_run_level_failure_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BrokenMongo(FakeMongoRepository):
        def find_stored(self, start_date: str, end_date: str, *, body_slug: str | None = None):
            raise ConnectionError("mongo unreachable")

    monkeypatch.setattr(
        assets_mod,
        "_build_transform_repos",
        _fake_repos_factory(_BrokenMongo(), FakeMinioRepository()),
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)
    with pytest.raises(RuntimeError, match="transform run failed"):
        assets_mod.processed_documents(context)


def test_partial_failure_succeeds_with_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    """A landing record with an unsupported document_type fails just that one
    group -- the run as a whole still succeeds, with the failure counted in
    metadata (mirrors TransformService's own per-group isolation).
    """
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    doc = _landing_doc(document_type="unknown_future_format")
    source_mongo.docs[doc["_id"]] = doc

    monkeypatch.setattr(
        assets_mod, "_build_transform_repos", _fake_repos_factory(source_mongo, source_minio)
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.processed_documents(context)

    assert result.metadata["found"].value == 1
    assert result.metadata["failed"].value == 1
    assert result.metadata["transformed"].value == 0


def test_quality_check_warns_when_failed_ratio_exceeds_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WRC_TRANSFORM_FAILED_RATIO_THRESHOLD", "0.05")
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    doc = _landing_doc(document_type="unknown_future_format")
    source_mongo.docs[doc["_id"]] = doc

    monkeypatch.setattr(
        assets_mod, "_build_transform_repos", _fake_repos_factory(source_mongo, source_minio)
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.processed_documents(context)

    (check_result,) = result.check_results
    assert check_result.passed is False


def test_dropped_variants_do_not_count_toward_failed_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A variant cluster where one candidate legitimately loses selection
    (the truncated sibling of a complete/truncated pair) must not move the
    quality `failed_ratio` -- only genuine per-group failures should.
    """
    monkeypatch.setenv("WRC_TRANSFORM_FAILED_RATIO_THRESHOLD", "0.01")
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    complete = _landing_doc(
        _id="wrc:en/cases/2024/january/complete.html",
        identifier="ADJ-00099002",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/complete.html",
        file_path="wrc/en/cases/2024/january/complete.html",
        file_hash="hash-complete",
    )
    truncated = _landing_doc(
        _id="wrc:en/cases/2024/january/truncated.html",
        identifier="ADJ-00099002",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/truncated.html",
        file_path="wrc/en/cases/2024/january/truncated.html",
        file_hash="hash-truncated",
    )
    source_mongo.docs[complete["_id"]] = complete
    source_mongo.docs[truncated["_id"]] = truncated
    source_minio.objects[complete["file_path"]] = (
        b'<div class="content"><p>' + b"Full decision text. " * 40 + b"</p></div>"
    )
    source_minio.objects[truncated["file_path"]] = (
        b'<div class="content"><p>Full decision text cuts off</p></div>'
    )

    monkeypatch.setattr(
        assets_mod, "_build_transform_repos", _fake_repos_factory(source_mongo, source_minio)
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)
    result = assets_mod.processed_documents(context)

    assert result.metadata["found"].value == 1
    assert result.metadata["failed"].value == 0
    assert result.metadata["dropped"].value == 1  # visible, but not a failure
    (check_result,) = result.check_results
    assert check_result.metadata["failed_ratio"].value == 0.0
    assert check_result.passed is True


def test_startup_failure_during_client_construction_raises_with_partition_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Mongo/MinIO client-construction failure happens before
    `TransformService` even exists -- it must still be wrapped with the same
    body/partition/date context as a run-level failure, not propagate raw.
    """

    def _broken_build(storage_settings, transform_settings):
        raise ConnectionError("mongo unreachable at construction")

    monkeypatch.setattr(assets_mod, "_build_transform_repos", _broken_build)

    context = build_asset_context(partition_key=_PARTITION_KEY)
    with pytest.raises(RuntimeError, match="transform run failed") as exc_info:
        assets_mod.processed_documents(context)

    message = str(exc_info.value)
    assert "wrc" in message
    assert "2024-01" in message
    assert "mongo unreachable at construction" in message


def test_body_slug_scoping_filters_correctly(monkeypatch: pytest.MonkeyPatch) -> None:
    """The `wrc` partition must never touch a `labour_court` record even
    though both fall in the same date window.
    """
    source_mongo = FakeMongoRepository()
    source_minio = FakeMinioRepository()
    wrc_doc = _landing_doc()
    other_doc = _landing_doc(
        _id="labour_court:en/cases/2024/january/rp1_2024.html",
        body_slug="labour_court",
        body_name="Labour Court",
        identifier="RP1/2024",
        detail_url="https://www.workplacerelations.ie/en/cases/2024/january/rp1_2024.html",
        file_path="labour_court/en/cases/2024/january/rp1_2024.html",
        file_hash="hash-v2",
    )
    source_mongo.docs[wrc_doc["_id"]] = wrc_doc
    source_mongo.docs[other_doc["_id"]] = other_doc
    source_minio.objects[wrc_doc["file_path"]] = _CLEAN_HTML
    source_minio.objects[other_doc["file_path"]] = _CLEAN_HTML

    monkeypatch.setattr(
        assets_mod, "_build_transform_repos", _fake_repos_factory(source_mongo, source_minio)
    )

    context = build_asset_context(partition_key=_PARTITION_KEY)  # body_slug == "wrc"
    result = assets_mod.processed_documents(context)

    assert result.metadata["found"].value == 1  # only the wrc doc, not labour_court
    assert result.metadata["transformed"].value == 1
