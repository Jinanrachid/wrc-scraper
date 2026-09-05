"""Tests for wrc_scraper.storage.conditional_get.

The governing property under test is the safety rule: the advisor may only
ever return an ETag when a complete, verified prior version is on hand.
Every other situation -- including a store that raises -- must produce None,
i.e. a plain unconditional GET.
"""

from __future__ import annotations

import pytest

from wrc_scraper.storage.conditional_get import ConditionalGetAdvisor, unquote_etag

from .fakes import FakeMinioRepository, FakeMongoRepository

# The advisor keys everything on the body slug (storage identity), so callers
# pass the slug ("eat"), not the site's numeric id.
BODY_SLUG = "eat"
DETAIL_URL = "https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009.html"
DOC_ID = "eat:en/cases/2010/december/rp2147_2009.html"
OBJECT_KEY = "eat/en/cases/2010/december/rp2147_2009.pdf"


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("635084654748830000", "635084654748830000"),  # the bare form the site actually sends
        ('"635084654748830000"', "635084654748830000"),  # the RFC-9110 form, normalized to bare
        ('W/"635084654748830000"', "635084654748830000"),  # weak validator
        ('  "abc"  ', "abc"),
        ('""', None),
        ("", None),
        (None, None),
    ],
)
def test_unquote_etag(header: str | None, expected: str | None) -> None:
    """The WRC endpoints send a bare, unquoted ETag and match only on that
    exact token -- re-quoting it gets a full 200 (docs/SCRAPY_EXPERIMENTS.md
    Sec 20).
    """
    assert unquote_etag(header) == expected


def make_advisor(
    *, status: str = "stored", file_hash: str | None = "h", etag: str | None = '"etag-1"'
) -> tuple[ConditionalGetAdvisor, FakeMongoRepository, FakeMinioRepository]:
    mongo = FakeMongoRepository()
    minio = FakeMinioRepository()
    mongo.docs[DOC_ID] = {
        "_id": DOC_ID,
        "status": status,
        "file_path": OBJECT_KEY,
        "file_hash": file_hash,
        "remote_etag": etag,
    }
    minio.objects[OBJECT_KEY] = b"%PDF stored bytes"
    return ConditionalGetAdvisor(mongo, minio), mongo, minio


def test_returns_unquoted_etag_when_a_verified_prior_version_exists() -> None:
    advisor, _mongo, _minio = make_advisor()
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") == "etag-1"


def test_never_conditional_for_html_because_the_pages_have_no_validators() -> None:
    advisor, _mongo, _minio = make_advisor()
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "html_inline") is None


def test_no_etag_when_there_is_no_prior_record() -> None:
    advisor, mongo, _minio = make_advisor()
    mongo.docs.clear()
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_no_etag_when_the_prior_record_is_not_stored() -> None:
    advisor, _mongo, _minio = make_advisor(status="failed")
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_no_etag_when_the_prior_record_has_no_hash() -> None:
    advisor, _mongo, _minio = make_advisor(file_hash=None)
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_no_etag_when_none_was_ever_captured() -> None:
    advisor, _mongo, _minio = make_advisor(etag=None)
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_no_etag_when_the_stored_path_is_not_the_key_this_record_would_write() -> None:
    advisor, mongo, _minio = make_advisor()
    mongo.docs[DOC_ID]["file_path"] = "2/somewhere/else.pdf"
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_no_etag_when_the_stored_object_is_missing() -> None:
    """A 304 would claim "you already have it" -- but we don't. Must
    re-download.
    """
    advisor, _mongo, minio = make_advisor()
    minio.objects.clear()
    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None


def test_an_unreachable_store_degrades_to_an_unconditional_get() -> None:
    class ExplodingMongo:
        def get(self, doc_id: str) -> dict | None:
            raise ConnectionError("mongo is down")

    advisor = ConditionalGetAdvisor(ExplodingMongo(), FakeMinioRepository())

    assert advisor.etag_for(BODY_SLUG, DETAIL_URL, "pdf") is None
    assert "mongo is down" in advisor.last_error
