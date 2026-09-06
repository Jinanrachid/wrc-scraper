"""Tests for wrc_scraper.middlewares.ConditionalGetMiddleware.

The middleware's only job is deciding whether to attach a conditional GET
header to the chained binary-document request -- the actual "may this be
made conditional" decision stays in ConditionalGetAdvisor (already covered
by tests/storage/test_conditional_get.py) and idempotency stays in
IngestService. These tests exercise the middleware in isolation, constructed
directly (no real Scrapy crawler needed), against a stub advisor.
"""

from __future__ import annotations

import json
import logging

import pytest
from scrapy import Request

from wrc_scraper.middlewares import ConditionalGetMiddleware

DETAIL_URL = "https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009.html"


class StubAdvisor:
    def __init__(self, etag: str | None, *, last_error: str | None = None) -> None:
        self._etag = etag
        self.last_error = last_error
        self.calls: list[tuple[str, str, str]] = []

    def etag_for(self, body_slug: str, detail_url: str, document_type: str) -> str | None:
        self.calls.append((body_slug, detail_url, document_type))
        return self._etag


def _binary_request(document_type: str = "pdf", body: str = "2") -> Request:
    return Request(
        "https://www.workplacerelations.ie/en/eat_import/2010/12/x.pdf",
        meta={"body": body, "detail_url": DETAIL_URL, "document_type": document_type},
    )


def _events(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [json.loads(r.message) for r in caplog.records if r.name == "wrc.events"]


def test_binary_request_with_known_etag_gets_the_conditional_header(
    caplog: pytest.LogCaptureFixture,
) -> None:
    advisor = StubAdvisor("635084654748830000")
    middleware = ConditionalGetMiddleware(
        advisor, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )
    request = _binary_request()

    result = middleware.process_request(request, spider=None)

    assert result is None  # process_request modifies in place, doesn't short-circuit
    assert request.headers[b"If-None-Match"] == b"635084654748830000"
    assert advisor.calls == [("eat", DETAIL_URL, "pdf")]


def test_304_related_meta_is_set_so_a_304_is_treated_as_success() -> None:
    advisor = StubAdvisor("635084654748830000")
    middleware = ConditionalGetMiddleware(
        advisor, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )
    request = _binary_request()

    middleware.process_request(request, spider=None)

    # Without this, Scrapy's HttpError middleware would drop a 304 as an error.
    assert request.meta["handle_httpstatus_list"] == [304]
    # Carried through so parse_document_binary can report/record it.
    assert request.meta["conditional_etag"] == "635084654748830000"


def test_html_request_is_left_unconditional(caplog: pytest.LogCaptureFixture) -> None:
    """The HTML detail request carries no `document_type` in its meta, so it
    must never reach the advisor at all -- html pages have no validators.
    """
    advisor = StubAdvisor("some-etag")
    middleware = ConditionalGetMiddleware(
        advisor, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )
    request = Request(DETAIL_URL, meta={"body": "2"})

    middleware.process_request(request, spider=None)

    assert b"If-None-Match" not in request.headers
    assert "handle_httpstatus_list" not in request.meta
    assert advisor.calls == []  # never consulted for a request with no document_type


def test_no_known_etag_leaves_the_request_unconditional() -> None:
    advisor = StubAdvisor(None)  # nothing verified/stored yet
    middleware = ConditionalGetMiddleware(
        advisor, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )
    request = _binary_request()

    middleware.process_request(request, spider=None)

    assert b"If-None-Match" not in request.headers
    assert "handle_httpstatus_list" not in request.meta


def test_advisor_failure_leaves_the_request_unconditional_and_logs_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    advisor = StubAdvisor(None, last_error="ConnectionError('mongo is down')")
    middleware = ConditionalGetMiddleware(
        advisor, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        middleware.process_request(_binary_request(), spider=None)
        middleware.process_request(_binary_request(), spider=None)  # a second document

    unavailable = [e for e in _events(caplog) if e["event"] == "conditional_get_unavailable"]
    assert len(unavailable) == 1  # reported once, not once per request


def test_middleware_with_no_advisor_never_touches_the_request() -> None:
    """The disabled/broken-at-startup path (advisor is None): every request
    must fall through completely untouched.
    """
    middleware = ConditionalGetMiddleware(
        None, mongo_client=None, events_logger=logging.getLogger("wrc.events")
    )
    request = _binary_request()

    middleware.process_request(request, spider=None)

    assert b"If-None-Match" not in request.headers
    assert "handle_httpstatus_list" not in request.meta


def test_spider_closed_closes_the_mongo_client() -> None:
    class FakeMongoClient:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    client = FakeMongoClient()
    middleware = ConditionalGetMiddleware(
        StubAdvisor(None), mongo_client=client, events_logger=logging.getLogger("wrc.events")
    )

    middleware.spider_closed(spider=None)

    assert client.closed is True


def test_from_crawler_closes_mongo_client_when_a_later_build_step_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """build_mongo can succeed (a live, connected client) even though a later
    step in the same from_crawler try block -- here, build_minio -- then
    fails. That client must not leak: it has to be closed before
    from_crawler falls back to the disabled-middleware state, since nothing
    else holds a reference to it afterwards.
    """
    import wrc_scraper.middlewares as middlewares_module

    class FakeMongoClient:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    fake_client = FakeMongoClient()
    monkeypatch.setattr(middlewares_module, "build_mongo", lambda settings: (fake_client, object()))

    def _raise_minio(settings: object) -> None:
        raise RuntimeError("bad minio config")

    monkeypatch.setattr(middlewares_module, "build_minio", _raise_minio)

    middleware = ConditionalGetMiddleware.from_crawler(crawler=object())

    assert fake_client.closed is True
    assert middleware._advisor is None
