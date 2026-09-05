"""Fixture-based parsing tests for wrc_scraper.spiders.wrc_spider (Step 2.4 +
hardening pass).

No live requests -- fake scrapy.http.HtmlResponse objects built from saved
fixtures (tests/fixtures/), covering all 4 observed templates (WRC, Labour
Court, Equality inline; EAT-import embedded-PDF stub) plus a malformed row, a
missing-refNO row, an empty (last) page, and the defensive multi-document-link
branch.
"""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
from scrapy.http import HtmlResponse, Request

from wrc_scraper.config import DEFAULT_SEARCH_URL
from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.partitioning import DatePartition, PartitionUnit
from wrc_scraper.spiders.wrc_spider import (
    WrcSpider,
    _document_extension,
    _parse_result_total,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize(
    ("href", "expected"),
    [
        ("/en/eat_import/2010/12/x.pdf", "pdf"),
        ("/en/eat_import/2010/12/X.PDF", "pdf"),  # case-insensitive (Decision 1a)
        ("/en/eat_import/2010/12/x.pdf?download=1", "pdf"),  # query string stripped
        ("/en/eat_import/2010/12/x.Docx#frag", "docx"),  # fragment stripped, mixed case
        ("/en/eat_import/2010/12/x.pdf?type=pdfPreview&width=200", "pdf"),
        ("/en/cases/2024/january/adj-00047352.html", None),
        (
            "/en/privacy-policy/cookie_policy.pdf",
            "pdf",
        ),  # extension matches; chrome-filtering is separate
    ],
)
def test_document_extension_is_case_insensitive_and_strips_query_and_fragment(
    href: str, expected: str | None
) -> None:
    assert _document_extension(href) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Shows 1 to 10 of 234 results", 234),
        ("Shows 1 to 10 of 1,234 results", 1234),
        ("...OF 42 RESULTS...", 42),  # case-insensitive
        ("There are no search results fitting your keywords", None),  # empty page banner
        ("", None),
    ],
)
def test_parse_result_total_reads_the_banner_count(text: str, expected: int | None) -> None:
    assert _parse_result_total(text) == expected


def make_response(fixture_name: str, url: str, meta: dict | None = None) -> HtmlResponse:
    body = (FIXTURES_DIR / fixture_name).read_bytes()
    request = Request(url, meta=meta or {})
    return HtmlResponse(url=url, body=body, encoding="utf-8", request=request)


@pytest.fixture
def spider() -> WrcSpider:
    return WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")


@pytest.fixture
def partition() -> DatePartition:
    return DatePartition(
        start=date(2024, 1, 1), end=date(2024, 1, 31), partition_date=date(2024, 1, 1)
    )


def _seed_partition_state(
    spider: WrcSpider, body: str, partition: DatePartition
) -> tuple[str, str]:
    """Legitimate test scaffolding for unit-testing individual callbacks in
    isolation -- the alternative (driving a full crawl per test) is
    disproportionate here. Behavioral assertions (log events, yielded items)
    are used alongside this, not replaced by it, where that adds confidence.
    """
    key = (body, partition.partition_date.isoformat())
    spider._partition_state[key] = {
        "records_found": 0,
        "records_scraped": 0,
        "records_failed": 0,
        "records_expected": None,
        "pagination_done": False,
        "pending": {},
        "incomplete": False,
        "reason": None,
    }
    return key


def _events_from(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [json.loads(record.message) for record in caplog.records if record.name == "wrc.events"]


# -- env-configurable spider settings (hardening item 9) -----------------------


def test_partition_granularity_reads_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_PARTITION_UNIT", "days")
    monkeypatch.setenv("WRC_PARTITION_COUNT", "14")
    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    assert spider.granularity.unit == PartitionUnit.DAYS
    assert spider.granularity.count == 14


def test_partition_granularity_defaults_to_monthly_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WRC_PARTITION_UNIT", raising=False)
    monkeypatch.delenv("WRC_PARTITION_COUNT", raising=False)
    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    assert spider.granularity.unit == PartitionUnit.MONTHS
    assert spider.granularity.count == 1


def test_search_url_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_SEARCH_URL", "https://example.test/en/search/")
    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    assert spider.search_url == "https://example.test/en/search/"


def test_search_url_defaults_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WRC_SEARCH_URL", raising=False)
    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    assert spider.search_url == DEFAULT_SEARCH_URL


# -- listing parsing ----------------------------------------------------------


def test_parse_listing_wrc_extracts_expected_fields_and_follows_detail(
    spider: WrcSpider, partition: DatePartition
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    response = make_response(
        "listing_wrc.html",
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )

    results = list(spider.parse_listing(response))
    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]
    next_page_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_listing
    ]

    assert len(follow_requests) == 2
    assert len(next_page_requests) == 1  # non-empty page -> pagination continues

    first = follow_requests[0].cb_kwargs
    assert first["identifier"] == "ADJ-00047352"
    assert "Jessica Davis" in first["description"]
    assert first["published_date_raw"] == "31/01/2024"

    assert spider._partition_state[key]["records_found"] == 2
    assert len(spider._partition_state[key]["pending"]) == 2
    # The listing banner ("...of 234 results") is captured as the authoritative total.
    assert spider._partition_state[key]["records_expected"] == 234


def test_parse_listing_eat_import_identifier_is_the_business_reference(
    spider: WrcSpider, partition: DatePartition
) -> None:
    _seed_partition_state(spider, "15376", partition)
    response = make_response(
        "listing_eat_import.html",
        "https://www.workplacerelations.ie/en/search/?body=2",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )

    results = list(spider.parse_listing(response))
    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]

    assert len(follow_requests) == 1
    cb_kwargs = follow_requests[0].cb_kwargs
    # For EAT-import, h2.title (the identifier) is the business reference.
    assert cb_kwargs["identifier"] == "RP2147/2009, MN1794/2009, WT796/2009"
    # p.description is absent on this template -- falls back to h2.title text.
    assert cb_kwargs["description"] == "RP2147/2009, MN1794/2009, WT796/2009"


def test_parse_listing_malformed_row_is_logged_and_skipped_not_followed(
    spider: WrcSpider, partition: DatePartition
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    response = make_response(
        "listing_malformed_row.html",
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )

    results = list(spider.parse_listing(response))
    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]

    # 2 rows found, but only the well-formed one gets a follow request.
    assert spider._partition_state[key]["records_found"] == 2
    assert spider._partition_state[key]["records_failed"] == 1
    assert len(follow_requests) == 1
    assert follow_requests[0].cb_kwargs["identifier"] == "ADJ-00047352"


def test_parse_listing_missing_refno_is_tolerated_since_identifier_is_h2_title(
    spider: WrcSpider, partition: DatePartition
) -> None:
    """The record's identity/identifier come from h2.title, so a row missing
    the separate "Ref no:" span is still a perfectly usable record.
    """
    key = _seed_partition_state(spider, "15376", partition)
    response = make_response(
        "listing_missing_refno.html",
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )

    results = list(spider.parse_listing(response))
    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]

    assert spider._partition_state[key]["records_found"] == 2
    assert spider._partition_state[key]["records_failed"] == 0
    assert len(follow_requests) == 2
    assert follow_requests[0].cb_kwargs["identifier"] == "ADJ-00012345"


def test_parse_listing_missing_identifier_is_logged_and_skipped_not_followed(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """The identifier (h2.title) is required -- a row without it must be an
    explicit, logged failure rather than a record with a synthesized id.
    """
    key = _seed_partition_state(spider, "15376", partition)
    html = b"""<html><body><ul>
      <li class="each-item"><div class="row"><div class="col-sm-9">
        <h2 class="title"><a href="/en/cases/2024/january/x.html"></a></h2>
      </div><div class="col-sm-3"><span class="date">01/01/2024</span></div></div>
      <div class="row bottom-ref"><span class="refNO">ADJ-00099999</span></div></li>
    </ul></body></html>"""
    request = Request(
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    response = HtmlResponse(url=request.url, body=html, encoding="utf-8", request=request)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        results = list(spider.parse_listing(response))

    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]

    assert spider._partition_state[key]["records_found"] == 1
    assert spider._partition_state[key]["records_failed"] == 1
    assert follow_requests == []

    failed = [e for e in _events_from(caplog) if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert "identifier" in failed[0]["reason"]


def test_parse_listing_empty_page_completes_the_partition(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    response = make_response(
        "listing_empty.html",
        "https://www.workplacerelations.ie/en/search/?body=15376&pageNumber=99",
        meta={"body": "15376", "partition": partition, "page_number": 99},
    )

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        results = list(spider.parse_listing(response))

    assert results == []  # no rows, no next-page request
    # pagination_done + pending==0 (nothing was ever pending) -> partition completed and removed.
    assert key not in spider._partition_state
    assert spider._totals["partitions_completed"] == 1

    events = _events_from(caplog)
    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["incomplete"] is False
    assert completed[0]["records_found"] == 0


# -- detail parsing / document-type detection ----------------------------------


def test_parse_detail_wrc_inline_excludes_chrome_links_and_retains_raw_html(
    spider: WrcSpider, partition: DatePartition
) -> None:
    _seed_partition_state(spider, "15376", partition)
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html"
    fixture_text = (FIXTURES_DIR / "detail_wrc_inline.html").read_text()
    response = make_response("detail_wrc_inline.html", url)

    items = list(
        spider.parse_detail(
            response,
            body="15376",
            partition=partition,
            identifier="ADJ-00047352",
            published_date_raw="31/01/2024",
            description="Jessica Davis V St. Vincent's Private Hospital",
        )
    )

    assert len(items) == 1
    record = items[0]
    assert isinstance(record, WrcDecisionRecord)
    assert record.identifier == "ADJ-00047352"
    assert record.body_slug == "wrc"
    assert record.body_name == "Workplace Relations Commission"
    assert record.document_type == "html_inline"
    assert record.document_url == url
    assert record.published_date == "2024-01-31"
    # Hardening item 3: raw HTML retained, unmodified, no re-fetch needed later.
    assert record.raw_html == fixture_text
    assert record.raw_binary is None


def test_parse_detail_eat_import_chains_binary_request_and_finds_real_pdf_link(
    spider: WrcSpider, partition: DatePartition
) -> None:
    _seed_partition_state(spider, "2", partition)
    url = "https://www.workplacerelations.ie/en/cases/2010/december/rp2147_2009_mn1794_2009_wt796_2009.html"
    response = make_response("detail_eat_import_stub.html", url)

    results = list(
        spider.parse_detail(
            response,
            body="2",
            partition=partition,
            identifier="38086",
            published_date_raw="31/12/2010",
            description="RP2147/2009, MN1794/2009, WT796/2009",
        )
    )

    # pdf/doc/docx: parse_detail no longer yields the item directly -- it
    # chains one more request to actually fetch the binary (Phase 3).
    assert len(results) == 1
    binary_request = results[0]
    assert isinstance(binary_request, Request)
    assert binary_request.callback == spider.parse_document_binary
    assert binary_request.url.endswith(
        "/en/eat_import/2010/12/75d3358e-f145-40d5-9922-da2822791892.pdf"
    )
    assert "pdfPreview" not in binary_request.url  # not the <img> preview link

    binary_response = HtmlResponse(
        url=binary_request.url, body=b"%PDF-1.4 fake pdf bytes", request=binary_request
    )
    items = list(spider.parse_document_binary(binary_response, **binary_request.cb_kwargs))
    assert len(items) == 1
    record = items[0]
    assert record.document_type == "pdf"
    assert record.raw_binary == b"%PDF-1.4 fake pdf bytes"
    assert record.raw_html is None
    assert record.identifier == "38086"
    assert record.body_slug == "eat"
    assert record.body_name == "Employment Appeals Tribunal"


def test_parse_detail_labour_court_inline_no_heading_tag(
    spider: WrcSpider, partition: DatePartition
) -> None:
    _seed_partition_state(spider, "3", partition)
    url = "https://www.workplacerelations.ie/en/cases/2024/february/lcr22912.html"
    response = make_response("detail_labour_court_inline.html", url)

    items = list(
        spider.parse_detail(
            response,
            body="3",
            partition=partition,
            identifier="LCR22912",
            published_date_raw="30/01/2024",
            description="Sonoma Valley and a worker",
        )
    )

    assert items[0].document_type == "html_inline"


def test_parse_detail_equality_inline_h2_headings() -> None:
    spider = WrcSpider(start_date="2012-01-01", end_date="2012-01-31", bodies="1")
    partition = DatePartition(date(2012, 1, 1), date(2012, 1, 31), date(2012, 1, 1))
    _seed_partition_state(spider, "1", partition)
    url = "https://www.workplacerelations.ie/en/cases/2012/january/dec-e2012-009-full-case-report.html"
    response = make_response("detail_equality_inline.html", url)

    items = list(
        spider.parse_detail(
            response,
            body="1",
            partition=partition,
            identifier="DEC-E2012-009",
            published_date_raw="15/01/2012",
            description="248 Named Complainants",
        )
    )

    assert items[0].document_type == "html_inline"


def test_parse_detail_multiple_document_links_picks_dom_order_first_and_logs(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_partition_state(spider, "15376", partition)
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00099999.html"
    response = make_response("detail_multi_doc.html", url)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        results = list(
            spider.parse_detail(
                response,
                body="15376",
                partition=partition,
                identifier="ADJ-00099999",
                published_date_raw="01/01/2024",
                description="Synthetic multi-doc record",
            )
        )

    assert len(results) == 1
    binary_request = results[0]
    assert binary_request.url.endswith("first-document.pdf")  # DOM order

    events = _events_from(caplog)
    anomalies = [e for e in events if e["event"] == "multiple_document_links"]
    assert len(anomalies) == 1
    assert anomalies[0]["candidates"][0].endswith("first-document.pdf")


# -- request-level vs. record-level failure accounting -------------------------


def test_detail_failed_errback_increments_records_failed_not_scraped(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    detail_url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00000001.html"
    spider._partition_state[key]["pending"] = {detail_url: "ADJ-00000001"}
    spider._partition_state[key]["pagination_done"] = True

    request = Request(
        detail_url,
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    failure = SimpleNamespace(request=request, value=RuntimeError("boom"))

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        spider._detail_failed(failure)

    assert (
        key not in spider._partition_state
    )  # partition completed after the only pending item resolved
    assert spider._totals["records_failed"] == 1
    assert spider._totals["records_scraped"] == 0
    assert spider._totals["partitions_completed"] == 1

    events = _events_from(caplog)
    failed = [e for e in events if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert failed[0]["reason"] == "RuntimeError('boom')"
    assert failed[0]["url"] == request.url
    assert failed[0]["http_status"] is None


def test_records_found_equals_scraped_plus_failed_for_a_completed_partition(
    spider: WrcSpider, partition: DatePartition
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    listing_response = make_response(
        "listing_malformed_row.html",
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    list(spider.parse_listing(listing_response))  # 2 found: 1 failed immediately, 1 pending

    detail_url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html"
    detail_response = make_response("detail_wrc_inline.html", detail_url)
    list(
        spider.parse_detail(
            detail_response,
            body="15376",
            partition=partition,
            identifier="ADJ-00047352",
            published_date_raw="31/01/2024",
            description="A normal row after the malformed one",
        )
    )

    # Empty next page to mark pagination done and trigger completion.
    empty_response = make_response(
        "listing_empty.html",
        "https://www.workplacerelations.ie/en/search/?body=15376&pageNumber=2",
        meta={"body": "15376", "partition": partition, "page_number": 2},
    )
    list(spider.parse_listing(empty_response))

    assert key not in spider._partition_state
    assert spider._totals["records_found"] == 2
    assert spider._totals["records_scraped"] == 1
    assert spider._totals["records_failed"] == 1
    assert spider._totals["records_found"] == (
        spider._totals["records_scraped"] + spider._totals["records_failed"]
    )


# -- dangling partition reconciliation (hardening item 7) ----------------------


def test_closed_reconciles_dangling_partition_state_and_logs_each_pending_record(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """If the spider is shut down (item cap, kill, crash, unexpected exception)
    before every in-flight request resolves, the partition must not silently
    vanish -- and per assessment req #10/tip, EVERY un-scraped record must be
    individually logged with a reason, not just an aggregate partition note.
    """
    key = _seed_partition_state(spider, "15376", partition)
    spider._partition_state[key]["pagination_done"] = True
    # Two unresolved requests -- simulates a hard shutdown mid-flight.
    spider._partition_state[key]["pending"] = {
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html": "ADJ-1",
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-2.html": "ADJ-2",
    }
    spider._partition_state[key]["records_found"] = 2

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        spider.closed("finished")

    assert spider._partition_state == {}
    assert spider._totals["partitions_incomplete"] == 1
    assert spider._totals["records_failed"] == 2  # both dangling records counted

    events = _events_from(caplog)

    # Every dangling record gets its own record_failed -- not folded away.
    failed = [e for e in events if e["event"] == "record_failed"]
    assert len(failed) == 2
    assert {e["identifier"] for e in failed} == {"ADJ-1", "ADJ-2"}
    assert {e["url"] for e in failed} == {
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-1.html",
        "https://www.workplacerelations.ie/en/cases/2024/january/adj-2.html",
    }
    assert all("resolved" in e["reason"] for e in failed)

    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["incomplete"] is True
    assert completed[0]["records_failed"] == 2
    assert "pending" in completed[0]["reason"]

    summary = [e for e in events if e["event"] == "run_summary"]
    assert len(summary) == 1
    assert summary[0]["partitions_incomplete"] == 1
    assert summary[0]["records_failed"] == 2


def test_partition_reports_unaccounted_records_when_a_listing_page_failed(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """When a listing page fails mid-pagination, the rows behind it were never
    fetched, so their identifiers can't be logged individually. Req #10 is still
    served by quantifying the shortfall: the site said N, we scraped S, so
    records_unaccounted = N - S - F is reported with the partition-level reason.
    """
    key = _seed_partition_state(spider, "15376", partition)
    state = spider._partition_state[key]
    state["records_expected"] = 30  # the site's banner said 30
    state["records_found"] = 20  # only pages 1-2 were seen before page 3 failed
    state["records_scraped"] = 20
    state["records_failed"] = 0
    state["pagination_done"] = True  # stopped because a page failed
    state["incomplete"] = True
    state["reason"] = "listing page failed: DownloadTimeoutError(...)"

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        spider._maybe_complete_partition("15376", partition)
        spider.closed("finished")

    events = _events_from(caplog)
    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["records_expected"] == 30
    assert completed[0]["records_unaccounted"] == 10  # 30 - (20 scraped + 0 failed)
    assert completed[0]["incomplete"] is True
    assert "listing page failed" in completed[0]["reason"]

    summary = [e for e in events if e["event"] == "run_summary"]
    assert summary[0]["records_unaccounted"] == 10


def test_fully_scraped_partition_reports_zero_unaccounted(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """A partition that completes normally (all pages fetched) has expected ==
    scraped + failed, so records_unaccounted is 0 -- no false shortfall.
    """
    key = _seed_partition_state(spider, "15376", partition)
    state = spider._partition_state[key]
    state["records_expected"] = 20
    state["records_found"] = 20
    state["records_scraped"] = 18
    state["records_failed"] = 2
    state["pagination_done"] = True

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        spider._maybe_complete_partition("15376", partition)

    completed = [e for e in _events_from(caplog) if e["event"] == "partition_completed"]
    assert completed[0]["records_unaccounted"] == 0
    assert completed[0]["incomplete"] is False


# -- conditional GET for binary documents (assessment requirement #9) ----------


EAT_STUB_URL = (
    "https://www.workplacerelations.ie/en/cases/2010/december/"
    "rp2147_2009_mn1794_2009_wt796_2009.html"
)
EAT_STUB_CB_KWARGS = {
    "identifier": "38086",
    "published_date_raw": "31/12/2010",
    "description": "RP2147/2009, MN1794/2009, WT796/2009",
}


def _binary_request(spider: WrcSpider, partition: DatePartition) -> Request:
    _seed_partition_state(spider, "2", partition)
    response = make_response("detail_eat_import_stub.html", EAT_STUB_URL)
    results = list(
        spider.parse_detail(response, body="2", partition=partition, **EAT_STUB_CB_KWARGS)
    )
    assert len(results) == 1
    return results[0]


def _advisor_returning(etag: str | None) -> object:
    class StubAdvisor:
        last_error = None

        def etag_for(self, body: str, detail_url: str, document_type: str) -> str | None:
            assert document_type == "pdf"  # html is never made conditional
            return etag

    return StubAdvisor()


def test_binary_request_is_unconditional_when_nothing_is_stored_yet(
    spider: WrcSpider, partition: DatePartition
) -> None:
    spider._conditional_get = _advisor_returning(None)

    request = _binary_request(spider, partition)

    assert b"If-None-Match" not in request.headers
    assert "handle_httpstatus_list" not in request.meta


def test_binary_request_sends_the_unquoted_etag_and_accepts_a_304(
    spider: WrcSpider, partition: DatePartition
) -> None:
    spider._conditional_get = _advisor_returning("635084654748830000")

    request = _binary_request(spider, partition)

    # Unquoted -- the only form this site answers 304 to (Sec 20).
    assert request.headers[b"If-None-Match"] == b"635084654748830000"
    # Without this Scrapy's HttpError middleware would drop the 304 as an error.
    assert request.meta["handle_httpstatus_list"] == [304]


def test_304_yields_a_record_with_no_bytes_and_counts_a_skipped_download(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    spider._conditional_get = _advisor_returning("635084654748830000")
    request = _binary_request(spider, partition)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        not_modified = HtmlResponse(url=request.url, status=304, body=b"", request=request)
        items = list(spider.parse_document_binary(not_modified, **request.cb_kwargs))

    assert len(items) == 1
    record = items[0]
    assert record.not_modified is True
    assert record.raw_binary is None  # a 304 carries no body
    assert record.remote_etag == "635084654748830000"
    assert record.document_type == "pdf"
    assert record.identifier == "38086"  # metadata is still refreshed

    assert spider._totals["documents_not_modified"] == 1
    assert spider._totals["records_scraped"] == 1  # a skip is a success, not a failure
    events = [json.loads(r.message) for r in caplog.records]
    assert any(e["event"] == "document_not_modified" for e in events)


def test_a_200_answer_to_a_conditional_request_falls_back_to_the_full_download(
    spider: WrcSpider, partition: DatePartition
) -> None:
    """The quirk this relies on could break at any time. If the server ever
    ignores If-None-Match and answers 200, the record must carry real bytes
    and go through the normal SHA-256 comparison.
    """
    spider._conditional_get = _advisor_returning("635084654748830000")
    request = _binary_request(spider, partition)

    response = HtmlResponse(
        url=request.url,
        status=200,
        body=b"%PDF-1.4 fake pdf bytes",
        headers={"ETag": '"new-etag"'},
        request=request,
    )
    record = list(spider.parse_document_binary(response, **request.cb_kwargs))[0]

    assert record.not_modified is False
    assert record.raw_binary == b"%PDF-1.4 fake pdf bytes"
    assert record.remote_etag == '"new-etag"'
    assert spider._totals["documents_not_modified"] == 0


def test_an_unreachable_advisor_is_reported_once_and_leaves_the_crawl_unconditional(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    class BrokenAdvisor:
        last_error = "ConnectionError('mongo is down')"

        def etag_for(self, body: str, detail_url: str, document_type: str) -> str | None:
            return None

    spider._conditional_get = BrokenAdvisor()

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        request = _binary_request(spider, partition)
        spider._conditional_etag("2", EAT_STUB_URL, "pdf")  # a second document

    assert b"If-None-Match" not in request.headers
    events = [json.loads(r.message) for r in caplog.records]
    unavailable = [e for e in events if e["event"] == "conditional_get_unavailable"]
    assert len(unavailable) == 1  # logged once, not per request
