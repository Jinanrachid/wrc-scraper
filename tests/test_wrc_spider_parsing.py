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
    spider._tracker.partition_state[key] = {
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

    assert spider._tracker.partition_state[key]["records_found"] == 2
    assert len(spider._tracker.partition_state[key]["pending"]) == 2
    # The listing banner ("...of 234 results") is captured as the authoritative total.
    assert spider._tracker.partition_state[key]["records_expected"] == 234


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
    assert spider._tracker.partition_state[key]["records_found"] == 2
    assert spider._tracker.partition_state[key]["records_failed"] == 1
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

    assert spider._tracker.partition_state[key]["records_found"] == 2
    assert spider._tracker.partition_state[key]["records_failed"] == 0
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

    assert spider._tracker.partition_state[key]["records_found"] == 1
    assert spider._tracker.partition_state[key]["records_failed"] == 1
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
    assert key not in spider._tracker.partition_state
    assert spider._tracker.totals["partitions_completed"] == 1

    events = _events_from(caplog)
    completed = [e for e in events if e["event"] == "partition_completed"]
    assert len(completed) == 1
    assert completed[0]["incomplete"] is False
    assert completed[0]["records_found"] == 0


# -- pagination safeguards (docs/SCRAPY_EXPERIMENTS.md Sec 23) ----------------


def _listing_page_html(identifiers: list[str], total: int | None) -> bytes:
    """A minimal synthetic listing page: one row per identifier, and the
    "Shows ... of N results" banner when ``total`` is given (omitted entirely
    for the real "no search results" case, which uses listing_empty.html).
    """
    banner = f"<p>Shows 1 to {len(identifiers)} of {total} results</p>" if total is not None else ""
    rows = "".join(
        f"""<li class="each-item"><div class="row"><div class="col-sm-9">
        <h2 class="title" title="{ident}"><a href="/en/cases/2024/january/{ident.lower()}.html"
        title="{ident}">{ident}</a></h2>
        </div><div class="col-sm-3"><span class="date">01/01/2024</span></div></div>
        <div class="row bottom-ref"><span class="refNO">{ident}</span></div></li>"""
        for ident in identifiers
    )
    html = f'<html><body><div class="search-results">{banner}<ul>{rows}</ul></div></body></html>'
    return html.encode()


def _listing_response(html: bytes, page_number: int, partition: DatePartition, body: str = "15376"):
    url = f"https://www.workplacerelations.ie/en/search/?body={body}&pageNumber={page_number}"
    request = Request(url, meta={"body": body, "partition": partition, "page_number": page_number})
    return HtmlResponse(url=url, body=html, encoding="utf-8", request=request)


def test_multipage_pagination_matching_expected_count_stays_complete(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    key = _seed_partition_state(spider, "15376", partition)
    page1 = _listing_response(
        _listing_page_html(["ADJ-1", "ADJ-2"], total=2), page_number=1, partition=partition
    )
    page2 = _listing_response(
        (FIXTURES_DIR / "listing_empty.html").read_bytes(), page_number=2, partition=partition
    )

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        next_page_requests = [
            r
            for r in spider.parse_listing(page1)
            if isinstance(r, Request) and r.callback == spider.parse_listing
        ]
        assert len(next_page_requests) == 1  # non-empty page -> pagination continues
        list(spider.parse_listing(page2))

    state = spider._tracker.partition_state[key]
    assert state["records_expected"] == 2
    assert state["records_found"] == 2
    assert state["pagination_done"] is True
    assert state["incomplete"] is False

    mismatches = [e for e in _events_from(caplog) if e["event"] == "partition_count_mismatch"]
    assert mismatches == []


def test_records_expected_mismatch_marks_partition_incomplete(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """docs/SCRAPY_EXPERIMENTS.md Sec 23 verified records_expected is a stable,
    reliable total, so a mismatch against what was actually found means real
    rows are unaccounted for and the partition cannot be trusted as complete.
    """
    key = _seed_partition_state(spider, "15376", partition)
    # Banner claims 5, but only 3 rows are ever actually returned.
    page1 = _listing_response(
        _listing_page_html(["ADJ-1", "ADJ-2", "ADJ-3"], total=5), page_number=1, partition=partition
    )
    page2 = _listing_response(
        (FIXTURES_DIR / "listing_empty.html").read_bytes(), page_number=2, partition=partition
    )

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        list(spider.parse_listing(page1))
        list(spider.parse_listing(page2))

    state = spider._tracker.partition_state[key]
    assert state["records_expected"] == 5
    assert state["records_found"] == 3
    assert state["incomplete"] is True
    assert "records_found (3)" in state["reason"]
    assert "records_expected (5)" in state["reason"]

    mismatches = [e for e in _events_from(caplog) if e["event"] == "partition_count_mismatch"]
    assert len(mismatches) == 1
    assert mismatches[0]["records_expected"] == 5
    assert mismatches[0]["records_found"] == 3


def test_max_pages_safety_limit_stops_pagination_and_marks_incomplete(
    partition: DatePartition, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("WRC_MAX_PAGES", "2")
    spider = WrcSpider(start_date="2024-01-01", end_date="2024-01-31", bodies="15376")
    key = _seed_partition_state(spider, "15376", partition)

    page1 = _listing_response(
        _listing_page_html(["ADJ-1"], total=None), page_number=1, partition=partition
    )
    page2 = _listing_response(
        _listing_page_html(["ADJ-2"], total=None), page_number=2, partition=partition
    )

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        page1_results = list(spider.parse_listing(page1))
        page2_results = list(spider.parse_listing(page2))

    # Page 1 -> page 2 is still within the limit (WRC_MAX_PAGES=2).
    assert any(isinstance(r, Request) and r.callback == spider.parse_listing for r in page1_results)
    # Page 2 would continue to page 3, exceeding the limit -> pagination stops,
    # only the row's own detail request is yielded, no further listing request.
    assert not any(
        isinstance(r, Request) and r.callback == spider.parse_listing for r in page2_results
    )

    state = spider._tracker.partition_state[key]
    assert state["pagination_done"] is True
    assert state["incomplete"] is True
    assert "max page limit reached" in state["reason"]

    events = [e for e in _events_from(caplog) if e["event"] == "partition_max_pages_reached"]
    assert len(events) == 1
    assert events[0]["max_pages"] == 2
    assert events[0]["records_found"] == 2


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


def test_parse_detail_empty_html_inline_body_is_recorded_as_a_failure(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """Mirrors test_a_200_answer_with_an_empty_body_is_recorded_as_a_failure
    (the binary-download path): a truncated/zero-byte 200 for a detail page
    must not be counted as a successfully scraped record either.
    """
    _seed_partition_state(spider, "15376", partition)
    url = "https://www.workplacerelations.ie/en/cases/2024/january/adj-00047352.html"
    request = Request(url)
    response = HtmlResponse(url=url, status=200, body=b"", request=request)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
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

    assert items == []  # no record yielded for an empty body
    assert spider._tracker.totals["records_failed"] == 1
    assert spider._tracker.totals["records_scraped"] == 0

    failed = [e for e in _events_from(caplog) if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert "empty" in failed[0]["reason"]
    assert failed[0]["http_status"] == 200


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
    spider._tracker.partition_state[key]["pending"] = {detail_url: "ADJ-00000001"}
    spider._tracker.partition_state[key]["pagination_done"] = True

    request = Request(
        detail_url,
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    failure = SimpleNamespace(request=request, value=RuntimeError("boom"))

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        spider._detail_failed(failure)

    assert (
        key not in spider._tracker.partition_state
    )  # partition completed after the only pending item resolved
    assert spider._tracker.totals["records_failed"] == 1
    assert spider._tracker.totals["records_scraped"] == 0
    assert spider._tracker.totals["partitions_completed"] == 1

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

    assert key not in spider._tracker.partition_state
    assert spider._tracker.totals["records_found"] == 2
    assert spider._tracker.totals["records_scraped"] == 1
    assert spider._tracker.totals["records_failed"] == 1
    assert spider._tracker.totals["records_found"] == (
        spider._tracker.totals["records_scraped"] + spider._tracker.totals["records_failed"]
    )


# -- dangling partition reconciliation (hardening item 7) ----------------------
#
# The detailed accounting/logging behavior of reconciliation itself (every
# pending record individually logged, records_unaccounted arithmetic, the
# partition_completed/run_summary event content) is unit-tested directly and
# thoroughly against PartitionTracker in test_partition_tracker.py. What's
# worth proving here, at the spider level, is only the wiring: that
# WrcSpider.closed() actually delegates to the tracker rather than, say,
# skipping reconciliation on some shutdown path.


def test_closed_delegates_to_the_tracker_for_reconciliation_and_run_summary(
    spider: WrcSpider, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[str, tuple[object, ...]]] = []
    monkeypatch.setattr(
        spider._tracker, "reconcile_dangling", lambda: calls.append(("reconcile_dangling", ()))
    )
    monkeypatch.setattr(
        spider._tracker,
        "log_run_summary",
        lambda reason: calls.append(("log_run_summary", (reason,))),
    )

    spider.closed("finished")

    assert calls == [("reconcile_dangling", ()), ("log_run_summary", ("finished",))]


# -- chained binary request / 304 handling (assessment requirement #9) --------
#
# Header injection itself (If-None-Match, when it is/isn't sent, the
# once-per-run "unavailable" log) is ConditionalGetMiddleware's job now --
# see tests/test_middlewares.py. What's tested here is what stays in the
# spider: the chained request exposes exactly the meta the middleware needs,
# and parse_document_binary's own interpretation of a 304 vs. a 200 response
# (which only ever reads `request.meta.get("conditional_etag")`, wherever
# that came from).


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


def test_binary_request_carries_the_meta_the_conditional_get_middleware_needs(
    spider: WrcSpider, partition: DatePartition
) -> None:
    request = _binary_request(spider, partition)

    assert request.meta["body"] == "2"
    assert request.meta["detail_url"] == EAT_STUB_URL
    assert request.meta["document_type"] == "pdf"
    # The spider itself no longer decides this -- no header/meta is set here.
    assert b"If-None-Match" not in request.headers
    assert "handle_httpstatus_list" not in request.meta


def test_304_yields_a_record_with_no_bytes_and_counts_a_skipped_download(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    request = _binary_request(spider, partition)
    # What ConditionalGetMiddleware would have set on the outgoing request
    # before the site answered 304.
    request.meta["conditional_etag"] = "635084654748830000"

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

    assert spider._tracker.totals["documents_not_modified"] == 1
    assert spider._tracker.totals["records_scraped"] == 1  # a skip is a success, not a failure
    events = [json.loads(r.message) for r in caplog.records]
    assert any(e["event"] == "document_not_modified" for e in events)


def test_a_200_answer_to_a_conditional_request_falls_back_to_the_full_download(
    spider: WrcSpider, partition: DatePartition
) -> None:
    """The quirk this relies on could break at any time. If the server ever
    ignores If-None-Match and answers 200, the record must carry real bytes
    and go through the normal SHA-256 comparison.
    """
    request = _binary_request(spider, partition)
    request.meta["conditional_etag"] = "635084654748830000"

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
    assert spider._tracker.totals["documents_not_modified"] == 0


# -- per-row exception isolation in parse_listing (robustness pass, Fix 1) -----


def test_parse_listing_unexpected_row_error_is_isolated_and_page_continues(
    spider: WrcSpider,
    partition: DatePartition,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A novel, unexpected surprise while building ONE row's detail request
    must not abort the rest of the page. Rows 1 and 3 still get detail
    requests, the middle row is logged as record_failed with the exception
    repr as its reason, and pagination still continues to the next page.
    """
    key = _seed_partition_state(spider, "15376", partition)
    html = b"""<html><body><ul>
      <li class="each-item"><div class="row"><div class="col-sm-9">
        <h2 class="title" title="ADJ-1"><a href="/en/cases/2024/january/adj-1.html">ADJ-1</a></h2>
        <p class="description" title="Row one">Row one</p>
      </div><div class="col-sm-3"><span class="date">01/01/2024</span></div></div></li>
      <li class="each-item"><div class="row"><div class="col-sm-9">
        <h2 class="title" title="ADJ-2"><a href="/en/cases/2024/january/adj-2.html">ADJ-2</a></h2>
        <p class="description" title="Row two">Row two</p>
      </div><div class="col-sm-3"><span class="date">01/01/2024</span></div></div></li>
      <li class="each-item"><div class="row"><div class="col-sm-9">
        <h2 class="title" title="ADJ-3"><a href="/en/cases/2024/january/adj-3.html">ADJ-3</a></h2>
        <p class="description" title="Row three">Row three</p>
      </div><div class="col-sm-3"><span class="date">01/01/2024</span></div></div></li>
    </ul></body></html>"""
    request = Request(
        "https://www.workplacerelations.ie/en/search/?body=15376",
        meta={"body": "15376", "partition": partition, "page_number": 1},
    )
    response = HtmlResponse(url=request.url, body=html, encoding="utf-8", request=request)

    # Raise only on the 2nd row's mark_pending call, leaving rows 1 and 3 fine.
    original_mark_pending = spider._tracker.mark_pending
    calls = {"n": 0}

    def flaky_mark_pending(*args: object, **kwargs: object) -> None:
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("unexpected selector surprise")
        return original_mark_pending(*args, **kwargs)

    monkeypatch.setattr(spider._tracker, "mark_pending", flaky_mark_pending)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        results = list(spider.parse_listing(response))

    follow_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_detail
    ]
    next_page_requests = [
        r for r in results if isinstance(r, Request) and r.callback == spider.parse_listing
    ]

    # Rows 1 and 3 survived; only the middle row was dropped.
    assert [r.cb_kwargs["identifier"] for r in follow_requests] == ["ADJ-1", "ADJ-3"]
    # Pagination continuation is still emitted despite the mid-page error.
    assert len(next_page_requests) == 1

    assert spider._tracker.partition_state[key]["records_found"] == 3
    assert spider._tracker.partition_state[key]["records_failed"] == 1
    # Only the two surviving rows were marked pending (the failed row was not).
    assert len(spider._tracker.partition_state[key]["pending"]) == 2

    failed = [e for e in _events_from(caplog) if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert failed[0]["reason"] == "RuntimeError('unexpected selector surprise')"
    assert failed[0]["listing_url"] == response.url


# -- empty-body validation in parse_document_binary (robustness pass, Fix 2) ---


def test_a_200_answer_with_an_empty_body_is_recorded_as_a_failure(
    spider: WrcSpider, partition: DatePartition, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-304 response with a zero-length body is not a successful fetch:
    it must be an explicit, logged failure (http_status 200, "empty" reason,
    records_failed incremented) -- never a silently stored 0-byte document.
    """
    request = _binary_request(spider, partition)

    with caplog.at_level(logging.INFO, logger="wrc.events"):
        response = HtmlResponse(url=request.url, status=200, body=b"", request=request)
        items = list(spider.parse_document_binary(response, **request.cb_kwargs))

    assert items == []  # no record yielded for an empty body
    assert spider._tracker.totals["records_failed"] == 1
    assert spider._tracker.totals["records_scraped"] == 0
    assert spider._tracker.totals["documents_not_modified"] == 0

    failed = [e for e in _events_from(caplog) if e["event"] == "record_failed"]
    assert len(failed) == 1
    assert "empty" in failed[0]["reason"]
    assert failed[0]["http_status"] == 200
