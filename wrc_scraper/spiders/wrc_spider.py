"""Production WRC decisions spider: crawls the given body list x date range,
extracts metadata (identifier remapped to h2.title, a safe-identifier slug for
the transform stage), retains raw_html, logs structured JSONL events via
wrc_scraper.logging_utils, reconciles dangling partitions, and fetches
pdf/doc/docx binaries via a chained request so `raw_binary` reaches the item
without the storage layer re-fetching it.

Inputs (spider arguments, ISO dates):
    scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-01-31 \
        -a bodies=15376

`bodies` is optional, comma-separated body ids; defaults to all four known
bodies -- one spider, parametrized by body list x date partition, not four
near-identical spiders. Partition granularity defaults to monthly (measured
conclusion, docs/SCRAPY_EXPERIMENTS.md Sec 16), overridable via
WRC_PARTITION_UNIT / WRC_PARTITION_COUNT env vars. Target search URL
overridable via WRC_SEARCH_URL.

Binary documents (pdf/doc/docx) are fetched with a conditional GET when a
verified copy is already in the Landing Zone: `ConditionalGetMiddleware`
(wrc_scraper/middlewares.py) attaches the stored ETag as `If-None-Match` to
the chained binary request -- identified via its `body`/`detail_url`/
`document_type` meta -- and a `304 Not Modified` skips the download entirely
(assessment requirement #9). This is an optimization only -- whenever the
check can't be made safely the request falls back to a plain GET and the
usual SHA-256 comparison, so it can save work but never change an outcome.
See storage/conditional_get.py. HTML pages carry no validators, never carry
`document_type` in their meta, and are therefore never made conditional.
Disable with WRC_CONDITIONAL_GET=false.

Termination is purely by exhausting real work (empty-page pagination stop +
every body x partition combination processed). CLOSESPIDER_* settings are a
CLI-only smoke-test convenience and must never be added here or in settings.py.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import PurePosixPath
from urllib.parse import urlencode, urlsplit

import scrapy
from scrapy.http import Response, TextResponse

from wrc_scraper.bodies import body_name, body_slug
from wrc_scraper.config import PartitionSettings, ScrapingSettings
from wrc_scraper.items import WrcDecisionRecord
from wrc_scraper.logging_utils import get_events_logger
from wrc_scraper.partitioning import (
    DatePartition,
    PartitionGranularity,
    PartitionUnit,
    format_site_date,
    iter_date_partitions,
)
from wrc_scraper.spiders.partition_tracker import PartitionTracker
from wrc_scraper.validation import (
    KNOWN_BODY_IDS,
    parse_iso_date,
    validate_bodies,
    validate_date_range,
)

DOCUMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}
CHROME_DOC_FILENAMES = {"cookie_policy.pdf", "Decisions_Information_Guide.pdf"}

# The listing banner ("Shows 1 to 10 of 234 results") gives the authoritative
# per-query total. Captured so a partition that fails mid-pagination can report
# *how many* records went unscraped even though the unfetched rows' identifiers
# are unknowable (assessment req #10). Comma-safe; matches the count only.
_RESULT_TOTAL_RE = re.compile(r"of\s+([\d,]+)\s+results", re.IGNORECASE)


def _parse_result_total(text: str) -> int | None:
    """The site-reported total for a body x date query, or None when the banner
    is absent (e.g. the "no search results" page). Best-effort by design -- it
    only ever *adds* reporting detail, never gates scraping.
    """
    match = _RESULT_TOTAL_RE.search(text)
    if match is None:
        return None
    return int(match.group(1).replace(",", ""))


def _document_extension(href: str) -> str | None:
    """Return "pdf"/"doc"/"docx" if href's path (query/fragment stripped) has
    that extension, case-insensitively -- else None. Matching is done in
    Python, not via a case-sensitive CSS `$=` selector.
    """
    suffix = PurePosixPath(urlsplit(href).path).suffix.lower()
    return suffix[1:] if suffix in DOCUMENT_EXTENSIONS else None


class WrcSpider(scrapy.Spider):
    """Crawls WRC decisions across the given bodies and date range."""

    name = "wrc"

    def __init__(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        bodies: str | None = None,
        *args: object,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)

        if not start_date or not end_date:
            raise ValueError("both start_date and end_date are required (YYYY-MM-DD)")

        self.start_date = parse_iso_date(start_date)
        self.end_date = parse_iso_date(end_date)
        validate_date_range(self.start_date, self.end_date)

        self.bodies = bodies.split(",") if bodies else sorted(KNOWN_BODY_IDS)
        validate_bodies(self.bodies)

        partition_cfg = PartitionSettings.from_env()
        self.granularity = PartitionGranularity(
            unit=PartitionUnit(partition_cfg.unit),
            count=partition_cfg.count,
        )

        scraping_cfg = ScrapingSettings.from_env()
        self.search_url = scraping_cfg.search_url
        # Instance-level so it tracks WRC_SEARCH_URL / WRC_ALLOWED_DOMAINS -- else
        # overriding the search host would be silently dropped by OffsiteMiddleware.
        self.allowed_domains = scraping_cfg.allowed_domains
        self._max_pages = scraping_cfg.max_pages

        self._events_logger = get_events_logger()
        self._tracker = PartitionTracker(self._events_logger)

    async def start(self):
        for body in self.bodies:
            for partition in iter_date_partitions(self.start_date, self.end_date, self.granularity):
                self._tracker.start_partition(body, partition)
                yield self._search_request(body, partition, page_number=1)

    def _search_request(
        self, body: str, partition: DatePartition, page_number: int
    ) -> scrapy.Request:
        params = {
            "decisions": "1",
            "from": format_site_date(partition.start),
            "to": format_site_date(partition.end),
            "legislationsub": "",
            "body": body,
            "pageNumber": str(page_number),
        }
        url = f"{self.search_url}?{urlencode(params)}"
        return scrapy.Request(
            url,
            callback=self.parse_listing,
            errback=self._listing_failed,
            meta={
                "body": body,
                "partition": partition,
                "page_number": page_number,
            },
        )

    # -- listing -----------------------------------------------------------

    def parse_listing(self, response: TextResponse):
        body = response.meta["body"]
        partition: DatePartition = response.meta["partition"]
        page_number = response.meta["page_number"]

        # Capture the authoritative total once, from the first page that carries
        # the banner -- so a later page failure can still report the shortfall.
        if self._tracker.records_expected(body, partition) is None:
            total = _parse_result_total(response.text)
            if total is not None:
                self._tracker.set_records_expected(body, partition, total)

        rows = response.css("li.each-item")

        for row in rows:
            self._tracker.record_found(body, partition)

            detail_href = row.css("h2.title a::attr(href)").get()
            if not detail_href:
                self._tracker.record_immediate_failure(
                    body,
                    partition,
                    reason="listing row missing a detail link",
                    listing_url=response.url,
                )
                continue

            # `identifier` is h2.title -- the field the assessment PDF's own
            # screenshot labels "identifier". It is not the storage key (it
            # collides across corpora -- see storage/keys.py), so fail fast if
            # it's missing rather than synthesizing one from another field.
            identifier = row.css("h2.title::attr(title)").get(
                default=row.css("h2.title::text").get()
            )
            if not identifier or not identifier.strip():
                self._tracker.record_immediate_failure(
                    body,
                    partition,
                    reason="listing row missing h2.title (identifier)",
                    listing_url=response.url,
                )
                continue

            decision_date_raw = row.css("span.date::text").get()
            description = row.css("p.description::attr(title)").get(
                default=row.css("p.description::text").get()
                or row.css("h2.title::attr(title)").get()
                or row.css("h2.title::text").get()
            )

            # Build the Request once and read its .url back for the pending key
            # -- never recompute the URL a second way (e.g. via urljoin). Some
            # hrefs contain literal spaces (docs/SCRAPY_EXPERIMENTS.md Sec 11,
            # e.g. "ir- sc- 00000787.html"); urljoin leaves the space as-is
            # while the Request/response URL percent-encodes it (%20), so two
            # independently computed "detail_url"s would silently never match
            # and the record would misfire as a dangling failure on success.
            # Catch-all for any unexpected surprise while *constructing* the
            # detail request (e.g. a novel selector shape on a single row).
            # Without this, one bad row would abort the whole generator --
            # taking the remaining rows on this page *and* the pagination
            # continuation down with it. Isolate the failure to this row,
            # log it via the existing helper, and move on. Only construction
            # is guarded; `yield detail_request` stays outside so nothing
            # interferes with generator/`yield` control flow. The two explicit
            # checks above `continue` before reaching here, so their specific
            # reason strings are unaffected.
            try:
                detail_request = response.follow(
                    detail_href,
                    callback=self.parse_detail,
                    errback=self._detail_failed,
                    cb_kwargs={
                        "body": body,
                        "partition": partition,
                        "identifier": identifier.strip(),
                        "published_date_raw": (decision_date_raw or "").strip(),
                        "description": (description or "").strip(),
                    },
                )
                self._tracker.mark_pending(body, partition, detail_request.url, identifier.strip())
            except Exception as exc:  # noqa: BLE001 -- isolate one row, keep the page going
                self._tracker.record_immediate_failure(
                    body,
                    partition,
                    reason=repr(exc),
                    listing_url=response.url,
                )
                continue
            yield detail_request

        if rows:
            next_page = page_number + 1
            # Safety ceiling, not a real limit: docs/SCRAPY_EXPERIMENTS.md Sec 23
            # verified pageNumber is respected and the largest observed real WRC
            # partition (234 records) needs 24 pages. This only guards against a
            # runaway loop if that ever stops being true; it must never bind on
            # a real partition at this project's scale.
            if next_page > self._max_pages:
                self._tracker.finish_pagination_at_max_pages(body, partition, self._max_pages)
                return
            yield self._search_request(body, partition, page_number=next_page)
        else:
            self._tracker.finish_pagination_normally(body, partition)

    def _listing_failed(self, failure) -> None:
        request = failure.request
        body = request.meta["body"]
        partition: DatePartition = request.meta["partition"]

        reason = repr(failure.value)
        self._log_event(
            "partition_listing_page_failed",
            body=body,
            partition_date=partition.partition_date.isoformat(),
            page_number=request.meta["page_number"],
            url=request.url,
            reason=reason,
        )
        self._tracker.mark_pagination_failed(
            body, partition, reason=f"listing page failed: {reason}"
        )

    # -- detail --------------------------------------------------------------

    def parse_detail(
        self,
        response: TextResponse,
        body: str,
        partition: DatePartition,
        identifier: str,
        published_date_raw: str,
        description: str,
    ):
        document_type, document_url = self._resolve_document(response)
        published_date_iso = self._parse_decision_date(published_date_raw)

        if document_type == "html_inline":
            # Mirrors the empty-body guard in parse_document_binary: a
            # truncated/zero-byte 200 must not be counted as a successfully
            # scraped record just because the HTTP layer succeeded.
            if not response.text.strip():
                self._tracker.record_failed(
                    body,
                    partition,
                    detail_url=response.url,
                    reason="empty response body for html_inline document",
                    url=response.url,
                    http_status=response.status,
                )
                self._tracker.maybe_complete(body, partition)
                return

            record = WrcDecisionRecord(
                identifier=identifier,
                description=description,
                published_date=published_date_iso,
                detail_url=response.url,
                document_type=document_type,
                document_url=document_url,
                partition_date=partition.partition_date.isoformat(),
                body_slug=body_slug(body),
                body_name=body_name(body),
                scraped_at=datetime.now(UTC).isoformat(),
                raw_html=response.text,
            )
            self._tracker.record_scraped(body, partition, response.url)
            yield record
            self._tracker.maybe_complete(body, partition)
            return

        # pdf/doc/docx: chain one more request to fetch the actual binary, so
        # the item carries raw_binary and the storage layer never needs
        # to re-fetch it (mirrors how raw_html is already handled above).
        #
        # `body`/`detail_url`/`document_type` in meta are exactly what
        # ConditionalGetMiddleware (wrc_scraper/middlewares.py) needs to decide
        # whether this GET can be made conditional on a stored ETag -- if so,
        # it attaches If-None-Match plus `handle_httpstatus_list: [304]`
        # itself before the request is sent (a 304 here is a success;
        # Scrapy's HttpError middleware would otherwise treat it as a
        # failure). detail_url (not document_url) is also the
        # pending-tracking key: it's what was marked pending in
        # parse_listing, and _binary_failed needs it to clear the right entry
        # -- request.url here is the *document* URL.
        meta: dict[str, object] = {
            "body": body,
            "partition": partition,
            "detail_url": response.url,
            "document_type": document_type,
        }

        yield scrapy.Request(
            document_url,
            callback=self.parse_document_binary,
            errback=self._binary_failed,
            meta=meta,
            cb_kwargs={
                "body": body,
                "partition": partition,
                "identifier": identifier,
                "description": description,
                "published_date_iso": published_date_iso,
                "detail_url": response.url,
                "document_type": document_type,
                "document_url": document_url,
            },
        )

    def parse_document_binary(
        self,
        response: Response,
        body: str,
        partition: DatePartition,
        identifier: str,
        description: str,
        published_date_iso: str,
        detail_url: str,
        document_type: str,
        document_url: str,
    ):
        # Captured for free from the response already fetched -- no extra
        # request. Verified live (docs/SCRAPY_EXPERIMENTS.md Sec 18) that the
        # WRC document endpoints return a stable ETag. It is what makes the
        # conditional GET above possible, but SHA-256 stays authoritative for
        # deciding whether stored content actually changed.
        response_etag = response.headers.get("ETag", b"").decode("utf-8") or None
        not_modified = response.status == 304

        if not_modified:
            # 304: no body was sent. The bytes already in the Landing Zone
            # are current, so the record deliberately carries no raw_binary --
            # the storage layer re-confirms the stored copy and bumps
            # last_checked_at instead of re-hashing and re-uploading.
            self._tracker.record_document_not_modified()
            self._log_event(
                "document_not_modified",
                body=body,
                partition_date=partition.partition_date.isoformat(),
                url=response.url,
                etag=response.request.meta.get("conditional_etag"),
            )

        # A non-304 response with a zero-length body is not a successful fetch:
        # storing it would create a "successfully scraped" 0-byte document that
        # looks fine in the stats but is useless downstream. Fail it explicitly
        # (a clearly logged failure beats a silently-worse outcome) rather than
        # letting an empty `raw_binary` flow through as if it were real content.
        # 304s legitimately carry no body and are handled above, so they're
        # excluded from this check.
        if not not_modified and len(response.body) == 0:
            self._tracker.record_failed(
                body,
                partition,
                detail_url=detail_url,
                reason=f"empty response body for {document_type} document",
                url=response.url,
                http_status=response.status,
            )
            self._tracker.maybe_complete(body, partition)
            return

        record = WrcDecisionRecord(
            identifier=identifier,
            description=description,
            published_date=published_date_iso,
            detail_url=detail_url,
            document_type=document_type,
            document_url=document_url,
            partition_date=partition.partition_date.isoformat(),
            body_slug=body_slug(body),
            body_name=body_name(body),
            scraped_at=datetime.now(UTC).isoformat(),
            raw_binary=None if not_modified else response.body,
            remote_etag=response_etag or response.request.meta.get("conditional_etag"),
            not_modified=not_modified,
        )
        self._tracker.record_scraped(body, partition, detail_url)
        yield record
        self._tracker.maybe_complete(body, partition)

    def _binary_failed(self, failure) -> None:
        request = failure.request
        body = request.meta["body"]
        partition: DatePartition = request.meta["partition"]
        http_status = getattr(getattr(failure.value, "response", None), "status", None)
        self._tracker.record_failed(
            body,
            partition,
            detail_url=request.meta["detail_url"],
            reason=repr(failure.value),
            url=request.url,
            http_status=http_status,
        )
        self._tracker.maybe_complete(body, partition)

    def _detail_failed(self, failure) -> None:
        request = failure.request
        body = request.meta["body"]
        partition: DatePartition = request.meta["partition"]
        http_status = getattr(getattr(failure.value, "response", None), "status", None)
        self._tracker.record_failed(
            body,
            partition,
            detail_url=request.url,
            reason=repr(failure.value),
            url=request.url,
            http_status=http_status,
        )
        self._tracker.maybe_complete(body, partition)

    @staticmethod
    def _parse_decision_date(decision_date_raw: str) -> str:
        try:
            return datetime.strptime(decision_date_raw, "%d/%m/%Y").date().isoformat()
        except ValueError:
            return ""

    def _resolve_document(self, response: TextResponse) -> tuple[str, str]:
        content = response.css("div.content")
        candidates = self._find_document_links(content)

        if not candidates:
            # EAT-import template: .content yields nothing, but
            # the real download link lives in an adjacent related-items
            # block. This is a deliberate, structurally-targeted lookup, not
            # a whole-page fallback search.
            related = response.css("div.related-items.related-file")
            if related:
                candidates = self._find_document_links(related)

        if not candidates:
            return "html_inline", response.url

        if len(candidates) > 1:
            self._log_event(
                "multiple_document_links",
                url=response.url,
                candidates=[href for href, _ in candidates],
            )

        href, extension = candidates[0]
        return extension, response.urljoin(href)

    @staticmethod
    def _find_document_links(selector_root) -> list[tuple[str, str]]:
        """Genuine document links within selector_root, in DOM order, excluding
        known site-chrome PDFs.
        """
        candidates: list[tuple[str, str]] = []
        for href in selector_root.css("a::attr(href)").getall():
            extension = _document_extension(href)
            if extension is None:
                continue
            filename = PurePosixPath(urlsplit(href).path).name
            if filename in CHROME_DOC_FILENAMES:
                continue
            candidates.append((href, extension))
        return candidates

    # -- run lifecycle ---------------------------------------------------------

    def closed(self, reason: str) -> None:
        self._tracker.reconcile_dangling()
        self._tracker.log_run_summary(reason)

    def _log_event(self, event: str, **fields: object) -> None:
        self._events_logger.info(json.dumps({"event": event, **fields}))
