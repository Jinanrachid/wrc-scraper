"""Downloader middlewares: per-request latency stats, and the conditional-GET
header injection for binary documents.

LatencyStatsMiddleware is needed to satisfy the Step 2.0 experiment's
measurement requirements (mean/p95 latency per trial,
docs/SCRAPY_EXPERIMENTS.md Sec 15) -- Scrapy's own stats collector counts
requests/responses/status codes but doesn't time them.

ConditionalGetMiddleware is a downloader middleware -- not spider logic --
because its whole job is to modify an outgoing request before it's sent; see
its own docstring below for why it lives here instead of in WrcSpider.
"""

from __future__ import annotations

import json
import logging
import time

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.statscollectors import StatsCollector

from wrc_scraper.bodies import body_slug
from wrc_scraper.config import ScrapingSettings
from wrc_scraper.logging_utils import get_events_logger
from wrc_scraper.storage.conditional_get import ConditionalGetAdvisor
from wrc_scraper.storage.factory import StorageSettings, build_minio, build_mongo


class LatencyStatsMiddleware:
    """Records wall-clock request latency and reports mean/p95 into the run's stats."""

    def __init__(self, stats: StatsCollector) -> None:
        self.stats = stats
        self.samples: list[float] = []

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> LatencyStatsMiddleware:
        assert crawler.stats is not None
        middleware = cls(crawler.stats)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_request(self, request: Request) -> None:
        request.meta["_latency_start"] = time.monotonic()

    def process_response(self, request: Request, response: Response) -> Response:
        self._record(request)
        return response

    def process_exception(self, request: Request, exception: Exception) -> None:
        self._record(request)
        return None

    def _record(self, request: Request) -> None:
        start = request.meta.get("_latency_start")
        if start is not None:
            self.samples.append(time.monotonic() - start)

    def spider_closed(self, spider: Spider) -> None:
        if not self.samples:
            return
        ordered = sorted(self.samples)
        n = len(ordered)
        mean = sum(ordered) / n
        p95_index = min(n - 1, round(0.95 * (n - 1)))
        self.stats.set_value("latency/mean_seconds", round(mean, 3))
        self.stats.set_value("latency/p95_seconds", round(ordered[p95_index], 3))
        self.stats.set_value("latency/sample_count", n)


class ConditionalGetMiddleware:
    """Adds a conditional GET header to the chained binary-document request
    when a verified prior copy is already in the Landing Zone (assessment
    requirement #9: don't re-download unchanged files).

    This is a downloader middleware, not spider logic, because it only ever
    decides whether to *modify an outgoing request*, using metadata
    (`body`, `detail_url`, `document_type`) the spider already attaches to
    that one request -- the chained request built in `WrcSpider.parse_detail`
    once a pdf/doc/docx has been resolved. The HTML detail request carries no
    `document_type` in its meta and so never matches `process_request` below;
    it is therefore never made conditional, matching
    storage/conditional_get.py's own refusal to advise on html_inline.

    It makes no idempotency decision of its own -- that stays entirely in
    ConditionalGetAdvisor (read-only lookup) and IngestService (the actual
    state machine) -- so keeping this here does not create a second
    storage/idempotency abstraction, only a second, independent read-only
    connection pair to the same store the pipeline writes to. Owns and closes
    that connection pair itself, via the same from_crawler/spider_closed
    lifecycle as LatencyStatsMiddleware above.
    """

    def __init__(
        self,
        advisor: ConditionalGetAdvisor | None,
        mongo_client: object | None,
        events_logger: logging.Logger,
    ) -> None:
        self._advisor = advisor
        self._mongo_client = mongo_client
        self._events_logger = events_logger
        self._error_logged = False

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> ConditionalGetMiddleware:
        events_logger = get_events_logger()
        scraping_cfg = ScrapingSettings.from_env()

        if not scraping_cfg.conditional_get_enabled:
            middleware = cls(None, None, events_logger)
            middleware._log_event("conditional_get_disabled", reason="WRC_CONDITIONAL_GET is off")
            return middleware

        mongo_client = None
        try:
            storage_settings = StorageSettings.from_env()
            mongo_client, mongo_repo = build_mongo(storage_settings)
            advisor = ConditionalGetAdvisor(mongo_repo, build_minio(storage_settings))
        except Exception as exc:  # noqa: BLE001 -- an optimization must never break the crawl
            # build_mongo can succeed (a live, connected client) even though a
            # later step in this same block -- build_minio, ConditionalGetAdvisor
            # construction -- then fails. Without this, that client is discarded
            # here with no reference left to it anywhere, and the disabled
            # middleware instance below never gets a spider_closed hook to
            # close it, leaking the connection for the life of the process.
            if mongo_client is not None:
                mongo_client.close()
            middleware = cls(None, None, events_logger)
            middleware._log_event("conditional_get_disabled", reason=repr(exc))
            return middleware

        middleware = cls(advisor, mongo_client, events_logger)
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def process_request(self, request: Request, spider: Spider) -> None:
        if self._advisor is None:
            return None

        document_type = request.meta.get("document_type")
        detail_url = request.meta.get("detail_url")
        body = request.meta.get("body")
        if document_type is None or detail_url is None or body is None:
            return None  # not the chained binary-document request

        etag = self._advisor.etag_for(body_slug(body), detail_url, document_type)
        error = self._advisor.last_error
        if error is not None and not self._error_logged:
            # Reported once, not once per request: an unreachable store would
            # otherwise flood the log with the same line for every document.
            self._error_logged = True
            self._log_event("conditional_get_unavailable", reason=error)
        if etag is None:
            return None

        request.headers["If-None-Match"] = etag  # unquoted -- the only form this site honours
        request.meta["handle_httpstatus_list"] = [304]
        request.meta["conditional_etag"] = etag
        return None

    def spider_closed(self, spider: Spider) -> None:
        if self._mongo_client is not None:
            self._mongo_client.close()

    def _log_event(self, event: str, **fields: object) -> None:
        self._events_logger.info(json.dumps({"event": event, **fields}))
