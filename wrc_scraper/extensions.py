"""Small downloader middleware for per-request latency stats.

Needed to satisfy the Step 2.0 experiment's measurement requirements (mean/p95
latency per trial, docs/SCRAPY_EXPERIMENTS.md Sec 15) -- Scrapy's own stats
collector counts requests/responses/status codes but doesn't time them.
"""

from __future__ import annotations

import time

from scrapy import Request, Spider, signals
from scrapy.crawler import Crawler
from scrapy.http import Response
from scrapy.statscollectors import StatsCollector


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
