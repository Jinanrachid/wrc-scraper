"""Per-(body, partition) crawl bookkeeping, extracted out of WrcSpider.

Framework-agnostic: depends only on `partitioning.DatePartition` and a
`logging.Logger`, never on Scrapy -- so it's unit-testable without a crawl,
the same way `storage.ingest_service.IngestService` and
`storage.conditional_get.ConditionalGetAdvisor` are.

The spider still decides *when* a partition starts, a record is found/
scraped/failed, or pagination ends (all of that requires reading a Scrapy
response); this class owns *how* the resulting state/counters are updated
and how the corresponding structured events are emitted, so that bookkeeping
logic has exactly one place to read and to test.

Tracks, per (body, partition_date): found/scraped/failed counts, whether
pagination has reached its last page, how many detail requests (including
any chained binary-document fetch) are still outstanding, and -- if marked
incomplete -- why. A partition is "completed" once pagination is done AND
nothing is still pending (see `maybe_complete`).
"""

from __future__ import annotations

import json
import logging

from wrc_scraper.partitioning import DatePartition, format_site_date


class PartitionTracker:
    def __init__(self, events_logger: logging.Logger) -> None:
        self._events_logger = events_logger
        self.partition_state: dict[tuple[str, str], dict] = {}
        self.totals = {
            "partitions_completed": 0,
            "partitions_incomplete": 0,
            "records_found": 0,
            "records_scraped": 0,
            "records_failed": 0,
            # Records the site reported but that were never scraped and could
            # not be individually logged as record_failed -- i.e. rows behind a
            # listing page that failed / was never reached. Quantifies the
            # remainder of X (assessment req #10) that per-record logging
            # can't enumerate.
            "records_unaccounted": 0,
            "documents_not_modified": 0,
        }

    @staticmethod
    def _key(body: str, partition: DatePartition) -> tuple[str, str]:
        return (body, partition.partition_date.isoformat())

    def start_partition(self, body: str, partition: DatePartition) -> None:
        key = self._key(body, partition)
        self.partition_state[key] = {
            "records_found": 0,
            "records_scraped": 0,
            "records_failed": 0,
            # Site-reported total ("of N results"), captured on the first
            # listing page; None until seen. Lets a partition that fails
            # mid-pagination quantify the unscraped remainder.
            "records_expected": None,
            "pagination_done": False,
            # detail_url -> identifier for every record whose detail (or
            # chained binary) request hasn't resolved yet -- lets a hard
            # shutdown (item cap, kill, crash) log a record_failed for each
            # one individually rather than only an aggregate partition-level
            # note (assessment req #10/tip: every one of the X un-scraped
            # records must be logged with a reason).
            "pending": {},
            "incomplete": False,
            "reason": None,
        }
        self._log_event(
            "partition_started",
            body=body,
            partition_date=key[1],
            date_from=format_site_date(partition.start),
            date_to=format_site_date(partition.end),
        )

    def records_expected(self, body: str, partition: DatePartition) -> int | None:
        return self.partition_state[self._key(body, partition)]["records_expected"]

    def set_records_expected(self, body: str, partition: DatePartition, total: int) -> None:
        self.partition_state[self._key(body, partition)]["records_expected"] = total

    def record_found(self, body: str, partition: DatePartition) -> None:
        key = self._key(body, partition)
        self.partition_state[key]["records_found"] += 1
        self.totals["records_found"] += 1

    def mark_pending(
        self, body: str, partition: DatePartition, detail_url: str, identifier: str
    ) -> None:
        key = self._key(body, partition)
        self.partition_state[key]["pending"][detail_url] = identifier

    def record_scraped(self, body: str, partition: DatePartition, detail_url: str) -> None:
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["records_scraped"] += 1
        state["pending"].pop(detail_url, None)
        self.totals["records_scraped"] += 1

    def record_document_not_modified(self) -> None:
        self.totals["documents_not_modified"] += 1

    def record_failed(
        self,
        body: str,
        partition: DatePartition,
        *,
        detail_url: str,
        reason: str,
        url: str,
        http_status: int | None = None,
    ) -> None:
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["records_failed"] += 1
        state["pending"].pop(detail_url, None)
        self.totals["records_failed"] += 1
        self._log_event(
            "record_failed",
            body=body,
            partition_date=key[1],
            reason=reason,
            http_status=http_status,
            url=url,
        )

    def record_immediate_failure(
        self, body: str, partition: DatePartition, *, reason: str, listing_url: str
    ) -> None:
        """A listing row that fails before any detail request is ever sent
        (missing detail href / missing h2.title identifier) -- counted
        found+failed, but `pending` was never incremented for it, so it must
        not be decremented either.
        """
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["records_failed"] += 1
        self.totals["records_failed"] += 1
        self._log_event(
            "record_failed",
            body=body,
            partition_date=key[1],
            reason=reason,
            listing_url=listing_url,
        )

    def finish_pagination_normally(self, body: str, partition: DatePartition) -> None:
        """The site returned an empty page: pagination is genuinely done.

        docs/SCRAPY_EXPERIMENTS.md Sec 23 verified records_expected is a
        stable, reliable total, so a mismatch against what was actually found
        means real rows went unaccounted for some other reason, and the
        partition can't be trusted as complete even though pagination itself
        terminated normally.
        """
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["pagination_done"] = True
        expected = state["records_expected"]
        if expected is not None and state["records_found"] != expected:
            state["incomplete"] = True
            state["reason"] = (
                f"records_found ({state['records_found']}) != records_expected ({expected})"
            )
            self._log_event(
                "partition_count_mismatch",
                body=body,
                partition_date=key[1],
                records_expected=expected,
                records_found=state["records_found"],
            )
        self.maybe_complete(body, partition)

    def finish_pagination_at_max_pages(
        self, body: str, partition: DatePartition, max_pages: int
    ) -> None:
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["pagination_done"] = True
        state["incomplete"] = True
        state["reason"] = f"max page limit reached (WRC_MAX_PAGES={max_pages})"
        self._log_event(
            "partition_max_pages_reached",
            body=body,
            partition_date=key[1],
            max_pages=max_pages,
            records_found=state["records_found"],
        )
        self.maybe_complete(body, partition)

    def mark_pagination_failed(self, body: str, partition: DatePartition, *, reason: str) -> None:
        """We don't know how many further pages/records this partition had --
        stop paginating it rather than guess, and flag it incomplete.
        """
        key = self._key(body, partition)
        state = self.partition_state[key]
        state["pagination_done"] = True
        state["incomplete"] = True
        state["reason"] = reason
        self.maybe_complete(body, partition)

    def maybe_complete(self, body: str, partition: DatePartition) -> None:
        key = self._key(body, partition)
        state = self.partition_state[key]
        if not state["pagination_done"] or state["pending"]:
            return
        self._complete(key, state)

    def _complete(self, key: tuple[str, str], state: dict) -> None:
        body, partition_date = key
        # X (req #10) = expected - scraped. Of that, records_failed were logged
        # individually; records_unaccounted is the rest -- rows the site counted
        # but that were never fetched (behind a failed/unreached listing page),
        # whose identifiers are unknowable, so they're reported as a count with
        # the partition-level reason rather than per-record.
        expected = state["records_expected"]
        accounted = state["records_scraped"] + state["records_failed"]
        unaccounted = max(expected - accounted, 0) if expected is not None else 0
        self._log_event(
            "partition_completed",
            body=body,
            partition_date=partition_date,
            records_expected=expected,
            records_found=state["records_found"],
            records_scraped=state["records_scraped"],
            records_failed=state["records_failed"],
            records_unaccounted=unaccounted,
            incomplete=state["incomplete"],
            reason=state["reason"],
        )
        self.totals["records_unaccounted"] += unaccounted
        if state["incomplete"]:
            self.totals["partitions_incomplete"] += 1
        else:
            self.totals["partitions_completed"] += 1
        del self.partition_state[key]

    def reconcile_dangling(self) -> None:
        """Safety net: if the spider is shut down (item cap, kill, crash,
        unexpected exception) before every in-flight detail/binary request
        resolves, `pending` never empties and the partition would otherwise
        silently vanish instead of being reported.

        Assessment req #10/tip: "every single record from X is logged with the
        reason" -- an aggregate `partition_completed(incomplete=True)` note is
        not enough on its own. So every still-pending record gets its own
        `record_failed` event here, individually, before the partition-level
        summary is logged. Called once at spider close, before the run summary.
        """
        for key, state in list(self.partition_state.items()):
            body, partition_date = key
            for detail_url, identifier in state["pending"].items():
                state["records_failed"] += 1
                self.totals["records_failed"] += 1
                self._log_event(
                    "record_failed",
                    body=body,
                    partition_date=partition_date,
                    identifier=identifier,
                    url=detail_url,
                    reason="spider closed before this record's request resolved",
                )
            state["pending"] = {}
            state["incomplete"] = True
            state["reason"] = state["reason"] or "spider closed with partition state still pending"
            self._complete(key, state)

    def log_run_summary(self, finish_reason: str) -> None:
        self._log_event("run_summary", finish_reason=finish_reason, **self.totals)

    def _log_event(self, event: str, **fields: object) -> None:
        self._events_logger.info(json.dumps({"event": event, **fields}))
