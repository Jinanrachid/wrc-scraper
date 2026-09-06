"""Dedicated logger for structured JSON events.

`self.logger.info(json.dumps(...))` on a Scrapy spider logger still goes through
Scrapy's default formatter, which prefixes every line with a timestamp/level/name
(e.g. "2026-... [wrc] INFO: {...}") -- so the line as printed isn't valid JSON on
its own. This module gives structured events their own non-propagating logger with
a bare "%(message)s" formatter, so each emitted line is genuinely parseable JSON,
while Scrapy's own diagnostic logging (crawl stats, warnings, retries) is left
completely untouched on the normal spider logger.

Level and destination are configurable (no hardcoded operational values): the
level comes from ``WRC_LOG_LEVEL`` (default INFO) and, if ``WRC_EVENTS_LOG_FILE``
is set, events are additionally written there as JSON lines (otherwise stdout).
"""

from __future__ import annotations

import logging
import sys

from wrc_scraper.config import env_optional_str, env_str

EVENTS_LOGGER_NAME = "wrc.events"


def get_events_logger() -> logging.Logger:
    """Return the structured-events logger, configuring it on first use."""
    logger = logging.getLogger(EVENTS_LOGGER_NAME)
    if not logger.handlers:
        formatter = logging.Formatter("%(message)s")
        log_file = env_optional_str("WRC_EVENTS_LOG_FILE")
        handler: logging.Handler = (
            logging.FileHandler(log_file, encoding="utf-8")
            if log_file
            else logging.StreamHandler(sys.stdout)
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(env_str("WRC_LOG_LEVEL", "INFO").upper())
        logger.propagate = False
    return logger
