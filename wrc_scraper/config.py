"""Centralized, environment-driven configuration -- the single source of truth.

The assessment requires that *all* connection strings, storage paths, partition
sizes, and scraping parameters be configurable via environment variables or a
config file, with no hardcoded operational values. This module is where every
such value is read, defaulted, validated, and documented exactly once.

Everything else -- Scrapy's ``settings.py``, the spider, the storage factory,
and the structured-events logger -- pulls from the typed settings objects below
instead of calling ``os.environ`` directly, so there is no scattered, undocumented
configuration and no value defined in more than one place. Every variable is
prefixed ``WRC_`` and documented in ``.env.example``.

Only *operational/deployment* configuration lives here. Domain logic that is part
of the extraction contract (known body ids, document extensions, site-chrome
filenames) intentionally stays in code -- it is not deployment configuration.
"""

from __future__ import annotations

import dataclasses
import os
from urllib.parse import urlsplit

_TRUTHY = ("1", "true", "yes", "on")


# -- primitive env readers ----------------------------------------------------


def env_str(name: str, default: str) -> str:
    return os.environ.get(name, default)


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got {raw!r}") from exc


def env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUTHY


def env_int_list(name: str, default: list[int]) -> list[int]:
    """Comma-separated ints (e.g. WRC_RETRY_HTTP_CODES="429,503"). Empty/unset
    falls back to ``default``.
    """
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)
    try:
        return [int(part) for part in raw.split(",") if part.strip()]
    except ValueError as exc:
        raise ValueError(f"{name} must be a comma-separated list of integers, got {raw!r}") from exc


def env_str_list(name: str, default: list[str]) -> list[str]:
    """Comma-separated strings. Empty/unset falls back to ``default``."""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return list(default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def _host_domain(url: str) -> str:
    """The registrable-ish domain of ``url`` for Scrapy's ``allowed_domains``.

    Strips a leading ``www.`` so a search URL of ``https://www.example.ie/...``
    yields ``example.ie`` (Scrapy's OffsiteMiddleware then also permits the
    ``www.`` subdomain and the document hosts under the same domain).
    """
    host = urlsplit(url).hostname or ""
    return host[4:] if host.startswith("www.") else host


# -- partitioning -------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PartitionSettings:
    unit: str
    count: int

    @classmethod
    def from_env(cls) -> PartitionSettings:
        return cls(
            unit=env_str("WRC_PARTITION_UNIT", "months"),
            count=env_int("WRC_PARTITION_COUNT", 1),
        )


# -- scraping / crawl behavior ------------------------------------------------

DEFAULT_SEARCH_URL = "https://www.workplacerelations.ie/en/search/"
DEFAULT_USER_AGENT = "wrc_scraper (Kedra coding assessment; contact: jinanrachid@gmail.com)"
DEFAULT_RETRY_HTTP_CODES = [429, 500, 502, 503, 504, 408, 522, 524]


@dataclasses.dataclass(frozen=True)
class ScrapingSettings:
    # target
    search_url: str
    allowed_domains: list[str]
    user_agent: str
    robotstxt_obey: bool
    # concurrency / throttle. Ramp in docs/SCRAPY_EXPERIMENTS.md Sec 15 found 24
    # optimal under good latency; the default was revised down to 16 with a 60s
    # timeout (Sec 21) after a live run showed 24/30 producing timeout noise when
    # WRC latency was elevated -- the robust choice for an unattended run.
    concurrent_requests: int
    concurrent_requests_per_domain: int
    download_delay: float
    autothrottle_enabled: bool
    autothrottle_start_delay: float
    autothrottle_max_delay: float
    autothrottle_target_concurrency: float
    # reliability
    retry_enabled: bool
    retry_times: int
    retry_http_codes: list[int]
    download_timeout: int
    download_maxsize: int
    # misc hardening
    cookies_enabled: bool
    telnetconsole_enabled: bool
    conditional_get_enabled: bool
    # logging
    log_level: str

    @classmethod
    def from_env(cls) -> ScrapingSettings:
        search_url = env_str("WRC_SEARCH_URL", DEFAULT_SEARCH_URL)
        # allowed_domains defaults to the search URL's own domain, so overriding
        # WRC_SEARCH_URL to another host doesn't get silently blocked by the
        # OffsiteMiddleware; still explicitly overridable via WRC_ALLOWED_DOMAINS.
        allowed_domains = env_str_list("WRC_ALLOWED_DOMAINS", [_host_domain(search_url)])

        concurrent_requests = env_int("WRC_CONCURRENT_REQUESTS", 16)
        return cls(
            search_url=search_url,
            allowed_domains=allowed_domains,
            user_agent=env_str("WRC_USER_AGENT", DEFAULT_USER_AGENT),
            robotstxt_obey=env_bool("WRC_ROBOTSTXT_OBEY", False),
            concurrent_requests=concurrent_requests,
            # Single-domain crawl: default the per-domain cap equal to the global
            # one, else Scrapy's default (8) would silently bind below it.
            concurrent_requests_per_domain=env_int(
                "WRC_CONCURRENT_REQUESTS_PER_DOMAIN", concurrent_requests
            ),
            download_delay=env_float("WRC_DOWNLOAD_DELAY", 0.0),
            autothrottle_enabled=env_bool("WRC_AUTOTHROTTLE_ENABLED", True),
            autothrottle_start_delay=env_float("WRC_AUTOTHROTTLE_START_DELAY", 0.0),
            autothrottle_max_delay=env_float("WRC_AUTOTHROTTLE_MAX_DELAY", 30.0),
            # Defaults to the measured concurrency so healthy-condition behavior
            # reproduces the validated baseline instead of undercutting it.
            autothrottle_target_concurrency=env_float(
                "WRC_AUTOTHROTTLE_TARGET_CONCURRENCY", float(concurrent_requests)
            ),
            retry_enabled=env_bool("WRC_RETRY_ENABLED", True),
            retry_times=env_int("WRC_RETRY_TIMES", 3),
            retry_http_codes=env_int_list("WRC_RETRY_HTTP_CODES", DEFAULT_RETRY_HTTP_CODES),
            download_timeout=env_int("WRC_DOWNLOAD_TIMEOUT", 60),
            download_maxsize=env_int("WRC_DOWNLOAD_MAXSIZE", 50 * 1024 * 1024),
            cookies_enabled=env_bool("WRC_COOKIES_ENABLED", False),
            telnetconsole_enabled=env_bool("WRC_TELNETCONSOLE_ENABLED", False),
            conditional_get_enabled=env_bool("WRC_CONDITIONAL_GET", True),
            log_level=env_str("WRC_LOG_LEVEL", "INFO"),
        )


# -- transformation (Phase 4) --------------------------------------------------


@dataclasses.dataclass(frozen=True)
class TransformSettings:
    mongo_transformed_collection: str
    minio_transformed_bucket: str
    keep_images: bool
    near_tie_chars: int
    concurrency: int

    @classmethod
    def from_env(cls) -> TransformSettings:
        return cls(
            mongo_transformed_collection=env_str(
                "WRC_MONGO_TRANSFORMED_COLLECTION", "transformed_metadata"
            ),
            minio_transformed_bucket=env_str("WRC_MINIO_TRANSFORMED_BUCKET", "wrc-transformed"),
            # Default drops all <img> from cleaned HTML (docs/SCRAPY_EXPERIMENTS.md
            # Sec 22): the only images observed inside div.content are invisible
            # 1x1 layout spacers, which add no content to a text corpus. Set true
            # to keep images instead (src rewritten to an absolute URL).
            keep_images=env_bool("WRC_TRANSFORM_KEEP_IMAGES", False),
            # Variant-cluster canonical selection (Sec 19/22): candidates whose
            # cleaned content length differs by at most this many characters are
            # logged as an ambiguous near-tie rather than decided silently.
            near_tie_chars=env_int("WRC_TRANSFORM_NEAR_TIE_CHARS", 50),
            # Bounded thread-pool size for processing independent (body_slug,
            # identifier) groups concurrently -- the workload is I/O-bound
            # (Mongo reads/writes, MinIO reads/writes, HTML parsing in between),
            # and both pymongo's MongoClient and the minio-py client are safe
            # for concurrent use from multiple threads. 1 disables the pool
            # entirely (fully sequential), matching the original behavior.
            concurrency=env_int("WRC_TRANSFORM_CONCURRENCY", 8),
        )
