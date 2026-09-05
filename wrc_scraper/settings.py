# Scrapy settings for wrc_scraper project.
#
# This module is a thin adapter: it maps Scrapy's UPPERCASE setting names onto
# the centralized, env-driven configuration in ``wrc_scraper.config``. No
# operational value is defined here directly -- every connection string, storage
# path, partition size, and scraping parameter comes from an environment
# variable (with a documented default), per the assessment's "no hardcoded
# values" requirement. See ``.env.example`` for the full contract and
# ``docs/SCRAPY_EXPERIMENTS.md`` Sec 15 for the measured concurrency/throttle
# baseline behind the defaults.

from wrc_scraper.config import ScrapingSettings

_cfg = ScrapingSettings.from_env()

BOT_NAME = "wrc_scraper"

SPIDER_MODULES = ["wrc_scraper.spiders"]
NEWSPIDER_MODULE = "wrc_scraper.spiders"

ADDONS = {}

# Pin the event-loop reactor explicitly rather than inheriting Scrapy's
# version-dependent default. Defaults to the asyncio-backed Twisted reactor
# (AsyncioSelectorReactor); overridable via WRC_TWISTED_REACTOR. Locking this
# keeps an unattended production run deterministic across Scrapy upgrades and
# guarantees a supported loop for any async/await in spiders or middleware.
TWISTED_REACTOR = _cfg.twisted_reactor

# Identify the crawler honestly (docs/SCRAPY_EXPERIMENTS.md robots.txt section).
USER_AGENT = _cfg.user_agent

# robots.txt disallows the capitalized /en/Cases/ and *_Import/ folders, but the
# site's real links are lowercase, which Scrapy's parser (protego) reports as
# ALLOWED -- verified in docs/SCRAPY_EXPERIMENTS.md. Defaulting to False is a
# deliberate, documented decision (the assessment requires this exact data), not
# an accidental default; still overridable via WRC_ROBOTSTXT_OBEY.
ROBOTSTXT_OBEY = _cfg.robotstxt_obey

# Concurrency and throttling -- measured via the Step 2.0 adaptive ramp
# experiment against the live site (docs/SCRAPY_EXPERIMENTS.md Sec 15). Per-domain
# is locked equal to the global cap by default (single-domain crawl), else
# Scrapy's default of 8 would silently bind below CONCURRENT_REQUESTS.
CONCURRENT_REQUESTS = _cfg.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _cfg.concurrent_requests_per_domain
DOWNLOAD_DELAY = _cfg.download_delay

# AutoThrottle as a runtime safety net, reconciled with the measured baseline
# (hardening item 8): start delay and target concurrency default to the validated
# ~0-delay / measured-concurrency behavior so healthy conditions reproduce the
# baseline, while still adapting upward automatically under real latency
# degradation. Target concurrency defaults to CONCURRENT_REQUESTS (not a fixed
# literal), so tuning WRC_CONCURRENT_REQUESTS keeps AutoThrottle consistent.
AUTOTHROTTLE_ENABLED = _cfg.autothrottle_enabled
AUTOTHROTTLE_START_DELAY = _cfg.autothrottle_start_delay
AUTOTHROTTLE_MAX_DELAY = _cfg.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = _cfg.autothrottle_target_concurrency

# Retry / timeout / max download size.
RETRY_ENABLED = _cfg.retry_enabled
RETRY_TIMES = _cfg.retry_times
RETRY_HTTP_CODES = _cfg.retry_http_codes
DOWNLOAD_TIMEOUT = _cfg.download_timeout
# 0 (Scrapy's default = unbounded) is deliberately avoided: an unbounded download
# could stall a partition indefinitely on a pathological file.
DOWNLOAD_MAXSIZE = _cfg.download_maxsize

# Stateless GET crawl (docs/SCRAPY_EXPERIMENTS.md Sec 1): no cookies needed, so
# disable the cookie middleware. Telnet console is off by default as attack-surface
# hardening. Both overridable via env for debugging.
COOKIES_ENABLED = _cfg.cookies_enabled
TELNETCONSOLE_ENABLED = _cfg.telnetconsole_enabled

LOG_LEVEL = _cfg.log_level

# Records per-request latency (mean/p95) into the run's stats -- used by the
# Step 2.0 experiments (docs/SCRAPY_EXPERIMENTS.md Sec 15) and kept on in
# production since it's cheap and useful in the run summary.
DOWNLOADER_MIDDLEWARES = {
    "wrc_scraper.extensions.LatencyStatsMiddleware": 950,
}

# Writes every scraped item to MongoDB + MinIO via IngestService
# (wrc_scraper/storage/) -- requires `docker compose up -d`. Connection settings
# are the WRC_MONGO_*/WRC_MINIO_* env vars read in wrc_scraper.config /
# wrc_scraper.storage.factory. To crawl without storage (e.g. a parsing smoke
# test), override on the command line: `-s ITEM_PIPELINES={}`.
ITEM_PIPELINES = {
    "wrc_scraper.pipelines.MongoMinioPipeline": 300,
}

# Future-proof a deprecated default.
FEED_EXPORT_ENCODING = "utf-8"
