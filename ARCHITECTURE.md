# Architecture

```text
Scrapy → Landing MongoDB + MinIO → Transformation (BeautifulSoup) → Processed MongoDB + MinIO
                         ↑
                    Dagster orchestration
```

`WrcSpider` crawls one `(month, body)` partition at a time and writes each record through
`IngestService` into an immutable **Landing Zone** (MongoDB metadata + MinIO artifacts).
`TransformService` reads the Landing Zone, cleans HTML with BeautifulSoup (binaries pass through
unchanged), and writes a **Transformed Zone** (its own collection + bucket). Dagster orchestrates
`landing_documents → processed_documents` as same-partition dependent assets, holding no business
logic — both assets call the same ingest/transform code a manual CLI run would.

## Date partitioning

Both stages partition by **calendar month**: the spider via `WRC_PARTITION_UNIT`/`_COUNT`, Dagster via
a `MultiPartitionsDefinition` of `MonthlyPartitionsDefinition` (month) × `StaticPartitionsDefinition`
(body_slug), so each partition is one `(month, body_slug)` pair. Day/week/month granularity all
yielded identical logical records — granularity changes how work is chunked, not what is scraped.
Monthly matches the site's URL structure (`/en/cases/2024/january/...`), keeps the partition count
manageable (~1,824 for 1989–2026 × 4 bodies vs. ~7,900 weekly), avoids near-empty buckets, and bounds
retry/recovery to one month of one body. It stays configurable per source.

## Retries and rate limiting

Three independent layers, not one stacked budget:

- **Scrapy (request level)** — `RETRY_TIMES`/`RETRY_HTTP_CODES` retry a request on 429/5xx/timeout;
  `AutoThrottle` adapts delay/concurrency to latency. `CONCURRENT_REQUESTS=16`/`DOWNLOAD_TIMEOUT=60`
  were tuned for highest *reliable* throughput (24 was the raw optimum but caused timeout noise under
  elevated site latency).
- **Dagster partition level** — a `RetryPolicy` (exponential backoff + jitter) retries only the one
  `(month, body)` partition that failed after Scrapy's retries were exhausted.
- **Dagster run level** — the daemon's `run_retries` relaunches a run that crashed outright.

All three are safe only because storage is idempotent (below) — a retry can never duplicate a record.

## Deduplication / idempotency

Landing identity is `(body_slug, detail_url)`, not `identifier`/`ref_no`: sampling found `ref_no` and
the site's `identifier` colliding across imported records, while `detail_url` never did. Identity
gives deterministic Mongo `_id`s and MinIO keys, so a re-crawl targets the same record instead of
duplicating it. Content dedup is separate: a SHA-256 `file_hash` decides whether bytes actually
changed — a matching hash with the object still present skips the write, and binaries skip the
*download* via a conditional GET on a stored ETag (HTML has no validator, so only its write is
skipped). MinIO is written before Mongo is marked `stored`, so a crash between them leaves a harmless
orphan object, never a record pointing at nothing. Transformation is idempotent the same way and never
writes the Landing Zone; where several `detail_url`s share one `identifier` (a variant cluster), it
picks one canonical copy — non-empty HTML over binary, then longest content — into a collision-
resistant `identifier.ext` key.

## What would change for 50+ sources

**Stays (already source-agnostic):** partitioned ingestion, deterministic identity, content hashing,
object storage + metadata DB, the ingest/transform split, and Dagster's partition/retry/backfill
machinery. The data contract doesn't change — only how it's configured, deployed, scaled, and operated.

**Extraction & configuration.** One spider per site behind the shared item contract and
storage/transform layer. Partitioning becomes source-specific — `(source, section, month)` where
applicable — so sources scale and recover independently. Per-source rate limits, retry policies, and
schedules live in a **source registry** rather than one global `WRC_*` config, and at scale sources
split into separate Scrapy projects/repos so one source's dependency or deploy churn can't break
another's.

**Distributed execution.** A real run launcher (Dagster on ECS) schedules each partition run as its own
ECS task instead of the single local process this project uses, launching the Scrapy crawl via
**Dagster Pipes**. Idempotent storage makes concurrent, out-of-order, and retried runs safe from
duplication.

**Storage.** Artifacts stay in S3/MinIO under a `source/section/month/` prefix with lifecycle policies
for cold data. Metadata stays in MongoDB. Dagster instance storage moves from local SQLite to shared
PostgreSQL for a multi-worker deployment.

**Operations.** Centralized structured-log aggregation with per-source dashboards and alerting so one
source's failures surface independently; per-source data-quality asset checks; and credentials for all
of the above (registry entries included) pulled from a secrets manager rather than environment files.
