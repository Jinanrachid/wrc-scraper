# WRC Scraping Pipeline

A Scrapy-based data pipeline that scrapes decision data from the Irish
[Workplace Relations Commission](https://www.workplacerelations.ie/en/search/)
decisions database, stores it in a Landing Zone, transforms it into a separate
processed zone, and orchestrates both stages with Dagster. Built for the Kedra
software developer coding assessment.

The pipeline covers four deciding bodies exposed by the WRC search, crawls them
per date partition, and persists both structured metadata (MongoDB) and the raw
decision artifacts — inline HTML pages and PDF/DOC/DOCX documents (MinIO).

| Body id | Slug           | Name                          |
| ------- | -------------- | ----------------------------- |
| `1`     | `equality`     | Equality Tribunal             |
| `2`     | `eat`          | Employment Appeals Tribunal   |
| `3`     | `labour_court` | Labour Court                  |
| `15376` | `wrc`          | Workplace Relations Commission |

Key characteristics:

- **Date-based partitioning** — work is chunked by calendar month (configurable),
  matching the site's own URL structure and keeping partition counts manageable.
- **Landing Zone storage** — every scraped record is written to MongoDB (metadata)
  and MinIO (artifacts) with deterministic, idempotent keys.
- **Transformation** — a separate stage cleans HTML and writes a processed zone
  (its own Mongo collection + MinIO bucket); the Landing Zone is never modified.
- **Dagster orchestration** — the crawl and transform stages run as two
  partitioned assets with a dependency between them.

## Architecture

```text
Scrapy (WrcSpider)
   ↓  items (metadata + raw bytes)
Landing MongoDB + MinIO         (landing_metadata / wrc-landing)
   ↓  reads only
Transformation (BeautifulSoup)
   ↓
Processed MongoDB + MinIO       (transformed_metadata / wrc-transformed)
   ↓
Dagster orchestration           (landing_documents → processed_documents)
```

| Component      | Role                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------- |
| **Scrapy**     | Crawls body × date-range partitions, extracts metadata, fetches HTML and binary documents. |
| **MongoDB**    | Stores one metadata record per document, keyed for deterministic identity and dedup.     |
| **MinIO**      | S3-compatible object store for the raw (landing) and cleaned (transformed) artifacts.    |
| **Transform**  | Cleans HTML, selects a canonical variant per document, writes the processed zone.        |
| **Dagster**    | Partitions, orders, retries, and reports on the ingestion and transformation stages.     |

For the detailed design — identity/dedup rationale, retry layering, idempotency
guarantees, and how the design scales to 50+ sources — see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Project structure

```text
wrc_scraper/
├── spiders/            # WrcSpider + per-partition progress tracker
├── storage/            # framework-agnostic storage layer
│   ├── keys.py         #   deterministic Mongo _id / MinIO object keys
│   ├── hashing.py      #   SHA-256 content hashing
│   ├── ingest_service.py   # idempotency state machine + variant clustering
│   ├── conditional_get.py  # ETag-based conditional GET for binaries
│   ├── mongo_repository.py / minio_repository.py
│   └── factory.py      #   builds repositories from WRC_* env vars
├── transform/          # transformation stage
│   ├── cli.py          #   `wrc-transform` entry point
│   ├── service.py      #   TransformService.transform_range
│   └── html_cleaner.py #   BeautifulSoup HTML cleaning
├── orchestration/      # Dagster layer
│   ├── assets.py       #   landing_documents / processed_documents + checks
│   ├── partitions.py   #   (month × body_slug) partitioning
│   ├── scrapy_runner.py    # subprocess crawl invocation
│   └── definitions.py  #   Dagster entry point (jobs + defs)
├── bodies.py           # deciding-body registry (id → slug → name)
├── config.py           # all WRC_* environment configuration
├── partitioning.py     # generic date-range partitioning
├── items.py            # WrcDecisionRecord schema
├── pipelines.py        # Scrapy ITEM_PIPELINES → storage adapter
├── middlewares.py      # conditional-GET downloader middleware
├── settings.py         # Scrapy settings (all env-driven)
└── logging_utils.py    # structured JSONL events logger

tests/                  # unit + integration tests (storage / transform / orchestration)
ARCHITECTURE.md         # end-to-end design write-up
docker-compose.yml      # mongo + minio + app (+ optional mongo-express)
Dockerfile              # runtime image for the pipeline
```

## Requirements

| Requirement          | Version / notes                                              |
| -------------------- | ------------------------------------------------------------ |
| Python               | 3.12 or 3.13 (`requires-python = ">=3.12,<3.14"`)            |
| Docker + Compose     | For MongoDB, MinIO, and the optional containerized app       |
| `uv`                 | Used for dependency management (`uv.lock`), local dev, and the Docker build |

Core runtime dependencies (from `pyproject.toml`): Scrapy, PyMongo, MinIO,
BeautifulSoup4, lxml. Dagster (`dagster`, `dagster-webserver`) is declared as a
dependency group; lint/test tooling (`ruff`, `pytest`, `mongomock`) is the `dev`
extra.

## Configuration

All operational values are environment-driven through `WRC_*` variables read in
`wrc_scraper/config.py`. Nothing operational is hardcoded.
[`.env.example`](.env.example) is the authoritative list of every variable, its
purpose, and its default; copy it to get started:

```bash
cp .env.example .env
```

`.env` is auto-loaded by Docker Compose. For the Python app on the host, export
it into the environment first (`set -a; source .env; set +a`). Every value has a
built-in default, so an absent `.env` behaves exactly like the committed
defaults.

Configuration is grouped into these categories (see `.env.example` for the full
set):

| Category         | Covers                                                                   |
| ---------------- | ------------------------------------------------------------------------ |
| MongoDB          | Connection URI, database, landing/transformed collections.               |
| MinIO            | Endpoint, access/secret keys, landing/transformed buckets, TLS flag.     |
| Scraping         | Search URL, user agent, concurrency, throttling, retries, timeouts.      |
| Partitioning     | `WRC_PARTITION_UNIT` (`months`\|`days`) and `WRC_PARTITION_COUNT`.        |
| Transformation   | Transformed collection/bucket, image handling, variant near-tie window.  |
| Orchestration    | Dagster quality-check thresholds and partition-level retry policy.       |

The default MinIO credentials (`minioadmin`/`minioadmin`) are **dev-only** —
override them for any real deployment. No credentials are committed or baked into
the Docker image.

## Local setup

The project is managed with `uv`. A single sync installs the runtime
dependencies, the Dagster orchestration group, and the lint/test tooling:

```bash
uv sync --all-extras        # creates .venv with everything needed
docker compose up -d mongo minio   # MongoDB (27017) + MinIO (9000 API / 9001 console)
```

`uv sync --all-extras` installs into `.venv`; run tools via `.venv/bin/<tool>` or
prefix commands with `uv run`. (Plain `uv sync` omits the `ruff`/`pytest`/
`mongomock` extra.)

MongoDB and MinIO must be running for any command that writes to storage. A
`docker compose up -d mongo minio` covers this without building the app image.

## Docker setup

The full stack can also run in containers via `docker-compose.yml` + `Dockerfile`.

```bash
docker compose up -d        # builds the app image, starts mongo + minio + app
# Dagster UI → http://localhost:3000
```

Compose starts these services:

| Service         | Role                                                                 | Started by default |
| --------------- | ------------------------------------------------------------------- | ------------------ |
| `mongo`         | MongoDB metadata store (named volume `wrc_mongo_data`).             | Yes                |
| `minio`         | MinIO object store (named volume `wrc_minio_data`).                | Yes                |
| `app`           | Project image running the Dagster webserver + daemon.              | Yes                |
| `mongo-express` | Optional Mongo browser UI (`--profile tools`).                     | No (opt-in)        |

The `app` image is built from `pyproject.toml` + `uv.lock` (`uv sync --frozen`),
runs as a non-root user, and receives all configuration from `WRC_*` environment
variables — nothing is baked in. Inside the Compose network the app reaches
storage by **service name**, not `localhost`: `docker-compose.yml` sets
`WRC_MONGO_URI=mongodb://mongo:27017` and `WRC_MINIO_ENDPOINT=minio:9000` on the
`app` service, overriding the host-oriented `.env` defaults.

Browse the data:

- **MinIO console** — http://localhost:9001 (login with `WRC_MINIO_ACCESS_KEY` /
  `WRC_MINIO_SECRET_KEY`).
- **Mongo metadata** — optional, opt-in:
  ```bash
  docker compose --profile tools up -d mongo-express   # UI → http://localhost:8081
  ```

Dagster's instance state inside the container is backed by a named volume
(`wrc_dagster_home`) rather than a host bind mount — this avoids SQLite file-lock
errors on Docker Desktop for macOS during concurrent backfills (see
`ARCHITECTURE.md` and the compose comments for detail). Reset all state with
`docker compose down -v`.

## Scrapy usage

Run the spider against a body list and date range. Dates are **ISO
`YYYY-MM-DD`**, both required and inclusive:

```bash
.venv/bin/scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-01-31 -a bodies=15376
```

Spider arguments:

| Argument     | Required | Notes                                                                        |
| ------------ | -------- | ---------------------------------------------------------------------------- |
| `start_date` | Yes      | ISO `YYYY-MM-DD`, inclusive.                                                  |
| `end_date`   | Yes      | ISO `YYYY-MM-DD`, inclusive.                                                  |
| `bodies`     | No       | Comma-separated body ids (`1`, `2`, `3`, `15376`). Defaults to all four.     |

The spider partitions the requested range (monthly by default) and crawls each
`(body, partition)` combination, paging through the listing until exhausted. Each
scraped record is written to MongoDB + MinIO through the `ITEM_PIPELINES` storage
adapter, so the storage services above must be running. Throughout a run it
tracks found/scraped/failed counts per partition and emits a final run summary
(see [Logging](#logging--observability)).

To crawl without writing to storage (e.g. a parsing smoke test):

```bash
.venv/bin/scrapy crawl wrc -a start_date=2024-01-01 -a end_date=2024-01-31 \
  -a bodies=15376 -s ITEM_PIPELINES={} -O output.json
```

## Transformation

The transformation stage reads the Landing Zone and writes a separate processed
zone. Run it over an inclusive ISO date range:

```bash
.venv/bin/wrc-transform --start-date 2024-01-01 --end-date 2024-01-31
# equivalent module form:
.venv/bin/python -m wrc_scraper.transform.cli --start-date 2024-01-01 --end-date 2024-01-31
```

What it does:

- Reads every `landing_metadata` record with `status == "stored"` whose
  `partition_date` falls in the range, and pulls each file from the landing MinIO
  bucket.
- **HTML** is cleaned with BeautifulSoup (down to the decision content subtree,
  stripped of presentational markup) and re-hashed. **PDF/DOC/DOCX binaries are
  stored unchanged.**
- When several landing records share one `identifier` (a variant cluster), a
  non-empty **HTML variant always takes precedence** over binary variants;
  otherwise the longest content wins as canonical and the rest are logged as
  dropped.
- Each transformed document is renamed to an identifier-based storage filename
  (`{body_slug}/{sanitized identifier}.{ext}`), keeping the processed bucket
  browsable and collision-safe.
- Output goes to a **separate** collection/bucket; the Landing Zone is **never
  modified**.
- Re-running the same range is **idempotent**: a transformed record already
  reflecting its source cluster's current file hashes is skipped.

Transformation writes to `WRC_MONGO_TRANSFORMED_COLLECTION` /
`WRC_MINIO_TRANSFORMED_BUCKET` (same Mongo database and MinIO endpoint as the
landing store). See `.env.example` for image-handling and variant-tie options.

## Dagster

Dagster wraps the two stages as partitioned assets:

```text
landing_documents
        ↓  (same-partition dependency)
processed_documents
```

- **Partitioning** — both assets are partitioned on `(month × body_slug)`: one
  partition per calendar month per deciding body. `processed_documents` depends on
  `landing_documents` at the *same* partition, so a transform run only reads the
  Landing Zone slice its matching crawl just wrote.
- **Ingestion** — `landing_documents` invokes `scrapy crawl wrc` as a subprocess
  (a fresh process per partition, because Scrapy's Twisted reactor cannot be
  restarted in-process), then verifies the crawl finished cleanly and surfaces its
  run summary as metadata.
- **Transformation** — `processed_documents` calls
  `TransformService.transform_range` in-process, scoped to the partition's month
  window and `body_slug`.
- **Quality checks / retries** — each asset carries a partition-level retry policy
  and a WARN-severity quality check that flags a partition whose per-record failure
  ratio exceeds a configurable threshold.

Start Dagster locally:

```bash
docker compose up -d mongo minio
export DAGSTER_HOME="$(pwd)/.dagster_home"   # applies .dagster_home/dagster.yaml
.venv/bin/dagster dev                        # UI → http://localhost:3000
```

`workspace.yaml` points Dagster at `wrc_scraper.orchestration.definitions`. From
the UI you can materialize a single `(month, body_slug)` partition or launch a
backfill across many; the daemon (started automatically by `dagster dev`) is
required for backfills to execute. Setting `DAGSTER_HOME` ensures the committed
`.dagster_home/dagster.yaml` (run retries, run coordinator) takes effect rather
than an ephemeral instance.

The module also defines three named jobs for targeting: `wrc_pipeline` (both
stages), `landing` (crawl only), and `process` (transform only). Multi-partition
keys are formatted `"{body_slug}|{YYYY-MM-01}"`, e.g. `"wrc|2024-01-01"`, which is
the form to use when materializing from the CLI:

```bash
.venv/bin/dagster asset materialize \
  -m wrc_scraper.orchestration.definitions \
  --select "landing_documents,processed_documents" --partition "wrc|2024-01-01"
```

## Testing

```bash
.venv/bin/pytest
```

The suite covers:

- **Unit tests** — configuration, validation, partitioning, hashing, keys.
- **Scrapy behavior** — spider parsing, pipelines, middlewares, partition tracking.
- **Idempotency** — the ingest state machine skips unchanged content.
- **Failure handling** — per-record failures are counted, not fatal.
- **MongoDB / MinIO integration** — `tests/storage/test_integration.py` exercises
  the real storage layer and auto-skips when the Compose services aren't running.
- **Transformation** — HTML cleaning, canonical variant selection, CLI, end-to-end.
- **Dagster** — partition invariants, asset dependency, ingestion/transform assets.

The full suite currently reports **245 passing** tests.

## Code quality

Linting and formatting use Ruff (configured in `pyproject.toml`):

```bash
ruff check .
ruff format --check .
```

Both currently pass cleanly across the project.

## Data and storage behavior

- **Deterministic identity** — Landing Zone records are keyed on
  `(body_slug, detail_url)`, so a re-crawl targets the same Mongo `_id` and MinIO
  object key rather than creating duplicates.
- **SHA-256 content hashing** — a `file_hash` decides whether stored content
  actually changed, independent of identity.
- **Idempotent reruns** — a matching hash with the object still present skips the
  write entirely; only new or changed content is re-stored.
- **Conditional download** — unchanged binary documents are skipped at the
  *download* level via an ETag-based conditional GET (HTML pages carry no
  validator, so only their write can be skipped).
- **Immutable Landing Zone** — transformation reads the landing store and writes
  a separate processed zone; it never modifies landing data.
- **Identifier-based transformed filenames** — transformed objects are renamed to
  a sanitized, collision-resistant `identifier` key, while the original
  `identifier` in metadata is left unchanged.

Detailed rationale for these guarantees is in [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Logging / observability

The pipeline emits structured JSON-lines events on a dedicated logger, kept
separate from Scrapy's own diagnostic output so each line is parseable JSON. The
level is controlled by `WRC_LOG_LEVEL`; setting `WRC_EVENTS_LOG_FILE` writes
events to a file instead of stdout.

Events include partition lifecycle markers (`partition_started`,
`partition_completed`), per-record failures (`record_failed`), and a final
`run_summary` carrying run-level counts:

- partition / body being processed;
- records found;
- records scraped;
- failed downloads;
- unaccounted / incomplete records;
- completed vs. incomplete partitions and the crawl finish reason.

Dagster additionally surfaces these counts as asset metadata and quality-check
results in the UI.

## Assessment-specific implementation notes

Engineering decisions worth calling out for a reviewer (full reasoning in
`ARCHITECTURE.md`):

- **Date partitions** — chosen because granularity does not change *what* is
  scraped (measured), only how work is chunked; monthly matches the site's URL
  structure and keeps partition counts and near-empty buckets manageable.
- **Identity on `detail_url`** — the site's `identifier` was measured to collide
  across corpora; `detail_url` (with `body_slug`) is the reliable identity key and
  survives the site renumbering a body.
- **Conditional GET for binaries** — WRC document endpoints return a stable ETag,
  so unchanged PDF/DOC/DOCX files skip the download; it's a pure optimization
  (falls back to a full GET + hash whenever it can't be applied safely).
- **HTML preferred over binary variants** — within a variant cluster, a non-empty
  cleaned HTML page is the most useful canonical copy for a text corpus.
- **Immutable Landing Zone** — keeping raw captures untouched makes the
  transformation stage safe to re-run and re-tune without re-crawling.
