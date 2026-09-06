# syntax=docker/dockerfile:1
#
# Runtime image for the WRC scraper/pipeline (Scrapy -> Mongo/MinIO -> Transform
# -> Mongo/MinIO -> Dagster orchestration). This image only provides the
# runtime environment for that existing architecture; it does not change it.
#
# Dependencies are installed from the project's existing uv-managed
# configuration (pyproject.toml + uv.lock) via `uv sync --frozen`, which
# reproduces exactly what `uv sync` on the host installs by default:
# the base runtime deps (scrapy, pymongo, minio, beautifulsoup4, lxml) plus the
# `dev` *dependency group* (dagster, dagster-webserver) -- required so the
# same image can load the Dagster definitions (`workspace.yaml`). This is
# distinct from the `dev` *extra* (ruff/pytest/mongomock, project.optional-
# dependencies.dev), which stays out of this image since it's test/lint
# tooling, not part of the running application.
#
# All operational configuration (Mongo/MinIO connection info, scraping
# parameters, etc.) continues to come from the environment at container run
# time (see .env.example) -- nothing is hardcoded here.

FROM python:3.12-slim

# Pin uv to the version used to generate uv.lock in this repo, by copying the
# static binary out of astral's official distroless image (recommended
# installation method: https://docs.astral.sh/uv/guides/integration/docker/).
COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

# Container-appropriate Python behavior: no .pyc files, unbuffered stdout/stderr
# (so logs -- including this project's structured JSONL events -- appear
# immediately rather than being buffered).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}" \
    # Default for the env-substituted max_concurrent_runs in
    # .dagster_home/dagster.yaml. Dagster's env substitution has no inline YAML
    # default, so bake the committed default here; docker-compose.yml and .env
    # override it. This keeps `docker run ...` (without Compose/.env) working.
    WRC_DAGSTER_MAX_CONCURRENT_RUNS=4

WORKDIR /app

# Install dependencies first, in their own layer, from the lockfile alone --
# this layer is only invalidated when pyproject.toml/uv.lock change, not on
# every source edit (Docker layer caching).
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

# Now copy the application source itself (filtered by .dockerignore: no .git,
# caches, egg-info, virtualenvs, local .env files, tests, docs, etc.).
COPY . /app

# Install the project (editable-off is unnecessary here -- single-stage image,
# so there's no separate "builder" copy step to strip the source back out of).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# Run as a non-root user.
RUN groupadd --system --gid 1000 wrc \
    && useradd --system --uid 1000 --gid wrc --create-home --home-dir /home/wrc wrc \
    && chown -R wrc:wrc /app
USER wrc

# Default: bring up the Dagster webserver + daemon against the workspace
# defined in workspace.yaml (loads wrc_scraper.orchestration.definitions),
# matching how this project is already run (`dagster dev`). Override `command`
# to instead run a crawl (`scrapy crawl wrc -a ...`) or the transform CLI
# (`wrc-transform --start-date ... --end-date ...`) in the same image.
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000"]
