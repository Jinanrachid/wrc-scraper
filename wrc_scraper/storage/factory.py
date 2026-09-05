"""Environment-driven construction of the Mongo/MinIO repositories.

Both the item pipeline (which writes) and the spider's conditional-GET
advisor (which reads) need the same clients pointed at the same store. This
module is the single place that translates `WRC_*` environment variables into
repositories, so the two can never drift apart into looking at different
databases or buckets (CLAUDE.md: no hardcoded configuration, no duplicated
logic).

Client construction is kept in functions rather than done at import time so
nothing here requires pymongo/minio to be installed just to import the
package -- the heavy imports stay local, as in the repositories themselves.
"""

from __future__ import annotations

import dataclasses
from typing import Any

from wrc_scraper.config import TransformSettings, env_bool, env_str
from wrc_scraper.storage.minio_repository import MinioRepository
from wrc_scraper.storage.mongo_repository import MongoRepository


@dataclasses.dataclass(frozen=True)
class StorageSettings:
    mongo_uri: str
    mongo_database: str
    mongo_collection: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_bucket: str
    minio_secure: bool

    @classmethod
    def from_env(cls) -> StorageSettings:
        return cls(
            mongo_uri=env_str("WRC_MONGO_URI", "mongodb://localhost:27017"),
            mongo_database=env_str("WRC_MONGO_DATABASE", "wrc"),
            mongo_collection=env_str("WRC_MONGO_COLLECTION", "landing_metadata"),
            minio_endpoint=env_str("WRC_MINIO_ENDPOINT", "localhost:9000"),
            minio_access_key=env_str("WRC_MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=env_str("WRC_MINIO_SECRET_KEY", "minioadmin"),
            minio_bucket=env_str("WRC_MINIO_BUCKET", "wrc-landing"),
            minio_secure=env_bool("WRC_MINIO_SECURE", False),
        )


def _mongo_client(settings: StorageSettings) -> Any:
    import pymongo  # noqa: PLC0415 -- optional/heavy import kept local

    return pymongo.MongoClient(settings.mongo_uri)


def _minio_client(settings: StorageSettings) -> Any:
    from minio import Minio  # noqa: PLC0415 -- optional/heavy import kept local

    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def build_mongo(settings: StorageSettings) -> tuple[Any, MongoRepository]:
    """Return (client, repository) for the Landing Zone collection. The client
    is returned separately because only the caller knows when the run is over
    and it should be closed.
    """
    client = _mongo_client(settings)
    return client, MongoRepository(client, settings.mongo_database, settings.mongo_collection)


def build_minio(settings: StorageSettings) -> MinioRepository:
    """Return a repository for the Landing Zone bucket."""
    return MinioRepository(_minio_client(settings), settings.minio_bucket)


def build_transformed_mongo(
    settings: StorageSettings, transform: TransformSettings
) -> tuple[Any, MongoRepository]:
    """Return (client, repository) for the Phase 4 transformed-metadata
    collection -- the same Mongo cluster/database as the Landing Zone, a
    different collection (`transform.mongo_transformed_collection`). Same
    `MongoRepository` class as the landing side (CLAUDE.md: don't fork the
    repository classes), just pointed elsewhere.
    """
    client = _mongo_client(settings)
    return client, MongoRepository(
        client, settings.mongo_database, transform.mongo_transformed_collection
    )


def build_transformed_minio(
    settings: StorageSettings, transform: TransformSettings
) -> MinioRepository:
    """Return a repository for the Phase 4 transformed-documents bucket -- the
    same MinIO endpoint/credentials, a different bucket
    (`transform.minio_transformed_bucket`).
    """
    return MinioRepository(_minio_client(settings), transform.minio_transformed_bucket)
