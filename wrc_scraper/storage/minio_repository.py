"""Thin MinIO client wrapper (Phase 3, Decision 3).

Deliberately thin, mirroring mongo_repository.py: one method per operation,
no business logic. IngestService depends on this class's public interface
(duck-typed -- see storage/ingest_service.py's MinioPort) so it can be
swapped for an in-memory fake in unit tests without a live MinIO server.
"""

from __future__ import annotations

import io
from typing import Any


class MinioRepository:
    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def ensure_bucket(self) -> None:
        if not self._client.bucket_exists(self._bucket):
            self._client.make_bucket(self._bucket)

    def object_exists(self, key: str) -> bool:
        from minio.error import S3Error  # noqa: PLC0415 -- optional/heavy import kept local

        try:
            self._client.stat_object(self._bucket, key)
            return True
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject"):
                return False
            raise

    def put_object(self, key: str, data: bytes, content_type: str) -> None:
        self._client.put_object(
            self._bucket,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
