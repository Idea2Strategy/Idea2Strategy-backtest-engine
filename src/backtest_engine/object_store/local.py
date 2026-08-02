"""Immutable objects on a local filesystem, one root per store.

Used by tests, by single-node runs and as the offline fallback when no S3 endpoint is
configured. It satisfies the same `ObjectStore` contract as `S3ObjectStore`, including
the metadata contract: the local store is content-addressed, so the SHA-256 is
recomputed from the bytes on disk instead of being read back from provider metadata.
That is strictly stronger than S3's shallow metadata check, never weaker.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from pathlib import Path
from typing import BinaryIO

from .base import (
    PARQUET_MEDIA_TYPE,
    ObjectReceipt,
    VerificationResult,
    normalize_object_key,
    sha256_bytes,
)
from .errors import ObjectStoreConflict, ObjectVerificationError
from .paths import long_path, short_temp_path


__all__ = ["LocalObjectStore"]

_READ_CHUNK = 1024 * 1024


class LocalObjectStore:
    """Publish immutable objects atomically beneath one local root."""

    storage_provider = "LOCAL"

    def __init__(self, root: Path | str, *, bucket_name: str | None = None) -> None:
        self.root = Path(root).expanduser().resolve()
        # `storage.objects.bucket_name` is NOT NULL. A local store still has to name
        # the container it published into; the root directory name is that name unless
        # the caller supplies a logical one.
        name = bucket_name if bucket_name is not None else self.root.name
        if not name or not name.strip():
            raise ValueError("bucket_name must be a non-empty string")
        self._bucket_name = name

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def path_for(self, object_key: str) -> Path:
        """Resolve a key beneath the root, refusing anything that escapes it."""

        normalized = normalize_object_key(object_key)
        candidate = Path(os.path.normpath(self.root / normalized))
        # Containment is checked on the plain path, before switching to the
        # extended-length form used for the actual filesystem calls.
        try:
            candidate.relative_to(self.root)
        except ValueError as exc:  # pragma: no cover - normalize_object_key catches these first
            raise ObjectStoreConflict(f"object_key escapes the store root: {object_key!r}") from exc
        return Path(long_path(candidate))

    def put(self, object_key: str, data: bytes) -> ObjectReceipt:
        if not isinstance(data, bytes):
            raise TypeError(f"object body must be bytes, got {type(data).__name__}")
        content_hash = sha256_bytes(data)
        destination = self.path_for(object_key)

        if destination.exists():
            stored = self.verify(object_key, content_hash, deep=True)
            if not stored.ok:
                raise ObjectStoreConflict(
                    f"immutable object key already holds different bytes: {object_key} "
                    f"(stored {stored.content_hash or 'unreadable'}, offered {content_hash})"
                )
            return self._receipt(object_key, destination, content_hash, len(data), reconciled=True)

        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = short_temp_path(destination)
        try:
            staging.write_bytes(data)
            if _sha256_file(staging) != content_hash:  # pragma: no cover - torn local write
                raise ObjectVerificationError(f"SHA-256 changed while staging {object_key}")
            staging.replace(destination)
        finally:
            staging.unlink(missing_ok=True)

        written = self.verify(object_key, content_hash, deep=True)
        if not written.ok:  # pragma: no cover - torn local write
            raise ObjectVerificationError(
                f"stored object does not match after write: {object_key} ({written.message})"
            )
        return self._receipt(object_key, destination, content_hash, len(data), reconciled=False)

    def exists(self, object_key: str) -> bool:
        return self.path_for(object_key).is_file()

    def open(self, object_key: str) -> BinaryIO:
        return self.path_for(object_key).open("rb")

    def metadata(self, object_key: str) -> Mapping[str, str]:
        path = self.path_for(object_key)
        if not path.is_file():
            raise FileNotFoundError(f"object not found: {object_key}")
        return {
            "sha256": _sha256_file(path),
            "content_type": PARQUET_MEDIA_TYPE,
            "byte_size": str(path.stat().st_size),
        }

    def verify(self, object_key: str, expected_sha256: str, *, deep: bool = False) -> VerificationResult:
        """Re-hash the bytes on disk. `deep` changes nothing here; local is always deep."""

        path = self.path_for(object_key)
        if not path.is_file():
            return VerificationResult(False, "", 0, f"object missing: {object_key}", deep=True)
        actual = _sha256_file(path)
        matched = actual == expected_sha256
        return VerificationResult(
            matched,
            actual,
            path.stat().st_size,
            "" if matched else f"sha256 mismatch: stored {actual}, expected {expected_sha256}",
            deep=True,
        )

    def _receipt(
        self, object_key: str, destination: Path, content_hash: str, byte_size: int, *, reconciled: bool
    ) -> ObjectReceipt:
        return ObjectReceipt(
            storage_provider=self.storage_provider,
            bucket_name=self._bucket_name,
            object_key=normalize_object_key(object_key),
            # There is no provider-side version on a filesystem, and the column is NOT
            # NULL. The content hash is the only honest version identifier: the store
            # is content-addressed and immutable, so it names exactly these bytes.
            provider_version_id=content_hash,
            content_hash=content_hash,
            byte_size=byte_size,
            local_path=str(destination),
            reconciled=reconciled,
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()
