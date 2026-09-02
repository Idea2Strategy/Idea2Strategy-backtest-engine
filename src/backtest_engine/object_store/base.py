"""The single `ObjectStore` protocol both adapters implement, and its value types.

The contract is deliberately narrow and identical for the local filesystem and for
S3-compatible storage:

* `put` is **immutable**. Writing identical bytes to an existing key is idempotent and
  reports `reconciled=True`; writing *different* bytes to an existing key raises
  `ObjectStoreConflict`. Nothing in this package overwrites an object.
* every object carries its SHA-256 in provider metadata, and `put` verifies the stored
  object before returning, so a receipt is evidence rather than a hope.
* the receipt carries exactly the columns `storage.objects` needs to identify an
  object: provider, bucket, key, provider version, hash and size.

`put` takes bytes rather than a source path (the sibling pipeline's store takes a
path) because the backtest engine produces Parquet in memory: writing a temporary file
purely to hand it to the store would add a filesystem round-trip and a second failure
mode to every publish.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import BinaryIO, Protocol, runtime_checkable

from .errors import ObjectKeyError


__all__ = [
    "PARQUET_MEDIA_TYPE",
    "ObjectReceipt",
    "ObjectStore",
    "Sleeper",
    "VerificationResult",
    "normalize_object_key",
    "sha256_bytes",
]


PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"

#: Injected so backoff is deterministic and tests never sleep for real.
Sleeper = Callable[[float], None]

_DRIVE = re.compile(r"^[A-Za-z]:")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalize_object_key(object_key: str) -> str:
    """Return a store-relative POSIX key, or raise `ObjectKeyError`.

    This is the path-traversal gate. It runs for *both* adapters: an S3 key containing
    `..` is not a traversal on S3 itself, but it is one the moment the same key is
    mirrored to a local cache or restored to disk, and the two adapters must accept
    exactly the same key set or the contract tests are meaningless.
    """

    if not isinstance(object_key, str) or not object_key.strip():
        raise ObjectKeyError("object_key must be a non-empty string")
    if "\x00" in object_key:
        raise ObjectKeyError("object_key must not contain a NUL byte")
    if ":" in object_key:
        # Drive letters and NTFS alternate data streams (`name.parquet:stream`).
        raise ObjectKeyError(f"object_key must not contain ':': {object_key!r}")
    if _DRIVE.match(object_key):  # pragma: no cover - already rejected by the ':' guard
        raise ObjectKeyError(f"object_key must be relative, got {object_key!r}")
    text = object_key.replace("\\", "/")
    if text.startswith("/"):
        raise ObjectKeyError(f"object_key must be relative, got {object_key!r}")
    segments = text.split("/")
    for segment in segments:
        if segment in ("", ".", ".."):
            raise ObjectKeyError(f"object_key must not contain empty or relative segments: {object_key!r}")
        if segment != segment.strip():
            raise ObjectKeyError(f"object_key segments must not be padded with whitespace: {object_key!r}")
    return "/".join(segments)


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Outcome of checking one stored object against an expected SHA-256."""

    ok: bool
    content_hash: str
    byte_size: int
    message: str = ""
    deep: bool = False


@dataclass(frozen=True, slots=True)
class ObjectReceipt:
    """Evidence that one object is stored, with the identity `storage.objects` needs.

    `reconciled` is `True` when the object was already present with identical bytes,
    which covers both an idempotent re-publish and a 412 race with another writer.
    """

    storage_provider: str
    bucket_name: str
    object_key: str
    provider_version_id: str
    content_hash: str
    byte_size: int
    etag: str | None = None
    local_path: str | None = None
    reconciled: bool = False


@runtime_checkable
class ObjectStore(Protocol):
    """One contract, satisfied identically by the local and the S3 adapter."""

    @property
    def storage_provider(self) -> str:
        """`storage.objects.storage_provider`: `LOCAL` or `S3_COMPATIBLE`."""
        ...

    @property
    def bucket_name(self) -> str:
        """`storage.objects.bucket_name`, which is NOT NULL even for the local store."""
        ...

    def put(self, object_key: str, data: bytes) -> ObjectReceipt: ...

    def delete_if_matches(self, object_key: str, expected_sha256: str) -> bool: ...

    def exists(self, object_key: str) -> bool: ...

    def open(self, object_key: str) -> BinaryIO: ...

    def metadata(self, object_key: str) -> Mapping[str, str]: ...

    def verify(self, object_key: str, expected_sha256: str, *, deep: bool = False) -> VerificationResult: ...
