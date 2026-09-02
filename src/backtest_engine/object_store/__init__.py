"""Immutable object storage for backtest result details (spec 2.5, cards D03/D27).

One `ObjectStore` protocol, two adapters — `LocalObjectStore` and `S3ObjectStore` —
that satisfy the same key, checksum and metadata contract and are exercised by the
same contract tests. Keys are built and parsed only through `BacktestObjectKey`.
Every stored object converts to a complete `storage.objects` row through
`StorageObjectRecord`; see `registration` for why this package does not insert it.
"""

from __future__ import annotations

from .base import (
    PARQUET_MEDIA_TYPE,
    ObjectReceipt,
    ObjectStore,
    Sleeper,
    VerificationResult,
    normalize_object_key,
    sha256_bytes,
)
from .errors import (
    ObjectKeyError,
    ObjectStoreConflict,
    ObjectStoreError,
    ObjectVerificationError,
    StorageWriteNotAuthorized,
)
from .keys import BACKTEST_RESULT_PREFIX, MAX_PART_NUMBER, PARQUET_SUFFIX, BacktestObjectKey
from .local import LocalObjectStore
from .paths import long_path, short_temp_path
from .registration import (
    PARQUET_FILE_FORMAT,
    RETENTION_POLICY_VERSION,
    UNCOMPRESSED_CODEC,
    InMemoryStorageObjectRegistry,
    ObjectStatus,
    RegisteredObject,
    StorageObjectProducerClaim,
    StorageObjectRecord,
    StorageObjectRegistrar,
    StorageObjectRegistration,
    StorageObjectUpload,
    StorageObjectWritePort,
    UnauthorizedStorageObjectWritePort,
)
from .s3 import RETRYABLE_ERROR_CODES, S3ObjectStore


__all__ = [
    "BACKTEST_RESULT_PREFIX",
    "MAX_PART_NUMBER",
    "PARQUET_FILE_FORMAT",
    "PARQUET_MEDIA_TYPE",
    "PARQUET_SUFFIX",
    "RETENTION_POLICY_VERSION",
    "RETRYABLE_ERROR_CODES",
    "UNCOMPRESSED_CODEC",
    "BacktestObjectKey",
    "InMemoryStorageObjectRegistry",
    "LocalObjectStore",
    "ObjectKeyError",
    "ObjectReceipt",
    "ObjectStatus",
    "ObjectStore",
    "ObjectStoreConflict",
    "ObjectStoreError",
    "ObjectVerificationError",
    "RegisteredObject",
    "S3ObjectStore",
    "Sleeper",
    "StorageObjectProducerClaim",
    "StorageObjectRecord",
    "StorageObjectRegistrar",
    "StorageObjectRegistration",
    "StorageObjectUpload",
    "StorageObjectWritePort",
    "StorageWriteNotAuthorized",
    "UnauthorizedStorageObjectWritePort",
    "VerificationResult",
    "long_path",
    "normalize_object_key",
    "sha256_bytes",
    "short_temp_path",
]
