"""`storage.objects` registration: the row value, the write port, and the registrar.

## What this module produces

`StorageObjectRecord` is the fully populated value for one `storage.objects` row
(`db/schema.dbml`). It is a value object, not a repository: it validates every NOT NULL
column, carries the object's lifecycle status, and converts to the canonical
`StorageObjectRow` dataclass the persistence layer already defines.

`StorageObjectRegistrar` is the one place a backtest result object is published:

1. write the bytes through an `ObjectStore`,
2. register **exactly one** `storage.objects` row, in `STAGED`,
3. re-verify the stored bytes (deep: the body is re-hashed, not the metadata),
4. promote that same row to `AVAILABLE` — and only then.

The status lifecycle is the canonical one: an object is `STAGED` when the bytes are in
the store and becomes `AVAILABLE` **only** once its checksum has been verified — the
table note says exactly that ("크기·해시·스키마·Parquet footer·코덱 검증 후에만
AVAILABLE"). `StorageObjectRecord` refuses to be constructed as `AVAILABLE` without a
`verified_at`. A failed verification does not roll the row back and does not raise
before it is recorded: the row goes to `QUARANTINED`, so a bad object is *evidence*
rather than a silent gap, and the caller still gets an exception.

## Why this module does not talk to Postgres

`storage` ownership is currently contradictory:

* `backend/.../DatabaseAccessPolicy.java:36` registers the `storage` schema as
  `SHARED`, and
* the backend implementation checklist calls `storage` a D-owned schema.

`src/backtest_engine/persistence/repositories.py` resolved that for reads by shipping
`StorageObjectReader` with no write method at all, and
`persistence.engine.install_runtime_guards` rejects a write statement against the
schema if one is attempted. So the registrar depends on a narrow port,
`StorageObjectWritePort`, defined here rather than in the persistence package:

* `InMemoryStorageObjectRegistry` is the process-local binding. It is a real
  implementation of the canonical rules (one row per object, idempotent re-register,
  conflict on divergent content, no `AVAILABLE` without verification) and is what a
  single-node run and the tests use.
* `UnauthorizedStorageObjectWritePort` is the binding to use where a *durable* row is
  required today. It fails closed and names the contradiction instead of issuing an
  unauthorised INSERT.

When ownership is settled, a Postgres-backed `StorageObjectWritePort` is added in the
persistence package and one binding changes; no call site does. Nothing here authors
`storage` DDL.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Protocol
from uuid import UUID

from backtest_engine.persistence.rows import ObjectStatus, StorageObjectRow

from .base import PARQUET_MEDIA_TYPE, ObjectReceipt, ObjectStore
from .errors import ObjectStoreConflict, ObjectVerificationError, StorageWriteNotAuthorized


__all__ = [
    "PARQUET_FILE_FORMAT",
    "RETENTION_POLICY_VERSION",
    "UNCOMPRESSED_CODEC",
    "InMemoryStorageObjectRegistry",
    "ObjectStatus",
    "RegisteredObject",
    "StorageObjectRecord",
    "StorageObjectRegistrar",
    "StorageObjectWritePort",
    "UnauthorizedStorageObjectWritePort",
]


#: `storage.objects.file_format`.
PARQUET_FILE_FORMAT = "PARQUET"

#: `storage.objects.compression_codec`. The canonical note mandates explicit
#: UNCOMPRESSED for Parquet objects, and `backtest.detail_manifests` repeats it.
UNCOMPRESSED_CODEC = "UNCOMPRESSED"

#: `storage.objects.retention_policy_version` is NOT NULL and has no default. Backtest
#: detail objects are evidence for an immutable official result, so they follow the
#: result's own retention rules; this is the version identifier of that rule set.
RETENTION_POLICY_VERSION = "retention:backtest-detail:1.0.0"

#: A verified object keeps its `verified_at` for the rest of its life, including after
#: a later revision supersedes it.
_VERIFIED_STATUSES = frozenset({ObjectStatus.AVAILABLE, ObjectStatus.SUPERSEDED})
#: These states mean "not readable as canonical evidence", so `verified_at` is absent.
_UNVERIFIED_STATUSES = frozenset({ObjectStatus.STAGED, ObjectStatus.QUARANTINED})


def _required(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} is NOT NULL in storage.objects and must not be blank")
    return value


@dataclass(frozen=True, slots=True)
class StorageObjectRecord:
    """One `storage.objects` row, complete and validated, in a lifecycle state."""

    object_id: UUID
    status: ObjectStatus
    storage_provider: str
    bucket_name: str
    object_key: str
    provider_version_id: str
    content_hash: str
    byte_size: int
    file_format: str
    compression_codec: str
    media_type: str
    schema_version: str
    row_count: int
    period_start: datetime
    period_end: datetime
    retention_policy_version: str
    created_at: datetime
    verified_at: datetime | None = None
    encryption_key_ref: str | None = None
    retention_until: datetime | None = None
    legal_hold: bool = False
    quarantined_at: datetime | None = None
    superseded_at: datetime | None = None
    deleted_at: datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.object_id, UUID):
            raise ValueError(f"object_id must be a UUID, got {self.object_id!r}")
        if not isinstance(self.status, ObjectStatus):
            raise ValueError(f"status must be an ObjectStatus, got {self.status!r}")
        for label in (
            "storage_provider",
            "bucket_name",
            "object_key",
            "provider_version_id",
            "content_hash",
            "file_format",
            "compression_codec",
            "media_type",
            "schema_version",
            "retention_policy_version",
        ):
            _required(getattr(self, label), label)
        for label in ("byte_size", "row_count"):
            value = getattr(self, label)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{label} must be a non-negative integer, got {value!r}")
        for label in ("period_start", "period_end", "created_at"):
            value = getattr(self, label)
            if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{label} must be a timezone-aware datetime, got {value!r}")
        if self.period_end < self.period_start:
            raise ValueError("period_end must not precede period_start")
        if self.status in _VERIFIED_STATUSES and self.verified_at is None:
            raise ValueError(
                f"storage.objects becomes {self.status.value} only after verification, "
                "so verified_at must be set"
            )
        if self.status in _UNVERIFIED_STATUSES and self.verified_at is not None:
            raise ValueError(
                f"a {self.status.value} storage object must not carry verified_at; "
                "it is not AVAILABLE"
            )

    @classmethod
    def staged(
        cls,
        *,
        object_id: UUID,
        receipt: ObjectReceipt,
        schema_version: str,
        row_count: int,
        period_start: datetime,
        period_end: datetime,
        created_at: datetime,
        media_type: str = PARQUET_MEDIA_TYPE,
        file_format: str = PARQUET_FILE_FORMAT,
        compression_codec: str = UNCOMPRESSED_CODEC,
        retention_policy_version: str = RETENTION_POLICY_VERSION,
        encryption_key_ref: str | None = None,
        retention_until: datetime | None = None,
        legal_hold: bool = False,
    ) -> StorageObjectRecord:
        """Build the STAGED row for an object the store has just acknowledged."""

        return cls(
            object_id=object_id,
            status=ObjectStatus.STAGED,
            storage_provider=receipt.storage_provider,
            bucket_name=receipt.bucket_name,
            object_key=receipt.object_key,
            provider_version_id=receipt.provider_version_id,
            content_hash=receipt.content_hash,
            byte_size=receipt.byte_size,
            file_format=file_format,
            compression_codec=compression_codec,
            media_type=media_type,
            schema_version=schema_version,
            row_count=row_count,
            period_start=period_start,
            period_end=period_end,
            retention_policy_version=retention_policy_version,
            created_at=created_at,
            verified_at=None,
            encryption_key_ref=encryption_key_ref,
            retention_until=retention_until,
            legal_hold=legal_hold,
        )

    def verified(self, verified_at: datetime) -> StorageObjectRecord:
        """Promote a STAGED object to AVAILABLE after its checksum was re-checked."""

        if self.status is not ObjectStatus.STAGED:
            raise ValueError(f"only a STAGED object can become AVAILABLE, this one is {self.status.value}")
        return replace(self, status=ObjectStatus.AVAILABLE, verified_at=verified_at)

    def superseded(self, superseded_at: datetime) -> StorageObjectRecord:
        """A later revision replaced this object. The bytes are not overwritten."""

        if self.status is not ObjectStatus.AVAILABLE:
            raise ValueError(f"only a verified AVAILABLE object can be superseded, this one is {self.status.value}")
        return replace(self, status=ObjectStatus.SUPERSEDED, superseded_at=superseded_at)

    def quarantined(self, quarantined_at: datetime) -> StorageObjectRecord:
        """Verification failed after publication; the object must not be read."""

        return replace(
            self,
            status=ObjectStatus.QUARANTINED,
            verified_at=None,
            quarantined_at=quarantined_at,
        )

    def to_row(self) -> StorageObjectRow:
        """The canonical `storage.objects` row, every NOT NULL column populated."""

        return StorageObjectRow(
            id=self.object_id,
            status=self.status,
            storage_provider=self.storage_provider,
            bucket_name=self.bucket_name,
            object_key=self.object_key,
            provider_version_id=self.provider_version_id,
            content_hash=self.content_hash,
            byte_size=self.byte_size,
            file_format=self.file_format,
            compression_codec=self.compression_codec,
            media_type=self.media_type,
            schema_version=self.schema_version,
            row_count=self.row_count,
            period_start=self.period_start,
            period_end=self.period_end,
            encryption_key_ref=self.encryption_key_ref,
            retention_policy_version=self.retention_policy_version,
            retention_until=self.retention_until,
            legal_hold=self.legal_hold,
            created_at=self.created_at,
            verified_at=self.verified_at,
            quarantined_at=self.quarantined_at,
            superseded_at=self.superseded_at,
            deleted_at=self.deleted_at,
        )


class StorageObjectWritePort(Protocol):
    """The narrow seam an authorised `storage.objects` writer plugs into.

    Four operations, matching the canonical lifecycle exactly:

    * `register` inserts the `STAGED` row and is idempotent on `object_id`. Offering a
      *different* object under an `object_id` that already exists is a conflict, never
      an update — `storage.objects` rows are immutable identity.
    * `mark_available` is the only transition that produces `AVAILABLE`, and it takes
      the verification time it is being granted for.
    * `quarantine` records a verification failure against the row that already exists.
    * `find` reads one row back, so a caller can prove the row is there.

    Implementing this against Postgres is a schema-ownership decision (see the module
    docstring), not a code change at any call site.
    """

    def register(self, record: StorageObjectRecord) -> UUID: ...

    def mark_available(self, object_id: UUID, verified_at: datetime) -> StorageObjectRecord: ...

    def quarantine(self, object_id: UUID, quarantined_at: datetime) -> StorageObjectRecord: ...

    def find(self, object_id: UUID) -> StorageObjectRecord | None: ...


class UnauthorizedStorageObjectWritePort:
    """The binding for "a durable row is required": refuse, loudly, with the reason.

    This is not a stub that quietly succeeds. Writing `storage.objects` from this
    repository today would be an unauthorised write against a schema that
    `DatabaseAccessPolicy` marks SHARED, so the port fails closed and names the
    contradiction that has to be resolved first.
    """

    reason = (
        "storage.objects write is not authorised from backtest-engine: "
        "DatabaseAccessPolicy.java:36 registers the `storage` schema as SHARED while the "
        "backend implementation checklist calls it D-owned. "
        "persistence.StorageObjectReader is read-only for the same reason and "
        "persistence.engine.install_runtime_guards rejects the statement. "
        "Resolve the ownership contradiction, then bind an authorised StorageObjectWritePort."
    )

    def register(self, record: StorageObjectRecord) -> UUID:
        raise StorageWriteNotAuthorized(f"{self.reason} (offered object_key={record.object_key})")

    def mark_available(self, object_id: UUID, verified_at: datetime) -> StorageObjectRecord:
        raise StorageWriteNotAuthorized(f"{self.reason} (offered object_id={object_id})")

    def quarantine(self, object_id: UUID, quarantined_at: datetime) -> StorageObjectRecord:
        raise StorageWriteNotAuthorized(f"{self.reason} (offered object_id={object_id})")

    def find(self, object_id: UUID) -> StorageObjectRecord | None:
        raise StorageWriteNotAuthorized(f"{self.reason} (offered object_id={object_id})")


#: The columns that make two offers of the same `object_id` the same object. Everything
#: else about a row (its status and its timestamps) is lifecycle, not identity.
_IDENTITY_COLUMNS = (
    "storage_provider",
    "bucket_name",
    "object_key",
    "provider_version_id",
    "content_hash",
    "byte_size",
    "file_format",
    "compression_codec",
    "media_type",
    "schema_version",
    "row_count",
)


class InMemoryStorageObjectRegistry:
    """A process-local `storage.objects` table that enforces the canonical rules.

    Used by single-node runs and by tests. It is not a mock: re-registering the same
    object returns the existing row instead of adding a second one, registering a
    different object under a taken `object_id` (or a taken key) is a conflict, and
    `AVAILABLE` is reachable only from `STAGED` via `mark_available`.
    """

    def __init__(self) -> None:
        self._rows: dict[UUID, StorageObjectRecord] = {}
        self._object_id_by_key: dict[tuple[str, str, str], UUID] = {}
        self._lock = threading.RLock()
        #: Number of `register` calls, including the idempotent ones. Distinct from
        #: `len(rows())`, which is the number of rows those calls produced.
        self.register_calls = 0

    @staticmethod
    def _key(record: StorageObjectRecord) -> tuple[str, str, str]:
        return (record.storage_provider, record.bucket_name, record.object_key)

    @staticmethod
    def _same_object(left: StorageObjectRecord, right: StorageObjectRecord) -> bool:
        return all(getattr(left, column) == getattr(right, column) for column in _IDENTITY_COLUMNS)

    def register(self, record: StorageObjectRecord) -> UUID:
        if not isinstance(record, StorageObjectRecord):
            raise TypeError(f"record must be a StorageObjectRecord, got {type(record).__name__}")
        if record.status is not ObjectStatus.STAGED:
            raise ObjectStoreConflict(
                f"a storage.objects row is inserted as {ObjectStatus.STAGED.value}, "
                f"not {record.status.value}: {record.object_key}"
            )
        with self._lock:
            self.register_calls += 1
            key = self._key(record)
            existing = self._rows.get(record.object_id)
            if existing is not None:
                if not self._same_object(existing, record):
                    raise ObjectStoreConflict(
                        f"storage.objects row {record.object_id} already describes a different object "
                        f"({existing.object_key} != {record.object_key})"
                    )
                return existing.object_id
            taken = self._object_id_by_key.get(key)
            if taken is not None:
                raise ObjectStoreConflict(
                    f"object key is already registered under {taken}: {record.object_key}"
                )
            self._rows[record.object_id] = record
            self._object_id_by_key[key] = record.object_id
            return record.object_id

    def mark_available(self, object_id: UUID, verified_at: datetime) -> StorageObjectRecord:
        with self._lock:
            current = self._require(object_id)
            if current.status is ObjectStatus.AVAILABLE:
                if current.verified_at != verified_at:
                    raise ObjectStoreConflict(
                        f"storage.objects row {object_id} was already verified at {current.verified_at}"
                    )
                return current
            promoted = current.verified(verified_at)
            self._rows[object_id] = promoted
            return promoted

    def quarantine(self, object_id: UUID, quarantined_at: datetime) -> StorageObjectRecord:
        with self._lock:
            quarantined = self._require(object_id).quarantined(quarantined_at)
            self._rows[object_id] = quarantined
            return quarantined

    def find(self, object_id: UUID) -> StorageObjectRecord | None:
        with self._lock:
            return self._rows.get(object_id)

    def rows(self) -> tuple[StorageObjectRecord, ...]:
        """Every row, ordered by object key so assertions are stable."""

        with self._lock:
            return tuple(sorted(self._rows.values(), key=lambda row: row.object_key))

    def _require(self, object_id: UUID) -> StorageObjectRecord:
        try:
            return self._rows[object_id]
        except KeyError as exc:
            raise ObjectStoreConflict(f"storage.objects row not found: {object_id}") from exc


@dataclass(frozen=True, slots=True)
class RegisteredObject:
    """One published, verified object and the `storage.objects` row that proves it."""

    receipt: ObjectReceipt
    record: StorageObjectRecord


class StorageObjectRegistrar:
    """Publish bytes and register the single `storage.objects` row for them."""

    def __init__(self, store: ObjectStore, port: StorageObjectWritePort) -> None:
        self._store = store
        self._port = port

    @property
    def store(self) -> ObjectStore:
        return self._store

    @property
    def port(self) -> StorageObjectWritePort:
        return self._port

    def publish(
        self,
        *,
        object_id: UUID,
        object_key: str,
        data: bytes,
        schema_version: str,
        row_count: int,
        period_start: datetime,
        period_end: datetime,
        created_at: datetime,
        verified_at: datetime,
        expected_content_hash: str | None = None,
        media_type: str = PARQUET_MEDIA_TYPE,
        file_format: str = PARQUET_FILE_FORMAT,
        compression_codec: str = UNCOMPRESSED_CODEC,
        retention_policy_version: str = RETENTION_POLICY_VERSION,
        encryption_key_ref: str | None = None,
        retention_until: datetime | None = None,
        legal_hold: bool = False,
    ) -> RegisteredObject:
        """Write, register STAGED, verify, promote to AVAILABLE. Fails closed."""

        receipt = self._store.put(object_key, data)
        if expected_content_hash is not None and receipt.content_hash != expected_content_hash:
            raise ObjectVerificationError(
                f"object store returned a different object for {object_key}: "
                f"stored {receipt.content_hash}, expected {expected_content_hash}"
            )

        staged = StorageObjectRecord.staged(
            object_id=object_id,
            receipt=receipt,
            schema_version=schema_version,
            row_count=row_count,
            period_start=period_start,
            period_end=period_end,
            created_at=created_at,
            media_type=media_type,
            file_format=file_format,
            compression_codec=compression_codec,
            retention_policy_version=retention_policy_version,
            encryption_key_ref=encryption_key_ref,
            retention_until=retention_until,
            legal_hold=legal_hold,
        )
        registered_id = self._port.register(staged)

        check = self._store.verify(object_key, receipt.content_hash, deep=True)
        if not check.ok:
            # The row is not deleted: a quarantined object is auditable evidence, and a
            # missing row would make the failure invisible.
            self._port.quarantine(registered_id, verified_at)
            raise ObjectVerificationError(
                f"stored object failed verification and was quarantined: {object_key} ({check.message})"
            )
        return RegisteredObject(receipt=receipt, record=self._port.mark_available(registered_id, verified_at))
