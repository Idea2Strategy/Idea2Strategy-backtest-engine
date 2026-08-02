"""Immutable ET-week Parquet detail objects and their canonical relational manifests.

Canonical model: `backtest.detail_manifests` in `db/schema.dbml`.

```
indexes { (run_id, record_type, week_start_date, part_number) [unique], (object_id) [unique] }
Note: '백테스트 상세 Parquet 오브젝트는 명시적 UNCOMPRESSED이며 ET 월요일 주 경계를 넘지 않는다.'
```

Four things follow from that note and are enforced here:

* **ET Monday week boundary.** A detail object holds rows from exactly one ET week.
  The previous implementation partitioned by ET *month*, which is a different model
  and produced objects that the canonical unique key cannot even address.
* **`part_number`.** One week of one record type can exceed a sensible object size, so
  a week is split into 1-based parts of at most `max_rows_per_part` rows, in time
  order. Part *k* ends before part *k+1* begins.
* **`period_start` / `period_end`.** The real extent of the rows in the part, not the
  week bounds, matching the canonical reference row (09:30 to 16:00 inside a week).
* **UNCOMPRESSED.** `pq.write_table(..., compression="none")`, asserted on read from
  the Parquet footer, and reported as `UNCOMPRESSED` in
  `storage.objects.compression_codec`.

Two hashes travel with every part. `source_set_hash` covers the identities of the
source rows that fed the part, so a part that silently gained or lost a row is
detectable without re-running the backtest. `detail_hash` covers the whole manifest
row including the object identity, and is what `detail_manifests.detail_hash` stores.

Money is serialised through `backtest_engine.money`: every monetary and price column
is quantised once, at this storage boundary, to the canonical `numeric(24,8)` text
form, so a reproducibility hash is always taken over quantised values (spec 2.3).

Changing the partition boundary, the codec and the money text form changes **every**
content hash. That is correct: the previous hashes encoded a model the canonical
schema rejects.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from itertools import pairwise
from typing import cast

import pyarrow as pa
import pyarrow.parquet as pq

from backtest_engine.execution_model import LedgerTransaction
from backtest_engine.money import format_money, quantize_money, quantize_quantity
from backtest_engine.monthly_judgment import ET, ET_TIMEZONE_ID
from backtest_engine.object_store import (
    PARQUET_FILE_FORMAT,
    PARQUET_MEDIA_TYPE,
    UNCOMPRESSED_CODEC,
    BacktestObjectKey,
    ObjectStore,
    ObjectStoreError,
    ObjectVerificationError,
    StorageObjectRecord,
    StorageObjectRegistrar,
    StorageObjectWritePort,
    StorageWriteNotAuthorized,
)
from backtest_engine.persistence.rows import DetailManifestRow
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultSnapshot,
    ResultSnapshotBuilder,
)


__all__ = [
    "COMPRESSION_CODEC",
    "DEFAULT_MAX_ROWS_PER_PART",
    "FILE_FORMAT",
    "PARQUET_MEDIA_TYPE",
    "SCHEMA_VERSION",
    "DetailIntegrityError",
    "DetailManifestConflict",
    "DetailObjectBuilder",
    "DetailObjectBundle",
    "DetailObjectDescriptor",
    "DetailObjectKind",
    "DetailObjectManifest",
    "DetailObjectPublisher",
    "DetailObjectValidationError",
    "EtWeek",
    "InMemoryDetailManifestStore",
    "PerformancePoint",
    "PublishedDetailObject",
    "PublishedDetails",
    "ReplayLedgerDetail",
    "StoredDetailObject",
    "reassemble_detail_bundle",
]


#: `backtest.detail_manifests.schema_version` and `storage.objects.schema_version` are
#: both varchar(40), not integers.
SCHEMA_VERSION = "1.0.0"
FILE_FORMAT = PARQUET_FILE_FORMAT
COMPRESSION_CODEC = UNCOMPRESSED_CODEC

#: Rows per Parquet part. A week that exceeds this is split into `part=0001`, `0002`, …
DEFAULT_MAX_ROWS_PER_PART = 100_000

SHA256 = re.compile(r"^[0-9a-f]{64}$")
ZERO = Decimal("0")

_OBJECT_ID_NAMESPACE = "idea2strategy:d27:object"
_PART_MANIFEST_NAMESPACE = "idea2strategy:d27:detail-manifest"
_BUNDLE_MANIFEST_NAMESPACE = "idea2strategy:d27:manifest"


class DetailObjectValidationError(ValueError):
    """Raised when detail evidence cannot be bound unambiguously."""


class DetailIntegrityError(RuntimeError):
    """Raised when an immutable detail object or manifest is inconsistent."""


class DetailManifestConflict(RuntimeError):
    """Raised when one official result is assigned conflicting details."""


class DetailObjectKind(str, Enum):
    """`backtest.detail_manifests.record_type`."""

    TRADE_DETAIL = "TRADE_DETAIL"
    POSITION_SNAPSHOT = "POSITION_SNAPSHOT"
    REPLAY_LEDGER = "REPLAY_LEDGER"
    CALCULATION_SERIES = "CALCULATION_SERIES"


# --------------------------------------------------------------------------------
# small validators
# --------------------------------------------------------------------------------


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise DetailObjectValidationError(f"{label} must be a non-empty string")
    return value


def _uuid(value: str, label: str) -> str:
    _text(value, label)
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise DetailObjectValidationError(f"{label} must be a UUID") from exc


def _optional_uuid(value: str | None, label: str) -> str | None:
    return None if value is None else _uuid(value, label)


def _hash(value: str, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise DetailObjectValidationError(f"{label} must be a lowercase SHA-256")
    return value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise DetailObjectValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise DetailObjectValidationError(f"{label} must be a finite Decimal")
    return value


def _money_text(value: Decimal | None) -> str | None:
    """Canonical `numeric(24,8)` text, quantised once through `money.py` (spec 2.3)."""

    return None if value is None else format_money(quantize_money(value))


def _quantity_text(value: Decimal | None) -> str | None:
    return None if value is None else format_money(quantize_quantity(value, fractional_eligible=True))


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


# --------------------------------------------------------------------------------
# ET Monday week
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, order=True)
class EtWeek:
    """The ET Monday a detail object belongs to. Detail objects never cross one."""

    start_date: date

    def __post_init__(self) -> None:
        if not isinstance(self.start_date, date) or isinstance(self.start_date, datetime):
            raise DetailObjectValidationError("EtWeek.start_date must be a date")
        if self.start_date.weekday() != 0:
            raise DetailObjectValidationError(
                f"EtWeek.start_date must be a Monday in {ET_TIMEZONE_ID}, got {self.start_date.isoformat()}"
            )

    @classmethod
    def from_instant(cls, value: datetime) -> EtWeek:
        local = _utc(value, "instant").astimezone(ET)
        return cls(local.date() - timedelta(days=local.weekday()))

    @property
    def key(self) -> str:
        return self.start_date.isoformat()


# --------------------------------------------------------------------------------
# inputs
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ReplayLedgerDetail:
    run_snapshot_id: str
    transaction: LedgerTransaction

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_snapshot_id", _hash(self.run_snapshot_id, "ledger.run_snapshot_id"))
        if not isinstance(self.transaction, LedgerTransaction):
            raise DetailObjectValidationError("transaction must be a LedgerTransaction")


@dataclass(frozen=True, slots=True)
class PerformancePoint:
    point_id: str
    run_snapshot_id: str
    occurred_at: datetime
    metric_id: str
    value: Decimal
    instrument_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "point_id", _uuid(self.point_id, "point_id"))
        object.__setattr__(self, "run_snapshot_id", _hash(self.run_snapshot_id, "point.run_snapshot_id"))
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        _text(self.metric_id, "metric_id")
        _decimal(self.value, "value")
        object.__setattr__(self, "instrument_id", _optional_uuid(self.instrument_id, "instrument_id"))


# --------------------------------------------------------------------------------
# descriptors and manifests
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DetailObjectDescriptor:
    """One `backtest.detail_manifests` row and its `storage.objects` identity."""

    storage_object_id: str
    detail_manifest_id: str
    record_type: DetailObjectKind
    week: EtWeek
    part_number: int
    period_start: datetime
    period_end: datetime
    object_key: str
    media_type: str
    file_format: str
    compression_codec: str
    schema_version: str
    row_count: int
    byte_size: int
    content_hash: str
    source_set_hash: str
    detail_hash: str
    created_at: datetime
    base_object_id: str | None = None
    correction_of_object_id: str | None = None
    supersedes_manifest_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "storage_object_id", _uuid(self.storage_object_id, "storage_object_id"))
        object.__setattr__(self, "detail_manifest_id", _uuid(self.detail_manifest_id, "detail_manifest_id"))
        if not isinstance(self.record_type, DetailObjectKind):
            raise DetailObjectValidationError("record_type is unsupported")
        if not isinstance(self.week, EtWeek):
            raise DetailObjectValidationError("week must be an EtWeek")
        if not isinstance(self.part_number, int) or isinstance(self.part_number, bool) or self.part_number < 1:
            raise DetailObjectValidationError("part_number starts at 1")
        object.__setattr__(self, "period_start", _utc(self.period_start, "period_start"))
        object.__setattr__(self, "period_end", _utc(self.period_end, "period_end"))
        if self.period_end < self.period_start:
            raise DetailObjectValidationError("period_end must not precede period_start")
        _text(self.object_key, "object_key")
        if self.media_type != PARQUET_MEDIA_TYPE:
            raise DetailObjectValidationError(f"media_type must be {PARQUET_MEDIA_TYPE}")
        if self.file_format != FILE_FORMAT:
            raise DetailObjectValidationError(f"file_format must be {FILE_FORMAT}")
        if self.compression_codec != COMPRESSION_CODEC:
            raise DetailObjectValidationError(f"compression_codec must be {COMPRESSION_CODEC}")
        if self.schema_version != SCHEMA_VERSION:
            raise DetailObjectValidationError(f"schema_version must be {SCHEMA_VERSION}")
        for label in ("row_count", "byte_size"):
            value = getattr(self, label)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise DetailObjectValidationError(f"{label} must be a non-negative integer")
        _hash(self.content_hash, "content_hash")
        _hash(self.source_set_hash, "source_set_hash")
        _hash(self.detail_hash, "detail_hash")
        object.__setattr__(self, "created_at", _utc(self.created_at, "created_at"))
        object.__setattr__(self, "base_object_id", _optional_uuid(self.base_object_id, "base_object_id"))
        object.__setattr__(
            self,
            "correction_of_object_id",
            _optional_uuid(self.correction_of_object_id, "correction_of_object_id"),
        )
        object.__setattr__(
            self,
            "supersedes_manifest_id",
            _optional_uuid(self.supersedes_manifest_id, "supersedes_manifest_id"),
        )
        if self.correction_of_object_id == self.storage_object_id:
            raise DetailObjectValidationError("an object cannot be a correction of itself")
        if self.supersedes_manifest_id == self.detail_manifest_id:
            raise DetailObjectValidationError("a detail manifest cannot supersede itself")
        if (self.correction_of_object_id is None) != (self.base_object_id is None):
            raise DetailObjectValidationError(
                "base_object_id and correction_of_object_id are set together or not at all"
            )

    @property
    def week_start_date(self) -> date:
        return self.week.start_date

    def as_manifest_row(self, run_id: str) -> DetailManifestRow:
        """The canonical `backtest.detail_manifests` row for this part."""

        return DetailManifestRow(
            id=uuid.UUID(self.detail_manifest_id),
            run_id=uuid.UUID(run_id),
            object_id=uuid.UUID(self.storage_object_id),
            record_type=self.record_type.value,
            week_start_date=self.week.start_date,
            period_start=self.period_start,
            period_end=self.period_end,
            part_number=self.part_number,
            row_count=self.row_count,
            schema_version=self.schema_version,
            source_set_hash=self.source_set_hash,
            supersedes_manifest_id=(
                None if self.supersedes_manifest_id is None else uuid.UUID(self.supersedes_manifest_id)
            ),
            detail_hash=self.detail_hash,
            created_at=self.created_at,
        )


@dataclass(frozen=True, slots=True)
class StoredDetailObject:
    descriptor: DetailObjectDescriptor
    parquet_bytes: bytes


@dataclass(frozen=True, slots=True)
class DetailObjectManifest:
    """The aggregate over one run's detail parts. One `as_rows()` entry per part."""

    detail_manifest_id: str
    result_manifest_id: str
    run_snapshot_id: str
    backtest_run_id: str
    strategy_version_id: str
    objects: tuple[DetailObjectDescriptor, ...]
    source_set_hash: str
    manifest_hash: str
    created_at: datetime
    supersedes_manifest_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "detail_manifest_id", _uuid(self.detail_manifest_id, "detail_manifest_id"))
        object.__setattr__(self, "result_manifest_id", _uuid(self.result_manifest_id, "result_manifest_id"))
        object.__setattr__(self, "run_snapshot_id", _hash(self.run_snapshot_id, "manifest.run_snapshot_id"))
        object.__setattr__(self, "backtest_run_id", _uuid(self.backtest_run_id, "manifest.backtest_run_id"))
        object.__setattr__(
            self, "strategy_version_id", _uuid(self.strategy_version_id, "manifest.strategy_version_id")
        )
        objects = tuple(self.objects)
        if any(not isinstance(item, DetailObjectDescriptor) for item in objects):
            raise DetailObjectValidationError("objects must contain DetailObjectDescriptor values")
        object_ids = [item.storage_object_id for item in objects]
        if len(set(object_ids)) != len(object_ids):
            raise DetailObjectValidationError("storage_object_id values must be unique")
        manifest_ids = [item.detail_manifest_id for item in objects]
        if len(set(manifest_ids)) != len(manifest_ids):
            raise DetailObjectValidationError("detail_manifest_id values must be unique")
        partitions = [(item.record_type, item.week, item.part_number) for item in objects]
        if len(set(partitions)) != len(partitions):
            raise DetailObjectValidationError(
                "(record_type, week_start_date, part_number) partitions must be unique"
            )
        object.__setattr__(self, "objects", objects)
        _hash(self.source_set_hash, "manifest.source_set_hash")
        _hash(self.manifest_hash, "manifest_hash")
        object.__setattr__(self, "created_at", _utc(self.created_at, "manifest.created_at"))
        object.__setattr__(
            self,
            "supersedes_manifest_id",
            _optional_uuid(self.supersedes_manifest_id, "manifest.supersedes_manifest_id"),
        )
        if self.supersedes_manifest_id == self.detail_manifest_id:
            raise DetailObjectValidationError("a detail manifest cannot supersede itself")

    def as_rows(self) -> tuple[DetailManifestRow, ...]:
        """One canonical `backtest.detail_manifests` row per Parquet part."""

        return tuple(item.as_manifest_row(self.backtest_run_id) for item in self.objects)


@dataclass(frozen=True, slots=True)
class DetailObjectBundle:
    manifest: DetailObjectManifest
    objects: tuple[StoredDetailObject, ...]


# --------------------------------------------------------------------------------
# builder
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Partition:
    record_type: DetailObjectKind
    week: EtWeek
    rows: list[dict[str, object]]


class DetailObjectBuilder:
    """Builds deterministic ET-week Parquet parts and their relational manifest."""

    def __init__(self, max_rows_per_part: int = DEFAULT_MAX_ROWS_PER_PART) -> None:
        if not isinstance(max_rows_per_part, int) or isinstance(max_rows_per_part, bool) or max_rows_per_part < 1:
            raise DetailObjectValidationError("max_rows_per_part must be a positive integer")
        self.max_rows_per_part = max_rows_per_part

    def build(
        self,
        result: ResultSnapshot,
        replay_ledger: Iterable[ReplayLedgerDetail],
        calculation_series: Iterable[PerformancePoint],
        created_at: datetime,
        *,
        supersedes: DetailObjectBundle | None = None,
    ) -> DetailObjectBundle:
        """Build one run's detail bundle.

        `supersedes` is the previously published bundle for the same run. Every part
        whose bytes changed records the lineage the canonical schema asks for:
        `supersedes_manifest_id` on the manifest row and
        `base_object_id`/`correction_of_object_id` on the object. A part whose bytes are
        byte-identical is *not* a correction and carries no lineage.
        """

        ResultSnapshotBuilder.verify(result)
        created_at = _utc(created_at, "created_at")
        run = result.run_snapshot
        run_snapshot_id = run.snapshot_id
        ledger = tuple(replay_ledger)
        points = tuple(calculation_series)
        previous = self._previous_parts(supersedes, run.backtest_run_id, created_at)

        self._validate_inputs(result, run_snapshot_id, ledger, points, created_at)

        partitions = self._partition(result, ledger, points)
        objects: list[StoredDetailObject] = []
        for partition in partitions:
            for index in range(0, len(partition.rows), self.max_rows_per_part):
                chunk = partition.rows[index : index + self.max_rows_per_part]
                objects.append(
                    self._build_object(
                        result,
                        partition.record_type,
                        partition.week,
                        index // self.max_rows_per_part + 1,
                        chunk,
                        created_at,
                        previous,
                    )
                )

        descriptors = tuple(item.descriptor for item in objects)
        source_set_hash = _sha256(_canonical(_source_set_payload(descriptors)))
        manifest_payload = _manifest_payload(
            result.manifest.result_manifest_id,
            run_snapshot_id,
            run.backtest_run_id,
            run.strategy_version_id,
            descriptors,
            source_set_hash,
            created_at,
            None if supersedes is None else supersedes.manifest.detail_manifest_id,
        )
        manifest_hash = _sha256(_canonical(manifest_payload))
        bundle = DetailObjectBundle(
            manifest=DetailObjectManifest(
                detail_manifest_id=_bundle_manifest_id(manifest_hash),
                result_manifest_id=result.manifest.result_manifest_id,
                run_snapshot_id=run_snapshot_id,
                backtest_run_id=run.backtest_run_id,
                strategy_version_id=run.strategy_version_id,
                objects=descriptors,
                source_set_hash=source_set_hash,
                manifest_hash=manifest_hash,
                created_at=created_at,
                supersedes_manifest_id=None if supersedes is None else supersedes.manifest.detail_manifest_id,
            ),
            objects=tuple(objects),
        )
        self.verify(bundle)
        return bundle

    # -- inputs ------------------------------------------------------------------

    @staticmethod
    def _previous_parts(
        supersedes: DetailObjectBundle | None,
        backtest_run_id: str,
        created_at: datetime,
    ) -> dict[tuple[DetailObjectKind, EtWeek, int], DetailObjectDescriptor]:
        if supersedes is None:
            return {}
        if not isinstance(supersedes, DetailObjectBundle):
            raise DetailObjectValidationError("supersedes must be a DetailObjectBundle")
        DetailObjectBuilder.verify(supersedes)
        if supersedes.manifest.backtest_run_id != backtest_run_id:
            raise DetailObjectValidationError("a correction must supersede details of the same backtest run")
        if supersedes.manifest.created_at > created_at:
            raise DetailObjectValidationError("a correction must not predate the details it supersedes")
        return {
            (item.record_type, item.week, item.part_number): item for item in supersedes.manifest.objects
        }

    @staticmethod
    def _validate_inputs(
        result: ResultSnapshot,
        run_snapshot_id: str,
        ledger: tuple[ReplayLedgerDetail, ...],
        points: tuple[PerformancePoint, ...],
        created_at: datetime,
    ) -> None:
        if any(not isinstance(item, ReplayLedgerDetail) for item in ledger):
            raise DetailObjectValidationError("replay_ledger must contain ReplayLedgerDetail values")
        if any(not isinstance(item, PerformancePoint) for item in points):
            raise DetailObjectValidationError("calculation_series must contain PerformancePoint values")
        transaction_ids = [item.transaction.transaction_id for item in ledger]
        if len(set(transaction_ids)) != len(transaction_ids):
            raise DetailObjectValidationError("transaction_id values must be unique")
        entry_ids = [entry.entry_id for item in ledger for entry in item.transaction.entries]
        if len(set(entry_ids)) != len(entry_ids):
            raise DetailObjectValidationError("ledger entry_id values must be unique")
        point_ids = [item.point_id for item in points]
        if len(set(point_ids)) != len(point_ids):
            raise DetailObjectValidationError("point_id values must be unique")
        if any(item.run_snapshot_id != run_snapshot_id for item in ledger):
            raise DetailObjectValidationError("every ledger transaction must reference the run snapshot")
        if any(item.run_snapshot_id != run_snapshot_id for item in points):
            raise DetailObjectValidationError("every calculation point must reference the run snapshot")

        latest = [item.occurred_at for item in result.records]
        latest.extend(item.transaction.posted_at for item in ledger)
        latest.extend(item.occurred_at for item in points)
        if latest and max(latest) > created_at:
            raise DetailObjectValidationError("created_at must not precede a detail record")

    def _partition(
        self,
        result: ResultSnapshot,
        ledger: tuple[ReplayLedgerDetail, ...],
        points: tuple[PerformancePoint, ...],
    ) -> list[_Partition]:
        buckets: dict[tuple[DetailObjectKind, EtWeek], list[dict[str, object]]] = {}

        def bucket(record_type: DetailObjectKind, week: EtWeek) -> list[dict[str, object]]:
            return buckets.setdefault((record_type, week), [])

        for record in result.records:
            week = EtWeek.from_instant(record.occurred_at)
            bucket(DetailObjectKind.TRADE_DETAIL, week).append(_trade_row(record))
            for position in record.positions_after:
                bucket(DetailObjectKind.POSITION_SNAPSHOT, week).append(_position_row(record, position))
        for item in ledger:
            week = EtWeek.from_instant(item.transaction.posted_at)
            bucket(DetailObjectKind.REPLAY_LEDGER, week).extend(_ledger_rows(item.transaction))
        for point in points:
            week = EtWeek.from_instant(point.occurred_at)
            bucket(DetailObjectKind.CALCULATION_SERIES, week).append(_calculation_row(point))

        return [
            _Partition(record_type, week, _sort_rows(record_type, rows))
            for (record_type, week), rows in sorted(
                buckets.items(), key=lambda item: (item[0][1], item[0][0].value)
            )
        ]

    # -- one part ----------------------------------------------------------------

    def _build_object(
        self,
        result: ResultSnapshot,
        record_type: DetailObjectKind,
        week: EtWeek,
        part_number: int,
        rows: list[dict[str, object]],
        created_at: datetime,
        previous: Mapping[tuple[DetailObjectKind, EtWeek, int], DetailObjectDescriptor],
    ) -> StoredDetailObject:
        time_field = _time_field(record_type)
        # Every row-builder writes a timezone-aware datetime into the time column and
        # the Arrow schema declares it `timestamp[us, tz=UTC]`, so the narrowing is an
        # invariant of `_partition`, not an assumption about caller input.
        instants = [cast("datetime", row[time_field]) for row in rows]
        period_start = min(instants)
        period_end = max(instants)
        source_set_hash = _sha256(_canonical(_identity_payload(record_type, rows)))

        metadata = {
            b"schema_version": SCHEMA_VERSION.encode(),
            b"run_snapshot_id": result.run_snapshot.snapshot_id.encode(),
            b"backtest_run_id": result.run_snapshot.backtest_run_id.encode(),
            b"strategy_version_id": result.run_snapshot.strategy_version_id.encode(),
            b"result_manifest_id": result.manifest.result_manifest_id.encode(),
            b"record_type": record_type.value.encode(),
            b"week_start_date": week.key.encode(),
            b"part_number": str(part_number).encode(),
            b"timezone_id": ET_TIMEZONE_ID.encode(),
            b"compression_codec": COMPRESSION_CODEC.encode(),
            b"source_set_hash": source_set_hash.encode(),
        }
        schema = _schema(record_type).with_metadata(metadata)
        table = pa.Table.from_pylist(rows, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            # Canonical: `storage.objects.compression_codec` and the detail_manifests
            # note both mandate explicit UNCOMPRESSED. The previous "zstd" made every
            # object unreadable to a reader that trusts the canonical model.
            compression="none",
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
            data_page_version="2.0",
            row_group_size=max(1, len(rows)),
        )
        parquet_bytes = sink.getvalue().to_pybytes()
        content_hash = _sha256(parquet_bytes)
        storage_object_id = _object_id(
            result.run_snapshot.snapshot_id, record_type, week, part_number, content_hash
        )
        object_key = BacktestObjectKey(
            run_id=result.run_snapshot.backtest_run_id,
            record_type=record_type.value,
            week_start=week.start_date,
            part_number=part_number,
            content_hash=content_hash,
        ).render()

        superseded = previous.get((record_type, week, part_number))
        if superseded is not None and superseded.content_hash == content_hash:
            superseded = None
        base_object_id = None if superseded is None else (superseded.base_object_id or superseded.storage_object_id)

        detail_hash = _sha256(
            _canonical(
                _detail_payload(
                    result_manifest_id=result.manifest.result_manifest_id,
                    run_snapshot_id=result.run_snapshot.snapshot_id,
                    backtest_run_id=result.run_snapshot.backtest_run_id,
                    record_type=record_type,
                    week=week,
                    part_number=part_number,
                    period_start=period_start,
                    period_end=period_end,
                    row_count=len(rows),
                    byte_size=len(parquet_bytes),
                    content_hash=content_hash,
                    source_set_hash=source_set_hash,
                    object_key=object_key,
                    base_object_id=base_object_id,
                    correction_of_object_id=None if superseded is None else superseded.storage_object_id,
                    supersedes_manifest_id=None if superseded is None else superseded.detail_manifest_id,
                )
            )
        )
        return StoredDetailObject(
            descriptor=DetailObjectDescriptor(
                storage_object_id=storage_object_id,
                detail_manifest_id=_part_manifest_id(detail_hash),
                record_type=record_type,
                week=week,
                part_number=part_number,
                period_start=period_start,
                period_end=period_end,
                object_key=object_key,
                media_type=PARQUET_MEDIA_TYPE,
                file_format=FILE_FORMAT,
                compression_codec=COMPRESSION_CODEC,
                schema_version=SCHEMA_VERSION,
                row_count=len(rows),
                byte_size=len(parquet_bytes),
                content_hash=content_hash,
                source_set_hash=source_set_hash,
                detail_hash=detail_hash,
                created_at=created_at,
                base_object_id=base_object_id,
                correction_of_object_id=None if superseded is None else superseded.storage_object_id,
                supersedes_manifest_id=None if superseded is None else superseded.detail_manifest_id,
            ),
            parquet_bytes=parquet_bytes,
        )

    # -- verification -------------------------------------------------------------

    @staticmethod
    def verify(bundle: DetailObjectBundle) -> None:
        """Re-derive every published fact from the Parquet bytes. Fails closed."""

        if not isinstance(bundle, DetailObjectBundle):
            raise DetailIntegrityError("detail bundle type is invalid")
        manifest = bundle.manifest
        if not isinstance(manifest, DetailObjectManifest):
            raise DetailIntegrityError("detail manifest type is invalid")
        objects = tuple(bundle.objects)
        if any(not isinstance(item, StoredDetailObject) for item in objects):
            raise DetailIntegrityError("detail objects contain an invalid type")
        descriptors = tuple(item.descriptor for item in objects)
        if descriptors != manifest.objects:
            raise DetailIntegrityError("manifest object is missing or unexpected")

        for item in objects:
            _verify_object(manifest, item)

        _verify_parts(manifest)

        expected_source_set = _sha256(_canonical(_source_set_payload(manifest.objects)))
        if manifest.source_set_hash != expected_source_set:
            raise DetailIntegrityError("detail manifest source_set_hash does not match")
        expected_payload = _manifest_payload(
            manifest.result_manifest_id,
            manifest.run_snapshot_id,
            manifest.backtest_run_id,
            manifest.strategy_version_id,
            manifest.objects,
            manifest.source_set_hash,
            manifest.created_at,
            manifest.supersedes_manifest_id,
        )
        expected_hash = _sha256(_canonical(expected_payload))
        if manifest.manifest_hash != expected_hash:
            raise DetailIntegrityError("detail manifest hash does not match")
        if manifest.detail_manifest_id != _bundle_manifest_id(expected_hash):
            raise DetailIntegrityError("detail manifest identity does not match")


def _verify_object(manifest: DetailObjectManifest, item: StoredDetailObject) -> None:
    descriptor = item.descriptor
    if not isinstance(item.parquet_bytes, bytes):
        raise DetailIntegrityError("detail object content must be bytes")
    if len(item.parquet_bytes) != descriptor.byte_size:
        raise DetailIntegrityError("detail object size does not match")
    if _sha256(item.parquet_bytes) != descriptor.content_hash:
        raise DetailIntegrityError("detail object hash does not match")
    expected_id = _object_id(
        manifest.run_snapshot_id,
        descriptor.record_type,
        descriptor.week,
        descriptor.part_number,
        descriptor.content_hash,
    )
    if descriptor.storage_object_id != expected_id:
        raise DetailIntegrityError("detail object identity does not match")
    expected_key = BacktestObjectKey(
        run_id=manifest.backtest_run_id,
        record_type=descriptor.record_type.value,
        week_start=descriptor.week.start_date,
        part_number=descriptor.part_number,
        content_hash=descriptor.content_hash,
    ).render()
    if descriptor.object_key != expected_key:
        raise DetailIntegrityError("detail object key does not match")

    try:
        parquet = pq.ParquetFile(pa.BufferReader(item.parquet_bytes))
    except (OSError, pa.ArrowException) as exc:
        raise DetailIntegrityError("detail object is not valid Parquet") from exc
    if parquet.metadata.num_rows != descriptor.row_count:
        raise DetailIntegrityError("detail object row count does not match")

    codecs = {
        parquet.metadata.row_group(group).column(column).compression
        for group in range(parquet.metadata.num_row_groups)
        for column in range(parquet.metadata.row_group(group).num_columns)
    }
    if codecs - {COMPRESSION_CODEC}:
        raise DetailIntegrityError(
            f"detail object compression codec must be {COMPRESSION_CODEC}, footer reports {sorted(codecs)}"
        )

    metadata = parquet.schema_arrow.metadata or {}
    expected_metadata = {
        b"schema_version": SCHEMA_VERSION.encode(),
        b"run_snapshot_id": manifest.run_snapshot_id.encode(),
        b"backtest_run_id": manifest.backtest_run_id.encode(),
        b"strategy_version_id": manifest.strategy_version_id.encode(),
        b"result_manifest_id": manifest.result_manifest_id.encode(),
        b"record_type": descriptor.record_type.value.encode(),
        b"week_start_date": descriptor.week.key.encode(),
        b"part_number": str(descriptor.part_number).encode(),
        b"timezone_id": ET_TIMEZONE_ID.encode(),
        b"compression_codec": COMPRESSION_CODEC.encode(),
        b"source_set_hash": descriptor.source_set_hash.encode(),
    }
    if metadata != expected_metadata:
        raise DetailIntegrityError("detail object metadata does not match")

    rows = parquet.read().to_pylist()
    time_field = _time_field(descriptor.record_type)
    for row in rows:
        if EtWeek.from_instant(row[time_field]) != descriptor.week:
            raise DetailIntegrityError("detail row is in the wrong ET week")
    if rows:
        instants = [row[time_field] for row in rows]
        if min(instants) != descriptor.period_start or max(instants) != descriptor.period_end:
            raise DetailIntegrityError("detail object period bounds do not match its rows")
    if rows != _sort_rows(descriptor.record_type, list(rows)):
        raise DetailIntegrityError("detail object rows are not in canonical order")
    if _sha256(_canonical(_identity_payload(descriptor.record_type, rows))) != descriptor.source_set_hash:
        raise DetailIntegrityError("detail object source_set_hash does not match its rows")

    expected_detail_hash = _sha256(
        _canonical(
            _detail_payload(
                result_manifest_id=manifest.result_manifest_id,
                run_snapshot_id=manifest.run_snapshot_id,
                backtest_run_id=manifest.backtest_run_id,
                record_type=descriptor.record_type,
                week=descriptor.week,
                part_number=descriptor.part_number,
                period_start=descriptor.period_start,
                period_end=descriptor.period_end,
                row_count=descriptor.row_count,
                byte_size=descriptor.byte_size,
                content_hash=descriptor.content_hash,
                source_set_hash=descriptor.source_set_hash,
                object_key=descriptor.object_key,
                base_object_id=descriptor.base_object_id,
                correction_of_object_id=descriptor.correction_of_object_id,
                supersedes_manifest_id=descriptor.supersedes_manifest_id,
            )
        )
    )
    if descriptor.detail_hash != expected_detail_hash:
        raise DetailIntegrityError("detail manifest row hash does not match")
    if descriptor.detail_manifest_id != _part_manifest_id(expected_detail_hash):
        raise DetailIntegrityError("detail manifest row identity does not match")


def _verify_parts(manifest: DetailObjectManifest) -> None:
    """Parts of one (record_type, week) are 1..N and strictly ordered in time."""

    grouped: dict[tuple[DetailObjectKind, EtWeek], list[DetailObjectDescriptor]] = {}
    for descriptor in manifest.objects:
        grouped.setdefault((descriptor.record_type, descriptor.week), []).append(descriptor)
    for parts in grouped.values():
        ordered = sorted(parts, key=lambda item: item.part_number)
        if [item.part_number for item in ordered] != list(range(1, len(ordered) + 1)):
            raise DetailIntegrityError("detail part numbers must run from 1 without gaps")
        for earlier, later in pairwise(ordered):
            if earlier.period_end > later.period_start:
                raise DetailIntegrityError("detail parts must not overlap in time")


def reassemble_detail_bundle(
    *,
    result_manifest_id: str,
    run_snapshot_id: str,
    backtest_run_id: str,
    strategy_version_id: str,
    created_at: datetime,
    parts: Sequence[tuple[DetailObjectDescriptor, bytes]],
    supersedes_manifest_id: str | None = None,
) -> DetailObjectBundle:
    """Rebuild a published bundle from its per-part rows and the stored bytes.

    `backtest.detail_manifests` holds one row per Parquet part; the *bundle* identity
    (`detail_manifest_id`, `manifest_hash`, `source_set_hash`) is not a column anywhere,
    because it is a pure function of the parts. This derives it the same way
    :meth:`DetailObjectBuilder.build` does, so a bundle read back out of PostgreSQL and
    the object store is identical to the one the worker published, or it fails.

    Two things are checked before any hash is taken:

    * every part must carry the bundle's `created_at`. `build` stamps one instant on
      every part, so a part that disagrees came from a different publish; and
    * the part order is re-derived rather than trusted, because SQL returns rows in
      whatever order the query asked for and the bundle hash covers the sequence.

    `verify` then re-derives every published fact from the Parquet bytes themselves, so
    a lost, truncated or edited object raises `DetailIntegrityError` here rather than
    producing a short answer downstream.
    """

    created_at = _utc(created_at, "created_at")
    ordered = sorted(
        parts, key=lambda item: (item[0].week, item[0].record_type.value, item[0].part_number)
    )
    for descriptor, _ in ordered:
        if not isinstance(descriptor, DetailObjectDescriptor):
            raise DetailIntegrityError("detail parts must carry DetailObjectDescriptor values")
        if descriptor.created_at != created_at:
            raise DetailIntegrityError(
                f"detail part {descriptor.object_key} has created_at "
                f"{descriptor.created_at.isoformat()}, but this bundle was published at "
                f"{created_at.isoformat()}"
            )

    descriptors = tuple(descriptor for descriptor, _ in ordered)
    source_set_hash = _sha256(_canonical(_source_set_payload(descriptors)))
    manifest_hash = _sha256(
        _canonical(
            _manifest_payload(
                result_manifest_id,
                run_snapshot_id,
                backtest_run_id,
                strategy_version_id,
                descriptors,
                source_set_hash,
                created_at,
                supersedes_manifest_id,
            )
        )
    )
    bundle = DetailObjectBundle(
        manifest=DetailObjectManifest(
            detail_manifest_id=_bundle_manifest_id(manifest_hash),
            result_manifest_id=result_manifest_id,
            run_snapshot_id=run_snapshot_id,
            backtest_run_id=backtest_run_id,
            strategy_version_id=strategy_version_id,
            objects=descriptors,
            source_set_hash=source_set_hash,
            manifest_hash=manifest_hash,
            created_at=created_at,
            supersedes_manifest_id=supersedes_manifest_id,
        ),
        objects=tuple(
            StoredDetailObject(descriptor=descriptor, parquet_bytes=data)
            for descriptor, data in ordered
        ),
    )
    DetailObjectBuilder.verify(bundle)
    return bundle


# --------------------------------------------------------------------------------
# publishing
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PublishedDetailObject:
    descriptor: DetailObjectDescriptor
    storage_object: StorageObjectRecord
    manifest_row: DetailManifestRow


@dataclass(frozen=True, slots=True)
class PublishedDetails:
    """Everything one publish produced: the bundle, the object rows, the manifest rows."""

    manifest: DetailObjectManifest
    objects: tuple[PublishedDetailObject, ...]

    def storage_object_records(self) -> tuple[StorageObjectRecord, ...]:
        return tuple(item.storage_object for item in self.objects)

    def manifest_rows(self) -> tuple[DetailManifestRow, ...]:
        return tuple(item.manifest_row for item in self.objects)


class DetailObjectPublisher:
    """Writes detail parts to an object store and produces their canonical rows.

    Every part goes through `StorageObjectRegistrar`, so each object gets exactly one
    `storage.objects` row, that row is `STAGED` before the bytes are re-verified, and it
    reaches `AVAILABLE` only after a deep verification succeeds.

    `storage_write_port` is required rather than optional: an optional port would make
    "no row was written" indistinguishable from "the row was written", which is the
    failure mode this card exists to remove. Pass
    `object_store.InMemoryStorageObjectRegistry` for a single-node run, or
    `object_store.UnauthorizedStorageObjectWritePort` where a durable row is required
    and `storage` ownership is still unresolved — that one fails closed.
    """

    def __init__(self, store: ObjectStore, *, storage_write_port: StorageObjectWritePort) -> None:
        self._store = store
        self._write_port = storage_write_port
        self._registrar = StorageObjectRegistrar(store, storage_write_port)

    @property
    def store(self) -> ObjectStore:
        return self._store

    def publish(self, bundle: DetailObjectBundle, *, verified_at: datetime) -> PublishedDetails:
        DetailObjectBuilder.verify(bundle)
        verified_at = _utc(verified_at, "verified_at")
        published: list[PublishedDetailObject] = []
        for item in bundle.objects:
            descriptor = item.descriptor
            try:
                registered = self._registrar.publish(
                    object_id=uuid.UUID(descriptor.storage_object_id),
                    object_key=descriptor.object_key,
                    data=item.parquet_bytes,
                    schema_version=descriptor.schema_version,
                    row_count=descriptor.row_count,
                    period_start=descriptor.period_start,
                    period_end=descriptor.period_end,
                    created_at=descriptor.created_at,
                    verified_at=verified_at,
                    expected_content_hash=descriptor.content_hash,
                    media_type=descriptor.media_type,
                    file_format=descriptor.file_format,
                    compression_codec=descriptor.compression_codec,
                )
            except ObjectVerificationError as exc:
                raise DetailIntegrityError(
                    f"published detail object failed verification: {descriptor.object_key} ({exc})"
                ) from exc
            except StorageWriteNotAuthorized:
                # Not a detail-integrity problem: the object is fine and the caller
                # asked for a row it is not allowed to write. Surface it unchanged.
                raise
            except ObjectStoreError as exc:
                raise DetailIntegrityError(
                    f"detail object could not be published: {descriptor.object_key}"
                ) from exc
            if registered.record.byte_size != descriptor.byte_size:
                raise DetailIntegrityError(f"object store returned a different object: {descriptor.object_key}")
            published.append(
                PublishedDetailObject(
                    descriptor=descriptor,
                    storage_object=registered.record,
                    manifest_row=descriptor.as_manifest_row(bundle.manifest.backtest_run_id),
                )
            )
        return PublishedDetails(manifest=bundle.manifest, objects=tuple(published))


class InMemoryDetailManifestStore:
    """Bundle-level idempotent store used until the run publish path owns the writes.

    The conflict rules mirror `persistence.DetailManifestRepository`: the same bundle
    twice is idempotent, a different bundle for the same official result is a conflict.
    """

    def __init__(self) -> None:
        self._by_manifest_id: dict[str, DetailObjectBundle] = {}
        self._detail_by_result_manifest: dict[str, str] = {}

    def put(self, bundle: DetailObjectBundle) -> DetailObjectManifest:
        DetailObjectBuilder.verify(bundle)
        result_manifest_id = bundle.manifest.result_manifest_id
        existing_id = self._detail_by_result_manifest.get(result_manifest_id)
        if existing_id is not None:
            existing = self._by_manifest_id[existing_id]
            if existing == bundle:
                return existing.manifest
            raise DetailManifestConflict("result manifest already has different official detail objects")
        self._by_manifest_id[bundle.manifest.detail_manifest_id] = bundle
        self._detail_by_result_manifest[result_manifest_id] = bundle.manifest.detail_manifest_id
        return bundle.manifest

    def get(self, detail_manifest_id: str) -> DetailObjectBundle:
        detail_manifest_id = _uuid(detail_manifest_id, "detail_manifest_id")
        try:
            bundle = self._by_manifest_id[detail_manifest_id]
        except KeyError as exc:
            raise KeyError(f"detail manifest not found: {detail_manifest_id}") from exc
        DetailObjectBuilder.verify(bundle)
        return bundle


# --------------------------------------------------------------------------------
# row shapes
# --------------------------------------------------------------------------------


def _trade_row(record: ResultRecord) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "occurred_at": record.occurred_at,
        "kind": record.kind.value,
        "order_id": record.order_id,
        "instrument_id": record.instrument_id,
        "order_status": record.order_status.value,
        "cash_after": _money_text(record.cash_after),
        "reason_code": record.reason_code,
        "fill_id": record.fill_id,
        "quantity": _quantity_text(record.quantity),
        "base_price": _money_text(record.base_price),
        "price": _money_text(record.price),
        "gross_amount": _money_text(record.gross_amount),
        "slippage_amount": _money_text(record.slippage_amount),
        "fee": _money_text(record.fee),
        "cost_basis": _money_text(record.cost_basis),
        "realized_pnl": _money_text(record.realized_pnl),
    }


def _position_row(record: ResultRecord, position: PositionAfter) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "occurred_at": record.occurred_at,
        "instrument_id": position.instrument_id,
        "quantity": _quantity_text(position.quantity),
        "cost_basis": _money_text(position.cost_basis),
        "cash_after": _money_text(record.cash_after),
    }


def _ledger_rows(transaction: LedgerTransaction) -> list[dict[str, object]]:
    return [
        {
            "transaction_id": transaction.transaction_id,
            "posted_at": transaction.posted_at,
            "source_event_id": transaction.source_event_id,
            "entry_id": entry.entry_id,
            "account_code": entry.account_code,
            "direction": entry.direction.value,
            "amount": _money_text(entry.amount),
            "currency": entry.currency,
        }
        for entry in transaction.entries
    ]


def _calculation_row(point: PerformancePoint) -> dict[str, object]:
    return {
        "point_id": point.point_id,
        "occurred_at": point.occurred_at,
        "metric_id": point.metric_id,
        "value": _money_text(point.value),
        "instrument_id": point.instrument_id,
    }


def _schema(record_type: DetailObjectKind) -> pa.Schema:
    timestamp = pa.timestamp("us", tz="UTC")
    string = pa.string()
    fields = {
        DetailObjectKind.TRADE_DETAIL: [
            pa.field("record_id", string, nullable=False),
            pa.field("occurred_at", timestamp, nullable=False),
            pa.field("kind", string, nullable=False),
            pa.field("order_id", string, nullable=False),
            pa.field("instrument_id", string, nullable=False),
            pa.field("order_status", string, nullable=False),
            pa.field("cash_after", string, nullable=False),
            pa.field("reason_code", string),
            pa.field("fill_id", string),
            pa.field("quantity", string),
            pa.field("base_price", string),
            pa.field("price", string),
            pa.field("gross_amount", string),
            pa.field("slippage_amount", string),
            pa.field("fee", string),
            pa.field("cost_basis", string),
            pa.field("realized_pnl", string),
        ],
        DetailObjectKind.POSITION_SNAPSHOT: [
            pa.field("record_id", string, nullable=False),
            pa.field("occurred_at", timestamp, nullable=False),
            pa.field("instrument_id", string, nullable=False),
            pa.field("quantity", string, nullable=False),
            pa.field("cost_basis", string, nullable=False),
            pa.field("cash_after", string, nullable=False),
        ],
        DetailObjectKind.REPLAY_LEDGER: [
            pa.field("transaction_id", string, nullable=False),
            pa.field("posted_at", timestamp, nullable=False),
            pa.field("source_event_id", string, nullable=False),
            pa.field("entry_id", string, nullable=False),
            pa.field("account_code", string, nullable=False),
            pa.field("direction", string, nullable=False),
            pa.field("amount", string, nullable=False),
            pa.field("currency", string, nullable=False),
        ],
        DetailObjectKind.CALCULATION_SERIES: [
            pa.field("point_id", string, nullable=False),
            pa.field("occurred_at", timestamp, nullable=False),
            pa.field("metric_id", string, nullable=False),
            pa.field("value", string, nullable=False),
            pa.field("instrument_id", string),
        ],
    }[record_type]
    return pa.schema(fields)


def _time_field(record_type: DetailObjectKind) -> str:
    return "posted_at" if record_type is DetailObjectKind.REPLAY_LEDGER else "occurred_at"


_SORT_KEYS: dict[DetailObjectKind, tuple[str, ...]] = {
    DetailObjectKind.TRADE_DETAIL: ("occurred_at", "record_id"),
    DetailObjectKind.POSITION_SNAPSHOT: ("occurred_at", "record_id", "instrument_id"),
    DetailObjectKind.REPLAY_LEDGER: ("posted_at", "transaction_id", "entry_id"),
    DetailObjectKind.CALCULATION_SERIES: ("occurred_at", "point_id"),
}

#: The columns that identify a source row, for `source_set_hash`.
_IDENTITY_KEYS: dict[DetailObjectKind, tuple[str, ...]] = {
    DetailObjectKind.TRADE_DETAIL: ("record_id",),
    DetailObjectKind.POSITION_SNAPSHOT: ("record_id", "instrument_id"),
    DetailObjectKind.REPLAY_LEDGER: ("transaction_id", "entry_id"),
    DetailObjectKind.CALCULATION_SERIES: ("point_id",),
}


def _sort_rows(record_type: DetailObjectKind, rows: list[dict[str, object]]) -> list[dict[str, object]]:
    identifiers = _SORT_KEYS[record_type]
    return sorted(rows, key=lambda row: tuple(str(row[field]) for field in identifiers))


def _identity_payload(record_type: DetailObjectKind, rows: Sequence[Mapping[str, object]]) -> dict[str, object]:
    keys = _IDENTITY_KEYS[record_type]
    return {
        "schema_version": SCHEMA_VERSION,
        "record_type": record_type.value,
        "identities": [[str(row[key]) for key in keys] for row in rows],
    }


# --------------------------------------------------------------------------------
# identities and hash payloads
# --------------------------------------------------------------------------------


def _object_id(
    run_snapshot_id: str,
    record_type: DetailObjectKind,
    week: EtWeek,
    part_number: int,
    content_hash: str,
) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{_OBJECT_ID_NAMESPACE}:{run_snapshot_id}:{record_type.value}:"
            f"{week.key}:{part_number:04d}:{content_hash}",
        )
    )


def _part_manifest_id(detail_hash: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{_PART_MANIFEST_NAMESPACE}:{detail_hash}"))


def _bundle_manifest_id(manifest_hash: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{_BUNDLE_MANIFEST_NAMESPACE}:{manifest_hash}"))


def _detail_payload(
    *,
    result_manifest_id: str,
    run_snapshot_id: str,
    backtest_run_id: str,
    record_type: DetailObjectKind,
    week: EtWeek,
    part_number: int,
    period_start: datetime,
    period_end: datetime,
    row_count: int,
    byte_size: int,
    content_hash: str,
    source_set_hash: str,
    object_key: str,
    base_object_id: str | None,
    correction_of_object_id: str | None,
    supersedes_manifest_id: str | None,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "result_manifest_id": result_manifest_id,
        "run_snapshot_id": run_snapshot_id,
        "backtest_run_id": backtest_run_id,
        "record_type": record_type.value,
        "week_start_date": week.key,
        "timezone_id": ET_TIMEZONE_ID,
        "part_number": part_number,
        "period_start": _timestamp(period_start),
        "period_end": _timestamp(period_end),
        "row_count": row_count,
        "byte_size": byte_size,
        "content_hash": content_hash,
        "source_set_hash": source_set_hash,
        "object_key": object_key,
        "media_type": PARQUET_MEDIA_TYPE,
        "file_format": FILE_FORMAT,
        "compression_codec": COMPRESSION_CODEC,
        "base_object_id": base_object_id,
        "correction_of_object_id": correction_of_object_id,
        "supersedes_manifest_id": supersedes_manifest_id,
    }


def _descriptor_payload(descriptor: DetailObjectDescriptor) -> dict[str, object]:
    return {
        "storage_object_id": descriptor.storage_object_id,
        "detail_manifest_id": descriptor.detail_manifest_id,
        "record_type": descriptor.record_type.value,
        "week_start_date": descriptor.week.key,
        "timezone_id": ET_TIMEZONE_ID,
        "part_number": descriptor.part_number,
        "period_start": _timestamp(descriptor.period_start),
        "period_end": _timestamp(descriptor.period_end),
        "object_key": descriptor.object_key,
        "media_type": descriptor.media_type,
        "file_format": descriptor.file_format,
        "compression_codec": descriptor.compression_codec,
        "schema_version": descriptor.schema_version,
        "row_count": descriptor.row_count,
        "byte_size": descriptor.byte_size,
        "content_hash": descriptor.content_hash,
        "source_set_hash": descriptor.source_set_hash,
        "detail_hash": descriptor.detail_hash,
        "created_at": _timestamp(descriptor.created_at),
        "base_object_id": descriptor.base_object_id,
        "correction_of_object_id": descriptor.correction_of_object_id,
        "supersedes_manifest_id": descriptor.supersedes_manifest_id,
    }


def _source_set_payload(objects: Sequence[DetailObjectDescriptor]) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "parts": [
            {
                "record_type": item.record_type.value,
                "week_start_date": item.week.key,
                "part_number": item.part_number,
                "source_set_hash": item.source_set_hash,
            }
            for item in objects
        ],
    }


def _manifest_payload(
    result_manifest_id: str,
    run_snapshot_id: str,
    backtest_run_id: str,
    strategy_version_id: str,
    objects: Sequence[DetailObjectDescriptor],
    source_set_hash: str,
    created_at: datetime,
    supersedes_manifest_id: str | None,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "result_manifest_id": result_manifest_id,
        "run_snapshot_id": run_snapshot_id,
        "backtest_run_id": backtest_run_id,
        "strategy_version_id": strategy_version_id,
        "source_set_hash": source_set_hash,
        "created_at": _timestamp(created_at),
        "supersedes_manifest_id": supersedes_manifest_id,
        "objects": [_descriptor_payload(item) for item in objects],
    }
