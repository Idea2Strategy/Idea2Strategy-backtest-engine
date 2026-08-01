"""Immutable ET-month Parquet details linked by relational manifests."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import pyarrow as pa
import pyarrow.parquet as pq

from backtest_engine.execution_model import LedgerTransaction
from backtest_engine.monthly_judgment import ET_TIMEZONE_ID, EtMonth
from backtest_engine.result_snapshot import (
    ResultRecord,
    ResultSnapshot,
    ResultSnapshotBuilder,
)


SCHEMA_VERSION = 1
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
SHA256 = re.compile(r"^[0-9a-f]{64}$")
ZERO = Decimal("0")


class DetailObjectValidationError(ValueError):
    """Raised when detail evidence cannot be bound unambiguously."""


class DetailIntegrityError(RuntimeError):
    """Raised when an immutable detail object or manifest is inconsistent."""


class DetailManifestConflict(RuntimeError):
    """Raised when one official result is assigned conflicting details."""


class DetailObjectKind(str, Enum):
    TRADE_DETAIL = "TRADE_DETAIL"
    POSITION_SNAPSHOT = "POSITION_SNAPSHOT"
    REPLAY_LEDGER = "REPLAY_LEDGER"
    CALCULATION_SERIES = "CALCULATION_SERIES"


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
        raise DetailObjectValidationError(
            f"{label} must be a lowercase SHA-256"
        )
    return value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise DetailObjectValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise DetailObjectValidationError(f"{label} must be a finite Decimal")
    return value


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize()
    return "0" if normalized == ZERO else format(normalized, "f")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, slots=True)
class ReplayLedgerDetail:
    run_snapshot_id: str
    transaction: LedgerTransaction

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "ledger.run_snapshot_id"),
        )
        if not isinstance(self.transaction, LedgerTransaction):
            raise DetailObjectValidationError(
                "transaction must be a LedgerTransaction"
            )


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
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "point.run_snapshot_id"),
        )
        object.__setattr__(
            self, "occurred_at", _utc(self.occurred_at, "occurred_at")
        )
        _text(self.metric_id, "metric_id")
        _decimal(self.value, "value")
        object.__setattr__(
            self,
            "instrument_id",
            _optional_uuid(self.instrument_id, "instrument_id"),
        )


@dataclass(frozen=True, slots=True)
class DetailObjectDescriptor:
    storage_object_id: str
    kind: DetailObjectKind
    et_month: EtMonth
    object_key: str
    media_type: str
    schema_version: int
    row_count: int
    byte_size: int
    content_hash: str
    created_at: datetime
    base_object_id: str | None = None
    correction_of_object_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "storage_object_id",
            _uuid(self.storage_object_id, "storage_object_id"),
        )
        if not isinstance(self.kind, DetailObjectKind):
            raise DetailObjectValidationError("object kind is unsupported")
        if not isinstance(self.et_month, EtMonth):
            raise DetailObjectValidationError("et_month must be an EtMonth")
        _text(self.object_key, "object_key")
        if self.media_type != PARQUET_MEDIA_TYPE:
            raise DetailObjectValidationError(
                f"media_type must be {PARQUET_MEDIA_TYPE}"
            )
        if self.schema_version != SCHEMA_VERSION:
            raise DetailObjectValidationError(
                f"schema_version must be {SCHEMA_VERSION}"
            )
        for label in ("row_count", "byte_size"):
            value = getattr(self, label)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise DetailObjectValidationError(
                    f"{label} must be a non-negative integer"
                )
        _hash(self.content_hash, "content_hash")
        object.__setattr__(
            self, "created_at", _utc(self.created_at, "created_at")
        )
        object.__setattr__(
            self,
            "base_object_id",
            _optional_uuid(self.base_object_id, "base_object_id"),
        )
        object.__setattr__(
            self,
            "correction_of_object_id",
            _optional_uuid(
                self.correction_of_object_id, "correction_of_object_id"
            ),
        )


@dataclass(frozen=True, slots=True)
class StoredDetailObject:
    descriptor: DetailObjectDescriptor
    parquet_bytes: bytes


@dataclass(frozen=True, slots=True)
class DetailObjectManifest:
    detail_manifest_id: str
    result_manifest_id: str
    run_snapshot_id: str
    backtest_run_id: str
    strategy_version_id: str
    objects: tuple[DetailObjectDescriptor, ...]
    manifest_hash: str
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "detail_manifest_id",
            _uuid(self.detail_manifest_id, "detail_manifest_id"),
        )
        object.__setattr__(
            self,
            "result_manifest_id",
            _uuid(self.result_manifest_id, "result_manifest_id"),
        )
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "manifest.run_snapshot_id"),
        )
        object.__setattr__(
            self,
            "backtest_run_id",
            _uuid(self.backtest_run_id, "manifest.backtest_run_id"),
        )
        object.__setattr__(
            self,
            "strategy_version_id",
            _uuid(self.strategy_version_id, "manifest.strategy_version_id"),
        )
        objects = tuple(self.objects)
        if any(not isinstance(item, DetailObjectDescriptor) for item in objects):
            raise DetailObjectValidationError(
                "objects must contain DetailObjectDescriptor values"
            )
        object_ids = [item.storage_object_id for item in objects]
        if len(set(object_ids)) != len(object_ids):
            raise DetailObjectValidationError(
                "storage_object_id values must be unique"
            )
        partitions = [(item.et_month, item.kind) for item in objects]
        if len(set(partitions)) != len(partitions):
            raise DetailObjectValidationError(
                "ET month and record kind partitions must be unique"
            )
        object.__setattr__(self, "objects", objects)
        _hash(self.manifest_hash, "manifest_hash")
        object.__setattr__(
            self, "created_at", _utc(self.created_at, "manifest.created_at")
        )

    def as_record(self) -> dict[str, object]:
        return {
            "detail_manifest_id": self.detail_manifest_id,
            "result_manifest_id": self.result_manifest_id,
            "run_snapshot_id": self.run_snapshot_id,
            "backtest_run_id": self.backtest_run_id,
            "strategy_version_id": self.strategy_version_id,
            "manifest_hash": self.manifest_hash,
            "created_at": _timestamp(self.created_at),
            "objects": [_descriptor_payload(item) for item in self.objects],
        }


@dataclass(frozen=True, slots=True)
class DetailObjectBundle:
    manifest: DetailObjectManifest
    objects: tuple[StoredDetailObject, ...]


class DetailObjectBuilder:
    """Builds deterministic Parquet partitions and their RDB manifest."""

    def build(
        self,
        result: ResultSnapshot,
        replay_ledger: Iterable[ReplayLedgerDetail],
        calculation_series: Iterable[PerformancePoint],
        created_at: datetime,
    ) -> DetailObjectBundle:
        ResultSnapshotBuilder.verify(result)
        created_at = _utc(created_at, "created_at")
        run = result.run_snapshot
        run_snapshot_id = run.snapshot_id
        ledger = tuple(replay_ledger)
        points = tuple(calculation_series)

        if any(not isinstance(item, ReplayLedgerDetail) for item in ledger):
            raise DetailObjectValidationError(
                "replay_ledger must contain ReplayLedgerDetail values"
            )
        if any(not isinstance(item, PerformancePoint) for item in points):
            raise DetailObjectValidationError(
                "calculation_series must contain PerformancePoint values"
            )
        transaction_ids = [item.transaction.transaction_id for item in ledger]
        if len(set(transaction_ids)) != len(transaction_ids):
            raise DetailObjectValidationError(
                "transaction_id values must be unique"
            )
        entry_ids = [
            entry.entry_id for item in ledger for entry in item.transaction.entries
        ]
        if len(set(entry_ids)) != len(entry_ids):
            raise DetailObjectValidationError("ledger entry_id values must be unique")
        point_ids = [item.point_id for item in points]
        if len(set(point_ids)) != len(point_ids):
            raise DetailObjectValidationError("point_id values must be unique")
        if any(item.run_snapshot_id != run_snapshot_id for item in ledger):
            raise DetailObjectValidationError(
                "every ledger transaction must reference the run snapshot"
            )
        if any(item.run_snapshot_id != run_snapshot_id for item in points):
            raise DetailObjectValidationError(
                "every calculation point must reference the run snapshot"
            )

        latest = [item.occurred_at for item in result.records]
        latest.extend(item.transaction.posted_at for item in ledger)
        latest.extend(item.occurred_at for item in points)
        if latest and max(latest) > created_at:
            raise DetailObjectValidationError(
                "created_at must not precede a detail record"
            )

        partitions: dict[
            tuple[EtMonth, DetailObjectKind], list[dict[str, object]]
        ] = {}
        for record in result.records:
            month = EtMonth.from_instant(record.occurred_at)
            partitions.setdefault(
                (month, DetailObjectKind.TRADE_DETAIL), []
            ).append(_trade_row(record))
            for position in record.positions_after:
                partitions.setdefault(
                    (month, DetailObjectKind.POSITION_SNAPSHOT), []
                ).append(_position_row(record, position))
        for item in ledger:
            month = EtMonth.from_instant(item.transaction.posted_at)
            rows = partitions.setdefault(
                (month, DetailObjectKind.REPLAY_LEDGER), []
            )
            rows.extend(_ledger_rows(item.transaction))
        for point in points:
            month = EtMonth.from_instant(point.occurred_at)
            partitions.setdefault(
                (month, DetailObjectKind.CALCULATION_SERIES), []
            ).append(_calculation_row(point))

        objects = tuple(
            self._build_object(
                result,
                month,
                kind,
                _sort_rows(kind, rows),
                created_at,
            )
            for (month, kind), rows in sorted(
                partitions.items(), key=lambda item: (item[0][0], item[0][1].value)
            )
        )
        descriptors = tuple(item.descriptor for item in objects)
        manifest_payload = _manifest_payload(
            result.manifest.result_manifest_id,
            run_snapshot_id,
            run.backtest_run_id,
            run.strategy_version_id,
            descriptors,
            created_at,
        )
        manifest_hash = _sha256(_canonical(manifest_payload))
        detail_manifest_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"idea2strategy:d27:manifest:{manifest_hash}",
            )
        )
        bundle = DetailObjectBundle(
            manifest=DetailObjectManifest(
                detail_manifest_id=detail_manifest_id,
                result_manifest_id=result.manifest.result_manifest_id,
                run_snapshot_id=run_snapshot_id,
                backtest_run_id=run.backtest_run_id,
                strategy_version_id=run.strategy_version_id,
                objects=descriptors,
                manifest_hash=manifest_hash,
                created_at=created_at,
            ),
            objects=objects,
        )
        self.verify(bundle)
        return bundle

    def _build_object(
        self,
        result: ResultSnapshot,
        month: EtMonth,
        kind: DetailObjectKind,
        rows: list[dict[str, object]],
        created_at: datetime,
    ) -> StoredDetailObject:
        metadata = {
            b"schema_version": str(SCHEMA_VERSION).encode(),
            b"run_snapshot_id": result.run_snapshot.snapshot_id.encode(),
            b"backtest_run_id": result.run_snapshot.backtest_run_id.encode(),
            b"strategy_version_id": result.run_snapshot.strategy_version_id.encode(),
            b"result_manifest_id": result.manifest.result_manifest_id.encode(),
            b"et_month": month.key.encode(),
            b"timezone_id": ET_TIMEZONE_ID.encode(),
            b"record_kind": kind.value.encode(),
        }
        schema = _schema(kind).with_metadata(metadata)
        table = pa.Table.from_pylist(rows, schema=schema)
        sink = pa.BufferOutputStream()
        pq.write_table(
            table,
            sink,
            compression="zstd",
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
            data_page_version="2.0",
            row_group_size=max(1, len(rows)),
        )
        parquet_bytes = sink.getvalue().to_pybytes()
        content_hash = _sha256(parquet_bytes)
        storage_object_id = _object_id(
            result.run_snapshot.snapshot_id, month, kind, content_hash
        )
        object_key = _object_key(
            result.run_snapshot.backtest_run_id, month, kind, content_hash
        )
        return StoredDetailObject(
            descriptor=DetailObjectDescriptor(
                storage_object_id=storage_object_id,
                kind=kind,
                et_month=month,
                object_key=object_key,
                media_type=PARQUET_MEDIA_TYPE,
                schema_version=SCHEMA_VERSION,
                row_count=len(rows),
                byte_size=len(parquet_bytes),
                content_hash=content_hash,
                created_at=created_at,
            ),
            parquet_bytes=parquet_bytes,
        )

    @staticmethod
    def verify(bundle: DetailObjectBundle) -> None:
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
            descriptor = item.descriptor
            if not isinstance(item.parquet_bytes, bytes):
                raise DetailIntegrityError("detail object content must be bytes")
            if len(item.parquet_bytes) != descriptor.byte_size:
                raise DetailIntegrityError("detail object size does not match")
            if _sha256(item.parquet_bytes) != descriptor.content_hash:
                raise DetailIntegrityError("detail object hash does not match")
            expected_id = _object_id(
                manifest.run_snapshot_id,
                descriptor.et_month,
                descriptor.kind,
                descriptor.content_hash,
            )
            if descriptor.storage_object_id != expected_id:
                raise DetailIntegrityError("detail object identity does not match")
            expected_key = _object_key(
                manifest.backtest_run_id,
                descriptor.et_month,
                descriptor.kind,
                descriptor.content_hash,
            )
            if descriptor.object_key != expected_key:
                raise DetailIntegrityError("detail object key does not match")
            try:
                parquet = pq.ParquetFile(pa.BufferReader(item.parquet_bytes))
            except (OSError, pa.ArrowException) as exc:
                raise DetailIntegrityError("detail object is not valid Parquet") from exc
            if parquet.metadata.num_rows != descriptor.row_count:
                raise DetailIntegrityError("detail object row count does not match")
            metadata = parquet.schema_arrow.metadata or {}
            expected_metadata = {
                b"schema_version": str(SCHEMA_VERSION).encode(),
                b"run_snapshot_id": manifest.run_snapshot_id.encode(),
                b"backtest_run_id": manifest.backtest_run_id.encode(),
                b"strategy_version_id": manifest.strategy_version_id.encode(),
                b"result_manifest_id": manifest.result_manifest_id.encode(),
                b"et_month": descriptor.et_month.key.encode(),
                b"timezone_id": ET_TIMEZONE_ID.encode(),
                b"record_kind": descriptor.kind.value.encode(),
            }
            if metadata != expected_metadata:
                raise DetailIntegrityError("detail object metadata does not match")
            for row in parquet.read(columns=[_time_field(descriptor.kind)]).to_pylist():
                if EtMonth.from_instant(row[_time_field(descriptor.kind)]) != descriptor.et_month:
                    raise DetailIntegrityError("detail row is in the wrong ET month")

        expected_payload = _manifest_payload(
            manifest.result_manifest_id,
            manifest.run_snapshot_id,
            manifest.backtest_run_id,
            manifest.strategy_version_id,
            manifest.objects,
            manifest.created_at,
        )
        expected_hash = _sha256(_canonical(expected_payload))
        if manifest.manifest_hash != expected_hash:
            raise DetailIntegrityError("detail manifest hash does not match")
        expected_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"idea2strategy:d27:manifest:{expected_hash}",
            )
        )
        if manifest.detail_manifest_id != expected_id:
            raise DetailIntegrityError("detail manifest identity does not match")


class InMemoryDetailManifestStore:
    """Testable immutable boundary for future RDB and object-store adapters."""

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
            raise DetailManifestConflict(
                "result manifest already has different official detail objects"
            )
        self._by_manifest_id[bundle.manifest.detail_manifest_id] = bundle
        self._detail_by_result_manifest[result_manifest_id] = (
            bundle.manifest.detail_manifest_id
        )
        return bundle.manifest

    def get(self, detail_manifest_id: str) -> DetailObjectBundle:
        detail_manifest_id = _uuid(detail_manifest_id, "detail_manifest_id")
        try:
            bundle = self._by_manifest_id[detail_manifest_id]
        except KeyError as exc:
            raise KeyError(
                f"detail manifest not found: {detail_manifest_id}"
            ) from exc
        DetailObjectBuilder.verify(bundle)
        return bundle


def _trade_row(record: ResultRecord) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "occurred_at": record.occurred_at,
        "kind": record.kind.value,
        "order_id": record.order_id,
        "instrument_id": record.instrument_id,
        "order_status": record.order_status.value,
        "cash_after": _decimal_text(record.cash_after),
        "reason_code": record.reason_code,
        "fill_id": record.fill_id,
        "quantity": _decimal_text(record.quantity),
        "base_price": _decimal_text(record.base_price),
        "price": _decimal_text(record.price),
        "gross_amount": _decimal_text(record.gross_amount),
        "slippage_amount": _decimal_text(record.slippage_amount),
        "fee": _decimal_text(record.fee),
        "cost_basis": _decimal_text(record.cost_basis),
        "realized_pnl": _decimal_text(record.realized_pnl),
    }


def _position_row(record: ResultRecord, position: object) -> dict[str, object]:
    return {
        "record_id": record.record_id,
        "occurred_at": record.occurred_at,
        "instrument_id": position.instrument_id,
        "quantity": _decimal_text(position.quantity),
        "cost_basis": _decimal_text(position.cost_basis),
        "cash_after": _decimal_text(record.cash_after),
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
            "amount": _decimal_text(entry.amount),
            "currency": entry.currency,
        }
        for entry in transaction.entries
    ]


def _calculation_row(point: PerformancePoint) -> dict[str, object]:
    return {
        "point_id": point.point_id,
        "occurred_at": point.occurred_at,
        "metric_id": point.metric_id,
        "value": _decimal_text(point.value),
        "instrument_id": point.instrument_id,
    }


def _schema(kind: DetailObjectKind) -> pa.Schema:
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
    }[kind]
    return pa.schema(fields)


def _time_field(kind: DetailObjectKind) -> str:
    return "posted_at" if kind is DetailObjectKind.REPLAY_LEDGER else "occurred_at"


def _sort_rows(
    kind: DetailObjectKind, rows: list[dict[str, object]]
) -> list[dict[str, object]]:
    identifiers = {
        DetailObjectKind.TRADE_DETAIL: ("occurred_at", "record_id"),
        DetailObjectKind.POSITION_SNAPSHOT: (
            "occurred_at",
            "record_id",
            "instrument_id",
        ),
        DetailObjectKind.REPLAY_LEDGER: (
            "posted_at",
            "transaction_id",
            "entry_id",
        ),
        DetailObjectKind.CALCULATION_SERIES: ("occurred_at", "point_id"),
    }[kind]
    return sorted(rows, key=lambda row: tuple(row[field] for field in identifiers))


def _object_id(
    run_snapshot_id: str,
    month: EtMonth,
    kind: DetailObjectKind,
    content_hash: str,
) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "idea2strategy:d27:object:"
            f"{run_snapshot_id}:{month.key}:{kind.value}:{content_hash}",
        )
    )


def _object_key(
    backtest_run_id: str,
    month: EtMonth,
    kind: DetailObjectKind,
    content_hash: str,
) -> str:
    return (
        f"backtest-details/run={backtest_run_id}/et_month={month.key}/"
        f"kind={kind.value.lower()}/{content_hash}.parquet"
    )


def _descriptor_payload(descriptor: DetailObjectDescriptor) -> dict[str, object]:
    return {
        "storage_object_id": descriptor.storage_object_id,
        "record_kind": descriptor.kind.value,
        "et_month": descriptor.et_month.key,
        "timezone_id": ET_TIMEZONE_ID,
        "object_key": descriptor.object_key,
        "media_type": descriptor.media_type,
        "schema_version": descriptor.schema_version,
        "row_count": descriptor.row_count,
        "byte_size": descriptor.byte_size,
        "content_hash": descriptor.content_hash,
        "created_at": _timestamp(descriptor.created_at),
        "base_object_id": descriptor.base_object_id,
        "correction_of_object_id": descriptor.correction_of_object_id,
    }


def _manifest_payload(
    result_manifest_id: str,
    run_snapshot_id: str,
    backtest_run_id: str,
    strategy_version_id: str,
    objects: tuple[DetailObjectDescriptor, ...],
    created_at: datetime,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "result_manifest_id": result_manifest_id,
        "run_snapshot_id": run_snapshot_id,
        "backtest_run_id": backtest_run_id,
        "strategy_version_id": strategy_version_id,
        "created_at": _timestamp(created_at),
        "objects": [_descriptor_payload(item) for item in objects],
    }
