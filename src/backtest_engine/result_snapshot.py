"""Immutable D25 trade-detail and compact performance result snapshots."""

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

from backtest_engine.execution_model import (
    BacktestOrder,
    Fill,
    OrderStatus,
)


ZERO = Decimal("0")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SCHEMA_VERSION = 1
MEDIA_TYPE = "application/vnd.idea2strategy.backtest-results+json"


class ResultSnapshotValidationError(ValueError):
    """Raised when a result cannot preserve its exact execution meaning."""


class ResultIntegrityError(RuntimeError):
    """Raised when immutable result evidence does not match its manifest."""


class ResultSnapshotConflict(RuntimeError):
    """Raised when one run snapshot is assigned conflicting official results."""


class ResultRecordKind(str, Enum):
    ORDER = "ORDER"
    FILL = "FILL"
    CANCELLATION = "CANCELLATION"
    REJECTION = "REJECTION"


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ResultSnapshotValidationError(f"{label} must be a non-empty string")
    return value


def _uuid(value: str, label: str) -> str:
    _text(value, label)
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ResultSnapshotValidationError(f"{label} must be a UUID") from exc


def _hash(value: str, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise ResultSnapshotValidationError(
            f"{label} must be a lowercase SHA-256"
        )
    return value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ResultSnapshotValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _decimal(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ResultSnapshotValidationError(f"{label} must be a finite Decimal")
    return value


def _non_negative(value: Decimal, label: str) -> Decimal:
    value = _decimal(value, label)
    if value < ZERO:
        raise ResultSnapshotValidationError(f"{label} must be non-negative")
    return value


def _positive(value: Decimal, label: str) -> Decimal:
    value = _decimal(value, label)
    if value <= ZERO:
        raise ResultSnapshotValidationError(f"{label} must be positive")
    return value


def _decimal_text(value: Decimal) -> str:
    normalized = value.normalize()
    return "0" if normalized == ZERO else format(normalized, "f")


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, slots=True)
class RunSnapshot:
    backtest_run_id: str
    strategy_version_id: str
    input_bundle_fingerprint: str
    calculation_model_version: str
    cost_model_version: str
    execution_model_version: str
    initial_cash: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "backtest_run_id",
            _uuid(self.backtest_run_id, "backtest_run_id"),
        )
        object.__setattr__(
            self,
            "strategy_version_id",
            _uuid(self.strategy_version_id, "strategy_version_id"),
        )
        object.__setattr__(
            self,
            "input_bundle_fingerprint",
            _hash(self.input_bundle_fingerprint, "input_bundle_fingerprint"),
        )
        for field in (
            "calculation_model_version",
            "cost_model_version",
            "execution_model_version",
        ):
            _text(getattr(self, field), field)
        _non_negative(self.initial_cash, "initial_cash")

    @property
    def snapshot_id(self) -> str:
        return _sha256(_canonical_bytes(_run_payload(self)))


@dataclass(frozen=True, slots=True, order=True)
class PositionAfter:
    instrument_id: str
    quantity: Decimal
    cost_basis: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "instrument_id",
            _uuid(self.instrument_id, "position.instrument_id"),
        )
        _non_negative(self.quantity, "position.quantity")
        _non_negative(self.cost_basis, "position.cost_basis")
        if self.quantity == ZERO and self.cost_basis != ZERO:
            raise ResultSnapshotValidationError(
                "zero position quantity requires zero cost_basis"
            )


@dataclass(frozen=True, slots=True)
class ResultRecord:
    run_snapshot_id: str
    record_id: str
    kind: ResultRecordKind
    occurred_at: datetime
    order_id: str
    instrument_id: str
    order_status: OrderStatus
    cash_after: Decimal
    positions_after: tuple[PositionAfter, ...]
    reason_code: str | None = None
    fill_id: str | None = None
    quantity: Decimal | None = None
    base_price: Decimal | None = None
    price: Decimal | None = None
    gross_amount: Decimal | None = None
    slippage_amount: Decimal | None = None
    fee: Decimal | None = None
    cost_basis: Decimal | None = None
    realized_pnl: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "run_snapshot_id"),
        )
        object.__setattr__(self, "record_id", _uuid(self.record_id, "record_id"))
        if not isinstance(self.kind, ResultRecordKind):
            raise ResultSnapshotValidationError("kind is unsupported")
        object.__setattr__(
            self, "occurred_at", _utc(self.occurred_at, "occurred_at")
        )
        object.__setattr__(self, "order_id", _uuid(self.order_id, "order_id"))
        object.__setattr__(
            self,
            "instrument_id",
            _uuid(self.instrument_id, "instrument_id"),
        )
        if not isinstance(self.order_status, OrderStatus):
            raise ResultSnapshotValidationError("order_status is unsupported")
        _non_negative(self.cash_after, "cash_after")

        positions = tuple(self.positions_after)
        if any(not isinstance(item, PositionAfter) for item in positions):
            raise ResultSnapshotValidationError(
                "positions_after must contain PositionAfter values"
            )
        if len({item.instrument_id for item in positions}) != len(positions):
            raise ResultSnapshotValidationError(
                "positions_after instrument_id values must be unique"
            )
        object.__setattr__(
            self,
            "positions_after",
            tuple(sorted(positions, key=lambda item: item.instrument_id)),
        )

        if self.kind is ResultRecordKind.FILL:
            self._validate_fill()
        else:
            self._validate_order_event()

    def _validate_fill(self) -> None:
        fields = (
            self.fill_id,
            self.quantity,
            self.base_price,
            self.price,
            self.gross_amount,
            self.slippage_amount,
            self.fee,
            self.cost_basis,
            self.realized_pnl,
        )
        if any(value is None for value in fields):
            raise ResultSnapshotValidationError(
                "fill fields must all be supplied for FILL records"
            )
        assert self.fill_id is not None
        object.__setattr__(self, "fill_id", _uuid(self.fill_id, "fill_id"))
        assert self.quantity is not None
        assert self.base_price is not None
        assert self.price is not None
        assert self.gross_amount is not None
        assert self.slippage_amount is not None
        assert self.fee is not None
        assert self.cost_basis is not None
        assert self.realized_pnl is not None
        _positive(self.quantity, "quantity")
        _positive(self.base_price, "base_price")
        _positive(self.price, "price")
        _positive(self.gross_amount, "gross_amount")
        _non_negative(self.slippage_amount, "slippage_amount")
        _non_negative(self.fee, "fee")
        _non_negative(self.cost_basis, "cost_basis")
        _decimal(self.realized_pnl, "realized_pnl")
        if self.gross_amount != self.price * self.quantity:
            raise ResultSnapshotValidationError(
                "gross_amount must equal price times quantity"
            )
        expected_slippage = abs(self.price - self.base_price) * self.quantity
        if self.slippage_amount != expected_slippage:
            raise ResultSnapshotValidationError(
                "slippage_amount must match base and final prices"
            )
        if self.order_status not in {
            OrderStatus.PARTIALLY_FILLED,
            OrderStatus.FILLED,
        }:
            raise ResultSnapshotValidationError(
                "FILL order_status must be PARTIALLY_FILLED or FILLED"
            )
        if self.reason_code is not None:
            raise ResultSnapshotValidationError(
                "FILL records must not contain reason_code"
            )

    def _validate_order_event(self) -> None:
        fill_values = (
            self.fill_id,
            self.quantity,
            self.base_price,
            self.price,
            self.gross_amount,
            self.slippage_amount,
            self.fee,
            self.cost_basis,
            self.realized_pnl,
        )
        if any(value is not None for value in fill_values):
            raise ResultSnapshotValidationError(
                "fill fields are allowed only for FILL records"
            )
        expected = {
            ResultRecordKind.ORDER: {OrderStatus.ACCEPTED},
            ResultRecordKind.CANCELLATION: {
                OrderStatus.CANCELLED,
                OrderStatus.EXPIRED,
            },
            ResultRecordKind.REJECTION: {OrderStatus.REJECTED},
        }[self.kind]
        if self.order_status not in expected:
            raise ResultSnapshotValidationError(
                f"{self.kind.value} has an incompatible order_status"
            )
        if self.kind is ResultRecordKind.ORDER:
            if self.reason_code is not None:
                raise ResultSnapshotValidationError(
                    "accepted ORDER must not contain reason_code"
                )
        else:
            _text(self.reason_code, "reason_code")


@dataclass(frozen=True, slots=True)
class PerformanceSummary:
    run_snapshot_id: str
    order_count: int
    fill_count: int
    cancellation_count: int
    rejection_count: int
    total_fees: Decimal
    total_slippage: Decimal
    realized_pnl: Decimal
    initial_cash: Decimal
    ending_cash: Decimal
    ending_positions: tuple[PositionAfter, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "summary.run_snapshot_id"),
        )
        for field in (
            "order_count",
            "fill_count",
            "cancellation_count",
            "rejection_count",
        ):
            value = getattr(self, field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ResultSnapshotValidationError(
                    f"summary.{field} must be a non-negative integer"
                )
        _non_negative(self.total_fees, "summary.total_fees")
        _non_negative(self.total_slippage, "summary.total_slippage")
        _decimal(self.realized_pnl, "summary.realized_pnl")
        _non_negative(self.initial_cash, "summary.initial_cash")
        _non_negative(self.ending_cash, "summary.ending_cash")
        positions = tuple(self.ending_positions)
        if any(not isinstance(item, PositionAfter) for item in positions):
            raise ResultSnapshotValidationError(
                "summary.ending_positions must contain PositionAfter values"
            )
        if len({item.instrument_id for item in positions}) != len(positions):
            raise ResultSnapshotValidationError(
                "summary ending position instrument_id values must be unique"
            )
        object.__setattr__(
            self,
            "ending_positions",
            tuple(sorted(positions, key=lambda item: item.instrument_id)),
        )


@dataclass(frozen=True, slots=True)
class ResultObjectManifest:
    result_manifest_id: str
    run_snapshot_id: str
    backtest_run_id: str
    strategy_version_id: str
    object_key: str
    media_type: str
    schema_version: int
    record_count: int
    byte_size: int
    content_hash: str
    summary_hash: str
    completed_at: datetime

    def __post_init__(self) -> None:
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
        _text(self.object_key, "manifest.object_key")
        if self.media_type != MEDIA_TYPE:
            raise ResultSnapshotValidationError(
                f"manifest.media_type must be {MEDIA_TYPE}"
            )
        if self.schema_version != SCHEMA_VERSION:
            raise ResultSnapshotValidationError(
                f"manifest.schema_version must be {SCHEMA_VERSION}"
            )
        for field in ("record_count", "byte_size"):
            value = getattr(self, field)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ResultSnapshotValidationError(
                    f"manifest.{field} must be a non-negative integer"
                )
        _hash(self.content_hash, "manifest.content_hash")
        _hash(self.summary_hash, "manifest.summary_hash")
        object.__setattr__(
            self,
            "completed_at",
            _utc(self.completed_at, "manifest.completed_at"),
        )


@dataclass(frozen=True, slots=True)
class ResultSnapshot:
    run_snapshot: RunSnapshot
    records: tuple[ResultRecord, ...]
    summary: PerformanceSummary
    manifest: ResultObjectManifest
    object_bytes: bytes

    def completion_fields(self) -> dict[str, str]:
        return {"result_manifest_id": self.manifest.result_manifest_id}


def order_result_record(
    run_snapshot: RunSnapshot,
    order: BacktestOrder,
    occurred_at: datetime,
    cash_after: Decimal,
    positions_after: Iterable[PositionAfter],
) -> ResultRecord:
    if not isinstance(run_snapshot, RunSnapshot):
        raise ResultSnapshotValidationError("run_snapshot must be a RunSnapshot")
    if not isinstance(order, BacktestOrder):
        raise ResultSnapshotValidationError("order must be a BacktestOrder")
    occurred_at = _utc(occurred_at, "occurred_at")
    kind = {
        OrderStatus.ACCEPTED: ResultRecordKind.ORDER,
        OrderStatus.CANCELLED: ResultRecordKind.CANCELLATION,
        OrderStatus.EXPIRED: ResultRecordKind.CANCELLATION,
        OrderStatus.REJECTED: ResultRecordKind.REJECTION,
    }.get(order.status)
    if kind is None:
        raise ResultSnapshotValidationError(
            "PARTIALLY_FILLED and FILLED orders require fill_result_record"
        )
    record_id = _event_id(
        run_snapshot.snapshot_id,
        kind,
        order.order_id,
        occurred_at,
        order.status,
        None,
    )
    return ResultRecord(
        run_snapshot_id=run_snapshot.snapshot_id,
        record_id=record_id,
        kind=kind,
        occurred_at=occurred_at,
        order_id=order.order_id,
        instrument_id=order.instrument_id,
        order_status=order.status,
        cash_after=cash_after,
        positions_after=tuple(positions_after),
        reason_code=order.reason_code,
    )


def fill_result_record(
    run_snapshot: RunSnapshot,
    fill: Fill,
    order_after: BacktestOrder,
    cash_after: Decimal,
    positions_after: Iterable[PositionAfter],
) -> ResultRecord:
    if not isinstance(run_snapshot, RunSnapshot):
        raise ResultSnapshotValidationError("run_snapshot must be a RunSnapshot")
    if not isinstance(fill, Fill):
        raise ResultSnapshotValidationError("fill must be a Fill")
    if not isinstance(order_after, BacktestOrder):
        raise ResultSnapshotValidationError(
            "order_after must be a BacktestOrder"
        )
    if fill.order_id != order_after.order_id:
        raise ResultSnapshotValidationError("fill and order_after order_id must match")
    if fill.instrument_id != order_after.instrument_id:
        raise ResultSnapshotValidationError(
            "fill and order_after instrument_id must match"
        )
    if order_after.status not in {
        OrderStatus.PARTIALLY_FILLED,
        OrderStatus.FILLED,
    }:
        raise ResultSnapshotValidationError(
            "order_after status must reflect an individual fill"
        )
    record_id = _event_id(
        run_snapshot.snapshot_id,
        ResultRecordKind.FILL,
        fill.order_id,
        fill.occurred_at,
        order_after.status,
        fill.fill_id,
    )
    return ResultRecord(
        run_snapshot_id=run_snapshot.snapshot_id,
        record_id=record_id,
        kind=ResultRecordKind.FILL,
        occurred_at=fill.occurred_at,
        order_id=fill.order_id,
        instrument_id=fill.instrument_id,
        order_status=order_after.status,
        cash_after=cash_after,
        positions_after=tuple(positions_after),
        fill_id=fill.fill_id,
        quantity=fill.quantity,
        base_price=fill.base_price,
        price=fill.price,
        gross_amount=fill.gross_amount,
        slippage_amount=fill.slippage_amount,
        fee=fill.fee,
        cost_basis=fill.cost_basis,
        realized_pnl=fill.realized_pnl,
    )


class ResultSnapshotBuilder:
    """Builds and verifies storage-agnostic immutable result evidence."""

    def build(
        self,
        run_snapshot: RunSnapshot,
        records: Iterable[ResultRecord],
        completed_at: datetime,
    ) -> ResultSnapshot:
        if not isinstance(run_snapshot, RunSnapshot):
            raise ResultSnapshotValidationError(
                "run_snapshot must be a RunSnapshot"
            )
        completed_at = _utc(completed_at, "completed_at")
        supplied = tuple(records)
        if any(not isinstance(item, ResultRecord) for item in supplied):
            raise ResultSnapshotValidationError(
                "records must contain ResultRecord values"
            )
        if len({item.record_id for item in supplied}) != len(supplied):
            raise ResultSnapshotValidationError("record_id values must be unique")
        fill_ids = [
            item.fill_id
            for item in supplied
            if item.kind is ResultRecordKind.FILL
        ]
        if len(set(fill_ids)) != len(fill_ids):
            raise ResultSnapshotValidationError("fill_id values must be unique")
        if any(item.run_snapshot_id != run_snapshot.snapshot_id for item in supplied):
            raise ResultSnapshotValidationError(
                "every record must reference the same run snapshot"
            )
        ordered = tuple(sorted(supplied, key=_record_key))
        if ordered and ordered[-1].occurred_at > completed_at:
            raise ResultSnapshotValidationError(
                "completed_at must not precede a result record"
            )

        summary = _performance_summary(run_snapshot, ordered)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "run_snapshot": _run_payload(run_snapshot),
            "records": [_record_payload(item) for item in ordered],
        }
        object_bytes = _canonical_bytes(payload)
        content_hash = _sha256(object_bytes)
        summary_hash = _sha256(_canonical_bytes(_summary_payload(summary)))
        manifest_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                "idea2strategy:d25:manifest:"
                f"{run_snapshot.backtest_run_id}:{run_snapshot.snapshot_id}:"
                f"{content_hash}:{summary_hash}",
            )
        )
        manifest = ResultObjectManifest(
            result_manifest_id=manifest_id,
            run_snapshot_id=run_snapshot.snapshot_id,
            backtest_run_id=run_snapshot.backtest_run_id,
            strategy_version_id=run_snapshot.strategy_version_id,
            object_key=(
                f"backtest-results/{run_snapshot.backtest_run_id}/"
                f"{content_hash}.json"
            ),
            media_type=MEDIA_TYPE,
            schema_version=SCHEMA_VERSION,
            record_count=len(ordered),
            byte_size=len(object_bytes),
            content_hash=content_hash,
            summary_hash=summary_hash,
            completed_at=completed_at,
        )
        result = ResultSnapshot(
            run_snapshot=run_snapshot,
            records=ordered,
            summary=summary,
            manifest=manifest,
            object_bytes=object_bytes,
        )
        self.verify(result)
        return result

    @staticmethod
    def verify(result: ResultSnapshot) -> None:
        if not isinstance(result, ResultSnapshot):
            raise ResultIntegrityError("result snapshot type is invalid")
        manifest = result.manifest
        if any(not isinstance(item, ResultRecord) for item in result.records):
            raise ResultIntegrityError("result records contain an invalid type")
        if tuple(sorted(result.records, key=_record_key)) != result.records:
            raise ResultIntegrityError("result records are not canonically ordered")
        if len({item.record_id for item in result.records}) != len(result.records):
            raise ResultIntegrityError("result record identity is duplicated")
        fill_ids = [
            item.fill_id
            for item in result.records
            if item.kind is ResultRecordKind.FILL
        ]
        if len(set(fill_ids)) != len(fill_ids):
            raise ResultIntegrityError("result fill identity is duplicated")
        if any(
            item.run_snapshot_id != result.run_snapshot.snapshot_id
            for item in result.records
        ):
            raise ResultIntegrityError("result record run snapshot does not match")
        if (
            result.records
            and result.records[-1].occurred_at > manifest.completed_at
        ):
            raise ResultIntegrityError("result completion precedes a detail record")
        if not isinstance(result.object_bytes, bytes):
            raise ResultIntegrityError("result content must be bytes")
        if len(result.object_bytes) != manifest.byte_size:
            raise ResultIntegrityError("result content byte size does not match")
        if _sha256(result.object_bytes) != manifest.content_hash:
            raise ResultIntegrityError("result content hash does not match")
        expected_payload = {
            "schema_version": SCHEMA_VERSION,
            "run_snapshot": _run_payload(result.run_snapshot),
            "records": [_record_payload(item) for item in result.records],
        }
        if result.object_bytes != _canonical_bytes(expected_payload):
            raise ResultIntegrityError("result content does not match records")
        if manifest.record_count != len(result.records):
            raise ResultIntegrityError("result content record count does not match")
        if manifest.run_snapshot_id != result.run_snapshot.snapshot_id:
            raise ResultIntegrityError("manifest run snapshot does not match")
        if manifest.backtest_run_id != result.run_snapshot.backtest_run_id:
            raise ResultIntegrityError("manifest backtest run does not match")
        if manifest.strategy_version_id != result.run_snapshot.strategy_version_id:
            raise ResultIntegrityError("manifest strategy version does not match")
        expected_summary = _performance_summary(
            result.run_snapshot, result.records
        )
        if expected_summary != result.summary:
            raise ResultIntegrityError("result summary does not match detail records")
        summary_hash = _sha256(
            _canonical_bytes(_summary_payload(result.summary))
        )
        if summary_hash != manifest.summary_hash:
            raise ResultIntegrityError("result summary hash does not match")
        expected_key = (
            f"backtest-results/{result.run_snapshot.backtest_run_id}/"
            f"{manifest.content_hash}.json"
        )
        if manifest.object_key != expected_key:
            raise ResultIntegrityError("result content object key does not match")
        expected_manifest_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                "idea2strategy:d25:manifest:"
                f"{result.run_snapshot.backtest_run_id}:"
                f"{result.run_snapshot.snapshot_id}:"
                f"{manifest.content_hash}:{manifest.summary_hash}",
            )
        )
        if manifest.result_manifest_id != expected_manifest_id:
            raise ResultIntegrityError("result manifest identity does not match")


class InMemoryResultSnapshotStore:
    """Testable immutable boundary for future object/RDB adapters."""

    def __init__(self) -> None:
        self._by_manifest_id: dict[str, ResultSnapshot] = {}
        self._manifest_by_run_snapshot: dict[str, str] = {}
        self._manifest_by_backtest_run: dict[str, str] = {}

    def put(self, result: ResultSnapshot) -> ResultObjectManifest:
        ResultSnapshotBuilder.verify(result)
        snapshot_id = result.run_snapshot.snapshot_id
        existing_id = self._manifest_by_run_snapshot.get(snapshot_id)
        if existing_id is not None:
            existing = self._by_manifest_id[existing_id]
            if existing == result:
                return existing.manifest
            raise ResultSnapshotConflict(
                "run snapshot already has a different immutable result"
            )
        run_id = result.run_snapshot.backtest_run_id
        existing_id = self._manifest_by_backtest_run.get(run_id)
        if existing_id is not None:
            raise ResultSnapshotConflict(
                "backtest run already has a different immutable snapshot result"
            )
        self._by_manifest_id[result.manifest.result_manifest_id] = result
        self._manifest_by_run_snapshot[snapshot_id] = (
            result.manifest.result_manifest_id
        )
        self._manifest_by_backtest_run[run_id] = result.manifest.result_manifest_id
        return result.manifest

    def get(self, result_manifest_id: str) -> ResultSnapshot:
        result_manifest_id = _uuid(result_manifest_id, "result_manifest_id")
        try:
            result = self._by_manifest_id[result_manifest_id]
        except KeyError as exc:
            raise KeyError(
                f"result manifest not found: {result_manifest_id}"
            ) from exc
        ResultSnapshotBuilder.verify(result)
        return result


def _event_id(
    run_snapshot_id: str,
    kind: ResultRecordKind,
    order_id: str,
    occurred_at: datetime,
    order_status: OrderStatus,
    fill_id: str | None,
) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            "idea2strategy:d25:record:"
            f"{run_snapshot_id}:{kind.value}:{order_id}:"
            f"{_timestamp(occurred_at)}:{order_status.value}:{fill_id or '-'}",
        )
    )


def _performance_summary(
    run_snapshot: RunSnapshot,
    records: Iterable[ResultRecord],
) -> PerformanceSummary:
    ordered = tuple(sorted(records, key=_record_key))
    fills = tuple(
        item for item in ordered if item.kind is ResultRecordKind.FILL
    )
    return PerformanceSummary(
        run_snapshot_id=run_snapshot.snapshot_id,
        order_count=len({item.order_id for item in ordered}),
        fill_count=len(fills),
        cancellation_count=sum(
            item.kind is ResultRecordKind.CANCELLATION for item in ordered
        ),
        rejection_count=sum(
            item.kind is ResultRecordKind.REJECTION for item in ordered
        ),
        total_fees=sum((item.fee or ZERO for item in fills), ZERO),
        total_slippage=sum(
            (item.slippage_amount or ZERO for item in fills), ZERO
        ),
        realized_pnl=sum((item.realized_pnl or ZERO for item in fills), ZERO),
        initial_cash=run_snapshot.initial_cash,
        ending_cash=(ordered[-1].cash_after if ordered else run_snapshot.initial_cash),
        ending_positions=(ordered[-1].positions_after if ordered else ()),
    )


def _run_payload(run: RunSnapshot) -> dict[str, object]:
    return {
        "backtest_run_id": run.backtest_run_id,
        "strategy_version_id": run.strategy_version_id,
        "input_bundle_fingerprint": run.input_bundle_fingerprint,
        "calculation_model_version": run.calculation_model_version,
        "cost_model_version": run.cost_model_version,
        "execution_model_version": run.execution_model_version,
        "initial_cash": _decimal_text(run.initial_cash),
    }


def _position_payload(position: PositionAfter) -> dict[str, str]:
    return {
        "instrument_id": position.instrument_id,
        "quantity": _decimal_text(position.quantity),
        "cost_basis": _decimal_text(position.cost_basis),
    }


def _record_payload(record: ResultRecord) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_snapshot_id": record.run_snapshot_id,
        "record_id": record.record_id,
        "kind": record.kind.value,
        "occurred_at": _timestamp(record.occurred_at),
        "order_id": record.order_id,
        "instrument_id": record.instrument_id,
        "order_status": record.order_status.value,
        "cash_after": _decimal_text(record.cash_after),
        "positions_after": [
            _position_payload(item) for item in record.positions_after
        ],
    }
    if record.reason_code is not None:
        payload["reason_code"] = record.reason_code
    if record.kind is ResultRecordKind.FILL:
        assert record.fill_id is not None
        payload.update(
            {
                "fill_id": record.fill_id,
                "quantity": _decimal_text(record.quantity or ZERO),
                "base_price": _decimal_text(record.base_price or ZERO),
                "price": _decimal_text(record.price or ZERO),
                "gross_amount": _decimal_text(record.gross_amount or ZERO),
                "slippage_amount": _decimal_text(
                    record.slippage_amount or ZERO
                ),
                "fee": _decimal_text(record.fee or ZERO),
                "cost_basis": _decimal_text(record.cost_basis or ZERO),
                "realized_pnl": _decimal_text(record.realized_pnl or ZERO),
            }
        )
    return payload


def _summary_payload(summary: PerformanceSummary) -> dict[str, object]:
    return {
        "run_snapshot_id": summary.run_snapshot_id,
        "order_count": summary.order_count,
        "fill_count": summary.fill_count,
        "cancellation_count": summary.cancellation_count,
        "rejection_count": summary.rejection_count,
        "total_fees": _decimal_text(summary.total_fees),
        "total_slippage": _decimal_text(summary.total_slippage),
        "realized_pnl": _decimal_text(summary.realized_pnl),
        "initial_cash": _decimal_text(summary.initial_cash),
        "ending_cash": _decimal_text(summary.ending_cash),
        "ending_positions": [
            _position_payload(item) for item in summary.ending_positions
        ],
    }


def _record_key(record: ResultRecord) -> tuple[datetime, int, str]:
    rank = {
        ResultRecordKind.ORDER: 0,
        ResultRecordKind.FILL: 1,
        ResultRecordKind.CANCELLATION: 2,
        ResultRecordKind.REJECTION: 3,
    }[record.kind]
    return record.occurred_at, rank, record.record_id
