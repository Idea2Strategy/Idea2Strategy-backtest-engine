"""Immutable D25 trade-detail and compact performance result snapshots.

The detail object (``records``) is the evidence; :class:`PerformanceSummary` is
the derived, re-computable view of it that becomes one
``backtest.performance_summaries`` row. Nothing in the summary is copied out of
a record: cash is re-walked from the fill ledger, positions come from the last
fill, and every metric is produced by :mod:`backtest_engine.performance` under
the versions it declares.

Four canonical hashes identify a summary and are all computed over
**post-quantization** values (spec 2.3, ``precision:1.0.0``):

``run_snapshot_id``
    The pinned run inputs (``RunSnapshot.snapshot_id``).
``source_set_hash``
    The run snapshot plus the exact detail record set. Independent of how the
    run is valued, so two valuations of the same execution share it.
``input_hash``
    ``source_set_hash`` plus the valuation grid, the metric catalog and
    calculation rules versions, the precision rules version and the calculation
    instant. Everything a re-computation needs.
``result_hash``
    ``input_hash`` plus the equity curve and the exact decimal text of every
    metric. Two runs with the same ``input_hash`` and different ``result_hash``
    are a reproducibility failure.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Any

from backtest_engine.execution_model import (
    BacktestOrder,
    Fill,
    OrderStatus,
)
from backtest_engine.money import (
    PRECISION_RULES_VERSION,
    format_money,
    quantize_money,
    quantize_quantity,
)
from backtest_engine.performance import (
    CALCULATION_RULES_VERSION,
    METRIC_CATALOG_VERSION,
    EquityCurve,
    LedgerEvent,
    MarkPrice,
    MetricSet,
    PositionState,
    TradeStatistics,
    ValuationBasis,
    ValuationInstant,
    ValuationPeriodicity,
    ValuationSeries,
    build_equity_curve,
    build_metrics,
    metrics_hash_material,
)
from backtest_engine.performance import (
    metrics_document as _render_metrics_document,
)


ZERO = Decimal("0")
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SCHEMA_VERSION = 1
MEDIA_TYPE = "application/vnd.idea2strategy.backtest-results+json"

SOURCE_SET_HASH_DOMAIN = "backtest.performance.source_set:1.0.0"
INPUT_HASH_DOMAIN = "backtest.performance.input:1.0.0"
RESULT_HASH_DOMAIN = "backtest.performance.result:1.0.0"


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
        # Stored evidence is quantized evidence: a position that cannot survive
        # `numeric(24,8)` must not be representable here at all. Quantity keeps
        # 8 places because a fractional-eligible instrument may hold them; a
        # whole-share instrument is enforced upstream by the execution model.
        object.__setattr__(
            self,
            "quantity",
            quantize_quantity(
                self.quantity,
                fractional_eligible=True,
                label="position.quantity",
            ),
        )
        object.__setattr__(
            self,
            "cost_basis",
            quantize_money(self.cost_basis, "position.cost_basis"),
        )
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
        object.__setattr__(
            self, "cash_after", quantize_money(self.cash_after, "cash_after")
        )

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
        # Every stored fill amount passes through `precision:1.0.0` exactly
        # once, here, so the detail object and every hash taken over it are
        # already at the canonical `numeric(24,8)` scale.
        object.__setattr__(
            self,
            "quantity",
            quantize_quantity(
                self.quantity, fractional_eligible=True, label="quantity"
            ),
        )
        for name in (
            "base_price",
            "price",
            "gross_amount",
            "slippage_amount",
            "fee",
            "cost_basis",
            "realized_pnl",
        ):
            object.__setattr__(
                self, name, quantize_money(getattr(self, name), name)
            )
        if self.gross_amount != quantize_money(
            self.price * self.quantity, "gross_amount"
        ):
            raise ResultSnapshotValidationError(
                "gross_amount must equal price times quantity"
            )
        expected_slippage = quantize_money(
            abs(self.price - self.base_price) * self.quantity, "slippage_amount"
        )
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
    """One ``backtest.performance_summaries`` row in domain form.

    Every monetary field is quantized here, so the object can only ever hold
    values that round-trip through ``numeric(24,8)``. ``metrics`` is the full
    :data:`~backtest_engine.performance.METRIC_RULES` catalog, not a subset:
    a metric that cannot be computed for this run carries ``value is None``,
    which is distinct from zero.
    """

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
    equity_curve: EquityCurve
    metrics: MetricSet
    calculated_at: datetime
    source_set_hash: str
    input_hash: str
    result_hash: str
    metric_catalog_version: str = METRIC_CATALOG_VERSION
    calculation_rules_version: str = CALCULATION_RULES_VERSION

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
        for field in (
            "total_fees",
            "total_slippage",
            "realized_pnl",
            "initial_cash",
            "ending_cash",
        ):
            value = getattr(self, field)
            if field == "realized_pnl":
                _decimal(value, f"summary.{field}")
            else:
                _non_negative(value, f"summary.{field}")
            object.__setattr__(
                self, field, quantize_money(value, f"summary.{field}")
            )
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

        if not isinstance(self.equity_curve, EquityCurve):
            raise ResultSnapshotValidationError(
                "summary.equity_curve must be an EquityCurve"
            )
        if not isinstance(self.metrics, MetricSet):
            raise ResultSnapshotValidationError("summary.metrics must be a MetricSet")
        if (
            self.metrics.basis is not self.equity_curve.basis
            or self.metrics.periodicity is not self.equity_curve.periodicity
        ):
            raise ResultSnapshotValidationError(
                "summary.metrics must be qualified by the same valuation basis as "
                "summary.equity_curve"
            )
        object.__setattr__(
            self, "calculated_at", _utc(self.calculated_at, "summary.calculated_at")
        )
        # A stored summary states which catalog produced it, and this build
        # refuses to relabel numbers it did not compute under that catalog.
        if self.metric_catalog_version != METRIC_CATALOG_VERSION:
            raise ResultSnapshotValidationError(
                f"this build computes only {METRIC_CATALOG_VERSION}, not "
                f"{self.metric_catalog_version}"
            )
        if self.calculation_rules_version != CALCULATION_RULES_VERSION:
            raise ResultSnapshotValidationError(
                f"this build computes only {CALCULATION_RULES_VERSION}, not "
                f"{self.calculation_rules_version}"
            )
        for field in ("source_set_hash", "input_hash", "result_hash"):
            _hash(getattr(self, field), f"summary.{field}")

    def metrics_document(self) -> Mapping[str, Any]:
        """The ``performance_summaries.metrics_document`` jsonb payload."""
        return MappingProxyType(_render_metrics_document(self.metrics))


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
class PerformanceRow:
    """The exact column set of ``backtest.performance_summaries``.

    Kept here, in the domain layer, rather than imported from
    ``backtest_engine.persistence``: the persistence package pulls in
    SQLAlchemy, and the result snapshot must stay usable without a database.
    The field names and types are identical to
    ``persistence.rows.PerformanceSummaryRow`` so a repository can insert this
    directly.
    """

    run_id: uuid.UUID
    metric_catalog_version: str
    metrics_document: Mapping[str, Any]
    calculation_rules_version: str
    source_set_hash: str
    input_hash: str
    result_hash: str
    calculated_at: datetime


@dataclass(frozen=True, slots=True)
class ResultSnapshot:
    run_snapshot: RunSnapshot
    records: tuple[ResultRecord, ...]
    summary: PerformanceSummary
    manifest: ResultObjectManifest
    object_bytes: bytes

    def completion_fields(self) -> dict[str, str]:
        """The ``backtest.v1`` COMPLETED detail fields.

        camelCase with ``sha256:``-prefixed digests, matching the envelope
        convention B publishes on ``strategy-bot.v1`` (spec 2.1).
        ``resultHash`` is the summary's, which transitively covers the detail
        record set, the valuation grid and every metric, so a redelivery of a
        genuinely different outcome cannot reuse the idempotency key.
        """
        return {
            "resultManifestId": self.manifest.result_manifest_id,
            "resultHash": f"sha256:{self.summary.result_hash}",
        }

    def performance_row(self) -> PerformanceRow:
        """Project the summary onto ``backtest.performance_summaries``."""
        summary = self.summary
        return PerformanceRow(
            run_id=uuid.UUID(self.run_snapshot.backtest_run_id),
            metric_catalog_version=summary.metric_catalog_version,
            metrics_document=summary.metrics_document(),
            calculation_rules_version=summary.calculation_rules_version,
            source_set_hash=summary.source_set_hash,
            input_hash=summary.input_hash,
            result_hash=summary.result_hash,
            calculated_at=summary.calculated_at,
        )


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
        valuation_series: ValuationSeries | None = None,
    ) -> ResultSnapshot:
        """Assemble the immutable detail object, summary and manifest.

        ``valuation_series`` is the mark grid the equity curve is sampled on.
        Omitting it is not a hidden default: it selects the explicit
        ``COST_BASIS`` / ``EVENT`` curve built by
        :meth:`ValuationSeries.event_driven`, whose limitations (no Sharpe, no
        unrealised P&L) are recorded on the curve and in the metrics document.
        """
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

        summary = _performance_summary(
            run_snapshot, ordered, valuation_series, completed_at
        )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "run_snapshot": _run_payload(run_snapshot),
            "records": [_record_payload(item) for item in ordered],
        }
        if valuation_series is not None and valuation_series.basis is ValuationBasis.MARK_TO_MARKET:
            payload["valuation"] = _series_payload(valuation_series)
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
    def rebuild(object_bytes: bytes, calculated_at: datetime) -> ResultSnapshot:
        """Recover a published `ResultSnapshot` from its stored object bytes.

        The durable read model keeps no copy of the snapshot: the JSON object in
        storage is the evidence, and `backtest.performance_summaries.calculated_at` is
        the completion instant the manifest and every hash were taken at. Parsing is
        deliberately *not* trusted — the parsed records are re-serialised through
        :meth:`build`, and the result is rejected unless the bytes it produces are
        byte-identical to the ones supplied. A field this parser dropped, coerced or
        re-ordered therefore fails here instead of being served as evidence.

        Raises `ResultIntegrityError`; never returns a partially recovered snapshot.
        """
        if not isinstance(object_bytes, bytes):
            raise ResultIntegrityError("result object content must be bytes")
        try:
            payload = json.loads(object_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResultIntegrityError(f"result object is not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise ResultIntegrityError("result object must be a JSON object")
        if payload.get("schema_version") != SCHEMA_VERSION:
            raise ResultIntegrityError(
                f"result object schema_version must be {SCHEMA_VERSION}, "
                f"got {payload.get('schema_version')!r}"
            )
        try:
            run_snapshot = _run_snapshot_from_payload(payload["run_snapshot"])
            records = [
                _record_from_payload(run_snapshot.snapshot_id, item)
                for item in payload["records"]
            ]
            valuation_series = (
                _series_from_payload(payload["valuation"])
                if "valuation" in payload
                else None
            )
        except (KeyError, TypeError, ValueError, ArithmeticError) as exc:
            raise ResultIntegrityError(f"result object cannot be parsed: {exc}") from exc

        rebuilt = ResultSnapshotBuilder().build(
            run_snapshot, records, calculated_at, valuation_series
        )
        if rebuilt.object_bytes != object_bytes:
            raise ResultIntegrityError(
                "rebuilt result object does not match the stored bytes; the stored "
                "object, the parse, or the completion instant disagree"
            )
        return rebuilt

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
        if result.summary.equity_curve.basis is ValuationBasis.MARK_TO_MARKET:
            expected_payload["valuation"] = _series_payload(
                result.summary.equity_curve.to_series()
            )
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
        if not isinstance(result.summary, PerformanceSummary):
            raise ResultIntegrityError("result summary type is invalid")
        if result.summary.calculated_at != manifest.completed_at:
            raise ResultIntegrityError(
                "result summary was not calculated at the completion instant"
            )
        # `to_series` recovers the exact valuation grid the stored curve was
        # sampled on, so the whole summary is re-derived from evidence rather
        # than trusted.
        expected_summary = _performance_summary(
            result.run_snapshot,
            result.records,
            result.summary.equity_curve.to_series(),
            manifest.completed_at,
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


#: The nine columns a FILL record carries and no other kind may.
_FILL_FIELDS = (
    "fill_id",
    "quantity",
    "base_price",
    "price",
    "gross_amount",
    "slippage_amount",
    "fee",
    "cost_basis",
    "realized_pnl",
)


def _parse_instant(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"timestamp must be a string, got {value!r}")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _parse_decimal(value: object) -> Decimal:
    if not isinstance(value, str):
        raise ValueError(f"decimal must be canonical text, got {value!r}")
    return Decimal(value)


def _series_from_payload(payload: object) -> ValuationSeries:
    """Inverse of ``_series_payload`` for immutable mark-to-market evidence."""
    if not isinstance(payload, dict):
        raise ValueError("valuation must be a JSON object")
    basis = ValuationBasis(payload["basis"])
    if payload.get("basis_rule_id") != basis.rule_id:
        raise ValueError("valuation basis_rule_id does not match basis")
    raw_instants = payload["instants"]
    if not isinstance(raw_instants, list):
        raise ValueError("valuation instants must be a list")
    instants = []
    for item in raw_instants:
        if not isinstance(item, dict) or not isinstance(item.get("marks"), list):
            raise ValueError("valuation instant must contain marks")
        marks = []
        for mark in item["marks"]:
            if not isinstance(mark, list) or len(mark) != 2:
                raise ValueError("valuation mark must be [instrument_id, price]")
            marks.append(MarkPrice(mark[0], _parse_decimal(mark[1])))
        instants.append(ValuationInstant(_parse_instant(item["as_of"]), tuple(marks)))
    return ValuationSeries(
        basis=basis,
        periodicity=ValuationPeriodicity(payload["periodicity"]),
        opening_at=_parse_instant(payload["opening_at"]),
        instants=tuple(instants),
    )


def _run_snapshot_from_payload(payload: object) -> RunSnapshot:
    """Inverse of `_run_payload`. Every field is required; nothing defaults."""

    if not isinstance(payload, dict):
        raise ValueError("run_snapshot must be a JSON object")
    return RunSnapshot(
        backtest_run_id=payload["backtest_run_id"],
        strategy_version_id=payload["strategy_version_id"],
        input_bundle_fingerprint=payload["input_bundle_fingerprint"],
        calculation_model_version=payload["calculation_model_version"],
        cost_model_version=payload["cost_model_version"],
        execution_model_version=payload["execution_model_version"],
        initial_cash=_parse_decimal(payload["initial_cash"]),
    )


def _record_from_payload(run_snapshot_id: str, payload: object) -> ResultRecord:
    """Inverse of `_record_payload`.

    `reason_code` and the nine fill columns are omitted from the payload rather than
    written as null, so "absent" is the only representation of "not applicable" and
    reading one back must not invent a zero.
    """

    if not isinstance(payload, dict):
        raise ValueError("result record must be a JSON object")
    if payload["run_snapshot_id"] != run_snapshot_id:
        raise ValueError("result record references a different run snapshot")
    kind = ResultRecordKind(payload["kind"])
    optional: dict[str, object] = {}
    if "reason_code" in payload:
        optional["reason_code"] = payload["reason_code"]
    if kind is ResultRecordKind.FILL:
        optional["fill_id"] = payload["fill_id"]
        optional.update(
            {name: _parse_decimal(payload[name]) for name in _FILL_FIELDS if name != "fill_id"}
        )
    elif any(name in payload for name in _FILL_FIELDS):
        raise ValueError(f"a {kind.value} record must not carry fill columns")
    return ResultRecord(
        run_snapshot_id=payload["run_snapshot_id"],
        record_id=payload["record_id"],
        kind=kind,
        occurred_at=_parse_instant(payload["occurred_at"]),
        order_id=payload["order_id"],
        instrument_id=payload["instrument_id"],
        order_status=OrderStatus(payload["order_status"]),
        cash_after=_parse_decimal(payload["cash_after"]),
        positions_after=tuple(
            PositionAfter(
                instrument_id=item["instrument_id"],
                quantity=_parse_decimal(item["quantity"]),
                cost_basis=_parse_decimal(item["cost_basis"]),
            )
            for item in payload["positions_after"]
        ),
        **optional,
    )


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


@dataclass(frozen=True, slots=True)
class _FillLedger:
    """What a canonical walk of the FILL records establishes."""

    events: tuple[LedgerEvent, ...]
    trades: TradeStatistics
    ending_cash: Decimal
    ending_positions: tuple[PositionAfter, ...]


def _fill_ledger(
    run_snapshot: RunSnapshot,
    ordered: tuple[ResultRecord, ...],
) -> _FillLedger:
    """Re-walk cash and positions from the fills alone.

    ``cash_after`` on a detail record is the engine's own report and is never
    read here: a rejection or cancellation carries the cash it *saw*, which is
    not the run's ending cash, and the pre-rebuild summary copied exactly that
    number out of the last record.

    A fill's direction is not stored on the record; it is *established* by the
    position book. A fill whose position delta is neither ``+quantity`` nor
    ``-quantity`` describes an execution that cannot have happened, and is
    rejected rather than guessed at.
    """

    cash = quantize_money(run_snapshot.initial_cash, "initial_cash")
    book: dict[str, PositionAfter] = {}
    by_instant: dict[datetime, tuple[Decimal, dict[str, PositionAfter]]] = {}
    fill_count = closing_count = winning_count = losing_count = 0
    realized_pnl = total_fees = total_slippage = ZERO

    for record in _causal_fill_order(ordered):
        quantity = record.quantity
        gross_amount = record.gross_amount
        fee = record.fee
        realized = record.realized_pnl
        slippage = record.slippage_amount
        if (
            quantity is None
            or gross_amount is None
            or fee is None
            or realized is None
            or slippage is None
        ):  # pragma: no cover - ResultRecord already rejects a partial fill
            raise ResultSnapshotValidationError("fill fields must all be supplied")

        after = {
            item.instrument_id: item
            for item in record.positions_after
            if item.quantity > ZERO
        }
        held_before = book.get(record.instrument_id)
        held_after = after.get(record.instrument_id)
        before_quantity = held_before.quantity if held_before is not None else ZERO
        after_quantity = held_after.quantity if held_after is not None else ZERO
        delta = after_quantity - before_quantity
        if delta == quantity:
            opening = True
        elif delta == -quantity:
            opening = False
        else:
            raise ResultSnapshotValidationError(
                f"fill {record.fill_id} moved the position quantity of "
                f"{record.instrument_id} by {delta}, which is neither "
                f"+{quantity} nor -{quantity}"
            )
        for instrument_id in sorted(
            (set(book) | set(after)) - {record.instrument_id}
        ):
            if book.get(instrument_id) != after.get(instrument_id):
                raise ResultSnapshotValidationError(
                    f"fill {record.fill_id} changed the position of "
                    f"{instrument_id}, which it does not trade"
                )

        # Long-only: buying pays the gross and the fee, selling receives the
        # gross less the fee. Both are quantized once, here.
        cash_delta = quantize_money(
            -(gross_amount + fee) if opening else gross_amount - fee,
            "fill cash flow",
        )
        cash = quantize_money(cash + cash_delta, "ending_cash")

        fill_count += 1
        if not opening:
            closing_count += 1
            if realized > ZERO:
                winning_count += 1
            elif realized < ZERO:
                losing_count += 1
        realized_pnl += realized
        total_fees += fee
        total_slippage += slippage
        book = after

        # Two fills on the same instant are one ledger movement; the equity
        # curve samples instants, not records.
        previous = by_instant.get(record.occurred_at)
        merged = cash_delta if previous is None else previous[0] + cash_delta
        by_instant[record.occurred_at] = (
            quantize_money(merged, "ledger cash_delta"),
            after,
        )

    events = tuple(
        LedgerEvent(
            as_of=instant,
            cash_delta=cash_delta,
            positions=tuple(
                PositionState(
                    instrument_id=item.instrument_id,
                    quantity=item.quantity,
                    cost_basis=quantize_money(item.cost_basis, "position cost_basis"),
                )
                for item in positions.values()
            ),
        )
        for instant, (cash_delta, positions) in sorted(by_instant.items())
    )
    return _FillLedger(
        events=events,
        trades=TradeStatistics(
            fill_count=fill_count,
            closing_trade_count=closing_count,
            winning_trade_count=winning_count,
            losing_trade_count=losing_count,
            realized_pnl=quantize_money(realized_pnl, "realized_pnl"),
            total_fees=quantize_money(total_fees, "total_fees"),
            total_slippage=quantize_money(total_slippage, "total_slippage"),
        ),
        ending_cash=cash,
        ending_positions=tuple(
            sorted(book.values(), key=lambda item: item.instrument_id)
        ),
    )


def _causal_fill_order(
    ordered: tuple[ResultRecord, ...],
) -> tuple[ResultRecord, ...]:
    """Restore the causal order of fills sharing one market-data instant.

    Record ids are content-derived and therefore cannot encode the order in
    which multiple orders consumed the same bar.  Their successive position
    snapshots do encode that order, so follow that chain before rebuilding the
    ledger.
    """

    fills = [item for item in ordered if item.kind is ResultRecordKind.FILL]
    result: list[ResultRecord] = []
    book: dict[str, PositionAfter] = {}
    index = 0
    while index < len(fills):
        instant = fills[index].occurred_at
        pending: list[ResultRecord] = []
        while index < len(fills) and fills[index].occurred_at == instant:
            pending.append(fills[index])
            index += 1

        while pending:
            matched_index = next(
                (
                    candidate_index
                    for candidate_index, candidate in enumerate(pending)
                    if _matches_position_transition(book, candidate)
                ),
                None,
            )
            if matched_index is None:
                # Preserve the deterministic record ordering so the canonical
                # validation below reports its detailed contradiction.
                result.extend(pending)
                break
            record = pending.pop(matched_index)
            result.append(record)
            book = {
                item.instrument_id: item
                for item in record.positions_after
                if item.quantity > ZERO
            }
    return tuple(result)


def _matches_position_transition(
    book: dict[str, PositionAfter], record: ResultRecord
) -> bool:
    if record.quantity is None:
        return False
    after = {
        item.instrument_id: item
        for item in record.positions_after
        if item.quantity > ZERO
    }
    before_quantity = book.get(
        record.instrument_id, PositionAfter(record.instrument_id, ZERO, ZERO)
    ).quantity
    after_quantity = after.get(
        record.instrument_id, PositionAfter(record.instrument_id, ZERO, ZERO)
    ).quantity
    if (after_quantity - before_quantity).copy_abs() != record.quantity:
        return False
    return all(
        book.get(instrument_id) == after.get(instrument_id)
        for instrument_id in (set(book) | set(after)) - {record.instrument_id}
    )


def _performance_summary(
    run_snapshot: RunSnapshot,
    records: Iterable[ResultRecord],
    valuation_series: ValuationSeries | None,
    calculated_at: datetime,
) -> PerformanceSummary:
    ordered = tuple(sorted(records, key=_record_key))
    calculated_at = _utc(calculated_at, "calculated_at")
    ledger = _fill_ledger(run_snapshot, ordered)

    if valuation_series is None:
        series = ValuationSeries.event_driven(ledger.events, through=calculated_at)
    elif isinstance(valuation_series, ValuationSeries):
        series = valuation_series
    else:
        raise ResultSnapshotValidationError(
            "valuation_series must be a ValuationSeries"
        )
    curve = build_equity_curve(run_snapshot.initial_cash, ledger.events, series)
    metrics = build_metrics(curve, ledger.trades)

    source_set_hash = _sha256(
        _canonical_bytes(
            {
                "domain": SOURCE_SET_HASH_DOMAIN,
                "run_snapshot": _run_payload(run_snapshot),
                "records": [_record_payload(item) for item in ordered],
            }
        )
    )
    input_hash = _sha256(
        _canonical_bytes(
            {
                "domain": INPUT_HASH_DOMAIN,
                "source_set_hash": source_set_hash,
                "metric_catalog_version": METRIC_CATALOG_VERSION,
                "calculation_rules_version": CALCULATION_RULES_VERSION,
                "precision_rules_version": PRECISION_RULES_VERSION,
                "calculated_at": _timestamp(calculated_at),
                "valuation": _series_payload(series),
            }
        )
    )
    result_hash = _sha256(
        _canonical_bytes(
            {
                "domain": RESULT_HASH_DOMAIN,
                "input_hash": input_hash,
                "equity_curve": _curve_payload(curve),
                "metrics": metrics_hash_material(metrics),
            }
        )
    )

    return PerformanceSummary(
        run_snapshot_id=run_snapshot.snapshot_id,
        order_count=len({item.order_id for item in ordered}),
        fill_count=ledger.trades.fill_count,
        cancellation_count=sum(
            item.kind is ResultRecordKind.CANCELLATION for item in ordered
        ),
        rejection_count=sum(
            item.kind is ResultRecordKind.REJECTION for item in ordered
        ),
        total_fees=ledger.trades.total_fees,
        total_slippage=ledger.trades.total_slippage,
        realized_pnl=ledger.trades.realized_pnl,
        initial_cash=run_snapshot.initial_cash,
        ending_cash=ledger.ending_cash,
        ending_positions=ledger.ending_positions,
        equity_curve=curve,
        metrics=metrics,
        calculated_at=calculated_at,
        source_set_hash=source_set_hash,
        input_hash=input_hash,
        result_hash=result_hash,
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


def _series_payload(series: ValuationSeries) -> dict[str, object]:
    """Exact text form of the valuation grid, marks quantized before hashing."""
    return {
        "basis": series.basis.value,
        "basis_rule_id": series.basis.rule_id,
        "periodicity": series.periodicity.value,
        "opening_at": _timestamp(series.opening_at),
        "instants": [
            {
                "as_of": _timestamp(instant.as_of),
                "marks": [
                    [
                        mark.instrument_id,
                        format_money(quantize_money(mark.price, "mark price")),
                    ]
                    for mark in instant.marks
                ],
            }
            for instant in series.instants
        ],
    }


def _curve_payload(curve: EquityCurve) -> dict[str, object]:
    """Every stored curve value is already quantized by `build_equity_curve`."""
    return {
        "basis": curve.basis.value,
        "periodicity": curve.periodicity.value,
        "points": [
            {
                "as_of": _timestamp(point.as_of),
                "cash": format_money(point.cash),
                "position_value": format_money(point.position_value),
                "equity": format_money(point.equity),
                "holdings": [
                    [
                        holding.instrument_id,
                        _decimal_text(holding.quantity),
                        format_money(holding.mark_price),
                        format_money(holding.market_value),
                    ]
                    for holding in point.holdings
                ],
            }
            for point in curve.points
        ],
    }


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
        "metric_catalog_version": summary.metric_catalog_version,
        "calculation_rules_version": summary.calculation_rules_version,
        "calculated_at": _timestamp(summary.calculated_at),
        "source_set_hash": summary.source_set_hash,
        "input_hash": summary.input_hash,
        "result_hash": summary.result_hash,
    }


def _record_key(record: ResultRecord) -> tuple[datetime, int, str]:
    rank = {
        ResultRecordKind.ORDER: 0,
        ResultRecordKind.FILL: 1,
        ResultRecordKind.CANCELLATION: 2,
        ResultRecordKind.REJECTION: 3,
    }[record.kind]
    return record.occurred_at, rank, record.record_id
