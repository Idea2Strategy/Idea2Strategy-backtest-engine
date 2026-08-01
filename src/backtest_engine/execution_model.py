"""Deterministic long-only order, fill, position, and ledger model for D23."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_FLOOR
from enum import Enum

from backtest_engine.execution_policy import ExecutionPolicy


ZERO = Decimal("0")
ONE = Decimal("1")


class ExecutionModelValidationError(ValueError):
    """Raised when an order or bar cannot preserve the official model."""


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP = "STOP"
    STOP_LIMIT = "STOP_LIMIT"
    TRAILING_STOP = "TRAILING_STOP"


class TimeInForce(str, Enum):
    DAY = "DAY"
    GTC = "GTC"
    GTD = "GTD"


class QuantityMode(str, Enum):
    WHOLE_SHARES = "WHOLE_SHARES"
    FRACTIONAL_SHARES = "FRACTIONAL_SHARES"
    NOTIONAL_AMOUNT = "NOTIONAL_AMOUNT"


class OrderStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    REJECTED = "REJECTED"


class LedgerDirection(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


def _utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ExecutionModelValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _uuid(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise ExecutionModelValidationError(f"{label} must be a UUID")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ExecutionModelValidationError(f"{label} must be a UUID") from exc


def _positive(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value <= ZERO:
        raise ExecutionModelValidationError(f"{label} must be a positive Decimal")
    return value


def _non_negative(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite() or value < ZERO:
        raise ExecutionModelValidationError(f"{label} must be a non-negative Decimal")
    return value


def _whole(value: Decimal) -> Decimal:
    return value.to_integral_value(rounding=ROUND_FLOOR)


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_strategy_notional: Decimal
    max_gross_exposure: Decimal
    max_instrument_exposure: Decimal

    def __post_init__(self) -> None:
        _positive(self.max_strategy_notional, "max_strategy_notional")
        _positive(self.max_gross_exposure, "max_gross_exposure")
        _positive(self.max_instrument_exposure, "max_instrument_exposure")


@dataclass(frozen=True, slots=True)
class OrderRequest:
    order_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    quantity_mode: QuantityMode
    time_in_force: TimeInForce
    submitted_at: datetime
    eligible_at: datetime
    day_expires_at: datetime
    reference_price: Decimal
    expires_at: datetime | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    trail_percent: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _uuid(self.order_id, "order_id"))
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        if not isinstance(self.side, OrderSide):
            raise ExecutionModelValidationError("side is unsupported")
        if not isinstance(self.order_type, OrderType):
            raise ExecutionModelValidationError("order_type is unsupported")
        if not isinstance(self.quantity_mode, QuantityMode):
            raise ExecutionModelValidationError("quantity_mode is unsupported")
        if not isinstance(self.time_in_force, TimeInForce):
            raise ExecutionModelValidationError("time_in_force is unsupported")
        _positive(self.quantity, "quantity")
        _positive(self.reference_price, "reference_price")

        submitted_at = _utc(self.submitted_at, "submitted_at")
        eligible_at = _utc(self.eligible_at, "eligible_at")
        day_expires_at = _utc(self.day_expires_at, "day_expires_at")
        if eligible_at < submitted_at:
            raise ExecutionModelValidationError(
                "eligible_at must not precede submitted_at"
            )
        if day_expires_at <= eligible_at:
            raise ExecutionModelValidationError(
                "day_expires_at must follow eligible_at"
            )
        object.__setattr__(self, "submitted_at", submitted_at)
        object.__setattr__(self, "eligible_at", eligible_at)
        object.__setattr__(self, "day_expires_at", day_expires_at)

        if self.order_type is OrderType.MARKET and self.time_in_force is not TimeInForce.DAY:
            raise ExecutionModelValidationError("MARKET orders require DAY")
        self._validate_parameters()
        self._validate_quantity_mode()
        self._validate_expiry()

    def _validate_parameters(self) -> None:
        supplied = (self.limit_price, self.stop_price, self.trail_percent)
        expected = {
            OrderType.MARKET: (False, False, False),
            OrderType.LIMIT: (True, False, False),
            OrderType.STOP: (False, True, False),
            OrderType.STOP_LIMIT: (True, True, False),
            OrderType.TRAILING_STOP: (False, False, True),
        }[self.order_type]
        if tuple(value is not None for value in supplied) != expected:
            raise ExecutionModelValidationError(
                f"invalid price parameters for {self.order_type.value}"
            )
        for label, value in zip(
            ("limit_price", "stop_price", "trail_percent"), supplied
        ):
            if value is not None:
                _positive(value, label)
        if self.trail_percent is not None and self.trail_percent > ONE:
            raise ExecutionModelValidationError("trail_percent must be at most 1")

    def _validate_quantity_mode(self) -> None:
        if self.quantity_mode is QuantityMode.WHOLE_SHARES:
            if self.quantity != _whole(self.quantity):
                raise ExecutionModelValidationError(
                    "whole-share quantity must be integral"
                )
            return
        if self.quantity_mode is QuantityMode.NOTIONAL_AMOUNT:
            raise ExecutionModelValidationError(
                "notional amount orders are outside the D23 share model"
            )
        if (
            self.order_type is not OrderType.MARKET
            or self.time_in_force is not TimeInForce.DAY
        ):
            raise ExecutionModelValidationError(
                "fractional shares require a long MARKET/DAY order"
            )

    def _validate_expiry(self) -> None:
        if self.time_in_force is TimeInForce.GTD:
            if self.expires_at is None:
                raise ExecutionModelValidationError("GTD requires expires_at")
            expires_at = _utc(self.expires_at, "expires_at")
            if not self.eligible_at < expires_at <= self.submitted_at + timedelta(days=90):
                raise ExecutionModelValidationError(
                    "GTD expires_at must be after eligibility and within 90 days"
                )
            object.__setattr__(self, "expires_at", expires_at)
        elif self.expires_at is not None:
            raise ExecutionModelValidationError(
                "expires_at is allowed only for GTD"
            )


@dataclass(frozen=True, slots=True)
class MinuteBar:
    instrument_id: str
    starts_at: datetime
    ends_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    complete: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        starts_at = _utc(self.starts_at, "starts_at")
        ends_at = _utc(self.ends_at, "ends_at")
        if ends_at - starts_at != timedelta(minutes=1):
            raise ExecutionModelValidationError("bar must cover exactly one minute")
        object.__setattr__(self, "starts_at", starts_at)
        object.__setattr__(self, "ends_at", ends_at)
        for label in ("open", "high", "low", "close"):
            _positive(getattr(self, label), label)
        _non_negative(self.volume, "volume")
        if (
            self.high < max(self.open, self.close)
            or self.low > min(self.open, self.close)
            or self.low > self.high
        ):
            raise ExecutionModelValidationError("OHLC values are inconsistent")
        if not isinstance(self.complete, bool):
            raise ExecutionModelValidationError("complete must be boolean")


@dataclass(frozen=True, slots=True)
class BacktestOrder:
    order_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    remaining_quantity: Decimal
    status: OrderStatus
    submitted_at: datetime
    eligible_at: datetime
    expires_at: datetime
    reason_code: str | None


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    instrument_id: str
    quantity: Decimal
    cost_basis: Decimal


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    entry_id: str
    account_code: str
    direction: LedgerDirection
    amount: Decimal
    source_event_id: str
    currency: str = "USD"

    def __post_init__(self) -> None:
        _uuid(self.entry_id, "entry_id")
        _uuid(self.source_event_id, "source_event_id")
        if not self.account_code:
            raise ExecutionModelValidationError("account_code must not be empty")
        _positive(self.amount, "ledger amount")
        if self.currency != "USD":
            raise ExecutionModelValidationError("D23 ledger currency must be USD")


@dataclass(frozen=True, slots=True)
class LedgerTransaction:
    transaction_id: str
    source_event_id: str
    posted_at: datetime
    entries: tuple[LedgerEntry, ...]

    def __post_init__(self) -> None:
        _uuid(self.transaction_id, "transaction_id")
        _uuid(self.source_event_id, "source_event_id")
        object.__setattr__(self, "posted_at", _utc(self.posted_at, "posted_at"))
        entries = tuple(self.entries)
        if len(entries) < 2:
            raise ExecutionModelValidationError(
                "ledger transaction requires at least two entries"
            )
        if any(entry.source_event_id != self.source_event_id for entry in entries):
            raise ExecutionModelValidationError(
                "ledger entry source_event_id must match transaction"
            )
        if len({entry.entry_id for entry in entries}) != len(entries):
            raise ExecutionModelValidationError("ledger entry IDs must be unique")
        debits = sum(
            (entry.amount for entry in entries if entry.direction is LedgerDirection.DEBIT),
            ZERO,
        )
        credits = sum(
            (entry.amount for entry in entries if entry.direction is LedgerDirection.CREDIT),
            ZERO,
        )
        if debits != credits:
            raise ExecutionModelValidationError("ledger transaction must be balanced")
        object.__setattr__(self, "entries", entries)


@dataclass(frozen=True, slots=True)
class Fill:
    fill_id: str
    order_id: str
    instrument_id: str
    side: OrderSide
    quantity: Decimal
    base_price: Decimal
    price: Decimal
    gross_amount: Decimal
    slippage_amount: Decimal
    fee: Decimal
    cost_basis: Decimal
    realized_pnl: Decimal
    occurred_at: datetime
    ledger_transaction_id: str


@dataclass(slots=True)
class _Lot:
    quantity: Decimal
    cost_basis: Decimal


@dataclass(slots=True)
class _OrderState:
    request: OrderRequest
    expires_at: datetime
    remaining_quantity: Decimal
    status: OrderStatus = OrderStatus.ACCEPTED
    reason_code: str | None = None
    fill_sequence: int = 0
    triggered: bool = False
    trail_reference: Decimal | None = None
    reserved_cash: Decimal = ZERO
    reserved_notional: Decimal = ZERO
    reserved_quantity: Decimal = ZERO


class BacktestExecutionModel:
    """Replays official long orders without same-bar look-ahead."""

    def __init__(
        self,
        policy: ExecutionPolicy,
        initial_cash: Decimal,
        risk_limits: RiskLimits,
    ) -> None:
        if not isinstance(policy, ExecutionPolicy):
            raise ExecutionModelValidationError("policy must be an ExecutionPolicy")
        self._policy = policy
        self._cash = _non_negative(initial_cash, "initial_cash")
        if not isinstance(risk_limits, RiskLimits):
            raise ExecutionModelValidationError("risk_limits must be RiskLimits")
        self._risk_limits = risk_limits
        self._orders: dict[str, _OrderState] = {}
        self._lots: dict[str, list[_Lot]] = {}
        self._fills: list[Fill] = []
        self._ledger: list[LedgerTransaction] = []
        self._now: datetime | None = None

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def model_version(self) -> str:
        return self._policy.calculation_model_version

    @property
    def fills(self) -> tuple[Fill, ...]:
        return tuple(self._fills)

    @property
    def ledger_transactions(self) -> tuple[LedgerTransaction, ...]:
        return tuple(self._ledger)

    def seed_long_position(
        self, instrument_id: str, quantity: Decimal, unit_cost: Decimal
    ) -> None:
        if self._orders or self._fills:
            raise ExecutionModelValidationError(
                "seed positions must be loaded before order processing"
            )
        instrument_id = _uuid(instrument_id, "instrument_id")
        _positive(quantity, "quantity")
        _positive(unit_cost, "unit_cost")
        self._lots.setdefault(instrument_id, []).append(
            _Lot(quantity, quantity * unit_cost)
        )

    def position(self, instrument_id: str) -> PositionSnapshot:
        instrument_id = _uuid(instrument_id, "instrument_id")
        lots = self._lots.get(instrument_id, [])
        return PositionSnapshot(
            instrument_id,
            sum((lot.quantity for lot in lots), ZERO),
            sum((lot.cost_basis for lot in lots), ZERO),
        )

    def order(self, order_id: str) -> BacktestOrder:
        order_id = _uuid(order_id, "order_id")
        try:
            return self._snapshot(self._orders[order_id])
        except KeyError as exc:
            raise KeyError(f"unknown order_id: {order_id}") from exc

    def submit(self, request: OrderRequest) -> BacktestOrder:
        if not isinstance(request, OrderRequest):
            raise ExecutionModelValidationError("request must be an OrderRequest")
        if request.order_id in self._orders:
            raise ExecutionModelValidationError("order_id must be unique")
        expires_at = {
            TimeInForce.DAY: request.day_expires_at,
            TimeInForce.GTC: request.submitted_at + timedelta(days=90),
            TimeInForce.GTD: request.expires_at,
        }[request.time_in_force]
        assert expires_at is not None
        state = _OrderState(
            request=request,
            expires_at=expires_at,
            remaining_quantity=request.quantity,
            trail_reference=request.reference_price
            if request.order_type is OrderType.TRAILING_STOP
            else None,
        )
        self._orders[request.order_id] = state
        self._reserve_or_reject(state)
        return self._snapshot(state)

    def cancel(self, order_id: str, cancelled_at: datetime) -> BacktestOrder:
        _utc(cancelled_at, "cancelled_at")
        state = self._orders[_uuid(order_id, "order_id")]
        if self._is_open(state):
            state.status = OrderStatus.CANCELLED
            state.reason_code = "EXPLICITLY_CANCELLED"
            self._release_reservation(state)
        return self._snapshot(state)

    def advance_time(self, instant: datetime) -> tuple[BacktestOrder, ...]:
        now = _utc(instant, "instant")
        if self._now is not None and now < self._now:
            raise ExecutionModelValidationError("execution clock must not move backward")
        self._now = now
        expired: list[BacktestOrder] = []
        for state in sorted(self._orders.values(), key=self._order_key):
            if self._is_open(state) and now >= state.expires_at:
                state.status = OrderStatus.EXPIRED
                state.reason_code = {
                    TimeInForce.DAY: "DAY_EXPIRED",
                    TimeInForce.GTC: "GTC_90_DAY_EXPIRED",
                    TimeInForce.GTD: "GTD_EXPIRED",
                }[state.request.time_in_force]
                self._release_reservation(state)
                expired.append(self._snapshot(state))
        return tuple(expired)

    def process_bars(self, bars: list[MinuteBar]) -> tuple[Fill, ...]:
        if any(not isinstance(bar, MinuteBar) for bar in bars):
            raise ExecutionModelValidationError("bars must contain MinuteBar values")
        fills: list[Fill] = []
        for bar in sorted(bars, key=lambda item: (item.starts_at, item.instrument_id)):
            fills.extend(self.process_bar(bar))
        return tuple(fills)

    def process_bar(self, bar: MinuteBar) -> tuple[Fill, ...]:
        if not isinstance(bar, MinuteBar):
            raise ExecutionModelValidationError("bar must be a MinuteBar")
        self.advance_time(bar.starts_at)
        if not bar.complete or bar.volume <= ZERO:
            return ()
        fills: list[Fill] = []
        states = sorted(
            (
                state
                for state in self._orders.values()
                if state.request.instrument_id == bar.instrument_id
                and self._is_open(state)
                and state.request.eligible_at <= bar.starts_at
            ),
            key=self._order_key,
        )
        for state in states:
            base_price = self._base_price(state, bar)
            if base_price is None:
                continue
            price = self._slipped_price(state.request.side, base_price)
            if not self._within_limit(state, price):
                continue
            quantity = self._fillable_quantity(state, price)
            if quantity <= ZERO:
                continue
            fills.append(self._record_fill(state, bar.ends_at, quantity, base_price, price))
        return tuple(fills)

    def _reserve_or_reject(self, state: _OrderState) -> None:
        request = state.request
        if request.side is OrderSide.SELL:
            available = self.position(request.instrument_id).quantity - sum(
                other.reserved_quantity
                for other in self._orders.values()
                if other is not state
                and self._is_open(other)
                and other.request.instrument_id == request.instrument_id
                and other.request.side is OrderSide.SELL
            )
            if available < request.quantity:
                self._reject(state, "POSITION_UNAVAILABLE")
                return
            state.reserved_quantity = request.quantity
            return

        estimated_price = self._slipped_price(OrderSide.BUY, request.reference_price)
        estimated_notional = estimated_price * request.quantity
        estimated_cash = estimated_notional * (ONE + self._policy.fee_rate)
        available_cash = self._cash - self._reserved_cash(excluding=state)
        if estimated_cash > available_cash:
            self._reject(state, "INSUFFICIENT_AVAILABLE_CASH")
            return
        current_exposure = self._position_exposure()
        other_reserved = self._reserved_notional(excluding=state)
        if current_exposure + other_reserved + estimated_cash > self._risk_limits.max_strategy_notional:
            self._reject(state, "STRATEGY_BUDGET_EXCEEDED")
            return
        if current_exposure + other_reserved + estimated_notional > self._risk_limits.max_gross_exposure:
            self._reject(state, "GROSS_EXPOSURE_EXCEEDED")
            return
        instrument_exposure = self.position(request.instrument_id).cost_basis + sum(
            other.reserved_notional
            for other in self._orders.values()
            if other is not state
            and self._is_open(other)
            and other.request.instrument_id == request.instrument_id
            and other.request.side is OrderSide.BUY
        )
        if instrument_exposure + estimated_notional > self._risk_limits.max_instrument_exposure:
            self._reject(state, "INSTRUMENT_EXPOSURE_EXCEEDED")
            return
        state.reserved_cash = estimated_cash
        state.reserved_notional = estimated_notional

    def _base_price(self, state: _OrderState, bar: MinuteBar) -> Decimal | None:
        request = state.request
        if request.order_type is OrderType.MARKET:
            return bar.open
        if request.order_type is OrderType.LIMIT:
            assert request.limit_price is not None
            return self._limit_base(request.side, request.limit_price, bar)
        if request.order_type is OrderType.STOP:
            assert request.stop_price is not None
            return self._stop_base(request.side, request.stop_price, bar)
        if request.order_type is OrderType.STOP_LIMIT:
            assert request.stop_price is not None and request.limit_price is not None
            if not state.triggered:
                trigger_base = self._stop_base(request.side, request.stop_price, bar)
                if trigger_base is None:
                    return None
                state.triggered = True
                if request.side is OrderSide.BUY:
                    return trigger_base if trigger_base <= request.limit_price else None
                return trigger_base if trigger_base >= request.limit_price else None
            return self._limit_base(request.side, request.limit_price, bar)
        assert request.trail_percent is not None and state.trail_reference is not None
        if request.side is OrderSide.SELL:
            stop = state.trail_reference * (ONE - request.trail_percent)
            if bar.open <= stop:
                return bar.open
            if bar.low <= stop:
                return stop
            state.trail_reference = max(state.trail_reference, bar.high)
            return None
        stop = state.trail_reference * (ONE + request.trail_percent)
        if bar.open >= stop:
            return bar.open
        if bar.high >= stop:
            return stop
        state.trail_reference = min(state.trail_reference, bar.low)
        return None

    @staticmethod
    def _limit_base(
        side: OrderSide, limit_price: Decimal, bar: MinuteBar
    ) -> Decimal | None:
        if side is OrderSide.BUY:
            return min(bar.open, limit_price) if bar.low <= limit_price else None
        return max(bar.open, limit_price) if bar.high >= limit_price else None

    @staticmethod
    def _stop_base(
        side: OrderSide, stop_price: Decimal, bar: MinuteBar
    ) -> Decimal | None:
        if side is OrderSide.BUY:
            if bar.open >= stop_price:
                return bar.open
            return stop_price if bar.high >= stop_price else None
        if bar.open <= stop_price:
            return bar.open
        return stop_price if bar.low <= stop_price else None

    def _slipped_price(self, side: OrderSide, base_price: Decimal) -> Decimal:
        direction = ONE + self._policy.slippage_rate
        if side is OrderSide.SELL:
            direction = ONE - self._policy.slippage_rate
        return base_price * direction

    @staticmethod
    def _within_limit(state: _OrderState, price: Decimal) -> bool:
        limit = state.request.limit_price
        if limit is None:
            return True
        if state.request.side is OrderSide.BUY:
            return price <= limit
        return price >= limit

    def _fillable_quantity(self, state: _OrderState, price: Decimal) -> Decimal:
        request = state.request
        if request.side is OrderSide.SELL:
            available = self.position(request.instrument_id).quantity
            quantity = min(state.remaining_quantity, available)
        else:
            other_cash = self._reserved_cash(excluding=state)
            unit_cash = price * (ONE + self._policy.fee_rate)
            cash_quantity = max(self._cash - other_cash, ZERO) / unit_cash
            position_exposure = self._position_exposure()
            other_notional = self._reserved_notional(excluding=state)
            budget_amount = max(
                self._risk_limits.max_strategy_notional
                - position_exposure
                - self._reserved_cash(excluding=state),
                ZERO,
            )
            budget_quantity = budget_amount / unit_cash
            gross_amount = max(
                self._risk_limits.max_gross_exposure
                - position_exposure
                - other_notional,
                ZERO,
            )
            gross_quantity = gross_amount / price
            instrument_amount = max(
                self._risk_limits.max_instrument_exposure
                - self.position(request.instrument_id).cost_basis
                - self._instrument_reserved_notional(request.instrument_id, excluding=state),
                ZERO,
            )
            instrument_quantity = instrument_amount / price
            quantity = min(
                state.remaining_quantity,
                cash_quantity,
                budget_quantity,
                gross_quantity,
                instrument_quantity,
            )
        if request.quantity_mode is QuantityMode.WHOLE_SHARES:
            quantity = _whole(quantity)
        return max(quantity, ZERO)

    def _record_fill(
        self,
        state: _OrderState,
        occurred_at: datetime,
        quantity: Decimal,
        base_price: Decimal,
        price: Decimal,
    ) -> Fill:
        state.fill_sequence += 1
        fill_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"idea2strategy:d23:{state.request.order_id}:{state.fill_sequence}",
            )
        )
        gross = price * quantity
        fee = gross * self._policy.fee_rate
        slippage = abs(price - base_price) * quantity
        if state.request.side is OrderSide.BUY:
            self._cash -= gross + fee
            self._lots.setdefault(state.request.instrument_id, []).append(
                _Lot(quantity, gross)
            )
            cost_basis = gross
            realized_pnl = ZERO
            entries = self._buy_entries(fill_id, gross, fee)
        else:
            cost_basis = self._consume_fifo(state.request.instrument_id, quantity)
            self._cash += gross - fee
            realized_pnl = gross - cost_basis
            entries = self._sell_entries(fill_id, gross, fee, cost_basis, realized_pnl)

        transaction_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"idea2strategy:d23:ledger:{fill_id}")
        )
        transaction = LedgerTransaction(
            transaction_id,
            fill_id,
            occurred_at,
            tuple(entries),
        )
        self._ledger.append(transaction)

        state.remaining_quantity -= quantity
        if state.remaining_quantity == ZERO:
            state.status = OrderStatus.FILLED
            self._release_reservation(state)
        else:
            state.status = OrderStatus.PARTIALLY_FILLED
            self._refresh_reservation(state)

        fill = Fill(
            fill_id=fill_id,
            order_id=state.request.order_id,
            instrument_id=state.request.instrument_id,
            side=state.request.side,
            quantity=quantity,
            base_price=base_price,
            price=price,
            gross_amount=gross,
            slippage_amount=slippage,
            fee=fee,
            cost_basis=cost_basis,
            realized_pnl=realized_pnl,
            occurred_at=occurred_at,
            ledger_transaction_id=transaction_id,
        )
        self._fills.append(fill)
        return fill

    def _buy_entries(
        self, fill_id: str, gross: Decimal, fee: Decimal
    ) -> list[LedgerEntry]:
        return self._entries(
            fill_id,
            [
                ("ASSET:SECURITIES", LedgerDirection.DEBIT, gross),
                ("EXPENSE:FEE", LedgerDirection.DEBIT, fee),
                ("ASSET:CASH", LedgerDirection.CREDIT, gross + fee),
            ],
        )

    def _sell_entries(
        self,
        fill_id: str,
        gross: Decimal,
        fee: Decimal,
        cost_basis: Decimal,
        realized_pnl: Decimal,
    ) -> list[LedgerEntry]:
        raw: list[tuple[str, LedgerDirection, Decimal]] = [
            ("ASSET:CASH", LedgerDirection.DEBIT, gross - fee),
            ("EXPENSE:FEE", LedgerDirection.DEBIT, fee),
            ("ASSET:SECURITIES", LedgerDirection.CREDIT, cost_basis),
        ]
        if realized_pnl > ZERO:
            raw.append(
                ("INCOME:REALIZED_PNL", LedgerDirection.CREDIT, realized_pnl)
            )
        elif realized_pnl < ZERO:
            raw.append(
                ("EXPENSE:REALIZED_LOSS", LedgerDirection.DEBIT, -realized_pnl)
            )
        return self._entries(fill_id, raw)

    @staticmethod
    def _entries(
        fill_id: str,
        values: list[tuple[str, LedgerDirection, Decimal]],
    ) -> list[LedgerEntry]:
        entries: list[LedgerEntry] = []
        for index, (account, direction, amount) in enumerate(values, start=1):
            if amount == ZERO:
                continue
            entries.append(
                LedgerEntry(
                    entry_id=str(
                        uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"idea2strategy:d23:entry:{fill_id}:{index}",
                        )
                    ),
                    account_code=account,
                    direction=direction,
                    amount=amount,
                    source_event_id=fill_id,
                )
            )
        return entries

    def _consume_fifo(self, instrument_id: str, quantity: Decimal) -> Decimal:
        lots = self._lots.get(instrument_id, [])
        remaining = quantity
        basis = ZERO
        while remaining > ZERO:
            if not lots:
                raise ExecutionModelValidationError(
                    "position changed below reserved sell quantity"
                )
            lot = lots[0]
            consumed = min(remaining, lot.quantity)
            unit_basis = lot.cost_basis / lot.quantity
            consumed_basis = unit_basis * consumed
            basis += consumed_basis
            lot.quantity -= consumed
            lot.cost_basis -= consumed_basis
            remaining -= consumed
            if lot.quantity == ZERO:
                lots.pop(0)
        return basis

    def _refresh_reservation(self, state: _OrderState) -> None:
        if state.request.side is OrderSide.SELL:
            state.reserved_quantity = state.remaining_quantity
            return
        estimated_price = self._slipped_price(
            OrderSide.BUY, state.request.reference_price
        )
        state.reserved_notional = estimated_price * state.remaining_quantity
        state.reserved_cash = state.reserved_notional * (ONE + self._policy.fee_rate)

    @staticmethod
    def _release_reservation(state: _OrderState) -> None:
        state.reserved_cash = ZERO
        state.reserved_notional = ZERO
        state.reserved_quantity = ZERO

    @staticmethod
    def _reject(state: _OrderState, reason_code: str) -> None:
        state.status = OrderStatus.REJECTED
        state.reason_code = reason_code

    @staticmethod
    def _is_open(state: _OrderState) -> bool:
        return state.status in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}

    @staticmethod
    def _order_key(state: _OrderState) -> tuple[datetime, str]:
        return state.request.submitted_at, state.request.order_id

    @staticmethod
    def _snapshot(state: _OrderState) -> BacktestOrder:
        request = state.request
        return BacktestOrder(
            order_id=request.order_id,
            instrument_id=request.instrument_id,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            remaining_quantity=state.remaining_quantity,
            status=state.status,
            submitted_at=request.submitted_at,
            eligible_at=request.eligible_at,
            expires_at=state.expires_at,
            reason_code=state.reason_code,
        )

    def _reserved_cash(self, excluding: _OrderState | None = None) -> Decimal:
        return sum(
            (
                state.reserved_cash
                for state in self._orders.values()
                if state is not excluding and self._is_open(state)
            ),
            ZERO,
        )

    def _reserved_notional(self, excluding: _OrderState | None = None) -> Decimal:
        return sum(
            (
                state.reserved_notional
                for state in self._orders.values()
                if state is not excluding and self._is_open(state)
            ),
            ZERO,
        )

    def _instrument_reserved_notional(
        self, instrument_id: str, excluding: _OrderState | None = None
    ) -> Decimal:
        return sum(
            (
                state.reserved_notional
                for state in self._orders.values()
                if state is not excluding
                and self._is_open(state)
                and state.request.instrument_id == instrument_id
            ),
            ZERO,
        )

    def _position_exposure(self) -> Decimal:
        return sum(
            (
                lot.cost_basis
                for instrument_lots in self._lots.values()
                for lot in instrument_lots
            ),
            ZERO,
        )
