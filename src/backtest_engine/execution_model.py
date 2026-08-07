"""Deterministic order, fill, position and double-entry ledger model for card D23.

D23 requires that, from the **next valid one-minute bar**, the engine evaluate the
price condition of each order type and reproduce fractional and partial fills,
DAY/GTC/GTD, a 0.2% fee, 0.05% slippage, budget and risk limits, and a
double-entry ledger.

Declared scope
--------------
This module is **long-only**. There is no short inventory, no margin and no borrow
fee; a SELL may never exceed the quantity already held (``POSITION_UNAVAILABLE``).
Short selling belongs to card F12 in the live ``trading-engine`` and is deliberately
absent here rather than half-modelled.

Quantity modes follow card **F08A** ("소수점 거래 가능 롱 종목의 시장가 DAY만 금액·소수점
주문을 허용하고 지정가·스탑·스탑리밋·트레일링 및 신규 숏은 정수 수량만 허용한다"), which
this module mirrors rule-for-rule against
``trading-domain/.../eligibility/OrderEligibilityPolicy.java``:

* ``WHOLE_SHARES`` - any order type. A non-integral quantity is **rejected**
  (``WHOLE_SHARES_REQUIRE_INTEGER_QUANTITY``); it is never silently floored.
* ``FRACTIONAL_SHARES`` and ``NOTIONAL_AMOUNT`` - only on a fractional-enabled
  instrument, only MARKET + DAY, and (for notional) only on the long side.

Everything the model computes or stores is money or a quantity, and every one of
those values is produced by :mod:`backtest_engine.money` (rule ``precision:1.0.0``),
the single quantization point, so that all results round-trip through the canonical
``numeric(24,8)`` columns.

Policy inputs
-------------
Nothing in this module invents a default. ``ExecutionPolicy`` supplies the fee rate,
slippage basis points, accounting rules version and buffer policy id that
``backtest.runs`` pins; :class:`ExecutionMicrostructurePolicy` and
:class:`InstrumentFractionalPolicy` supply the remaining values, are required
constructor arguments, and have no field defaults.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal, localcontext
from enum import Enum

from backtest_engine.elements.core import resolution_period
from backtest_engine.execution_policy import ExecutionPolicy
from backtest_engine.money import (
    PRECISION_RULES_VERSION,
    QUANTITY_QUANTUM,
    apply_rate,
    assert_quantized_money,
    quantize_money,
    quantize_quantity,
)


ZERO = Decimal("0")
ONE = Decimal("1")
BASIS_POINTS = Decimal(10_000)

# Wide enough that the intermediate division in _floor_to_quantum is exact for any
# value that can survive money.py's numeric(24,8) range check.
_WORKING_PRECISION = 60

CHART_OF_ACCOUNTS_VERSION = "accounting:1.0.0"
"""The only ``accounting_rules_version`` whose chart of accounts this build posts.

The account codes below are the ones F's live ledger writes
(``trading-messaging/.../contracts/trading/v1/ledger-transaction.json``:
``CASH``, ``SECURITY``, plus ``FEE_EXPENSE`` and ``REALIZED_PNL``). A run pinned to
any other accounting rules version is refused rather than posted into a chart of
accounts this build does not implement -- the same fail-loud rule
:mod:`backtest_engine.money` applies to ``precision_rules_version``.
"""

EXECUTION_MICROSTRUCTURE_RULES_VERSION = "d23-microstructure:1.0.0"

# ---------------------------------------------------------------------------
# Named execution rules. Every comparison below cites one of these instead of
# carrying a bare literal.
# ---------------------------------------------------------------------------

LIMIT_PROTECTED_PRICE_RULE = "market:limit-protected-slippage"
"""Slippage may move the price up to the limit but never through it.

The base price of a marketable limit order is already bounded by the limit
(``min(open, limit)`` for BUY, ``max(open, limit)`` for SELL), so clamping the
slipped price to the limit yields an executed price that always lies **between the
observed base price and the limit**. No price better than the market actually
traded is ever invented (card F08: "유리한 가격을 임의 생성하지 않는다"), and the limit
is never crossed. This is the same rule as F's
``RealisticFillModel.limitProtected``.
"""

VOLUME_PARTICIPATION_RULE = "market:bar-volume-participation"
"""A bar can only fill up to a declared fraction of the volume it actually traded.

F caps a live fill at the displayed size on the aggressing side
(``RealisticFillModel``: ``min(remaining, askSize|bidSize)``). A backtest has no
order book, only a one-minute bar, so the bar-data analogue is a participation
limit on the bar's traded volume. The capacity is computed once per bar and shared
by every order on that bar, so N orders cannot each claim the same liquidity.
"""

CAPACITY_FLOOR_RULE = "market:capacity-floor-to-quantum"
"""A fill is the largest whole quantum that fits inside every constraint.

Fill sizing rounds **down** to the order's quantity quantum, because rounding to
nearest could produce a fill that exceeds the cash, risk or liquidity capacity that
produced it. Any shortfall stays visible as an open remainder; it is never absorbed.
"""

NOTIONAL_RESIDUAL_RULE = "market:notional-residual-below-one-quantum"
"""A notional order is complete once its unspent budget cannot buy one quantum."""

ORDER_HORIZON_RULE = "market:order-horizon-within-policy"
"""No order may outlive the policy's maximum order horizon.

Canonical basis: ``db/schema.dbml`` ``trading.orders`` check
``order_expiry_within_ninety_days`` -- ``expires_at <= accepted_at + interval
'90 days'``. Both this horizon and the GTC horizon are required fields of the
run's :class:`~backtest_engine.execution_policy.ExecutionPolicy`; this module
reads them and has no value of its own to fall back on.
"""

REJECT_REASON_CODES = frozenset(
    {
        "FRACTIONAL_INSTRUMENT_NOT_ENABLED",
        "FRACTIONAL_REQUIRES_MARKET_DAY",
        "GROSS_EXPOSURE_EXCEEDED",
        "INSTRUMENT_EXPOSURE_EXCEEDED",
        "INSUFFICIENT_AVAILABLE_CASH",
        "NOTIONAL_REQUIRES_LONG_EXPOSURE",
        "ORDER_HORIZON_EXCEEDED",
        "POSITION_UNAVAILABLE",
        "STRATEGY_BUDGET_EXCEEDED",
        "WHOLE_SHARES_REQUIRE_INTEGER_QUANTITY",
    }
)
"""Every reason a submitted order can be refused. Mirrors F's
``OrderEligibilityReason`` for the F08A rules and adds D's budget/risk codes."""


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


class LedgerAccount(str, Enum):
    """The chart of accounts of :data:`CHART_OF_ACCOUNTS_VERSION`.

    These are F's live account codes, not a parallel D-only vocabulary. F keeps a
    single ``REALIZED_PNL`` account and encodes the sign in the entry direction
    (CREDIT for a gain, DEBIT for a loss), so this module does the same instead of
    inventing a second loss account.
    """

    CASH = "CASH"
    SECURITY = "SECURITY"
    FEE_EXPENSE = "FEE_EXPENSE"
    REALIZED_PNL = "REALIZED_PNL"


_ACCOUNT_CODES = frozenset(account.value for account in LedgerAccount)


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
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


def _is_integral(value: Decimal) -> bool:
    return value == value.to_integral_value(rounding=ROUND_FLOOR)


def _floor_to_quantum(value: Decimal, quantum: Decimal) -> Decimal:
    """Largest multiple of ``quantum`` that does not exceed ``value``.

    See :data:`CAPACITY_FLOOR_RULE`. This is a capacity floor, not a rounding rule:
    the result is exact at the quantum, so passing it through
    :func:`money.quantize_quantity` afterwards never changes it.
    """

    if value <= ZERO:
        return ZERO
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        steps = (value / quantum).to_integral_value(rounding=ROUND_FLOOR)
        return steps * quantum


@dataclass(frozen=True, slots=True)
class ExecutionMicrostructurePolicy:
    """The D23 microstructure values ``backtest.runs`` does not pin as columns.

    No field has a default. A caller must state each value, and the published
    :data:`D23_MICROSTRUCTURE_POLICY_V1` must be passed explicitly, so nothing in
    this module can fall back to an unstated policy.
    """

    version: str
    max_volume_participation_bps: int
    buying_power_buffer_policy_id: str
    buying_power_buffer_bps: int

    def __post_init__(self) -> None:
        if not isinstance(self.version, str) or not self.version:
            raise ExecutionModelValidationError(
                "microstructure policy version must not be empty"
            )
        participation = self.max_volume_participation_bps
        if (
            not isinstance(participation, int)
            or isinstance(participation, bool)
            or not 0 < participation <= BASIS_POINTS
        ):
            raise ExecutionModelValidationError(
                "max_volume_participation_bps must be between 1 and 10000 basis points"
            )
        buffer_bps = self.buying_power_buffer_bps
        if (
            not isinstance(buffer_bps, int)
            or isinstance(buffer_bps, bool)
            or not 0 <= buffer_bps < BASIS_POINTS
        ):
            raise ExecutionModelValidationError(
                "buying_power_buffer_bps must be between 0 and 9999 basis points"
            )
        object.__setattr__(
            self,
            "buying_power_buffer_policy_id",
            _uuid(self.buying_power_buffer_policy_id, "buying_power_buffer_policy_id"),
        )

    @property
    def max_volume_participation_rate(self) -> Decimal:
        """The basis-point cap read back as an exact decimal rate."""
        return Decimal(self.max_volume_participation_bps) / BASIS_POINTS

    @property
    def buying_power_buffer_rate(self) -> Decimal:
        return Decimal(self.buying_power_buffer_bps) / BASIS_POINTS


D23_MICROSTRUCTURE_POLICY_V1 = ExecutionMicrostructurePolicy(
    version=EXECUTION_MICROSTRUCTURE_RULES_VERSION,
    # 10.00%. A fill of at most a tenth of a one-minute bar's traded volume can be
    # assumed to consume liquidity the bar's own OHLC already reflects, so the model
    # needs no price-impact term; above that, filling at an OHLC-derived price would
    # invent liquidity that did not exist. See VOLUME_PARTICIPATION_RULE.
    max_volume_participation_bps=1000,
    # The GTC and maximum order horizons are NOT here: they are pinned per run
    # by `ExecutionPolicy` (`execution_policy.py`), which is what
    # `backtest.runs` records. See ORDER_HORIZON_RULE.
    # db/schema.dbml trading.buying_power_buffer_policy_versions seed row
    # '00000000-0000-4000-8000-000000000001' (policy_code REVIEW_EXAMPLE, buffer_bps 1),
    # which is also the id D17_EXECUTION_POLICY_FIXTURE pins.
    buying_power_buffer_policy_id="00000000-0000-4000-8000-000000000001",
    buying_power_buffer_bps=1,
)


@dataclass(frozen=True, slots=True)
class InstrumentFractionalPolicy:
    """Which instruments may be traded fractionally, and under which catalog version.

    Mirrors F's ``InstrumentFractionalPolicy(instrumentId, fractionalEnabled,
    policyVersion)``. There is no canonical column for fractional eligibility yet,
    so it is an explicit required input: an empty set means "no fractional trading",
    stated by the caller rather than assumed by this module.
    """

    policy_version: str
    fractional_instrument_ids: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.policy_version, str) or not self.policy_version:
            raise ExecutionModelValidationError(
                "fractional policy_version must not be empty"
            )
        normalised = frozenset(
            _uuid(value, "fractional_instrument_id")
            for value in self.fractional_instrument_ids
        )
        object.__setattr__(self, "fractional_instrument_ids", normalised)

    def enabled_for(self, instrument_id: str) -> bool:
        return instrument_id in self.fractional_instrument_ids


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_strategy_notional: Decimal
    max_gross_exposure: Decimal
    max_instrument_exposure: Decimal

    def __post_init__(self) -> None:
        for name in (
            "max_strategy_notional",
            "max_gross_exposure",
            "max_instrument_exposure",
        ):
            value = _positive(getattr(self, name), name)
            object.__setattr__(self, name, quantize_money(value, name))


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """An immutable order contract.

    Exactly one of ``quantity`` and ``notional_amount`` is supplied, mirroring the
    canonical ``trading.order_intents`` check ``intent_exactly_one_requested_measure``.
    Only structural well-formedness is validated here; every policy rule (F08A
    eligibility, the order horizon, budget and risk) is a submit-time rejection with
    a reason code, so a refused order still appears in the result snapshot.
    """

    order_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    quantity_mode: QuantityMode
    time_in_force: TimeInForce
    submitted_at: datetime
    eligible_at: datetime
    day_expires_at: datetime
    reference_price: Decimal
    quantity: Decimal | None = None
    notional_amount: Decimal | None = None
    expires_at: datetime | None = None
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    trail_percent: Decimal | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "order_id", _uuid(self.order_id, "order_id"))
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        for label, enum_type in (
            ("side", OrderSide),
            ("order_type", OrderType),
            ("quantity_mode", QuantityMode),
            ("time_in_force", TimeInForce),
        ):
            if not isinstance(getattr(self, label), enum_type):
                raise ExecutionModelValidationError(f"{label} is unsupported")
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

        # db/schema.dbml trading.orders check `order_market_contract_valid`.
        if self.order_type is OrderType.MARKET and self.time_in_force is not TimeInForce.DAY:
            raise ExecutionModelValidationError("MARKET orders require DAY")
        self._validate_requested_measure()
        self._validate_parameters()
        self._validate_expiry()

    def _validate_requested_measure(self) -> None:
        if (self.quantity is None) == (self.notional_amount is None):
            raise ExecutionModelValidationError(
                "an order carries exactly one of quantity or notional_amount"
            )
        if self.quantity_mode is QuantityMode.NOTIONAL_AMOUNT:
            if self.notional_amount is None:
                raise ExecutionModelValidationError(
                    "NOTIONAL_AMOUNT orders require notional_amount"
                )
            object.__setattr__(
                self,
                "notional_amount",
                quantize_money(
                    _positive(self.notional_amount, "notional_amount"), "notional_amount"
                ),
            )
            return
        if self.quantity is None:
            raise ExecutionModelValidationError(
                f"{self.quantity_mode.value} orders require quantity"
            )
        _positive(self.quantity, "quantity")

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
            ("limit_price", "stop_price", "trail_percent"), supplied, strict=True
        ):
            if value is not None:
                _positive(value, label)
        if self.trail_percent is not None and self.trail_percent > ONE:
            raise ExecutionModelValidationError("trail_percent must be at most 1")

    def _validate_expiry(self) -> None:
        if self.time_in_force is TimeInForce.GTD:
            if self.expires_at is None:
                raise ExecutionModelValidationError("GTD requires expires_at")
            expires_at = _utc(self.expires_at, "expires_at")
            # The policy horizon cap is applied at submit time; only ordering is
            # structural. Canonical: trading.orders `order_expiry_after_acceptance`.
            if expires_at <= self.eligible_at:
                raise ExecutionModelValidationError(
                    "GTD expires_at must follow eligibility"
                )
            object.__setattr__(self, "expires_at", expires_at)
        elif self.expires_at is not None:
            raise ExecutionModelValidationError("expires_at is allowed only for GTD")


@dataclass(frozen=True, slots=True)
class ExecutionBar:
    """A completed bar used for deterministic fills at any supported resolution."""

    instrument_id: str
    starts_at: datetime
    ends_at: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    complete: bool = True
    resolution: str = field(default="1m", kw_only=True)
    session_truncated: bool = field(default=False, kw_only=True)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        starts_at = _utc(self.starts_at, "starts_at")
        ends_at = _utc(self.ends_at, "ends_at")
        period = resolution_period(self.resolution)
        actual_period = ends_at - starts_at
        if actual_period <= timedelta(0) or (
            actual_period != period
            and (not self.session_truncated or actual_period > period)
        ):
            raise ExecutionModelValidationError(
                "bar must cover its declared resolution or be a shorter "
                "session-truncated bar"
            )
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
        if not isinstance(self.session_truncated, bool):
            raise ExecutionModelValidationError("session_truncated must be boolean")


@dataclass(frozen=True, slots=True)
class MinuteBar(ExecutionBar):
    """Backward-compatible exact one-minute execution bar."""

    resolution: str = field(default="1m", init=False, repr=False)


@dataclass(frozen=True, slots=True)
class BacktestOrder:
    """Read model of an order.

    The share fields keep their original position so existing consumers keep
    working; the notional fields are appended and default to "not a notional
    order". These are data defaults on a projection, not policy defaults: the model
    always populates every field explicitly.
    """

    order_id: str
    instrument_id: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal | None
    remaining_quantity: Decimal | None
    status: OrderStatus
    submitted_at: datetime
    eligible_at: datetime
    expires_at: datetime
    reason_code: str | None
    notional_amount: Decimal | None = None
    remaining_notional: Decimal | None = None
    filled_quantity: Decimal = ZERO


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
        if not isinstance(self.direction, LedgerDirection):
            raise ExecutionModelValidationError("direction is unsupported")
        if not self.account_code:
            raise ExecutionModelValidationError("account_code must not be empty")
        if self.account_code not in _ACCOUNT_CODES:
            raise ExecutionModelValidationError(
                f"{self.account_code} is not in the {CHART_OF_ACCOUNTS_VERSION} "
                "chart of accounts"
            )
        _positive(self.amount, "ledger amount")
        assert_quantized_money(self.amount, "ledger amount")
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
    remaining_quantity: Decimal | None
    remaining_notional: Decimal | None
    filled_quantity: Decimal = ZERO
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
        *,
        microstructure: ExecutionMicrostructurePolicy,
        fractional_policy: InstrumentFractionalPolicy,
    ) -> None:
        if not isinstance(policy, ExecutionPolicy):
            raise ExecutionModelValidationError("policy must be an ExecutionPolicy")
        if not isinstance(microstructure, ExecutionMicrostructurePolicy):
            raise ExecutionModelValidationError(
                "microstructure must be an ExecutionMicrostructurePolicy"
            )
        if not isinstance(fractional_policy, InstrumentFractionalPolicy):
            raise ExecutionModelValidationError(
                "fractional_policy must be an InstrumentFractionalPolicy"
            )
        if not isinstance(risk_limits, RiskLimits):
            raise ExecutionModelValidationError("risk_limits must be RiskLimits")
        if policy.accounting_rules_version != CHART_OF_ACCOUNTS_VERSION:
            raise ExecutionModelValidationError(
                f"this build posts only the {CHART_OF_ACCOUNTS_VERSION} chart of "
                f"accounts, not {policy.accounting_rules_version}"
            )
        if (
            microstructure.buying_power_buffer_policy_id
            != policy.buying_power_buffer_policy_id
        ):
            raise ExecutionModelValidationError(
                "microstructure buying_power_buffer_policy_id must match the run's "
                "pinned buying_power_buffer_policy_id"
            )
        self._policy = policy
        self._microstructure = microstructure
        self._fractional_policy = fractional_policy
        self._risk_limits = risk_limits
        self._cash = quantize_money(
            _non_negative(initial_cash, "initial_cash"), "initial_cash"
        )
        self._orders: dict[str, _OrderState] = {}
        self._lots: dict[str, list[_Lot]] = {}
        self._fills: list[Fill] = []
        self._ledger: list[LedgerTransaction] = []
        self._now: datetime | None = None

    # -- published versions -------------------------------------------------

    @property
    def model_version(self) -> str:
        return self._policy.calculation_model_version

    @property
    def microstructure_version(self) -> str:
        return self._microstructure.version

    @property
    def chart_of_accounts_version(self) -> str:
        return self._policy.accounting_rules_version

    @property
    def precision_rules_version(self) -> str:
        return PRECISION_RULES_VERSION

    @property
    def fractional_policy_version(self) -> str:
        return self._fractional_policy.policy_version

    # -- state --------------------------------------------------------------

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def buying_power(self) -> Decimal:
        """Cash less the pinned buying-power buffer.

        Canonical: ``trading.buying_power_buffer_policy_versions.buffer_bps``, which
        every order-sizing decision in ``trading`` reserves against. New commitments
        are measured against this, never against raw cash; settled cash movements
        still use the full balance.
        """

        buffer_amount = apply_rate(
            self._cash, self._microstructure.buying_power_buffer_rate, "buying_power_buffer"
        )
        return quantize_money(self._cash - buffer_amount, "buying_power")

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
        quantity = quantize_quantity(
            quantity,
            fractional_eligible=self._fractional_policy.enabled_for(instrument_id),
            label="seed quantity",
        )
        self._lots.setdefault(instrument_id, []).append(
            _Lot(quantity, quantize_money(quantity * unit_cost, "seed cost_basis"))
        )

    def position(self, instrument_id: str) -> PositionSnapshot:
        instrument_id = _uuid(instrument_id, "instrument_id")
        lots = self._lots.get(instrument_id, [])
        return PositionSnapshot(
            instrument_id,
            sum((lot.quantity for lot in lots), ZERO),
            quantize_money(sum((lot.cost_basis for lot in lots), ZERO), "cost_basis"),
        )

    def order(self, order_id: str) -> BacktestOrder:
        return self._snapshot(self._state(order_id))

    def submit(self, request: OrderRequest) -> BacktestOrder:
        if not isinstance(request, OrderRequest):
            raise ExecutionModelValidationError("request must be an OrderRequest")
        if request.order_id in self._orders:
            raise ExecutionModelValidationError("order_id must be unique")

        expires_at = self._expiry_of(request)
        state = _OrderState(
            request=request,
            expires_at=expires_at,
            remaining_quantity=request.quantity,
            remaining_notional=request.notional_amount,
            trail_reference=quantize_money(request.reference_price, "trail_reference")
            if request.order_type is OrderType.TRAILING_STOP
            else None,
        )
        self._orders[request.order_id] = state

        eligibility = self._eligibility_reason(request)
        if eligibility is not None:
            self._reject(state, eligibility)
            return self._snapshot(state)
        if expires_at > request.submitted_at + self._policy.max_order_horizon:
            # ORDER_HORIZON_RULE
            self._reject(state, "ORDER_HORIZON_EXCEEDED")
            return self._snapshot(state)

        if request.quantity is not None:
            state.remaining_quantity = quantize_quantity(
                request.quantity,
                fractional_eligible=request.quantity_mode is not QuantityMode.WHOLE_SHARES,
                label="quantity",
            )
        self._reserve_or_reject(state)
        return self._snapshot(state)

    def cancel(self, order_id: str, cancelled_at: datetime) -> BacktestOrder:
        _utc(cancelled_at, "cancelled_at")
        state = self._state(order_id)
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
                    TimeInForce.GTC: "GTC_HORIZON_EXPIRED",
                    TimeInForce.GTD: "GTD_EXPIRED",
                }[state.request.time_in_force]
                self._release_reservation(state)
                expired.append(self._snapshot(state))
        return tuple(expired)

    def process_bars(self, bars: Iterable[ExecutionBar]) -> tuple[Fill, ...]:
        ordered = list(bars)
        if any(not isinstance(bar, ExecutionBar) for bar in ordered):
            raise ExecutionModelValidationError("bars must contain ExecutionBar values")
        fills: list[Fill] = []
        for bar in sorted(ordered, key=lambda item: (item.starts_at, item.instrument_id)):
            fills.extend(self.process_bar(bar))
        return tuple(fills)

    def process_bar(self, bar: ExecutionBar) -> tuple[Fill, ...]:
        if not isinstance(bar, ExecutionBar):
            raise ExecutionModelValidationError("bar must be an ExecutionBar")
        self.advance_time(bar.starts_at)
        if not bar.complete:
            return ()
        # VOLUME_PARTICIPATION_RULE: one capacity per bar, shared by every order.
        capacity = _floor_to_quantum(
            bar.volume * self._microstructure.max_volume_participation_rate,
            QUANTITY_QUANTUM,
        )
        if capacity <= ZERO:
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
            if capacity <= ZERO:
                break
            base_price = self._base_price(state, bar)
            if base_price is None:
                continue
            base_price = quantize_money(base_price, "base_price")
            price = self._execution_price(state.request, base_price)
            quantity = self._fillable_quantity(state, price, capacity)
            if quantity <= ZERO:
                continue
            capacity -= quantity
            fills.append(self._record_fill(state, bar.ends_at, quantity, base_price, price))
        return tuple(fills)

    # -- eligibility (card F08A) -------------------------------------------

    def _eligibility_reason(self, request: OrderRequest) -> str | None:
        """The F08A rule this request breaks, or ``None``.

        Rule order matches F's ``OrderEligibilityPolicy``: instrument enablement,
        then position effect, then the MARKET/DAY restriction.
        """

        if request.quantity_mode is QuantityMode.WHOLE_SHARES:
            assert request.quantity is not None
            if not _is_integral(request.quantity):
                return "WHOLE_SHARES_REQUIRE_INTEGER_QUANTITY"
            return None
        if not self._fractional_policy.enabled_for(request.instrument_id):
            return "FRACTIONAL_INSTRUMENT_NOT_ENABLED"
        if (
            request.quantity_mode is QuantityMode.NOTIONAL_AMOUNT
            and request.side is not OrderSide.BUY
        ):
            # A notional disposal would have to guess a price to size itself and
            # could overshoot the held lot; F08A only permits amount orders to
            # increase long exposure.
            return "NOTIONAL_REQUIRES_LONG_EXPOSURE"
        if (
            request.order_type is not OrderType.MARKET
            or request.time_in_force is not TimeInForce.DAY
        ):
            return "FRACTIONAL_REQUIRES_MARKET_DAY"
        return None

    def _expiry_of(self, request: OrderRequest) -> datetime:
        if request.time_in_force is TimeInForce.DAY:
            return request.day_expires_at
        if request.time_in_force is TimeInForce.GTC:
            return request.submitted_at + self._policy.good_till_cancelled_horizon
        assert request.expires_at is not None
        return request.expires_at

    # -- reservations -------------------------------------------------------

    def _reserve_or_reject(self, state: _OrderState) -> None:
        request = state.request
        if request.side is OrderSide.SELL:
            assert state.remaining_quantity is not None
            available = self.position(request.instrument_id).quantity - sum(
                (
                    other.reserved_quantity
                    for other in self._orders.values()
                    if other is not state
                    and self._is_open(other)
                    and other.request.instrument_id == request.instrument_id
                    and other.request.side is OrderSide.SELL
                ),
                ZERO,
            )
            if available < state.remaining_quantity:
                self._reject(state, "POSITION_UNAVAILABLE")
                return
            state.reserved_quantity = state.remaining_quantity
            return

        estimated_notional, estimated_cash = self._estimated_commitment(state)
        available_cash = self.buying_power - self._reserved_cash(excluding=state)
        if estimated_cash > available_cash:
            self._reject(state, "INSUFFICIENT_AVAILABLE_CASH")
            return
        current_exposure = self._position_exposure()
        other_reserved = self._reserved_notional(excluding=state)
        if (
            current_exposure + other_reserved + estimated_cash
            > self._risk_limits.max_strategy_notional
        ):
            self._reject(state, "STRATEGY_BUDGET_EXCEEDED")
            return
        if (
            current_exposure + other_reserved + estimated_notional
            > self._risk_limits.max_gross_exposure
        ):
            self._reject(state, "GROSS_EXPOSURE_EXCEEDED")
            return
        instrument_exposure = self.position(
            request.instrument_id
        ).cost_basis + self._instrument_reserved_notional(
            request.instrument_id, excluding=state
        )
        if (
            instrument_exposure + estimated_notional
            > self._risk_limits.max_instrument_exposure
        ):
            self._reject(state, "INSTRUMENT_EXPOSURE_EXCEEDED")
            return
        state.reserved_cash = estimated_cash
        state.reserved_notional = estimated_notional

    def _estimated_commitment(self, state: _OrderState) -> tuple[Decimal, Decimal]:
        """The notional and the cash (notional plus fee) an open BUY still needs."""

        if state.remaining_notional is not None:
            notional = state.remaining_notional
        else:
            assert state.remaining_quantity is not None
            estimated_price = self._estimated_price(
                OrderSide.BUY, state.request.reference_price
            )
            notional = quantize_money(
                estimated_price * state.remaining_quantity, "estimated_notional"
            )
        fee = apply_rate(notional, self._policy.fee_rate, "estimated_fee")
        return notional, quantize_money(notional + fee, "estimated_cash")

    def _refresh_reservation(self, state: _OrderState) -> None:
        if state.request.side is OrderSide.SELL:
            assert state.remaining_quantity is not None
            state.reserved_quantity = state.remaining_quantity
            return
        state.reserved_notional, state.reserved_cash = self._estimated_commitment(state)

    @staticmethod
    def _release_reservation(state: _OrderState) -> None:
        state.reserved_cash = ZERO
        state.reserved_notional = ZERO
        state.reserved_quantity = ZERO

    # -- prices -------------------------------------------------------------

    def _base_price(self, state: _OrderState, bar: ExecutionBar) -> Decimal | None:
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
        offset = apply_rate(state.trail_reference, request.trail_percent, "trail_offset")
        if request.side is OrderSide.SELL:
            stop = quantize_money(state.trail_reference - offset, "trailing_stop")
            if bar.open <= stop:
                return bar.open
            if bar.low <= stop:
                return stop
            state.trail_reference = quantize_money(
                max(state.trail_reference, bar.high), "trail_reference"
            )
            return None
        stop = quantize_money(state.trail_reference + offset, "trailing_stop")
        if bar.open >= stop:
            return bar.open
        if bar.high >= stop:
            return stop
        state.trail_reference = quantize_money(
            min(state.trail_reference, bar.low), "trail_reference"
        )
        return None

    @staticmethod
    def _limit_base(
        side: OrderSide, limit_price: Decimal, bar: ExecutionBar
    ) -> Decimal | None:
        if side is OrderSide.BUY:
            return min(bar.open, limit_price) if bar.low <= limit_price else None
        return max(bar.open, limit_price) if bar.high >= limit_price else None

    @staticmethod
    def _stop_base(
        side: OrderSide, stop_price: Decimal, bar: ExecutionBar
    ) -> Decimal | None:
        if side is OrderSide.BUY:
            if bar.open >= stop_price:
                return bar.open
            return stop_price if bar.high >= stop_price else None
        if bar.open <= stop_price:
            return bar.open
        return stop_price if bar.low <= stop_price else None

    def _estimated_price(self, side: OrderSide, reference_price: Decimal) -> Decimal:
        """Slipped reference price used for reservations, before any limit clamp."""

        slippage = apply_rate(
            reference_price, self._policy.slippage_rate, "estimated_slippage"
        )
        if side is OrderSide.SELL:
            return quantize_money(reference_price - slippage, "estimated_price")
        return quantize_money(reference_price + slippage, "estimated_price")

    def _execution_price(self, request: OrderRequest, base_price: Decimal) -> Decimal:
        """Slipped price, clamped to the limit. See :data:`LIMIT_PROTECTED_PRICE_RULE`."""

        slippage = apply_rate(base_price, self._policy.slippage_rate, "slippage")
        limit = request.limit_price
        if request.side is OrderSide.BUY:
            price = quantize_money(base_price + slippage, "price")
            if limit is not None and price > limit:
                return quantize_money(limit, "price")
            return price
        price = quantize_money(base_price - slippage, "price")
        if limit is not None and price < limit:
            return quantize_money(limit, "price")
        return price

    # -- sizing -------------------------------------------------------------

    def _fillable_quantity(
        self, state: _OrderState, price: Decimal, bar_capacity: Decimal
    ) -> Decimal:
        request = state.request
        # VOLUME_PARTICIPATION_RULE applies to both sides.
        caps: list[Decimal] = [bar_capacity]
        if request.side is OrderSide.SELL:
            assert state.remaining_quantity is not None
            caps.append(state.remaining_quantity)
            caps.append(self.position(request.instrument_id).quantity)
        else:
            fee = apply_rate(price, self._policy.fee_rate, "unit_fee")
            unit_cash = quantize_money(price + fee, "unit_cash")
            if state.remaining_notional is not None:
                caps.append(state.remaining_notional / price)
            else:
                assert state.remaining_quantity is not None
                caps.append(state.remaining_quantity)
            position_exposure = self._position_exposure()
            caps.append(
                max(self.buying_power - self._reserved_cash(excluding=state), ZERO)
                / unit_cash
            )
            caps.append(
                max(
                    self._risk_limits.max_strategy_notional
                    - position_exposure
                    - self._reserved_cash(excluding=state),
                    ZERO,
                )
                / unit_cash
            )
            caps.append(
                max(
                    self._risk_limits.max_gross_exposure
                    - position_exposure
                    - self._reserved_notional(excluding=state),
                    ZERO,
                )
                / price
            )
            caps.append(
                max(
                    self._risk_limits.max_instrument_exposure
                    - self.position(request.instrument_id).cost_basis
                    - self._instrument_reserved_notional(
                        request.instrument_id, excluding=state
                    ),
                    ZERO,
                )
                / price
            )

        fractional = request.quantity_mode is not QuantityMode.WHOLE_SHARES
        quantum = QUANTITY_QUANTUM if fractional else ONE
        # CAPACITY_FLOOR_RULE: round down, then re-assert the canonical scale.
        quantity = _floor_to_quantum(min(caps), quantum)
        return quantize_quantity(
            quantity, fractional_eligible=fractional, label="fill quantity"
        )

    # -- fills --------------------------------------------------------------

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
        gross = quantize_money(price * quantity, "gross_amount")
        fee = apply_rate(gross, self._policy.fee_rate, "fee")
        slippage = quantize_money(
            (price - base_price).copy_abs() * quantity, "slippage_amount"
        )
        if state.request.side is OrderSide.BUY:
            self._cash = quantize_money(self._cash - gross - fee, "cash")
            self._lots.setdefault(state.request.instrument_id, []).append(
                _Lot(quantity, gross)
            )
            cost_basis = gross
            realized_pnl = quantize_money(ZERO, "realized_pnl")
            entries = self._buy_entries(fill_id, gross, fee)
        else:
            cost_basis = self._consume_fifo(state.request.instrument_id, quantity)
            self._cash = quantize_money(self._cash + gross - fee, "cash")
            realized_pnl = quantize_money(gross - cost_basis, "realized_pnl")
            entries = self._sell_entries(fill_id, gross, fee, cost_basis, realized_pnl)

        transaction_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"idea2strategy:d23:ledger:{fill_id}")
        )
        self._ledger.append(
            LedgerTransaction(transaction_id, fill_id, occurred_at, tuple(entries))
        )

        state.filled_quantity = quantize_quantity(
            state.filled_quantity + quantity,
            fractional_eligible=state.request.quantity_mode is not QuantityMode.WHOLE_SHARES,
            label="filled_quantity",
        )
        self._advance_remainder(state, quantity, gross, price)

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

    def _advance_remainder(
        self, state: _OrderState, quantity: Decimal, gross: Decimal, price: Decimal
    ) -> None:
        if state.remaining_notional is not None:
            state.remaining_notional = quantize_money(
                max(state.remaining_notional - gross, ZERO), "remaining_notional"
            )
            # NOTIONAL_RESIDUAL_RULE
            complete = state.remaining_notional < price * QUANTITY_QUANTUM
        else:
            assert state.remaining_quantity is not None
            state.remaining_quantity = quantize_quantity(
                state.remaining_quantity - quantity,
                fractional_eligible=state.request.quantity_mode
                is not QuantityMode.WHOLE_SHARES,
                label="remaining_quantity",
            )
            complete = state.remaining_quantity == ZERO
        if complete:
            state.status = OrderStatus.FILLED
            self._release_reservation(state)
        else:
            state.status = OrderStatus.PARTIALLY_FILLED
            self._refresh_reservation(state)

    # -- ledger -------------------------------------------------------------

    def _buy_entries(
        self, fill_id: str, gross: Decimal, fee: Decimal
    ) -> list[LedgerEntry]:
        return self._entries(
            fill_id,
            [
                (LedgerAccount.SECURITY, LedgerDirection.DEBIT, gross),
                (LedgerAccount.FEE_EXPENSE, LedgerDirection.DEBIT, fee),
                (
                    LedgerAccount.CASH,
                    LedgerDirection.CREDIT,
                    quantize_money(gross + fee, "cash entry"),
                ),
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
        raw: list[tuple[LedgerAccount, LedgerDirection, Decimal]] = [
            (
                LedgerAccount.CASH,
                LedgerDirection.DEBIT,
                quantize_money(gross - fee, "cash entry"),
            ),
            (LedgerAccount.FEE_EXPENSE, LedgerDirection.DEBIT, fee),
            (LedgerAccount.SECURITY, LedgerDirection.CREDIT, cost_basis),
        ]
        if realized_pnl > ZERO:
            raw.append((LedgerAccount.REALIZED_PNL, LedgerDirection.CREDIT, realized_pnl))
        elif realized_pnl < ZERO:
            raw.append(
                (
                    LedgerAccount.REALIZED_PNL,
                    LedgerDirection.DEBIT,
                    quantize_money(-realized_pnl, "realized loss"),
                )
            )
        return self._entries(fill_id, raw)

    @staticmethod
    def _entries(
        fill_id: str,
        values: Sequence[tuple[LedgerAccount, LedgerDirection, Decimal]],
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
                    account_code=account.value,
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
            if consumed == lot.quantity:
                # Retiring a lot consumes its recorded basis exactly, so repeated
                # partial sales can never strand or invent a sub-quantum residue.
                consumed_basis = lot.cost_basis
            else:
                consumed_basis = quantize_money(
                    lot.cost_basis * consumed / lot.quantity, "cost_basis"
                )
            basis += consumed_basis
            lot.quantity -= consumed
            lot.cost_basis = quantize_money(lot.cost_basis - consumed_basis, "cost_basis")
            remaining -= consumed
            if lot.quantity == ZERO:
                lots.pop(0)
        return quantize_money(basis, "cost_basis")

    # -- helpers ------------------------------------------------------------

    def _state(self, order_id: str) -> _OrderState:
        normalised = _uuid(order_id, "order_id")
        try:
            return self._orders[normalised]
        except KeyError as exc:
            raise KeyError(f"unknown order_id: {normalised}") from exc

    @staticmethod
    def _reject(state: _OrderState, reason_code: str) -> None:
        if reason_code not in REJECT_REASON_CODES:
            raise ExecutionModelValidationError(f"undeclared reject code: {reason_code}")
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
            notional_amount=request.notional_amount,
            remaining_quantity=state.remaining_quantity,
            remaining_notional=state.remaining_notional,
            filled_quantity=state.filled_quantity,
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
                and state.request.side is OrderSide.BUY
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
