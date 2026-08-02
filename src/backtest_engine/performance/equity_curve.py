"""The mark-to-market equity curve every D25 metric is derived from.

This module is deliberately free of any dependency on ``result_snapshot`` so
that the snapshot layer can depend on it without a cycle. It speaks a minimal
ledger vocabulary:

``LedgerEvent``
    One instant at which cash moved and/or the position book changed. The
    caller supplies the *signed cash delta* and the *complete* position state
    after the event, so nothing here has to know about order sides.

``ValuationSeries``
    The grid the curve is sampled on, plus the basis used to value open
    positions at each sample.

Two valuation bases exist and both are named, versioned rules stored on the
resulting curve. Neither is a hidden default: a caller that supplies marks gets
``MARK_TO_MARKET``, and a caller that supplies no grid at all must go through
:meth:`ValuationSeries.event_driven`, which is explicit about producing
``COST_BASIS`` / ``EVENT``.

``MARK_TO_MARKET`` (``equity.valuation:mark_to_market:1.0.0``)
    Open positions are valued at the mark price supplied for that instant. A
    holding with no mark at that instant is an error, not a fallback: silently
    substituting cost basis would report a fabricated unrealised P&L of zero.

``COST_BASIS`` (``equity.valuation:cost_basis:1.0.0``)
    Open positions are valued at their recorded cost basis, so unrealised P&L
    is zero *by definition of the basis* and the curve measures realised
    performance only. Every metric computed from such a curve is qualified by
    the ``valuationBasis`` field of the metrics document.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from enum import Enum

from backtest_engine.money import quantize_money

from .catalog import PerformanceCalculationError


__all__ = [
    "COST_BASIS_RULE_ID",
    "MARK_TO_MARKET_RULE_ID",
    "OPENING_INSTANT_LEAD",
    "OPENING_INSTANT_RULE_ID",
    "EquityCurve",
    "EquityPoint",
    "Holding",
    "LedgerEvent",
    "MarkPrice",
    "PositionState",
    "ValuationBasis",
    "ValuationInstant",
    "ValuationPeriodicity",
    "ValuationSeries",
    "build_equity_curve",
]


MARK_TO_MARKET_RULE_ID = "equity.valuation:mark_to_market:1.0.0"
COST_BASIS_RULE_ID = "equity.valuation:cost_basis:1.0.0"

#: ``equity.opening_instant:first_activity_minus_1us:1.0.0``. The opening state
#: has to be observable *strictly before* the first ledger event, otherwise the
#: first event's effect would already be inside the baseline and the first
#: periodic return would be zero. One microsecond is the resolution of
#: ``datetime``, so this is the smallest lead that keeps the grid strictly
#: increasing.
OPENING_INSTANT_RULE_ID = "equity.opening_instant:first_activity_minus_1us:1.0.0"
OPENING_INSTANT_LEAD = timedelta(microseconds=1)

_ZERO = Decimal("0")
# Wide enough that a cost-basis unit price stays exact before it is quantised.
_WORKING_PRECISION = 60


class ValuationBasis(str, Enum):
    MARK_TO_MARKET = "MARK_TO_MARKET"
    COST_BASIS = "COST_BASIS"

    @property
    def rule_id(self) -> str:
        return MARK_TO_MARKET_RULE_ID if self is ValuationBasis.MARK_TO_MARKET else COST_BASIS_RULE_ID


class ValuationPeriodicity(str, Enum):
    """DAILY is one sample per trading session close; EVENT is one per ledger event."""

    DAILY = "DAILY"
    EVENT = "EVENT"


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PerformanceCalculationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _finite(value: Decimal, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise PerformanceCalculationError(f"{label} must be a finite Decimal")
    return value


def _non_negative(value: Decimal, label: str) -> Decimal:
    if _finite(value, label) < _ZERO:
        raise PerformanceCalculationError(f"{label} must be non-negative")
    return value


def _positive(value: Decimal, label: str) -> Decimal:
    if _finite(value, label) <= _ZERO:
        raise PerformanceCalculationError(f"{label} must be positive")
    return value


@dataclass(frozen=True, slots=True, order=True)
class MarkPrice:
    instrument_id: str
    price: Decimal

    def __post_init__(self) -> None:
        if not self.instrument_id:
            raise PerformanceCalculationError("mark instrument_id must not be empty")
        _positive(self.price, "mark price")


@dataclass(frozen=True, slots=True, order=True)
class PositionState:
    """The complete state of one holding after a ledger event."""

    instrument_id: str
    quantity: Decimal
    cost_basis: Decimal

    def __post_init__(self) -> None:
        if not self.instrument_id:
            raise PerformanceCalculationError("position instrument_id must not be empty")
        _positive(self.quantity, "position quantity")
        _non_negative(self.cost_basis, "position cost_basis")


@dataclass(frozen=True, slots=True)
class LedgerEvent:
    as_of: datetime
    cash_delta: Decimal
    positions: tuple[PositionState, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of, "ledger event as_of"))
        _finite(self.cash_delta, "ledger event cash_delta")
        positions = tuple(self.positions)
        if any(not isinstance(item, PositionState) for item in positions):
            raise PerformanceCalculationError("ledger event positions must be PositionState values")
        if len({item.instrument_id for item in positions}) != len(positions):
            raise PerformanceCalculationError("ledger event positions must be unique per instrument")
        object.__setattr__(self, "positions", tuple(sorted(positions)))


@dataclass(frozen=True, slots=True)
class ValuationInstant:
    as_of: datetime
    marks: tuple[MarkPrice, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of, "valuation instant as_of"))
        marks = tuple(self.marks)
        if any(not isinstance(item, MarkPrice) for item in marks):
            raise PerformanceCalculationError("valuation instant marks must be MarkPrice values")
        if len({item.instrument_id for item in marks}) != len(marks):
            raise PerformanceCalculationError("valuation instant marks must be unique per instrument")
        object.__setattr__(self, "marks", tuple(sorted(marks)))


@dataclass(frozen=True, slots=True)
class ValuationSeries:
    basis: ValuationBasis
    periodicity: ValuationPeriodicity
    opening_at: datetime
    instants: tuple[ValuationInstant, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.basis, ValuationBasis):
            raise PerformanceCalculationError("valuation basis is unsupported")
        if not isinstance(self.periodicity, ValuationPeriodicity):
            raise PerformanceCalculationError("valuation periodicity is unsupported")
        object.__setattr__(self, "opening_at", _utc(self.opening_at, "valuation opening_at"))
        instants = tuple(self.instants)
        if any(not isinstance(item, ValuationInstant) for item in instants):
            raise PerformanceCalculationError("valuation instants must be ValuationInstant values")
        moments = [item.as_of for item in instants]
        if moments != sorted(set(moments)):
            raise PerformanceCalculationError("valuation instants must be strictly increasing in time")
        if moments and moments[0] <= self.opening_at:
            raise PerformanceCalculationError("every valuation instant must follow the opening instant")
        if self.basis is ValuationBasis.COST_BASIS and any(item.marks for item in instants):
            raise PerformanceCalculationError("a COST_BASIS series must not carry mark prices")
        object.__setattr__(self, "instants", instants)

    @classmethod
    def event_driven(cls, events: Iterable[LedgerEvent], *, through: datetime) -> ValuationSeries:
        """The explicit no-market-data grid: one COST_BASIS sample per ledger event.

        Used when the caller has no price series to mark against. ``through`` is
        the run's completion instant and always contributes a closing sample, so
        a run whose only event is its last fill still has that fill valued.
        The opening instant follows
        ``equity.opening_instant:first_activity_minus_1us:1.0.0``.

        Sharpe is undefined on the resulting curve because the grid is not
        daily; that is the honest answer, not a defect.
        """

        closing = _utc(through, "valuation through")
        moments = sorted({_utc(event.as_of, "ledger event as_of") for event in events} | {closing})
        if moments[-1] > closing:
            raise PerformanceCalculationError("a ledger event cannot follow the run's completion instant")
        return cls(
            basis=ValuationBasis.COST_BASIS,
            periodicity=ValuationPeriodicity.EVENT,
            opening_at=moments[0] - OPENING_INSTANT_LEAD,
            instants=tuple(ValuationInstant(moment, ()) for moment in moments),
        )


@dataclass(frozen=True, slots=True, order=True)
class Holding:
    instrument_id: str
    quantity: Decimal
    mark_price: Decimal
    market_value: Decimal

    def __post_init__(self) -> None:
        if not self.instrument_id:
            raise PerformanceCalculationError("holding instrument_id must not be empty")
        _positive(self.quantity, "holding quantity")
        _non_negative(self.mark_price, "holding mark_price")
        _non_negative(self.market_value, "holding market_value")


@dataclass(frozen=True, slots=True)
class EquityPoint:
    as_of: datetime
    cash: Decimal
    holdings: tuple[Holding, ...]
    position_value: Decimal
    equity: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "as_of", _utc(self.as_of, "equity point as_of"))
        _finite(self.cash, "equity point cash")
        holdings = tuple(self.holdings)
        if any(not isinstance(item, Holding) for item in holdings):
            raise PerformanceCalculationError("equity point holdings must be Holding values")
        if len({item.instrument_id for item in holdings}) != len(holdings):
            raise PerformanceCalculationError("equity point holdings must be unique per instrument")
        object.__setattr__(self, "holdings", tuple(sorted(holdings)))
        _finite(self.position_value, "equity point position_value")
        _finite(self.equity, "equity point equity")
        if self.position_value != sum((item.market_value for item in holdings), _ZERO):
            raise PerformanceCalculationError("equity point position_value must equal the holding market values")
        if self.equity != self.cash + self.position_value:
            raise PerformanceCalculationError("equity point equity must equal cash plus position_value")


@dataclass(frozen=True, slots=True)
class EquityCurve:
    basis: ValuationBasis
    periodicity: ValuationPeriodicity
    points: tuple[EquityPoint, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.basis, ValuationBasis):
            raise PerformanceCalculationError("equity curve basis is unsupported")
        if not isinstance(self.periodicity, ValuationPeriodicity):
            raise PerformanceCalculationError("equity curve periodicity is unsupported")
        points = tuple(self.points)
        if not points:
            raise PerformanceCalculationError("an equity curve must have at least the opening point")
        if any(not isinstance(item, EquityPoint) for item in points):
            raise PerformanceCalculationError("equity curve points must be EquityPoint values")
        moments = [item.as_of for item in points]
        if moments != sorted(set(moments)):
            raise PerformanceCalculationError("equity curve points must be strictly increasing in time")
        if self.basis is ValuationBasis.COST_BASIS and self.periodicity is ValuationPeriodicity.DAILY:
            raise PerformanceCalculationError("a COST_BASIS curve is never a DAILY mark grid")
        object.__setattr__(self, "points", points)

    @property
    def opening(self) -> EquityPoint:
        return self.points[0]

    @property
    def closing(self) -> EquityPoint:
        return self.points[-1]

    def to_series(self) -> ValuationSeries:
        """Recover the inputs that produced this curve, for re-derivation in `verify`."""

        return ValuationSeries(
            basis=self.basis,
            periodicity=self.periodicity,
            opening_at=self.opening.as_of,
            instants=tuple(
                ValuationInstant(
                    point.as_of,
                    ()
                    if self.basis is ValuationBasis.COST_BASIS
                    else tuple(MarkPrice(item.instrument_id, item.mark_price) for item in point.holdings),
                )
                for point in self.points[1:]
            ),
        )


def _state_at(events: Sequence[LedgerEvent], as_of: datetime) -> tuple[Decimal, tuple[PositionState, ...]]:
    cash_delta = _ZERO
    positions: tuple[PositionState, ...] = ()
    for event in events:
        if event.as_of > as_of:
            break
        cash_delta += event.cash_delta
        positions = event.positions
    return cash_delta, positions


def _cost_basis_holding(position: PositionState) -> Holding:
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        unit_price = position.cost_basis / position.quantity
    return Holding(
        instrument_id=position.instrument_id,
        quantity=position.quantity,
        mark_price=quantize_money(unit_price, "cost basis unit price"),
        market_value=quantize_money(position.cost_basis, "cost basis market value"),
    )


def _mark_to_market_holding(position: PositionState, marks: dict[str, Decimal], as_of: datetime) -> Holding:
    price = marks.get(position.instrument_id)
    if price is None:
        raise PerformanceCalculationError(
            f"no mark price for instrument {position.instrument_id} at {as_of.isoformat()}; "
            "a MARK_TO_MARKET curve cannot value an unmarked holding"
        )
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        market_value = position.quantity * price
    return Holding(
        instrument_id=position.instrument_id,
        quantity=position.quantity,
        mark_price=quantize_money(price, "mark price"),
        market_value=quantize_money(market_value, "market value"),
    )


def build_equity_curve(
    initial_cash: Decimal,
    events: Iterable[LedgerEvent],
    series: ValuationSeries,
) -> EquityCurve:
    """Sample cash + position value onto the supplied valuation grid.

    The first point is always the opening point: equity equals ``initial_cash``
    and no positions are held, which is what ``totalReturnPct`` measures from.
    """

    if not isinstance(series, ValuationSeries):
        raise PerformanceCalculationError("series must be a ValuationSeries")
    opening_cash = quantize_money(_non_negative(initial_cash, "initial_cash"), "initial_cash")

    ordered = tuple(sorted(events, key=lambda event: event.as_of))
    if any(not isinstance(event, LedgerEvent) for event in ordered):
        raise PerformanceCalculationError("events must contain LedgerEvent values")
    if len({event.as_of for event in ordered}) != len(ordered):
        raise PerformanceCalculationError("ledger events must not share an instant")
    if ordered and ordered[0].as_of < series.opening_at:
        raise PerformanceCalculationError("no ledger event may precede the opening instant")

    points = [
        EquityPoint(
            as_of=series.opening_at,
            cash=opening_cash,
            holdings=(),
            position_value=quantize_money(_ZERO),
            equity=opening_cash,
        )
    ]
    for instant in series.instants:
        cash_delta, positions = _state_at(ordered, instant.as_of)
        cash = quantize_money(opening_cash + cash_delta, "equity point cash")
        marks = {item.instrument_id: item.price for item in instant.marks}
        holdings = tuple(
            _cost_basis_holding(position)
            if series.basis is ValuationBasis.COST_BASIS
            else _mark_to_market_holding(position, marks, instant.as_of)
            for position in positions
        )
        position_value = quantize_money(
            sum((item.market_value for item in holdings), _ZERO),
            "equity point position_value",
        )
        points.append(
            EquityPoint(
                as_of=instant.as_of,
                cash=cash,
                holdings=holdings,
                position_value=position_value,
                equity=quantize_money(cash + position_value, "equity point equity"),
            )
        )
    return EquityCurve(basis=series.basis, periodicity=series.periodicity, points=tuple(points))
