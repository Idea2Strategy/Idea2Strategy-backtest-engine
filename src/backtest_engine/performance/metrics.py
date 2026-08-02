"""The D25 metric calculators.

Each public function implements exactly one rule from :data:`METRIC_RULES` and
its docstring restates that rule's definition. Nothing here reads a constant
that is not declared in :mod:`backtest_engine.performance.catalog`.

Rounding happens at two, and only two, boundaries:

1. :func:`periodic_returns` quantises each simple return to
   :data:`RETURN_SCALE`. The published return series is the input to Sharpe and
   volatility, so both are exactly re-derivable from the stored equity curve.
2. Each published metric is quantised to its unit's scale
   (:data:`PERCENT_SCALE`, :data:`RATIO_SCALE`, or ``numeric(24,8)`` for money
   via :mod:`backtest_engine.money`).

Everything in between runs at :data:`WORKING_PRECISION` significant digits with
``ROUND_HALF_EVEN``.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any

from backtest_engine.money import format_money, quantize_money

from .catalog import (
    CALCULATION_RULES_VERSION,
    METRIC_CATALOG_VERSION,
    METRIC_RULES,
    PERCENT_SCALE,
    RATIO_SCALE,
    RETURN_SCALE,
    RISK_FREE_ANNUAL_RATE,
    TRADING_DAYS_PER_YEAR,
    WORKING_PRECISION,
    Metric,
    MetricUnit,
    PerformanceCalculationError,
)
from .equity_curve import EquityCurve, ValuationBasis, ValuationPeriodicity


__all__ = [
    "MetricSet",
    "TradeStatistics",
    "annualized_volatility_pct",
    "build_metrics",
    "max_drawdown_pct",
    "metrics_document",
    "metrics_hash_material",
    "periodic_returns",
    "sharpe_ratio",
    "total_return_pct",
    "win_rate_pct",
]


_ZERO = Decimal("0")
_HUNDRED = Decimal("100")
_PERCENT_QUANTUM = Decimal(1).scaleb(-PERCENT_SCALE)
_RATIO_QUANTUM = Decimal(1).scaleb(-RATIO_SCALE)
_RETURN_QUANTUM = Decimal(1).scaleb(-RETURN_SCALE)


def _quantize(value: Decimal, quantum: Decimal) -> Decimal:
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return value.quantize(quantum, rounding=ROUND_HALF_EVEN)


def _count(value: int, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise PerformanceCalculationError(f"{label} must be a non-negative integer")
    return value


@dataclass(frozen=True, slots=True)
class TradeStatistics:
    """Fill-level aggregates the equity curve alone cannot express."""

    fill_count: int
    closing_trade_count: int
    winning_trade_count: int
    losing_trade_count: int
    realized_pnl: Decimal
    total_fees: Decimal
    total_slippage: Decimal

    def __post_init__(self) -> None:
        for name in ("fill_count", "closing_trade_count", "winning_trade_count", "losing_trade_count"):
            _count(getattr(self, name), name)
        if self.winning_trade_count + self.losing_trade_count > self.closing_trade_count:
            raise PerformanceCalculationError(
                "winning_trade_count + losing_trade_count cannot exceed closing_trade_count"
            )
        if self.closing_trade_count > self.fill_count:
            raise PerformanceCalculationError("closing_trade_count cannot exceed fill_count")
        for name in ("realized_pnl", "total_fees", "total_slippage"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise PerformanceCalculationError(f"{name} must be a finite Decimal")
        if self.total_fees < _ZERO or self.total_slippage < _ZERO:
            raise PerformanceCalculationError("total_fees and total_slippage must be non-negative")

    @classmethod
    def empty(cls) -> TradeStatistics:
        """A run that produced no fills. Not a default: an explicit zero state."""

        return cls(
            fill_count=0,
            closing_trade_count=0,
            winning_trade_count=0,
            losing_trade_count=0,
            realized_pnl=_ZERO,
            total_fees=_ZERO,
            total_slippage=_ZERO,
        )


def total_return_pct(curve: EquityCurve) -> Decimal | None:
    """``metric.total_return_pct:1.0.0`` - (last - first) / first * 100."""

    opening = curve.opening.equity
    if opening == _ZERO:
        return None
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = (curve.closing.equity - opening) / opening * _HUNDRED
    return _quantize(value, _PERCENT_QUANTUM)


def max_drawdown_pct(curve: EquityCurve) -> Decimal | None:
    """``metric.max_drawdown_pct:1.0.0`` - worst (equity - running peak) / peak * 100."""

    peak = curve.opening.equity
    if peak <= _ZERO:
        return None
    worst = _ZERO
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        for point in curve.points:
            if point.equity > peak:
                peak = point.equity
            if peak <= _ZERO:
                return None
            drawdown = (point.equity - peak) / peak * _HUNDRED
            worst = min(worst, drawdown)
    return _quantize(worst, _PERCENT_QUANTUM)


def periodic_returns(curve: EquityCurve) -> tuple[Decimal, ...]:
    """``metric.periodic_return:simple:1.0.0`` - (E[k] - E[k-1]) / E[k-1] per step.

    A step whose opening equity is zero has no defined simple return; the whole
    series is rejected rather than silently dropping the step, because dropping
    it would change the Sharpe sample size without saying so.
    """

    returns: list[Decimal] = []
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        for previous, current in zip(curve.points, curve.points[1:], strict=False):
            if previous.equity == _ZERO:
                raise PerformanceCalculationError(
                    f"equity is zero at {previous.as_of.isoformat()}; no simple return is defined"
                )
            returns.append((current.equity - previous.equity) / previous.equity)
    return tuple(_quantize(value, _RETURN_QUANTUM) for value in returns)


def _mean_and_sample_stdev(values: Sequence[Decimal]) -> tuple[Decimal, Decimal]:
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        size = Decimal(len(values))
        mean = sum(values, _ZERO) / size
        squares = sum(((value - mean) ** 2 for value in values), _ZERO)
        variance = squares / (size - Decimal(1))
        return mean, variance.sqrt()


def _period_risk_free_rate() -> Decimal:
    """Simple (non-compounded) de-annualisation of :data:`RISK_FREE_ANNUAL_RATE`.

    The rate is zero, so the simple and compounded conventions coincide; the
    simple one is the declared rule.
    """

    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return RISK_FREE_ANNUAL_RATE / Decimal(TRADING_DAYS_PER_YEAR)


def _annualization_factor() -> Decimal:
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        return Decimal(TRADING_DAYS_PER_YEAR).sqrt()


def sharpe_ratio(curve: EquityCurve) -> Decimal | None:
    """``metric.sharpe_ratio:1.0.0``.

    ``(mean(r) - rf) / stdev(r) * sqrt(252)`` where ``r`` is the DAILY simple
    return series, ``rf`` is the per-period risk-free rate derived from
    ``RISK_FREE_ANNUAL_RATE = 0``, ``stdev`` is the sample (n-1) standard
    deviation and 252 is ``TRADING_DAYS_PER_YEAR``.

    Returns ``None`` - not zero - when the grid is not DAILY, when fewer than
    two returns exist, or when the sample standard deviation is zero.
    """

    if curve.periodicity is not ValuationPeriodicity.DAILY:
        return None
    returns = periodic_returns(curve)
    if len(returns) < 2:
        return None
    mean, stdev = _mean_and_sample_stdev(returns)
    if stdev == _ZERO:
        return None
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = (mean - _period_risk_free_rate()) / stdev * _annualization_factor()
    return _quantize(value, _RATIO_QUANTUM)


def annualized_volatility_pct(curve: EquityCurve) -> Decimal | None:
    """``metric.annualized_volatility_pct:1.0.0`` - stdev(r) * sqrt(252) * 100."""

    if curve.periodicity is not ValuationPeriodicity.DAILY:
        return None
    returns = periodic_returns(curve)
    if len(returns) < 2:
        return None
    _, stdev = _mean_and_sample_stdev(returns)
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = stdev * _annualization_factor() * _HUNDRED
    return _quantize(value, _PERCENT_QUANTUM)


def win_rate_pct(*, closing_trade_count: int, winning_trade_count: int) -> Decimal | None:
    """``metric.win_rate_pct:1.0.0`` - winning / closing * 100, or None with no closing trades."""

    _count(closing_trade_count, "closing_trade_count")
    _count(winning_trade_count, "winning_trade_count")
    if winning_trade_count > closing_trade_count:
        raise PerformanceCalculationError("winning_trade_count cannot exceed closing_trade_count")
    if closing_trade_count == 0:
        return None
    with localcontext() as context:
        context.prec = WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        value = Decimal(winning_trade_count) / Decimal(closing_trade_count) * _HUNDRED
    return _quantize(value, _PERCENT_QUANTUM)


@dataclass(frozen=True, slots=True)
class MetricSet:
    """Every catalogue metric for one run, plus the basis they are qualified by."""

    basis: ValuationBasis
    periodicity: ValuationPeriodicity
    metrics: tuple[Metric, ...]

    def __post_init__(self) -> None:
        keys = [metric.key for metric in self.metrics]
        if sorted(keys) != sorted(METRIC_RULES):
            missing = sorted(set(METRIC_RULES) - set(keys))
            extra = sorted(set(keys) - set(METRIC_RULES))
            raise PerformanceCalculationError(
                f"metric set does not match {METRIC_CATALOG_VERSION}; missing={missing} unexpected={extra}"
            )
        object.__setattr__(self, "metrics", tuple(sorted(self.metrics, key=lambda metric: metric.key)))

    def __iter__(self) -> Iterator[Metric]:
        return iter(self.metrics)

    def __len__(self) -> int:
        return len(self.metrics)

    def __getitem__(self, key: str) -> Metric:
        for metric in self.metrics:
            if metric.key == key:
                return metric
        raise KeyError(key)


def _metric(key: str, value: Decimal | None) -> Metric:
    rule = METRIC_RULES[key]
    return Metric(key=key, rule_id=rule.rule_id, unit=rule.unit, value=value)


def build_metrics(curve: EquityCurve, trades: TradeStatistics) -> MetricSet:
    """Compute every catalogue metric for one equity curve and fill population."""

    if not isinstance(curve, EquityCurve):
        raise PerformanceCalculationError("curve must be an EquityCurve")
    if not isinstance(trades, TradeStatistics):
        raise PerformanceCalculationError("trades must be TradeStatistics")
    return MetricSet(
        basis=curve.basis,
        periodicity=curve.periodicity,
        metrics=(
            _metric("totalReturnPct", total_return_pct(curve)),
            _metric("maxDrawdownPct", max_drawdown_pct(curve)),
            _metric("sharpe", sharpe_ratio(curve)),
            _metric("annualizedVolatilityPct", annualized_volatility_pct(curve)),
            _metric(
                "winRatePct",
                win_rate_pct(
                    closing_trade_count=trades.closing_trade_count,
                    winning_trade_count=trades.winning_trade_count,
                ),
            ),
            _metric("startingEquity", quantize_money(curve.opening.equity, "startingEquity")),
            _metric("endingEquity", quantize_money(curve.closing.equity, "endingEquity")),
            _metric("endingCash", quantize_money(curve.closing.cash, "endingCash")),
            _metric("realizedPnl", quantize_money(trades.realized_pnl, "realizedPnl")),
            _metric("totalFees", quantize_money(trades.total_fees, "totalFees")),
            _metric("totalSlippage", quantize_money(trades.total_slippage, "totalSlippage")),
            _metric("fillCount", Decimal(trades.fill_count)),
            _metric("closingTradeCount", Decimal(trades.closing_trade_count)),
            _metric("winningTradeCount", Decimal(trades.winning_trade_count)),
            _metric("losingTradeCount", Decimal(trades.losing_trade_count)),
            _metric("valuationPointCount", Decimal(len(curve.points))),
        ),
    )


def _document_value(metric: Metric) -> Any:
    if metric.value is None:
        return None
    if metric.unit is MetricUnit.MONEY:
        # jsonb numbers are IEEE doubles in most drivers; money stays text.
        return format_money(metric.value)
    if metric.unit is MetricUnit.COUNT:
        return int(metric.value)
    # PERCENT and RATIO are already quantised to 8 dp, so the shortest float
    # repr round-trips them exactly and matches the canonical DBML example
    # ({"totalReturnPct":12.64,...}).
    return float(metric.value)


def _exact_text(metric: Metric) -> str | None:
    if metric.value is None:
        return None
    if metric.unit is MetricUnit.COUNT:
        return str(int(metric.value))
    return format(metric.value, "f")


def metrics_document(metric_set: MetricSet) -> dict[str, Any]:
    """The ``backtest.performance_summaries.metrics_document`` jsonb payload."""

    if not isinstance(metric_set, MetricSet):
        raise PerformanceCalculationError("metric_set must be a MetricSet")
    document: dict[str, Any] = {metric.key: _document_value(metric) for metric in metric_set}
    document["metricCatalogVersion"] = METRIC_CATALOG_VERSION
    document["calculationRulesVersion"] = CALCULATION_RULES_VERSION
    document["valuationBasis"] = metric_set.basis.value
    document["valuationBasisRuleId"] = metric_set.basis.rule_id
    document["valuationPeriodicity"] = metric_set.periodicity.value
    document["metricRules"] = {metric.key: metric.rule_id for metric in metric_set}
    return document


def metrics_hash_material(metric_set: MetricSet) -> list[list[str | None]]:
    """Exact, float-free material for ``result_hash``.

    ``metrics_document`` renders percentages as JSON numbers to match the
    canonical example. That rendering never feeds a hash: this does, using the
    exact decimal text of the same values.
    """

    if not isinstance(metric_set, MetricSet):
        raise PerformanceCalculationError("metric_set must be a MetricSet")
    return sorted(
        [metric.key, metric.rule_id, metric.unit.value, _exact_text(metric)] for metric in metric_set
    )


def metric_rule_index() -> Mapping[str, str]:
    """Catalogue key -> rule id, for callers that want the index without a run."""

    return {key: rule.rule_id for key, rule in METRIC_RULES.items()}
