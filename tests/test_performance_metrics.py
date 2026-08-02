"""D25 performance metric unit tests.

Every expected value below is hand-computed and the arithmetic is shown in a
comment. Where a square root is involved the reference value was produced by
exact integer arithmetic (`math.isqrt` on a scaled integer), which is a
different algorithm from the `Decimal.sqrt` used in production, so the test is
not comparing the implementation with itself.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.performance import (
    CALCULATION_RULES_VERSION,
    METRIC_CATALOG_VERSION,
    METRIC_RULES,
    RISK_FREE_ANNUAL_RATE,
    TRADING_DAYS_PER_YEAR,
    EquityCurve,
    EquityPoint,
    Holding,
    LedgerEvent,
    MarkPrice,
    Metric,
    MetricUnit,
    PerformanceCalculationError,
    PositionState,
    TradeStatistics,
    ValuationBasis,
    ValuationInstant,
    ValuationPeriodicity,
    ValuationSeries,
    build_equity_curve,
    build_metrics,
    max_drawdown_pct,
    metrics_document,
    metrics_hash_material,
    periodic_returns,
    sharpe_ratio,
    total_return_pct,
    win_rate_pct,
)


INSTRUMENT_A = "00000000-0000-4000-8000-0000000000a1"
INSTRUMENT_B = "00000000-0000-4000-8000-0000000000a2"


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _point(as_of: str, cash: str, holdings: tuple[Holding, ...] = ()) -> EquityPoint:
    position_value = sum((item.market_value for item in holdings), Decimal("0"))
    return EquityPoint(
        as_of=_utc(as_of),
        cash=Decimal(cash),
        holdings=holdings,
        position_value=position_value,
        equity=Decimal(cash) + position_value,
    )


def _holding(quantity: str, mark_price: str, instrument_id: str = INSTRUMENT_A) -> Holding:
    return Holding(
        instrument_id=instrument_id,
        quantity=Decimal(quantity),
        mark_price=Decimal(mark_price),
        market_value=Decimal(quantity) * Decimal(mark_price),
    )


def _reference_curve() -> EquityCurve:
    """Equity 10000 -> 10200 -> 10098 -> 10299.96 on a four-point daily grid.

    Cash is held flat at 10000 and the whole movement is carried by one holding
    so the equity series is exactly the series the metric arithmetic below uses.
    """

    return EquityCurve(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        points=(
            _point("2025-11-03T14:00:00Z", "10000"),
            _point("2025-11-03T21:00:00Z", "10000", (_holding("2", "100"),)),  # 200
            _point("2025-11-04T21:00:00Z", "10000", (_holding("2", "49"),)),  # 98
            _point("2025-11-05T21:00:00Z", "10000", (_holding("2", "149.98"),)),  # 299.96
        ),
    )


# --------------------------------------------------------------------------
# equity curve construction
# --------------------------------------------------------------------------


def test_mark_to_market_curve_prices_open_positions_at_the_supplied_mark() -> None:
    events = (
        # BUY 10 @ 100, no fee: cash 10000 - 1000 = 9000, holding 10 @ cost 1000.
        LedgerEvent(
            as_of=_utc("2025-11-03T14:30:00Z"),
            cash_delta=Decimal("-1000"),
            positions=(PositionState(INSTRUMENT_A, Decimal("10"), Decimal("1000")),),
        ),
        # SELL 10 @ 105: cash 9000 + 1050 = 10050, flat.
        LedgerEvent(
            as_of=_utc("2025-11-05T15:00:00Z"),
            cash_delta=Decimal("1050"),
            positions=(),
        ),
    )
    series = ValuationSeries(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        opening_at=_utc("2025-11-03T14:00:00Z"),
        instants=(
            ValuationInstant(_utc("2025-11-03T21:00:00Z"), (MarkPrice(INSTRUMENT_A, Decimal("102")),)),
            ValuationInstant(_utc("2025-11-04T21:00:00Z"), (MarkPrice(INSTRUMENT_A, Decimal("98")),)),
            ValuationInstant(_utc("2025-11-05T21:00:00Z"), ()),
        ),
    )

    curve = build_equity_curve(Decimal("10000"), events, series)

    # opening 10000 | 9000 + 10*102 = 10020 | 9000 + 10*98 = 9980 | 10050 + 0
    assert [point.equity for point in curve.points] == [
        Decimal("10000.00000000"),
        Decimal("10020.00000000"),
        Decimal("9980.00000000"),
        Decimal("10050.00000000"),
    ]
    assert [point.cash for point in curve.points] == [
        Decimal("10000.00000000"),
        Decimal("9000.00000000"),
        Decimal("9000.00000000"),
        Decimal("10050.00000000"),
    ]
    assert curve.points[1].holdings == (
        Holding(INSTRUMENT_A, Decimal("10"), Decimal("102.00000000"), Decimal("1020.00000000")),
    )
    assert curve.points[3].holdings == ()


def test_mark_to_market_refuses_to_value_a_holding_that_has_no_mark() -> None:
    events = (
        LedgerEvent(
            as_of=_utc("2025-11-03T14:30:00Z"),
            cash_delta=Decimal("-1000"),
            positions=(PositionState(INSTRUMENT_A, Decimal("10"), Decimal("1000")),),
        ),
    )
    series = ValuationSeries(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        opening_at=_utc("2025-11-03T14:00:00Z"),
        instants=(
            ValuationInstant(_utc("2025-11-03T21:00:00Z"), (MarkPrice(INSTRUMENT_B, Decimal("7")),)),
        ),
    )

    with pytest.raises(PerformanceCalculationError, match="mark price"):
        build_equity_curve(Decimal("10000"), events, series)


def test_cost_basis_event_series_values_holdings_at_recorded_cost() -> None:
    events = (
        LedgerEvent(
            as_of=_utc("2025-11-03T14:30:00Z"),
            cash_delta=Decimal("-1002.001"),
            positions=(PositionState(INSTRUMENT_A, Decimal("10"), Decimal("1000.5")),),
        ),
    )
    series = ValuationSeries.event_driven(events, through=_utc("2025-11-03T21:00:00Z"))

    curve = build_equity_curve(Decimal("10000"), events, series)

    assert curve.basis is ValuationBasis.COST_BASIS
    assert curve.periodicity is ValuationPeriodicity.EVENT
    # The opening instant is the first activity minus exactly one microsecond.
    assert series.opening_at == _utc("2025-11-03T14:29:59.999999Z")
    # 10000 - 1002.001 = 8997.999 cash, plus 1000.5 of cost basis = 9998.499,
    # held flat through the closing sample at 21:00Z.
    assert [point.equity for point in curve.points] == [
        Decimal("10000.00000000"),
        Decimal("9998.49900000"),
        Decimal("9998.49900000"),
    ]
    # The cost-basis mark is the unit cost 1000.5 / 10 = 100.05.
    assert curve.points[1].holdings == (
        Holding(INSTRUMENT_A, Decimal("10"), Decimal("100.05000000"), Decimal("1000.50000000")),
    )


def test_event_driven_series_rejects_an_event_after_the_completion_instant() -> None:
    events = (LedgerEvent(_utc("2025-11-03T22:00:00Z"), Decimal("-1"), ()),)

    with pytest.raises(PerformanceCalculationError, match="completion"):
        ValuationSeries.event_driven(events, through=_utc("2025-11-03T21:00:00Z"))


def test_curve_rejects_unordered_instants_and_an_event_before_the_opening() -> None:
    with pytest.raises(PerformanceCalculationError, match="strictly increasing"):
        ValuationSeries(
            basis=ValuationBasis.MARK_TO_MARKET,
            periodicity=ValuationPeriodicity.DAILY,
            opening_at=_utc("2025-11-03T14:00:00Z"),
            instants=(
                ValuationInstant(_utc("2025-11-04T21:00:00Z"), ()),
                ValuationInstant(_utc("2025-11-03T21:00:00Z"), ()),
            ),
        )
    with pytest.raises(PerformanceCalculationError, match="opening"):
        ValuationSeries(
            basis=ValuationBasis.MARK_TO_MARKET,
            periodicity=ValuationPeriodicity.DAILY,
            opening_at=_utc("2025-11-03T14:00:00Z"),
            instants=(ValuationInstant(_utc("2025-11-03T13:00:00Z"), ()),),
        )
    series = ValuationSeries(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        opening_at=_utc("2025-11-03T14:00:00Z"),
        instants=(ValuationInstant(_utc("2025-11-03T21:00:00Z"), ()),),
    )
    with pytest.raises(PerformanceCalculationError, match="precede the opening"):
        build_equity_curve(
            Decimal("10000"),
            (LedgerEvent(_utc("2025-11-03T13:00:00Z"), Decimal("0"), ()),),
            series,
        )


# --------------------------------------------------------------------------
# metric arithmetic
# --------------------------------------------------------------------------


def test_total_return_pct_is_the_first_to_last_equity_change() -> None:
    # (10299.96 - 10000) / 10000 * 100 = 299.96 / 100 = 2.9996
    assert total_return_pct(_reference_curve()) == Decimal("2.99960000")


def test_max_drawdown_pct_is_the_worst_peak_to_trough_fall() -> None:
    # running peaks: 10000, 10200, 10200, 10299.96
    # drawdowns:      0,      0,   (10098-10200)/10200 = -0.01,  0
    assert max_drawdown_pct(_reference_curve()) == Decimal("-1.00000000")


def test_max_drawdown_is_zero_for_a_monotonically_rising_curve() -> None:
    curve = EquityCurve(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        points=(
            _point("2025-11-03T21:00:00Z", "10000"),
            _point("2025-11-04T21:00:00Z", "10500"),
        ),
    )

    assert max_drawdown_pct(curve) == Decimal("0.00000000")


def test_periodic_returns_are_simple_point_to_point_returns() -> None:
    # 200/10000 = 0.02 | -102/10200 = -0.01 | 201.96/10098 = 0.02
    assert periodic_returns(_reference_curve()) == (
        Decimal("0.02000000"),
        Decimal("-0.01000000"),
        Decimal("0.02000000"),
    )


def test_sharpe_uses_a_zero_risk_free_rate_sample_stdev_and_root_252() -> None:
    # returns  = [0.02, -0.01, 0.02]         (three daily periods)
    # mean     = 0.03 / 3                    = 0.01
    # sample variance (n-1) = (0.01^2 + 0.02^2 + 0.01^2) / 2 = 0.0006 / 2 = 0.0003
    # sharpe   = (mean - 0) / sqrt(0.0003) * sqrt(252) = 0.01 * sqrt(840000)
    # sqrt(840000) = 916.5151389911680013176...  (math.isqrt(840000 * 10**80))
    # => 9.1651513899116800131...  -> 9.16515139 at 8 dp
    assert RISK_FREE_ANNUAL_RATE == Decimal("0")
    assert TRADING_DAYS_PER_YEAR == 252
    assert sharpe_ratio(_reference_curve()) == Decimal("9.16515139")


def test_sharpe_is_undefined_without_two_periodic_returns_or_without_dispersion() -> None:
    single = EquityCurve(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        points=(
            _point("2025-11-03T21:00:00Z", "10000"),
            _point("2025-11-04T21:00:00Z", "10100"),
        ),
    )
    flat = EquityCurve(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        points=(
            _point("2025-11-03T21:00:00Z", "10000"),
            _point("2025-11-04T21:00:00Z", "10100"),
            _point("2025-11-05T21:00:00Z", "10201"),
        ),
    )

    assert sharpe_ratio(single) is None
    # both returns are exactly 0.01, so the sample standard deviation is zero.
    assert periodic_returns(flat) == (Decimal("0.01000000"), Decimal("0.01000000"))
    assert sharpe_ratio(flat) is None


def test_sharpe_is_undefined_on_a_non_daily_grid_because_root_252_would_be_meaningless() -> None:
    event_curve = EquityCurve(
        basis=ValuationBasis.COST_BASIS,
        periodicity=ValuationPeriodicity.EVENT,
        points=_reference_curve().points,
    )

    assert sharpe_ratio(event_curve) is None
    # The path-independent metrics stay defined on an event grid.
    assert total_return_pct(event_curve) == Decimal("2.99960000")


def test_win_rate_counts_profitable_closing_trades_only() -> None:
    # 3 wins out of 8 closing trades = 37.5 %
    assert win_rate_pct(closing_trade_count=8, winning_trade_count=3) == Decimal("37.50000000")
    assert win_rate_pct(closing_trade_count=0, winning_trade_count=0) is None
    with pytest.raises(PerformanceCalculationError, match="winning"):
        win_rate_pct(closing_trade_count=2, winning_trade_count=3)


# --------------------------------------------------------------------------
# metric set, document and hash material
# --------------------------------------------------------------------------


def _trades() -> TradeStatistics:
    return TradeStatistics(
        fill_count=20,
        closing_trade_count=8,
        winning_trade_count=3,
        losing_trade_count=4,
        realized_pnl=Decimal("123.45"),
        total_fees=Decimal("2.001"),
        total_slippage=Decimal("0.5"),
    )


def test_metric_set_declares_a_named_versioned_rule_for_every_metric() -> None:
    metrics = build_metrics(_reference_curve(), _trades())

    assert {metric.key for metric in metrics} == set(METRIC_RULES)
    for metric in metrics:
        assert metric.rule_id == METRIC_RULES[metric.key].rule_id
        assert METRIC_RULES[metric.key].definition.strip() != ""
    # No metric may be declared under a rule id that is not versioned.
    assert all(":" in rule.rule_id for rule in METRIC_RULES.values())


def test_metrics_document_matches_the_canonical_dbml_example_shape() -> None:
    document = metrics_document(build_metrics(_reference_curve(), _trades()))

    assert document["totalReturnPct"] == 2.9996
    assert document["maxDrawdownPct"] == -1.0
    assert document["sharpe"] == 9.16515139
    assert document["winRatePct"] == 37.5
    # Money never becomes a JSON float; it stays numeric(24,8) text.
    assert document["realizedPnl"] == "123.45000000"
    assert document["totalFees"] == "2.00100000"
    assert document["endingEquity"] == "10299.96000000"
    assert document["closingTradeCount"] == 8
    assert document["valuationBasis"] == "MARK_TO_MARKET"
    assert document["metricRules"]["sharpe"] == "metric.sharpe_ratio:1.0.0"


def test_metric_hash_material_is_exact_decimal_text_not_float_text() -> None:
    material = metrics_hash_material(build_metrics(_reference_curve(), _trades()))

    assert ["totalReturnPct", "metric.total_return_pct:1.0.0", "PERCENT", "2.99960000"] in material
    assert ["sharpe", "metric.sharpe_ratio:1.0.0", "RATIO", "9.16515139"] in material
    assert ["realizedPnl", "metric.realized_pnl:1.0.0", "MONEY", "123.45000000"] in material
    assert material == sorted(material)


def test_an_undefined_metric_is_null_in_both_the_document_and_the_hash_material() -> None:
    flat = EquityCurve(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        points=(
            _point("2025-11-03T21:00:00Z", "10000"),
            _point("2025-11-04T21:00:00Z", "10100"),
        ),
    )
    metrics = build_metrics(flat, TradeStatistics.empty())

    document = metrics_document(metrics)
    assert document["sharpe"] is None
    assert document["winRatePct"] is None
    assert ["sharpe", "metric.sharpe_ratio:1.0.0", "RATIO", None] in metrics_hash_material(metrics)


def test_catalog_and_rules_versions_are_pinned_literals() -> None:
    assert METRIC_CATALOG_VERSION == "metrics:1.0.0"
    assert CALCULATION_RULES_VERSION == "metric-rules:1.0.0"


def test_metric_rejects_a_unit_value_mismatch() -> None:
    with pytest.raises(PerformanceCalculationError, match="COUNT"):
        Metric(key="tradeCount", rule_id="metric.trade_count:1.0.0", unit=MetricUnit.COUNT, value=Decimal("1.5"))
