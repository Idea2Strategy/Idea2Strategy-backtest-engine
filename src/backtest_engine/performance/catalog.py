"""The versioned D25 metric catalogue.

Two version strings are written to ``backtest.performance_summaries``:

``metric_catalog_version`` (:data:`METRIC_CATALOG_VERSION`)
    *Which* metrics exist. Adding, removing or renaming a key in
    :data:`METRIC_RULES` is a catalogue change.

``calculation_rules_version`` (:data:`CALCULATION_RULES_VERSION`)
    *How* those metrics are computed. Changing any formula, periodicity,
    annualisation factor, rounding scale or the risk-free treatment is a rules
    change, even when the catalogue is untouched.

Every metric carries its own ``rule_id`` as well, so a stored
``metrics_document`` says exactly which formula produced each number without
having to look up a changelog. There are no unnamed constants in a formula:
:data:`RISK_FREE_ANNUAL_RATE`, :data:`TRADING_DAYS_PER_YEAR`,
:data:`PERCENT_SCALE` and :data:`RATIO_SCALE` are declared here and are part of
:data:`CALCULATION_RULES_VERSION`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from types import MappingProxyType


__all__ = [
    "ANNUALIZATION_RULE_ID",
    "CALCULATION_RULES_VERSION",
    "METRIC_CATALOG_VERSION",
    "METRIC_RULES",
    "PERCENT_SCALE",
    "PERIODIC_RETURN_RULE_ID",
    "RATIO_SCALE",
    "RETURN_SCALE",
    "RISK_FREE_ANNUAL_RATE",
    "RISK_FREE_RULE_ID",
    "SAMPLE_STDEV_RULE_ID",
    "TRADING_DAYS_PER_YEAR",
    "WORKING_PRECISION",
    "Metric",
    "MetricRule",
    "MetricUnit",
    "PerformanceCalculationError",
]


METRIC_CATALOG_VERSION = "metrics:1.0.0"
CALCULATION_RULES_VERSION = "metric-rules:1.0.0"

#: Fractional digits kept for PERCENT metrics before they are stored or hashed.
PERCENT_SCALE = 8
#: Fractional digits kept for RATIO metrics (Sharpe) before storage or hashing.
RATIO_SCALE = 8

#: Annual risk-free rate used by the Sharpe rule. Zero, deliberately and
#: visibly: this engine has no funding curve input, so "excess return" is the
#: raw periodic return. When a curve is introduced this constant changes and
#: `CALCULATION_RULES_VERSION` must be bumped with it.
RISK_FREE_ANNUAL_RATE = Decimal("0")
RISK_FREE_RULE_ID = "metric.risk_free_rate:1.0.0"

#: Annualisation base for the Sharpe and volatility rules. 252 is the US equity
#: trading-day convention; it is only valid against a DAILY valuation grid, and
#: `sharpe_ratio` refuses to annualise anything else rather than silently
#: applying it to an event grid.
TRADING_DAYS_PER_YEAR = 252
ANNUALIZATION_RULE_ID = "metric.annualization:trading_days_252:1.0.0"

#: Fractional digits kept for one periodic return. The published return series
#: is the *input* to the Sharpe and volatility rules, so pinning its scale is
#: what makes those ratios exactly re-derivable from the stored curve.
RETURN_SCALE = 8
PERIODIC_RETURN_RULE_ID = "metric.periodic_return:simple:1.0.0"

#: Sample standard deviation with Bessel's correction (divide by n-1), not the
#: population form. Named so the choice cannot be mistaken for an oversight.
SAMPLE_STDEV_RULE_ID = "metric.dispersion:sample_stdev_bessel:1.0.0"

#: Significant digits used for every intermediate. There are exactly two
#: rounding boundaries in the metric pipeline: the periodic returns (at
#: RETURN_SCALE) and the published metric value (at its unit's scale).
WORKING_PRECISION = 50


class PerformanceCalculationError(ValueError):
    """Raised when a metric cannot be computed under the declared rules."""


class MetricUnit(str, Enum):
    """How a metric value is rendered and what values are legal for it."""

    PERCENT = "PERCENT"
    RATIO = "RATIO"
    MONEY = "MONEY"
    COUNT = "COUNT"


@dataclass(frozen=True, slots=True)
class MetricRule:
    """One named, versioned formula."""

    rule_id: str
    unit: MetricUnit
    definition: str

    def __post_init__(self) -> None:
        if ":" not in self.rule_id:
            raise PerformanceCalculationError(
                f"rule_id must be versioned as '<name>:<semver>', got {self.rule_id!r}"
            )
        if not self.definition.strip():
            raise PerformanceCalculationError(f"{self.rule_id} must carry a written definition")


@dataclass(frozen=True, slots=True, order=True)
class Metric:
    """One computed value bound to the rule that produced it.

    ``value is None`` means the rule is *undefined* for this run (for example
    Sharpe with fewer than two periodic returns). It never means zero.
    """

    key: str
    rule_id: str
    unit: MetricUnit
    value: Decimal | None

    def __post_init__(self) -> None:
        if not self.key:
            raise PerformanceCalculationError("metric key must not be empty")
        if not isinstance(self.unit, MetricUnit):
            raise PerformanceCalculationError(f"{self.key} has an unsupported unit")
        if self.value is None:
            return
        if not isinstance(self.value, Decimal) or not self.value.is_finite():
            raise PerformanceCalculationError(f"{self.key} must be a finite Decimal or None")
        if self.unit is MetricUnit.COUNT and self.value != self.value.to_integral_value():
            raise PerformanceCalculationError(f"{self.key} is a COUNT and must be integral, got {self.value}")


def _rule(rule_id: str, unit: MetricUnit, definition: str) -> MetricRule:
    return MetricRule(rule_id=rule_id, unit=unit, definition=definition)


#: The catalogue. Key is the ``metrics_document`` field name (camelCase, as in
#: the canonical `db/schema.dbml` example).
METRIC_RULES: Mapping[str, MetricRule] = MappingProxyType(
    {
        "totalReturnPct": _rule(
            "metric.total_return_pct:1.0.0",
            MetricUnit.PERCENT,
            "(equity[last] - equity[0]) / equity[0] * 100, over the equity curve's "
            "own points. equity[0] is the opening point, whose equity is the run's "
            "initial cash. Undefined when equity[0] is zero. Half-even to 8 dp.",
        ),
        "maxDrawdownPct": _rule(
            "metric.max_drawdown_pct:1.0.0",
            MetricUnit.PERCENT,
            "min over t of (equity[t] - peak[t]) / peak[t] * 100 where peak[t] is the "
            "running maximum of equity over [0, t]. Non-positive by construction; "
            "0 when the curve never falls below a prior peak. Half-even to 8 dp.",
        ),
        "sharpe": _rule(
            "metric.sharpe_ratio:1.0.0",
            MetricUnit.RATIO,
            "mean(r) - rf) / stdev(r) * sqrt(252) where r are the simple "
            "point-to-point returns of a DAILY equity grid, rf is the per-period "
            "risk-free rate derived from RISK_FREE_ANNUAL_RATE = 0 (so excess "
            "return equals raw return), stdev is the sample standard deviation "
            "with Bessel's correction (n-1), and 252 = TRADING_DAYS_PER_YEAR. "
            "Undefined when the grid is not DAILY, when there are fewer than two "
            "returns, or when stdev is zero. Half-even to 8 dp.",
        ),
        "annualizedVolatilityPct": _rule(
            "metric.annualized_volatility_pct:1.0.0",
            MetricUnit.PERCENT,
            "stdev(r) * sqrt(252) * 100 using the same returns, sample standard "
            "deviation and annualisation base as metric.sharpe_ratio. Published so "
            "the Sharpe numerator and denominator are both auditable. Undefined "
            "under exactly the same conditions as Sharpe except zero dispersion, "
            "which yields 0. Half-even to 8 dp.",
        ),
        "winRatePct": _rule(
            "metric.win_rate_pct:1.0.0",
            MetricUnit.PERCENT,
            "winning_trade_count / closing_trade_count * 100. A closing trade is a "
            "fill that reduces an open position and therefore realises P&L; a "
            "winning trade is a closing trade with realized_pnl > 0. A closing "
            "trade with realized_pnl == 0 counts in the denominator only. Opening "
            "fills are excluded entirely. Undefined when there are no closing "
            "trades. Half-even to 8 dp.",
        ),
        "startingEquity": _rule(
            "metric.starting_equity:1.0.0",
            MetricUnit.MONEY,
            "equity[0] of the equity curve, which is the run's initial cash.",
        ),
        "endingEquity": _rule(
            "metric.ending_equity:1.0.0",
            MetricUnit.MONEY,
            "equity[last] of the equity curve: derived ending cash plus the value "
            "of every open position under the curve's valuation basis.",
        ),
        "endingCash": _rule(
            "metric.ending_cash:1.0.0",
            MetricUnit.MONEY,
            "initial cash plus the sum of every fill's signed cash flow. Derived "
            "from the ledger, never read off the last detail record's cash_after.",
        ),
        "realizedPnl": _rule(
            "metric.realized_pnl:1.0.0",
            MetricUnit.MONEY,
            "Sum of realized_pnl over every fill record.",
        ),
        "totalFees": _rule(
            "metric.total_fees:1.0.0",
            MetricUnit.MONEY,
            "Sum of fee over every fill record.",
        ),
        "totalSlippage": _rule(
            "metric.total_slippage:1.0.0",
            MetricUnit.MONEY,
            "Sum of slippage_amount over every fill record.",
        ),
        "fillCount": _rule(
            "metric.fill_count:1.0.0",
            MetricUnit.COUNT,
            "Number of FILL detail records, opening and closing alike.",
        ),
        "closingTradeCount": _rule(
            "metric.closing_trade_count:1.0.0",
            MetricUnit.COUNT,
            "Number of fills that reduce an open position, i.e. the win-rate denominator.",
        ),
        "winningTradeCount": _rule(
            "metric.winning_trade_count:1.0.0",
            MetricUnit.COUNT,
            "Number of closing fills with realized_pnl > 0.",
        ),
        "losingTradeCount": _rule(
            "metric.losing_trade_count:1.0.0",
            MetricUnit.COUNT,
            "Number of closing fills with realized_pnl < 0. Break-even closing "
            "fills are in neither the winning nor the losing count.",
        ),
        "valuationPointCount": _rule(
            "metric.valuation_point_count:1.0.0",
            MetricUnit.COUNT,
            "Number of points on the equity curve including the opening point. "
            "The Sharpe sample size is this value minus one.",
        ),
    }
)
