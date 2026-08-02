r"""Versioned feature definitions shared by the backtest and trading runtimes.

Card D92 requires the Python backtest runtime and the Java trading runtime to
agree. A feature is therefore specified here as an exact arithmetic procedure,
not as "an RSI". Every clause below is normative; an implementation that differs
in any of them is a different feature and needs a different
``definition_version``.

Feature catalog version: ``features:1.0.0``
===========================================

``RSI_14`` - ``definition_version = "rsi:1.0.0"``
-------------------------------------------------

**Method:** ``SIMPLE_AVERAGE_BOUNDED_WINDOW``. This is the simple-average
("Cutler's") RSI, **not** Wilder's exponentially smoothed RSI.

*Why not Wilder's.* Wilder's recursion has unbounded memory: its value at time
``t`` depends on where the series was first seeded. A backtest seeded at the
start of a quarter and a live runtime seeded at process start would then produce
different numbers for the same instant, and no amount of test discipline could
reconcile them. A bounded window makes the value a pure function of the last
``periods + 1`` bars, so the two runtimes agree by construction.

**Periodicity.** The bar duration named by the ``LOAD_FEATURE`` step's
``resolution`` argument. B's published plan uses ``"1m"``. The value is defined
identically at every supported resolution; the resolution selects which series
is read, nothing else.

**Data kind.** ``ADJUSTED_BAR`` - corporate-action-adjusted bars. Raw bars are
not substitutable.

**Window.** Let ``as_of`` be the evaluation instant. Consider the bars of the
instrument's ``(ADJUSTED_BAR, resolution)`` series whose ``ends_at <= as_of``,
in ascending ``starts_at`` order, and take the last ``15``. A bar that has not
finished by ``as_of`` is never read; that is the look-ahead rule.

**Warm-up.** ``periods = 14`` price changes, therefore ``required_bars = 15``.
Fewer than 15 completed bars is *not* a value of 0, 50 or 100: it raises
``ElementInputMissing`` with ``input_reason = "FEATURE_WARMUP_INCOMPLETE"``. A
plan's warm-up requirement in wall-clock terms is
``15 * resolution_period(resolution)`` before the evaluation start; that is what
:func:`backtest_engine.basic_runtime.derive_data_requirements` writes into each
``DataRequirement.warmup_from``.

**Formula.** With ``C[0..14]`` the closes of the 15 window bars, oldest first::

    D[i] = C[i] - C[i-1]                        for i = 1..14
    U    = ( sum over i of max(D[i], 0) ) / 14
    V    = ( sum over i of max(-D[i], 0) ) / 14

    if V == 0 and U == 0:  RSI = 50            # a perfectly flat window
    elif V == 0:           RSI = 100           # only gains
    else:                  RSI = 100 - 100 / (1 + U / V)

The ``V == 0 and U == 0`` case is a pinned convention, not a mathematical
result: ``U/V`` is undefined there. 50 is chosen because a flat window is
neither overbought nor oversold. It must be reproduced verbatim.

**Arithmetic.** Exact decimal arithmetic at 34 significant digits with
``ROUND_HALF_EVEN`` (IEEE 754 ``decimal128``), then a single final quantization
of the result to 8 fractional digits with ``ROUND_HALF_EVEN``. Java equivalent::

    MathContext mc = new MathContext(34, RoundingMode.HALF_EVEN);
    ...
    rsi.setScale(8, RoundingMode.HALF_EVEN);

Binary floating point (``double``) is not permitted: it does not round-trip the
8-decimal contract value.

**Range.** ``0.00000000 <= RSI <= 100.00000000``.

**Worked vectors** (usable directly as Java test vectors):

===================================================  ==============
closes                                               RSI_14
===================================================  ==============
``100, 101, ..., 114`` (14 x +1)                     ``100.00000000``
``114, 113, ..., 100`` (14 x -1)                     ``0.00000000``
``100`` x 15                                         ``50.00000000``
``100,102,101,103,...,108,107`` (7 x +2, 7 x -1)     ``66.66666667``
``100, 101, 99, 99, ...`` (+1, -2, 12 x 0)           ``33.33333333``
===================================================  ==============
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from itertools import pairwise
from types import MappingProxyType

from backtest_engine.elements.core import ElementEvaluationError, SeriesBar


__all__ = [
    "FEATURE_CATALOG_VERSION",
    "FEATURE_REGISTRY",
    "FEATURE_VALUE_QUANTUM",
    "FEATURE_VALUE_SCALE",
    "FEATURE_WORKING_PRECISION",
    "RSI_14_DEFINITION",
    "FeatureDefinition",
    "feature_definition",
]


FEATURE_CATALOG_VERSION = "features:1.0.0"
"""Version of the feature *set*; each feature also carries its own version."""

FEATURE_VALUE_SCALE = 8
FEATURE_VALUE_QUANTUM = Decimal("0.00000001")
FEATURE_WORKING_PRECISION = 34
"""IEEE 754 ``decimal128`` significand width; ``MathContext(34, HALF_EVEN)``."""

_ZERO = Decimal(0)
_HUNDRED = Decimal(100)
_ONE = Decimal(1)
_FLAT_WINDOW_RSI = Decimal(50)


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    """One pinned, independently versioned feature computation."""

    feature_id: str
    definition_version: str
    method: str
    data_kind: str
    periods: int
    value_scale: int
    compute: Callable[[Sequence[SeriesBar]], Decimal]

    @property
    def required_bars(self) -> int:
        """Completed bars needed before the feature has any value at all."""
        return self.periods + 1

    @property
    def slug(self) -> str:
        """The ``rsi`` of ``rsi:1.0.0``."""
        return self.definition_version.split(":", 1)[0]

    @property
    def semantic_version(self) -> str:
        """The ``1.0.0`` of ``rsi:1.0.0``.

        B's ``requiredFeature.featureVersion`` is an exact ``major.minor.patch``
        string with no slug, so this is the half of ``definition_version`` that
        the two contracts can compare directly.
        """
        return self.definition_version.split(":", 1)[1]


def _quantize(value: Decimal) -> Decimal:
    quantized = value.quantize(FEATURE_VALUE_QUANTUM, rounding=ROUND_HALF_EVEN)
    if quantized == 0:
        # -0E-8 and 0E-8 compare equal but render differently, and the rendered
        # form reaches the step trace and the reproducibility hash.
        return quantized.copy_abs()
    return quantized


def compute_rsi_14(window: Sequence[SeriesBar]) -> Decimal:
    """RSI_14 under ``rsi:1.0.0``. See the module docstring for the specification."""
    expected = RSI_14_PERIODS + 1
    if len(window) != expected:
        raise ElementEvaluationError(
            f"RSI_14 requires exactly {expected} bars, got {len(window)}"
        )
    with localcontext() as context:
        context.prec = FEATURE_WORKING_PRECISION
        context.rounding = ROUND_HALF_EVEN
        gain_total = _ZERO
        loss_total = _ZERO
        for previous, current in pairwise(window):
            change = current.close - previous.close
            if change > _ZERO:
                gain_total += change
            elif change < _ZERO:
                loss_total -= change
        periods = Decimal(RSI_14_PERIODS)
        average_gain = gain_total / periods
        average_loss = loss_total / periods
        if average_loss == _ZERO:
            value = _FLAT_WINDOW_RSI if average_gain == _ZERO else _HUNDRED
        else:
            relative_strength = average_gain / average_loss
            value = _HUNDRED - (_HUNDRED / (_ONE + relative_strength))
    return _quantize(value)


RSI_14_PERIODS = 14

RSI_14_DEFINITION = FeatureDefinition(
    feature_id="RSI_14",
    definition_version="rsi:1.0.0",
    method="SIMPLE_AVERAGE_BOUNDED_WINDOW",
    data_kind="ADJUSTED_BAR",
    periods=RSI_14_PERIODS,
    value_scale=FEATURE_VALUE_SCALE,
    compute=compute_rsi_14,
)


FEATURE_REGISTRY: Mapping[str, FeatureDefinition] = MappingProxyType(
    {RSI_14_DEFINITION.feature_id: RSI_14_DEFINITION}
)


def feature_definition(feature_id: str) -> FeatureDefinition | None:
    """The definition this build implements, or ``None`` if it implements none."""
    return FEATURE_REGISTRY.get(feature_id)
