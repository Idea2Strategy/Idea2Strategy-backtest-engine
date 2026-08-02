"""The single monetary quantization point for the backtest engine (spec 2.3).

Canonical storage for every monetary column is ``numeric(24,8)``
(``db/schema.dbml`` ``backtest.runs.initial_cash_amount`` and siblings) and
``backtest.runs.precision_rules_version`` pins the rule set that produced those
values. This module is that rule set.

Rules
-----
* Rule identifier: ``precision:1.0.0``.
* Monetary amounts (cash, fees, slippage amounts, realized P&L) quantize to
  8 fractional digits with ``ROUND_HALF_EVEN``.
* Quantities quantize to 8 fractional digits for fractional-eligible
  instruments and to an integer otherwise, also with ``ROUND_HALF_EVEN``.
* Rates (fee ``0.002``, slippage ``0.0005``) are **never** quantized. Only the
  product of a rate and a base amount is quantized, exactly once, by
  :func:`apply_rate`.
* Reproducibility hashes are computed over quantized values, so
  :func:`format_money` is the canonical text form.

Every caller that stores or hashes money must pass through this module; no
other module may call ``Decimal.quantize`` on a monetary value.
"""

from __future__ import annotations

from decimal import (
    ROUND_HALF_EVEN,
    Decimal,
    DecimalException,
    localcontext,
)


__all__ = [
    "FEE_RATE",
    "MONEY_QUANTUM",
    "MONEY_ROUNDING",
    "MONEY_SCALE",
    "NUMERIC_24_8_MAX",
    "NUMERIC_24_8_MIN",
    "PRECISION_RULES_VERSION",
    "QUANTITY_QUANTUM",
    "SLIPPAGE_RATE",
    "MoneyPrecisionError",
    "apply_rate",
    "assert_quantized_money",
    "format_money",
    "is_quantized_money",
    "quantize_money",
    "quantize_quantity",
]


PRECISION_RULES_VERSION = "precision:1.0.0"
"""Value written to ``backtest.runs.precision_rules_version``."""

MONEY_SCALE = 8
MONEY_QUANTUM = Decimal("0.00000001")
QUANTITY_QUANTUM = MONEY_QUANTUM
MONEY_ROUNDING = ROUND_HALF_EVEN

# numeric(24,8): 24 significant digits of which 8 are fractional.
NUMERIC_24_8_MAX = Decimal("9999999999999999.99999999")
NUMERIC_24_8_MIN = -NUMERIC_24_8_MAX

# Rates are policy inputs, not stored money. They are deliberately not
# quantized; see apply_rate.
FEE_RATE = Decimal("0.002")
SLIPPAGE_RATE = Decimal("0.0005")

# Wide enough that quantization and rate multiplication are exact for any
# value that can survive the numeric(24,8) range check below, and independent
# of whatever decimal context the caller happens to be running in.
_WORKING_PRECISION = 60


class MoneyPrecisionError(ValueError):
    """Raised when a value cannot be represented under ``precision:1.0.0``."""


def _coerce(value: Decimal | int, label: str) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        raise MoneyPrecisionError(
            f"{label} must be a Decimal or int, not {type(value).__name__}"
        )
    decimal_value = value if isinstance(value, Decimal) else Decimal(value)
    if not decimal_value.is_finite():
        raise MoneyPrecisionError(f"{label} must be a finite Decimal, got {value!r}")
    return decimal_value


def _quantize(value: Decimal, quantum: Decimal, label: str) -> Decimal:
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        context.rounding = MONEY_ROUNDING
        try:
            quantized = value.quantize(quantum, rounding=MONEY_ROUNDING)
        except DecimalException as exc:  # pragma: no cover - guarded by range check
            raise MoneyPrecisionError(
                f"{label} does not fit numeric(24,8): {value!r}"
            ) from exc
    if quantized < NUMERIC_24_8_MIN or quantized > NUMERIC_24_8_MAX:
        raise MoneyPrecisionError(f"{label} does not fit numeric(24,8): {value!r}")
    if quantized == 0:
        # -0E-8 and 0E-8 are numerically equal but not textually equal, and the
        # text form feeds reproducibility hashes.
        return quantized.copy_abs()
    return quantized


def quantize_money(value: Decimal | int, label: str = "amount") -> Decimal:
    """Quantize a monetary amount to the canonical ``numeric(24,8)`` scale."""
    return _quantize(_coerce(value, label), MONEY_QUANTUM, label)


def quantize_quantity(
    value: Decimal | int,
    *,
    fractional_eligible: bool,
    label: str = "quantity",
) -> Decimal:
    """Quantize an order or position quantity.

    ``fractional_eligible`` instruments keep 8 fractional digits; every other
    instrument is whole-share only and quantizes to an integer.
    """
    quantum = QUANTITY_QUANTUM if fractional_eligible else Decimal(1)
    return _quantize(_coerce(value, label), quantum, label)


def apply_rate(
    base: Decimal | int,
    rate: Decimal,
    label: str = "amount",
) -> Decimal:
    """Multiply an amount by an unquantized rate and quantize the result only.

    The rate keeps its declared precision (``0.002``, ``0.0005``); quantization
    happens exactly once, on the product.
    """
    base_value = _coerce(base, label)
    rate_value = _coerce(rate, f"{label} rate")
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        context.rounding = MONEY_ROUNDING
        product = base_value * rate_value
    return _quantize(product, MONEY_QUANTUM, label)


def is_quantized_money(value: object) -> bool:
    """True only for a finite Decimal already stored at exactly 8 dp."""
    if not isinstance(value, Decimal) or not value.is_finite():
        return False
    return value.as_tuple().exponent == -MONEY_SCALE


def assert_quantized_money(value: Decimal, label: str) -> Decimal:
    """Invariant gate: return ``value`` or raise if it never passed this module."""
    if not is_quantized_money(value):
        raise MoneyPrecisionError(
            f"{label} is not quantized to {MONEY_SCALE} places under "
            f"{PRECISION_RULES_VERSION}: {value!r}"
        )
    return value


def format_money(value: Decimal) -> str:
    """Canonical ``numeric(24,8)`` text form used for storage and hashing."""
    assert_quantized_money(value, "value")
    return f"{value:f}"
