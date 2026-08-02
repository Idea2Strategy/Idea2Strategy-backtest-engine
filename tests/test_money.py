"""Failing-first coverage for the single monetary quantization point (spec 2.3).

Every expected value in this module is a hardcoded literal. Nothing here
recomputes the production formula, so a constant-returning implementation
cannot pass.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from backtest_engine.money import (
    FEE_RATE,
    MONEY_QUANTUM,
    MONEY_SCALE,
    NUMERIC_24_8_MAX,
    NUMERIC_24_8_MIN,
    PRECISION_RULES_VERSION,
    SLIPPAGE_RATE,
    MoneyPrecisionError,
    apply_rate,
    assert_quantized_money,
    format_money,
    is_quantized_money,
    quantize_money,
    quantize_quantity,
)


def test_precision_rules_are_pinned_to_the_published_identifier() -> None:
    assert PRECISION_RULES_VERSION == "precision:1.0.0"
    assert MONEY_SCALE == 8
    assert MONEY_QUANTUM == Decimal("0.00000001")
    assert NUMERIC_24_8_MAX == Decimal("9999999999999999.99999999")
    assert NUMERIC_24_8_MIN == Decimal("-9999999999999999.99999999")


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        # Ties that must round DOWN to the even neighbour.
        ("0.000000005", "0.00000000"),
        ("0.000000025", "0.00000002"),
        ("1.234567885", "1.23456788"),
        # Ties that must round UP to the even neighbour.
        ("0.000000015", "0.00000002"),
        ("0.000000035", "0.00000004"),
        ("1.234567895", "1.23456790"),
        # Non-tie control values.
        ("0.0000000049999", "0.00000000"),
        ("0.0000000050001", "0.00000001"),
    ],
)
def test_money_quantization_uses_bankers_rounding_in_both_directions(
    raw: str, expected_text: str
) -> None:
    assert format_money(quantize_money(Decimal(raw))) == expected_text


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        ("-0.000000005", "0.00000000"),
        ("-0.000000015", "-0.00000002"),
        ("-0.000000025", "-0.00000002"),
        ("-1.234567895", "-1.23456790"),
        ("-1.234567885", "-1.23456788"),
        ("-8997.4990000", "-8997.49900000"),
    ],
)
def test_money_quantization_is_symmetric_for_negative_amounts(
    raw: str, expected_text: str
) -> None:
    assert format_money(quantize_money(Decimal(raw))) == expected_text


def test_money_quantization_never_produces_negative_zero() -> None:
    quantized = quantize_money(Decimal("-0.000000004"))

    assert format_money(quantized) == "0.00000000"
    assert quantized.is_signed() is False


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        ("9999999999999999.99999999", "9999999999999999.99999999"),
        ("-9999999999999999.99999999", "-9999999999999999.99999999"),
        ("9999999999999999.999999994", "9999999999999999.99999999"),
        ("-9999999999999999.999999994", "-9999999999999999.99999999"),
    ],
)
def test_money_quantization_accepts_the_numeric_24_8_boundary(
    raw: str, expected_text: str
) -> None:
    assert format_money(quantize_money(Decimal(raw))) == expected_text


@pytest.mark.parametrize(
    "raw",
    [
        "10000000000000000.00000000",
        "-10000000000000000.00000000",
        "9999999999999999.999999995",
    ],
)
def test_money_quantization_rejects_values_outside_numeric_24_8(raw: str) -> None:
    with pytest.raises(MoneyPrecisionError, match="numeric\\(24,8\\)"):
        quantize_money(Decimal(raw))


@pytest.mark.parametrize("raw", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity"])
def test_money_quantization_rejects_non_finite_decimals(raw: str) -> None:
    with pytest.raises(MoneyPrecisionError, match="finite"):
        quantize_money(Decimal(raw))


def test_money_quantization_rejects_binary_floats() -> None:
    with pytest.raises(MoneyPrecisionError, match="Decimal"):
        quantize_money(0.1)  # type: ignore[arg-type]


def test_money_quantization_accepts_integers_without_losing_scale() -> None:
    assert format_money(quantize_money(100000)) == "100000.00000000"


def test_rates_are_kept_unquantized_and_only_the_product_is_quantized() -> None:
    assert FEE_RATE == Decimal("0.002")
    assert SLIPPAGE_RATE == Decimal("0.0005")
    assert FEE_RATE.as_tuple().exponent == -3
    assert SLIPPAGE_RATE.as_tuple().exponent == -4

    # 1234.56 * 0.002 = 2.46912 exactly; the stored value carries 8 dp.
    assert format_money(apply_rate(Decimal("1234.56"), FEE_RATE)) == "2.46912000"
    # 187.325 * 0.0005 = 0.0936625 exactly.
    assert format_money(apply_rate(Decimal("187.325"), SLIPPAGE_RATE)) == "0.09366250"
    # A product that genuinely needs banker's rounding at the 8th place.
    assert format_money(apply_rate(Decimal("0.0000075"), FEE_RATE)) == "0.00000002"
    assert format_money(apply_rate(Decimal("0.0000125"), FEE_RATE)) == "0.00000002"


def test_apply_rate_rejects_a_non_finite_rate() -> None:
    with pytest.raises(MoneyPrecisionError, match="finite"):
        apply_rate(Decimal("100"), Decimal("NaN"))


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        ("2.5", "2.50000000"),
        ("0.123456785", "0.12345678"),
        ("0.123456795", "0.12345680"),
    ],
)
def test_fractional_quantity_uses_eight_places(raw: str, expected_text: str) -> None:
    quantity = quantize_quantity(Decimal(raw), fractional_eligible=True)

    assert format_money(quantity) == expected_text


@pytest.mark.parametrize(
    ("raw", "expected_text"),
    [
        ("2.5", "2"),
        ("3.5", "4"),
        ("-2.5", "-2"),
        ("-3.5", "-4"),
        ("7", "7"),
    ],
)
def test_whole_share_quantity_rounds_half_even_to_an_integer(
    raw: str, expected_text: str
) -> None:
    quantity = quantize_quantity(Decimal(raw), fractional_eligible=False)

    assert str(quantity) == expected_text
    assert quantity.as_tuple().exponent == 0


def test_quantity_quantization_rejects_non_finite_decimals() -> None:
    with pytest.raises(MoneyPrecisionError, match="finite"):
        quantize_quantity(Decimal("Infinity"), fractional_eligible=True)


def test_is_quantized_money_checks_the_exact_stored_scale() -> None:
    assert is_quantized_money(Decimal("1.00000000")) is True
    assert is_quantized_money(Decimal("1.0")) is False
    assert is_quantized_money(Decimal("1")) is False
    assert is_quantized_money(Decimal("1.000000000")) is False
    assert is_quantized_money(Decimal("NaN")) is False


def test_assert_quantized_money_is_an_invariant_gate() -> None:
    value = Decimal("8997.49900000")

    assert assert_quantized_money(value, "ending_cash") is value

    with pytest.raises(MoneyPrecisionError, match="ending_cash"):
        assert_quantized_money(Decimal("8997.4990000"), "ending_cash")


def test_format_money_round_trips_through_the_numeric_24_8_text_form() -> None:
    assert format_money(quantize_money(Decimal("100000"))) == "100000.00000000"
    assert Decimal(format_money(quantize_money(Decimal("100000")))) == Decimal(
        "100000.00000000"
    )

    with pytest.raises(MoneyPrecisionError, match="quantized"):
        format_money(Decimal("1.5"))
