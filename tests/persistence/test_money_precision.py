"""`numeric(24,8)` must round-trip exactly, and over-precise values must be refused.

PostgreSQL silently rounds a value that does not fit the column scale. Nothing in this
codebase quantised before now, so this is a live risk rather than a theoretical one.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from backtest_engine.persistence import (
    BacktestPersistence,
    MoneyPrecisionError,
    validate_money,
)

from .support import make_run


EXACT_VALUES = [
    Decimal("0.00000000"),
    Decimal("0.00000001"),
    Decimal("100000.00000000"),
    Decimal("12345.67890123"),
    Decimal("9999999999999999.99999999"),
    Decimal("-4321.00000009"),
]


@pytest.mark.parametrize("value", EXACT_VALUES)
def test_validate_money_accepts_representable_values(value: Decimal) -> None:
    assert validate_money(value, "initial_cash_amount") == value


@pytest.mark.parametrize(
    "value",
    [
        Decimal("0.000000001"),
        Decimal("1.123456789"),
        Decimal("-1.123456789"),
    ],
)
def test_validate_money_refuses_to_let_postgres_round(value: Decimal) -> None:
    with pytest.raises(MoneyPrecisionError, match="fractional digits"):
        validate_money(value, "initial_cash_amount")


def test_validate_money_refuses_values_wider_than_the_column() -> None:
    with pytest.raises(MoneyPrecisionError, match="integral digits"):
        validate_money(Decimal("99999999999999999.0"), "initial_cash_amount")


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity")])
def test_validate_money_refuses_non_finite(value: Decimal) -> None:
    with pytest.raises(MoneyPrecisionError, match="finite"):
        validate_money(value, "initial_cash_amount")


def test_validate_money_refuses_float() -> None:
    with pytest.raises(MoneyPrecisionError, match="must be a Decimal"):
        validate_money(100000.0, "initial_cash_amount")  # type: ignore[arg-type]


@pytest.mark.docker
@pytest.mark.parametrize("value", EXACT_VALUES)
def test_numeric_24_8_round_trips_without_precision_loss(persistence: BacktestPersistence, value: Decimal) -> None:
    row = make_run(idempotency_key=f"PRECISION:{value}", initial_cash_amount=value)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(row)

    with persistence.unit_of_work() as uow:
        stored = uow.runs.get(row.id).initial_cash_amount
        # `==` on Decimal ignores trailing zeros, so read the stored digits as text too.
        stored_text = uow.connection.execute(
            text("SELECT initial_cash_amount::text FROM backtest.runs WHERE id = :id"),
            {"id": str(row.id)},
        ).scalar_one()

    assert isinstance(stored, Decimal)
    assert stored == value
    assert stored_text == f"{value:.8f}"
