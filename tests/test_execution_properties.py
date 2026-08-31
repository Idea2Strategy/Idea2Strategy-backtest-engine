"""Generated invariants and an independent arithmetic oracle for the execution core."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from backtest_engine.basic_runtime import _ReplayExecutionState
from backtest_engine.elements.orders import OrderCandidate
from backtest_engine.execution_model import (
    D23_MICROSTRUCTURE_POLICY_V1,
    BacktestExecutionModel,
    InstrumentFractionalPolicy,
    LedgerDirection,
    MinuteBar,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    QuantityMode,
    RiskLimits,
    TimeInForce,
)
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE


INSTRUMENT = "00000000-0000-4000-8000-000000000401"
NOW = datetime(2025, 11, 28, 14, 31, tzinfo=UTC)
MONEY = Decimal("0.00000001")


def _money(value: Decimal) -> Decimal:
    """Oracle quantizer: intentionally independent of production money helpers."""
    return value.quantize(MONEY, rounding=ROUND_HALF_EVEN)


def _model(cash: Decimal = Decimal("1000000")) -> BacktestExecutionModel:
    return BacktestExecutionModel(
        D17_EXECUTION_POLICY_FIXTURE,
        cash,
        RiskLimits(cash, cash, cash),
        microstructure=D23_MICROSTRUCTURE_POLICY_V1,
        fractional_policy=InstrumentFractionalPolicy("property-v1", frozenset({INSTRUMENT})),
    )


def _request(order_number: int, side: OrderSide, quantity: int, reference: int) -> OrderRequest:
    return OrderRequest(
        order_id=str(uuid.UUID(int=order_number)),
        instrument_id=INSTRUMENT,
        side=side,
        order_type=OrderType.MARKET,
        quantity_mode=QuantityMode.WHOLE_SHARES,
        time_in_force=TimeInForce.DAY,
        submitted_at=NOW,
        eligible_at=NOW,
        day_expires_at=NOW + timedelta(hours=6),
        reference_price=Decimal(reference),
        quantity=Decimal(quantity),
    )


def _bar(price: int, *, minute: int = 0, volume: int = 1_000_000) -> MinuteBar:
    instant = NOW + timedelta(minutes=minute)
    value = Decimal(price)
    return MinuteBar(
        instrument_id=INSTRUMENT,
        starts_at=instant,
        ends_at=instant + timedelta(minutes=1),
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal(volume),
        complete=True,
    )


@settings(max_examples=100, deadline=None)
@given(
    quantity=st.integers(min_value=1, max_value=100),
    buy_price=st.integers(min_value=1, max_value=500),
    sell_price=st.integers(min_value=1, max_value=500),
)
def test_round_trip_matches_independent_cash_position_and_ledger_oracle(
    quantity: int, buy_price: int, sell_price: int
) -> None:
    initial_cash = Decimal("1000000.00000000")
    model = _model(initial_cash)
    assert model.submit(_request(1, OrderSide.BUY, quantity, buy_price)).status is OrderStatus.ACCEPTED
    buy = model.process_bar(_bar(buy_price))[0]

    expected_buy_price = _money(Decimal(buy_price) * Decimal("1.0005"))
    expected_buy_gross = _money(expected_buy_price * quantity)
    expected_buy_fee = _money(expected_buy_gross * Decimal("0.002"))
    assert (buy.price, buy.gross_amount, buy.fee) == (
        expected_buy_price,
        expected_buy_gross,
        expected_buy_fee,
    )
    assert model.cash == _money(initial_cash - expected_buy_gross - expected_buy_fee)
    assert model.position(INSTRUMENT).quantity == Decimal(quantity)
    assert model.position(INSTRUMENT).cost_basis == expected_buy_gross

    assert model.submit(_request(2, OrderSide.SELL, quantity, sell_price)).status is OrderStatus.ACCEPTED
    sell = model.process_bar(_bar(sell_price, minute=1))[0]
    expected_sell_price = _money(Decimal(sell_price) * Decimal("0.9995"))
    expected_sell_gross = _money(expected_sell_price * quantity)
    expected_sell_fee = _money(expected_sell_gross * Decimal("0.002"))
    assert (sell.price, sell.gross_amount, sell.fee) == (
        expected_sell_price,
        expected_sell_gross,
        expected_sell_fee,
    )
    assert sell.cost_basis == expected_buy_gross
    assert sell.realized_pnl == _money(expected_sell_gross - expected_buy_gross)
    assert model.cash == _money(
        initial_cash - expected_buy_gross - expected_buy_fee + expected_sell_gross - expected_sell_fee
    )
    assert model.position(INSTRUMENT).quantity == 0
    assert model.position(INSTRUMENT).cost_basis == 0

    for transaction in model.ledger_transactions:
        debits = sum(entry.amount for entry in transaction.entries if entry.direction is LedgerDirection.DEBIT)
        credits = sum(entry.amount for entry in transaction.entries if entry.direction is LedgerDirection.CREDIT)
        assert debits == credits


@settings(max_examples=75, deadline=None)
@given(
    cash=st.integers(min_value=100, max_value=100_000),
    excess=st.integers(min_value=1, max_value=10_000),
)
def test_risk_rejection_is_a_pure_state_transition(cash: int, excess: int) -> None:
    model = _model(Decimal(cash))
    before = (model.cash, model.position(INSTRUMENT), model.fills, model.ledger_transactions)
    request = _request(3, OrderSide.BUY, cash + excess, 1)

    rejected = model.submit(request)

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code in {
        "INSUFFICIENT_AVAILABLE_CASH",
        "STRATEGY_BUDGET_EXCEEDED",
        "GROSS_EXPOSURE_EXCEEDED",
        "INSTRUMENT_EXPOSURE_EXCEEDED",
    }
    assert (model.cash, model.position(INSTRUMENT), model.fills, model.ledger_transactions) == before


def _candidate(interval: int) -> OrderCandidate:
    return OrderCandidate(
        evaluation_id="property-gate",
        instrument_id=INSTRUMENT,
        partition_key="partition",
        flow_id="flow",
        side="BUY",
        order_type="MARKET",
        allocation=None,
        reference_price=Decimal("100.00000000"),
        decided_at=NOW,
        eligible_at=NOW,
        session_date_et=NOW.date(),
        session_closes_at=NOW + timedelta(hours=6),
        budget_cap_bps=10_000,
        order_percent=Decimal("100"),
        execution_mode="대기 후 재진입",
        wait_mode="N봉 이후",
        wait_interval=interval,
        max_executions=100,
    )


@settings(max_examples=50, deadline=None)
@given(interval=st.integers(min_value=1, max_value=30), observations=st.integers(min_value=1, max_value=100))
def test_bar_wait_gate_matches_an_independent_counter_model(interval: int, observations: int) -> None:
    state = _ReplayExecutionState()
    candidate = _candidate(interval)
    actual = [state.accepts(candidate) for _ in range(observations)]
    expected = [index % interval == 0 for index in range(observations)]

    assert actual == expected
