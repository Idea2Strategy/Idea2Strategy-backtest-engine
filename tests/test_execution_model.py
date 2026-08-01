from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from backtest_engine.execution_model import (
    BacktestExecutionModel,
    ExecutionModelValidationError,
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
OTHER_INSTRUMENT = "00000000-0000-4000-8000-000000000402"


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _model(
    cash: str = "10000",
    strategy_budget: str = "10000",
    gross_limit: str = "10000",
    instrument_limit: str = "10000",
) -> BacktestExecutionModel:
    return BacktestExecutionModel(
        policy=D17_EXECUTION_POLICY_FIXTURE,
        initial_cash=Decimal(cash),
        risk_limits=RiskLimits(
            max_strategy_notional=Decimal(strategy_budget),
            max_gross_exposure=Decimal(gross_limit),
            max_instrument_exposure=Decimal(instrument_limit),
        ),
    )


def _request(
    order_id: str = "00000000-0000-4000-8000-000000000501",
    *,
    instrument_id: str = INSTRUMENT,
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.MARKET,
    quantity: str = "10",
    quantity_mode: QuantityMode = QuantityMode.WHOLE_SHARES,
    time_in_force: TimeInForce = TimeInForce.DAY,
    submitted_at: str = "2025-11-28T14:31:00Z",
    eligible_at: str = "2025-11-28T14:31:00Z",
    day_expires_at: str = "2025-11-28T18:00:00Z",
    expires_at: str | None = None,
    reference_price: str = "100",
    limit_price: str | None = None,
    stop_price: str | None = None,
    trail_percent: str | None = None,
) -> OrderRequest:
    return OrderRequest(
        order_id=order_id,
        instrument_id=instrument_id,
        side=side,
        order_type=order_type,
        quantity=Decimal(quantity),
        quantity_mode=quantity_mode,
        time_in_force=time_in_force,
        submitted_at=_utc(submitted_at),
        eligible_at=_utc(eligible_at),
        day_expires_at=_utc(day_expires_at),
        expires_at=_utc(expires_at) if expires_at else None,
        reference_price=Decimal(reference_price),
        limit_price=Decimal(limit_price) if limit_price else None,
        stop_price=Decimal(stop_price) if stop_price else None,
        trail_percent=Decimal(trail_percent) if trail_percent else None,
    )


def _bar(
    starts_at: str = "2025-11-28T14:31:00Z",
    *,
    instrument_id: str = INSTRUMENT,
    open_price: str = "100",
    high: str = "101",
    low: str = "99",
    close: str = "100",
    volume: str = "1000",
    complete: bool = True,
) -> MinuteBar:
    start = _utc(starts_at)
    return MinuteBar(
        instrument_id=instrument_id,
        starts_at=start,
        ends_at=start + timedelta(minutes=1),
        open=Decimal(open_price),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
        complete=complete,
    )


def _assert_balanced(model: BacktestExecutionModel) -> None:
    for transaction in model.ledger_transactions:
        assert {entry.currency for entry in transaction.entries} == {"USD"}
        debits = sum(
            entry.amount
            for entry in transaction.entries
            if entry.direction is LedgerDirection.DEBIT
        )
        credits = sum(
            entry.amount
            for entry in transaction.entries
            if entry.direction is LedgerDirection.CREDIT
        )
        assert debits == credits


def test_market_order_waits_for_next_eligible_bar_and_posts_costs_to_ledger() -> None:
    model = _model()
    model.submit(_request())

    assert model.process_bar(_bar("2025-11-28T14:30:00Z")) == ()
    fills = model.process_bar(_bar())

    assert len(fills) == 1
    fill = fills[0]
    assert fill.base_price == Decimal("100")
    assert fill.price == Decimal("100.0500")
    assert fill.gross_amount == Decimal("1000.5000")
    assert fill.slippage_amount == Decimal("0.5000")
    assert fill.fee == Decimal("2.0010000")
    assert model.cash == Decimal("8997.4990000")
    assert model.position(INSTRUMENT).quantity == Decimal("10")
    assert model.order(fill.order_id).status is OrderStatus.FILLED
    _assert_balanced(model)


def test_zero_volume_and_incomplete_bars_never_create_optimistic_fills() -> None:
    model = _model()
    model.submit(_request())

    assert model.process_bar(_bar(volume="0")) == ()
    assert model.process_bar(_bar("2025-11-28T14:32:00Z", complete=False)) == ()
    assert model.order("00000000-0000-4000-8000-000000000501").status is OrderStatus.ACCEPTED


def test_positive_volume_does_not_cap_an_otherwise_valid_fill() -> None:
    model = _model(cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000")
    model.submit(_request(quantity="250"))

    fills = model.process_bar(_bar(volume="1"))

    assert fills[0].quantity == Decimal("250")
    assert model.order(fills[0].order_id).status is OrderStatus.FILLED


def test_limit_price_improvement_is_allowed_but_slippage_cannot_cross_the_limit() -> None:
    improved = _model()
    improved.submit(_request(order_type=OrderType.LIMIT, limit_price="100"))
    fill = improved.process_bar(
        _bar(open_price="99", high="100", low="98", close="99")
    )[0]
    assert fill.base_price == Decimal("99")
    assert fill.price == Decimal("99.0495")

    capped = _model()
    capped.submit(_request(order_type=OrderType.LIMIT, limit_price="100"))
    assert capped.process_bar(_bar(open_price="100", high="101", low="99")) == ()


@pytest.mark.parametrize(
    ("order_type", "kwargs", "bar", "expected_base"),
    [
        (
            OrderType.STOP,
            {"stop_price": "101"},
            _bar(open_price="100", high="102", low="99", close="101"),
            Decimal("101"),
        ),
        (
            OrderType.STOP_LIMIT,
            {"stop_price": "101", "limit_price": "102"},
            _bar(open_price="100", high="102", low="99", close="101"),
            Decimal("101"),
        ),
    ],
)
def test_stop_orders_trigger_deterministically_from_ohlc(
    order_type: OrderType,
    kwargs: dict[str, str],
    bar: MinuteBar,
    expected_base: Decimal,
) -> None:
    model = _model()
    model.submit(_request(order_type=order_type, **kwargs))

    fill = model.process_bar(bar)[0]

    assert fill.base_price == expected_base
    assert model.order(fill.order_id).status is OrderStatus.FILLED


def test_trailing_sell_updates_its_reference_then_triggers_on_a_later_bar() -> None:
    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("10"), Decimal("80"))
    model.submit(
        _request(
            side=OrderSide.SELL,
            order_type=OrderType.TRAILING_STOP,
            trail_percent="0.10",
            reference_price="100",
        )
    )

    assert model.process_bar(
        _bar(open_price="100", high="110", low="95", close="108")
    ) == ()
    fill = model.process_bar(
        _bar("2025-11-28T14:32:00Z", open_price="98", high="100", low="97", close="99")
    )[0]

    assert fill.base_price == Decimal("98")
    assert fill.price == Decimal("97.9510")


def test_day_gtd_and_gtc_orders_expire_at_their_pinned_boundaries() -> None:
    model = _model(cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000")
    day = model.submit(_request(order_type=OrderType.LIMIT, limit_price="50"))
    gtd = model.submit(
        _request(
            order_id="00000000-0000-4000-8000-000000000502",
            order_type=OrderType.LIMIT,
            limit_price="50",
            time_in_force=TimeInForce.GTD,
            expires_at="2025-12-01T21:00:00Z",
        )
    )
    gtc = model.submit(
        _request(
            order_id="00000000-0000-4000-8000-000000000503",
            order_type=OrderType.LIMIT,
            limit_price="50",
            time_in_force=TimeInForce.GTC,
        )
    )

    model.advance_time(day.expires_at)
    assert model.order(day.order_id).status is OrderStatus.EXPIRED
    assert model.order(gtd.order_id).status is OrderStatus.ACCEPTED
    model.advance_time(gtd.expires_at)
    assert model.order(gtd.order_id).status is OrderStatus.EXPIRED
    model.advance_time(gtc.submitted_at + timedelta(days=90))
    assert model.order(gtc.order_id).status is OrderStatus.EXPIRED


def test_cash_reservation_blocks_overcommit_and_cancel_releases_it() -> None:
    model = _model(cash="1000", strategy_budget="1000", gross_limit="1000", instrument_limit="1000")
    first = model.submit(_request(quantity="5", reference_price="100"))
    second = model.submit(
        _request(
            order_id="00000000-0000-4000-8000-000000000502",
            quantity="6",
            reference_price="100",
        )
    )

    assert first.status is OrderStatus.ACCEPTED
    assert second.status is OrderStatus.REJECTED
    assert second.reason_code == "INSUFFICIENT_AVAILABLE_CASH"
    model.cancel(first.order_id, _utc("2025-11-28T14:32:00Z"))
    replacement = model.submit(
        _request(
            order_id="00000000-0000-4000-8000-000000000503",
            quantity="6",
            reference_price="100",
        )
    )
    assert replacement.status is OrderStatus.ACCEPTED


def test_fill_rechecks_cash_and_keeps_the_unfilled_remainder_open() -> None:
    model = _model(cash="150", strategy_budget="1000", gross_limit="1000", instrument_limit="1000")
    model.submit(_request(quantity="2", reference_price="50"))

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.quantity == Decimal("1")
    order = model.order(fill.order_id)
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.remaining_quantity == Decimal("1")
    assert model.position(INSTRUMENT).quantity == Decimal("1")


def test_fill_rechecks_instrument_risk_at_the_actual_bar_price() -> None:
    model = _model(
        cash="10000",
        strategy_budget="10000",
        gross_limit="10000",
        instrument_limit="600",
    )
    model.submit(_request(quantity="10", reference_price="50"))

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.quantity == Decimal("5")
    assert model.order(fill.order_id).status is OrderStatus.PARTIALLY_FILLED


def test_fractional_long_market_day_is_allowed_but_other_combinations_fail() -> None:
    model = _model()
    model.submit(
        _request(quantity="0.5", quantity_mode=QuantityMode.FRACTIONAL_SHARES)
    )
    assert model.process_bar(_bar())[0].quantity == Decimal("0.5")

    with pytest.raises(ExecutionModelValidationError, match="fractional"):
        _request(
            order_type=OrderType.LIMIT,
            limit_price="100",
            quantity="0.5",
            quantity_mode=QuantityMode.FRACTIONAL_SHARES,
        )


def test_budget_and_instrument_risk_rejections_are_explicit() -> None:
    budget = _model(strategy_budget="500")
    rejected = budget.submit(_request(quantity="10", reference_price="100"))
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "STRATEGY_BUDGET_EXCEEDED"

    instrument = _model(instrument_limit="500")
    rejected = instrument.submit(_request(quantity="10", reference_price="100"))
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "INSTRUMENT_EXPOSURE_EXCEEDED"


def test_fifo_sale_records_realized_profit_and_balanced_double_entry() -> None:
    model = _model(cash="10000", strategy_budget="20000", gross_limit="20000", instrument_limit="20000")
    model.seed_long_position(INSTRUMENT, Decimal("2"), Decimal("90"))
    model.seed_long_position(INSTRUMENT, Decimal("3"), Decimal("100"))
    model.submit(
        _request(
            side=OrderSide.SELL,
            quantity="4",
            reference_price="120",
        )
    )

    fill = model.process_bar(
        _bar(open_price="120", high="121", low="119", close="120")
    )[0]

    assert fill.price == Decimal("119.9400")
    assert fill.cost_basis == Decimal("380")
    assert fill.realized_pnl == Decimal("99.7600")
    assert model.position(INSTRUMENT).quantity == Decimal("1")
    assert model.position(INSTRUMENT).cost_basis == Decimal("100")
    _assert_balanced(model)


def test_fifo_sale_loss_uses_a_balanced_loss_entry() -> None:
    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("1"), Decimal("120"))
    model.submit(_request(side=OrderSide.SELL, quantity="1", reference_price="100"))

    fill = model.process_bar(_bar())[0]

    assert fill.realized_pnl == Decimal("-20.0500")
    accounts = {
        entry.account_code for entry in model.ledger_transactions[0].entries
    }
    assert "EXPENSE:REALIZED_LOSS" in accounts
    _assert_balanced(model)


def test_order_processing_and_fill_identifiers_are_deterministic() -> None:
    def run() -> tuple[tuple[str, str], ...]:
        model = _model(cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000")
        model.submit(
            _request(
                order_id="00000000-0000-4000-8000-000000000502",
                instrument_id=OTHER_INSTRUMENT,
                quantity="1",
            )
        )
        model.submit(_request(quantity="1"))
        fills = model.process_bars(
            [_bar(instrument_id=OTHER_INSTRUMENT), _bar(instrument_id=INSTRUMENT)]
        )
        return tuple((fill.order_id, fill.fill_id) for fill in fills)

    first = run()
    second = run()

    assert first == second
    assert [order_id for order_id, _ in first] == sorted(order_id for order_id, _ in first)


def test_rejects_naive_times_invalid_ohlc_and_market_non_day() -> None:
    with pytest.raises(ExecutionModelValidationError, match="timezone-aware"):
        replace(_request(), submitted_at=datetime(2025, 11, 28, 14, 31))

    with pytest.raises(ExecutionModelValidationError, match="OHLC"):
        _bar(open_price="100", high="99", low="98", close="100")

    with pytest.raises(ExecutionModelValidationError, match="MARKET.*DAY"):
        _request(time_in_force=TimeInForce.GTC)
