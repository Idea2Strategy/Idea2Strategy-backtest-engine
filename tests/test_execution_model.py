"""Oracles for the D23 order/fill/position/ledger model.

Every monetary expectation is a hand-computed literal, never a re-derivation of a
production expression, and every quantized value is asserted at exactly the
canonical `numeric(24,8)` scale so a regression in `money.py` routing fails here.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from backtest_engine.execution_model import (
    CHART_OF_ACCOUNTS_VERSION,
    D23_MICROSTRUCTURE_POLICY_V1,
    BacktestExecutionModel,
    ExecutionBar,
    ExecutionMicrostructurePolicy,
    ExecutionModelValidationError,
    InstrumentFractionalPolicy,
    LedgerAccount,
    LedgerDirection,
    LedgerEntry,
    LedgerTransaction,
    MinuteBar,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    QuantityMode,
    RiskLimits,
    TimeInForce,
)
from backtest_engine.execution_policy import (
    D17_EXECUTION_POLICY_FIXTURE,
    ExecutionPolicy,
)
from backtest_engine.money import MoneyPrecisionError, is_quantized_money


INSTRUMENT = "00000000-0000-4000-8000-000000000401"
OTHER_INSTRUMENT = "00000000-0000-4000-8000-000000000402"
NON_FRACTIONAL_INSTRUMENT = "00000000-0000-4000-8000-000000000403"
EVENT_ID = "00000000-0000-4000-8000-000000000601"
OTHER_EVENT_ID = "00000000-0000-4000-8000-000000000602"
ENTRY_ID = "00000000-0000-4000-8000-000000000701"
OTHER_ENTRY_ID = "00000000-0000-4000-8000-000000000702"

FRACTIONAL_POLICY = InstrumentFractionalPolicy(
    policy_version="alpaca-v1",
    fractional_instrument_ids=frozenset({INSTRUMENT, OTHER_INSTRUMENT}),
)


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _model(
    cash: str = "10000",
    strategy_budget: str = "10000",
    gross_limit: str = "10000",
    instrument_limit: str = "10000",
    *,
    microstructure: ExecutionMicrostructurePolicy = D23_MICROSTRUCTURE_POLICY_V1,
    policy: ExecutionPolicy = D17_EXECUTION_POLICY_FIXTURE,
) -> BacktestExecutionModel:
    return BacktestExecutionModel(
        policy=policy,
        initial_cash=Decimal(cash),
        risk_limits=RiskLimits(
            max_strategy_notional=Decimal(strategy_budget),
            max_gross_exposure=Decimal(gross_limit),
            max_instrument_exposure=Decimal(instrument_limit),
        ),
        microstructure=microstructure,
        fractional_policy=FRACTIONAL_POLICY,
    )


def _request(
    order_id: str = "00000000-0000-4000-8000-000000000501",
    *,
    instrument_id: str = INSTRUMENT,
    side: OrderSide = OrderSide.BUY,
    order_type: OrderType = OrderType.MARKET,
    quantity: str | None = "10",
    notional_amount: str | None = None,
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
        quantity_mode=quantity_mode,
        time_in_force=time_in_force,
        submitted_at=_utc(submitted_at),
        eligible_at=_utc(eligible_at),
        day_expires_at=_utc(day_expires_at),
        reference_price=Decimal(reference_price),
        quantity=Decimal(quantity) if quantity is not None else None,
        notional_amount=Decimal(notional_amount) if notional_amount is not None else None,
        expires_at=_utc(expires_at) if expires_at else None,
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


def test_overlapping_multi_resolution_bars_do_not_reverse_the_execution_clock() -> None:
    model = _model()
    one_hour = ExecutionBar(
        instrument_id=INSTRUMENT,
        starts_at=_utc("2025-11-28T17:00:00Z"),
        ends_at=_utc("2025-11-28T18:00:00Z"),
        open=Decimal("100"), high=Decimal("101"), low=Decimal("99"),
        close=Decimal("100"), volume=Decimal("1000"), resolution="1h",
    )
    four_hour = ExecutionBar(
        instrument_id=OTHER_INSTRUMENT,
        starts_at=_utc("2025-11-28T14:00:00Z"),
        ends_at=_utc("2025-11-28T18:00:00Z"),
        open=Decimal("200"), high=Decimal("202"), low=Decimal("198"),
        close=Decimal("200"), volume=Decimal("1000"), resolution="4h",
    )

    assert model.process_bar(one_hour) == ()
    assert model.process_bar(four_hour) == ()


def _entry(
    *,
    entry_id: str = ENTRY_ID,
    account_code: str = LedgerAccount.CASH,
    direction: LedgerDirection = LedgerDirection.DEBIT,
    amount: str = "1.00000000",
    source_event_id: str = EVENT_ID,
    currency: str = "USD",
) -> LedgerEntry:
    return LedgerEntry(
        entry_id=entry_id,
        account_code=account_code,
        direction=direction,
        amount=Decimal(amount),
        source_event_id=source_event_id,
        currency=currency,
    )


# ---------------------------------------------------------------------------
# Declared policy values. These are the "no hidden default policy" gates.
# ---------------------------------------------------------------------------


def test_the_published_microstructure_policy_pins_every_declared_value() -> None:
    policy = D23_MICROSTRUCTURE_POLICY_V1

    assert policy.version == "d23-microstructure:1.0.0"
    assert policy.max_volume_participation_bps == 1000
    assert policy.max_volume_participation_rate == Decimal("0.1")
    assert policy.buying_power_buffer_policy_id == "00000000-0000-4000-8000-000000000001"
    assert policy.buying_power_buffer_bps == 1
    assert CHART_OF_ACCOUNTS_VERSION == "accounting:1.0.0"


def test_the_microstructure_policy_has_no_defaults_at_all() -> None:
    """Every field must be supplied; nothing may be silently assumed."""

    # The order horizons are deliberately absent: they belong to the run's
    # ExecutionPolicy, so there is exactly one place that pins them.
    required = {
        "version",
        "max_volume_participation_bps",
        "buying_power_buffer_policy_id",
        "buying_power_buffer_bps",
    }
    fields = ExecutionMicrostructurePolicy.__dataclass_fields__
    assert set(fields) == required
    import dataclasses

    for name in required:
        field = fields[name]
        assert field.default is dataclasses.MISSING, name
        assert field.default_factory is dataclasses.MISSING, name


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"version": ""}, "version"),
        ({"max_volume_participation_bps": 0}, "participation"),
        ({"max_volume_participation_bps": 10001}, "participation"),
        ({"buying_power_buffer_bps": 10000}, "buffer"),
        ({"buying_power_buffer_bps": -1}, "buffer"),
        ({"buying_power_buffer_policy_id": "not-a-uuid"}, "UUID"),
    ],
)
def test_microstructure_policy_rejects_unusable_values(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ExecutionModelValidationError, match=message):
        replace(D23_MICROSTRUCTURE_POLICY_V1, **overrides)


def test_model_refuses_a_microstructure_policy_bound_to_another_buffer_policy() -> None:
    foreign = replace(
        D23_MICROSTRUCTURE_POLICY_V1,
        buying_power_buffer_policy_id="00000000-0000-4000-8000-000000000009",
    )
    with pytest.raises(ExecutionModelValidationError, match="buying_power_buffer_policy_id"):
        _model(microstructure=foreign)


def test_model_refuses_an_accounting_rules_version_it_does_not_implement() -> None:
    foreign = replace(D17_EXECUTION_POLICY_FIXTURE, accounting_rules_version="accounting:2.0.0")
    with pytest.raises(ExecutionModelValidationError, match="accounting"):
        BacktestExecutionModel(
            policy=foreign,
            initial_cash=Decimal("10000"),
            risk_limits=RiskLimits(Decimal("1"), Decimal("1"), Decimal("1")),
            microstructure=D23_MICROSTRUCTURE_POLICY_V1,
            fractional_policy=FRACTIONAL_POLICY,
        )


def test_model_publishes_the_versions_it_ran_under() -> None:
    model = _model()

    assert model.model_version == "backtest-calculation-v1"
    assert model.microstructure_version == "d23-microstructure:1.0.0"
    assert model.chart_of_accounts_version == "accounting:1.0.0"
    assert model.precision_rules_version == "precision:1.0.0"
    assert model.fractional_policy_version == "alpaca-v1"


# ---------------------------------------------------------------------------
# Monetary precision
# ---------------------------------------------------------------------------


def test_market_order_waits_for_next_eligible_bar_and_posts_quantized_costs() -> None:
    """base 100 -> slippage 100*0.0005 = 0.05 -> price 100.05; 10 shares.

    gross 1000.50, fee 1000.50*0.002 = 2.001, cash 10000 - 1002.501 = 8997.499.
    """

    model = _model()
    model.submit(_request())

    assert model.process_bar(_bar("2025-11-28T14:30:00Z")) == ()
    fills = model.process_bar(_bar())

    assert len(fills) == 1
    fill = fills[0]
    assert fill.base_price == Decimal("100.00000000")
    assert fill.price == Decimal("100.05000000")
    assert fill.quantity == Decimal("10")
    assert fill.gross_amount == Decimal("1000.50000000")
    assert fill.slippage_amount == Decimal("0.50000000")
    assert fill.fee == Decimal("2.00100000")
    assert fill.cost_basis == Decimal("1000.50000000")
    assert fill.realized_pnl == Decimal("0.00000000")
    assert model.cash == Decimal("8997.49900000")
    assert model.position(INSTRUMENT).quantity == Decimal("10")
    assert model.order(fill.order_id).status is OrderStatus.FILLED

    for value in (
        fill.base_price,
        fill.price,
        fill.gross_amount,
        fill.slippage_amount,
        fill.fee,
        fill.cost_basis,
        fill.realized_pnl,
        model.cash,
        model.position(INSTRUMENT).cost_basis,
    ):
        assert is_quantized_money(value), value


def test_fractional_fill_quantizes_products_that_would_otherwise_overflow_scale() -> None:
    """0.33333333 * 100.05 = 33.3499996665 -> 33.34999967 at 8 dp.

    fee = 33.34999967 * 0.002 = 0.06669999934 -> 0.06670000.
    cash = 10000 - 33.34999967 - 0.06670000 = 9966.58330033.
    slippage = 0.05 * 0.33333333 = 0.0166666665 -> 0.01666667.
    """

    model = _model()
    model.submit(
        _request(quantity="0.33333333", quantity_mode=QuantityMode.FRACTIONAL_SHARES)
    )

    fill = model.process_bar(_bar())[0]

    assert fill.quantity == Decimal("0.33333333")
    assert fill.gross_amount == Decimal("33.34999967")
    assert fill.fee == Decimal("0.06670000")
    assert fill.slippage_amount == Decimal("0.01666667")
    assert model.cash == Decimal("9966.58330033")


def test_non_finite_and_unrepresentable_money_is_refused_by_the_shared_rule() -> None:
    with pytest.raises(MoneyPrecisionError):
        _model(cash="99999999999999999999")


# ---------------------------------------------------------------------------
# Volume participation (defect: volume never sized a fill)
# ---------------------------------------------------------------------------


def test_bar_volume_caps_the_fill_at_the_declared_participation_limit() -> None:
    """1000 traded shares x 10.00% participation = 100 fillable shares."""

    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
    model.submit(_request(quantity="250"))

    fill = model.process_bar(_bar(volume="1000"))[0]

    assert fill.quantity == Decimal("100")
    order = model.order(fill.order_id)
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.remaining_quantity == Decimal("150")


def test_participation_capacity_is_shared_by_every_order_on_the_same_bar() -> None:
    """Capacity 100 is consumed in submission order, not granted to each order."""

    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
    model.submit(_request(quantity="80"))
    model.submit(
        _request(order_id="00000000-0000-4000-8000-000000000502", quantity="80")
    )

    fills = model.process_bar(_bar(volume="1000"))

    assert [fill.quantity for fill in fills] == [Decimal("80"), Decimal("20")]
    assert (
        model.order("00000000-0000-4000-8000-000000000502").remaining_quantity
        == Decimal("60")
    )


def test_participation_capacity_is_replenished_by_the_next_bar() -> None:
    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
    model.submit(_request(quantity="150"))

    first = model.process_bar(_bar(volume="1000"))[0]
    second = model.process_bar(_bar("2025-11-28T14:32:00Z", volume="1000"))[0]

    assert first.quantity == Decimal("100")
    assert second.quantity == Decimal("50")
    assert model.order(first.order_id).status is OrderStatus.FILLED


def test_thin_volume_can_size_a_fill_below_one_whole_share() -> None:
    """5 traded shares x 10% = 0.5 -> a whole-share order cannot fill at all."""

    model = _model()
    model.submit(_request(quantity="10"))

    assert model.process_bar(_bar(volume="5")) == ()
    assert model.order("00000000-0000-4000-8000-000000000501").status is OrderStatus.ACCEPTED


def test_zero_volume_and_incomplete_bars_never_create_optimistic_fills() -> None:
    model = _model()
    model.submit(_request())

    assert model.process_bar(_bar(volume="0")) == ()
    assert model.process_bar(_bar("2025-11-28T14:32:00Z", complete=False)) == ()
    assert model.order("00000000-0000-4000-8000-000000000501").status is OrderStatus.ACCEPTED


# ---------------------------------------------------------------------------
# Price conditions by order type
# ---------------------------------------------------------------------------


def test_buy_limit_price_is_protected_instead_of_being_crossed_by_slippage() -> None:
    """base = min(open, limit) = 100; slipped 100.05 > limit -> clamp to 100.00.

    Aligned with F's RealisticFillModel.limitProtected. The executed price always
    lies between the observed base price and the limit, so no favourable price is
    invented and the limit is never crossed.
    """

    model = _model()
    model.submit(_request(order_type=OrderType.LIMIT, limit_price="100"))

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.base_price == Decimal("100.00000000")
    assert fill.price == Decimal("100.00000000")
    assert fill.slippage_amount == Decimal("0.00000000")
    assert model.order(fill.order_id).status is OrderStatus.FILLED


def test_buy_limit_keeps_genuine_price_improvement_when_slippage_stays_inside() -> None:
    """base 99 -> 99 * 1.0005 = 99.0495, still below the 100 limit, so no clamp."""

    model = _model()
    model.submit(_request(order_type=OrderType.LIMIT, limit_price="100"))

    fill = model.process_bar(_bar(open_price="99", high="100", low="98", close="99"))[0]

    assert fill.base_price == Decimal("99.00000000")
    assert fill.price == Decimal("99.04950000")
    assert fill.slippage_amount == Decimal("0.49500000")


def test_sell_limit_price_is_protected_on_the_other_side() -> None:
    """base = max(open, limit) = 100; slipped 99.95 < limit -> clamp to 100.00."""

    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("10"), Decimal("80"))
    model.submit(
        _request(side=OrderSide.SELL, order_type=OrderType.LIMIT, limit_price="100", quantity="10")
    )

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.price == Decimal("100.00000000")
    assert fill.slippage_amount == Decimal("0.00000000")


def test_limit_order_that_the_market_never_reaches_does_not_fill() -> None:
    model = _model()
    model.submit(_request(order_type=OrderType.LIMIT, limit_price="50"))

    assert model.process_bar(_bar(open_price="100", high="101", low="99")) == ()
    assert model.order("00000000-0000-4000-8000-000000000501").status is OrderStatus.ACCEPTED


@pytest.mark.parametrize(
    ("order_type", "kwargs", "bar", "expected_base", "expected_price"),
    [
        (
            OrderType.STOP,
            {"stop_price": "101"},
            _bar(open_price="100", high="102", low="99", close="101"),
            Decimal("101.00000000"),
            Decimal("101.05050000"),
        ),
        (
            OrderType.STOP_LIMIT,
            {"stop_price": "101", "limit_price": "102"},
            _bar(open_price="100", high="102", low="99", close="101"),
            Decimal("101.00000000"),
            Decimal("101.05050000"),
        ),
    ],
)
def test_stop_orders_trigger_deterministically_from_ohlc(
    order_type: OrderType,
    kwargs: dict[str, str],
    bar: MinuteBar,
    expected_base: Decimal,
    expected_price: Decimal,
) -> None:
    model = _model()
    model.submit(_request(order_type=order_type, **kwargs))

    fill = model.process_bar(bar)[0]

    assert fill.base_price == expected_base
    assert fill.price == expected_price
    assert model.order(fill.order_id).status is OrderStatus.FILLED


def test_stop_limit_that_triggers_above_its_limit_waits_as_a_plain_limit_order() -> None:
    """Gap through the stop: trigger base 105 > limit 102, so no fill on that bar."""

    model = _model()
    model.submit(
        _request(order_type=OrderType.STOP_LIMIT, stop_price="101", limit_price="102")
    )

    assert model.process_bar(_bar(open_price="105", high="106", low="104", close="105")) == ()
    assert model.order("00000000-0000-4000-8000-000000000501").status is OrderStatus.ACCEPTED

    fill = model.process_bar(
        _bar("2025-11-28T14:32:00Z", open_price="101", high="103", low="100", close="102")
    )[0]
    assert fill.base_price == Decimal("101.00000000")
    assert fill.price == Decimal("101.05050000")


def test_trailing_sell_updates_its_reference_then_triggers_on_a_later_bar() -> None:
    """Reference ratchets 100 -> 110; stop = 110 * 0.90 = 99; open 98 <= 99 -> fill at 98."""

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

    assert model.process_bar(_bar(open_price="100", high="110", low="95", close="108")) == ()
    fill = model.process_bar(
        _bar("2025-11-28T14:32:00Z", open_price="98", high="100", low="97", close="99")
    )[0]

    assert fill.base_price == Decimal("98.00000000")
    assert fill.price == Decimal("97.95100000")


def test_trailing_buy_ratchets_down_then_triggers_on_a_later_bar() -> None:
    """Reference ratchets 100 -> 90 (bar low); stop = 90 * 1.10 = 99; high 100 >= 99."""

    model = _model()
    model.submit(
        _request(
            order_type=OrderType.TRAILING_STOP,
            trail_percent="0.10",
            reference_price="100",
            quantity="1",
        )
    )

    assert model.process_bar(_bar(open_price="95", high="96", low="90", close="92")) == ()
    fill = model.process_bar(
        _bar("2025-11-28T14:32:00Z", open_price="95", high="100", low="94", close="99")
    )[0]

    assert fill.base_price == Decimal("99.00000000")
    assert fill.price == Decimal("99.04950000")


# ---------------------------------------------------------------------------
# Time in force
# ---------------------------------------------------------------------------


def test_day_gtd_and_gtc_orders_expire_at_their_policy_pinned_boundaries() -> None:
    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
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

    assert day.expires_at == _utc("2025-11-28T18:00:00Z")
    assert gtd.expires_at == _utc("2025-12-01T21:00:00Z")
    assert gtc.expires_at == _utc("2026-02-26T14:31:00Z")

    model.advance_time(day.expires_at)
    assert model.order(day.order_id).status is OrderStatus.EXPIRED
    assert model.order(day.order_id).reason_code == "DAY_EXPIRED"
    assert model.order(gtd.order_id).status is OrderStatus.ACCEPTED

    model.advance_time(gtd.expires_at)
    assert model.order(gtd.order_id).status is OrderStatus.EXPIRED
    assert model.order(gtd.order_id).reason_code == "GTD_EXPIRED"

    model.advance_time(gtc.expires_at)
    assert model.order(gtc.order_id).status is OrderStatus.EXPIRED
    assert model.order(gtc.order_id).reason_code == "GTC_HORIZON_EXPIRED"


def test_gtc_horizon_is_read_from_the_execution_policy_and_is_not_hardcoded() -> None:
    """The former `execution_model.py:440` 90-day literal is now policy-pinned.

    Two policies that differ only in `good_till_cancelled_horizon` must produce
    two different expiries off the same request.
    """

    default_expiry = _model().submit(
        _request(order_type=OrderType.LIMIT, limit_price="50", time_in_force=TimeInForce.GTC)
    ).expires_at
    assert default_expiry == _utc("2026-02-26T14:31:00Z")

    short_horizon = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        version="short-gtc-policy",
        good_till_cancelled_horizon=timedelta(days=7),
    )
    gtc = _model(policy=short_horizon).submit(
        _request(order_type=OrderType.LIMIT, limit_price="50", time_in_force=TimeInForce.GTC)
    )

    assert gtc.expires_at == _utc("2025-12-05T14:31:00Z")


def test_gtd_beyond_the_canonical_order_horizon_is_rejected_with_a_reason_code() -> None:
    model = _model()

    rejected = model.submit(
        _request(
            order_type=OrderType.LIMIT,
            limit_price="50",
            time_in_force=TimeInForce.GTD,
            expires_at="2026-03-30T21:00:00Z",
        )
    )

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "ORDER_HORIZON_EXCEEDED"
    assert model._open_order_ids == set()


def test_gtd_exactly_on_the_horizon_boundary_is_accepted() -> None:
    model = _model()

    accepted = model.submit(
        _request(
            order_type=OrderType.LIMIT,
            limit_price="50",
            time_in_force=TimeInForce.GTD,
            expires_at="2026-02-26T14:31:00Z",
        )
    )

    assert accepted.status is OrderStatus.ACCEPTED


def test_execution_clock_must_not_move_backward() -> None:
    model = _model()
    model.advance_time(_utc("2025-11-28T14:31:00Z"))

    with pytest.raises(ExecutionModelValidationError, match="backward"):
        model.advance_time(_utc("2025-11-28T14:30:00Z"))


# ---------------------------------------------------------------------------
# Cash, buying power buffer, budget and risk
# ---------------------------------------------------------------------------


def test_buying_power_buffer_withholds_the_declared_basis_points_of_cash() -> None:
    """1 bp of 10000 = 1.00, so buying power is 9999.00, not 10000.00."""

    model = _model()

    assert model.cash == Decimal("10000.00000000")
    assert model.buying_power == Decimal("9999.00000000")

    no_buffer = _model(microstructure=replace(D23_MICROSTRUCTURE_POLICY_V1, buying_power_buffer_bps=0))
    assert no_buffer.buying_power == Decimal("10000.00000000")


def test_buying_power_buffer_is_what_rejects_a_marginal_order() -> None:
    """cost 9999.00450000 fits 10000.00 cash but not the 9999.00 buying power."""

    model = _model(
        cash="10000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )

    rejected = model.submit(_request(quantity="99.75", quantity_mode=QuantityMode.FRACTIONAL_SHARES))

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "INSUFFICIENT_AVAILABLE_CASH"

    unbuffered = _model(
        cash="10000",
        strategy_budget="100000",
        gross_limit="100000",
        instrument_limit="100000",
        microstructure=replace(D23_MICROSTRUCTURE_POLICY_V1, buying_power_buffer_bps=0),
    )
    accepted = unbuffered.submit(
        _request(quantity="99.75", quantity_mode=QuantityMode.FRACTIONAL_SHARES)
    )
    assert accepted.status is OrderStatus.ACCEPTED


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
    """buying power 149.985 / unit cash 100.2501 = 1.496 -> one whole share."""

    model = _model(cash="150", strategy_budget="1000", gross_limit="1000", instrument_limit="1000")
    model.submit(_request(quantity="2", reference_price="50"))

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.quantity == Decimal("1")
    order = model.order(fill.order_id)
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.remaining_quantity == Decimal("1")
    assert model.position(INSTRUMENT).quantity == Decimal("1")
    assert model.cash == Decimal("49.74990000")


def test_fill_rechecks_instrument_risk_at_the_actual_bar_price() -> None:
    """600 headroom / 100.05 = 5.997 -> five whole shares."""

    model = _model(
        cash="10000", strategy_budget="10000", gross_limit="10000", instrument_limit="600"
    )
    model.submit(_request(quantity="10", reference_price="50"))

    fill = model.process_bar(_bar(open_price="100", high="101", low="99"))[0]

    assert fill.quantity == Decimal("5")
    assert model.order(fill.order_id).status is OrderStatus.PARTIALLY_FILLED


def test_budget_and_instrument_risk_rejections_are_explicit() -> None:
    budget = _model(strategy_budget="500")
    rejected = budget.submit(_request(quantity="10", reference_price="100"))
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "STRATEGY_BUDGET_EXCEEDED"

    instrument = _model(instrument_limit="500")
    rejected = instrument.submit(_request(quantity="10", reference_price="100"))
    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "INSTRUMENT_EXPOSURE_EXCEEDED"


def test_gross_exposure_limit_rejects_independently_of_the_strategy_budget() -> None:
    """notional 1000.50 > gross 1000, while cost 1002.501 stays under budget 2000."""

    model = _model(
        cash="10000", strategy_budget="2000", gross_limit="1000", instrument_limit="10000"
    )

    rejected = model.submit(_request(quantity="10", reference_price="100"))

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "GROSS_EXPOSURE_EXCEEDED"


def test_selling_more_than_the_held_position_is_rejected_as_position_unavailable() -> None:
    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("3"), Decimal("90"))

    rejected = model.submit(_request(side=OrderSide.SELL, quantity="4"))

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "POSITION_UNAVAILABLE"


def test_a_second_sell_cannot_reserve_the_same_shares() -> None:
    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("4"), Decimal("90"))
    first = model.submit(_request(side=OrderSide.SELL, quantity="3"))
    second = model.submit(
        _request(
            order_id="00000000-0000-4000-8000-000000000502",
            side=OrderSide.SELL,
            quantity="2",
        )
    )

    assert first.status is OrderStatus.ACCEPTED
    assert second.reason_code == "POSITION_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Declared scope: fractional, notional, short (cards D23 / F08A)
# ---------------------------------------------------------------------------


def test_fractional_long_market_day_on_an_enabled_instrument_is_allowed() -> None:
    model = _model()
    accepted = model.submit(
        _request(quantity="0.5", quantity_mode=QuantityMode.FRACTIONAL_SHARES)
    )

    assert accepted.status is OrderStatus.ACCEPTED
    assert model.process_bar(_bar())[0].quantity == Decimal("0.5")


@pytest.mark.parametrize(
    ("overrides", "reason_code"),
    [
        (
            {"order_type": OrderType.LIMIT, "limit_price": "100"},
            "FRACTIONAL_REQUIRES_MARKET_DAY",
        ),
        (
            {"instrument_id": NON_FRACTIONAL_INSTRUMENT},
            "FRACTIONAL_INSTRUMENT_NOT_ENABLED",
        ),
    ],
)
def test_fractional_scope_violations_are_rejected_with_f08a_reason_codes(
    overrides: dict[str, object], reason_code: str
) -> None:
    model = _model()

    rejected = model.submit(
        _request(quantity="0.5", quantity_mode=QuantityMode.FRACTIONAL_SHARES, **overrides)
    )

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == reason_code


def test_a_whole_share_order_never_silently_floors_a_fractional_request() -> None:
    """F rejects rather than rounding; D must not quietly turn 2.0001 into 2."""

    model = _model()

    rejected = model.submit(_request(quantity="2.0001"))

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == "WHOLE_SHARES_REQUIRE_INTEGER_QUANTITY"
    assert rejected.quantity == Decimal("2.0001")


def test_notional_long_market_day_order_spends_its_budget_and_then_closes() -> None:
    """500 / 100.05 = 4.9975012493... -> 4.99750124 shares after the capacity floor.

    gross = 100.05 * 4.99750124 = 499.999999062 -> 499.99999906.
    The unspent 0.00000094 is below one quantum's worth (100.05e-8 = 0.0000010005),
    so NOTIONAL_RESIDUAL_RULE closes the order as FILLED.
    """

    model = _model()
    model.submit(
        _request(
            quantity=None,
            notional_amount="500",
            quantity_mode=QuantityMode.NOTIONAL_AMOUNT,
        )
    )

    fill = model.process_bar(_bar())[0]

    assert fill.quantity == Decimal("4.99750124")
    assert fill.gross_amount == Decimal("499.99999906")
    assert fill.fee == Decimal("1.00000000")
    order = model.order(fill.order_id)
    assert order.status is OrderStatus.FILLED
    assert order.quantity is None
    assert order.notional_amount == Decimal("500.00000000")


def test_a_notional_order_capped_by_liquidity_keeps_its_remaining_budget_open() -> None:
    """Capacity 0.5 shares (5 traded x 10%); 0.5 * 100.05 = 50.025 spent of 500."""

    model = _model()
    model.submit(
        _request(
            quantity=None,
            notional_amount="500",
            quantity_mode=QuantityMode.NOTIONAL_AMOUNT,
        )
    )

    fill = model.process_bar(_bar(volume="5"))[0]

    assert fill.quantity == Decimal("0.50000000")
    assert fill.gross_amount == Decimal("50.02500000")
    order = model.order(fill.order_id)
    assert order.status is OrderStatus.PARTIALLY_FILLED
    assert order.remaining_notional == Decimal("449.97500000")


@pytest.mark.parametrize(
    ("overrides", "reason_code"),
    [
        (
            {"side": OrderSide.SELL},
            "NOTIONAL_REQUIRES_LONG_EXPOSURE",
        ),
        (
            {"instrument_id": NON_FRACTIONAL_INSTRUMENT},
            "FRACTIONAL_INSTRUMENT_NOT_ENABLED",
        ),
        (
            {"order_type": OrderType.LIMIT, "limit_price": "100"},
            "FRACTIONAL_REQUIRES_MARKET_DAY",
        ),
    ],
)
def test_notional_scope_violations_are_rejected_with_f08a_reason_codes(
    overrides: dict[str, object], reason_code: str
) -> None:
    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("10"), Decimal("90"))

    rejected = model.submit(
        _request(
            quantity=None,
            notional_amount="500",
            quantity_mode=QuantityMode.NOTIONAL_AMOUNT,
            **overrides,
        )
    )

    assert rejected.status is OrderStatus.REJECTED
    assert rejected.reason_code == reason_code


@pytest.mark.parametrize(
    ("quantity", "notional_amount", "quantity_mode", "message"),
    [
        ("10", "500", QuantityMode.WHOLE_SHARES, "exactly one"),
        (None, None, QuantityMode.WHOLE_SHARES, "exactly one"),
        ("10", None, QuantityMode.NOTIONAL_AMOUNT, "notional_amount"),
        (None, "500", QuantityMode.WHOLE_SHARES, "quantity"),
        (None, "500", QuantityMode.FRACTIONAL_SHARES, "quantity"),
    ],
)
def test_the_requested_measure_is_exactly_one_of_quantity_or_notional(
    quantity: str | None,
    notional_amount: str | None,
    quantity_mode: QuantityMode,
    message: str,
) -> None:
    with pytest.raises(ExecutionModelValidationError, match=message):
        _request(
            quantity=quantity, notional_amount=notional_amount, quantity_mode=quantity_mode
        )


# ---------------------------------------------------------------------------
# FIFO cost basis, realized P&L, double-entry ledger
# ---------------------------------------------------------------------------


def test_fifo_sale_records_realized_profit_and_a_balanced_double_entry() -> None:
    """Sell 4 at 120 - 0.06 = 119.94; gross 479.76; basis 2*90 + 2*100 = 380."""

    model = _model(
        cash="10000", strategy_budget="20000", gross_limit="20000", instrument_limit="20000"
    )
    model.seed_long_position(INSTRUMENT, Decimal("2"), Decimal("90"))
    model.seed_long_position(INSTRUMENT, Decimal("3"), Decimal("100"))
    model.submit(_request(side=OrderSide.SELL, quantity="4", reference_price="120"))

    fill = model.process_bar(_bar(open_price="120", high="121", low="119", close="120"))[0]

    assert fill.price == Decimal("119.94000000")
    assert fill.gross_amount == Decimal("479.76000000")
    assert fill.fee == Decimal("0.95952000")
    assert fill.cost_basis == Decimal("380.00000000")
    assert fill.realized_pnl == Decimal("99.76000000")
    assert model.cash == Decimal("10478.80048000")
    assert model.position(INSTRUMENT).quantity == Decimal("1")
    assert model.position(INSTRUMENT).cost_basis == Decimal("100.00000000")

    transaction = model.ledger_transactions[0]
    posted = {
        (entry.account_code, entry.direction): entry.amount for entry in transaction.entries
    }
    assert posted == {
        (LedgerAccount.CASH, LedgerDirection.DEBIT): Decimal("478.80048000"),
        (LedgerAccount.FEE_EXPENSE, LedgerDirection.DEBIT): Decimal("0.95952000"),
        (LedgerAccount.SECURITY, LedgerDirection.CREDIT): Decimal("380.00000000"),
        (LedgerAccount.REALIZED_PNL, LedgerDirection.CREDIT): Decimal("99.76000000"),
    }


def test_fifo_sale_loss_debits_the_same_realized_pnl_account() -> None:
    """F keeps one REALIZED_PNL account and encodes the sign in the direction."""

    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("1"), Decimal("120"))
    model.submit(_request(side=OrderSide.SELL, quantity="1", reference_price="100"))

    fill = model.process_bar(_bar())[0]

    assert fill.realized_pnl == Decimal("-20.05000000")
    posted = {
        (entry.account_code, entry.direction): entry.amount
        for entry in model.ledger_transactions[0].entries
    }
    assert posted == {
        (LedgerAccount.CASH, LedgerDirection.DEBIT): Decimal("99.75010000"),
        (LedgerAccount.FEE_EXPENSE, LedgerDirection.DEBIT): Decimal("0.19990000"),
        (LedgerAccount.REALIZED_PNL, LedgerDirection.DEBIT): Decimal("20.05000000"),
        (LedgerAccount.SECURITY, LedgerDirection.CREDIT): Decimal("120.00000000"),
    }


def test_fifo_consumes_a_partial_lot_without_leaving_cost_basis_dust() -> None:
    """3 shares costing 100.00000001 -> selling 1 must not strand 1e-8 of basis."""

    model = _model(
        cash="10000", strategy_budget="20000", gross_limit="20000", instrument_limit="20000"
    )
    model.seed_long_position(INSTRUMENT, Decimal("3"), Decimal("33.33333334"))
    model.submit(_request(side=OrderSide.SELL, quantity="3", reference_price="120"))

    fills = model.process_bars(
        [_bar(open_price="120", high="121", low="119", close="120", volume="10")]
    )

    assert fills[0].quantity == Decimal("1")
    assert fills[0].cost_basis == Decimal("33.33333334")
    assert model.position(INSTRUMENT).cost_basis == Decimal("66.66666668")


def test_consume_fifo_refuses_to_invent_cost_basis_it_does_not_hold() -> None:
    """Defensive invariant: no public path can reach it, so it is exercised directly."""

    model = _model()
    model.seed_long_position(INSTRUMENT, Decimal("1"), Decimal("90"))

    with pytest.raises(ExecutionModelValidationError, match="below reserved sell quantity"):
        model._consume_fifo(INSTRUMENT, Decimal("2"))


# ---------------------------------------------------------------------------
# Ledger value objects
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"entry_id": "not-a-uuid"}, "entry_id"),
        ({"source_event_id": "not-a-uuid"}, "source_event_id"),
        ({"account_code": ""}, "account_code"),
        ({"account_code": "ASSET:CASH"}, "chart of accounts"),
        ({"amount": "0"}, "ledger amount"),
        ({"amount": "-1"}, "ledger amount"),
        ({"currency": "KRW"}, "USD"),
    ],
)
def test_ledger_entry_validators(overrides: dict[str, str], message: str) -> None:
    with pytest.raises(ExecutionModelValidationError, match=message):
        _entry(**overrides)


def test_ledger_entry_amount_must_already_be_quantized() -> None:
    with pytest.raises(MoneyPrecisionError):
        _entry(amount="1.123456789")


@pytest.mark.parametrize(
    ("entries", "message"),
    [
        ((), "at least two entries"),
        ((_entry(),), "at least two entries"),
        (
            (
                _entry(entry_id=ENTRY_ID),
                _entry(
                    entry_id=OTHER_ENTRY_ID,
                    direction=LedgerDirection.CREDIT,
                    source_event_id=OTHER_EVENT_ID,
                ),
            ),
            "source_event_id",
        ),
        (
            (_entry(), _entry(direction=LedgerDirection.CREDIT)),
            "unique",
        ),
        (
            (
                _entry(entry_id=ENTRY_ID),
                _entry(
                    entry_id=OTHER_ENTRY_ID,
                    direction=LedgerDirection.CREDIT,
                    amount="2.00000000",
                ),
            ),
            "balanced",
        ),
    ],
)
def test_ledger_transaction_validators(
    entries: tuple[LedgerEntry, ...], message: str
) -> None:
    with pytest.raises(ExecutionModelValidationError, match=message):
        LedgerTransaction(
            transaction_id="00000000-0000-4000-8000-000000000801",
            source_event_id=EVENT_ID,
            posted_at=_utc("2025-11-28T14:32:00Z"),
            entries=entries,
        )


def test_ledger_transaction_rejects_a_naive_posting_time() -> None:
    with pytest.raises(ExecutionModelValidationError, match="timezone-aware"):
        LedgerTransaction(
            transaction_id="00000000-0000-4000-8000-000000000801",
            source_event_id=EVENT_ID,
            posted_at=datetime(2025, 11, 28, 14, 32),
            entries=(_entry(), _entry(entry_id=OTHER_ENTRY_ID, direction=LedgerDirection.CREDIT)),
        )


# ---------------------------------------------------------------------------
# Identity and determinism (pinned literals, not self-comparison)
# ---------------------------------------------------------------------------


def test_fill_transaction_and_entry_identifiers_are_pinned_literals() -> None:
    model = _model()
    model.submit(_request())

    fill = model.process_bar(_bar())[0]
    transaction = model.ledger_transactions[0]

    assert fill.fill_id == "41015ffe-105a-5038-857e-97d051fe02c1"
    assert fill.ledger_transaction_id == "c0d0ad79-2cb2-505a-b008-dc3e04f3e611"
    assert transaction.transaction_id == "c0d0ad79-2cb2-505a-b008-dc3e04f3e611"
    assert transaction.source_event_id == fill.fill_id
    assert [entry.entry_id for entry in transaction.entries] == [
        "7d79bbde-00b0-5fe5-8bcd-25c9a48baf62",
        "c61614b6-911c-5e3f-955c-d8b55edf4a1e",
        "ebf1d077-7a5a-5a5b-b3c5-4d3f5dde6cd2",
    ]


def test_a_second_fill_of_the_same_order_gets_the_pinned_sequence_two_identity() -> None:
    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
    model.submit(_request(quantity="150"))

    model.process_bar(_bar(volume="1000"))
    second = model.process_bar(_bar("2025-11-28T14:32:00Z", volume="1000"))[0]

    assert second.fill_id == "a4f499b2-7fad-5a3c-a4a4-4db313990ec2"
    assert second.ledger_transaction_id == "36254b21-9fc4-5d0a-9ad0-ea22a3729dbc"


def test_bars_are_replayed_in_a_pinned_deterministic_order() -> None:
    model = _model(
        cash="100000", strategy_budget="100000", gross_limit="100000", instrument_limit="100000"
    )
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

    assert [(fill.order_id, fill.fill_id) for fill in fills] == [
        (
            "00000000-0000-4000-8000-000000000501",
            "41015ffe-105a-5038-857e-97d051fe02c1",
        ),
        (
            "00000000-0000-4000-8000-000000000502",
            "8e4c747a-c37a-526f-a93e-a7ecb352bd49",
        ),
    ]


# ---------------------------------------------------------------------------
# Lifecycle edges
# ---------------------------------------------------------------------------


def test_duplicate_order_id_on_submit_is_refused() -> None:
    model = _model()
    model.submit(_request())

    with pytest.raises(ExecutionModelValidationError, match="unique"):
        model.submit(_request(quantity="1"))


def test_cancelling_an_unknown_order_raises_a_named_error() -> None:
    model = _model()

    with pytest.raises(KeyError, match="unknown order_id"):
        model.cancel("00000000-0000-4000-8000-000000000509", _utc("2025-11-28T14:32:00Z"))


def test_cancelling_a_terminal_order_is_an_idempotent_no_op() -> None:
    model = _model()
    model.submit(_request())
    filled = model.process_bar(_bar())[0]

    cancelled = model.cancel(filled.order_id, _utc("2025-11-28T14:32:00Z"))
    again = model.cancel(filled.order_id, _utc("2025-11-28T14:33:00Z"))

    assert cancelled.status is OrderStatus.FILLED
    assert cancelled.reason_code is None
    assert again == cancelled


def test_cancelling_an_open_order_records_the_explicit_reason() -> None:
    model = _model()
    model.submit(_request())

    cancelled = model.cancel(
        "00000000-0000-4000-8000-000000000501", _utc("2025-11-28T14:32:00Z")
    )

    assert cancelled.status is OrderStatus.CANCELLED
    assert cancelled.reason_code == "EXPLICITLY_CANCELLED"


def test_seeding_a_position_after_order_processing_is_refused() -> None:
    model = _model()
    model.submit(_request())

    with pytest.raises(ExecutionModelValidationError, match="before order processing"):
        model.seed_long_position(INSTRUMENT, Decimal("1"), Decimal("100"))


def test_rejects_naive_times_invalid_ohlc_and_market_non_day() -> None:
    with pytest.raises(ExecutionModelValidationError, match="timezone-aware"):
        replace(_request(), submitted_at=datetime(2025, 11, 28, 14, 31))

    with pytest.raises(ExecutionModelValidationError, match="OHLC"):
        _bar(open_price="100", high="99", low="98", close="100")

    with pytest.raises(ExecutionModelValidationError, match="MARKET.*DAY"):
        _request(time_in_force=TimeInForce.GTC)
