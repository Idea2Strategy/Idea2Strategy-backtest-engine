from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import pytest

from backtest_engine.contracts import build_backtest_result_event
from backtest_engine.execution_model import (
    BacktestOrder,
    Fill,
    OrderSide,
    OrderStatus,
    OrderType,
)
from backtest_engine.money import format_money, is_quantized_money
from backtest_engine.performance import (
    CALCULATION_RULES_VERSION,
    METRIC_CATALOG_VERSION,
    MarkPrice,
    ValuationBasis,
    ValuationInstant,
    ValuationPeriodicity,
    ValuationSeries,
)
from backtest_engine.result_snapshot import (
    InMemoryResultSnapshotStore,
    PositionAfter,
    ResultIntegrityError,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshot,
    ResultSnapshotBuilder,
    ResultSnapshotConflict,
    ResultSnapshotValidationError,
    RunSnapshot,
    fill_result_record,
    order_result_record,
)


RUN_ID = "00000000-0000-4000-8000-000000000801"
STRATEGY_VERSION_ID = "00000000-0000-4000-8000-000000000802"
ORDER_ID = "00000000-0000-4000-8000-000000000803"
OTHER_ORDER_ID = "00000000-0000-4000-8000-000000000804"
THIRD_ORDER_ID = "00000000-0000-4000-8000-000000000805"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000000806"
OTHER_INSTRUMENT_ID = "00000000-0000-4000-8000-000000000807"
FILL_ID = "00000000-0000-4000-8000-000000000808"
SECOND_FILL_ID = "00000000-0000-4000-8000-000000000810"
BOT_ID = "00000000-0000-4000-8000-000000000831"
OWNER_ACCOUNT_ID = "00000000-0000-4000-8000-000000000832"

#: `_run().snapshot_id`. Pinned so a change to the run payload or to the digest
#: is a visible, reviewed change rather than a silent one.
RUN_SNAPSHOT_ID = "90a84df2fcebbc226b2d427e6996a61e315294027532f677485b61fcb50cccfe"


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run() -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=RUN_ID,
        strategy_version_id=STRATEGY_VERSION_ID,
        input_bundle_fingerprint="a" * 64,
        calculation_model_version="calc-v1",
        cost_model_version="cost-v1",
        execution_model_version="execution-v1",
        initial_cash=Decimal("10000"),
    )


def _position(
    instrument_id: str = INSTRUMENT_ID,
    quantity: str = "10",
    cost_basis: str = "1000.5000",
) -> PositionAfter:
    return PositionAfter(
        instrument_id=instrument_id,
        quantity=Decimal(quantity),
        cost_basis=Decimal(cost_basis),
    )


def _record(
    *,
    kind: ResultRecordKind,
    record_id: str,
    occurred_at: str,
    order_id: str,
    instrument_id: str = INSTRUMENT_ID,
    status: OrderStatus,
    cash_after: str,
    positions_after: tuple[PositionAfter, ...] = (),
    reason_code: str | None = None,
    fill_id: str | None = None,
    quantity: str | None = None,
    base_price: str | None = None,
    price: str | None = None,
    gross_amount: str | None = None,
    slippage_amount: str | None = None,
    fee: str | None = None,
    cost_basis: str | None = None,
    realized_pnl: str | None = None,
) -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=_run().snapshot_id,
        record_id=record_id,
        kind=kind,
        occurred_at=_utc(occurred_at),
        order_id=order_id,
        instrument_id=instrument_id,
        order_status=status,
        cash_after=Decimal(cash_after),
        positions_after=positions_after,
        reason_code=reason_code,
        fill_id=fill_id,
        quantity=Decimal(quantity) if quantity is not None else None,
        base_price=Decimal(base_price) if base_price is not None else None,
        price=Decimal(price) if price is not None else None,
        gross_amount=Decimal(gross_amount) if gross_amount is not None else None,
        slippage_amount=(
            Decimal(slippage_amount) if slippage_amount is not None else None
        ),
        fee=Decimal(fee) if fee is not None else None,
        cost_basis=Decimal(cost_basis) if cost_basis is not None else None,
        realized_pnl=(Decimal(realized_pnl) if realized_pnl is not None else None),
    )


def _accepted_record() -> ResultRecord:
    return _record(
        kind=ResultRecordKind.ORDER,
        record_id="00000000-0000-4000-8000-000000000811",
        occurred_at="2025-11-28T14:30:00Z",
        order_id=ORDER_ID,
        status=OrderStatus.ACCEPTED,
        cash_after="10000",
    )


def _fill_record() -> ResultRecord:
    return _record(
        kind=ResultRecordKind.FILL,
        record_id="00000000-0000-4000-8000-000000000812",
        occurred_at="2025-11-28T14:31:00Z",
        order_id=ORDER_ID,
        status=OrderStatus.FILLED,
        cash_after="8997.4990000",
        positions_after=(_position(),),
        fill_id=FILL_ID,
        quantity="10",
        base_price="100",
        price="100.05",
        gross_amount="1000.5000",
        slippage_amount="0.5000",
        fee="2.0010000",
        cost_basis="1000.5000",
        realized_pnl="0",
    )


# ---------------------------------------------------------------------------
# snapshot assembly
# ---------------------------------------------------------------------------


def test_builds_snapshot_linked_detail_manifest_summary_and_completion_fields() -> None:
    records = [
        _record(
            kind=ResultRecordKind.CANCELLATION,
            record_id="00000000-0000-4000-8000-000000000814",
            occurred_at="2025-11-28T14:33:00Z",
            order_id=THIRD_ORDER_ID,
            status=OrderStatus.CANCELLED,
            cash_after="8997.4990000",
            positions_after=(_position(),),
            reason_code="EXPLICITLY_CANCELLED",
        ),
        _fill_record(),
        _record(
            kind=ResultRecordKind.REJECTION,
            record_id="00000000-0000-4000-8000-000000000813",
            occurred_at="2025-11-28T14:32:00Z",
            order_id=OTHER_ORDER_ID,
            instrument_id=OTHER_INSTRUMENT_ID,
            status=OrderStatus.REJECTED,
            cash_after="8997.4990000",
            positions_after=(_position(),),
            reason_code="STRATEGY_BUDGET_EXCEEDED",
        ),
        _accepted_record(),
    ]

    result = ResultSnapshotBuilder().build(
        _run(), records, _utc("2025-11-28T14:35:00Z")
    )

    assert [record.kind for record in result.records] == [
        ResultRecordKind.ORDER,
        ResultRecordKind.FILL,
        ResultRecordKind.REJECTION,
        ResultRecordKind.CANCELLATION,
    ]
    assert result.summary.order_count == 3
    assert result.summary.fill_count == 1
    assert result.summary.cancellation_count == 1
    assert result.summary.rejection_count == 1
    assert result.summary.total_fees == Decimal("2.0010000")
    assert result.summary.total_slippage == Decimal("0.5000")
    assert result.summary.realized_pnl == Decimal("0")
    # 10000 - (1000.5000 gross + 2.0010000 fee) = 8997.4990000
    assert result.summary.ending_cash == Decimal("8997.49900000")
    assert result.summary.ending_positions == (_position(),)

    # The completion fields have to drop straight into B's envelope
    # convention: camelCase and `sha256:`-prefixed, status COMPLETED (never
    # COMPLETE). `build_backtest_result_event` validates against the JSON
    # Schema and re-derives the idempotency key, so a wrong shape raises here.
    fields = result.completion_fields()
    event = build_backtest_result_event(
        status="COMPLETED",
        backtest_run_id=RUN_ID,
        bot_id=BOT_ID,
        owner_account_id=OWNER_ACCOUNT_ID,
        expected_snapshot_hash="sha256:" + "b" * 64,
        input_bundle_fingerprint="sha256:" + "a" * 64,
        execution_policy_version="official-backtest-policy-v1",
        message_id="00000000-0000-4000-8000-000000000821",
        occurred_at="2025-11-28T14:35:00Z",
        correlation_id="00000000-0000-4000-8000-000000000822",
        attempt=1,
        completedAt="2025-11-28T14:35:00Z",
        **fields,
    )

    assert event["status"] == "COMPLETED"
    assert event["metadata"]["messageType"] == "BACKTEST_COMPLETED"
    assert event["resultManifestId"] == result.manifest.result_manifest_id
    assert event["resultHash"] == f"sha256:{result.summary.result_hash}"


def test_output_is_deterministic_regardless_of_record_input_order() -> None:
    records = [_accepted_record(), _fill_record()]
    builder = ResultSnapshotBuilder()

    first = builder.build(_run(), records, _utc("2025-11-28T14:35:00Z"))
    second = builder.build(
        _run(), list(reversed(records)), _utc("2025-11-28T14:35:00Z")
    )

    assert first == second
    # A constant-returning implementation would pass `first == second`; these
    # pin the actual canonical identity.
    assert first.run_snapshot.snapshot_id == RUN_SNAPSHOT_ID
    assert first.manifest.object_key == (
        f"backtest-results/{RUN_ID}/{first.manifest.content_hash}.json"
    )


def test_no_trade_run_has_a_reproducible_empty_detail_object() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), [], _utc("2025-11-28T14:35:00Z")
    )

    assert result.records == ()
    assert result.manifest.record_count == 0
    assert result.summary.order_count == 0
    assert result.summary.fill_count == 0
    assert result.summary.ending_cash == Decimal("10000")
    assert result.summary.ending_positions == ()
    # Opening point plus the completion sample; both equal to the initial cash.
    assert [point.equity for point in result.summary.equity_curve.points] == [
        Decimal("10000.00000000"),
        Decimal("10000.00000000"),
    ]
    assert result.summary.metrics["totalReturnPct"].value == Decimal("0.00000000")


def test_record_id_is_a_pinned_content_addressed_uuid5() -> None:
    """`_event_id` is uuid5(NAMESPACE_URL, material) over

    ``idea2strategy:d25:record:<snapshot_id>:<kind>:<order_id>:<ts>:<status>:<fill_id|->``

    Pinned as a literal: comparing a generated id with itself would also pass
    for an implementation that returned a fixed value.
    """

    order = BacktestOrder(
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        remaining_quantity=Decimal("10"),
        status=OrderStatus.ACCEPTED,
        submitted_at=_utc("2025-11-28T14:30:00Z"),
        eligible_at=_utc("2025-11-28T14:30:00Z"),
        expires_at=_utc("2025-11-28T21:00:00Z"),
        reason_code=None,
    )

    record = order_result_record(
        _run(), order, _utc("2025-11-28T14:30:00Z"), Decimal("10000"), ()
    )

    assert record.record_id == "87d7ff2b-225c-5246-a8ea-338b2ce003b7"
    assert _fill_via_adapter().record_id == "3c6de302-cc80-58de-97c7-e08bcc4f35dc"


def _fill_via_adapter() -> ResultRecord:
    order = BacktestOrder(
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        remaining_quantity=Decimal("0"),
        status=OrderStatus.FILLED,
        submitted_at=_utc("2025-11-28T14:30:00Z"),
        eligible_at=_utc("2025-11-28T14:31:00Z"),
        expires_at=_utc("2025-11-28T21:00:00Z"),
        reason_code=None,
    )
    fill = Fill(
        fill_id=FILL_ID,
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        quantity=Decimal("10"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("1000.5000"),
        slippage_amount=Decimal("0.5000"),
        fee=Decimal("2.0010000"),
        cost_basis=Decimal("1000.5000"),
        realized_pnl=Decimal("0"),
        occurred_at=_utc("2025-11-28T14:31:00Z"),
        ledger_transaction_id="00000000-0000-4000-8000-000000000809",
    )
    return fill_result_record(_run(), fill, order, Decimal("8997.4990000"), (_position(),))


def test_d23_order_and_fill_adapters_preserve_costs_and_after_state() -> None:
    record = _fill_via_adapter()

    assert record.kind is ResultRecordKind.FILL
    assert record.fill_id == FILL_ID
    assert record.fee == Decimal("2.0010000")
    assert record.slippage_amount == Decimal("0.5000")
    assert record.realized_pnl == Decimal("0")
    assert record.cash_after == Decimal("8997.4990000")
    assert record.positions_after == (_position(),)


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (OrderStatus.ACCEPTED, ResultRecordKind.ORDER),
        (OrderStatus.CANCELLED, ResultRecordKind.CANCELLATION),
        (OrderStatus.EXPIRED, ResultRecordKind.CANCELLATION),
        (OrderStatus.REJECTED, ResultRecordKind.REJECTION),
    ],
)
def test_d23_order_adapter_preserves_terminal_reason(
    status: OrderStatus, kind: ResultRecordKind
) -> None:
    reason = None if status is OrderStatus.ACCEPTED else f"{status.value}_REASON"
    order = BacktestOrder(
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity=Decimal("2"),
        remaining_quantity=Decimal("2"),
        status=status,
        submitted_at=_utc("2025-11-28T14:30:00Z"),
        eligible_at=_utc("2025-11-28T14:31:00Z"),
        expires_at=_utc("2025-11-28T21:00:00Z"),
        reason_code=reason,
    )

    record = order_result_record(
        _run(),
        order,
        _utc("2025-11-28T14:32:00Z"),
        Decimal("10000"),
        (),
    )

    assert record.kind is kind
    assert record.reason_code == reason


def test_store_is_idempotent_but_rejects_a_different_snapshot_result() -> None:
    builder = ResultSnapshotBuilder()
    first = builder.build(
        _run(), [_accepted_record()], _utc("2025-11-28T14:35:00Z")
    )
    changed = builder.build(
        _run(), [_accepted_record(), _fill_record()], _utc("2025-11-28T14:35:00Z")
    )
    store = InMemoryResultSnapshotStore()

    assert store.put(first) == first.manifest
    assert store.put(first) == first.manifest
    assert store.get(first.manifest.result_manifest_id) == first
    with pytest.raises(ResultSnapshotConflict, match="run snapshot"):
        store.put(changed)

    different_snapshot = builder.build(
        replace(_run(), input_bundle_fingerprint="b" * 64),
        [],
        _utc("2025-11-28T14:35:00Z"),
    )
    with pytest.raises(ResultSnapshotConflict, match="backtest run"):
        store.put(different_snapshot)


# ---------------------------------------------------------------------------
# D25: the ledger-derived cash and the performance summary
# ---------------------------------------------------------------------------


def _buy_then_sell() -> list[ResultRecord]:
    """BUY 10 @ 100 (fee 1), then SELL 10 @ 110 (fee 1.1).

    cash: 10000 - 1000 - 1 = 8999 -> 8999 + 1100 - 1.1 = 10097.9
    realized: 1100 - 1000 = 100 gross of the sell fee.
    """

    return [
        _record(
            kind=ResultRecordKind.FILL,
            record_id="00000000-0000-4000-8000-000000000841",
            occurred_at="2025-11-03T14:30:00Z",
            order_id=ORDER_ID,
            status=OrderStatus.FILLED,
            cash_after="8999",
            positions_after=(_position(quantity="10", cost_basis="1000"),),
            fill_id=FILL_ID,
            quantity="10",
            base_price="100",
            price="100",
            gross_amount="1000",
            slippage_amount="0",
            fee="1",
            cost_basis="1000",
            realized_pnl="0",
        ),
        _record(
            kind=ResultRecordKind.FILL,
            record_id="00000000-0000-4000-8000-000000000842",
            occurred_at="2025-11-05T15:00:00Z",
            order_id=OTHER_ORDER_ID,
            status=OrderStatus.FILLED,
            cash_after="10097.9",
            positions_after=(),
            fill_id=SECOND_FILL_ID,
            quantity="10",
            base_price="110",
            price="110",
            gross_amount="1100",
            slippage_amount="0",
            fee="1.1",
            cost_basis="1000",
            realized_pnl="100",
        ),
    ]


def test_ending_cash_is_derived_from_the_fill_ledger_not_from_the_last_record() -> None:
    stale = _record(
        kind=ResultRecordKind.REJECTION,
        record_id="00000000-0000-4000-8000-000000000843",
        occurred_at="2025-11-05T16:00:00Z",
        order_id=THIRD_ORDER_ID,
        status=OrderStatus.REJECTED,
        # Deliberately wrong: the pre-rebuild summary copied this number out.
        cash_after="777",
        reason_code="STRATEGY_BUDGET_EXCEEDED",
    )

    result = ResultSnapshotBuilder().build(
        _run(), [*_buy_then_sell(), stale], _utc("2025-11-05T17:00:00Z")
    )

    # 10000 - (1000 + 1) + (1100 - 1.1) = 10097.9
    assert result.summary.ending_cash == Decimal("10097.90000000")
    assert result.records[-1].cash_after == Decimal("777")


def test_closing_fill_drives_win_rate_and_realized_pnl() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), _buy_then_sell(), _utc("2025-11-05T17:00:00Z")
    )
    metrics = result.summary.metrics

    assert metrics["fillCount"].value == Decimal("2")
    # Only the second fill reduces the position, so it is the sole closing trade.
    assert metrics["closingTradeCount"].value == Decimal("1")
    assert metrics["winningTradeCount"].value == Decimal("1")
    assert metrics["losingTradeCount"].value == Decimal("0")
    assert metrics["winRatePct"].value == Decimal("100.00000000")
    assert metrics["realizedPnl"].value == Decimal("100.00000000")


def test_a_loss_making_closing_fill_is_counted_as_a_loss() -> None:
    records = _buy_then_sell()
    losing = replace(records[1], gross_amount=Decimal("900"), price=Decimal("90"), base_price=Decimal("90"),
                     cash_after=Decimal("9897.9"), realized_pnl=Decimal("-100"))
    result = ResultSnapshotBuilder().build(
        _run(), [records[0], losing], _utc("2025-11-05T17:00:00Z")
    )
    metrics = result.summary.metrics

    assert metrics["winningTradeCount"].value == Decimal("0")
    assert metrics["losingTradeCount"].value == Decimal("1")
    assert metrics["winRatePct"].value == Decimal("0.00000000")
    # 10000 - 1001 + (900 - 1.1) = 9897.9
    assert result.summary.ending_cash == Decimal("9897.90000000")


def test_a_fill_whose_position_delta_contradicts_its_quantity_is_rejected() -> None:
    broken = replace(_fill_record(), positions_after=(_position(quantity="3", cost_basis="300"),))

    with pytest.raises(ResultSnapshotValidationError, match="position quantity"):
        ResultSnapshotBuilder().build(
            _run(), [_accepted_record(), broken], _utc("2025-11-28T14:35:00Z")
        )


def _mark_series() -> ValuationSeries:
    return ValuationSeries(
        basis=ValuationBasis.MARK_TO_MARKET,
        periodicity=ValuationPeriodicity.DAILY,
        opening_at=_utc("2025-11-03T14:00:00Z"),
        instants=(
            ValuationInstant(_utc("2025-11-03T21:00:00Z"), (MarkPrice(INSTRUMENT_ID, Decimal("102")),)),
            ValuationInstant(_utc("2025-11-04T21:00:00Z"), (MarkPrice(INSTRUMENT_ID, Decimal("98")),)),
            ValuationInstant(_utc("2025-11-05T21:00:00Z"), ()),
        ),
    )


def test_a_supplied_daily_mark_grid_produces_a_mark_to_market_equity_curve() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), _buy_then_sell(), _utc("2025-11-05T21:00:00Z"), _mark_series()
    )
    curve = result.summary.equity_curve

    assert curve.basis is ValuationBasis.MARK_TO_MARKET
    assert curve.periodicity is ValuationPeriodicity.DAILY
    # 10000 | 8999 + 10*102 = 10019 | 8999 + 10*98 = 9979 | 10097.9 flat
    assert [point.equity for point in curve.points] == [
        Decimal("10000.00000000"),
        Decimal("10019.00000000"),
        Decimal("9979.00000000"),
        Decimal("10097.90000000"),
    ]
    # (10097.9 - 10000) / 10000 * 100 = 0.979
    assert result.summary.metrics["totalReturnPct"].value == Decimal("0.97900000")
    # running peak at the trough is 10019; (9979 - 10019) / 10019 * 100
    # = -40 / 10019 * 100 = -0.39924144...  -> -0.39924144 at 8 dp
    assert result.summary.metrics["maxDrawdownPct"].value == Decimal("-0.39924144")
    assert result.summary.metrics["sharpe"].value is not None


def test_without_a_mark_grid_the_curve_is_an_explicit_cost_basis_event_curve() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), [_accepted_record(), _fill_record()], _utc("2025-11-28T14:35:00Z")
    )
    curve = result.summary.equity_curve

    assert curve.basis is ValuationBasis.COST_BASIS
    assert curve.periodicity is ValuationPeriodicity.EVENT
    # Unrealised P&L is zero by definition of the basis, so equity only loses
    # the fee: cash 10000 - (1000.5 gross + 2.001 fee) = 8997.499, plus the
    # position back at its 1000.5 cost basis = 9997.999, i.e. 10000 - 2.001.
    assert curve.closing.equity == Decimal("9997.99900000")
    # Sharpe cannot be annualised off an event grid, and says so.
    assert result.summary.metrics["sharpe"].value is None
    assert result.summary.metrics_document()["valuationBasis"] == "COST_BASIS"


def test_summary_carries_the_four_canonical_hashes_and_the_catalog_versions() -> None:
    builder = ResultSnapshotBuilder()
    result = builder.build(_run(), _buy_then_sell(), _utc("2025-11-05T17:00:00Z"))
    summary = result.summary

    assert summary.metric_catalog_version == METRIC_CATALOG_VERSION
    assert summary.calculation_rules_version == CALCULATION_RULES_VERSION
    assert summary.calculated_at == _utc("2025-11-05T17:00:00Z")
    # The fourth canonical hash: the pinned run inputs the other three hang off.
    assert summary.run_snapshot_id == RUN_SNAPSHOT_ID
    for value in (summary.source_set_hash, summary.input_hash, summary.result_hash):
        assert len(value) == 64 and value == value.lower()
    assert len({summary.source_set_hash, summary.input_hash, summary.result_hash}) == 3

    # A different valuation grid keeps the same source set but changes the
    # inputs and therefore the result.
    marked = builder.build(
        _run(), _buy_then_sell(), _utc("2025-11-05T21:00:00Z"), _mark_series()
    )
    assert marked.summary.source_set_hash == summary.source_set_hash
    assert marked.summary.input_hash != summary.input_hash
    assert marked.summary.result_hash != summary.result_hash


def test_performance_row_matches_the_canonical_performance_summaries_columns() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), _buy_then_sell(), _utc("2025-11-05T17:00:00Z")
    )

    row = result.performance_row()

    assert row.run_id == UUID(RUN_ID)
    assert row.metric_catalog_version == METRIC_CATALOG_VERSION
    assert row.calculation_rules_version == CALCULATION_RULES_VERSION
    assert row.source_set_hash == result.summary.source_set_hash
    assert row.input_hash == result.summary.input_hash
    assert row.result_hash == result.summary.result_hash
    assert row.calculated_at == _utc("2025-11-05T17:00:00Z")
    # jsonb has to survive the driver's json.dumps without a custom encoder.
    document = json.loads(json.dumps(dict(row.metrics_document)))
    assert document["totalReturnPct"] == 0.979
    assert document["endingCash"] == "10097.90000000"
    assert document["metricRules"]["winRatePct"] == "metric.win_rate_pct:1.0.0"


# ---------------------------------------------------------------------------
# precision:1.0.0 routing (spec 2.3): money.py is the only quantization point
# ---------------------------------------------------------------------------


def test_every_stored_amount_sits_at_the_canonical_numeric_24_8_scale() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), _buy_then_sell(), _utc("2025-11-05T17:00:00Z")
    )
    fill = result.records[0]
    summary = result.summary

    for value in (
        fill.cash_after,
        fill.base_price,
        fill.price,
        fill.gross_amount,
        fill.slippage_amount,
        fill.fee,
        fill.cost_basis,
        fill.realized_pnl,
        summary.total_fees,
        summary.total_slippage,
        summary.realized_pnl,
        summary.initial_cash,
        summary.ending_cash,
        summary.equity_curve.closing.equity,
        summary.equity_curve.closing.cash,
    ):
        assert is_quantized_money(value), value

    # `numeric(24,8)` text is the storage form, and it is what the hashes see.
    assert format_money(summary.ending_cash) == "10097.90000000"
    assert format_money(fill.fee) == "1.00000000"


def test_an_over_precise_amount_is_quantized_half_even_at_the_single_point() -> None:
    """9 dp in, 8 dp stored, ties to even -- and the derivations agree."""

    record = _record(
        kind=ResultRecordKind.FILL,
        record_id="00000000-0000-4000-8000-000000000861",
        occurred_at="2025-11-28T14:31:00Z",
        order_id=ORDER_ID,
        status=OrderStatus.FILLED,
        cash_after="9899.87654322",
        positions_after=(
            PositionAfter(
                instrument_id=INSTRUMENT_ID,
                quantity=Decimal("1"),
                cost_basis=Decimal("100.123456785"),
            ),
        ),
        fill_id=FILL_ID,
        quantity="1",
        base_price="100.123456785",
        price="100.123456785",
        # price * 1 quantized half-even: the dropped digit is exactly 5 and the
        # kept digit 8 is even, so it stays 8.
        gross_amount="100.12345678",
        slippage_amount="0",
        fee="0",
        cost_basis="100.123456785",
        realized_pnl="0",
    )

    assert format_money(record.price) == "100.12345678"
    assert format_money(record.gross_amount) == "100.12345678"
    assert format_money(record.positions_after[0].cost_basis) == "100.12345678"
    assert format_money(record.positions_after[0].quantity) == "1.00000000"


# ---------------------------------------------------------------------------
# `verify` failure branches
# ---------------------------------------------------------------------------


def _verifiable() -> ResultSnapshot:
    return ResultSnapshotBuilder().build(
        _run(), [_accepted_record(), _fill_record()], _utc("2025-11-28T14:35:00Z")
    )


def test_verify_rejects_a_value_that_is_not_a_result_snapshot() -> None:
    with pytest.raises(ResultIntegrityError, match="type is invalid"):
        ResultSnapshotBuilder.verify(object())  # type: ignore[arg-type]


def test_verify_rejects_records_that_are_not_canonically_ordered() -> None:
    result = _verifiable()

    with pytest.raises(ResultIntegrityError, match="canonically ordered"):
        ResultSnapshotBuilder.verify(replace(result, records=tuple(reversed(result.records))))


def test_verify_rejects_duplicated_record_and_fill_identity() -> None:
    result = _verifiable()
    accepted, fill = result.records

    with pytest.raises(ResultIntegrityError, match="record identity"):
        ResultSnapshotBuilder.verify(replace(result, records=(accepted, accepted)))

    twin = replace(fill, record_id="00000000-0000-4000-8000-000000000816")
    with pytest.raises(ResultIntegrityError, match="fill identity"):
        ResultSnapshotBuilder.verify(replace(result, records=(fill, twin)))


def test_verify_rejects_a_record_from_a_different_run_snapshot() -> None:
    result = _verifiable()
    foreign = replace(result.records[0], run_snapshot_id="b" * 64)

    with pytest.raises(ResultIntegrityError, match="record run snapshot"):
        ResultSnapshotBuilder.verify(replace(result, records=(foreign, result.records[1])))


def test_verify_rejects_a_completion_instant_that_precedes_a_detail_record() -> None:
    result = _verifiable()
    early = replace(result.manifest, completed_at=_utc("2025-11-28T14:30:30Z"))

    with pytest.raises(ResultIntegrityError, match="completion precedes"):
        ResultSnapshotBuilder.verify(replace(result, manifest=early))


def test_verify_rejects_content_that_is_not_bytes_or_has_the_wrong_size() -> None:
    result = _verifiable()

    with pytest.raises(ResultIntegrityError, match="must be bytes"):
        ResultSnapshotBuilder.verify(replace(result, object_bytes="{}"))  # type: ignore[arg-type]
    with pytest.raises(ResultIntegrityError, match="byte size"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, byte_size=result.manifest.byte_size + 1))
        )


def test_verify_rejects_a_record_count_that_disagrees_with_the_content() -> None:
    result = _verifiable()

    with pytest.raises(ResultIntegrityError, match="record count"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, record_count=99))
        )


def test_verify_rejects_a_manifest_bound_to_the_wrong_run_or_strategy() -> None:
    result = _verifiable()

    with pytest.raises(ResultIntegrityError, match="manifest run snapshot"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, run_snapshot_id="b" * 64))
        )
    with pytest.raises(ResultIntegrityError, match="manifest backtest run"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, backtest_run_id=OTHER_ORDER_ID))
        )
    with pytest.raises(ResultIntegrityError, match="manifest strategy version"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, strategy_version_id=OTHER_ORDER_ID))
        )


def test_verify_rejects_a_summary_hash_object_key_or_manifest_id_that_was_swapped() -> None:
    result = _verifiable()
    other = ResultSnapshotBuilder().build(_run(), [_accepted_record()], _utc("2025-11-28T14:35:00Z"))

    with pytest.raises(ResultIntegrityError, match="summary hash"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, summary_hash="c" * 64))
        )
    with pytest.raises(ResultIntegrityError, match="object key"):
        ResultSnapshotBuilder.verify(
            replace(result, manifest=replace(result.manifest, object_key="backtest-results/elsewhere.json"))
        )
    with pytest.raises(ResultIntegrityError, match="manifest identity"):
        ResultSnapshotBuilder.verify(
            replace(
                result,
                manifest=replace(result.manifest, result_manifest_id=other.manifest.result_manifest_id),
            )
        )


def test_tampered_object_or_summary_fails_closed() -> None:
    result = ResultSnapshotBuilder().build(
        _run(), [_accepted_record()], _utc("2025-11-28T14:35:00Z")
    )

    with pytest.raises(ResultIntegrityError, match="content"):
        ResultSnapshotBuilder.verify(replace(result, object_bytes=b"{}"))
    with pytest.raises(ResultIntegrityError, match="summary"):
        ResultSnapshotBuilder.verify(
            replace(
                result,
                summary=replace(
                    result.summary, total_fees=result.summary.total_fees + Decimal("1")
                ),
            )
        )


# ---------------------------------------------------------------------------
# record-level validation
# ---------------------------------------------------------------------------


def test_validate_fill_re_derives_gross_amount_from_price_and_quantity() -> None:
    with pytest.raises(ResultSnapshotValidationError, match="gross_amount"):
        # 100.05 * 10 = 1000.50, not 1000.00
        _record(
            kind=ResultRecordKind.FILL,
            record_id="00000000-0000-4000-8000-000000000851",
            occurred_at="2025-11-28T14:31:00Z",
            order_id=ORDER_ID,
            status=OrderStatus.FILLED,
            cash_after="8998",
            positions_after=(_position(),),
            fill_id=FILL_ID,
            quantity="10",
            base_price="100",
            price="100.05",
            gross_amount="1000.0000",
            slippage_amount="0.5000",
            fee="2.0010000",
            cost_basis="1000.5000",
            realized_pnl="0",
        )


def test_validate_fill_re_derives_slippage_from_base_and_final_price() -> None:
    with pytest.raises(ResultSnapshotValidationError, match="slippage_amount"):
        # |100.05 - 100| * 10 = 0.50, not 0.75
        _record(
            kind=ResultRecordKind.FILL,
            record_id="00000000-0000-4000-8000-000000000852",
            occurred_at="2025-11-28T14:31:00Z",
            order_id=ORDER_ID,
            status=OrderStatus.FILLED,
            cash_after="8998",
            positions_after=(_position(),),
            fill_id=FILL_ID,
            quantity="10",
            base_price="100",
            price="100.05",
            gross_amount="1000.5000",
            slippage_amount="0.7500",
            fee="2.0010000",
            cost_basis="1000.5000",
            realized_pnl="0",
        )


def test_validate_fill_accepts_a_sell_priced_below_the_base_price() -> None:
    # A sell slips *down*: |99.95 - 100| * 10 = 0.50 is still a positive cost.
    record = _record(
        kind=ResultRecordKind.FILL,
        record_id="00000000-0000-4000-8000-000000000853",
        occurred_at="2025-11-28T14:31:00Z",
        order_id=ORDER_ID,
        status=OrderStatus.FILLED,
        cash_after="10997.5",
        positions_after=(),
        fill_id=FILL_ID,
        quantity="10",
        base_price="100",
        price="99.95",
        gross_amount="999.5000",
        slippage_amount="0.5000",
        fee="2",
        cost_basis="1000",
        realized_pnl="-2.5",
    )

    assert record.slippage_amount == Decimal("0.5")


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (OrderStatus.ACCEPTED, "FILL order_status"),
        (OrderStatus.CANCELLED, "FILL order_status"),
    ],
)
def test_validate_fill_rejects_a_status_that_is_not_a_fill(status: OrderStatus, message: str) -> None:
    with pytest.raises(ResultSnapshotValidationError, match=message):
        replace(_fill_record(), order_status=status)


def test_rejects_mixed_snapshot_duplicate_event_and_incomplete_fill() -> None:
    accepted = _accepted_record()
    with pytest.raises(ResultSnapshotValidationError, match="run snapshot"):
        ResultSnapshotBuilder().build(
            replace(_run(), input_bundle_fingerprint="b" * 64),
            [accepted],
            _utc("2025-11-28T14:35:00Z"),
        )
    with pytest.raises(ResultSnapshotValidationError, match="record_id"):
        ResultSnapshotBuilder().build(
            _run(), [accepted, accepted], _utc("2025-11-28T14:35:00Z")
        )
    duplicate_fill = replace(
        _fill_record(),
        record_id="00000000-0000-4000-8000-000000000816",
    )
    with pytest.raises(ResultSnapshotValidationError, match="fill_id"):
        ResultSnapshotBuilder().build(
            _run(),
            [_fill_record(), duplicate_fill],
            _utc("2025-11-28T14:35:00Z"),
        )
    with pytest.raises(ResultSnapshotValidationError, match="fill fields"):
        _record(
            kind=ResultRecordKind.FILL,
            record_id="00000000-0000-4000-8000-000000000815",
            occurred_at="2025-11-28T14:31:00Z",
            order_id=ORDER_ID,
            status=OrderStatus.FILLED,
            cash_after="10000",
        )


def test_rejects_naive_times_duplicate_positions_and_fill_order_mismatch() -> None:
    with pytest.raises(ResultSnapshotValidationError, match="timezone-aware"):
        replace(_accepted_record(), occurred_at=datetime(2025, 11, 28, 14, 30))
    with pytest.raises(ResultSnapshotValidationError, match="instrument_id"):
        replace(
            _accepted_record(),
            positions_after=(_position(), _position()),
        )

    order = BacktestOrder(
        order_id=OTHER_ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        order_type=OrderType.MARKET,
        quantity=Decimal("10"),
        remaining_quantity=Decimal("0"),
        status=OrderStatus.FILLED,
        submitted_at=_utc("2025-11-28T14:30:00Z"),
        eligible_at=_utc("2025-11-28T14:31:00Z"),
        expires_at=_utc("2025-11-28T21:00:00Z"),
        reason_code=None,
    )
    fill = Fill(
        fill_id=FILL_ID,
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        side=OrderSide.BUY,
        quantity=Decimal("10"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("1000.5000"),
        slippage_amount=Decimal("0.5000"),
        fee=Decimal("2.0010000"),
        cost_basis=Decimal("1000.5000"),
        realized_pnl=Decimal("0"),
        occurred_at=_utc("2025-11-28T14:31:00Z"),
        ledger_transaction_id="00000000-0000-4000-8000-000000000809",
    )
    with pytest.raises(ResultSnapshotValidationError, match="order_id"):
        fill_result_record(_run(), fill, order, Decimal("8997"), (_position(),))
