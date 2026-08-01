from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.contracts import validate_backtest_result
from backtest_engine.execution_model import (
    BacktestOrder,
    Fill,
    OrderSide,
    OrderStatus,
    OrderType,
)
from backtest_engine.result_snapshot import (
    InMemoryResultSnapshotStore,
    PositionAfter,
    ResultIntegrityError,
    ResultRecord,
    ResultRecordKind,
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
    assert result.manifest.run_snapshot_id == _run().snapshot_id
    assert result.manifest.backtest_run_id == RUN_ID
    assert result.manifest.record_count == 4
    assert result.manifest.byte_size == len(result.object_bytes)
    assert result.manifest.object_key.endswith(
        f"/{result.manifest.content_hash}.json"
    )
    assert result.summary.order_count == 3
    assert result.summary.fill_count == 1
    assert result.summary.cancellation_count == 1
    assert result.summary.rejection_count == 1
    assert result.summary.total_fees == Decimal("2.0010000")
    assert result.summary.total_slippage == Decimal("0.5000")
    assert result.summary.realized_pnl == Decimal("0")
    assert result.summary.ending_cash == Decimal("8997.4990000")
    assert result.summary.ending_positions == (_position(),)

    fields = result.completion_fields()
    validate_backtest_result(
        {
            "contract_id": "com06.backtest-result",
            "schema_version": 1,
            "message_id": "00000000-0000-4000-8000-000000000821",
            "occurred_at": "2025-11-28T14:35:00Z",
            "correlation_id": "00000000-0000-4000-8000-000000000822",
            "idempotency_key": "d25-complete",
            "event_type": "BACKTEST_COMPLETE",
            "backtest_run_id": RUN_ID,
            "status": "COMPLETE",
            "completed_at": "2025-11-28T14:35:00Z",
            **fields,
        }
    )


def test_output_is_deterministic_regardless_of_record_input_order() -> None:
    records = [_accepted_record(), _fill_record()]
    builder = ResultSnapshotBuilder()

    first = builder.build(_run(), records, _utc("2025-11-28T14:35:00Z"))
    second = builder.build(
        _run(), list(reversed(records)), _utc("2025-11-28T14:35:00Z")
    )

    assert first == second


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


def test_d23_order_and_fill_adapters_preserve_costs_and_after_state() -> None:
    run = _run()
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

    record = fill_result_record(
        run,
        fill,
        order,
        Decimal("8997.4990000"),
        (_position(),),
    )

    assert record.kind is ResultRecordKind.FILL
    assert record.fill_id == fill.fill_id
    assert record.fee == fill.fee
    assert record.slippage_amount == fill.slippage_amount
    assert record.realized_pnl == fill.realized_pnl
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
