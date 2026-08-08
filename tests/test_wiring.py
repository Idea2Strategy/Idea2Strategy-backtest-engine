"""BT7: the adapters that satisfy the orchestrator's three unsatisfied Protocols.

These tests are Docker-free and drive the adapters directly. The end-to-end test
that puts them behind HTTP, SQS, PostgreSQL and an object store is
``tests/test_reproducibility_e2e.py``; what is pinned here is the behaviour that
would otherwise only be visible as "the digest moved".

Almost nothing is faked. The plan is B's published compiled plan loaded through
the real ``BasicPlanRuntime``, the execution engine wraps the real
``BacktestExecutionModel``, and the candidates are real
``elements.orders.OrderCandidate`` values -- unsized, exactly as the plan emits
them.
"""

from __future__ import annotations

import copy
from datetime import UTC, date, datetime
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace
from typing import Any

import pytest

from backtest_engine.attempt_coordinator import RunState
from backtest_engine.basic_runtime import (
    BAR_CLOSED_EVENT_TYPE,
    BasicPlanReplay,
    BasicPlanRuntime,
    derive_data_requirements,
)
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.data_availability import (
    AvailabilityStatus,
    DataAvailabilityAssessor,
    DataObservation,
    TimeInterval,
)
from backtest_engine.elements import SeriesBar, resolution_period
from backtest_engine.elements.orders import OrderCandidate
from backtest_engine.event_clock import MarketDataEvent, MarketEventClock
from backtest_engine.execution_model import BacktestExecutionModel, RiskLimits
from backtest_engine.orchestrator import (
    PlanReplay,
    ReplayOutcome,
    ReplayStatus,
    bar_events_from_table,
)
from backtest_engine.result_snapshot import ResultRecordKind, RunSnapshot
from backtest_engine.wiring import (
    COST_MODEL_VERSION,
    EXECUTION_MODEL_VERSION,
    BasicPlanReplayFactory,
    ExecutionModelEngine,
    JobNotSatisfiable,
    OrchestratorJobHandler,
    WiringError,
    dataset_coverage,
    evaluation_window,
)
from backtest_engine.worker import JobContext, JobResult
from d_reproducibility_testkit import (
    BAR,
    CLOSES,
    COMPLETED_AT,
    E2E_EXECUTION_POLICY,
    E2E_FRACTIONAL_POLICY,
    E2E_MICROSTRUCTURE,
    E2E_RISK_LIMITS,
    FIRST_BAR_START,
    INSTRUMENT_ID,
    compiled_plan,
    dataset_manifest,
    market_bars_parquet,
    write_market_data,
)


SESSION_CLOSE = datetime(2024, 1, 2, 21, 0, tzinfo=UTC)
INITIAL_CASH = Decimal("100000.00000000")


def _plan() -> Any:
    return BasicPlanRuntime().load(compiled_plan())


def _run_snapshot() -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id="55555555-5555-4555-8555-555555555555",
        strategy_version_id="00000000-0000-4000-8000-0000000000b1",
        input_bundle_fingerprint="a" * 64,
        calculation_model_version=E2E_EXECUTION_POLICY.calculation_model_version,
        cost_model_version=COST_MODEL_VERSION,
        execution_model_version=EXECUTION_MODEL_VERSION,
        initial_cash=INITIAL_CASH,
    )


def _engine(
    *,
    risk_limits: RiskLimits | None = None,
    initial_cash: Decimal = INITIAL_CASH,
) -> ExecutionModelEngine:
    return ExecutionModelEngine(
        model=BacktestExecutionModel(
            E2E_EXECUTION_POLICY,
            initial_cash,
            risk_limits or E2E_RISK_LIMITS,
            microstructure=E2E_MICROSTRUCTURE,
            fractional_policy=E2E_FRACTIONAL_POLICY,
        ),
        run_snapshot=_run_snapshot(),
        policy=E2E_EXECUTION_POLICY,
        fractional_policy=E2E_FRACTIONAL_POLICY,
    )


def _candidate(
    *,
    side: str = "BUY",
    allocation: Fraction | None = Fraction(1, 1),
    budget_cap_bps: int = 10000,
    reference_price: Decimal = Decimal("100.00000000"),
    decided_at: datetime = datetime(2024, 1, 2, 14, 45, tzinfo=UTC),
) -> OrderCandidate:
    return OrderCandidate(
        evaluation_id=f"eval-{decided_at.isoformat()}-{side}",
        instrument_id=INSTRUMENT_ID,
        partition_key="partition-1",
        flow_id="flow-1",
        side=side,
        order_type="MARKET",
        allocation=allocation,
        reference_price=reference_price,
        decided_at=decided_at,
        eligible_at=decided_at,
        session_date_et=date(2024, 1, 2),
        session_closes_at=SESSION_CLOSE,
        budget_cap_bps=budget_cap_bps,
    )


def _bar_event(index: int, *, resolution: str = "1m") -> MarketDataEvent:
    period = resolution_period(resolution)
    starts_at = FIRST_BAR_START + BAR * index
    close = Decimal(CLOSES[index])
    previous = Decimal(CLOSES[index - 1]) if index else close
    return MarketDataEvent(
        event_id=f"BAR:{INSTRUMENT_ID}:{starts_at.isoformat()}",
        instrument_id=INSTRUMENT_ID,
        occurred_at=starts_at + period,
        available_at=starts_at + period,
        source_sequence=index + 1,
        event_type=BAR_CLOSED_EVENT_TYPE,
        payload={
            "dataKind": "ADJUSTED_BAR",
            "resolution": resolution,
            "bar": SeriesBar(
                instrument_id=INSTRUMENT_ID,
                resolution=resolution,
                starts_at=starts_at,
                ends_at=starts_at + period,
                close=close,
                volume=Decimal(20_000),
            ),
            "providerSymbol": "AAPL",
            "open": previous,
            "high": max(previous, close),
            "low": min(previous, close),
        },
    )


# ==========================================================================
# PlanReplayFactory
# ==========================================================================


def test_replay_factory_produces_something_the_orchestrator_will_accept(tmp_path: Any) -> None:
    """The seam BT-a left open: a loaded plan bound to the run's clock."""
    fixture = write_market_data(tmp_path)
    plan = _plan()
    evaluation_from, evaluation_through = evaluation_window(fixture.manifest, plan)
    requirements = derive_data_requirements(
        plan, evaluation_from=evaluation_from, evaluation_through=evaluation_through
    )
    observation = DataObservation(
        requirement_id=requirements[0].requirement_id,
        instrument_id=INSTRUMENT_ID,
        data_kind="ADJUSTED_BAR",
        resolution="1m",
        available_intervals=(
            TimeInterval(FIRST_BAR_START, FIRST_BAR_START + BAR * len(CLOSES)),
        ),
        verified=True,
    )
    assessment = DataAvailabilityAssessor().assess(requirements, [observation])
    clock = MarketEventClock(
        XNYS_CALENDAR.session_schedule(date(2024, 1, 2), date(2024, 1, 2)),
        [_bar_event(index) for index in range(len(CLOSES))],
    )

    replay = BasicPlanReplayFactory(BasicPlanRuntime(), plan)(
        clock=clock, assessment=assessment
    )

    assert isinstance(replay, BasicPlanReplay)
    assert isinstance(replay, PlanReplay)
    # The plan really runs, and the shaped series produces exactly one candidate.
    evaluations = replay.run()
    decided = [item.occurred_at for item in evaluations if item.candidates]
    assert decided == [datetime(2024, 1, 2, 14, 45, tzinfo=UTC)]


# ==========================================================================
# ExecutionEngine: sizing
# ==========================================================================


def test_the_engine_sizes_a_candidate_that_carries_no_quantity() -> None:
    """The whole point of the seam: the plan says *how much of the budget*.

    100000 cash, less the seeded 50bp buying-power buffer, is 99500 of buying
    power. A share costs 100 + 5bp slippage + 20bp fee = 100.2501, and 99500
    buys 992 whole shares with 25.24 left over that cannot buy a 993rd.
    """
    engine = _engine()
    candidate = _candidate()

    assert not hasattr(candidate, "quantity")
    order_id = engine.place(candidate)

    assert order_id is not None
    assert [record.kind for record in engine.records] == [ResultRecordKind.ORDER]
    assert engine.summary().positions == {INSTRUMENT_ID: Decimal(0)}
    placed = engine.records[0]
    assert placed.order_id == order_id
    assert placed.cash_after == INITIAL_CASH


@pytest.mark.parametrize(
    ("allocation", "budget_cap_bps", "expected_notional"),
    [
        # 992 shares reserved at the slipped price 100.05.
        (Fraction(1, 1), 10000, Decimal("99249.60000000")),
        # Half the budget: 49750 / 100.2501 -> 496 shares.
        (Fraction(1, 2), 10000, Decimal("49624.80000000")),
        # 25% of the budget by cap rather than by allocation: 24875 -> 248 shares.
        (Fraction(1, 1), 2500, Decimal("24812.40000000")),
    ],
)
def test_allocation_and_budget_cap_both_move_the_size(
    allocation: Fraction, budget_cap_bps: int, expected_notional: Decimal
) -> None:
    """Two independent inputs; a sizing rule that ignored either would tie."""
    engine = _engine()

    order_id = engine.place(
        _candidate(allocation=allocation, budget_cap_bps=budget_cap_bps)
    )

    assert order_id is not None
    # The reservation is the sized notional at the slipped reference price, which
    # is what the fill then consumes.
    settled = engine.settle(_bar_event(15))
    assert settled == 1
    fill = engine.records[-1]
    assert fill.kind is ResultRecordKind.FILL
    assert fill.gross_amount == expected_notional


def test_a_budget_that_cannot_buy_one_whole_share_is_declined_not_ordered() -> None:
    """`place` returns None and no order request is created at all.

    A 1bp cap on 99500 of buying power is 9.95, which does not reach one 100.2501
    share. Submitting a zero-quantity order would be a contract violation; the
    engine declines instead, and the orchestrator's step log records the
    candidate that produced nothing.
    """
    engine = _engine()

    assert engine.place(_candidate(budget_cap_bps=1)) is None
    assert engine.records == ()
    assert engine.declined_candidates == ("eval-2024-01-02T14:45:00+00:00-BUY",)


def test_a_refused_order_is_still_recorded_as_evidence() -> None:
    """A risk rejection is a result record, not a silent no-op."""
    engine = _engine(
        risk_limits=RiskLimits(
            max_strategy_notional=Decimal("1000000.00000000"),
            max_gross_exposure=Decimal("1000000.00000000"),
            max_instrument_exposure=Decimal("1.00000000"),
        )
    )

    assert engine.place(_candidate()) is None
    assert [record.kind for record in engine.records] == [ResultRecordKind.REJECTION]
    assert engine.records[0].reason_code == "INSTRUMENT_EXPOSURE_EXCEEDED"
    assert engine.declined_candidates == ()


def test_a_sell_candidate_is_sized_from_the_held_position() -> None:
    """A disposal carries no allocation; its size is what the run actually holds."""
    engine = _engine()
    engine.place(_candidate())
    engine.settle(_bar_event(15))

    held = engine.summary().positions[INSTRUMENT_ID]
    sell = _candidate(
        side="SELL",
        allocation=None,
        decided_at=datetime(2024, 1, 2, 14, 47, tzinfo=UTC),
    )
    order_id = engine.place(sell)

    assert held == Decimal(992)
    assert order_id is not None
    assert engine.settle(_bar_event(17)) == 1
    assert engine.summary().positions[INSTRUMENT_ID] == Decimal(0)


def test_a_buy_candidate_without_an_allocation_is_a_wiring_error() -> None:
    engine = _engine()

    with pytest.raises(WiringError, match="allocation share"):
        engine.place(_candidate(allocation=None))


# ==========================================================================
# ExecutionEngine: settlement
# ==========================================================================


def test_settlement_translates_the_orchestrator_event_into_a_matching_bar() -> None:
    engine = _engine()
    engine.place(_candidate())

    assert engine.settle(_bar_event(15)) == 1
    fill = engine.records[-1]
    assert fill.kind is ResultRecordKind.FILL
    assert fill.occurred_at == datetime(2024, 1, 2, 14, 46, tzinfo=UTC)
    # base 100.00 (the bar's open) plus 5bp slippage.
    assert fill.base_price == Decimal("100.00000000")
    assert fill.price == Decimal("100.05000000")
    assert fill.quantity == Decimal("992.00000000")
    assert fill.fee == Decimal("198.49920000")

    summary = engine.summary()
    assert summary.fill_count == 1
    # 100000 - 99249.60 gross - 198.4992 fee.
    assert summary.cash == Decimal("551.90080000")
    assert summary.ledger_entry_count == 3


@pytest.mark.parametrize("resolution", ["30m", "1h", "4h", "1d"])
def test_settlement_accepts_each_backtest_market_data_resolution(resolution: str) -> None:
    engine = _engine()
    engine.place(_candidate())

    assert engine.settle(_bar_event(15, resolution=resolution)) == 1


def test_settlement_never_fills_an_order_on_the_bar_that_decided_it() -> None:
    engine = _engine()
    engine.place(_candidate())

    # Bar 14 closes at 14:45, the decision instant. It must not fill.
    assert engine.settle(_bar_event(14)) == 0
    assert engine.settle(_bar_event(15)) == 1


def test_events_built_by_the_orchestrator_are_settleable(tmp_path: Any) -> None:
    """Guards the payload contract between `bar_events_from_table` and the engine."""
    import pyarrow.parquet as pq

    fixture = write_market_data(tmp_path)
    table = pq.read_table(fixture.path)
    events = bar_events_from_table(table, data_kind="ADJUSTED_BAR", resolution="1m")
    engine = _engine()
    engine.place(_candidate())

    fills = sum(engine.settle(event) for event in events)

    assert len(events) == len(CLOSES)
    assert fills == 1


def test_cancelled_replay_is_published_and_acknowledged_as_cancelled() -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []

        def publish(self, event: Any, *, delivery_attempt: int) -> None:
            self.events.append(dict(event) | {"deliveryAttempt": delivery_attempt})

    sink = Sink()
    handler = object.__new__(OrchestratorJobHandler)
    handler._sink = sink
    handler._wall_clock = lambda: COMPLETED_AT
    envelope = SimpleNamespace(
        run_id="55555555-5555-4555-8555-555555555555",
        bot_id="00000000-0000-4000-8000-0000000000b1",
        owner_account_id="66666666-6666-4666-8666-666666666666",
        expected_snapshot_hash="sha256:" + "1" * 64,
        input_bundle_fingerprint="sha256:" + "2" * 64,
        execution_policy_version="official-backtest-policy-v1",
    )
    binding = SimpleNamespace(
        envelope=envelope,
        correlation_id="77777777-7777-4777-8777-777777777777",
    )
    context = JobContext("execution-key", 2, 3, "message-1", "worker-1")
    outcome = ReplayOutcome(
        run_id=str(envelope.run_id),
        status=ReplayStatus.CANCELLED,
        availability_status=AvailabilityStatus.AVAILABLE,
        steps=(),
        replay_digest="",
        reason_code="USER_CANCELLED",
    )

    reported = handler._report(
        binding,
        outcome,
        SimpleNamespace(state=RunState.CANCELLED),
        context,
    )

    assert reported.result is JobResult.CANCELLED
    assert sink.events[0]["status"] == "CANCELLED"
    assert sink.events[0]["metadata"]["messageType"] == "BACKTEST_CANCELLED"
    assert sink.events[0]["reasonCode"] == "USER_CANCELLED"
    assert sink.events[0]["deliveryAttempt"] == 3


# ==========================================================================
# The evaluation window comes from the pinned dataset
# ==========================================================================


def test_the_pinned_dataset_coverage_decides_the_evaluation_window() -> None:
    """Warm-up is consumed from the front of the delivered coverage."""
    parquet_bytes = market_bars_parquet()
    manifest = dataset_manifest(
        __import__("hashlib").sha256(parquet_bytes).hexdigest(),
        row_count=len(CLOSES),
        coverage_end=FIRST_BAR_START + BAR * len(CLOSES),
    )

    assert dataset_coverage(manifest) == (
        FIRST_BAR_START,
        FIRST_BAR_START + BAR * len(CLOSES),
    )
    # RSI_14 needs 15 completed 1m bars, so evaluation starts 15 minutes in.
    assert evaluation_window(manifest, _plan()) == (
        datetime(2024, 1, 2, 14, 45, tzinfo=UTC),
        datetime(2024, 1, 2, 14, 50, tzinfo=UTC),
    )


def test_a_dataset_shorter_than_the_warmup_is_permanently_unsatisfiable() -> None:
    """Retrying cannot conjure history, so the worker must not loop on it."""
    short = CLOSES[:10]
    parquet_bytes = market_bars_parquet(short)
    manifest = dataset_manifest(
        __import__("hashlib").sha256(parquet_bytes).hexdigest(),
        row_count=len(short),
        coverage_end=FIRST_BAR_START + BAR * len(short),
    )

    with pytest.raises(JobNotSatisfiable) as failure:
        evaluation_window(manifest, _plan())

    assert failure.value.reason_code == "REQUIRED_DATA_UNAVAILABLE"


def test_dataset_coverage_reads_the_objects_not_the_dataset_window() -> None:
    """The manifest window is a whole quarter; the objects are one session."""
    parquet_bytes = market_bars_parquet()
    manifest = dataset_manifest(
        __import__("hashlib").sha256(parquet_bytes).hexdigest(),
        row_count=len(CLOSES),
        coverage_end=FIRST_BAR_START + BAR * len(CLOSES),
    )
    widened = copy.deepcopy(manifest)

    assert widened["period_start"] == "2024-01-01T05:00:00Z"
    assert widened["period_end"] == "2024-04-01T04:00:00Z"
    assert dataset_coverage(widened)[1] == FIRST_BAR_START + BAR * len(CLOSES)


def test_the_pinned_completion_instant_follows_every_replay_instant() -> None:
    """Guards the fixture: `completed_at` must not precede a result record."""
    assert COMPLETED_AT > FIRST_BAR_START + BAR * len(CLOSES)
