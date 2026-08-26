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
import logging
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal, localcontext
from fractions import Fraction
from types import SimpleNamespace
from typing import Any

import pytest

from backtest_engine.attempt_coordinator import (
    AttemptCoordinator,
    AttemptFailure,
    AttemptPolicy,
    FailureKind,
    ResourceSample,
    RunState,
)
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
from backtest_engine.result_snapshot import ResultRecordKind, ResultSnapshotBuilder, RunSnapshot
from backtest_engine.wiring import (
    COST_MODEL_VERSION,
    EXECUTION_MODEL_VERSION,
    BasicPlanReplayFactory,
    ExecutionModelEngine,
    JobEnvelope,
    JobNotSatisfiable,
    OrchestratorJobHandler,
    WiringError,
    _CancellationAwareMonitor,
    _metric_percent,
    dataset_coverage,
    evaluation_window,
    segmented_dataset_coverage,
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
    instrument_id: str = INSTRUMENT_ID,
    side: str = "BUY",
    allocation: Fraction | None = Fraction(1, 1),
    budget_cap_bps: int = 10000,
    max_position_percent: Decimal = Decimal("100"),
    reference_price: Decimal = Decimal("100.00000000"),
    decided_at: datetime = datetime(2024, 1, 2, 14, 45, tzinfo=UTC),
) -> OrderCandidate:
    return OrderCandidate(
        evaluation_id=f"eval-{decided_at.isoformat()}-{side}",
        instrument_id=instrument_id,
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
        max_position_percent=max_position_percent,
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


def test_per_instrument_cap_counts_filled_and_reserved_buy_exposure() -> None:
    engine = _engine()
    first = _candidate(
        budget_cap_bps=2500,
        max_position_percent=Decimal("40"),
    )
    assert engine.place(first) is not None
    assert engine.settle(_bar_event(15)) == 1

    second = _candidate(
        budget_cap_bps=2500,
        max_position_percent=Decimal("40"),
        decided_at=datetime(2024, 1, 2, 14, 47, tzinfo=UTC),
    )
    assert engine.place(second) is not None

    third = _candidate(
        budget_cap_bps=2500,
        max_position_percent=Decimal("40"),
        decided_at=datetime(2024, 1, 2, 14, 48, tzinfo=UTC),
    )
    assert engine.place(third) is None
    assert engine.records[-1].kind is ResultRecordKind.REJECTION
    assert engine.records[-1].reason_code == "MAX_INSTRUMENT_POSITION_PERCENT"

    assert engine.settle(_bar_event(17)) >= 1
    position = engine.summary().positions[INSTRUMENT_ID]
    latest_fill_price = engine.records[-1].price
    assert latest_fill_price is not None
    assert position * latest_fill_price <= Decimal("40000.00000000")


def test_position_caps_are_isolated_between_instruments() -> None:
    engine = _engine()
    other_instrument = "00000000-0000-4000-8000-000000000302"

    assert engine.place(_candidate(
        budget_cap_bps=2000,
        max_position_percent=Decimal("20"),
    )) is not None
    assert engine.place(_candidate(
        instrument_id=other_instrument,
        budget_cap_bps=2000,
        max_position_percent=Decimal("20"),
        decided_at=datetime(2024, 1, 2, 14, 47, tzinfo=UTC),
    )) is not None


def test_a_sell_candidate_is_sized_from_the_held_position() -> None:
    """A disposal carries no allocation; its size is what the run actually holds."""
    engine = _engine()
    engine.place(_candidate())
    engine.settle(_bar_event(15))

    held = engine.summary().positions[INSTRUMENT_ID]
    sell = _candidate(
        side="SELL",
        allocation=None,
        max_position_percent=Decimal("1"),
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


def test_durable_worker_cancellation_is_applied_at_the_next_replay_checkpoint() -> None:
    policy = AttemptPolicy(
        max_attempts=3,
        lease_duration=timedelta(seconds=30),
        attempt_timeout=timedelta(minutes=10),
        max_cpu_time=timedelta(minutes=5),
        max_memory_bytes=512 * 1024 * 1024,
    )
    coordinator = AttemptCoordinator("cancel-run", policy, COMPLETED_AT)
    lease = coordinator.acquire("worker-1", COMPLETED_AT)
    delegate = SimpleNamespace(sample=lambda: ResourceSample(
        cpu_time=timedelta(seconds=1), memory_bytes=1024
    ))
    monitor = _CancellationAwareMonitor(
        delegate, coordinator, lambda: "USER_CANCELLED",
        lambda: COMPLETED_AT + timedelta(seconds=1),
    )

    sample = monitor.sample()
    with pytest.raises(AttemptFailure) as raised:
        coordinator.heartbeat(lease, COMPLETED_AT + timedelta(seconds=1), sample)

    assert raised.value.kind is FailureKind.CANCELLED
    assert coordinator.state is RunState.CANCELLED


def _terminal_failure_envelope() -> JobEnvelope:
    return JobEnvelope(
        run_id=uuid.UUID("55555555-5555-4555-8555-555555555555"),
        bot_id=uuid.UUID("00000000-0000-4000-8000-0000000000b1"),
        owner_account_id=uuid.UUID("66666666-6666-4666-8666-666666666666"),
        idempotency_key="terminal-publish-failure",
        input_bundle_id=uuid.UUID("00000000-0000-4000-8000-0000000000b2"),
        input_bundle_fingerprint="sha256:" + "1" * 64,
        execution_policy_version="official-backtest-policy-v1",
        compiled_plan_checksum="sha256:" + "2" * 64,
        dataset_manifest_id=uuid.UUID("00000000-0000-4000-8000-0000000000b3"),
        expected_dataset_hash="sha256:" + "3" * 64,
        expected_snapshot_hash="sha256:" + "4" * 64,
        datasets=(),
        feature_materializations=(),
        evaluation_period_id=None,
        input_set_hash=None,
    )


def _terminal_failure_handler() -> OrchestratorJobHandler:
    handler = object.__new__(OrchestratorJobHandler)
    handler._wall_clock = lambda: COMPLETED_AT
    handler._correlation_id = "77777777-7777-4777-8777-777777777777"

    def unsatisfiable(*_args: Any) -> None:
        raise JobNotSatisfiable("compiled plan is not resolvable", reason_code="REQUIRED_INPUT_UNAVAILABLE")

    handler.bind = unsatisfiable  # type: ignore[method-assign]
    return handler


def test_unsatisfiable_binding_publishes_the_terminal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Sink:
        def __init__(self) -> None:
            self.events: list[dict[str, Any]] = []

        def publish(self, event: Any, *, delivery_attempt: int) -> None:
            self.events.append(dict(event) | {"deliveryAttempt": delivery_attempt})

    envelope = _terminal_failure_envelope()
    sink = Sink()
    handler = _terminal_failure_handler()
    handler._sink = sink
    context = JobContext("execution-key", 1, 1, "message-1", "worker-1")
    monkeypatch.setattr(JobEnvelope, "parse", classmethod(lambda _cls, _job: envelope))

    outcome = handler({}, context)

    assert outcome.result is JobResult.PERMANENT_FAILURE
    assert outcome.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert sink.events[0]["status"] == "FAILED"
    assert sink.events[0]["failureCode"] == "REQUIRED_INPUT_UNAVAILABLE"
    assert sink.events[0]["retryable"] is False
    assert sink.events[0]["metadata"]["correlationId"] == "77777777-7777-4777-8777-777777777777"


def test_terminal_binding_failure_survives_its_failure_event_publish_failing(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = _terminal_failure_envelope()
    handler = _terminal_failure_handler()

    def unavailable_sink(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("result sink unavailable")

    handler._publish = unavailable_sink  # type: ignore[method-assign]
    context = JobContext("execution-key", 1, 1, "message-1", "worker-1")
    monkeypatch.setattr(JobEnvelope, "parse", classmethod(lambda _cls, _job: envelope))

    with caplog.at_level(logging.ERROR):
        outcome = handler({}, context)

    assert outcome.result is JobResult.PERMANENT_FAILURE
    assert outcome.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert "result sink unavailable" in caplog.text
    assert "REQUIRED_INPUT_UNAVAILABLE" in caplog.text


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


def test_adjacent_manifests_of_the_same_resolution_form_one_cover() -> None:
    first = dataset_manifest("1" * 64, row_count=1, coverage_end=FIRST_BAR_START + BAR)
    second = dataset_manifest("2" * 64, row_count=1, coverage_end=FIRST_BAR_START + BAR)
    for manifest in (first, second):
        manifest["resolution"] = "1h"
    first["objects"][0]["period_start"] = "2024-01-01T00:00:00Z"
    first["objects"][0]["period_end"] = "2025-01-01T00:00:00Z"
    second["objects"][0]["period_start"] = "2025-01-01T00:00:00Z"
    second["objects"][0]["period_end"] = "2026-01-01T00:00:00Z"

    assert segmented_dataset_coverage((second, first)) == (
        datetime(2024, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_segmented_manifest_cover_rejects_a_gap() -> None:
    first = dataset_manifest("1" * 64, row_count=1, coverage_end=FIRST_BAR_START + BAR)
    second = dataset_manifest("2" * 64, row_count=1, coverage_end=FIRST_BAR_START + BAR)
    for manifest in (first, second):
        manifest["resolution"] = "4h"
    first["objects"][0]["period_start"] = "2024-01-01T00:00:00Z"
    first["objects"][0]["period_end"] = "2025-01-01T00:00:00Z"
    second["objects"][0]["period_start"] = "2025-02-01T00:00:00Z"
    second["objects"][0]["period_end"] = "2026-01-01T00:00:00Z"

    with pytest.raises(JobNotSatisfiable, match="gap"):
        segmented_dataset_coverage((first, second))


def test_the_pinned_completion_instant_follows_every_replay_instant() -> None:
    """Guards the fixture: `completed_at` must not precede a result record."""
    assert COMPLETED_AT > FIRST_BAR_START + BAR * len(CLOSES)


# ==========================================================================
# Position metrics published to the strategy -- live parity
# ==========================================================================


STRATEGY_RESOLUTION = "30m"
"""``holdingBars`` is published for the four strategy clocks only, never a display bar."""


def _holding_bars(engine: ExecutionModelEngine, index: int) -> str:
    """The ``holdingBars`` value the strategy reads at the bar with this index."""
    event = _bar_event(index, resolution=STRATEGY_RESOLUTION)
    values = engine.runtime_values(event.occurred_at, (event,))
    return values[INSTRUMENT_ID][f"position.holdingBars.{STRATEGY_RESOLUTION}"]


def _settle(engine: ExecutionModelEngine, index: int) -> int:
    return engine.settle(_bar_event(index, resolution=STRATEGY_RESOLUTION))


def test_engine_builds_the_official_daily_valuation_grid_from_market_instants() -> None:
    engine = _engine()
    engine.place(_candidate())
    first = _bar_event(15, resolution=STRATEGY_RESOLUTION)
    later_same_session = _bar_event(16, resolution=STRATEGY_RESOLUTION)
    engine.settle(first)
    engine.runtime_values(first.occurred_at, (first,))
    engine.runtime_values(later_same_session.occurred_at, (later_same_session,))

    series = engine.valuation_series

    assert series is not None
    assert series.opening_at == first.occurred_at - timedelta(microseconds=1)
    assert [instant.as_of for instant in series.instants] == [later_same_session.occurred_at]
    assert series.instants[0].marks[0].price == Decimal(CLOSES[16])
    assert series.periodicity.value == "DAILY"
    assert series.basis.value == "MARK_TO_MARKET"


def test_same_bar_buy_and_sell_records_preserve_each_fill_position_transition() -> None:
    """A bar can fill more than one open order; each record needs its own after-state."""
    engine = _engine()
    engine.place(_candidate(allocation=Fraction(1, 2)))
    _settle(engine, 15)

    engine.place(
        _candidate(
            allocation=Fraction(3, 1000),
            decided_at=datetime(2024, 1, 2, 14, 46, tzinfo=UTC),
        )
    )
    engine.place(
        _candidate(
            side="SELL",
            allocation=None,
            decided_at=datetime(2024, 1, 2, 14, 47, tzinfo=UTC),
        )
    )

    assert _settle(engine, 17) == 2
    fills = [record for record in engine.records if record.kind is ResultRecordKind.FILL]
    assert len(fills) == 3
    before_quantity = fills[0].positions_after[0].quantity
    assert fills[1].positions_after[0].quantity > before_quantity
    assert fills[2].positions_after[0].quantity < fills[1].positions_after[0].quantity
    ResultSnapshotBuilder().build(_run_snapshot(), engine.records, COMPLETED_AT)


def test_the_entry_bar_publishes_one_held_bar() -> None:
    """Live counts the entry bar itself, so the backtest must not start at zero.

    ``EvaluatingBotRuntime`` builds a fresh ``PositionTracker`` for the new position and then
    counts the bar that just closed, publishing 1. Zeroing the counter after counting instead
    left every later bar one behind, so an "N bars held" exit fired a bar late in the backtest
    and on time in production.
    """
    engine = _engine()
    engine.place(_candidate())
    _settle(engine, 15)

    assert _holding_bars(engine, 15) == "1"


def test_each_later_bar_adds_one_held_bar() -> None:
    engine = _engine()
    engine.place(_candidate())
    _settle(engine, 15)
    _holding_bars(engine, 15)

    assert [_holding_bars(engine, index) for index in (16, 17)] == ["2", "3"]


def test_re_reading_the_same_bar_does_not_count_it_twice() -> None:
    engine = _engine()
    engine.place(_candidate())
    _settle(engine, 15)

    assert [_holding_bars(engine, 15) for _ in range(3)] == ["1", "1", "1"]


def test_a_new_position_restarts_the_count_at_one() -> None:
    """A re-entry is a new cycle, and its first held bar is again bar one."""
    engine = _engine()
    engine.place(_candidate())
    _settle(engine, 15)
    _holding_bars(engine, 15)
    _holding_bars(engine, 16)

    engine.place(_candidate(side="SELL", allocation=None,
                            decided_at=datetime(2024, 1, 2, 14, 47, tzinfo=UTC)))
    _settle(engine, 17)
    assert engine.summary().positions[INSTRUMENT_ID] == Decimal(0)

    # Bar 19 opens at 14:49, after this decision, so it is the bar that may fill it.
    engine.place(_candidate(decided_at=datetime(2024, 1, 2, 14, 48, tzinfo=UTC)))

    assert _settle(engine, 19) == 1
    assert _holding_bars(engine, 19) == "1"


def test_a_published_position_metric_rounds_half_even() -> None:
    """precision:1.0.0 names the rounding, so an exact tie cannot go two ways.

    The live runtime rounded these HALF_UP, so one position published two different return
    percentages depending on which runtime computed it -- and the rendered form reaches the
    step trace a POSITION_RETURN threshold is judged from.
    """
    # 0.00000000025 / 1 is 0.000000025 percent: an exact tie at the ninth decimal whose
    # eighth digit is even, which is the only place the two modes disagree.
    assert _metric_percent(Decimal("0.00000000025"), Decimal("1")) == Decimal("0.00000002")


def test_a_published_position_metric_ignores_the_ambient_rounding_mode() -> None:
    """Quantizing without naming a mode would inherit whatever the process last set."""
    with localcontext() as context:
        context.rounding = ROUND_HALF_UP

        assert _metric_percent(Decimal("0.00000000025"), Decimal("1")) == Decimal("0.00000002")


def test_a_zero_denominator_publishes_zero_rather_than_raising() -> None:
    assert _metric_percent(Decimal("1"), Decimal("0")) == Decimal("0")
