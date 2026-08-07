"""BT4 / D18: the orchestrator that actually assembles and runs a backtest.

Before this card the only assembly code in the repository lived inside
``tests/d_reproducibility_testkit.py``. These tests pin the promoted ``src``
orchestrator against behaviour the testkit never had.

Almost nothing here is a fake. The tests drive the **real**
:class:`MarketEventClock`, the **real** :class:`DataAvailabilityAssessor`, the
**real** :class:`BasicPlanReplay` and :class:`ExecutionGate` from BT-a, the
**real** ``OrderCandidate`` dataclass, the **real** ``XNYS_CALENDAR`` and the
**real** :class:`ParquetMarketDataReader` over bytes on disk. Only the plan's
condition evaluators (a stub runtime, so the fixture does not have to carry a
checksum-signed compiled plan), the execution model and the result publisher
are stood in for -- and those are exactly the seams owned by BT-b and BT-c.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.attempt_coordinator import (
    AttemptCoordinator,
    AttemptPolicy,
    AttemptState,
    FailureKind,
    ResourceSample,
    RunState,
)
from backtest_engine.basic_runtime import (
    BAR_CLOSED_EVENT_TYPE as RUNTIME_BAR_CLOSED_EVENT_TYPE,
)
from backtest_engine.basic_runtime import BasicPlanReplay, ExecutionGate
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.contracts import canonical_dataset_hash
from backtest_engine.data_availability import (
    AvailabilityAssessment,
    AvailabilityStatus,
    DataRequirement,
)
from backtest_engine.elements import InstrumentInput
from backtest_engine.elements.orders import OrderCandidate
from backtest_engine.event_clock import MarketDataEvent, MarketEventClock, MarketSessionStatus
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.market_data import ParquetMarketDataReader
from backtest_engine.orchestrator import (
    BAR_CLOSED_EVENT_TYPE,
    ORCHESTRATOR_VERSION,
    BacktestJob,
    BacktestOrchestrator,
    ExecutionSummary,
    PlanReplay,
    PublishedManifests,
    PublishRequest,
    ReplayOutcome,
    ReplayStatus,
    ResultPublicationError,
    bar_events_from_table,
)


AAPL = "11111111-1111-4111-8111-111111111111"
MSFT = "22222222-2222-4222-8222-222222222222"
ABSENT = "33333333-3333-4333-8333-333333333333"

RUN_ID = "55555555-5555-4555-8555-555555555555"
STORAGE_OBJECT_ID = "44444444-4444-4444-8444-444444444444"
MANIFEST_ID = "66666666-6666-4666-8666-666666666666"
DATASET_ID = "77777777-7777-4777-8777-777777777777"
RESULT_MANIFEST_ID = "88888888-8888-4888-8888-888888888888"
DETAIL_MANIFEST_ID = "99999999-9999-4999-8999-999999999999"

PERIOD_START = "2024-01-01T05:00:00Z"
PERIOD_END = "2024-04-01T04:00:00Z"

DATA_KIND = "BAR"
RESOLUTION = "15m"
BAR_INTERVAL = timedelta(minutes=15)

# Wall-clock (attempt lease) time. Deliberately unrelated to the 2024 replay
# clock: conflating the two is exactly the bug that separation prevents.
WALL_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _utc(hour: int, minute: int) -> datetime:
    return datetime(2024, 1, 2, hour, minute, tzinfo=timezone.utc)


#: ``bar_start_at`` UTC, instrument, symbol, open/high/low/close/volume.
#: MSFT is missing its 14:45 bar on purpose: that hole is what turns the
#: availability assessment DEGRADED and makes the 14:45 instant skip.
BAR_ROWS: tuple[tuple[datetime, str, str, float, float, float, float, int], ...] = (
    (_utc(14, 30), AAPL, "AAPL", 100.0, 102.0, 99.0, 101.0, 1000),
    (_utc(14, 30), MSFT, "MSFT", 200.0, 202.0, 199.0, 201.0, 2000),
    (_utc(14, 45), AAPL, "AAPL", 101.0, 103.0, 100.0, 102.0, 1200),
    (_utc(15, 0), AAPL, "AAPL", 102.0, 104.0, 101.0, 103.0, 1300),
    (_utc(15, 0), MSFT, "MSFT", 201.0, 203.0, 200.0, 202.0, 2100),
)

#: Bar close instants, which are the replay's evaluation instants.
FIRST, SECOND, THIRD = _utc(14, 45), _utc(15, 0), _utc(15, 15)

_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("provider_symbol", pa.string(), nullable=False),
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("session_date_et", pa.date32(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
    ],
    metadata={b"schema_version": b"market-bars-v2"},
)


def write_bars(path: Path) -> None:
    rows = [
        {
            "instrument_id": instrument_id,
            "provider_symbol": symbol,
            "bar_start_at": starts_at,
            "session_date_et": date(2024, 1, 2),
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
        for starts_at, instrument_id, symbol, open_, high, low, close, volume in BAR_ROWS
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), path, version="2.6")


def _bar_row(starts_at: datetime, session_date: date) -> dict[str, Any]:
    return {
        "instrument_id": AAPL,
        "provider_symbol": "AAPL",
        "bar_start_at": starts_at,
        "session_date_et": session_date,
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 101.0,
        "volume": 1000,
    }


def test_daily_bars_close_on_the_official_session_and_signal_the_next_session() -> None:
    schedule = XNYS_CALENDAR.session_schedule(date(2024, 1, 2), date(2024, 1, 3))
    table = pa.Table.from_pylist(
        [
            _bar_row(_utc(5, 0), date(2024, 1, 2)),
            _bar_row(datetime(2024, 1, 3, 5, 0, tzinfo=timezone.utc), date(2024, 1, 3)),
        ],
        schema=_SCHEMA,
    )
    events = bar_events_from_table(
        table,
        data_kind=DATA_KIND,
        resolution="1d",
        schedule=schedule,
    )

    first_session, second_session = schedule.sessions
    first_bar = events[0].payload["bar"]
    assert first_bar.starts_at == first_session.opens_at
    assert first_bar.ends_at == first_session.closes_at
    assert first_bar.session_truncated is True

    runtime = StubRuntime(buy_at=first_session.closes_at)
    replay = BasicPlanReplay(
        runtime=runtime,
        plan=StubPlan(),
        clock=MarketEventClock(schedule, events),
        assessment=AvailabilityAssessment(AvailabilityStatus.AVAILABLE, (), ()),
    )

    candidate = replay.run()[0].candidates[0]
    assert candidate.decided_at == first_session.closes_at
    assert candidate.eligible_at == second_session.opens_at
    assert candidate.session_date_et == second_session.trading_date_et
    assert candidate.session_closes_at == second_session.closes_at


def test_a_session_ending_four_hour_bar_is_explicitly_truncated() -> None:
    schedule = XNYS_CALENDAR.session_schedule(date(2024, 1, 2), date(2024, 1, 2))
    table = pa.Table.from_pylist(
        [_bar_row(_utc(18, 30), date(2024, 1, 2))],
        schema=_SCHEMA,
    )

    event = bar_events_from_table(
        table,
        data_kind=DATA_KIND,
        resolution="4h",
        schedule=schedule,
    )[0]

    bar = event.payload["bar"]
    assert bar.starts_at == _utc(18, 30)
    assert bar.ends_at == schedule.sessions[0].closes_at == _utc(21, 0)
    assert bar.session_truncated is True


def manifest_for(path: Path) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "storage_object_id": STORAGE_OBJECT_ID,
        "object_key": path.name,
        "content_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "object_kind": "PARQUET",
        "partition_granularity": "DAY",
        "partition_start": "2024-01-02",
        "partition_end": "2024-01-03",
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "shard_key": "s00-of-01",
        "part_number": 1,
        "row_count": len(BAR_ROWS),
        "schema_version": "market-bars-v2",
    }
    return {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": MANIFEST_ID,
        "dataset_id": DATASET_ID,
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": canonical_dataset_hash([metadata]),
        "schema_id": "market-bars-v2",
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "available_at": "2024-04-02T01:00:00Z",
        "objects": [metadata],
    }


def requirement(requirement_id: str, instrument_id: str) -> DataRequirement:
    return DataRequirement(
        requirement_id=requirement_id,
        instrument_id=instrument_id,
        data_kind=DATA_KIND,
        resolution=RESOLUTION,
        warmup_from=_utc(14, 30),
        evaluation_from=_utc(14, 30),
        evaluation_through=THIRD,
    )


# --------------------------------------------------------------------------
# Stand-ins for the seams owned by other cards
# --------------------------------------------------------------------------


class StubPlan:
    """The slice of ``BasicCompiledPlan`` that ``BasicPlanReplay`` calls."""

    def evaluation_id(self, occurred_at: datetime) -> str:
        return f"eval-{occurred_at.isoformat()}"


class _Result:
    def __init__(self, as_of: datetime) -> None:
        self.as_of = as_of
        self.decisions: tuple[Any, ...] = ()


class StubRuntime:
    """Condition evaluation, stubbed. Records the inputs it was handed."""

    def __init__(self, *, buy_at: datetime | None = SECOND) -> None:
        self.inputs_by_instant: dict[datetime, Mapping[str, InstrumentInput]] = {}
        self._buy_at = buy_at

    def execute(
        self,
        plan: Any,
        instrument_inputs: Mapping[str, InstrumentInput],
        *,
        as_of: datetime,
    ) -> _Result:
        self.inputs_by_instant[as_of] = instrument_inputs
        return _Result(as_of)

    def order_candidates(
        self,
        plan: Any,
        result: _Result,
        *,
        evaluation_id: str,
        session_date_et: date,
        session_closes_at: datetime,
        eligible_at: datetime | None = None,
    ) -> tuple[OrderCandidate, ...]:
        if result.as_of != self._buy_at:
            return ()
        return (
            OrderCandidate(
                evaluation_id=evaluation_id,
                instrument_id=AAPL,
                partition_key="p-1",
                flow_id="flow-1",
                side="BUY",
                order_type="MARKET",
                allocation=Fraction(1, 1),
                reference_price=Decimal("102.00000000"),
                decided_at=result.as_of,
                eligible_at=result.as_of if eligible_at is None else eligible_at,
                session_date_et=session_date_et,
                session_closes_at=session_closes_at,
                budget_cap_bps=10000,
            ),
        )


class ExplodingRuntime(StubRuntime):
    def execute(self, plan: Any, instrument_inputs: Any, *, as_of: datetime) -> _Result:
        raise ZeroDivisionError("condition evaluator blew up")


class RecordingEngine:
    """Sizes candidates and fills them on the next bar of that instrument.

    The point is not the arithmetic -- BT-b owns that -- it is that the
    orchestrator hands over an *unsized* candidate and receives an order id.
    """

    def __init__(self) -> None:
        self.placed: list[OrderCandidate] = []
        self.settled: list[str] = []
        self._outstanding: dict[str, int] = {}
        self._fills = 0

    def place(self, candidate: OrderCandidate) -> str | None:
        assert not hasattr(candidate, "quantity"), (
            "a plan candidate must not carry a quantity; sizing is the engine's job"
        )
        self.placed.append(candidate)
        self._outstanding[candidate.instrument_id] = (
            self._outstanding.get(candidate.instrument_id, 0) + 1
        )
        return f"order-{len(self.placed)}"

    def settle(self, event: MarketDataEvent) -> int:
        self.settled.append(event.event_id)
        pending = self._outstanding.get(event.instrument_id, 0)
        if pending == 0:
            return 0
        self._outstanding[event.instrument_id] = pending - 1
        self._fills += 1
        return 1

    def summary(self) -> ExecutionSummary:
        return ExecutionSummary(
            cash=Decimal("10000") - Decimal(self._fills) * Decimal("1020"),
            fill_count=self._fills,
            ledger_entry_count=self._fills * 2,
            positions={AAPL: Decimal(self._fills) * Decimal("10")},
        )


class RecordingPublisher:
    def __init__(self, *, explode: bool = False, raises: Exception | None = None) -> None:
        self.requests: list[PublishRequest] = []
        self._explode = explode
        self._raises = raises

    def publish(self, request: PublishRequest) -> PublishedManifests:
        self.requests.append(request)
        if self._raises is not None:
            raise self._raises
        if self._explode:
            raise RuntimeError("object store is unreachable")
        return PublishedManifests(
            result_manifest_id=RESULT_MANIFEST_ID,
            detail_manifest_id=DETAIL_MANIFEST_ID,
            result_hash="c" * 64,
        )


class WallClock:
    """Deterministic monotonic stand-in for ``datetime.now(timezone.utc)``."""

    def __init__(self, start: datetime = WALL_T0, step: timedelta = timedelta(seconds=1)) -> None:
        self._now = start
        self._step = step

    def __call__(self) -> datetime:
        current = self._now
        self._now = self._now + self._step
        return current


class FixedMonitor:
    def __init__(self, sample: ResourceSample | None = None) -> None:
        self.sample_value = sample or ResourceSample(timedelta(seconds=1), 64 * 1024 * 1024)
        self.calls = 0

    def sample(self) -> ResourceSample:
        self.calls += 1
        return self.sample_value


# --------------------------------------------------------------------------
# Harness
# --------------------------------------------------------------------------


def _policy(**overrides: Any) -> AttemptPolicy:
    base: dict[str, Any] = {
        "max_attempts": 2,
        "lease_duration": timedelta(minutes=5),
        "attempt_timeout": timedelta(minutes=30),
        "max_cpu_time": timedelta(minutes=5),
        "max_memory_bytes": 512 * 1024 * 1024,
    }
    base.update(overrides)
    return AttemptPolicy(**base)


def _job(
    manifest: Mapping[str, Any],
    *,
    requirements: tuple[DataRequirement, ...] | None = None,
    initial_cash: Decimal = Decimal("10000"),
) -> BacktestJob:
    return BacktestJob(
        run_id=RUN_ID,
        idempotency_key="OFFICIAL_BACKTEST:bt4",
        worker_execution_key=f"BACKTEST_RUN:{RUN_ID}:OFFICIAL_BACKTEST:bt4",
        manifest=manifest,
        execution_policy=D17_EXECUTION_POLICY_FIXTURE,
        requirements=requirements
        or (requirement("req-a", AAPL), requirement("req-b", MSFT)),
        data_kind=DATA_KIND,
        resolution=RESOLUTION,
        initial_cash=initial_cash,
    )


class Harness:
    def __init__(self, tmp_path: Path, runtime: StubRuntime, engine: RecordingEngine,
                 publisher: RecordingPublisher) -> None:
        self.runtime = runtime
        self.engine = engine
        self.publisher = publisher
        self.replays: list[BasicPlanReplay] = []
        self._root = tmp_path

    def factory(self, *, clock: Any, assessment: Any) -> BasicPlanReplay:
        replay = BasicPlanReplay(
            runtime=self.runtime,  # type: ignore[arg-type]
            plan=StubPlan(),  # type: ignore[arg-type]
            clock=clock,
            assessment=assessment,
        )
        self.replays.append(replay)
        return replay

    def orchestrator(self) -> BacktestOrchestrator:
        return BacktestOrchestrator(
            reader=ParquetMarketDataReader(self._root),
            calendar=XNYS_CALENDAR,
            replay_factory=self.factory,
            engine=self.engine,
            publisher=self.publisher,
            wall_clock=WallClock(),
        )


def _run(
    tmp_path: Path,
    *,
    runtime: StubRuntime | None = None,
    engine: RecordingEngine | None = None,
    publisher: RecordingPublisher | None = None,
    policy: AttemptPolicy | None = None,
    monitor: Any = None,
    job: BacktestJob | None = None,
) -> tuple[ReplayOutcome, Harness, AttemptCoordinator]:
    path = tmp_path / "bars.parquet"
    write_bars(path)
    harness = Harness(
        tmp_path,
        runtime or StubRuntime(),
        engine or RecordingEngine(),
        publisher or RecordingPublisher(),
    )
    coordinator = AttemptCoordinator(RUN_ID, policy or _policy(), WALL_T0)
    lease = coordinator.acquire("bt4-worker", WALL_T0)
    outcome = harness.orchestrator().run(
        job or _job(manifest_for(path)),
        coordinator=coordinator,
        lease=lease,
        monitor=monitor or FixedMonitor(),
    )
    return outcome, harness, coordinator


# --------------------------------------------------------------------------
# The seams line up with BT-a's real classes
# --------------------------------------------------------------------------


def test_bt_a_replay_and_gate_satisfy_the_orchestrator_protocols(tmp_path: Path) -> None:
    """If BT-a's surface drifts from the Protocol, fail here, not in production."""
    _, harness, _ = _run(tmp_path)

    replay = harness.replays[0]
    assert isinstance(replay, PlanReplay)
    assert isinstance(replay.gate, ExecutionGate)
    for name in ("is_evaluation_allowed", "is_order_trigger_allowed", "is_fill_allowed"):
        assert callable(getattr(replay.gate, name))
    # BT-a's `_instrument_inputs` filters the visible set on this exact token.
    # If it is renamed, every evaluation silently sees an empty input map.
    assert BAR_CLOSED_EVENT_TYPE == RUNTIME_BAR_CLOSED_EVENT_TYPE


def test_orchestrator_uses_bounded_parquet_batches_instead_of_materialized_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def materialized_read_is_forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("the worker must use iter_batches")

    monkeypatch.setattr(ParquetMarketDataReader, "read", materialized_read_is_forbidden)

    outcome, _, _ = _run(tmp_path)

    assert outcome.status is ReplayStatus.COMPLETED


# --------------------------------------------------------------------------
# The replay loop is genuinely driven by the event clock.
# --------------------------------------------------------------------------


def test_replay_instants_are_bar_close_instants_not_bar_starts(tmp_path: Path) -> None:
    """Five bars collapse into three instants, each one a bar *close*.

    If the loop iterated rows there would be five steps, and the first would
    act on a bar that had not finished forming.
    """
    outcome, _, _ = _run(tmp_path)

    assert [step.instant for step in outcome.steps] == [FIRST, SECOND, THIRD]
    assert [step.event_ids for step in outcome.steps] == [
        (f"BAR:{AAPL}:2024-01-02T14:30:00+00:00", f"BAR:{MSFT}:2024-01-02T14:30:00+00:00"),
        (f"BAR:{AAPL}:2024-01-02T14:45:00+00:00",),
        (f"BAR:{AAPL}:2024-01-02T15:00:00+00:00", f"BAR:{MSFT}:2024-01-02T15:00:00+00:00"),
    ]
    assert all(
        step.session_status is MarketSessionStatus.REGULAR_OPEN for step in outcome.steps
    )
    assert [step.evaluation_id for step in outcome.steps] == [
        f"eval-{FIRST.isoformat()}",
        f"eval-{SECOND.isoformat()}",
        f"eval-{THIRD.isoformat()}",
    ]


def test_events_carry_the_series_bar_the_plan_runtime_reads(tmp_path: Path) -> None:
    """The event payload must satisfy BT-a's ``_instrument_inputs`` contract."""
    outcome, harness, _ = _run(tmp_path)
    assert outcome.status is ReplayStatus.COMPLETED

    inputs = harness.runtime.inputs_by_instant[THIRD]
    series = inputs[AAPL].series[0]
    assert series.data_kind == DATA_KIND
    assert series.resolution == RESOLUTION
    assert [bar.close for bar in series.bars] == [
        Decimal("101"),
        Decimal("102"),
        Decimal("103"),
    ]


def test_evaluation_sees_the_whole_warmup_window_not_just_the_latest_bar(
    tmp_path: Path,
) -> None:
    """An indicator's lookback must survive to the evaluator.

    RSI_14 needs fourteen prior bars; an input map rebuilt from only the bars
    released at this instant could never supply them.
    """
    _, harness, _ = _run(tmp_path)

    counts = {
        instant: len(inputs[AAPL].series[0].bars)
        for instant, inputs in harness.runtime.inputs_by_instant.items()
        if AAPL in inputs
    }
    # 14:45 is absent because the MSFT gap bars evaluation there; the two
    # instants that do evaluate each see their whole history, not one bar.
    assert counts == {SECOND: 2, THIRD: 3}
    # MSFT has a hole, so by the last instant it has 2 bars where AAPL has 3.
    assert len(harness.runtime.inputs_by_instant[THIRD][MSFT].series[0].bars) == 2


# --------------------------------------------------------------------------
# Data availability is consulted at every instant, and actually suppresses work.
# --------------------------------------------------------------------------


def test_data_gap_suppresses_evaluation_orders_and_fills_at_that_instant(
    tmp_path: Path,
) -> None:
    """MSFT's missing 14:45 bar makes ``[14:45, 15:00)`` a skip interval.

    The first instant lands exactly on 14:45, so all three stages are barred:
    the plan is not evaluated, no candidate is placed, and the bars that closed
    then do not reach the execution engine. A degraded window that still filled
    orders would silently assume coverage the run does not have.
    """
    outcome, harness, _ = _run(tmp_path)

    assert outcome.availability_status is AvailabilityStatus.DEGRADED
    assert outcome.skipped_step_count == 1

    first, second, third = outcome.steps
    assert (first.evaluation_allowed, first.order_trigger_allowed, first.fill_allowed) == (
        False,
        False,
        False,
    )
    assert first.skip_reason == "DATA_GAP_EVALUATION_SKIPPED"
    assert second.evaluation_allowed and third.evaluation_allowed
    assert (second.skip_reason, third.skip_reason) == (None, None)

    # The barred instant reached neither the evaluator nor the engine.
    assert FIRST not in harness.runtime.inputs_by_instant
    assert harness.engine.settled == [
        f"BAR:{AAPL}:2024-01-02T14:45:00+00:00",
        f"BAR:{AAPL}:2024-01-02T15:00:00+00:00",
        f"BAR:{MSFT}:2024-01-02T15:00:00+00:00",
    ]


def test_absent_series_makes_the_run_unavailable_without_publishing(
    tmp_path: Path,
) -> None:
    path = tmp_path / "bars.parquet"
    write_bars(path)
    job = _job(
        manifest_for(path),
        requirements=(
            requirement("req-a", AAPL),
            requirement("req-b", MSFT),
            requirement("req-c", ABSENT),
        ),
    )
    outcome, harness, coordinator = _run(tmp_path, job=job)

    assert outcome.status is ReplayStatus.UNAVAILABLE
    assert outcome.reason_code == "REQUIRED_DATA_UNAVAILABLE"
    assert outcome.missing_requirements == ("req-c:REQUIRED_SERIES_ABSENT",)
    assert outcome.steps == ()
    assert harness.runtime.inputs_by_instant == {}
    assert harness.publisher.requests == []
    assert coordinator.state is RunState.FAILED
    assert coordinator.attempts[0].state is AttemptState.PERMANENT_FAILED
    assert coordinator.attempts[0].reason_code == "REQUIRED_DATA_UNAVAILABLE"


# --------------------------------------------------------------------------
# Orders, fills and publication.
# --------------------------------------------------------------------------


def test_completed_run_places_unsized_candidates_fills_them_and_publishes_once(
    tmp_path: Path,
) -> None:
    outcome, harness, coordinator = _run(tmp_path)

    assert outcome.status is ReplayStatus.COMPLETED
    assert len(harness.engine.placed) == 1
    candidate = harness.engine.placed[0]
    assert candidate.instrument_id == AAPL
    assert candidate.allocation == Fraction(1, 1)
    assert candidate.reference_price == Decimal("102.00000000")

    assert [step.placed_order_ids for step in outcome.steps] == [(), ("order-1",), ()]
    assert [step.candidate_count for step in outcome.steps] == [0, 1, 0]
    # The order decided at 15:00 fills on the *next* bar, never on the bar that
    # produced the decision.
    assert [step.fill_count for step in outcome.steps] == [0, 0, 1]

    assert len(harness.publisher.requests) == 1
    published = harness.publisher.requests[0]
    assert published.run_id == RUN_ID
    assert published.execution.fill_count == 1
    assert published.replay_digest == outcome.replay_digest
    assert published.orchestrator_version == ORCHESTRATOR_VERSION
    assert len(published.evaluations) == 3

    assert coordinator.state is RunState.COMPLETED
    assert coordinator.result_manifest_id == RESULT_MANIFEST_ID
    assert coordinator.detail_manifest_id == DETAIL_MANIFEST_ID
    assert outcome.result_hash == "c" * 64


def test_plan_replay_failure_fails_the_run_permanently_and_publishes_nothing(
    tmp_path: Path,
) -> None:
    outcome, harness, coordinator = _run(tmp_path, runtime=ExplodingRuntime())

    assert outcome.status is ReplayStatus.FAILED
    assert outcome.reason_code == "PLAN_REPLAY_FAILED"
    assert harness.publisher.requests == []
    assert coordinator.state is RunState.FAILED
    assert coordinator.attempts[0].state is AttemptState.PERMANENT_FAILED


def test_publisher_failure_is_retryable_and_leaves_the_run_waiting(
    tmp_path: Path,
) -> None:
    outcome, _, coordinator = _run(tmp_path, publisher=RecordingPublisher(explode=True))

    assert outcome.status is ReplayStatus.FAILED
    assert outcome.reason_code == "RESULT_PUBLICATION_FAILED"
    assert outcome.replay_digest != ""
    assert coordinator.state is RunState.WAITING
    assert coordinator.attempts[0].state is AttemptState.RETRYABLE_FAILED
    assert coordinator.result_manifest_id is None


def test_an_unclassified_publisher_failure_records_and_logs_its_cause(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """`RESULT_PUBLICATION_FAILED` alone does not say what went wrong.

    A publisher that does not classify its own failure gets the benefit of the doubt
    and is retried, but the exception type and message are attached to the outcome and
    logged, so diagnosing one no longer means reading the source.
    """

    caplog.set_level("ERROR", logger="backtest_engine.orchestrator")

    outcome, _, coordinator = _run(tmp_path, publisher=RecordingPublisher(explode=True))

    assert outcome.failure_detail == "RuntimeError: object store is unreachable"
    assert outcome.retryable is True
    assert coordinator.state is RunState.WAITING
    logged = [record for record in caplog.records if record.name == "backtest_engine.orchestrator"]
    assert len(logged) == 1
    assert "object store is unreachable" in logged[0].getMessage()
    assert logged[0].exc_info is not None, "the traceback must survive, not just the text"


def test_a_publisher_that_declares_a_permanent_failure_is_not_retried(
    tmp_path: Path,
) -> None:
    """A conflict that can never succeed on retry must not be reported as transient.

    `DurableResultPublisher` raises this for `PublishConflict`: the run already has a
    different immutable official result. Retrying it burns the whole delivery budget
    and then dead-letters anyway, having hidden the real reason the entire time.
    """

    publisher = RecordingPublisher(
        raises=ResultPublicationError(
            "run already has a different immutable result",
            retryable=False,
            reason_code="RESULT_PUBLICATION_CONFLICT",
        )
    )

    outcome, _, coordinator = _run(tmp_path, publisher=publisher)

    assert outcome.status is ReplayStatus.FAILED
    assert outcome.reason_code == "RESULT_PUBLICATION_CONFLICT"
    assert outcome.retryable is False
    assert outcome.failure_detail == (
        "ResultPublicationError: run already has a different immutable result"
    )
    assert coordinator.state is RunState.FAILED
    assert coordinator.attempts[0].state is AttemptState.PERMANENT_FAILED


# --------------------------------------------------------------------------
# Determinism, pinned to literals.
# --------------------------------------------------------------------------


#: SHA-256 of the canonical replay step log. A constant-returning
#: implementation cannot produce it, and any change to instant boundaries,
#: availability decisions or order flow moves it.
PINNED_REPLAY_DIGEST = (
    "5cd9dc179dbbee08a16342b5acc35c55c0aebaa700ceb781c407f7aa8661a1a9"
)


def test_replay_digest_is_pinned_to_a_literal(tmp_path: Path) -> None:
    outcome, _, _ = _run(tmp_path)

    assert outcome.replay_digest == PINNED_REPLAY_DIGEST


def test_replay_digest_moves_when_the_replay_moves(tmp_path: Path) -> None:
    """Guards the pinned literal above against a constant implementation."""
    baseline, _, _ = _run(tmp_path)
    other, _, _ = _run(tmp_path, runtime=StubRuntime(buy_at=THIRD))

    assert len(baseline.replay_digest) == 64
    assert other.replay_digest != baseline.replay_digest


def test_replay_digest_moves_when_initial_cash_moves(tmp_path: Path) -> None:
    baseline, _, _ = _run(tmp_path)
    path = tmp_path / "bars.parquet"
    other, _, _ = _run(
        tmp_path, job=_job(manifest_for(path), initial_cash=Decimal("25000"))
    )

    assert other.replay_digest != baseline.replay_digest


# --------------------------------------------------------------------------
# Resource limits stop the run before an official result exists.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("policy_override", "expected"),
    [
        ({"max_memory_bytes": 1}, FailureKind.MEMORY_LIMIT),
        ({"max_cpu_time": timedelta(microseconds=1)}, FailureKind.CPU_LIMIT),
    ],
)
def test_resource_limit_aborts_before_any_official_result(
    tmp_path: Path,
    policy_override: dict[str, Any],
    expected: FailureKind,
) -> None:
    outcome, harness, coordinator = _run(tmp_path, policy=_policy(**policy_override))

    assert outcome.status is ReplayStatus.FAILED
    assert outcome.reason_code == expected.value
    assert harness.publisher.requests == []
    assert coordinator.result_manifest_id is None
    assert coordinator.detail_manifest_id is None
    assert coordinator.attempts[0].failure_kind is expected
    # The budget is checked before the plan replay, so nothing was evaluated.
    assert harness.runtime.inputs_by_instant == {}


def test_cancellation_requested_mid_run_stops_the_replay(tmp_path: Path) -> None:
    path = tmp_path / "bars.parquet"
    write_bars(path)
    coordinator = AttemptCoordinator(RUN_ID, _policy(), WALL_T0)
    lease = coordinator.acquire("bt4-worker", WALL_T0)
    harness = Harness(tmp_path, StubRuntime(), RecordingEngine(), RecordingPublisher())

    class CancelAfterFirstInstant:
        def __init__(self) -> None:
            self.calls = 0

        def sample(self) -> ResourceSample:
            self.calls += 1
            # Call 1 guards the plan replay; call 2 guards the first instant.
            if self.calls == 3:
                coordinator.request_cancellation(
                    WALL_T0 + timedelta(seconds=30),
                    reason_code="USER_CANCELLED",
                    requested_by="00000000-0000-4000-8000-0000000028aa",
                )
            return ResourceSample(timedelta(seconds=1), 1024)

    outcome = harness.orchestrator().run(
        _job(manifest_for(path)),
        coordinator=coordinator,
        lease=lease,
        monitor=CancelAfterFirstInstant(),
    )

    assert outcome.status is ReplayStatus.CANCELLED
    assert outcome.reason_code == "USER_CANCELLED"
    assert len(outcome.steps) == 1
    assert harness.publisher.requests == []
    assert coordinator.state is RunState.CANCELLED
    assert coordinator.attempts[0].state is AttemptState.CANCELLED
