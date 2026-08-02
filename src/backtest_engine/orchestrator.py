"""The runnable backtest: assembly and replay, promoted out of the test tree.

Before BT4 the only code that assembled the domain modules into something
runnable lived in ``tests/d_reproducibility_testkit.py``. This module is that
assembly as production code, with the two orphaned modules the audit called out
wired into the execution path for the first time:

* :class:`~backtest_engine.event_clock.MarketEventClock` is built here and
  handed to the plan replay, which advances it. Evaluation instants are bar
  *close* instants; publication lag changes what an instant can see without
  changing when instants happen.
* :class:`~backtest_engine.data_availability.AvailabilityAssessment` is built
  here from the coverage the verified objects actually delivered, and is
  consulted at every instant through BT-a's ``ExecutionGate`` -- separately for
  evaluation, order trigger and fill.

Everything this module needs from a module owned by another rebuild card is a
:class:`typing.Protocol` below.

Two clocks are in play and conflating them is a bug:

``replay clock``
    Simulated market time (2024 in the fixtures), advanced by the event clock.
``wall clock``
    Real time. Drives lease expiry, attempt timeout and resource sampling.

**Sizing is not done here.** A plan emits an ``OrderCandidate`` carrying an
exact ``allocation`` fraction, a reference price and a budget cap; turning that
into a quantity needs available cash and buying-power rules, which live in the
execution model. The orchestrator hands the candidate across untouched.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from fractions import Fraction
from typing import Any, Protocol, runtime_checkable
from zoneinfo import ZoneInfo

from .attempt_coordinator import (
    AttemptCoordinator,
    AttemptFailure,
    AttemptLease,
    FailureKind,
    ResourceMonitor,
)
from .data_availability import (
    AvailabilityAssessment,
    AvailabilityStatus,
    DataAvailabilityAssessor,
    DataObservation,
    DataRequirement,
    TimeInterval,
)
from .elements import SeriesBar, resolution_period
from .event_clock import (
    MarketDataEvent,
    MarketEventClock,
    MarketSessionStatus,
    OfficialSessionSchedule,
)
from .execution_policy import ExecutionPolicy
from .market_data import MarketDataValidationError


__all__ = [
    "BAR_CLOSED_EVENT_TYPE",
    "ORCHESTRATOR_VERSION",
    "BacktestJob",
    "BacktestOrchestrator",
    "ExecutionEngine",
    "ExecutionGate",
    "ExecutionSummary",
    "MarketDataReader",
    "OrchestratorError",
    "OrderCandidate",
    "PlanEvaluation",
    "PlanReplay",
    "PlanReplayFactory",
    "PublishRequest",
    "PublishedManifests",
    "ReplayOutcome",
    "ReplayStatus",
    "ReplayStep",
    "ResultPublisher",
    "SessionCalendar",
    "bar_events_from_table",
    "observations_from_events",
    "replay_digest",
]


#: Bumped whenever a change to this module could move ``replay_digest``.
ORCHESTRATOR_VERSION = "backtest-orchestrator:1.0.0"

#: Must equal ``basic_runtime.BAR_CLOSED_EVENT_TYPE``: BT-a's
#: ``_instrument_inputs`` filters the visible set on this exact token, so an
#: event stream tagged anything else evaluates against no data at all.
BAR_CLOSED_EVENT_TYPE = "BAR_CLOSED"


class OrchestratorError(RuntimeError):
    """Raised when a job cannot be assembled into a runnable replay."""


class ReplayStatus(StrEnum):
    """Canonical ``backtest.run_status`` tokens this replay can end in."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNAVAILABLE = "UNAVAILABLE"
    CANCELLED = "CANCELLED"


# ==========================================================================
# Protocols -- the seams to modules owned by other rebuild cards
# ==========================================================================


class MarketDataReader(Protocol):
    """Satisfied by :class:`backtest_engine.market_data.ParquetMarketDataReader`."""

    def read(self, manifest: Mapping[str, Any], policy: ExecutionPolicy) -> Any: ...


class SessionCalendar(Protocol):
    """Satisfied by ``backtest_engine.calendar.PinnedSessionCalendar`` (BT-a)."""

    def session_schedule(
        self, first: date | None = None, last: date | None = None
    ) -> OfficialSessionSchedule: ...


@runtime_checkable
class OrderCandidate(Protocol):
    """Structural view of ``backtest_engine.elements.orders.OrderCandidate``.

    Note the absence of a quantity. That is the point: the plan decides *which*
    instrument and *what share of the budget*; the execution model decides how
    many shares that buys.
    """

    evaluation_id: str
    instrument_id: str
    flow_id: str
    partition_key: str
    side: str
    order_type: str
    allocation: Fraction | None
    reference_price: Decimal
    decided_at: datetime
    eligible_at: datetime
    session_date_et: date
    session_closes_at: datetime
    budget_cap_bps: int


@runtime_checkable
class PlanEvaluation(Protocol):
    """Structural view of ``backtest_engine.basic_runtime.PlanEvaluation``."""

    evaluation_id: str
    occurred_at: datetime
    session_status: MarketSessionStatus
    candidates: tuple[Any, ...]
    skip_reason: Any | None


@runtime_checkable
class ExecutionGate(Protocol):
    """Satisfied by ``backtest_engine.basic_runtime.ExecutionGate`` (BT-a)."""

    def is_evaluation_allowed(self, instant: datetime) -> bool: ...

    def is_order_trigger_allowed(self, instant: datetime) -> bool: ...

    def is_fill_allowed(self, instant: datetime) -> bool: ...


@runtime_checkable
class PlanReplay(Protocol):
    """Satisfied by ``backtest_engine.basic_runtime.BasicPlanReplay`` (BT-a)."""

    gate: ExecutionGate

    def run(self) -> Sequence[Any]: ...


class PlanReplayFactory(Protocol):
    """Binds a loaded plan to the clock and assessment the orchestrator built.

    The orchestrator owns the clock and the assessment because they come from
    the pinned dataset; the plan and the element catalog come from BT-a. This
    callable is where the two meet.
    """

    def __call__(
        self, *, clock: MarketEventClock, assessment: AvailabilityAssessment
    ) -> PlanReplay: ...


@dataclass(frozen=True, slots=True)
class ExecutionSummary:
    cash: Decimal
    fill_count: int
    ledger_entry_count: int
    positions: Mapping[str, Decimal]


class ExecutionEngine(Protocol):
    """Must be satisfied by ``execution_model.BacktestExecutionModel`` (BT-b).

    ``place`` receives an unsized candidate and returns the accepted order id,
    or ``None`` when the engine declined it (no buying power, a budget cap that
    rounds to zero shares, a risk limit). Sizing, quantization and rejection all
    belong to the engine.
    """

    def place(self, candidate: Any) -> str | None: ...

    def settle(self, event: MarketDataEvent) -> int: ...

    def summary(self) -> ExecutionSummary: ...


@dataclass(frozen=True, slots=True)
class PublishedManifests:
    result_manifest_id: str
    detail_manifest_id: str
    result_hash: str


class ResultPublisher(Protocol):
    """Must be satisfied by result snapshot + detail manifest (BT-b, BT-c)."""

    def publish(self, request: PublishRequest) -> PublishedManifests: ...


# ==========================================================================
# Values
# ==========================================================================


@dataclass(frozen=True, slots=True)
class ReplayStep:
    """One immutable row of the replay's audit trail, in market time."""

    sequence: int
    instant: datetime
    session_status: MarketSessionStatus
    event_ids: tuple[str, ...]
    evaluation_id: str | None
    evaluation_allowed: bool
    order_trigger_allowed: bool
    fill_allowed: bool
    candidate_count: int
    placed_order_ids: tuple[str, ...]
    fill_count: int
    skip_reason: str | None


@dataclass(frozen=True, slots=True)
class PublishRequest:
    run_id: str
    orchestrator_version: str
    execution_policy_version: str
    initial_cash: Decimal
    availability_status: AvailabilityStatus
    execution: ExecutionSummary
    evaluations: tuple[Any, ...]
    steps: tuple[ReplayStep, ...]
    replay_digest: str
    completed_at: datetime


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    run_id: str
    status: ReplayStatus
    availability_status: AvailabilityStatus
    steps: tuple[ReplayStep, ...]
    replay_digest: str
    reason_code: str | None = None
    missing_requirements: tuple[str, ...] = ()
    result_manifest_id: str | None = None
    detail_manifest_id: str | None = None
    result_hash: str | None = None

    @property
    def skipped_step_count(self) -> int:
        return sum(1 for step in self.steps if step.skip_reason is not None)


@dataclass(frozen=True, slots=True)
class BacktestJob:
    """One official backtest, fully pinned before any work begins."""

    run_id: str
    idempotency_key: str
    worker_execution_key: str
    manifest: Mapping[str, Any]
    execution_policy: ExecutionPolicy
    requirements: tuple[DataRequirement, ...]
    data_kind: str
    resolution: str
    initial_cash: Decimal

    def __post_init__(self) -> None:
        if not self.run_id:
            raise OrchestratorError("run_id must not be empty")
        if not self.worker_execution_key:
            raise OrchestratorError("worker_execution_key must not be empty")
        if not self.requirements:
            raise OrchestratorError("a job must declare at least one data requirement")
        if not self.data_kind or not self.resolution:
            raise OrchestratorError("data_kind and resolution must be pinned")
        if self.initial_cash < 0:
            raise OrchestratorError("initial_cash must not be negative")
        # One source of truth for the bar period: the element catalog's
        # resolution table. A separately configured interval could disagree
        # with the resolution the plan actually reads.
        resolution_period(self.resolution)

    @property
    def bar_interval(self) -> timedelta:
        return resolution_period(self.resolution)


# ==========================================================================
# Assembly helpers
# ==========================================================================


def bar_events_from_table(
    table: Any,
    *,
    data_kind: str,
    resolution: str,
    publication_lag: timedelta = timedelta(0),
) -> tuple[MarketDataEvent, ...]:
    """Turn verified Parquet rows into the clock's event stream.

    ``occurred_at`` is the bar's **close**, matching BT-a's
    ``basic_runtime.bar_closed_event``: a bar is a fact only once its period
    ends. ``available_at`` may lag that, never precede it.

    The payload carries BT-a's :class:`SeriesBar` under ``"bar"`` because
    ``basic_runtime._instrument_inputs`` reads exactly that key, and carries the
    full OHLC alongside it because the execution model needs the high and low to
    price a fill. Both consumers read the same event; neither has to re-read the
    Parquet.
    """
    if publication_lag < timedelta(0):
        raise OrchestratorError("publication_lag must not be negative")
    period = resolution_period(resolution)
    events: list[MarketDataEvent] = []
    for index, row in enumerate(table.to_pylist(), start=1):
        starts_at: datetime = row["bar_start_at"]
        ends_at = starts_at + period
        instrument_id = str(row["instrument_id"])
        events.append(
            MarketDataEvent(
                event_id=f"BAR:{instrument_id}:{starts_at.isoformat()}",
                instrument_id=instrument_id,
                occurred_at=ends_at,
                available_at=ends_at + publication_lag,
                source_sequence=index,
                event_type=BAR_CLOSED_EVENT_TYPE,
                payload={
                    "dataKind": data_kind,
                    "resolution": resolution,
                    "bar": SeriesBar(
                        instrument_id=instrument_id,
                        resolution=resolution,
                        starts_at=starts_at,
                        ends_at=ends_at,
                        close=Decimal(str(row["close"])),
                        volume=Decimal(str(row["volume"])),
                    ),
                    "providerSymbol": row["provider_symbol"],
                    "open": Decimal(str(row["open"])),
                    "high": Decimal(str(row["high"])),
                    "low": Decimal(str(row["low"])),
                },
            )
        )
    if not events:
        raise OrchestratorError("the pinned dataset contains no bars")
    return tuple(events)


def observations_from_events(
    requirements: Sequence[DataRequirement],
    events: Sequence[MarketDataEvent],
    bar_interval: timedelta,
) -> tuple[DataObservation, ...]:
    """Coverage as actually delivered, derived from the verified bars.

    ``verified=True`` is a statement of fact, not an assumption: the reader has
    already re-hashed every object against the manifest before a single event
    reached this point. An instrument with no bars yields an empty coverage
    tuple, which the assessor turns into ``REQUIRED_SERIES_ABSENT`` rather than
    into a silently empty replay.
    """
    by_instrument: dict[str, list[TimeInterval]] = {}
    for event in events:
        # The event's instant is the bar's close; the coverage it evidences is
        # the bar's period.
        by_instrument.setdefault(event.instrument_id, []).append(
            TimeInterval(event.occurred_at - bar_interval, event.occurred_at)
        )
    return tuple(
        DataObservation(
            requirement_id=requirement.requirement_id,
            instrument_id=requirement.instrument_id,
            data_kind=requirement.data_kind,
            resolution=requirement.resolution,
            available_intervals=tuple(by_instrument.get(requirement.instrument_id, ())),
            verified=True,
        )
        for requirement in requirements
    )


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")


def replay_digest(
    job: BacktestJob,
    availability: AvailabilityStatus,
    steps: Sequence[ReplayStep],
    summary: ExecutionSummary,
) -> str:
    """SHA-256 over the canonical replay log.

    Every input that can change a result is in here, so two runs agreeing on
    this digest agree on the replay that produced them.
    """
    payload = {
        "orchestratorVersion": ORCHESTRATOR_VERSION,
        "runId": job.run_id,
        "executionPolicyVersion": job.execution_policy.version,
        "precisionRulesVersion": job.execution_policy.precision_rules_version,
        "initialCash": str(job.initial_cash),
        "dataKind": job.data_kind,
        "resolution": job.resolution,
        "barIntervalSeconds": int(job.bar_interval.total_seconds()),
        "availabilityStatus": availability.value,
        "steps": [
            {
                "sequence": step.sequence,
                "instant": step.instant.isoformat(),
                "sessionStatus": step.session_status.value,
                "eventIds": list(step.event_ids),
                "evaluationId": step.evaluation_id,
                "evaluationAllowed": step.evaluation_allowed,
                "orderTriggerAllowed": step.order_trigger_allowed,
                "fillAllowed": step.fill_allowed,
                "candidateCount": step.candidate_count,
                "placedOrderIds": list(step.placed_order_ids),
                "fillCount": step.fill_count,
                "skipReason": step.skip_reason,
            }
            for step in steps
        ],
        "execution": {
            "cash": str(summary.cash),
            "fillCount": summary.fill_count,
            "ledgerEntryCount": summary.ledger_entry_count,
            "positions": {
                instrument_id: str(quantity)
                for instrument_id, quantity in sorted(summary.positions.items())
            },
        },
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


# ==========================================================================
# The orchestrator
# ==========================================================================


class BacktestOrchestrator:
    """Assembles the pinned inputs, replays the plan, executes, and publishes."""

    def __init__(
        self,
        *,
        reader: MarketDataReader,
        calendar: SessionCalendar,
        replay_factory: PlanReplayFactory,
        engine: ExecutionEngine,
        publisher: ResultPublisher,
        wall_clock: Callable[[], datetime] | None = None,
        assessor: DataAvailabilityAssessor | None = None,
        publication_lag: timedelta = timedelta(0),
    ) -> None:
        self._reader = reader
        self._calendar = calendar
        self._replay_factory = replay_factory
        self._engine = engine
        self._publisher = publisher
        self._wall_clock = wall_clock or (lambda: datetime.now(timezone.utc))
        self._assessor = assessor or DataAvailabilityAssessor()
        self._publication_lag = publication_lag

    def _schedule(self, policy: ExecutionPolicy) -> OfficialSessionSchedule:
        zone = ZoneInfo(policy.timezone)
        first = policy.period_start.astimezone(zone).date()
        last = (policy.period_end - timedelta(microseconds=1)).astimezone(zone).date()
        return self._calendar.session_schedule(first, last)

    # -- entry point ------------------------------------------------------

    def run(
        self,
        job: BacktestJob,
        *,
        coordinator: AttemptCoordinator,
        lease: AttemptLease,
        monitor: ResourceMonitor,
    ) -> ReplayOutcome:
        try:
            table = self._reader.read(job.manifest, job.execution_policy)
        except MarketDataValidationError:
            return self._abort(
                job, coordinator, lease, "INPUT_DATASET_UNREADABLE",
                retryable=False, status=ReplayStatus.FAILED,
            )

        events = bar_events_from_table(
            table,
            data_kind=job.data_kind,
            resolution=job.resolution,
            publication_lag=self._publication_lag,
        )
        assessment = self._assessor.assess(
            job.requirements,
            observations_from_events(job.requirements, events, job.bar_interval),
        )

        if assessment.status is AvailabilityStatus.UNAVAILABLE:
            fields = assessment.unavailable_contract_fields()
            outcome = self._abort(
                job, coordinator, lease, str(fields["reasonCode"]),
                retryable=False, status=ReplayStatus.UNAVAILABLE,
                availability=assessment.status,
            )
            declared = fields["missingRequirements"]
            missing = tuple(
                str(item) for item in (declared if isinstance(declared, list) else [])
            )
            return _with_missing(outcome, missing)

        clock = MarketEventClock(self._schedule(job.execution_policy), events)

        # Police the attempt before the plan replay, which is the single
        # longest-running call in the run.
        try:
            lease = coordinator.heartbeat(lease, self._wall_clock(), monitor.sample())
        except AttemptFailure as failure:
            return _from_attempt_failure(job, assessment, (), failure, coordinator)

        replay = self._replay_factory(clock=clock, assessment=assessment)
        try:
            evaluations = tuple(replay.run())
        except Exception:
            return self._abort(
                job, coordinator, lease, "PLAN_REPLAY_FAILED",
                retryable=False, status=ReplayStatus.FAILED,
                availability=assessment.status,
            )

        return self._execute(
            job, events, evaluations, replay.gate, assessment, coordinator, lease, monitor
        )

    # -- execution pass ---------------------------------------------------

    def _execute(
        self,
        job: BacktestJob,
        events: tuple[MarketDataEvent, ...],
        evaluations: tuple[Any, ...],
        gate: ExecutionGate,
        assessment: AvailabilityAssessment,
        coordinator: AttemptCoordinator,
        lease: AttemptLease,
        monitor: ResourceMonitor,
    ) -> ReplayOutcome:
        """Walk the pinned stream in market time, settling then triggering.

        Fills are settled against the bars closing at an instant *before* that
        instant's candidates are placed, so an order decided on a bar can never
        be filled by that same bar.
        """
        events_at: dict[datetime, list[MarketDataEvent]] = {}
        for event in events:
            events_at.setdefault(event.occurred_at, []).append(event)
        evaluation_at = {
            evaluation.occurred_at: evaluation for evaluation in evaluations
        }

        steps: list[ReplayStep] = []
        for sequence, instant in enumerate(sorted(events_at), start=1):
            try:
                lease = coordinator.heartbeat(
                    lease, self._wall_clock(), monitor.sample()
                )
            except AttemptFailure as failure:
                return _from_attempt_failure(
                    job, assessment, tuple(steps), failure, coordinator
                )

            evaluation = evaluation_at.get(instant)
            fill_allowed = gate.is_fill_allowed(instant)
            order_allowed = gate.is_order_trigger_allowed(instant)
            evaluation_allowed = gate.is_evaluation_allowed(instant)

            fill_count = 0
            if fill_allowed:
                for event in events_at[instant]:
                    fill_count += self._engine.settle(event)

            candidates = tuple(evaluation.candidates) if evaluation else ()
            placed: tuple[str, ...] = ()
            if candidates and order_allowed:
                placed = tuple(
                    order_id
                    for candidate in candidates
                    if (order_id := self._engine.place(candidate)) is not None
                )

            status = _session_status(evaluation, gate, instant)
            steps.append(
                ReplayStep(
                    sequence=sequence,
                    instant=instant,
                    session_status=status,
                    event_ids=tuple(
                        event.event_id for event in events_at[instant]
                    ),
                    evaluation_id=(
                        evaluation.evaluation_id if evaluation is not None else None
                    ),
                    evaluation_allowed=evaluation_allowed,
                    order_trigger_allowed=order_allowed,
                    fill_allowed=fill_allowed,
                    candidate_count=len(candidates),
                    placed_order_ids=placed,
                    fill_count=fill_count,
                    skip_reason=_skip_reason(
                        evaluation, evaluation_allowed, order_allowed, fill_allowed, status
                    ),
                )
            )

        return self._publish(
            job, assessment, evaluations, tuple(steps), coordinator, lease
        )

    def _publish(
        self,
        job: BacktestJob,
        assessment: AvailabilityAssessment,
        evaluations: tuple[Any, ...],
        steps: tuple[ReplayStep, ...],
        coordinator: AttemptCoordinator,
        lease: AttemptLease,
    ) -> ReplayOutcome:
        summary = self._engine.summary()
        digest = replay_digest(job, assessment.status, steps, summary)
        request = PublishRequest(
            run_id=job.run_id,
            orchestrator_version=ORCHESTRATOR_VERSION,
            execution_policy_version=job.execution_policy.version,
            initial_cash=job.initial_cash,
            availability_status=assessment.status,
            execution=summary,
            evaluations=evaluations,
            steps=steps,
            replay_digest=digest,
            completed_at=self._wall_clock(),
        )
        try:
            published = self._publisher.publish(request)
        except Exception:
            return self._abort(
                job, coordinator, lease, "RESULT_PUBLICATION_FAILED",
                retryable=True, status=ReplayStatus.FAILED,
                availability=assessment.status, steps=steps, digest=digest,
            )

        coordinator.complete(
            lease,
            self._wall_clock(),
            published.result_manifest_id,
            published.detail_manifest_id,
        )
        return ReplayOutcome(
            run_id=job.run_id,
            status=ReplayStatus.COMPLETED,
            availability_status=assessment.status,
            steps=steps,
            replay_digest=digest,
            result_manifest_id=published.result_manifest_id,
            detail_manifest_id=published.detail_manifest_id,
            result_hash=published.result_hash,
        )

    # -- failure path -----------------------------------------------------

    def _abort(
        self,
        job: BacktestJob,
        coordinator: AttemptCoordinator,
        lease: AttemptLease,
        reason_code: str,
        *,
        retryable: bool,
        status: ReplayStatus,
        availability: AvailabilityStatus = AvailabilityStatus.UNAVAILABLE,
        steps: tuple[ReplayStep, ...] = (),
        digest: str = "",
    ) -> ReplayOutcome:
        coordinator.fail(
            lease, self._wall_clock(), reason_code=reason_code, retryable=retryable
        )
        return ReplayOutcome(
            run_id=job.run_id,
            status=status,
            availability_status=availability,
            steps=steps,
            replay_digest=digest,
            reason_code=reason_code,
        )


def _session_status(
    evaluation: Any, gate: ExecutionGate, instant: datetime
) -> MarketSessionStatus:
    if evaluation is not None:
        return evaluation.session_status
    status_at: Any = getattr(gate, "session_status_at", None)
    if status_at is None:  # pragma: no cover - BT-a's gate always has it
        return MarketSessionStatus.CALENDAR_UNAVAILABLE
    status: MarketSessionStatus = status_at(instant)
    return status


def _skip_reason(
    evaluation: Any,
    evaluation_allowed: bool,
    order_allowed: bool,
    fill_allowed: bool,
    status: MarketSessionStatus,
) -> str | None:
    """Why this instant produced no work, distinguishing the four causes."""
    if evaluation is not None and evaluation.skip_reason is not None:
        return str(getattr(evaluation.skip_reason, "value", evaluation.skip_reason))
    if not (evaluation_allowed and order_allowed):
        return "DATA_UNAVAILABLE"
    if not fill_allowed:
        return (
            "DATA_UNAVAILABLE"
            if status is MarketSessionStatus.REGULAR_OPEN
            else status.value
        )
    return None


def _from_attempt_failure(
    job: BacktestJob,
    assessment: AvailabilityAssessment,
    steps: tuple[ReplayStep, ...],
    failure: AttemptFailure,
    coordinator: AttemptCoordinator,
) -> ReplayOutcome:
    """The coordinator already recorded the attempt; only report it here.

    The reason code is read back off the recorded attempt rather than
    re-derived, so the outcome and the durable attempt row cannot disagree: a
    cancellation keeps the requester's reason, a limit keeps the limit's.
    """
    attempts = coordinator.attempts
    recorded = attempts[-1].reason_code if attempts else None
    return ReplayOutcome(
        run_id=job.run_id,
        status=(
            ReplayStatus.CANCELLED
            if failure.kind is FailureKind.CANCELLED
            else ReplayStatus.FAILED
        ),
        availability_status=assessment.status,
        steps=steps,
        replay_digest="",
        reason_code=recorded or failure.kind.value,
    )


def _with_missing(outcome: ReplayOutcome, missing: tuple[str, ...]) -> ReplayOutcome:
    return ReplayOutcome(
        run_id=outcome.run_id,
        status=outcome.status,
        availability_status=outcome.availability_status,
        steps=outcome.steps,
        replay_digest=outcome.replay_digest,
        reason_code=outcome.reason_code,
        missing_requirements=missing,
    )
