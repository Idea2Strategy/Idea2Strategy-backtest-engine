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
import logging
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
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
    AttemptState,
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
    "ResultPublicationError",
    "ResultPublisher",
    "SessionCalendar",
    "bar_events_from_batches",
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


_LOG = logging.getLogger(__name__)


class _BoundedVisibleEvents:
    """Incremental look-ahead-safe view matching a live rolling bar cache."""

    def __init__(
        self,
        events: tuple[MarketDataEvent, ...],
        *,
        per_series_limit: int | None,
    ) -> None:
        self._events = tuple(sorted(
            events,
            key=lambda event: (event.available_at, event.source_sequence, event.event_id),
        ))
        self._cursor = 0
        self._per_series_limit = per_series_limit
        self._series: dict[tuple[str, str, str], deque[MarketDataEvent]] = {}
        self._other: list[MarketDataEvent] = []

    def advance_to(self, instant: datetime) -> tuple[MarketDataEvent, ...]:
        while (
            self._cursor < len(self._events)
            and self._events[self._cursor].available_at <= instant
        ):
            event = self._events[self._cursor]
            self._cursor += 1
            payload = event.payload
            if event.event_type == BAR_CLOSED_EVENT_TYPE:
                key = (
                    event.instrument_id,
                    str(payload.get("dataKind", "")),
                    str(payload.get("resolution", "")),
                )
                series = self._series.get(key)
                if series is None:
                    series = deque(maxlen=self._per_series_limit)
                    self._series[key] = series
                series.append(event)
            else:
                self._other.append(event)
        visible = [event for series in self._series.values() for event in series]
        visible.extend(self._other)
        return tuple(sorted(
            visible,
            key=lambda event: (event.available_at, event.source_sequence, event.event_id),
        ))


class OrchestratorError(RuntimeError):
    """Raised when a job cannot be assembled into a runnable replay."""


class ResultPublicationError(RuntimeError):
    """A `ResultPublisher` failed, and says whether retrying could ever help.

    Publication is the only step where "try again" and "never try again" are both
    plausible and the orchestrator cannot tell them apart: an unreachable object store
    is transient, while a run that already carries a *different* immutable official
    result is a conflict no number of redeliveries will resolve. Only the publisher
    knows which happened, so it states it here instead of raising a bare exception and
    letting the orchestrator guess `retryable=True`.

    A publisher that raises anything else is treated as transient - a bounded number of
    retries is the safer error - but its exception type and message are recorded on the
    outcome and logged with the traceback.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        reason_code: str = "RESULT_PUBLICATION_FAILED",
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.reason_code = reason_code


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

    def iter_batches(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
        *,
        instrument_ids: frozenset[str] | None = None,
    ) -> Iterable[Any]: ...

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

    def session_status_at(self, instant: datetime) -> MarketSessionStatus: ...

    def is_evaluation_allowed(self, instant: datetime) -> bool: ...

    def is_order_trigger_allowed(self, instant: datetime) -> bool: ...

    def is_fill_allowed(self, instant: datetime) -> bool: ...


@runtime_checkable
class PlanReplay(Protocol):
    """Satisfied by ``backtest_engine.basic_runtime.BasicPlanReplay`` (BT-a)."""

    gate: ExecutionGate

    def run(self) -> Sequence[Any]: ...

    def evaluate_at(
        self,
        instant: datetime,
        visible_events: tuple[MarketDataEvent, ...],
        runtime_values: Mapping[str, Mapping[str, str]] | None = None,
    ) -> Any: ...


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

    def runtime_values(
        self, instant: datetime, events: tuple[MarketDataEvent, ...]
    ) -> Mapping[str, Mapping[str, str]]: ...

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
    #: `"<ExceptionType>: <message>"` when a failure had an exception behind it. A
    #: `reason_code` names the *class* of failure; this names the instance, so a
    #: `RESULT_PUBLICATION_FAILED` can be diagnosed without reading the source.
    failure_detail: str | None = None
    #: Whether this failure was reported to the attempt coordinator as retryable.
    #: `None` for a successful outcome.
    retryable: bool | None = None

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
    manifests: tuple[Mapping[str, Any], ...] = ()
    evaluation_from: datetime | None = None
    evaluation_through: datetime | None = None

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
        manifests = tuple(self.manifests) or (self.manifest,)
        if any(not isinstance(item, Mapping) for item in manifests):
            raise OrchestratorError("manifests must contain dataset manifest mappings")
        object.__setattr__(self, "manifests", manifests)
        if (self.evaluation_from is None) != (self.evaluation_through is None):
            raise OrchestratorError("evaluation interval must include both boundaries")
        if self.evaluation_from is not None:
            evaluation_through = self.evaluation_through
            assert evaluation_through is not None
            if self.evaluation_from.tzinfo is None or evaluation_through.tzinfo is None:
                raise OrchestratorError("evaluation interval must be timezone-aware")
            if self.evaluation_from >= evaluation_through:
                raise OrchestratorError("evaluation interval must not be empty")
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
    schedule: OfficialSessionSchedule | None = None,
) -> tuple[MarketDataEvent, ...]:
    """Compatibility adapter for already-materialized Arrow tables."""
    return bar_events_from_batches(
        table.to_batches(),
        data_kind=data_kind,
        resolution=resolution,
        publication_lag=publication_lag,
        schedule=schedule,
    )


def bar_events_from_batches(
    batches: Iterable[Any],
    *,
    data_kind: str,
    resolution: str,
    publication_lag: timedelta = timedelta(0),
    schedule: OfficialSessionSchedule | None = None,
    allow_empty: bool = False,
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
    source_sequence = 0
    for batch in batches:
        for row in batch.to_pylist():
            source_sequence += 1
            source_starts_at: datetime = row["bar_start_at"]
            starts_at = source_starts_at
            session_truncated = False
            if schedule is not None:
                session = schedule.session_on(row["session_date_et"])
                if session is None:
                    raise MarketDataValidationError(
                        f"no official session exists for {row['session_date_et']}"
                    )
                if resolution == "1d":
                    # Daily providers may label a bar at midnight. Its economic
                    # interval is the official regular session, not that label.
                    starts_at = session.opens_at
                if not session.contains(starts_at):
                    raise MarketDataValidationError(
                        "bar_start_at is outside its official regular session"
                    )
                ends_at = min(starts_at + period, session.closes_at)
                session_truncated = ends_at - starts_at < period
            else:
                ends_at = starts_at + period
            instrument_id = str(row["instrument_id"])
            events.append(
                MarketDataEvent(
                    event_id=f"BAR:{instrument_id}:{source_starts_at.isoformat()}",
                    instrument_id=instrument_id,
                    occurred_at=ends_at,
                    available_at=ends_at + publication_lag,
                    source_sequence=source_sequence,
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
                            session_truncated=session_truncated,
                        ),
                        "sessionDateEt": row["session_date_et"],
                        "sourceBarStartAt": source_starts_at,
                        "providerSymbol": row["provider_symbol"],
                        "open": Decimal(str(row["open"])),
                        "high": Decimal(str(row["high"])),
                        "low": Decimal(str(row["low"])),
                    },
                )
            )
    if not events and not allow_empty:
        raise OrchestratorError("the pinned dataset contains no bars")
    return tuple(events)


def observations_from_events(
    requirements: Sequence[DataRequirement],
    events: Sequence[MarketDataEvent],
    bar_interval: timedelta,
    schedule: OfficialSessionSchedule | None = None,
) -> tuple[DataObservation, ...]:
    """Coverage as actually delivered, derived from the verified bars.

    ``verified=True`` is a statement of fact, not an assumption: the reader has
    already re-hashed every object against the manifest before a single event
    reached this point. An instrument with no bars yields an empty coverage
    tuple, which the assessor turns into ``REQUIRED_SERIES_ABSENT`` rather than
    into a silently empty replay.
    """
    by_series: dict[tuple[str, str, str], list[TimeInterval]] = {}
    for event in events:
        bar = event.payload.get("bar")
        interval = (
            TimeInterval(bar.starts_at, bar.ends_at)
            if isinstance(bar, SeriesBar)
            else TimeInterval(event.occurred_at - bar_interval, event.occurred_at)
        )
        data_kind = str(event.payload.get("dataKind", ""))
        resolution = str(event.payload.get("resolution", ""))
        by_series.setdefault((event.instrument_id, data_kind, resolution), []).append(interval)
    return tuple(
        DataObservation(
            requirement_id=requirement.requirement_id,
            instrument_id=requirement.instrument_id,
            data_kind=requirement.data_kind,
            resolution=requirement.resolution,
            available_intervals=(
                tuple(by_series.get((
                    requirement.instrument_id,
                    requirement.data_kind,
                    requirement.resolution,
                ), ()))
                + (
                    _closed_market_intervals(schedule, requirement.required_interval)
                    if schedule is not None
                    else ()
                )
            ),
            verified=True,
        )
        for requirement in requirements
    )


def _closed_market_intervals(
    schedule: OfficialSessionSchedule,
    target: TimeInterval,
) -> tuple[TimeInterval, ...]:
    """Intervals where no regular-session bar is expected within ``target``."""

    closed: list[TimeInterval] = []
    cursor = target.starts_at
    for session in schedule.sessions:
        if session.closes_at <= target.starts_at:
            continue
        if session.opens_at >= target.ends_at:
            break
        opens_at = max(session.opens_at, target.starts_at)
        closes_at = min(session.closes_at, target.ends_at)
        if cursor < opens_at:
            closed.append(TimeInterval(cursor, opens_at))
        cursor = max(cursor, closes_at)
    if cursor < target.ends_at:
        closed.append(TimeInterval(cursor, target.ends_at))
    return tuple(closed)


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
        schedule = self._schedule(job.execution_policy)
        required_instruments_by_resolution: dict[str, frozenset[str]] = {
            resolution: frozenset(
                requirement.instrument_id
                for requirement in job.requirements
                if requirement.resolution == resolution
            )
            for resolution in {requirement.resolution for requirement in job.requirements}
        }
        try:
            combined_events: list[MarketDataEvent] = []
            for manifest in job.manifests:
                resolution = str(manifest.get("resolution") or (
                    job.resolution if manifest is job.manifest else ""
                ))
                if not resolution:
                    raise MarketDataValidationError(
                        "every pinned dataset manifest must declare its resolution"
                    )
                required_instrument_ids = required_instruments_by_resolution.get(resolution)
                if not required_instrument_ids:
                    raise MarketDataValidationError(
                        f"pinned dataset resolution {resolution} is not required by the plan"
                    )
                manifest_events = bar_events_from_batches(
                    self._reader.iter_batches(
                        manifest,
                        job.execution_policy,
                        instrument_ids=required_instrument_ids,
                    ),
                    data_kind=job.data_kind,
                    resolution=resolution,
                    publication_lag=self._publication_lag,
                    schedule=schedule,
                    # Universe manifests are segmented by time. An instrument
                    # may legitimately have no rows in an early segment (for
                    # example before listing) while later pinned segments do.
                    # Availability is assessed over the combined verified
                    # stream below, not one storage segment at a time.
                    allow_empty=True,
                )
                combined_events.extend(
                    replace(event, event_id=f"{event.event_id}:{resolution}")
                    if len(job.manifests) > 1 else event
                    for event in manifest_events
                )
            combined_events.sort(key=lambda event: (
                event.occurred_at,
                resolution_period(str(event.payload.get("resolution", ""))),
                event.instrument_id,
                event.event_id,
            ))
            events = tuple(
                replace(event, source_sequence=index)
                for index, event in enumerate(
                    (
                        event
                        for event in combined_events
                        if event.occurred_at >= min(requirement.warmup_from for requirement in job.requirements)
                        and (job.evaluation_through is None or event.occurred_at <= job.evaluation_through)
                    ),
                    start=1,
                )
            )
        except MarketDataValidationError:
            return self._abort(
                job, coordinator, lease, "INPUT_DATASET_UNREADABLE",
                retryable=False, status=ReplayStatus.FAILED,
            )
        assessment = self._assessor.assess(
            job.requirements,
            observations_from_events(
                job.requirements,
                events,
                job.bar_interval,
                schedule=schedule,
            ),
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

        clock = MarketEventClock(schedule, events)

        # Police the attempt before the plan replay, which is the single
        # longest-running call in the run.
        try:
            lease = coordinator.heartbeat(lease, self._wall_clock(), monitor.sample())
        except AttemptFailure as failure:
            return _from_attempt_failure(job, assessment, (), failure, coordinator)

        replay = self._replay_factory(clock=clock, assessment=assessment)
        return self._execute(
            job, events, replay, replay.gate, assessment, coordinator, lease, monitor
        )

    # -- execution pass ---------------------------------------------------

    def _execute(
        self,
        job: BacktestJob,
        events: tuple[MarketDataEvent, ...],
        replay: PlanReplay,
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
        steps: list[ReplayStep] = []
        evaluations: list[Any] = []
        visible_window = _BoundedVisibleEvents(
            events,
            per_series_limit=getattr(replay, "visible_event_limit", None),
        )
        for sequence, instant in enumerate(sorted(events_at), start=1):
            if job.evaluation_through is not None and instant >= job.evaluation_through:
                break
            if job.evaluation_from is not None and instant < job.evaluation_from:
                visible_window.advance_to(instant)
                continue
            try:
                lease = coordinator.heartbeat(
                    lease, self._wall_clock(), monitor.sample()
                )
            except AttemptFailure as failure:
                return _from_attempt_failure(
                    job, assessment, tuple(steps), failure, coordinator
                )

            if gate.is_fill_allowed(instant):
                fillable_events = tuple(events_at[instant])
            elif gate.session_status_at(instant) is MarketSessionStatus.REGULAR_OPEN:
                # A data gap at an otherwise-open instant remains authoritative.
                fillable_events = ()
            else:
                # A session-closing bar becomes visible at the close, when the
                # market is no longer open. A resting order is nevertheless
                # evaluated at that bar's open, which is the historical instant
                # at which the fill would have happened.
                fillable_events = tuple(
                    event
                    for event in events_at[instant]
                    if gate.is_fill_allowed(event.payload["bar"].starts_at)
                )
            fill_allowed = bool(fillable_events)
            order_allowed = gate.is_order_trigger_allowed(instant)
            evaluation_allowed = gate.is_evaluation_allowed(instant)

            fill_count = 0
            if fill_allowed:
                for event in fillable_events:
                    fill_count += self._engine.settle(event)

            visible_events = visible_window.advance_to(instant)
            runtime_values_provider = getattr(self._engine, "runtime_values", None)
            runtime_values = (
                runtime_values_provider(instant, tuple(events_at[instant]))
                if runtime_values_provider is not None
                else {}
            )
            try:
                evaluation = replay.evaluate_at(instant, visible_events, runtime_values)
            except Exception as exc:
                detail = f"{type(exc).__name__}: {exc}"
                _LOG.error(
                    "backtest run %s plan replay failed at %s: %s",
                    job.run_id,
                    instant.isoformat(),
                    detail,
                    exc_info=exc,
                )
                return self._abort(
                    job, coordinator, lease, "PLAN_REPLAY_FAILED",
                    retryable=False, status=ReplayStatus.FAILED,
                    availability=assessment.status, steps=tuple(steps),
                    detail=detail,
                )
            compact_evaluation = getattr(replay, "compact_evaluation", None)
            evaluations.append(
                compact_evaluation(evaluation)
                if callable(compact_evaluation)
                else evaluation
            )

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
            job, assessment, tuple(evaluations), tuple(steps), coordinator, lease
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
        except Exception as exc:
            # The replay itself succeeded; only the write-out failed. Which reason code
            # and which retryability are the publisher's to state (`ResultPublicationError`);
            # anything else is unclassified, and an unclassified failure is retried but
            # never silently: the cause travels on the outcome and into the log.
            if isinstance(exc, ResultPublicationError):
                reason_code, retryable = exc.reason_code, exc.retryable
            else:
                reason_code, retryable = "RESULT_PUBLICATION_FAILED", True
            detail = f"{type(exc).__name__}: {exc}"
            _LOG.error(
                "backtest run %s could not publish its result (%s, retryable=%s): %s",
                job.run_id,
                reason_code,
                retryable,
                detail,
                exc_info=exc,
            )
            return self._abort(
                job, coordinator, lease, reason_code,
                retryable=retryable, status=ReplayStatus.FAILED,
                availability=assessment.status, steps=steps, digest=digest,
                detail=detail,
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
        detail: str | None = None,
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
            failure_detail=detail,
            retryable=retryable,
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
        failure_detail=f"{type(failure).__name__}: {failure}",
        # Read off the attempt the coordinator already recorded rather than re-derived,
        # for the same reason `reason_code` is: the outcome and the durable attempt row
        # must not be able to disagree about whether this is worth retrying.
        retryable=(
            attempts[-1].state is AttemptState.RETRYABLE_FAILED if attempts else None
        ),
    )


def _with_missing(outcome: ReplayOutcome, missing: tuple[str, ...]) -> ReplayOutcome:
    """`replace`, not a re-construction: a new field must not be dropped here."""

    return replace(outcome, missing_requirements=missing)
