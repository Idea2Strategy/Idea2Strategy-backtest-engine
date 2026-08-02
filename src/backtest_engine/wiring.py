"""BT7 -- the adapters that bind the orchestrator's seams to the real modules.

:mod:`backtest_engine.orchestrator` declares seven Protocols. Five were already
satisfied by modules other rebuild cards delivered; three had no producer at all,
and :mod:`backtest_engine.worker` had two more. This module is those five
adapters and nothing else -- no domain logic lives here, only the translation
between one card's vocabulary and another's.

=============================================  ==================================
Protocol                                       Adapter in this module
=============================================  ==================================
``orchestrator.PlanReplayFactory``             :class:`BasicPlanReplayFactory`
``orchestrator.ExecutionEngine``               :class:`ExecutionModelEngine`
``orchestrator.ResultPublisher``               :class:`DurableResultPublisher`
``worker.ExecutionKeyStore``                   :class:`PersistenceExecutionKeyStore`
``worker.JobHandler``                          :class:`OrchestratorJobHandler`
``object_store.StorageObjectWritePort``        :class:`PersistenceStorageObjectWritePort`
=============================================  ==================================

Sizing lives here, deliberately
-------------------------------
A plan emits an :class:`~backtest_engine.elements.orders.OrderCandidate` with an
exact ``allocation`` fraction, a ``reference_price`` and a ``budget_cap_bps`` --
and no quantity. Turning that into shares needs available cash, the buying-power
buffer, the fee rate and the slippage rate, none of which the evaluator knows.
:class:`ExecutionModelEngine` does it, under :data:`SIZING_RULES_VERSION`, and
hands the candidate to :class:`~backtest_engine.execution_model.BacktestExecutionModel`
untouched otherwise.

Nothing here invents a default
------------------------------
Every collaborator is a constructor argument. There is no module-level default
engine, store, catalog or clock: a deployment that has not chosen one cannot
accidentally get one, which is the failure mode the whole rebuild exists to
remove.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_FLOOR, Decimal, localcontext
from typing import Any, Protocol, cast

from sqlalchemy import update

from .attempt_coordinator import (
    AttemptCoordinator,
    AttemptPolicy,
    ResourceMonitor,
    RunState,
)
from .basic_runtime import (
    BasicCompiledPlan,
    BasicDecisionStatus,
    BasicPlanReplay,
    BasicPlanRuntime,
    PlanEvaluation,
    ReplaySkipReason,
    derive_data_requirements,
)
from .contracts import build_backtest_result_event
from .data_availability import AvailabilityAssessment
from .detail_object_manifest import (
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectPublisher,
    PerformancePoint,
    ReplayLedgerDetail,
)
from .event_clock import MarketDataEvent, MarketEventClock
from .execution_model import (
    BacktestExecutionModel,
    ExecutionMicrostructurePolicy,
    InstrumentFractionalPolicy,
    MinuteBar,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    QuantityMode,
    RiskLimits,
    TimeInForce,
)
from .execution_policy import BASIS_POINTS, ExecutionPolicy, ExecutionPolicyCatalog, ExecutionPolicyUnavailable
from .money import PRECISION_RULES_VERSION, QUANTITY_QUANTUM, apply_rate, quantize_money, quantize_quantity
from .monthly_judgment import (
    ConditionOutcome,
    JudgmentEvaluation,
    MonthlyJudgmentBuilder,
    MonthlyJudgmentSummary,
    StrategyMode,
)
from .object_store import (
    ObjectStore,
    ObjectStoreConflict,
    StorageObjectRecord,
    StorageObjectRegistrar,
)
from .orchestrator import (
    BacktestJob,
    BacktestOrchestrator,
    ExecutionSummary,
    OrchestratorError,
    PlanReplay,
    PublishedManifests,
    PublishRequest,
    ReplayOutcome,
    ReplayStatus,
    SessionCalendar,
)
from .persistence import (
    BacktestPersistence,
    DetailManifestRow,
    FailureConditionCountRow,
    InputBundleRow,
    InputDatasetRow,
    MonthlyJudgment,
    MonthlyJudgmentSummaryRow,
    ObjectStatus,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunPublication,
    StorageObjectRow,
    WorkStatus,
    publish_completed_run,
)
from .persistence.errors import InvalidStatusTransition as PersistedInvalidStatusTransition
from .persistence.errors import PersistenceError, PublishConflict, RowNotFound
from .persistence.tables import run_attempts as _run_attempts_table
from .result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultSnapshot,
    ResultSnapshotBuilder,
    RunSnapshot,
    fill_result_record,
    order_result_record,
)
from .worker import (
    ExecutionClaim,
    ExecutionRecordStatus,
    JobContext,
    JobOutcome,
    JobResult,
)


__all__ = [
    "COST_MODEL_VERSION",
    "EXECUTION_MODEL_VERSION",
    "RESULT_OBJECT_FILE_FORMAT",
    "SIZING_RULES_VERSION",
    "WIRING_VERSION",
    "BasicPlanReplayFactory",
    "DurableResultPublisher",
    "ExecutionModelEngine",
    "JobBinding",
    "JobEnvelope",
    "JobNotSatisfiable",
    "OrchestratorJobHandler",
    "PersistenceExecutionKeyStore",
    "PersistenceStorageObjectWritePort",
    "ResultSink",
    "WiringError",
    "dataset_coverage",
    "evaluation_window",
]


#: Bumped whenever a change in this module could move a published digest.
WIRING_VERSION = "backtest-wiring:1.0.0"

#: The rule set :meth:`ExecutionModelEngine.place` sizes a candidate under.
#: ``quantity = floor_to_quantum( buying_power * budgetCapBps/10000 * allocation
#: / (reference_price * (1 + slippage_rate) * (1 + fee_rate)) )``.
SIZING_RULES_VERSION = "sizing:budget-cap-floor-to-quantum:1.0.0"

#: ``RunSnapshot.cost_model_version`` / ``execution_model_version``. Both are
#: recorded inside ``run_snapshot_id``, so they are pinned identifiers rather
#: than free text a caller may vary.
COST_MODEL_VERSION = "backtest-cost:1.0.0"
EXECUTION_MODEL_VERSION = "backtest-execution:1.0.0"

#: ``storage.objects.file_format`` for the JSON result-snapshot object. The
#: detail parts are Parquet; the immutable result object is not.
RESULT_OBJECT_FILE_FORMAT = "JSON"

_ORDER_ID_NAMESPACE = uuid.UUID("2c1c1d0e-1f4b-4d67-9a2a-6a7f4a1b9c31")
_POINT_ID_NAMESPACE = uuid.UUID("6a0d1f22-8c4e-4c1b-9f30-2b5d4c8e77a2")
_RESULT_OBJECT_NAMESPACE = uuid.UUID("b7e2b2b9-5c3a-4d18-8b6f-9c1e2a4f0d55")
_ATTEMPT_ID_NAMESPACE = uuid.UUID("f4a1c60d-3b2e-4a77-9d13-58c0e6b4a920")

_ZERO = Decimal(0)
_ONE = Decimal(1)
_WORKING_PRECISION = 60


class WiringError(RuntimeError):
    """Raised when two modules cannot be bound together as configured."""


class JobNotSatisfiable(WiringError):
    """A queue message names an input this deployment cannot resolve.

    Retrying cannot fix it, so the worker dead-letters rather than looping.
    """

    def __init__(self, message: str, *, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _utc_text(value: datetime) -> str:
    """The ``utcTimestamp`` form every `backtest.v1` field is validated against."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _prefixed(digest: str) -> str:
    return digest if digest.startswith("sha256:") else f"sha256:{digest}"


def _floor_to_quantum(value: Decimal, quantum: Decimal) -> Decimal:
    """Largest multiple of ``quantum`` not exceeding ``value``.

    Mirrors ``execution_model.CAPACITY_FLOOR_RULE``: sizing rounds **down**,
    because rounding to nearest could produce an order larger than the budget
    that authorised it.
    """
    if value <= _ZERO:
        return _ZERO
    with localcontext() as context:
        context.prec = _WORKING_PRECISION
        steps = (value / quantum).to_integral_value(rounding=ROUND_FLOOR)
        return steps * quantum


# ==========================================================================
# orchestrator.PlanReplayFactory
# ==========================================================================


@dataclass(frozen=True, slots=True)
class BasicPlanReplayFactory:
    """Binds a loaded plan to the clock and assessment the orchestrator built.

    The plan and the element catalog come from BT-a's loader; the clock and the
    availability assessment come from the pinned dataset the orchestrator read.
    This callable is the only place the two meet, which is why the orchestrator
    takes it as a Protocol rather than constructing a replay itself.
    """

    runtime: BasicPlanRuntime
    plan: BasicCompiledPlan

    def __call__(
        self, *, clock: MarketEventClock, assessment: AvailabilityAssessment
    ) -> PlanReplay:
        # Declared as the orchestrator's Protocol rather than the concrete class:
        # the orchestrator is the consumer and this is its contract.
        #
        # The cast is a static-typing artefact, not a runtime gap.
        # `orchestrator.PlanReplay` declares `gate` as a mutable attribute, which
        # makes it invariant, so mypy demands that `BasicPlanReplay.gate` be
        # annotated with `orchestrator.ExecutionGate` rather than with BT-a's
        # structurally identical dataclass. `tests/test_wiring.py` asserts
        # `isinstance(replay, PlanReplay)` against the real object, which is the
        # check that actually matters. Declaring `PlanReplay.gate` read-only in
        # `orchestrator.py` would remove the cast; that module is BT-d's.
        return cast(
            "PlanReplay",
            BasicPlanReplay(
                runtime=self.runtime, plan=self.plan, clock=clock, assessment=assessment
            ),
        )


# ==========================================================================
# orchestrator.ExecutionEngine
# ==========================================================================


class ExecutionModelEngine:
    """``BacktestExecutionModel`` behind the orchestrator's ``ExecutionEngine``.

    Three responsibilities, none of which belong to either module alone:

    * **sizing** -- turn an unsized ``OrderCandidate`` into an ``OrderRequest``
      under :data:`SIZING_RULES_VERSION`;
    * **bar translation** -- turn the orchestrator's ``MarketDataEvent`` into
      the ``MinuteBar`` the matching engine consumes;
    * **evidence** -- accumulate the ``ResultRecord`` stream the result snapshot
      is built from, so a rejected or expired order is still visible.

    Only 1-minute bars are supported, because ``MinuteBar`` is defined as
    exactly one minute. A coarser resolution is refused rather than silently
    re-labelled.
    """

    def __init__(
        self,
        *,
        model: BacktestExecutionModel,
        run_snapshot: RunSnapshot,
        policy: ExecutionPolicy,
        fractional_policy: InstrumentFractionalPolicy,
    ) -> None:
        self._model = model
        self._run = run_snapshot
        self._policy = policy
        self._fractional = fractional_policy
        self._records: list[ResultRecord] = []
        self._instruments: set[str] = set()
        self._declined: list[str] = []

    # -- evidence ---------------------------------------------------------

    @property
    def records(self) -> tuple[ResultRecord, ...]:
        """Every order, rejection, expiry and fill this run produced, in order."""
        return tuple(self._records)

    @property
    def ledger_transactions(self) -> tuple[Any, ...]:
        return self._model.ledger_transactions

    @property
    def declined_candidates(self) -> tuple[str, ...]:
        """Evaluation ids whose candidate never became an order request."""
        return tuple(self._declined)

    # -- ExecutionEngine ---------------------------------------------------

    def place(self, candidate: Any) -> str | None:
        instrument_id = str(candidate.instrument_id)
        self._instruments.add(instrument_id)
        side = OrderSide(candidate.side)
        fractional = self._fractional.enabled_for(instrument_id)
        quantity = self._size(candidate, side, fractional_eligible=fractional)
        if quantity <= _ZERO:
            # The budget the plan allocated does not buy one whole quantum. This
            # is a decline, not a rejection: no order request exists to record.
            self._declined.append(str(candidate.evaluation_id))
            return None

        order_id = str(
            uuid.uuid5(
                _ORDER_ID_NAMESPACE,
                f"{self._run.snapshot_id}|{candidate.evaluation_id}|"
                f"{candidate.flow_id}|{instrument_id}",
            )
        )
        request = OrderRequest(
            order_id=order_id,
            instrument_id=instrument_id,
            side=side,
            order_type=OrderType(candidate.order_type),
            quantity=quantity,
            quantity_mode=(
                QuantityMode.FRACTIONAL_SHARES if fractional else QuantityMode.WHOLE_SHARES
            ),
            time_in_force=TimeInForce.DAY,
            submitted_at=candidate.decided_at,
            eligible_at=candidate.eligible_at,
            day_expires_at=candidate.session_closes_at,
            reference_price=candidate.reference_price,
        )
        order = self._model.submit(request)
        self._records.append(
            order_result_record(
                self._run, order, candidate.decided_at, self._model.cash, self._positions()
            )
        )
        return order_id if order.status is OrderStatus.ACCEPTED else None

    def settle(self, event: MarketDataEvent) -> int:
        bar = self._bar_of(event)
        for expired in self._model.advance_time(bar.starts_at):
            self._records.append(
                order_result_record(
                    self._run, expired, bar.starts_at, self._model.cash, self._positions()
                )
            )
        fills = self._model.process_bar(bar)
        for fill in fills:
            self._records.append(
                fill_result_record(
                    self._run,
                    fill,
                    self._model.order(fill.order_id),
                    self._model.cash,
                    self._positions(),
                )
            )
        return len(fills)

    def summary(self) -> ExecutionSummary:
        return ExecutionSummary(
            cash=self._model.cash,
            fill_count=len(self._model.fills),
            ledger_entry_count=sum(
                len(transaction.entries) for transaction in self._model.ledger_transactions
            ),
            positions={
                instrument_id: self._model.position(instrument_id).quantity
                for instrument_id in sorted(self._instruments)
            },
        )

    # -- internals ---------------------------------------------------------

    def _size(self, candidate: Any, side: OrderSide, *, fractional_eligible: bool) -> Decimal:
        """Shares this candidate's share of the budget buys. See the class docstring."""
        quantum = QUANTITY_QUANTUM if fractional_eligible else _ONE
        if side is OrderSide.SELL:
            # A disposal is sized by the held position, never by a cash budget.
            held = self._model.position(str(candidate.instrument_id)).quantity
            return _floor_to_quantum(held, quantum)

        allocation = candidate.allocation
        if allocation is None:
            raise WiringError(
                "a BUY candidate must carry an allocation share; sizing one without "
                "it would invent a position size"
            )
        budget = apply_rate(
            self._model.buying_power,
            Decimal(int(candidate.budget_cap_bps)) / BASIS_POINTS,
            "budget_cap",
        )
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            share = budget * Decimal(allocation.numerator) / Decimal(allocation.denominator)
        share = quantize_money(share, "allocated_budget")

        reference_price = candidate.reference_price
        unit_price = quantize_money(
            reference_price + apply_rate(reference_price, self._policy.slippage_rate, "slippage"),
            "unit_price",
        )
        unit_cash = quantize_money(
            unit_price + apply_rate(unit_price, self._policy.fee_rate, "unit_fee"), "unit_cash"
        )
        if unit_cash <= _ZERO:  # pragma: no cover - reference_price is positive by contract
            raise WiringError("a candidate's reference price must produce a positive unit cost")
        with localcontext() as context:
            context.prec = _WORKING_PRECISION
            raw = share / unit_cash
        return quantize_quantity(
            _floor_to_quantum(raw, quantum),
            fractional_eligible=fractional_eligible,
            label="order quantity",
        )

    def _bar_of(self, event: MarketDataEvent) -> MinuteBar:
        payload = event.payload
        series_bar = payload["bar"]
        if series_bar.ends_at - series_bar.starts_at != timedelta(minutes=1):
            raise WiringError(
                "the execution model consumes one-minute bars only; this run pinned "
                f"resolution {payload['resolution']!r}"
            )
        return MinuteBar(
            instrument_id=event.instrument_id,
            starts_at=series_bar.starts_at,
            ends_at=series_bar.ends_at,
            open=payload["open"],
            high=payload["high"],
            low=payload["low"],
            close=series_bar.close,
            volume=series_bar.volume,
        )

    def _positions(self) -> tuple[PositionAfter, ...]:
        held = []
        for instrument_id in sorted(self._instruments):
            snapshot = self._model.position(instrument_id)
            if snapshot.quantity > _ZERO:
                held.append(
                    PositionAfter(instrument_id, snapshot.quantity, snapshot.cost_basis)
                )
        return tuple(held)


# ==========================================================================
# object_store.StorageObjectWritePort
# ==========================================================================


class PersistenceStorageObjectWritePort:
    """The durable `storage.objects` writer the object store package left open.

    ``object_store.registration`` deliberately shipped only an in-memory registry
    and a fail-closed port, because `storage` ownership was contradictory. The
    persistence layer resolved that for rows: ``contribution.writable_schemas()``
    admits `storage` as a **row-only** schema (spec rule 2 and spec 2.5 both put
    `storage.objects` inside D's runtime write set) while authoring no `storage`
    DDL. This adapter is that resolution at the call site: it delegates every
    operation to ``StorageObjectRepository``, which enforces exactly the same
    lifecycle rules the in-memory registry does, in SQL.

    The persistence layer reports a lifecycle violation with its own exception
    family, so each is translated to the ``ObjectStoreConflict`` the port's other
    binding raises. Without that, a caller would have to know which binding it
    was talking to and the two would stop being interchangeable.
    """

    def __init__(self, persistence: BacktestPersistence) -> None:
        self._persistence = persistence

    def register(self, record: StorageObjectRecord) -> uuid.UUID:
        with _as_object_store_conflict(), self._persistence.unit_of_work() as uow:
            row, _ = uow.objects.register(record.to_row())
            return row.id

    def mark_available(self, object_id: uuid.UUID, verified_at: datetime) -> StorageObjectRecord:
        with _as_object_store_conflict(), self._persistence.unit_of_work() as uow:
            return _record_of(uow.objects.mark_available(object_id, verified_at))

    def quarantine(self, object_id: uuid.UUID, quarantined_at: datetime) -> StorageObjectRecord:
        with _as_object_store_conflict(), self._persistence.unit_of_work() as uow:
            return _record_of(uow.objects.quarantine(object_id, quarantined_at))

    def find(self, object_id: uuid.UUID) -> StorageObjectRecord | None:
        with self._persistence.read_only() as uow:
            row = uow.objects.find(object_id)
        return None if row is None else _record_of(row)


@contextmanager
def _as_object_store_conflict() -> Iterator[None]:
    """One conflict vocabulary for both bindings of `StorageObjectWritePort`."""
    try:
        yield
    except (PublishConflict, PersistedInvalidStatusTransition, RowNotFound) as exc:
        raise ObjectStoreConflict(str(exc)) from exc


def _record_of(row: StorageObjectRow) -> StorageObjectRecord:
    """Project a persisted `storage.objects` row back onto the value object."""
    if row.row_count is None or row.period_start is None or row.period_end is None:
        raise WiringError(
            f"storage object {row.id} was written by another producer and omits "
            "row_count or its period; it is not a backtest result object"
        )
    return StorageObjectRecord(
        object_id=row.id,
        status=ObjectStatus(row.status),
        storage_provider=row.storage_provider,
        bucket_name=row.bucket_name,
        object_key=row.object_key,
        provider_version_id=row.provider_version_id,
        content_hash=row.content_hash,
        byte_size=row.byte_size,
        file_format=row.file_format,
        compression_codec=row.compression_codec,
        media_type=row.media_type,
        schema_version=row.schema_version,
        row_count=row.row_count,
        period_start=row.period_start,
        period_end=row.period_end,
        retention_policy_version=row.retention_policy_version,
        created_at=row.created_at,
        verified_at=row.verified_at,
        encryption_key_ref=row.encryption_key_ref,
        retention_until=row.retention_until,
        legal_hold=row.legal_hold,
        quarantined_at=row.quarantined_at,
        superseded_at=row.superseded_at,
        deleted_at=row.deleted_at,
    )


# ==========================================================================
# worker.ExecutionKeyStore
# ==========================================================================


class PersistenceExecutionKeyStore:
    """The cross-process CAS, on ``backtest.run_attempts.worker_execution_key``.

    ``InMemoryExecutionKeyStore`` is the reference; this is the one that actually
    stops two workers on two machines from executing the same message twice,
    because the arbiter is the column's unique index rather than a process-local
    dictionary.

    One deliberate difference from the reference. ``worker_execution_key`` is
    ``UNIQUE`` across the whole table, so a redelivery of the same message can
    never become a *second* attempt row -- there is exactly one row per message,
    forever. ``release`` therefore returns that row to ``PENDING`` instead of
    allocating a new attempt number, and ``claim`` re-acquires a ``PENDING`` row
    with a conditional update whose affected row count is the CAS result. The
    attempt number is stable because it identifies the message, not the delivery.
    """

    def __init__(self, persistence: BacktestPersistence) -> None:
        self._persistence = persistence

    def claim(
        self, key: str, *, run_id: str, owner: str, now: datetime
    ) -> ExecutionClaim:
        run_uuid = uuid.UUID(run_id)
        with self._persistence.unit_of_work() as uow:
            existing = uow.attempts.find_by_execution_key(key)
            if existing is None:
                candidate = RunAttemptRow(
                    id=uuid.uuid5(_ATTEMPT_ID_NAMESPACE, key),
                    run_id=run_uuid,
                    attempt_number=uow.attempts.next_attempt_number(run_uuid),
                    worker_execution_key=key,
                    status=WorkStatus.RUNNING,
                    started_at=now,
                )
                try:
                    attempt, created = uow.attempts.claim(candidate)
                except PersistenceError:
                    # Another worker took this attempt slot between the count and
                    # the insert. Leave the message for redelivery rather than
                    # guessing a free number inside somebody else's race.
                    return ExecutionClaim(
                        acquired=False,
                        attempt_number=candidate.attempt_number,
                        existing_status=ExecutionRecordStatus.IN_PROGRESS,
                    )
                if created:
                    return ExecutionClaim(acquired=True, attempt_number=attempt.attempt_number)
                existing = attempt

            if existing.status is WorkStatus.PENDING:
                reclaimed = uow.connection.execute(
                    update(_run_attempts_table)
                    .where(
                        _run_attempts_table.c.worker_execution_key == key,
                        _run_attempts_table.c.status == WorkStatus.PENDING.value,
                    )
                    .values(
                        status=WorkStatus.RUNNING.value,
                        started_at=now,
                        completed_at=None,
                        failure_code=None,
                    )
                    .returning(_run_attempts_table.c.attempt_number)
                ).all()
                if reclaimed:
                    return ExecutionClaim(
                        acquired=True, attempt_number=int(reclaimed[0][0])
                    )
                existing = uow.attempts.find_by_execution_key(key)
                if existing is None:  # pragma: no cover - only on a torn write
                    raise WiringError(f"run attempt {key} vanished during the re-claim")

            return ExecutionClaim(
                acquired=False,
                attempt_number=existing.attempt_number,
                existing_status=_execution_status(existing.status),
            )

    def release(self, key: str, *, now: datetime) -> None:
        with self._persistence.unit_of_work() as uow:
            uow.connection.execute(
                update(_run_attempts_table)
                .where(_run_attempts_table.c.worker_execution_key == key)
                .values(
                    status=WorkStatus.PENDING.value,
                    completed_at=None,
                    failure_code=None,
                )
            )

    def finish(self, key: str, status: ExecutionRecordStatus, *, now: datetime) -> None:
        if status is ExecutionRecordStatus.IN_PROGRESS:
            raise ValueError("finish requires a terminal status")
        work_status = (
            WorkStatus.SUCCEEDED
            if status is ExecutionRecordStatus.SUCCEEDED
            else WorkStatus.FAILED
        )
        with self._persistence.unit_of_work() as uow:
            uow.attempts.complete(key, status=work_status, completed_at=now)

    def status(self, key: str) -> ExecutionRecordStatus | None:
        with self._persistence.read_only() as uow:
            attempt = uow.attempts.find_by_execution_key(key)
        return None if attempt is None else _execution_status(attempt.status)


def _execution_status(status: WorkStatus) -> ExecutionRecordStatus:
    if status is WorkStatus.SUCCEEDED:
        return ExecutionRecordStatus.SUCCEEDED
    if status in (WorkStatus.PENDING, WorkStatus.RUNNING):
        return ExecutionRecordStatus.IN_PROGRESS
    return ExecutionRecordStatus.FAILED


# ==========================================================================
# orchestrator.ResultPublisher
# ==========================================================================


@dataclass(frozen=True, slots=True)
class JobEnvelope:
    """What the queue message itself asserts, before anything is resolved.

    Split from :class:`JobBinding` because it is exactly the set of facts a
    ``backtest.v1`` event needs. A job whose *inputs* cannot be resolved can
    still be reported against the run it names, instead of vanishing into a
    dead-letter queue with the run row stuck at ``QUEUED``.
    """

    run_id: uuid.UUID
    bot_id: uuid.UUID
    owner_account_id: uuid.UUID
    idempotency_key: str
    input_bundle_fingerprint: str
    execution_policy_version: str
    compiled_plan_checksum: str
    dataset_manifest_id: uuid.UUID
    expected_snapshot_hash: str

    @classmethod
    def parse(cls, job: Mapping[str, Any]) -> JobEnvelope:
        try:
            return cls(
                run_id=uuid.UUID(str(job["backtestRunId"])),
                bot_id=uuid.UUID(str(job["botId"])),
                owner_account_id=uuid.UUID(str(job["ownerAccountId"])),
                idempotency_key=str(job["idempotencyKey"]),
                input_bundle_fingerprint=str(job["inputBundleFingerprint"]),
                execution_policy_version=str(job["executionPolicyVersion"]),
                compiled_plan_checksum=str(job["compiledPlanChecksum"]),
                dataset_manifest_id=uuid.UUID(str(job["datasetManifestId"])),
                expected_snapshot_hash=str(job["expectedSnapshotHash"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise JobNotSatisfiable(
                f"the queue message is not an official backtest job: {exc}",
                reason_code="MESSAGE_NOT_PARSEABLE",
            ) from exc


@dataclass(frozen=True, slots=True)
class JobBinding:
    """Everything one job pinned, resolved once and shared by every adapter."""

    envelope: JobEnvelope
    worker_execution_key: str
    manifest: Mapping[str, Any]
    policy: ExecutionPolicy
    plan: BasicCompiledPlan
    run_snapshot: RunSnapshot
    job: BacktestJob
    correlation_id: str

    @property
    def run_id(self) -> uuid.UUID:
        return self.envelope.run_id

    @property
    def input_bundle_fingerprint(self) -> str:
        return self.envelope.input_bundle_fingerprint

    @property
    def dataset_manifest_id(self) -> uuid.UUID:
        return self.envelope.dataset_manifest_id


class DurableResultPublisher:
    """Writes one completed run's evidence to the object store and PostgreSQL.

    The order is the one spec 2.5 mandates and is not an implementation detail:

    1. the immutable result-snapshot object and every ET-week detail Parquet part
       are written to the object store and registered in ``storage.objects``,
       reaching ``AVAILABLE`` only after their bytes were re-hashed;
    2. **then**, in a single transaction, ``publish_completed_run`` locks the
       input bundle, inserts the performance summary, the monthly judgment
       summaries with all six canonical counters, the ``detail_manifests`` rows
       and completes the attempt -- refusing to proceed unless every object it
       points at is already ``AVAILABLE``.

    A crash between the two leaves orphan objects, which are inert; a crash
    inside the second leaves nothing at all.
    """

    def __init__(
        self,
        *,
        binding: JobBinding,
        engine: ExecutionModelEngine,
        persistence: BacktestPersistence,
        object_store: ObjectStore,
        storage_write_port: Any,
    ) -> None:
        self._binding = binding
        self._engine = engine
        self._persistence = persistence
        self._store = object_store
        self._port = storage_write_port
        self._published: ResultSnapshot | None = None
        self._bundle: DetailObjectBundle | None = None

    @property
    def result(self) -> ResultSnapshot | None:
        """The snapshot this publisher wrote, for a caller that must read it back."""
        return self._published

    @property
    def details(self) -> DetailObjectBundle | None:
        return self._bundle

    def publish(self, request: PublishRequest) -> PublishedManifests:
        binding = self._binding
        snapshot = binding.run_snapshot
        result = ResultSnapshotBuilder().build(
            snapshot, self._engine.records, request.completed_at
        )
        ledger = tuple(
            ReplayLedgerDetail(snapshot.snapshot_id, transaction)
            for transaction in self._engine.ledger_transactions
        )
        points = tuple(
            PerformancePoint(
                point_id=str(
                    uuid.uuid5(
                        _POINT_ID_NAMESPACE,
                        f"{snapshot.snapshot_id}|equity|{point.as_of.isoformat()}",
                    )
                ),
                run_snapshot_id=snapshot.snapshot_id,
                occurred_at=point.as_of,
                metric_id="equity",
                value=point.equity,
            )
            for point in result.summary.equity_curve.points
        )
        bundle = DetailObjectBuilder().build(result, ledger, points, request.completed_at)

        self._publish_result_object(result, request.completed_at)
        published = DetailObjectPublisher(
            self._store, storage_write_port=self._port
        ).publish(bundle, verified_at=request.completed_at)

        monthly = MonthlyJudgmentBuilder().build(
            snapshot.snapshot_id,
            result.manifest.result_manifest_id,
            _judgment_evaluations(snapshot.snapshot_id, request.evaluations),
            result.records,
        )
        self._write(result, published.manifest_rows(), monthly, request.completed_at)

        self._published = result
        self._bundle = bundle
        return PublishedManifests(
            result_manifest_id=result.manifest.result_manifest_id,
            detail_manifest_id=bundle.manifest.detail_manifest_id,
            result_hash=result.summary.result_hash,
        )

    # -- steps -------------------------------------------------------------

    def _publish_result_object(self, result: ResultSnapshot, verified_at: datetime) -> None:
        """The result snapshot is an object too, and spec 2.5 wants a row for it."""
        manifest = result.manifest
        instants = [record.occurred_at for record in result.records]
        period_start = min(instants) if instants else manifest.completed_at
        period_end = max(instants) if instants else manifest.completed_at
        StorageObjectRegistrar(self._store, self._port).publish(
            object_id=uuid.uuid5(
                _RESULT_OBJECT_NAMESPACE,
                f"{manifest.run_snapshot_id}|{manifest.content_hash}",
            ),
            object_key=manifest.object_key,
            data=result.object_bytes,
            schema_version=str(manifest.schema_version),
            row_count=manifest.record_count,
            period_start=period_start,
            period_end=period_end,
            created_at=manifest.completed_at,
            verified_at=verified_at,
            expected_content_hash=manifest.content_hash,
            media_type=manifest.media_type,
            file_format=RESULT_OBJECT_FILE_FORMAT,
        )

    def _write(
        self,
        result: ResultSnapshot,
        manifest_rows: Sequence[DetailManifestRow],
        monthly: Sequence[MonthlyJudgmentSummary],
        completed_at: datetime,
    ) -> None:
        binding = self._binding
        performance = result.performance_row()
        bundle_id = uuid.uuid5(
            _RESULT_OBJECT_NAMESPACE, f"input-bundle|{binding.run_id}"
        )
        with self._persistence.unit_of_work() as uow:
            uow.inputs.lock(
                InputBundleRow(
                    id=bundle_id,
                    run_id=binding.run_id,
                    bundle_hash=binding.input_bundle_fingerprint,
                    as_of_at=binding.policy.period_end,
                    locked_at=completed_at,
                ),
                datasets=(
                    InputDatasetRow(
                        input_bundle_id=bundle_id,
                        dataset_manifest_id=binding.dataset_manifest_id,
                        purpose_code="MARKET_INPUT",
                        locked_dataset_hash=str(binding.manifest["dataset_hash"]),
                    ),
                ),
            )
            publish_completed_run(
                uow,
                RunPublication(
                    run_id=binding.run_id,
                    completed_at=completed_at,
                    result_hash=result.summary.result_hash,
                    performance=PerformanceSummaryRow(
                        run_id=binding.run_id,
                        metric_catalog_version=performance.metric_catalog_version,
                        metrics_document=dict(performance.metrics_document),
                        calculation_rules_version=performance.calculation_rules_version,
                        source_set_hash=performance.source_set_hash,
                        input_hash=performance.input_hash,
                        result_hash=performance.result_hash,
                        calculated_at=performance.calculated_at,
                    ),
                    monthly=tuple(
                        _monthly_judgment(binding.run_id, summary) for summary in monthly
                    ),
                    detail_manifests=tuple(manifest_rows),
                    worker_execution_key=binding.worker_execution_key,
                ),
            )


def _monthly_judgment(run_id: uuid.UUID, summary: MonthlyJudgmentSummary) -> MonthlyJudgment:
    summary_id = uuid.UUID(summary.summary_id)
    return MonthlyJudgment(
        summary=MonthlyJudgmentSummaryRow(
            id=summary_id,
            run_id=run_id,
            et_year_month=summary.et_month.key,
            evaluation_count=summary.evaluation_count,
            active_branch_count=summary.active_branch_count,
            trade_event_count=summary.trade_event_count,
            data_gap_count=summary.data_gap_count,
            triggered_count=summary.triggered_count,
            rejected_count=summary.rejected_count,
            summary_document=dict(summary.summary_document),
            summary_hash=summary.summary_hash,
        ),
        failure_counts=tuple(
            FailureConditionCountRow(
                id=uuid.uuid5(
                    _RESULT_OBJECT_NAMESPACE,
                    f"failure|{summary.summary_id}|{item.scope_id}|{item.condition_id}",
                ),
                monthly_summary_id=summary_id,
                flow_or_branch_key=item.scope_id,
                first_failure_condition_key=item.condition_id,
                occurrence_count=item.count,
            )
            for item in summary.failure_counts
        ),
    )


def _judgment_evaluations(
    run_snapshot_id: str, evaluations: Sequence[Any]
) -> tuple[JudgmentEvaluation, ...]:
    """Reduce the replay's plan evaluations to the transient monthly inputs.

    ``data_gap`` and ``trade_occurred`` are stated, never inferred: a skipped
    instant and an instant where every condition passed both produce an empty
    failure set, and the monthly counters must tell them apart.
    """
    reduced: list[JudgmentEvaluation] = []
    for evaluation in evaluations:
        if not isinstance(evaluation, PlanEvaluation):  # pragma: no cover - defensive
            raise WiringError("replay evaluations must be basic_runtime.PlanEvaluation values")
        data_gap = evaluation.skip_reason is ReplaySkipReason.DATA_GAP_EVALUATION_SKIPPED
        reduced.append(
            JudgmentEvaluation(
                evaluation_id=evaluation.evaluation_id,
                run_snapshot_id=run_snapshot_id,
                evaluated_at=evaluation.occurred_at,
                mode=StrategyMode.BASIC,
                trade_occurred=bool(evaluation.candidates),
                data_gap=data_gap,
                basic_outcomes=() if data_gap else _condition_outcomes(evaluation),
            )
        )
    return tuple(reduced)


def _condition_outcomes(evaluation: PlanEvaluation) -> tuple[ConditionOutcome, ...]:
    """One outcome per (instrument, step), in the plan's own decision order.

    The monthly histogram keeps only the *first* failing outcome of an
    evaluation, so the order is load-bearing and is the runtime's, not ours.
    """
    outcomes: list[ConditionOutcome] = []
    for decision in evaluation.decisions:
        for trace in decision.trace:
            outcomes.append(
                ConditionOutcome(f"{decision.instrument_id}|{trace.step_id}", trace.passed)
            )
        if decision.status is BasicDecisionStatus.INPUT_MISSING and not decision.trace:
            # An instrument the plan never received data for produced no step
            # trace at all; without this the month would report "no failures" for
            # an evaluation that never ran. A warm-up shortfall *does* leave a
            # trace entry, and that entry is already the failure.
            outcomes.append(
                ConditionOutcome(
                    f"{decision.instrument_id}|{decision.first_failure_step_id}", False
                )
            )
    return tuple(outcomes)


# ==========================================================================
# Job assembly
# ==========================================================================


def dataset_coverage(manifest: Mapping[str, Any]) -> tuple[datetime, datetime]:
    """The union of the pinned objects' declared coverage, in UTC.

    The manifest's own ``period_start``/``period_end`` describe the *dataset's*
    window, which the execution policy pins to whole ET quarters. The objects
    describe what was actually delivered, and that is what a run may evaluate
    over.
    """
    objects = manifest["objects"]
    if not objects:  # pragma: no cover - the schema requires minItems 1
        raise JobNotSatisfiable(
            "the pinned dataset manifest carries no objects",
            reason_code="INPUT_DATASET_UNREADABLE",
        )
    starts = [_parse_instant(item["period_start"]) for item in objects]
    ends = [_parse_instant(item["period_end"]) for item in objects]
    return min(starts), max(ends)


def evaluation_window(
    manifest: Mapping[str, Any], plan: BasicCompiledPlan
) -> tuple[datetime, datetime]:
    """Where warm-up ends and evaluation begins, for this plan on this dataset.

    The pinned dataset is the reproducibility boundary, so it -- not a separate
    configuration value -- decides the window: the longest feature warm-up the
    plan declares is consumed from the front of the delivered coverage, and
    everything after it is evaluated. Two runs of the same request therefore
    evaluate exactly the same instants without anyone restating them.
    """
    coverage_start, coverage_end = dataset_coverage(manifest)
    warmup = max(
        (feature.warmup_span for feature in plan.required_features),
        default=timedelta(0),
    )
    evaluation_from = coverage_start + warmup
    if evaluation_from >= coverage_end:
        raise JobNotSatisfiable(
            "the pinned dataset is shorter than the plan's warm-up window: "
            f"coverage {coverage_start.isoformat()}..{coverage_end.isoformat()} "
            f"cannot absorb {warmup}",
            reason_code="REQUIRED_DATA_UNAVAILABLE",
        )
    return evaluation_from, coverage_end


def _parse_instant(value: object) -> datetime:
    if not isinstance(value, str):  # pragma: no cover - the schema requires a string
        raise JobNotSatisfiable(
            f"dataset manifest timestamps must be strings, got {value!r}",
            reason_code="INPUT_DATASET_UNREADABLE",
        )
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class ResultSink(Protocol):
    """Where a worker publishes its ``backtest.v1`` result events.

    The production binding is an HTTP client for
    ``POST /api/v1/backtests/{id}/results``; ``delivery_attempt`` becomes the
    ``X-Delivery-Attempt`` header the API reads to decide when a failing event
    has exhausted its redelivery budget.
    """

    def publish(self, event: Mapping[str, Any], *, delivery_attempt: int) -> None: ...


class OrchestratorJobHandler:
    """``worker.JobHandler``: one queue message becomes one orchestrated run.

    The handler is where the queue's vocabulary (a JSON body and a delivery
    count) meets the domain's (a pinned :class:`BacktestJob`). It resolves every
    input the message *names* but does not carry, refuses the job outright when
    one cannot be resolved, and reports each state change back through
    :class:`ResultSink` so the run row and the queue never disagree.

    Two reporting rules follow from ``backtest.run_status`` being a one-way
    lifecycle, and neither is optional:

    * a ``RUNNING`` event is published on the **first** delivery only. A
      redelivery of the same message under the same attempt would carry the same
      content-bound idempotency key with a different envelope, which the
      ingestion endpoint correctly rejects as a conflict.
    * a **retryable** failure publishes nothing. ``FAILED`` is terminal in
      ``backtest.run_status``, so announcing a failure the worker intends to
      retry would make the retry unable to complete. The attempt row records it;
      the queue redelivers; the run stays ``RUNNING``.

    ``attempt_policy.max_attempts`` must be greater than 1. The coordinator
    polices one delivery, and its ``WAITING`` state is what tells the handler a
    failure was retryable; with a budget of one every failure would read as
    exhausted. The number of *deliveries* is bounded by the queue's
    ``max_receive_count``, not by this policy.
    """

    def __init__(
        self,
        *,
        persistence: BacktestPersistence,
        policies: ExecutionPolicyCatalog,
        plans: Any,
        manifests: Any,
        reader: Any,
        calendar: SessionCalendar,
        object_store: ObjectStore,
        storage_write_port: Any,
        sink: ResultSink,
        attempt_policy: AttemptPolicy,
        monitor: ResourceMonitor,
        microstructure: ExecutionMicrostructurePolicy,
        fractional_policy: InstrumentFractionalPolicy,
        risk_limits: RiskLimits,
        runtime: BasicPlanRuntime,
        wall_clock: Callable[[], datetime],
        correlation_id: str,
        publication_lag: timedelta = timedelta(0),
    ) -> None:
        if attempt_policy.max_attempts < 2:
            raise WiringError(
                "attempt_policy.max_attempts must exceed 1: with a budget of one the "
                "coordinator reports every failure as exhausted and no retryable "
                "failure could ever be redelivered"
            )
        self._persistence = persistence
        self._policies = policies
        self._plans = plans
        self._manifests = manifests
        self._reader = reader
        self._calendar = calendar
        self._store = object_store
        self._port = storage_write_port
        self._sink = sink
        self._attempt_policy = attempt_policy
        self._monitor = monitor
        self._microstructure = microstructure
        self._fractional = fractional_policy
        self._risk_limits = risk_limits
        self._runtime = runtime
        self._wall_clock = wall_clock
        self._correlation_id = correlation_id
        self._publication_lag = publication_lag
        self.last_outcome: ReplayOutcome | None = None
        self.last_publisher: DurableResultPublisher | None = None

    # -- JobHandler --------------------------------------------------------

    def __call__(self, job: Mapping[str, Any], context: JobContext) -> JobOutcome:
        try:
            envelope = JobEnvelope.parse(job)
        except JobNotSatisfiable as exc:
            # Not even addressable: there is no run to report against.
            return JobOutcome(JobResult.PERMANENT_FAILURE, reason_code=exc.reason_code)

        try:
            binding = self.bind(envelope, context)
        except JobNotSatisfiable as exc:
            self._publish(
                envelope,
                self._correlation_id,
                status="FAILED",
                delivery_attempt=context.receive_count,
                failedAt=_utc_text(self._wall_clock()),
                attempt=context.attempt_number,
                failureCode=exc.reason_code,
                retryable=False,
            )
            return JobOutcome(JobResult.PERMANENT_FAILURE, reason_code=exc.reason_code)

        started_at = self._wall_clock()
        if context.receive_count == 1:
            self._publish(
                binding.envelope,
                binding.correlation_id,
                status="RUNNING",
                delivery_attempt=context.receive_count,
                startedAt=_utc_text(started_at),
                attempt=context.attempt_number,
            )

        engine = ExecutionModelEngine(
            model=BacktestExecutionModel(
                binding.policy,
                binding.job.initial_cash,
                self._risk_limits,
                microstructure=self._microstructure,
                fractional_policy=self._fractional,
            ),
            run_snapshot=binding.run_snapshot,
            policy=binding.policy,
            fractional_policy=self._fractional,
        )
        publisher = DurableResultPublisher(
            binding=binding,
            engine=engine,
            persistence=self._persistence,
            object_store=self._store,
            storage_write_port=self._port,
        )
        self.last_publisher = publisher

        coordinator = AttemptCoordinator(str(binding.run_id), self._attempt_policy, started_at)
        lease = coordinator.acquire(context.worker_id, started_at)
        orchestrator = BacktestOrchestrator(
            reader=self._reader,
            calendar=self._calendar,
            replay_factory=BasicPlanReplayFactory(self._runtime, binding.plan),
            engine=engine,
            publisher=publisher,
            wall_clock=self._wall_clock,
            publication_lag=self._publication_lag,
        )
        outcome = orchestrator.run(
            binding.job, coordinator=coordinator, lease=lease, monitor=self._monitor
        )
        self.last_outcome = outcome
        return self._report(binding, outcome, coordinator, context)

    # -- binding -----------------------------------------------------------

    def bind(self, envelope: JobEnvelope, context: JobContext) -> JobBinding:
        """Resolve every input the message names. Raises `JobNotSatisfiable`."""
        try:
            policy = self._policies.get(envelope.execution_policy_version)
        except ExecutionPolicyUnavailable as exc:
            raise JobNotSatisfiable(str(exc), reason_code="REQUIRED_INPUT_UNAVAILABLE") from exc

        plan_checksum = envelope.compiled_plan_checksum
        plan_document = self._plans.by_checksum(plan_checksum)
        if plan_document is None:
            raise JobNotSatisfiable(
                f"compiled plan {plan_checksum} is not resolvable",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
            )
        manifest = self._manifests.by_id(envelope.dataset_manifest_id)
        if manifest is None:
            raise JobNotSatisfiable(
                f"dataset manifest {envelope.dataset_manifest_id} is not resolvable",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
            )

        plan = self._runtime.load(plan_document, compiled_plan_checksum=plan_checksum)
        evaluation_from, evaluation_through = evaluation_window(manifest, plan)
        requirements = derive_data_requirements(
            plan, evaluation_from=evaluation_from, evaluation_through=evaluation_through
        )
        if not requirements:  # pragma: no cover - a loaded plan always declares one
            raise JobNotSatisfiable(
                "the compiled plan declares no data requirement",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
            )
        data_kind, resolution = plan.reference_series

        run_snapshot = RunSnapshot(
            backtest_run_id=str(envelope.run_id),
            # `RunSnapshot` predates spec 2.2's move from `strategy_version_id` to
            # `bot_id` + `owner_account_id`. The bot id is the canonical identity,
            # so it is what goes in rather than an invented version row.
            strategy_version_id=str(envelope.bot_id),
            input_bundle_fingerprint=envelope.input_bundle_fingerprint.removeprefix("sha256:"),
            calculation_model_version=policy.calculation_model_version,
            cost_model_version=COST_MODEL_VERSION,
            execution_model_version=EXECUTION_MODEL_VERSION,
            initial_cash=plan.initial_cash,
        )
        try:
            backtest_job = BacktestJob(
                run_id=str(envelope.run_id),
                idempotency_key=envelope.idempotency_key,
                worker_execution_key=context.worker_execution_key,
                manifest=manifest,
                execution_policy=policy,
                requirements=requirements,
                data_kind=data_kind,
                resolution=resolution,
                initial_cash=plan.initial_cash,
            )
        except OrchestratorError as exc:
            raise JobNotSatisfiable(str(exc), reason_code="REQUIRED_INPUT_UNAVAILABLE") from exc

        return JobBinding(
            envelope=envelope,
            worker_execution_key=context.worker_execution_key,
            manifest=manifest,
            policy=policy,
            plan=plan,
            run_snapshot=run_snapshot,
            job=backtest_job,
            correlation_id=self._correlation_id,
        )

    # -- reporting ---------------------------------------------------------

    def _report(
        self,
        binding: JobBinding,
        outcome: ReplayOutcome,
        coordinator: AttemptCoordinator,
        context: JobContext,
    ) -> JobOutcome:
        now = self._wall_clock()
        envelope = binding.envelope
        if outcome.status is ReplayStatus.COMPLETED:
            if outcome.result_hash is None or outcome.result_manifest_id is None:
                raise WiringError(
                    "a COMPLETED outcome must carry the manifests it published"
                )
            self._publish(
                envelope,
                binding.correlation_id,
                status="COMPLETED",
                delivery_attempt=context.receive_count,
                completedAt=_utc_text(now),
                attempt=context.attempt_number,
                resultManifestId=outcome.result_manifest_id,
                resultHash=_prefixed(outcome.result_hash),
            )
            return JobOutcome(JobResult.SUCCEEDED, result_hash=outcome.result_hash)

        reason_code = outcome.reason_code or outcome.status.value
        if outcome.status is ReplayStatus.UNAVAILABLE:
            self._publish(
                envelope,
                binding.correlation_id,
                status="UNAVAILABLE",
                delivery_attempt=context.receive_count,
                decidedAt=_utc_text(now),
                reasonCode=reason_code,
                missingRequirements=list(outcome.missing_requirements) or [reason_code],
            )
            return JobOutcome(JobResult.PERMANENT_FAILURE, reason_code=reason_code)

        if coordinator.state is RunState.WAITING:
            # Retryable. `FAILED` is terminal in `backtest.run_status`, so
            # publishing it here would lock the run out of the very retry the
            # worker is about to make. The attempt row is the durable record.
            return JobOutcome(JobResult.RETRY, reason_code=reason_code)

        self._publish(
            envelope,
            binding.correlation_id,
            status="FAILED",
            delivery_attempt=context.receive_count,
            failedAt=_utc_text(now),
            attempt=context.attempt_number,
            failureCode=reason_code,
            retryable=False,
        )
        return JobOutcome(JobResult.PERMANENT_FAILURE, reason_code=reason_code)

    def _publish(
        self,
        envelope: JobEnvelope,
        correlation_id: str,
        *,
        status: str,
        delivery_attempt: int,
        **detail: Any,
    ) -> None:
        event = build_backtest_result_event(
            status=status,
            backtest_run_id=str(envelope.run_id),
            bot_id=str(envelope.bot_id),
            owner_account_id=str(envelope.owner_account_id),
            expected_snapshot_hash=envelope.expected_snapshot_hash,
            input_bundle_fingerprint=envelope.input_bundle_fingerprint,
            execution_policy_version=envelope.execution_policy_version,
            precision_rules_version=PRECISION_RULES_VERSION,
            message_id=str(
                uuid.uuid5(
                    _RESULT_OBJECT_NAMESPACE,
                    f"event|{envelope.run_id}|{status}|{delivery_attempt}",
                )
            ),
            occurred_at=_utc_text(self._wall_clock()),
            correlation_id=correlation_id,
            **detail,
        )
        self._sink.publish(event, delivery_attempt=delivery_attempt)
