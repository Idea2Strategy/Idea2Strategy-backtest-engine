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
``api.create_app`` (whole process)             :func:`build_api_runtime`
=============================================  ==================================

The API half of the deployment
------------------------------
:func:`build_api_runtime` is the other end of the same wire. The worker writes
``backtest.*`` and the object store through :class:`DurableResultPublisher`;
the API reads them back through
:class:`~backtest_engine.result_query.DurableBacktestResultQueryStore`, which is
built here and handed to ``create_app``. Before this, ``wiring`` built no query
store at all and the two D29 read routes correctly answered 503 in every real
deployment: the worker's output was unreachable over HTTP.

Both entry points resolve their collaborators the same way -- required
environment settings, and ``package.module:factory`` targets through
:func:`worker.load_factory` -- so ``backtest-api`` and ``backtest-worker`` are
configured alike and neither can start half-wired.

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

from sqlalchemy import select

from .api import create_app
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
    DetailIntegrityError,
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectPublisher,
    PerformancePoint,
    ReplayLedgerDetail,
)
from .elements import PinnedFeatureSeries
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
from .feature_outputs import (
    FeatureObjectReader,
    FeatureOutputBindingError,
    resolve_feature_materialization_pins,
)
from .lifecycle import BacktestLifecycleService, PersistenceRunGateway, SqsBacktestJobQueue
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
    ResultPublicationError,
    SessionCalendar,
)
from .persistence import (
    BacktestPersistence,
    DetailManifestRow,
    FailureConditionCountRow,
    InputBundleRow,
    InputDatasetRow,
    InputFeatureMaterializationRow,
    MonthlyJudgment,
    MonthlyJudgmentSummaryRow,
    ObjectStatus,
    PerformanceSummaryRow,
    RunPublication,
    StorageObjectRow,
    WorkStatus,
    create_backtest_engine,
    publish_completed_run,
)
from .persistence.errors import InvalidStatusTransition as PersistedInvalidStatusTransition
from .persistence.errors import PublishConflict, RowNotFound
from .persistence.tables import run_attempts as _run_attempts_table
from .result_query import BacktestResultQueryService, DurableBacktestResultQueryStore
from .result_snapshot import (
    PositionAfter,
    ResultIntegrityError,
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
    WorkerConfigurationError,
    load_factory,
)


__all__ = [
    "API_REQUIRED_ENV",
    "COST_MODEL_VERSION",
    "EXECUTION_MODEL_VERSION",
    "RESULT_OBJECT_FILE_FORMAT",
    "SIZING_RULES_VERSION",
    "WIRING_VERSION",
    "ApiRuntime",
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
    "build_api_runtime",
    "build_result_query_service",
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


def _pinned_hash(value: Any) -> str:
    digest = _prefixed(str(value))
    payload = digest.removeprefix("sha256:")
    if len(payload) != 64 or any(character not in "0123456789abcdef" for character in payload):
        raise ValueError("pinned hashes must use sha256:<64 lowercase hex>")
    return digest


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
    feature_series: tuple[PinnedFeatureSeries, ...] = ()
    require_pinned_features: bool = False

    def __call__(self, *, clock: MarketEventClock, assessment: AvailabilityAssessment) -> PlanReplay:
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
                runtime=self.runtime,
                plan=self.plan,
                clock=clock,
                assessment=assessment,
                feature_series=self.feature_series,
                require_pinned_features=self.require_pinned_features,
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
                f"{self._run.snapshot_id}|{candidate.evaluation_id}|{candidate.flow_id}|{instrument_id}",
            )
        )
        request = OrderRequest(
            order_id=order_id,
            instrument_id=instrument_id,
            side=side,
            order_type=OrderType(candidate.order_type),
            quantity=quantity,
            quantity_mode=(QuantityMode.FRACTIONAL_SHARES if fractional else QuantityMode.WHOLE_SHARES),
            time_in_force=TimeInForce.DAY,
            submitted_at=candidate.decided_at,
            eligible_at=candidate.eligible_at,
            day_expires_at=candidate.session_closes_at,
            reference_price=candidate.reference_price,
        )
        order = self._model.submit(request)
        self._records.append(
            order_result_record(self._run, order, candidate.decided_at, self._model.cash, self._positions())
        )
        return order_id if order.status is OrderStatus.ACCEPTED else None

    def settle(self, event: MarketDataEvent) -> int:
        bar = self._bar_of(event)
        for expired in self._model.advance_time(bar.starts_at):
            self._records.append(
                order_result_record(self._run, expired, bar.starts_at, self._model.cash, self._positions())
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
            ledger_entry_count=sum(len(transaction.entries) for transaction in self._model.ledger_transactions),
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
                "a BUY candidate must carry an allocation share; sizing one without it would invent a position size"
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
        unit_cash = quantize_money(unit_price + apply_rate(unit_price, self._policy.fee_rate, "unit_fee"), "unit_cash")
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
                held.append(PositionAfter(instrument_id, snapshot.quantity, snapshot.cost_basis))
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

    Claims are leased with database time. Every retry gets a fresh attempt row and
    fencing token; a late heartbeat or completion from an expired worker therefore
    cannot mutate the successor attempt.
    """

    def __init__(self, persistence: BacktestPersistence) -> None:
        self._persistence = persistence

    def claim(
        self,
        key: str,
        *,
        run_id: str,
        owner: str,
        now: datetime,
        lease_duration: timedelta | None = None,
    ) -> ExecutionClaim:
        if lease_duration is None:
            raise ValueError("persistent execution claims require a lease duration")
        run_uuid = uuid.UUID(run_id)
        with self._persistence.unit_of_work() as uow:
            attempt = uow.attempts.claim_fenced(
                run_uuid,
                worker_id=owner,
                execution_key=key,
                lease_duration=lease_duration,
            )
            if attempt is not None:
                return ExecutionClaim(
                    acquired=True,
                    attempt_number=attempt.attempt_number,
                    attempt_id=str(attempt.id),
                    claim_token=str(attempt.claim_token),
                )
            existing = uow.attempts.latest_for_run(run_uuid)
            return ExecutionClaim(
                acquired=False,
                attempt_number=0 if existing is None else existing.attempt_number,
                existing_status=None if existing is None else _execution_status(existing.status),
            )

    @staticmethod
    def _claim_ids(claim: ExecutionClaim | None) -> tuple[uuid.UUID, uuid.UUID]:
        if claim is None or claim.attempt_id is None or claim.claim_token is None:
            raise ValueError("persistent execution mutation requires its fencing claim")
        return uuid.UUID(claim.attempt_id), uuid.UUID(claim.claim_token)

    def heartbeat(self, key: str, claim: ExecutionClaim, *, lease_duration: timedelta) -> None:
        attempt_id, claim_token = self._claim_ids(claim)
        with self._persistence.unit_of_work() as uow:
            uow.attempts.heartbeat_fenced(attempt_id, claim_token, lease_duration=lease_duration)

    def release(self, key: str, *, now: datetime, claim: ExecutionClaim | None = None) -> None:
        attempt_id, claim_token = self._claim_ids(claim)
        with self._persistence.unit_of_work() as uow:
            uow.attempts.release_fenced(attempt_id, claim_token, terminal_reason_code="RETRY_RELEASED")

    def finish(
        self,
        key: str,
        status: ExecutionRecordStatus,
        *,
        now: datetime,
        claim: ExecutionClaim | None = None,
    ) -> None:
        if status is ExecutionRecordStatus.IN_PROGRESS:
            raise ValueError("finish requires a terminal status")
        attempt_id, claim_token = self._claim_ids(claim)
        work_status = WorkStatus.SUCCEEDED if status is ExecutionRecordStatus.SUCCEEDED else WorkStatus.FAILED
        with self._persistence.unit_of_work() as uow:
            uow.attempts.close_fenced(
                attempt_id,
                claim_token,
                status=work_status,
                terminal_reason_code=status.value,
                failure_code=None if work_status is WorkStatus.SUCCEEDED else "EXECUTION_FAILED",
            )

    def status(self, key: str) -> ExecutionRecordStatus | None:
        with self._persistence.read_only() as uow:
            attempt = (
                uow.connection.execute(
                    select(_run_attempts_table)
                    .where(_run_attempts_table.c.worker_execution_key.startswith(f"{key}:", autoescape=True))
                    .order_by(_run_attempts_table.c.attempt_number.desc())
                    .limit(1)
                )
                .mappings()
                .first()
            )
        return None if attempt is None else _execution_status(WorkStatus(attempt["status"]))


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
class DatasetPin:
    manifest_id: uuid.UUID
    purpose_code: str
    expected_hash: str | None


@dataclass(frozen=True, slots=True)
class FeatureMaterializationPin:
    materialization_id: uuid.UUID
    locked_result_hash: str


def verify_feature_materialization_pins(
    pins: Sequence[FeatureMaterializationPin], source: Any | None
) -> None:
    """Verify every immutable feature output, then fail closed until consumption is defined."""

    if not pins:
        return
    if source is None:
        raise JobNotSatisfiable(
            "feature materialization verification is unavailable",
            reason_code="REQUIRED_INPUT_UNAVAILABLE",
        )
    for pin in pins:
        feature = source.by_id(pin.materialization_id)
        if (
            feature is None
            or feature.get("status") != "SUCCEEDED"
            or _prefixed(str(feature.get("result_hash", ""))) != pin.locked_result_hash
            or feature.get("output_dataset_manifest_id") is None
            or feature.get("output_dataset_status") != "AVAILABLE"
            or not feature.get("output_dataset_hash")
        ):
            raise JobNotSatisfiable(
                f"feature materialization {pin.materialization_id} is missing or changed",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
            )
    raise JobNotSatisfiable(
        "locked feature outputs cannot yet be bound to compiled-plan inputs without canonical semantics",
        reason_code="FEATURE_OUTPUT_CONSUMPTION_UNSUPPORTED",
    )


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
    expected_dataset_hash: str | None
    expected_snapshot_hash: str
    datasets: tuple[DatasetPin, ...]
    feature_materializations: tuple[FeatureMaterializationPin, ...]
    feature_materialization_version: str
    evaluation_period_id: uuid.UUID | None
    input_set_hash: str | None

    @classmethod
    def parse(cls, job: Mapping[str, Any]) -> JobEnvelope:
        try:
            raw_datasets = job.get("datasets") or (
                {
                    "datasetManifestId": job["datasetManifestId"],
                    "purposeCode": "MARKET_BARS",
                    "expectedDatasetHash": job.get("expectedDatasetHash"),
                },
            )
            datasets = tuple(
                DatasetPin(
                    manifest_id=uuid.UUID(str(item["datasetManifestId"])),
                    purpose_code=str(item["purposeCode"]),
                    expected_hash=(
                        _pinned_hash(item["expectedDatasetHash"])
                        if item.get("expectedDatasetHash") is not None
                        else None
                    ),
                )
                for item in raw_datasets
            )
            features = tuple(
                FeatureMaterializationPin(
                    materialization_id=uuid.UUID(str(item["featureMaterializationId"])),
                    locked_result_hash=_pinned_hash(item["lockedResultHash"]),
                )
                for item in job.get("featureMaterializations", ())
            )
            if not datasets:
                raise ValueError("datasets must not be empty")
            representative_id = uuid.UUID(str(job["datasetManifestId"]))
            representative_hash = (
                _pinned_hash(job["expectedDatasetHash"])
                if job.get("expectedDatasetHash") is not None
                else None
            )
            representatives = [pin for pin in datasets if pin.manifest_id == representative_id]
            if len(representatives) != 1 or representatives[0].expected_hash != representative_hash:
                raise ValueError("representative dataset must match exactly one dataset pin")
            return cls(
                run_id=uuid.UUID(str(job["backtestRunId"])),
                bot_id=uuid.UUID(str(job["botId"])),
                owner_account_id=uuid.UUID(str(job["ownerAccountId"])),
                idempotency_key=str(job["idempotencyKey"]),
                input_bundle_fingerprint=_pinned_hash(job["inputBundleFingerprint"]),
                execution_policy_version=str(job["executionPolicyVersion"]),
                compiled_plan_checksum=_pinned_hash(job["compiledPlanChecksum"]),
                dataset_manifest_id=representative_id,
                expected_dataset_hash=(
                    representative_hash
                    if job.get("expectedDatasetHash") is not None
                    else None
                ),
                expected_snapshot_hash=_pinned_hash(job["expectedSnapshotHash"]),
                datasets=datasets,
                feature_materializations=features,
                feature_materialization_version=str(
                    job.get("featureMaterializationVersion", "legacy-unspecified")
                ),
                evaluation_period_id=(
                    uuid.UUID(str(job["evaluationPeriodId"]))
                    if job.get("evaluationPeriodId") is not None
                    else None
                ),
                input_set_hash=(
                    _pinned_hash(job["inputSetHash"])
                    if job.get("inputSetHash") is not None
                    else None
                ),
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
    attempt_id: uuid.UUID | None
    claim_token: uuid.UUID | None
    manifest: Mapping[str, Any]
    policy: ExecutionPolicy
    plan: BasicCompiledPlan
    run_snapshot: RunSnapshot
    job: BacktestJob
    correlation_id: str
    manifests: tuple[tuple[DatasetPin, Mapping[str, Any]], ...]
    feature_series: tuple[PinnedFeatureSeries, ...] = ()

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

    #: Failures that cannot become successes on a redelivery. Each one is a statement
    #: about *content*, not about the machine: the run already has a different
    #: immutable result, the run has left `RUNNING`, an object id is taken by different
    #: bytes, or the evidence does not verify against its own hashes. Retrying any of
    #: them burns the whole delivery budget and dead-letters with the reason hidden.
    _PERMANENT = (
        PublishConflict,
        PersistedInvalidStatusTransition,
        RowNotFound,
        ObjectStoreConflict,
        DetailIntegrityError,
        ResultIntegrityError,
    )

    def publish(self, request: PublishRequest) -> PublishedManifests:
        """Write the evidence, classifying a failure the orchestrator cannot classify.

        Anything not in :data:`_PERMANENT` -- a dropped connection, an object store
        timeout, a verification that may succeed on a second read -- propagates
        unclassified and the orchestrator retries it.
        """
        try:
            return self._publish(request)
        except ResultPublicationError:
            raise
        except self._PERMANENT as exc:
            raise ResultPublicationError(
                f"{type(exc).__name__}: {exc}",
                retryable=False,
                reason_code="RESULT_PUBLICATION_CONFLICT",
            ) from exc

    def _publish(self, request: PublishRequest) -> PublishedManifests:
        binding = self._binding
        snapshot = binding.run_snapshot
        result = ResultSnapshotBuilder().build(snapshot, self._engine.records, request.completed_at)
        ledger = tuple(
            ReplayLedgerDetail(snapshot.snapshot_id, transaction) for transaction in self._engine.ledger_transactions
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
        published = DetailObjectPublisher(self._store, storage_write_port=self._port).publish(
            bundle, verified_at=request.completed_at
        )

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
        bundle_id = uuid.uuid5(_RESULT_OBJECT_NAMESPACE, f"input-bundle|{binding.run_id}")
        with self._persistence.unit_of_work() as uow:
            uow.inputs.lock(
                InputBundleRow(
                    id=bundle_id,
                    run_id=binding.run_id,
                    bundle_hash=binding.input_bundle_fingerprint,
                    as_of_at=binding.policy.period_end,
                    locked_at=completed_at,
                ),
                datasets=tuple(
                    InputDatasetRow(
                        input_bundle_id=bundle_id,
                        dataset_manifest_id=pin.manifest_id,
                        purpose_code=pin.purpose_code,
                        locked_dataset_hash=str(manifest["dataset_hash"]),
                    )
                    for pin, manifest in binding.manifests
                ),
                features=tuple(
                    InputFeatureMaterializationRow(
                        input_bundle_id=bundle_id,
                        feature_materialization_id=pin.materialization_id,
                        locked_result_hash=pin.locked_result_hash,
                    )
                    for pin in binding.envelope.feature_materializations
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
                    monthly=tuple(_monthly_judgment(binding.run_id, summary) for summary in monthly),
                    detail_manifests=tuple(manifest_rows),
                    worker_execution_key=binding.worker_execution_key,
                    attempt_id=binding.attempt_id,
                    claim_token=binding.claim_token,
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


def _judgment_evaluations(run_snapshot_id: str, evaluations: Sequence[Any]) -> tuple[JudgmentEvaluation, ...]:
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
            outcomes.append(ConditionOutcome(f"{decision.instrument_id}|{trace.step_id}", trace.passed))
        if decision.status is BasicDecisionStatus.INPUT_MISSING and not decision.trace:
            # An instrument the plan never received data for produced no step
            # trace at all; without this the month would report "no failures" for
            # an evaluation that never ran. A warm-up shortfall *does* leave a
            # trace entry, and that entry is already the failure.
            outcomes.append(ConditionOutcome(f"{decision.instrument_id}|{decision.first_failure_step_id}", False))
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


def evaluation_window(manifest: Mapping[str, Any], plan: BasicCompiledPlan) -> tuple[datetime, datetime]:
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
        feature_materializations: Any | None = None,
        feature_object_reader: FeatureObjectReader | None = None,
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
        self._feature_materializations = feature_materializations
        self._feature_object_reader = feature_object_reader
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
            replay_factory=BasicPlanReplayFactory(
                self._runtime,
                binding.plan,
                feature_series=binding.feature_series,
                require_pinned_features=bool(binding.feature_series),
            ),
            engine=engine,
            publisher=publisher,
            wall_clock=self._wall_clock,
            publication_lag=self._publication_lag,
        )
        outcome = orchestrator.run(binding.job, coordinator=coordinator, lease=lease, monitor=self._monitor)
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
        resolved_manifests: list[tuple[DatasetPin, Mapping[str, Any]]] = []
        for pin in envelope.datasets:
            resolved = self._manifests.by_id(pin.manifest_id)
            if resolved is None or (
                pin.expected_hash is not None
                and _prefixed(str(resolved["dataset_hash"])) != pin.expected_hash
            ):
                raise JobNotSatisfiable(
                    f"dataset manifest {pin.manifest_id} is missing or changed",
                    reason_code="REQUIRED_INPUT_UNAVAILABLE",
                )
            resolved_manifests.append((pin, resolved))
        primary = [item for item in resolved_manifests if item[0].manifest_id == envelope.dataset_manifest_id]
        if len(primary) != 1:
            raise JobNotSatisfiable(
                "the representative dataset is not pinned exactly once",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
            )
        manifest = primary[0][1]
        plan = self._runtime.load(plan_document, compiled_plan_checksum=plan_checksum)
        evaluation_from, evaluation_through = evaluation_window(manifest, plan)
        feature_series: tuple[PinnedFeatureSeries, ...] = ()
        if self._feature_materializations is not None or envelope.feature_materializations:
            if self._feature_materializations is None or self._feature_object_reader is None:
                raise JobNotSatisfiable(
                    "feature materialization binding is unavailable",
                    reason_code="REQUIRED_INPUT_UNAVAILABLE",
                )
            try:
                feature_series = resolve_feature_materialization_pins(
                    plan=plan,
                    pins=envelope.feature_materializations,
                    source=self._feature_materializations,
                    reader=self._feature_object_reader,
                    evaluation_from=evaluation_from,
                    evaluation_through=evaluation_through,
                )
            except FeatureOutputBindingError as exc:
                raise JobNotSatisfiable(
                    str(exc), reason_code="REQUIRED_INPUT_UNAVAILABLE"
                ) from exc
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
            attempt_id=(uuid.UUID(context.attempt_id) if context.attempt_id is not None else None),
            claim_token=(uuid.UUID(context.claim_token) if context.claim_token is not None else None),
            manifest=manifest,
            policy=policy,
            plan=plan,
            run_snapshot=run_snapshot,
            job=backtest_job,
            correlation_id=self._correlation_id,
            manifests=tuple(resolved_manifests),
            feature_series=feature_series,
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
                raise WiringError("a COMPLETED outcome must carry the manifests it published")
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


# ==========================================================================
# The API process
# ==========================================================================


#: Every setting `build_api_runtime` refuses to start without. Nine of them are
#: `package.module:factory` targets, for the same reason
#: ``BACKTEST_JOB_HANDLER`` is one: the owner directory, the compiled-plan source,
#: the dataset-manifest source, the execution-policy catalog, the dead-letter sink,
#: the token source and the object store are all deployment decisions, and a default
#: for any of them is the hidden policy the rebuild exists to remove.
API_REQUIRED_ENV: tuple[str, ...] = (
    "BACKTEST_DATABASE_URL",
    "BACKTEST_QUEUE_URL",
    "BACKTEST_API_HOST",
    "BACKTEST_API_PORT",
    "BACKTEST_AUTHENTICATOR",
    "BACKTEST_OBJECT_STORE",
    "BACKTEST_OWNER_DIRECTORY",
    "BACKTEST_COMPILED_PLAN_SOURCE",
    "BACKTEST_DATASET_MANIFEST_SOURCE",
    "BACKTEST_EXECUTION_POLICY_CATALOG",
    "BACKTEST_DEAD_LETTER_SINK",
)

#: Factory settings, in the order they are resolved. Kept beside
#: :data:`API_REQUIRED_ENV` so adding one cannot forget to make it required.
_API_FACTORY_ENV: tuple[str, ...] = (
    "BACKTEST_AUTHENTICATOR",
    "BACKTEST_OBJECT_STORE",
    "BACKTEST_OWNER_DIRECTORY",
    "BACKTEST_COMPILED_PLAN_SOURCE",
    "BACKTEST_DATASET_MANIFEST_SOURCE",
    "BACKTEST_EXECUTION_POLICY_CATALOG",
    "BACKTEST_DEAD_LETTER_SINK",
)


def build_result_query_service(
    persistence: BacktestPersistence, object_store: ObjectStore
) -> BacktestResultQueryService:
    """The D29 result read model, over the rows and objects the worker published.

    This is the join the deployment was missing. ``DurableResultPublisher`` writes
    ``backtest.*`` and ``storage.objects``; this reads them back and reconstructs the
    immutable artifacts from the same bytes. Both halves must be given the *same*
    bucket: ``storage.objects`` identity includes ``bucket_name``, and the read model
    refuses an object registered against another one rather than serving bytes the row
    does not describe.
    """

    return BacktestResultQueryService(
        DurableBacktestResultQueryStore(persistence=persistence, object_store=object_store)
    )


@dataclass(frozen=True, slots=True)
class ApiRuntime:
    """A fully wired `backtest-api` process, not yet listening.

    Construction performs no I/O, so a configuration mistake is a `WiringError` before
    anything connects. :meth:`verify` is the separate, deliberate step that touches the
    database.
    """

    app: Any
    persistence: BacktestPersistence
    object_store: ObjectStore
    host: str
    port: int

    def verify(self) -> None:
        """Refuse to serve against a schema this build does not match.

        The runtime applies no DDL (COM07 `runtime-no-ddl`), so the only safe response
        to drift is to stop. Starting anyway would serve 500s from every query route
        while looking healthy.
        """

        self.persistence.verify_schema()


def build_api_runtime(environ: Mapping[str, str]) -> ApiRuntime:
    """Build the `/api/v1` application from the environment. No defaults, no I/O.

    Every missing setting is reported in one message rather than one per restart, and
    the message names the settings, not the first line that happened to fail.
    """

    missing = [name for name in API_REQUIRED_ENV if not environ.get(name)]
    if missing:
        raise WiringError(
            "backtest-api cannot start: missing required environment settings "
            + ", ".join(sorted(missing))
            + ". Every one of them is a deployment decision with no safe default; see "
            "wiring.API_REQUIRED_ENV."
        )

    port_text = environ["BACKTEST_API_PORT"]
    try:
        port = int(port_text)
    except ValueError as exc:
        raise WiringError(f"BACKTEST_API_PORT must be an integer, got {port_text!r}") from exc
    if not 1 <= port <= 65535:
        raise WiringError(f"BACKTEST_API_PORT must be a TCP port, got {port}")

    try:
        resolved = {name: load_factory(environ[name], name) for name in _API_FACTORY_ENV}
    except WorkerConfigurationError as exc:
        raise WiringError(str(exc)) from exc

    object_store = resolved["BACKTEST_OBJECT_STORE"]
    if not isinstance(object_store, ObjectStore):
        raise WiringError(
            f"BACKTEST_OBJECT_STORE must produce an object_store.ObjectStore, got {type(object_store).__name__}"
        )

    persistence = BacktestPersistence(create_backtest_engine(environ["BACKTEST_DATABASE_URL"]))

    import boto3

    lifecycle = BacktestLifecycleService(
        gateway=PersistenceRunGateway(persistence),
        queue=SqsBacktestJobQueue(
            # AWS's own settings are read from the mapping this function was given, not
            # from `os.environ`: a caller that passes an explicit environment must get
            # a client configured from it, or the two disagree about which region and
            # endpoint the deployment is on.
            boto3.client(
                "sqs",
                endpoint_url=environ.get("AWS_ENDPOINT_URL"),
                region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
            ),
            environ["BACKTEST_QUEUE_URL"],
        ),
        owners=resolved["BACKTEST_OWNER_DIRECTORY"],
        plans=resolved["BACKTEST_COMPILED_PLAN_SOURCE"],
        manifests=resolved["BACKTEST_DATASET_MANIFEST_SOURCE"],
        policies=resolved["BACKTEST_EXECUTION_POLICY_CATALOG"],
        dead_letters=resolved["BACKTEST_DEAD_LETTER_SINK"],
    )
    return ApiRuntime(
        app=create_app(
            lifecycle,
            resolved["BACKTEST_AUTHENTICATOR"],
            build_result_query_service(persistence, object_store),
        ),
        persistence=persistence,
        object_store=object_store,
        host=environ["BACKTEST_API_HOST"],
        port=port,
    )
