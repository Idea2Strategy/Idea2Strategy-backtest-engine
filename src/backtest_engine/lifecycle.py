"""Acceptance, dispatch and result ingestion for one official backtest run.

This is the half of D28 that sits behind the HTTP surface in `api.py`. It takes B's
`strategy-bot.v1` `OFFICIAL_BACKTEST_REQUESTED` verbatim, turns it into a canonical
`backtest.runs` row, dispatches the job, and applies the `backtest.v1` result events a
worker publishes back.

Three properties are load-bearing and each is enforced here rather than assumed:

**Determinism of identity.** `run_id` is `uuid5` of B's own `metadata.idempotencyKey`.
A redelivered request therefore addresses the same run without a database round trip,
and two processes racing the same delivery converge on the same row rather than
creating two runs.

**At-least-once safety.** Every mutating entry point is keyed by a content-bound
idempotency key. Replaying an identical message returns the first outcome; replaying a
*different* payload under the same key is a conflict, never a silent overwrite. This is
the difference between "safe to redeliver" and "last writer wins".

**No hidden defaults.** Every input the canonical `backtest.runs` row needs that B's
message does not carry - the owner account, the execution policy, the compiled plan's
initial cash, the dataset's hash - is resolved through an explicit port. A port that
cannot answer makes the request unsatisfiable; nothing is invented to fill the gap.

The storage boundary is `RunGateway`. `PersistenceRunGateway` is the durable
SQLAlchemy Core implementation over the canonical schema; `InMemoryRunGateway` is a
faithful fake used where a test is exercising HTTP behaviour rather than SQL. The
durable implementation is covered separately, against a real PostgreSQL 16 container,
in `tests/persistence/`.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from .contracts import (
    ContractValidationError,
    build_backtest_result_event,
    compute_input_bundle_fingerprint,
    validate_backtest_result_event,
    validate_official_backtest_request,
)
from .execution_policy import ExecutionPolicy, ExecutionPolicyCatalog, ExecutionPolicyUnavailable
from .money import PRECISION_RULES_VERSION
from .persistence.errors import IdempotencyConflict as PersistedIdempotencyConflict
from .persistence.errors import InvalidStatusTransition as PersistedInvalidStatusTransition
from .persistence.errors import RowNotFound
from .persistence.rows import (
    DetailManifestRow,
    InputBundleRow,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunInputPinRow,
    RunLane,
    RunRow,
    RunStatus,
)


__all__ = [
    "RUN_ID_NAMESPACE",
    "AcceptedRun",
    "BacktestJobQueue",
    "BacktestLifecycleService",
    "BacktestRun",
    "BacktestRunNotFound",
    "CompiledPlanSource",
    "DatasetManifestSource",
    "DeadLetterSink",
    "DeadLetteredMessage",
    "IdempotencyConflict",
    "InMemoryBacktestJobQueue",
    "InMemoryDeadLetterQueue",
    "InMemoryRunGateway",
    "InvalidStatusTransition",
    "LifecycleError",
    "NotRunOwner",
    "OwnerDirectory",
    "PersistenceRunGateway",
    "PreconditionFailed",
    "RequestNotSatisfiable",
    "ResultIngestion",
    "RunGateway",
    "SqsBacktestJobQueue",
    "StaticCompiledPlanSource",
    "StaticDatasetManifestSource",
    "StaticOwnerDirectory",
    "run_id_for",
]


#: `uuid5` namespace for deriving a run id from B's idempotency key. Fixed forever:
#: changing it would re-address every existing run.
RUN_ID_NAMESPACE = uuid5(NAMESPACE_URL, "https://contracts.idea2strategy.io/backtest/v1/run")


def _postgres_jsonb_payload_hash(document: Mapping[str, Any]) -> str:
    """Hash a JSON document in PostgreSQL ``jsonb::text`` canonical form.

    The Backend stores this digest on both the run and its Outbox envelope.  JSONB
    orders object keys by UTF-8 byte length and then byte value, and emits a space
    after separators.  Reproducing that representation here keeps the legacy
    consumer-owned insert path byte-compatible with the producer-owned transaction.
    """

    def ordered(value: Any) -> Any:
        if isinstance(value, Mapping):
            keys = sorted(value, key=lambda key: (len(str(key).encode("utf-8")), str(key).encode("utf-8")))
            return {str(key): ordered(value[key]) for key in keys}
        if isinstance(value, list):
            return [ordered(item) for item in value]
        return value

    canonical = json.dumps(
        ordered(document),
        ensure_ascii=False,
        separators=(", ", ": "),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class LifecycleError(Exception):
    """Base class for every lifecycle failure the API translates to a status code."""


class IdempotencyConflict(LifecycleError):
    """An idempotency key was reused for materially different content."""


class BacktestRunNotFound(LifecycleError):
    """No run with the given id exists."""


class NotRunOwner(LifecycleError):
    """The run exists but is owned by a different account."""


class InvalidStatusTransition(LifecycleError):
    """A result would move a run backwards, or out of a terminal status."""


class PreconditionFailed(LifecycleError):
    """An `If-Match` precondition did not match the run's current state.

    Carries the current state so a worker that lost the response to its previous
    write can reconcile without guessing.
    """

    def __init__(self, message: str, *, current: BacktestRun) -> None:
        super().__init__(message)
        self.current = current


class RequestNotSatisfiable(LifecycleError):
    """A required input could not be resolved, so no run can be created.

    This is deliberately *not* an `UNAVAILABLE` run: `UNAVAILABLE` is a status a
    persisted run reaches, and a run that cannot be constructed has nothing to persist.
    A data gap discovered after acceptance is the worker's path, not this one.
    """

    def __init__(self, message: str, *, reason_code: str, missing: Sequence[str]) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.missing = tuple(missing)


# ---------------------------------------------------------------------------
# Ports - everything B's message does not carry
# ---------------------------------------------------------------------------


class OwnerDirectory(Protocol):
    """Resolves a bot to its owning account.

    B's `OFFICIAL_BACKTEST_REQUESTED` carries `botId` but no owner, while spec 2.2
    identifies a run by `bot_id` + `owner_account_id`. The `bot` schema is read-only
    for this repository, so ownership is resolved through this port rather than joined.
    """

    def owner_of(self, bot_id: UUID) -> UUID | None: ...


class CompiledPlanSource(Protocol):
    """Resolves B's `basic-compiled-plan` by its `planChecksum`."""

    def by_checksum(self, checksum: str) -> Mapping[str, Any] | None: ...


class DatasetManifestSource(Protocol):
    """Resolves a `market-data.v1` dataset manifest by id."""

    def by_id(self, manifest_id: UUID) -> Mapping[str, Any] | None: ...


class BacktestJobQueue(Protocol):
    def publish(self, message: Mapping[str, Any]) -> None: ...


class DeadLetterSink(Protocol):
    def dead_letter(self, message: DeadLetteredMessage) -> None: ...


class SqsClient(Protocol):
    def send_message(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StaticOwnerDirectory:
    owners: Mapping[UUID, UUID]

    def owner_of(self, bot_id: UUID) -> UUID | None:
        return self.owners.get(bot_id)


@dataclass(frozen=True, slots=True)
class StaticCompiledPlanSource:
    plans: Mapping[str, Mapping[str, Any]]

    def by_checksum(self, checksum: str) -> Mapping[str, Any] | None:
        return self.plans.get(checksum)


@dataclass(frozen=True, slots=True)
class StaticDatasetManifestSource:
    manifests: Mapping[UUID, Mapping[str, Any]]

    def by_id(self, manifest_id: UUID) -> Mapping[str, Any] | None:
        return self.manifests.get(manifest_id)


# ---------------------------------------------------------------------------
# Aggregates
# ---------------------------------------------------------------------------


def _canonical(document: Mapping[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def run_id_for(idempotency_key: str) -> UUID:
    """Deterministic run id for one accepted request.

    Two deliveries of the same message address the same run without consulting the
    database, so a duplicate cannot create a second run even under a race.
    """
    if not idempotency_key:
        raise ValueError("idempotency_key must not be empty")
    return uuid5(RUN_ID_NAMESPACE, idempotency_key)


@dataclass(frozen=True, slots=True)
class BacktestRun:
    """The run aggregate as the API reports it."""

    run: RunRow
    attempts: tuple[RunAttemptRow, ...] = ()
    last_event: Mapping[str, Any] | None = None

    @property
    def backtest_run_id(self) -> UUID:
        return self.run.id

    @property
    def status(self) -> RunStatus:
        return self.run.status

    @property
    def owner_account_id(self) -> UUID | None:
        return self.run.owner_account_id

    @property
    def etag(self) -> str:
        """Concurrency token: status plus the number of applied attempts.

        Both move on every meaningful state change, so a stale `If-Match` from a
        worker whose previous response was lost is detected rather than replayed.
        """
        return f'"{self.run.status.value}.{len(self.attempts)}"'


@dataclass(frozen=True, slots=True)
class AcceptedRun:
    run: BacktestRun
    created: bool
    dispatched: bool


@dataclass(frozen=True, slots=True)
class ResultIngestion:
    run: BacktestRun
    applied: bool
    """`False` when this was a redelivery of an event that had already been applied."""


@dataclass(frozen=True, slots=True)
class DeadLetteredMessage:
    payload: Mapping[str, Any]
    reason: str
    failure_kind: str
    delivery_attempt: int


class InMemoryDeadLetterQueue:
    """Collects messages that must not be redelivered again."""

    def __init__(self) -> None:
        self._messages: list[DeadLetteredMessage] = []
        self._lock = threading.Lock()

    @property
    def messages(self) -> tuple[DeadLetteredMessage, ...]:
        with self._lock:
            return tuple(self._messages)

    def dead_letter(self, message: DeadLetteredMessage) -> None:
        with self._lock:
            self._messages.append(message)


class InMemoryBacktestJobQueue:
    def __init__(self) -> None:
        self._messages: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    @property
    def messages(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._messages)

    def publish(self, message: Mapping[str, Any]) -> None:
        with self._lock:
            self._messages.append(copy.deepcopy(dict(message)))


class SqsBacktestJobQueue:
    """SQS Standard publisher. Consumers must stay idempotent; delivery is at-least-once."""

    def __init__(self, client: SqsClient, queue_url: str) -> None:
        if not queue_url:
            raise ValueError("queue_url must not be empty")
        self._client = client
        self._queue_url = queue_url

    def publish(self, message: Mapping[str, Any]) -> None:
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=_canonical(message),
            MessageAttributes={
                "BacktestRunId": {"DataType": "String", "StringValue": str(message["backtestRunId"])},
                "IdempotencyKey": {"DataType": "String", "StringValue": str(message["idempotencyKey"])},
            },
        )


# ---------------------------------------------------------------------------
# Storage boundary
# ---------------------------------------------------------------------------


class RunGateway(Protocol):
    """Everything the lifecycle needs from durable storage, in one transaction each."""

    def accept(self, row: RunRow, pins: RunInputPinRow) -> tuple[RunRow, bool]: ...

    def get(self, run_id: UUID) -> RunRow: ...

    def list_by_owner(self, owner_account_id: UUID, *, limit: int, offset: int) -> tuple[RunRow, ...]: ...

    def attempts(self, run_id: UUID) -> tuple[RunAttemptRow, ...]: ...

    def attempts_for_runs(self, run_ids: Sequence[UUID]) -> Mapping[UUID, tuple[RunAttemptRow, ...]]: ...

    def performance(self, run_id: UUID) -> PerformanceSummaryRow | None: ...

    def monthly(self, run_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]: ...

    def manifests(self, run_id: UUID) -> tuple[DetailManifestRow, ...]: ...

    def transition(self, run_id: UUID, target: RunStatus, **values: Any) -> RunRow: ...

    def request_cancellation(
        self, run_id: UUID, *, reason_code: str, requested_at: datetime
    ) -> RunRow: ...


class PersistenceRunGateway:
    """Durable `RunGateway` over the canonical schema, in SQLAlchemy Core."""

    def __init__(self, persistence: Any) -> None:
        self._persistence = persistence

    @contextmanager
    def _write(self) -> Iterator[Any]:
        with self._persistence.unit_of_work() as uow:
            yield uow

    @contextmanager
    def _read(self) -> Iterator[Any]:
        with self._persistence.read_only() as uow:
            yield uow

    def accept(self, row: RunRow, pins: RunInputPinRow) -> tuple[RunRow, bool]:
        """Insert the run and its pinned request inputs in **one** transaction.

        Not two calls: a run whose `backtest.run_input_pins` row is missing cannot
        report its own reproducibility boundary, and `GET /{run_id}/inputs` would have
        to answer 500 for it forever. Both rows land or neither does.
        """

        if pins.run_id != row.id:
            raise IdempotencyConflict(f"pinned inputs belong to run {pins.run_id}, not {row.id}")
        try:
            with self._write() as uow:
                accepted, created = uow.runs.accept(row)
                # Compatibility for explicitly enabled test/provider fixtures only.
                # Production provider transactions create this bundle before the
                # consumer observes the run; production HTTP creation is closed.
                uow.inputs.lock(
                    InputBundleRow(
                        id=pins.input_bundle_id,
                        run_id=row.id,
                        bundle_hash=pins.input_bundle_fingerprint,
                        as_of_at=pins.pinned_at,
                        locked_at=pins.pinned_at,
                    ),
                    datasets=(),
                    features=(),
                )
                uow.pins.pin(pins)
                return accepted, created
        except PersistedIdempotencyConflict as exc:
            raise IdempotencyConflict(str(exc)) from exc

    def get(self, run_id: UUID) -> RunRow:
        try:
            with self._read() as uow:
                return uow.runs.get(run_id)
        except RowNotFound as exc:
            raise BacktestRunNotFound(str(exc)) from exc

    def list_by_owner(self, owner_account_id: UUID, *, limit: int, offset: int) -> tuple[RunRow, ...]:
        with self._read() as uow:
            return uow.runs.list_by_owner(owner_account_id, limit=limit, offset=offset)

    def attempts(self, run_id: UUID) -> tuple[RunAttemptRow, ...]:
        with self._read() as uow:
            return uow.attempts.list_for_run(run_id)

    def attempts_for_runs(self, run_ids: Sequence[UUID]) -> Mapping[UUID, tuple[RunAttemptRow, ...]]:
        if not run_ids:
            return {}
        with self._read() as uow:
            return uow.attempts.list_for_runs(list(run_ids))

    def performance(self, run_id: UUID) -> PerformanceSummaryRow | None:
        with self._read() as uow:
            return uow.performance.find(run_id)

    def monthly(self, run_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]:
        with self._read() as uow:
            return uow.monthly.list_for_run(run_id)

    def manifests(self, run_id: UUID) -> tuple[DetailManifestRow, ...]:
        with self._read() as uow:
            return uow.manifests.list_for_run(run_id)

    def transition(self, run_id: UUID, target: RunStatus, **values: Any) -> RunRow:
        try:
            with self._write() as uow:
                if target is RunStatus.RUNNING:
                    return uow.runs.mark_running(run_id, values["started_at"])
                if target is RunStatus.COMPLETED:
                    return uow.runs.mark_completed(
                        run_id,
                        values["completed_at"],
                        values["result_hash"],
                        result_manifest_id=values.get("result_manifest_id"),
                    )
                if target is RunStatus.FAILED:
                    return uow.runs.mark_failed(
                        run_id,
                        values["completed_at"],
                        values["failure_code"],
                        retryable=values.get("retryable"),
                    )
                if target is RunStatus.CANCELLED:
                    return uow.runs.mark_cancelled(
                        run_id,
                        values["cancelled_at"],
                        values["cancellation_reason_code"],
                    )
                if target is RunStatus.UNAVAILABLE:
                    return uow.runs.mark_unavailable(
                        run_id,
                        values["completed_at"],
                        values["failure_code"],
                        missing_requirements=values.get("missing_requirements"),
                    )
                raise InvalidStatusTransition(f"{target.value} is not a reachable result status")
        except PersistedInvalidStatusTransition as exc:
            raise InvalidStatusTransition(str(exc)) from exc
        except RowNotFound as exc:
            raise BacktestRunNotFound(str(exc)) from exc

    def request_cancellation(
        self, run_id: UUID, *, reason_code: str, requested_at: datetime
    ) -> RunRow:
        del requested_at  # Production ordering uses PostgreSQL clock_timestamp().
        try:
            with self._write() as uow:
                return uow.runs.request_cancellation(run_id, reason_code=reason_code)
        except RowNotFound as exc:
            raise BacktestRunNotFound(str(exc)) from exc


class InMemoryRunGateway:
    """Faithful in-process `RunGateway`, for tests about HTTP rather than SQL.

    It reproduces the two behaviours the canonical schema's constraints give the
    durable gateway - unique `idempotency_key` and the `run_status` transition table -
    because those are the behaviours the lifecycle depends on. It is never used in
    production; `create_app` requires a gateway to be supplied.
    """

    def __init__(self) -> None:
        self._runs: dict[UUID, RunRow] = {}
        self._by_key: dict[str, UUID] = {}
        self._attempts: dict[UUID, list[RunAttemptRow]] = {}
        self._pins: dict[UUID, RunInputPinRow] = {}
        self._lock = threading.RLock()

    def pins_of(self, run_id: UUID) -> RunInputPinRow | None:
        """Test seam: what `accept` pinned. Mirrors `uow.pins.find`."""

        with self._lock:
            return self._pins.get(run_id)

    def accept(self, row: RunRow, pins: RunInputPinRow) -> tuple[RunRow, bool]:
        if pins.run_id != row.id:
            raise IdempotencyConflict(f"pinned inputs belong to run {pins.run_id}, not {row.id}")
        with self._lock:
            existing_id = self._by_key.get(row.idempotency_key)
            if existing_id is not None:
                existing = self._runs[existing_id]
                differing = [
                    name
                    for name in (
                        "bot_id",
                        "owner_account_id",
                        "configuration_hash",
                        "evaluation_start",
                        "evaluation_end",
                        "initial_cash_amount",
                        "market_rules_version",
                        "accounting_rules_version",
                        "precision_rules_version",
                        "fee_policy_id",
                        "slippage_rate_bps",
                        "buying_power_buffer_policy_id",
                    )
                    if getattr(existing, name) != getattr(row, name)
                ]
                if differing:
                    raise IdempotencyConflict(
                        f"idempotency_key {row.idempotency_key!r} was already used for a "
                        f"different request; differing fields: {differing}"
                    )
                return existing, False
            if row.id in self._runs:
                raise IdempotencyConflict(f"run id {row.id} is already used by a different idempotency key")
            self._runs[row.id] = row
            self._by_key[row.idempotency_key] = row.id
            self._pins[row.id] = pins
            return row, True

    def get(self, run_id: UUID) -> RunRow:
        with self._lock:
            try:
                return self._runs[run_id]
            except KeyError as exc:
                raise BacktestRunNotFound(f"backtest run not found: {run_id}") from exc

    def list_by_owner(self, owner_account_id: UUID, *, limit: int, offset: int) -> tuple[RunRow, ...]:
        with self._lock:
            owned = [row for row in self._runs.values() if row.owner_account_id == owner_account_id]
        owned.sort(key=lambda row: (row.queued_at, row.id), reverse=True)
        return tuple(owned[offset : offset + limit])

    def attempts(self, run_id: UUID) -> tuple[RunAttemptRow, ...]:
        with self._lock:
            return tuple(self._attempts.get(run_id, ()))

    def attempts_for_runs(self, run_ids: Sequence[UUID]) -> Mapping[UUID, tuple[RunAttemptRow, ...]]:
        with self._lock:
            return {run_id: tuple(self._attempts[run_id]) for run_id in run_ids if self._attempts.get(run_id)}

    def record_attempt(self, row: RunAttemptRow) -> None:
        """Test seam for arranging attempt history; production writes go through BT-d."""
        with self._lock:
            self._attempts.setdefault(row.run_id, []).append(row)

    def performance(self, run_id: UUID) -> PerformanceSummaryRow | None:
        return None

    def monthly(self, run_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]:
        return ()

    def manifests(self, run_id: UUID) -> tuple[DetailManifestRow, ...]:
        return ()

    def transition(self, run_id: UUID, target: RunStatus, **values: Any) -> RunRow:
        from .persistence.rows import RUN_STATUS_TRANSITIONS

        with self._lock:
            current = self.get(run_id)
            if current.status is target:
                return current
            if target not in RUN_STATUS_TRANSITIONS[current.status]:
                raise InvalidStatusTransition(
                    f"backtest run {run_id} is {current.status.value}; it cannot move to {target.value}"
                )
            updated = replace(current, status=target, **values)
            self._runs[run_id] = updated
            return updated

    def request_cancellation(
        self, run_id: UUID, *, reason_code: str, requested_at: datetime
    ) -> RunRow:
        if not reason_code.strip():
            raise ValueError("reason_code must not be blank")
        with self._lock:
            current = self.get(run_id)
            if current.status in {
                RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED, RunStatus.UNAVAILABLE,
            }:
                return current
            values: dict[str, Any] = {
                "cancellation_requested_at": requested_at,
                "cancellation_reason_code": reason_code,
            }
            if current.status is RunStatus.QUEUED:
                values.update(
                    status=RunStatus.CANCELLED,
                    cancelled_at=requested_at,
                    completed_at=requested_at,
                )
            updated = replace(current, **values)
            self._runs[run_id] = updated
            return updated


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------


_TERMINAL_FAILURE_KIND = "CONTRACT_VIOLATION"
_TRANSIENT_FAILURE_KIND = "TRANSIENT"


@dataclass
class BacktestLifecycleService:
    """Accepts requests, dispatches jobs, and applies result events."""

    gateway: RunGateway
    queue: BacktestJobQueue
    owners: OwnerDirectory
    plans: CompiledPlanSource
    manifests: DatasetManifestSource
    policies: ExecutionPolicyCatalog
    dead_letters: DeadLetterSink | None = None
    max_delivery_attempts: int = 3
    _applied_events: dict[str, str] = field(default_factory=dict, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    # -- acceptance -------------------------------------------------------

    def accept(
        self,
        request: Mapping[str, Any],
        *,
        compiled_plan: Mapping[str, Any] | None = None,
    ) -> AcceptedRun:
        """Accept B's `OFFICIAL_BACKTEST_REQUESTED` and queue the job.

        Every digest in the message is verified, not trusted. Re-accepting the same
        message returns the existing run and does not enqueue a second job.

        The plan is resolved **before** validation when the caller did not supply
        one. That ordering is load-bearing: `validate_official_backtest_request`
        can only cross-check `compiledPlanChecksum` and `expectedSnapshotHash`
        against a plan it was given, and the production intake path
        (`release_intake.OfficialBacktestIntake`) never supplies one -- B's
        message names the checksum and D fetches the plan. Validating first and
        resolving afterwards let a request whose `expectedSnapshotHash` named a
        different strategy through, with a correctly-derived idempotency key, so
        the run executed the resolved plan under the wrong release's identity.
        """
        resolved_plan = compiled_plan if compiled_plan is not None else self._resolve_plan(request)
        validated = validate_official_backtest_request(request, compiled_plan=resolved_plan)
        row, pins, message = self._build_run(validated, compiled_plan=resolved_plan)

        run_row, created = self.gateway.accept(row, pins)
        dispatched = False
        if created:
            self.queue.publish(message)
            dispatched = True
        return AcceptedRun(run=self._load(run_row), created=created, dispatched=dispatched)

    def _resolve_plan(self, request: Mapping[str, Any]) -> Mapping[str, Any] | None:
        """The plan the message names, if this deployment can produce it.

        Returns `None` rather than raising when the message is too malformed to
        carry a checksum: validation runs next and reports the real fault, which
        is a better error than "plan not resolvable".
        """
        if not isinstance(request, Mapping):
            return None
        checksum = request.get("compiledPlanChecksum")
        if not isinstance(checksum, str) or not checksum:
            return None
        return self.plans.by_checksum(checksum)

    def _build_run(
        self,
        request: Mapping[str, Any],
        *,
        compiled_plan: Mapping[str, Any] | None,
    ) -> tuple[RunRow, RunInputPinRow, dict[str, Any]]:
        missing: list[str] = []

        bot_id = UUID(request["botId"])
        owner_account_id = self.owners.owner_of(bot_id)
        if owner_account_id is None:
            missing.append(f"owner:bot={bot_id}")

        checksum = request["compiledPlanChecksum"]
        plan = compiled_plan if compiled_plan is not None else self.plans.by_checksum(checksum)
        if plan is None:
            missing.append(f"compiledPlan:{checksum}")

        manifest_id = UUID(request["datasetManifestId"])
        manifest = self.manifests.by_id(manifest_id)
        if manifest is None:
            missing.append(f"datasetManifest:{manifest_id}")

        occurred_at = _parse_timestamp(request["metadata"]["occurredAt"])
        policy: ExecutionPolicy | None
        try:
            policy = self.policies.select(occurred_at)
        except ExecutionPolicyUnavailable as exc:
            policy = None
            missing.append(f"executionPolicy:{exc}")

        if missing or policy is None:
            raise RequestNotSatisfiable(
                f"the request cannot be turned into a run because required inputs are unresolved: {missing}",
                reason_code="REQUIRED_INPUT_UNAVAILABLE",
                missing=missing,
            )

        assert plan is not None and manifest is not None and owner_account_id is not None

        bundle = {
            "botId": str(bot_id),
            "ownerAccountId": str(owner_account_id),
            "expectedSnapshotHash": request["expectedSnapshotHash"],
            "compiledPlanChecksum": checksum,
            "datasetManifestId": str(manifest_id),
            "datasetHash": _prefixed(manifest["dataset_hash"]),
            "featureMaterializationVersion": str(manifest["schema_id"]),
            "executionPolicyVersion": policy.version,
            "precisionRulesVersion": policy.precision_rules_version,
        }
        configuration_hash = compute_input_bundle_fingerprint(bundle)
        idempotency_key = request["metadata"]["idempotencyKey"]
        registered_run_id = request.get("runId")
        run_id = UUID(str(registered_run_id)) if registered_run_id is not None else run_id_for(idempotency_key)

        lane = request.get("lane", RunLane.BASIC.value)
        if lane != RunLane.BASIC.value:
            raise ContractValidationError(f"official_backtest_request.lane must be {RunLane.BASIC.value}")
        aggregate_sequence = request.get("aggregateSequence", 1)
        if aggregate_sequence != 1:
            raise ContractValidationError("official_backtest_request.aggregateSequence must be 1")
        requested_policy = request.get("executionPolicyVersion", policy.version)
        if requested_policy != policy.version:
            raise ContractValidationError(
                "official_backtest_request.executionPolicyVersion does not match the policy selected for occurredAt"
            )

        row = RunRow(
            id=run_id,
            bot_id=bot_id,
            owner_account_id=owner_account_id,
            configuration_hash=configuration_hash,
            status=RunStatus.QUEUED,
            evaluation_start=_et_date(policy.period_start, policy),
            evaluation_end=_et_date(policy.period_end, policy),
            initial_cash_amount=Decimal(plan["executionSnapshot"]["initialCashAmount"]),
            market_rules_version=policy.market_rules_version,
            accounting_rules_version=policy.accounting_rules_version,
            precision_rules_version=policy.precision_rules_version,
            fee_policy_id=UUID(policy.fee_policy_id),
            slippage_rate_bps=policy.slippage_rate_bps,
            buying_power_buffer_policy_id=UUID(policy.buying_power_buffer_policy_id),
            idempotency_key=idempotency_key,
            queued_at=occurred_at,
            lane=RunLane.BASIC,
            message_id=UUID(request["metadata"]["messageId"]),
            canonical_payload_hash=_postgres_jsonb_payload_hash(request),
            aggregate_sequence=aggregate_sequence,
            execution_policy_version=policy.version,
            idempotency_scope=str(bot_id),
        )
        # Test-only compatibility identity. Production Backend creates this pin and
        # normalized bundle atomically before the consumer observes the request.
        pins = RunInputPinRow(
            run_id=run_id,
            input_bundle_id=uuid5(NAMESPACE_URL, f"backtest-input-bundle:{run_id}"),
            input_bundle_fingerprint=configuration_hash,
            input_contract_version=str(request["metadata"]["contractVersion"]),
            compiled_plan_checksum=checksum,
            strategy_snapshot_hash=request["expectedSnapshotHash"],
            execution_policy_version=policy.version,
            pinned_at=occurred_at,
        )
        message = {
            "backtestRunId": str(run_id),
            "inputBundleId": str(pins.input_bundle_id),
            "botId": str(bot_id),
            "ownerAccountId": str(owner_account_id),
            "idempotencyKey": idempotency_key,
            "inputBundleFingerprint": configuration_hash,
            "executionPolicyVersion": policy.version,
            "compiledPlanChecksum": checksum,
            "datasetManifestId": str(manifest_id),
            "expectedSnapshotHash": request["expectedSnapshotHash"],
            "featureMaterializations": copy.deepcopy(
                list(request.get("featureMaterializations", ()))
            ),
        }
        return row, pins, message

    # -- queries ----------------------------------------------------------

    def get(self, run_id: UUID, *, owner_account_id: UUID) -> BacktestRun:
        run = self._load(self.gateway.get(run_id))
        self._require_owner(run, owner_account_id)
        return run

    def list_runs(self, owner_account_id: UUID, *, limit: int = 50, offset: int = 0) -> tuple[BacktestRun, ...]:
        """One page of the owner's runs, each with its real attempt history.

        The attempts are loaded, not defaulted. `BacktestRun.attempts` defaults to
        `()` and the list endpoint serialises `attemptCount: len(run.attempts)`, so
        building the aggregate without them reported `0` for every run while the
        single-run endpoint -- which goes through `_load` -- reported the truth. The
        page is served by one batch read rather than one read per row, so a larger
        page does not cost more round trips.
        """

        rows = self.gateway.list_by_owner(owner_account_id, limit=limit, offset=offset)
        if not rows:
            return ()
        attempts = self.gateway.attempts_for_runs([row.id for row in rows])
        return tuple(BacktestRun(run=row, attempts=attempts.get(row.id, ())) for row in rows)

    def attempts_of(self, run_id: UUID, *, owner_account_id: UUID) -> tuple[RunAttemptRow, ...]:
        self.get(run_id, owner_account_id=owner_account_id)
        return self.gateway.attempts(run_id)

    def performance_of(self, run_id: UUID, *, owner_account_id: UUID) -> PerformanceSummaryRow | None:
        self.get(run_id, owner_account_id=owner_account_id)
        return self.gateway.performance(run_id)

    def monthly_of(self, run_id: UUID, *, owner_account_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]:
        self.get(run_id, owner_account_id=owner_account_id)
        return self.gateway.monthly(run_id)

    def manifests_of(self, run_id: UUID, *, owner_account_id: UUID) -> tuple[DetailManifestRow, ...]:
        self.get(run_id, owner_account_id=owner_account_id)
        return self.gateway.manifests(run_id)

    def request_cancellation(
        self,
        run_id: UUID,
        *,
        owner_account_id: UUID,
        reason_code: str = "USER_CANCELLED",
        requested_at: datetime | None = None,
    ) -> BacktestRun:
        """Cancel a queued run immediately or mark a running run for cooperative stop."""
        current = self.get(run_id, owner_account_id=owner_account_id)
        reason = reason_code.strip()
        if not reason:
            raise ValueError("reason_code must not be blank")
        if current.status in {
            RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.UNAVAILABLE,
        }:
            raise InvalidStatusTransition(
                f"backtest run {run_id} is already {current.status.value}"
            )
        row = self.gateway.request_cancellation(
            run_id,
            reason_code=reason,
            requested_at=requested_at or datetime.now(timezone.utc),
        )
        return self._load(row)

    def _require_owner(self, run: BacktestRun, owner_account_id: UUID) -> None:
        if run.owner_account_id != owner_account_id:
            raise NotRunOwner(f"backtest run {run.backtest_run_id} belongs to another account")

    def _load(self, row: RunRow) -> BacktestRun:
        return BacktestRun(run=row, attempts=self.gateway.attempts(row.id))

    # -- result ingestion --------------------------------------------------

    def ingest_result(
        self,
        event: Mapping[str, Any],
        *,
        if_match: str | None = None,
        delivery_attempt: int = 1,
    ) -> ResultIngestion:
        """Apply a `backtest.v1` result event.

        A redelivery of an event that was already applied returns the same outcome with
        `applied=False`, so a worker whose response was lost can retry safely. A
        *different* event under the same idempotency key is a conflict. A structurally
        invalid event is poison: retrying it can never succeed, so it goes straight to
        the dead-letter sink instead of consuming the redelivery budget.
        """
        try:
            validated = validate_backtest_result_event(event)
        except ContractValidationError as exc:
            self._dead_letter(event, str(exc), _TERMINAL_FAILURE_KIND, delivery_attempt)
            raise

        key = validated["metadata"]["idempotencyKey"]
        payload = _canonical(validated)
        run_id = UUID(validated["backtestRunId"])

        with self._lock:
            seen = self._applied_events.get(key)
            if seen is not None:
                if seen != payload:
                    raise IdempotencyConflict(f"result idempotency key {key} was already applied to different content")
                return ResultIngestion(run=self._load(self.gateway.get(run_id)), applied=False)

            current = self._load(self.gateway.get(run_id))
            if if_match is not None and if_match != current.etag:
                raise PreconditionFailed(
                    f"If-Match {if_match} does not match the run's current state "
                    f"{current.etag}; re-read the run before retrying",
                    current=current,
                )

            try:
                updated = self._apply(validated, run_id)
            except InvalidStatusTransition:
                raise
            except LifecycleError as exc:
                self._maybe_dead_letter(event, str(exc), delivery_attempt)
                raise

            self._applied_events[key] = payload
            return ResultIngestion(run=self._load(updated), applied=True)

    def _apply(self, event: Mapping[str, Any], run_id: UUID) -> RunRow:
        """Persist the whole terminal event, not the parts that already had a column.

        `validate_backtest_result_event` has already run, and the `backtest.v1`
        schema makes `resultManifestId`, `retryable` and `missingRequirements`
        *required* in their branches, so each is read with `[...]` rather than
        `.get(...)`: a build whose schema and whose persistence disagree about what
        a terminal event carries should fail loudly here, not silently drop a field
        the way this method used to.
        """

        status = RunStatus(event["status"])
        if status is RunStatus.QUEUED:
            return self.gateway.get(run_id)
        if status is RunStatus.RUNNING:
            return self.gateway.transition(run_id, RunStatus.RUNNING, started_at=_parse_timestamp(event["startedAt"]))
        if status is RunStatus.COMPLETED:
            return self.gateway.transition(
                run_id,
                RunStatus.COMPLETED,
                completed_at=_parse_timestamp(event["completedAt"]),
                result_hash=event["resultHash"],
                failure_code=None,
                result_manifest_id=UUID(event["resultManifestId"]),
            )
        if status is RunStatus.FAILED:
            return self.gateway.transition(
                run_id,
                RunStatus.FAILED,
                completed_at=_parse_timestamp(event["failedAt"]),
                failure_code=event["failureCode"],
                retryable=bool(event["retryable"]),
            )
        if status is RunStatus.CANCELLED:
            cancelled_at = _parse_timestamp(event["cancelledAt"])
            return self.gateway.transition(
                run_id,
                RunStatus.CANCELLED,
                completed_at=cancelled_at,
                cancelled_at=cancelled_at,
                cancellation_requested_at=cancelled_at,
                cancellation_reason_code=event["reasonCode"],
            )
        return self.gateway.transition(
            run_id,
            RunStatus.UNAVAILABLE,
            completed_at=_parse_timestamp(event["decidedAt"]),
            failure_code=event["reasonCode"],
            missing_requirements=tuple(event["missingRequirements"]),
        )

    def _maybe_dead_letter(self, event: Mapping[str, Any], reason: str, delivery_attempt: int) -> None:
        if delivery_attempt >= self.max_delivery_attempts:
            self._dead_letter(event, reason, _TRANSIENT_FAILURE_KIND, delivery_attempt)

    def _dead_letter(self, event: Mapping[str, Any], reason: str, failure_kind: str, delivery_attempt: int) -> None:
        if self.dead_letters is None:
            return
        self.dead_letters.dead_letter(
            DeadLetteredMessage(
                payload=copy.deepcopy(dict(event)),
                reason=reason,
                failure_kind=failure_kind,
                delivery_attempt=delivery_attempt,
            )
        )

    # -- outbound events ---------------------------------------------------

    def result_event_for(
        self,
        run: BacktestRun,
        *,
        status: str,
        correlation_id: str,
        message_id: str | None = None,
        expected_snapshot_hash: str,
        execution_policy_version: str,
        **detail: Any,
    ) -> dict[str, Any]:
        """Build the `backtest.v1` event this service would publish for `run`."""
        if run.owner_account_id is None:
            raise ValueError("an anonymized run cannot publish a new result event")
        return build_backtest_result_event(
            status=status,
            backtest_run_id=str(run.backtest_run_id),
            bot_id=str(run.run.bot_id),
            owner_account_id=str(run.owner_account_id),
            expected_snapshot_hash=expected_snapshot_hash,
            input_bundle_fingerprint=run.run.configuration_hash,
            execution_policy_version=execution_policy_version,
            precision_rules_version=run.run.precision_rules_version or PRECISION_RULES_VERSION,
            message_id=message_id or str(uuid4()),
            occurred_at=_format_timestamp(datetime.now(tz=timezone.utc)),
            correlation_id=correlation_id,
            **detail,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _et_date(moment: datetime, policy: ExecutionPolicy) -> date:
    from zoneinfo import ZoneInfo

    return moment.astimezone(ZoneInfo(policy.timezone)).date()


def _prefixed(digest: str) -> str:
    """Producer manifests carry bare hex; the fingerprint material uses B's prefix."""
    return digest if digest.startswith("sha256:") else f"sha256:{digest}"
