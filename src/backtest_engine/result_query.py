"""Owner-scoped read models for official automatic backtest results.

## The month/week join (card D27, spec 2.2)

Detail evidence is stored in **ET Monday week** parts, while the judgment API is
monthly. A week straddles a month boundary roughly once a month, so a month is never a
partition here: `monthly_trades` reads every week part that overlaps the requested ET
month and then places each row by the ET month of that row's own instant. The result is
cross-checked against the month's `MonthlyJudgmentSummary` before anything is returned,
so a week part that lost rows, or a summary that claims rows the objects do not carry,
fails closed instead of quietly returning a short list.

## Why this module defines its own run input

The read model consumes `RunProjection`, a value object declared here, rather than the
`lifecycle.BacktestRun` aggregate. Two reasons:

* the aggregate is the *write* model (a `backtest.runs` row plus its attempts); a
  read model that reaches into it inherits every change to the write path, and
* projection is where B's wire envelope stops mattering. The contract is validated once
  at intake by `lifecycle`; re-validating it here would duplicate that responsibility.

The API layer builds a `RunProjection` from the run aggregate. Identity follows spec
2.2: a run is identified by `bot_id` + `owner_account_id`, not by a strategy version,
and the terminal success status is `COMPLETED`.

## Where the durable read model gets its data, and what that cost

`persistence/protocols.py` recorded that `_QueryEntry` embeds a `ResultSnapshot`
holding the object *bytes*, which is why the projection could not be persisted as-is,
and named two ways out:

1. **reconstruct the projection on read** from `backtest.runs` + `input_bundles` +
   `performance_summaries` + `monthly_judgment_summaries` + `detail_manifests` + the
   object store, or
2. **change the aggregate** so it can be stored, which means a projection table the
   publisher has to keep in step with the canonical rows it already writes.

`DurableBacktestResultQueryStore` below is **option 1**. The reasons, and the price:

*Why.* A stored projection is a second copy of facts that already have a canonical
home. Every one of them — the run row, the performance summary, the six monthly
counters, the ET-week manifests, the Parquet parts — is written by
`DurableResultPublisher` in one transaction today. A projection table would be written
in the same transaction and read instead of them, so any bug that made the two
disagree would be invisible: the API would serve the projection and the canonical rows
would quietly say something else. Reconstructing removes that failure mode by
construction, because there is nothing to disagree with. It also means this card
authors no result-side schema, which matters while governance is fail-closed.

*What it costs, honestly.*

* **Reads are joins plus object fetches.** A completed run's `get_owned` runs five
  indexed queries and then fetches the result-snapshot JSON and every ET-week Parquet
  part from the object store. A stored projection would be one row. `overview`,
  `list_runs` and the run half of `inputs` avoid all of it through
  `get_owned_run`, which touches only `runs`, `run_input_pins` and (for a completed
  run) `monthly_judgment_summaries`; `monthly-trades` and `performance` pay in full.
  Nothing here caches, because a cache is a projection with worse invalidation.
* **Every read re-derives every hash.** `ResultSnapshotBuilder.rebuild` re-serialises
  the parsed records and `DetailObjectBuilder.verify` re-reads every Parquet footer.
  That is deliberate — it is what makes a lost or tampered object a
  `QueryIntegrityError` rather than an empty month — but it is real CPU per request,
  proportional to the run's size.
* **Two things the canonical schema cannot answer, and this store does not fake.**
  `RunProjection.missing_requirements` has no column anywhere (the `UNAVAILABLE`
  event's list is not persisted; only `runs.failure_code`, which is the reason code,
  is), so it comes back empty and the reason code carries the answer. And a *superseded*
  detail part cannot be reassembled, because `detail_manifests` has
  `supersedes_manifest_id` but not the `base_object_id` / `correction_of_object_id`
  that `detail_hash` covers; this store raises rather than reconstructing a descriptor
  with the lineage silently dropped. Both are recorded in
  `db/migration-contributions/change-requests/2026-08-02-backtest-run-input-pins.md`.

The one piece of storage this card did add is `backtest.run_input_pins`, and it is an
*input* row, not a projection: `compiled_plan_checksum`, `strategy_snapshot_hash`,
`input_bundle_hash`, `feature_materialization_version` and `execution_policy_version`
are explicit request pins and cannot be recovered from the independent bot launch
`runs.configuration_hash`. They are written in
the acceptance transaction, so `GET /{run_id}/inputs` answers at every status.
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from .detail_object_manifest import (
    DetailIntegrityError,
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectDescriptor,
    DetailObjectKind,
    DetailObjectValidationError,
    EtWeek,
    reassemble_detail_bundle,
)
from .monthly_judgment import (
    EtMonth,
    MonthlyJudgmentIntegrityError,
    MonthlyJudgmentSummary,
    MonthlyJudgmentValidationError,
    summary_from_document,
)
from .object_store import ObjectStore, sha256_bytes
from .persistence import BacktestPersistence
from .persistence.errors import PersistenceError, RowNotFound
from .persistence.rows import (
    DetailManifestRow,
    PerformanceSummaryRow,
    RunInputPinRow,
    RunRow,
    StorageObjectRow,
)
from .result_snapshot import (
    PerformanceSummary,
    PositionAfter,
    ResultIntegrityError,
    ResultSnapshot,
    ResultSnapshotBuilder,
)


__all__ = [
    "BacktestListItem",
    "BacktestOverview",
    "BacktestResultQueryService",
    "BacktestResultQueryStore",
    "DurableBacktestResultQueryStore",
    "InMemoryBacktestResultQueryStore",
    "InputModelView",
    "QueryIntegrityError",
    "QueryNotFound",
    "QueryNotReady",
    "QueryValidationError",
    "RunInputs",
    "RunProjection",
    "TradeDetailView",
]


#: Canonical `backtest.run_status` tokens (`db/schema.dbml`). The terminal success token
#: is `COMPLETED`; the `COMPLETE` this module used before was not in the enum.
QUEUED = "QUEUED"
RUNNING = "RUNNING"
COMPLETED = "COMPLETED"
FAILED = "FAILED"
UNAVAILABLE = "UNAVAILABLE"

_STATUSES = frozenset({QUEUED, RUNNING, COMPLETED, FAILED, UNAVAILABLE})
_TERMINAL = frozenset({COMPLETED, FAILED, UNAVAILABLE})

#: Mirrors `lifecycle.InMemoryBacktestRunStore._TRANSITIONS`, plus the no-op self
#: transition every projection needs to be re-applied idempotently.
_ALLOWED_TRANSITIONS: dict[str, frozenset[str]] = {
    QUEUED: frozenset({QUEUED, RUNNING, FAILED, UNAVAILABLE}),
    RUNNING: frozenset({RUNNING, COMPLETED, FAILED, UNAVAILABLE}),
    COMPLETED: frozenset({COMPLETED}),
    FAILED: frozenset({FAILED}),
    UNAVAILABLE: frozenset({UNAVAILABLE}),
}


class QueryValidationError(ValueError):
    """Raised when a query identity or projection input is malformed."""


class QueryNotFound(LookupError):
    """Owner-safe not-found response that does not reveal foreign runs."""


class QueryNotReady(RuntimeError):
    """Raised when result-only data is requested before successful completion."""


class QueryIntegrityError(RuntimeError):
    """Raised when immutable run, result, or detail identities disagree."""


# --------------------------------------------------------------------------------
# projection input
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunInputs:
    """The immutable reproducibility boundary of one run, as the API reports it."""

    compiled_plan_checksum: str
    strategy_snapshot_hash: str
    dataset_manifest_id: str
    dataset_hash: str
    input_bundle_fingerprint: str
    feature_materialization_version: str
    execution_policy_version: str
    precision_rules_version: str

    def __post_init__(self) -> None:
        for name in (
            "compiled_plan_checksum",
            "strategy_snapshot_hash",
            "dataset_manifest_id",
            "dataset_hash",
            "input_bundle_fingerprint",
            "feature_materialization_version",
            "execution_policy_version",
            "precision_rules_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise QueryValidationError(f"inputs.{name} must be a non-empty string")


@dataclass(frozen=True, slots=True)
class RunProjection:
    """One run as the read model needs it. Built by the API from the run aggregate."""

    run_id: str
    bot_id: str
    owner_account_id: str
    status: str
    queued_at: datetime
    inputs: RunInputs
    version: int = 1
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_code: str | None = None
    reason_code: str | None = None
    missing_requirements: tuple[str, ...] = ()
    result_manifest_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("run_id", "bot_id", "owner_account_id"):
            object.__setattr__(self, name, _uuid(getattr(self, name), name))
        if self.status not in _STATUSES:
            raise QueryValidationError(f"run status is unsupported: {self.status!r}")
        object.__setattr__(self, "queued_at", _aware(self.queued_at, "queued_at"))
        for name in ("started_at", "finished_at"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _aware(value, name))
        if not isinstance(self.inputs, RunInputs):
            raise QueryValidationError("inputs must be a RunInputs")
        if not isinstance(self.version, int) or isinstance(self.version, bool) or self.version < 1:
            raise QueryValidationError("version must be a positive integer")
        missing = tuple(self.missing_requirements)
        if any(not isinstance(item, str) or not item.strip() for item in missing):
            raise QueryValidationError("missing_requirements must contain non-empty strings")
        object.__setattr__(self, "missing_requirements", missing)
        if self.status == UNAVAILABLE and not self.reason_code:
            raise QueryValidationError("an UNAVAILABLE run must carry a reason_code")
        if self.status == FAILED and not self.failure_code:
            raise QueryValidationError("a FAILED run must carry a failure_code")
        if self.status == COMPLETED:
            object.__setattr__(
                self, "result_manifest_id", _uuid(self.result_manifest_id, "result_manifest_id")
            )
        elif self.result_manifest_id is not None:
            raise QueryValidationError(
                f"only a {COMPLETED} run has a result_manifest_id, this one is {self.status}"
            )
        if self.status in _TERMINAL and self.finished_at is None:
            raise QueryValidationError(f"a {self.status} run must carry finished_at")


# --------------------------------------------------------------------------------
# views
# --------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BacktestListItem:
    run_id: str
    bot_id: str
    status: str
    requested_at: datetime


@dataclass(frozen=True, slots=True)
class BacktestOverview:
    run_id: str
    bot_id: str
    status: str
    requested_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    reason_code: str | None
    missing_requirements: tuple[str, ...]
    result_manifest_id: str | None


@dataclass(frozen=True, slots=True)
class InputModelView:
    run_id: str
    bot_id: str
    strategy_snapshot_hash: str
    compiled_plan_checksum: str
    dataset_manifest_id: str
    dataset_hash: str
    input_bundle_fingerprint: str
    feature_materialization_version: str
    execution_policy_version: str
    precision_rules_version: str
    calculation_model_version: str | None
    cost_model_version: str | None
    execution_model_version: str | None


@dataclass(frozen=True, slots=True)
class TradeDetailView:
    record_id: str
    occurred_at: datetime
    kind: str
    order_id: str
    instrument_id: str
    order_status: str
    cash_after: Decimal
    positions_after: tuple[PositionAfter, ...]
    reason_code: str | None
    fill_id: str | None
    quantity: Decimal | None
    base_price: Decimal | None
    price: Decimal | None
    gross_amount: Decimal | None
    slippage_amount: Decimal | None
    fee: Decimal | None
    cost_basis: Decimal | None
    realized_pnl: Decimal | None


@dataclass(frozen=True, slots=True)
class _QueryEntry:
    run: RunProjection
    result: ResultSnapshot | None = None
    details: DetailObjectBundle | None = None
    monthly: tuple[MonthlyJudgmentSummary, ...] = field(default=())

    @property
    def owner_account_id(self) -> str:
        return self.run.owner_account_id


# --------------------------------------------------------------------------------
# store
# --------------------------------------------------------------------------------


class BacktestResultQueryStore(Protocol):
    """The three reads the service makes, split by how much evidence each needs.

    `get_owned_run` exists because `overview` and the run half of `inputs` need only
    the run itself. A durable store loads a completed run's result object and every
    ET-week Parquet part inside `get_owned`; making the metadata reads go through the
    same door would make an overview cost an object fetch per part.
    """

    def list_owned(
        self,
        owner_account_id: str,
        *,
        bot_id: str | None,
        limit: int,
        offset: int,
    ) -> tuple[_QueryEntry, ...]: ...

    def get_owned_run(self, owner_account_id: str, run_id: str) -> RunProjection: ...

    def get_owned(self, owner_account_id: str, run_id: str) -> _QueryEntry: ...


class InMemoryBacktestResultQueryStore:
    """Atomic local projection boundary for future RDB/object adapters."""

    def __init__(self) -> None:
        self._entries: dict[str, _QueryEntry] = {}
        self._lock = threading.RLock()

    def upsert_run(self, run: RunProjection) -> None:
        _validate_run(run)
        if run.status == COMPLETED:
            raise QueryIntegrityError(
                f"{COMPLETED} projections must use publish_completed atomically"
            )
        with self._lock:
            existing = self._entries.get(run.run_id)
            self._validate_update(existing, run)
            self._entries[run.run_id] = _QueryEntry(
                run=run,
                result=existing.result if existing is not None else None,
                details=existing.details if existing is not None else None,
                monthly=existing.monthly if existing is not None else (),
            )

    def publish_completed(
        self,
        run: RunProjection,
        result: ResultSnapshot,
        details: DetailObjectBundle,
        monthly: tuple[MonthlyJudgmentSummary, ...],
    ) -> None:
        monthly = tuple(monthly)
        _validate_completed(run, result, details, monthly)
        with self._lock:
            existing = self._entries.get(run.run_id)
            self._validate_update(existing, run, publishing_completed=True)
            candidate = _QueryEntry(run=run, result=result, details=details, monthly=monthly)
            if existing is not None and existing.result is not None:
                if existing == candidate:
                    return
                raise QueryIntegrityError(
                    "completed run already has a different immutable query result"
                )
            self._entries[run.run_id] = candidate

    def list_owned(
        self,
        owner_account_id: str,
        *,
        bot_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[_QueryEntry, ...]:
        """Owner's runs, newest first, filtered and paged *before* the slice.

        Filtering after the page is taken would drop runs the caller can never reach,
        so `bot_id` belongs here rather than in the service.
        """

        owner_account_id = _uuid(owner_account_id, "owner_account_id")
        if bot_id is not None:
            bot_id = _uuid(bot_id, "bot_id")
        _validate_page(limit, offset)
        with self._lock:
            owned = [
                entry
                for entry in self._entries.values()
                if entry.owner_account_id == owner_account_id
                and (bot_id is None or entry.run.bot_id == bot_id)
            ]
        owned.sort(key=lambda entry: (entry.run.queued_at, entry.run.run_id), reverse=True)
        return tuple(owned[offset : offset + limit])

    def get_owned_run(self, owner_account_id: str, run_id: str) -> RunProjection:
        return self.get_owned(owner_account_id, run_id).run

    def get_owned(self, owner_account_id: str, run_id: str) -> _QueryEntry:
        owner_account_id = _uuid(owner_account_id, "owner_account_id")
        run_id = _uuid(run_id, "run_id")
        with self._lock:
            entry = self._entries.get(run_id)
            if entry is None or entry.owner_account_id != owner_account_id:
                raise QueryNotFound("backtest not found")
            return entry

    @staticmethod
    def _validate_update(
        existing: _QueryEntry | None,
        run: RunProjection,
        *,
        publishing_completed: bool = False,
    ) -> None:
        if existing is None:
            return
        if existing.run.owner_account_id != run.owner_account_id:
            raise QueryIntegrityError("backtest owner cannot change")
        if existing.run.bot_id != run.bot_id:
            raise QueryIntegrityError("backtest bot cannot change")
        if existing.run.inputs != run.inputs:
            raise QueryIntegrityError("immutable backtest inputs cannot change")
        if run.version < existing.run.version:
            raise QueryIntegrityError("older query projection cannot replace a newer one")
        previous_status = existing.run.status
        if previous_status in _TERMINAL and run.status != previous_status:
            raise QueryIntegrityError("terminal backtest status cannot change")
        if run.status not in _ALLOWED_TRANSITIONS[previous_status]:
            raise QueryIntegrityError(
                f"invalid query status transition: {previous_status} to {run.status}"
            )
        if run.status == COMPLETED and not publishing_completed:
            raise QueryIntegrityError(
                f"{COMPLETED} projections must use publish_completed atomically"
            )


# --------------------------------------------------------------------------------
# the durable store: reconstruct-on-read
# --------------------------------------------------------------------------------


class DurableBacktestResultQueryStore:
    """`BacktestResultQueryStore` over the canonical tables and the object store.

    See the module docstring for why this reconstructs rather than reads a projection,
    and what that costs. Nothing here writes: publication is
    `wiring.DurableResultPublisher`'s job, and a read model that could write would be a
    second publisher.

    Every failure mode is a refusal. A run whose pins are missing, whose result object
    is absent from the bucket, whose bytes no longer hash to `storage.objects`, whose
    Parquet footer disagrees with `detail_manifests`, or whose monthly document no
    longer addresses its own `summary_hash`, raises `QueryIntegrityError`. None of them
    can produce a short month or an empty list.
    """

    def __init__(self, *, persistence: BacktestPersistence, object_store: ObjectStore) -> None:
        self._persistence = persistence
        self._store = object_store

    # -- store protocol ----------------------------------------------------

    def list_owned(
        self,
        owner_account_id: str,
        *,
        bot_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[_QueryEntry, ...]:
        """One page of the owner's runs, from rows: two queries plus one per completed
        run for its `result_manifest_id`.

        A listing that went through `get_owned` would fetch every ET-week Parquet part
        of every completed run on the page to answer four fields. The only object this
        can touch is the result snapshot of a completed run that produced no month at
        all, which is the fallback `_result_manifest_id` documents.
        """

        owner = UUID(_uuid(owner_account_id, "owner_account_id"))
        bot = None if bot_id is None else UUID(_uuid(bot_id, "bot_id"))
        _validate_page(limit, offset)
        with self._read() as uow:
            rows = uow.runs.list_by_owner(owner, limit=limit, offset=offset, bot_id=bot)
            pins = {pin.run_id: pin for pin in uow.pins.list_by_ids([row.id for row in rows])}
            return tuple(
                _QueryEntry(run=self._projection(uow, row, self._require_pin(pins.get(row.id), row)))
                for row in rows
            )

    def get_owned_run(self, owner_account_id: str, run_id: str) -> RunProjection:
        with self._read() as uow:
            row = self._owned_run(uow, owner_account_id, run_id)
            return self._projection(uow, row, self._require_pin(uow.pins.find(row.id), row))

    def get_owned(self, owner_account_id: str, run_id: str) -> _QueryEntry:
        with self._read() as uow:
            row = self._owned_run(uow, owner_account_id, run_id)
            pin = self._require_pin(uow.pins.find(row.id), row)
            if row.status.value != COMPLETED:
                return _QueryEntry(run=self._projection(uow, row, pin))

            performance = self._performance(uow, row)
            result = self._load_result(uow, row, performance)
            details = self._load_details(uow, row, result, performance.calculated_at)
            monthly = self._monthly(uow, row)
            self._check_result_agrees(row, pin, result, performance, monthly, uow)
            return _QueryEntry(
                run=self._projection(uow, row, pin, result.manifest.result_manifest_id),
                result=result,
                details=details,
                monthly=monthly,
            )

    # -- reads -------------------------------------------------------------

    @contextmanager
    def _read(self) -> Iterator[Any]:
        """One read-only transaction per query, so a page is internally consistent."""

        with self._persistence.read_only() as uow:
            yield uow

    @staticmethod
    def _owned_run(uow: Any, owner_account_id: str, run_id: str) -> RunRow:
        """Owner scoping is not-found, never forbidden: a foreign run must not leak."""

        owner = UUID(_uuid(owner_account_id, "owner_account_id"))
        identifier = UUID(_uuid(run_id, "run_id"))
        try:
            row: RunRow = uow.runs.get_owned(owner, identifier)
        except RowNotFound as exc:
            raise QueryNotFound("backtest not found") from exc
        return row

    @staticmethod
    def _require_pin(pin: RunInputPinRow | None, row: RunRow) -> RunInputPinRow:
        if pin is None:
            raise QueryIntegrityError(
                f"backtest run {row.id} has no backtest.run_input_pins row, so its pinned "
                "request inputs cannot be reported; the row is written in the acceptance "
                "transaction, so this run predates that migration or was inserted by "
                "another writer"
            )
        return pin

    @staticmethod
    def _performance(uow: Any, row: RunRow) -> PerformanceSummaryRow:
        try:
            summary: PerformanceSummaryRow = uow.performance.get(row.id)
        except RowNotFound as exc:
            raise QueryIntegrityError(
                f"backtest run {row.id} is {COMPLETED} but has no performance summary row"
            ) from exc
        return summary

    def _projection(
        self,
        uow: Any,
        row: RunRow,
        pin: RunInputPinRow,
        result_manifest_id: str | None = None,
    ) -> RunProjection:
        """One `backtest.runs` row plus its pins, as the read model needs it.

        `version` is derived rather than stored: `QUEUED` is 1, a started run 2, a
        terminal one 3. It only has to be monotone, and those three transitions are the
        only ones `backtest.run_status` allows.

        `missing_requirements` is empty because no column holds it - see the module
        docstring. `reason_code` carries the `UNAVAILABLE` answer, and it is
        `runs.failure_code`, which is exactly what the worker published.
        """

        status = row.status.value
        if status == COMPLETED and result_manifest_id is None:
            result_manifest_id = self._result_manifest_id(uow, row)
        try:
            return RunProjection(
                run_id=str(row.id),
                bot_id=str(row.bot_id),
                owner_account_id=str(row.owner_account_id),
                status=status,
                queued_at=row.queued_at,
                inputs=RunInputs(
                    compiled_plan_checksum=pin.compiled_plan_checksum,
                    strategy_snapshot_hash=pin.strategy_snapshot_hash,
                    dataset_manifest_id=str(pin.dataset_manifest_id),
                    dataset_hash=pin.dataset_hash,
                    input_bundle_fingerprint=pin.input_bundle_hash,
                    feature_materialization_version=pin.feature_materialization_version,
                    execution_policy_version=pin.execution_policy_version,
                    precision_rules_version=row.precision_rules_version,
                ),
                version=1 + (row.started_at is not None) + (status in _TERMINAL),
                started_at=row.started_at,
                finished_at=row.completed_at,
                failure_code=row.failure_code if status == FAILED else None,
                reason_code=row.failure_code if status == UNAVAILABLE else None,
                result_manifest_id=result_manifest_id,
            )
        except QueryValidationError as exc:
            # The caller supplied nothing wrong; the stored row cannot be rendered.
            raise QueryIntegrityError(f"backtest run {row.id} cannot be projected: {exc}") from exc

    def _result_manifest_id(self, uow: Any, row: RunRow) -> str:
        """The result manifest id of a completed run, without fetching an object.

        `backtest.*` has no column for it, but every
        `monthly_judgment_summaries.summary_document` names it and the row's
        `summary_hash` is the document's content address, so reading it there is as
        trustworthy as a column would be. A completed run that produced no month at all
        - no evaluations and no records - has no such row, and only then is the result
        object read.
        """

        summaries = self._monthly(uow, row)
        named = {item.result_manifest_id for item in summaries}
        if len(named) > 1:
            raise QueryIntegrityError(
                f"backtest run {row.id} has monthly summaries naming {len(named)} different "
                "result manifests"
            )
        if named:
            return next(iter(named))
        return self._load_result(uow, row, self._performance(uow, row)).manifest.result_manifest_id

    def _monthly(self, uow: Any, row: RunRow) -> tuple[MonthlyJudgmentSummary, ...]:
        """`monthly_judgment_summaries` rows, re-derived from their own documents.

        The six canonical counters exist both as columns and inside the document. They
        are asserted equal: a row whose columns and jsonb disagree is a write that went
        wrong, and serving either half of it would be serving a number nobody computed.
        """

        summaries: list[MonthlyJudgmentSummary] = []
        for stored in uow.monthly.list_for_run(row.id):
            try:
                summary = summary_from_document(stored.summary_document, stored.summary_hash)
            except (MonthlyJudgmentIntegrityError, MonthlyJudgmentValidationError) as exc:
                raise QueryIntegrityError(
                    f"monthly summary {stored.id} of run {row.id} is unreadable: {exc}"
                ) from exc
            if summary.et_month.key != stored.et_year_month:
                raise QueryIntegrityError(
                    f"monthly summary {stored.id} row says {stored.et_year_month} and its "
                    f"document says {summary.et_month.key}"
                )
            differing = [
                name
                for name in summary.counters()
                if getattr(stored, name) != getattr(summary, name)
            ]
            if differing:
                raise QueryIntegrityError(
                    f"monthly summary {stored.id} columns and summary_document disagree on "
                    f"{differing}"
                )
            summaries.append(summary)
        return tuple(summaries)

    def _load_result(
        self, uow: Any, row: RunRow, performance: PerformanceSummaryRow
    ) -> ResultSnapshot:
        stored = uow.objects.find_result_snapshot_object(row.id, bucket_name=self._store.bucket_name)
        if stored is None:
            raise QueryIntegrityError(
                f"backtest run {row.id} is {COMPLETED} but no AVAILABLE result snapshot object "
                f"is registered under backtest-results/{row.id}/ in bucket "
                f"{self._store.bucket_name!r}"
            )
        data = self._read_object(stored)
        try:
            result = ResultSnapshotBuilder.rebuild(data, performance.calculated_at)
        except ResultIntegrityError as exc:
            raise QueryIntegrityError(f"result object of run {row.id} is unusable: {exc}") from exc

        if result.manifest.content_hash != stored.content_hash:
            raise QueryIntegrityError(
                f"rebuilt result object of run {row.id} addresses {result.manifest.content_hash}, "
                f"storage.objects says {stored.content_hash}"
            )
        if result.run_snapshot.backtest_run_id != str(row.id):
            raise QueryIntegrityError(
                f"result object under run {row.id} reports run "
                f"{result.run_snapshot.backtest_run_id}"
            )
        if result.summary.result_hash != performance.result_hash:
            raise QueryIntegrityError(
                f"run {row.id} performance_summaries.result_hash is {performance.result_hash} "
                f"but the stored evidence produces {result.summary.result_hash}"
            )
        if row.result_hash is not None and row.result_hash.removeprefix("sha256:") != result.summary.result_hash:
            raise QueryIntegrityError(
                f"run {row.id} runs.result_hash is {row.result_hash} but the stored evidence "
                f"produces {result.summary.result_hash}"
            )
        return result

    def _load_details(
        self,
        uow: Any,
        row: RunRow,
        result: ResultSnapshot,
        created_at: datetime,
    ) -> DetailObjectBundle:
        manifests: Sequence[DetailManifestRow] = uow.manifests.list_for_run(row.id)
        try:
            objects = {
                stored.id: stored
                for stored in uow.objects.require_available(
                    [manifest.object_id for manifest in manifests]
                )
            }
        except (RowNotFound, PersistenceError) as exc:
            raise QueryIntegrityError(
                f"run {row.id} points at detail objects that are missing or not AVAILABLE: {exc}"
            ) from exc

        parts: list[tuple[DetailObjectDescriptor, bytes]] = []
        for manifest in manifests:
            stored = objects[manifest.object_id]
            parts.append((self._descriptor(manifest, stored), self._read_object(stored)))
        try:
            return reassemble_detail_bundle(
                result_manifest_id=result.manifest.result_manifest_id,
                run_snapshot_id=result.run_snapshot.snapshot_id,
                backtest_run_id=str(row.id),
                strategy_version_id=result.run_snapshot.strategy_version_id,
                created_at=created_at,
                parts=parts,
            )
        except (DetailIntegrityError, DetailObjectValidationError) as exc:
            raise QueryIntegrityError(
                f"detail evidence of run {row.id} does not reassemble: {exc}"
            ) from exc

    def _descriptor(
        self, manifest: DetailManifestRow, stored: StorageObjectRow
    ) -> DetailObjectDescriptor:
        if manifest.supersedes_manifest_id is not None:
            raise QueryIntegrityError(
                f"detail manifest {manifest.id} supersedes {manifest.supersedes_manifest_id}, but "
                "backtest.detail_manifests has no base_object_id / correction_of_object_id "
                "columns and detail_hash covers both, so the corrected part cannot be "
                "reassembled; see db/migration-contributions/change-requests/"
                "2026-08-02-backtest-run-input-pins.md"
            )
        if manifest.schema_version != stored.schema_version:
            raise QueryIntegrityError(
                f"detail manifest {manifest.id} declares schema {manifest.schema_version} and its "
                f"storage object declares {stored.schema_version}"
            )
        self._check_bucket(stored)
        return DetailObjectDescriptor(
            storage_object_id=str(manifest.object_id),
            detail_manifest_id=str(manifest.id),
            record_type=DetailObjectKind(manifest.record_type),
            week=EtWeek(manifest.week_start_date),
            part_number=manifest.part_number,
            period_start=manifest.period_start,
            period_end=manifest.period_end,
            object_key=stored.object_key,
            media_type=stored.media_type,
            file_format=stored.file_format,
            compression_codec=stored.compression_codec,
            schema_version=manifest.schema_version,
            row_count=manifest.row_count,
            byte_size=stored.byte_size,
            content_hash=stored.content_hash,
            source_set_hash=manifest.source_set_hash,
            detail_hash=manifest.detail_hash,
            created_at=manifest.created_at,
        )

    def _check_bucket(self, stored: StorageObjectRow) -> None:
        """`storage.objects` identity includes the provider and bucket.

        The same content in another deployment's bucket is a different row, and reading
        it out of *this* store would answer with bytes the row does not describe.
        """

        if (
            stored.storage_provider != self._store.storage_provider
            or stored.bucket_name != self._store.bucket_name
        ):
            raise QueryIntegrityError(
                f"storage object {stored.id} lives in "
                f"{stored.storage_provider}/{stored.bucket_name}, but this deployment reads "
                f"{self._store.storage_provider}/{self._store.bucket_name}"
            )

    def _read_object(self, stored: StorageObjectRow) -> bytes:
        """Fetch and re-hash. A lost or rewritten object is never an empty answer."""

        self._check_bucket(stored)
        try:
            with self._store.open(stored.object_key) as handle:
                data = handle.read()
        except Exception as exc:
            raise QueryIntegrityError(
                f"stored object {stored.object_key} could not be read back: {exc}"
            ) from exc
        if len(data) != stored.byte_size:
            raise QueryIntegrityError(
                f"stored object {stored.object_key} is {len(data)} bytes, storage.objects says "
                f"{stored.byte_size}"
            )
        digest = sha256_bytes(data)
        if digest != stored.content_hash:
            raise QueryIntegrityError(
                f"stored object {stored.object_key} hashes to {digest}, storage.objects says "
                f"{stored.content_hash}"
            )
        return data

    def _check_result_agrees(
        self,
        row: RunRow,
        pin: RunInputPinRow,
        result: ResultSnapshot,
        performance: PerformanceSummaryRow,
        monthly: tuple[MonthlyJudgmentSummary, ...],
        uow: Any,
    ) -> None:
        """Cross-artifact checks the in-memory store gets for free by holding one value.

        Reconstructing means the pieces arrive separately, so the agreements
        `publish_completed` asserts at write time are re-asserted here at read time.
        """

        if performance.metric_catalog_version != result.summary.metric_catalog_version:
            raise QueryIntegrityError(
                f"run {row.id} performance summary was computed under "
                f"{performance.metric_catalog_version}, the stored evidence under "
                f"{result.summary.metric_catalog_version}"
            )
        if performance.source_set_hash != result.summary.source_set_hash:
            raise QueryIntegrityError(f"run {row.id} performance source_set_hash does not match")
        if performance.input_hash != result.summary.input_hash:
            raise QueryIntegrityError(f"run {row.id} performance input_hash does not match")
        for summary in monthly:
            if summary.run_snapshot_id != result.run_snapshot.snapshot_id:
                raise QueryIntegrityError(
                    f"monthly summary {summary.summary_id} names another run snapshot"
                )
            if summary.result_manifest_id != result.manifest.result_manifest_id:
                raise QueryIntegrityError(
                    f"monthly summary {summary.summary_id} names another result manifest"
                )
        if result.run_snapshot.input_bundle_fingerprint != pin.input_bundle_hash.removeprefix("sha256:"):
            raise QueryIntegrityError(
                f"run {row.id} pinned {pin.input_bundle_hash} but the stored evidence was "
                f"produced from sha256:{result.run_snapshot.input_bundle_fingerprint}"
            )
        self._check_locked_dataset(uow, row, pin)

    @staticmethod
    def _check_locked_dataset(uow: Any, row: RunRow, pin: RunInputPinRow) -> None:
        """The request's dataset and the bundle the worker locked must be the same one.

        `run_input_pins` records what the request asked for; `input_datasets` records
        what the run actually locked. They are written by different processes at
        different times, so the read model states the agreement rather than assuming it.
        """

        try:
            bundle = uow.inputs.get_by_run(row.id)
        except RowNotFound:
            # A run can complete without a bundle only if the publisher changed; that
            # is not this read's business to police, and nothing is being reported
            # from the missing rows.
            return
        locked = {
            (dataset.dataset_manifest_id, dataset.locked_dataset_hash)
            for dataset in uow.inputs.datasets_for(bundle.id)
        }
        if locked and (pin.dataset_manifest_id, pin.dataset_hash) not in locked:
            raise QueryIntegrityError(
                f"run {row.id} pinned dataset {pin.dataset_manifest_id}@{pin.dataset_hash} but "
                f"locked {sorted((str(item[0]), item[1]) for item in locked)}"
            )


# --------------------------------------------------------------------------------
# service
# --------------------------------------------------------------------------------


class BacktestResultQueryService:
    def __init__(self, store: BacktestResultQueryStore) -> None:
        self._store = store

    def list_runs(
        self,
        owner_account_id: str,
        *,
        bot_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[BacktestListItem, ...]:
        """One page of the owner's runs, newest first.

        `limit` defaults to the same 50 the `/api/v1/backtests` route documents, and is
        a page size rather than a policy: the store applies it after filtering, so a
        caller that asks for more gets more.
        """

        return tuple(
            BacktestListItem(
                run_id=entry.run.run_id,
                bot_id=entry.run.bot_id,
                status=entry.run.status,
                requested_at=entry.run.queued_at,
            )
            for entry in self._store.list_owned(
                owner_account_id, bot_id=bot_id, limit=limit, offset=offset
            )
        )

    def overview(self, owner_account_id: str, run_id: str) -> BacktestOverview:
        run = self._store.get_owned_run(owner_account_id, run_id)
        return BacktestOverview(
            run_id=run.run_id,
            bot_id=run.bot_id,
            status=run.status,
            requested_at=run.queued_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            reason_code=run.reason_code if run.status == UNAVAILABLE else run.failure_code,
            missing_requirements=run.missing_requirements,
            result_manifest_id=run.result_manifest_id,
        )

    def inputs_and_models(self, owner_account_id: str, run_id: str) -> InputModelView:
        entry = self._store.get_owned(owner_account_id, run_id)
        inputs = entry.run.inputs
        snapshot = entry.result.run_snapshot if entry.result is not None else None
        return InputModelView(
            run_id=entry.run.run_id,
            bot_id=entry.run.bot_id,
            strategy_snapshot_hash=inputs.strategy_snapshot_hash,
            compiled_plan_checksum=inputs.compiled_plan_checksum,
            dataset_manifest_id=inputs.dataset_manifest_id,
            dataset_hash=inputs.dataset_hash,
            input_bundle_fingerprint=inputs.input_bundle_fingerprint,
            feature_materialization_version=inputs.feature_materialization_version,
            execution_policy_version=inputs.execution_policy_version,
            precision_rules_version=inputs.precision_rules_version,
            calculation_model_version=(
                snapshot.calculation_model_version if snapshot is not None else None
            ),
            cost_model_version=(snapshot.cost_model_version if snapshot is not None else None),
            execution_model_version=(
                snapshot.execution_model_version if snapshot is not None else None
            ),
        )

    def performance(self, owner_account_id: str, run_id: str) -> PerformanceSummary:
        entry = self._completed(owner_account_id, run_id)
        assert entry.result is not None
        return entry.result.summary

    def monthly_judgments(
        self, owner_account_id: str, run_id: str
    ) -> tuple[MonthlyJudgmentSummary, ...]:
        return self._completed(owner_account_id, run_id).monthly

    def monthly_trades(
        self,
        owner_account_id: str,
        run_id: str,
        et_month: EtMonth,
    ) -> tuple[TradeDetailView, ...]:
        if not isinstance(et_month, EtMonth):
            raise QueryValidationError("et_month must be an EtMonth")
        entry = self._completed(owner_account_id, run_id)
        assert entry.details is not None
        return _read_month(entry, et_month)

    def _completed(self, owner_account_id: str, run_id: str) -> _QueryEntry:
        entry = self._store.get_owned(owner_account_id, run_id)
        if entry.run.status != COMPLETED:
            raise QueryNotReady(
                f"backtest result is not available for status {entry.run.status}"
            )
        if entry.result is None or entry.details is None:
            raise QueryIntegrityError("completed run is missing immutable result artifacts")
        return entry


# --------------------------------------------------------------------------------
# validation
# --------------------------------------------------------------------------------


def _validate_page(limit: int, offset: int) -> None:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise QueryValidationError("limit must be a positive integer")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise QueryValidationError("offset must not be negative")


def _validate_run(run: RunProjection) -> None:
    if not isinstance(run, RunProjection):
        raise QueryValidationError("run must be a RunProjection")


def _validate_completed(
    run: RunProjection,
    result: ResultSnapshot,
    details: DetailObjectBundle,
    monthly: tuple[MonthlyJudgmentSummary, ...],
) -> None:
    _validate_run(run)
    if run.status != COMPLETED:
        raise QueryIntegrityError(f"only {COMPLETED} runs can publish result queries")
    try:
        ResultSnapshotBuilder.verify(result)
    except ResultIntegrityError as exc:
        raise QueryIntegrityError(f"result integrity failed: {exc}") from exc
    try:
        DetailObjectBuilder.verify(details)
    except DetailIntegrityError as exc:
        raise QueryIntegrityError(f"detail integrity failed: {exc}") from exc

    snapshot = result.run_snapshot
    manifest = result.manifest
    detail_manifest = details.manifest
    expected = {
        "run identity": (run.run_id, snapshot.backtest_run_id),
        "input fingerprint": (
            run.inputs.input_bundle_fingerprint,
            snapshot.input_bundle_fingerprint,
        ),
        "result manifest": (run.result_manifest_id, manifest.result_manifest_id),
        "result manifest run": (manifest.backtest_run_id, run.run_id),
        "detail result manifest": (
            detail_manifest.result_manifest_id,
            manifest.result_manifest_id,
        ),
        "detail run snapshot": (detail_manifest.run_snapshot_id, snapshot.snapshot_id),
        "detail run": (detail_manifest.backtest_run_id, run.run_id),
    }
    for label, values in expected.items():
        if values[0] != values[1]:
            raise QueryIntegrityError(f"{label} does not match")

    if any(not isinstance(item, MonthlyJudgmentSummary) for item in monthly):
        raise QueryIntegrityError("monthly values contain an invalid summary")
    months = [item.et_month for item in monthly]
    if len(set(months)) != len(months) or months != sorted(months):
        raise QueryIntegrityError("monthly summaries must be unique and ordered")
    for item in monthly:
        if item.run_snapshot_id != snapshot.snapshot_id:
            raise QueryIntegrityError("monthly run snapshot does not match")
        if item.result_manifest_id != manifest.result_manifest_id:
            raise QueryIntegrityError("monthly result manifest does not match")

    record_ids_by_month: dict[EtMonth, set[str]] = defaultdict(set)
    for record in result.records:
        record_ids_by_month[EtMonth.from_instant(record.occurred_at)].add(record.record_id)
    summary_by_month = {item.et_month: item for item in monthly}
    for month, record_ids in record_ids_by_month.items():
        summary = summary_by_month.get(month)
        if summary is None or set(summary.trade_record_ids) != record_ids:
            raise QueryIntegrityError(
                "monthly trade record identities do not match result records"
            )


# --------------------------------------------------------------------------------
# the ET week -> ET month join
# --------------------------------------------------------------------------------


def _week_overlaps_month(week: EtWeek, month: EtMonth) -> bool:
    """An ET Monday week covers seven ET dates, so it touches at most two ET months."""

    first = EtMonth(week.start_date.year, week.start_date.month)
    last_date = week.start_date + timedelta(days=6)
    return month in (first, EtMonth(last_date.year, last_date.month))


def _month_rows(
    details: DetailObjectBundle, record_type: DetailObjectKind, month: EtMonth
) -> list[dict[str, Any]]:
    """Rows of one record type whose own ET month is `month`, across week parts.

    Every part that overlaps the month is read — a single week part legitimately holds
    both October and November rows — and each row is then placed by its own instant.
    """

    rows: list[dict[str, Any]] = []
    for item in details.objects:
        descriptor = item.descriptor
        if descriptor.record_type is not record_type:
            continue
        if not _week_overlaps_month(descriptor.week, month):
            continue
        try:
            part = pq.read_table(pa.BufferReader(item.parquet_bytes)).to_pylist()
        except Exception as exc:
            raise QueryIntegrityError("monthly detail Parquet cannot be read") from exc
        rows.extend(row for row in part if EtMonth.from_instant(row["occurred_at"]) == month)
    return sorted(rows, key=lambda row: (row["occurred_at"], str(row["record_id"])))


def _read_month(entry: _QueryEntry, month: EtMonth) -> tuple[TradeDetailView, ...]:
    assert entry.details is not None
    summary = next((item for item in entry.monthly if item.et_month == month), None)
    trade_rows = _month_rows(entry.details, DetailObjectKind.TRADE_DETAIL, month)
    expected_ids = set(summary.trade_record_ids) if summary is not None else set()

    if {str(row["record_id"]) for row in trade_rows} != expected_ids:
        raise QueryIntegrityError(
            "monthly Parquet record identities do not match the monthly judgment summary"
        )
    if not trade_rows:
        return ()

    position_rows = _month_rows(entry.details, DetailObjectKind.POSITION_SNAPSHOT, month)
    positions: dict[str, list[PositionAfter]] = defaultdict(list)
    for row in position_rows:
        positions[str(row["record_id"])].append(
            PositionAfter(
                instrument_id=str(row["instrument_id"]),
                quantity=Decimal(str(row["quantity"])),
                cost_basis=Decimal(str(row["cost_basis"])),
            )
        )

    return tuple(
        TradeDetailView(
            record_id=str(row["record_id"]),
            occurred_at=row["occurred_at"],
            kind=str(row["kind"]),
            order_id=str(row["order_id"]),
            instrument_id=str(row["instrument_id"]),
            order_status=str(row["order_status"]),
            cash_after=Decimal(str(row["cash_after"])),
            positions_after=tuple(
                sorted(positions[str(row["record_id"])], key=lambda item: item.instrument_id)
            ),
            reason_code=_optional_text(row.get("reason_code")),
            fill_id=_optional_text(row.get("fill_id")),
            quantity=_optional_decimal(row.get("quantity")),
            base_price=_optional_decimal(row.get("base_price")),
            price=_optional_decimal(row.get("price")),
            gross_amount=_optional_decimal(row.get("gross_amount")),
            slippage_amount=_optional_decimal(row.get("slippage_amount")),
            fee=_optional_decimal(row.get("fee")),
            cost_basis=_optional_decimal(row.get("cost_basis")),
            realized_pnl=_optional_decimal(row.get("realized_pnl")),
        )
        for row in trade_rows
    )


# --------------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------------


def _uuid(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise QueryValidationError(f"{label} must be a UUID")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise QueryValidationError(f"{label} must be a UUID") from exc


def _aware(value: object, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise QueryValidationError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))
