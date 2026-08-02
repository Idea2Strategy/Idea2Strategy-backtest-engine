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
"""

from __future__ import annotations

import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from .detail_object_manifest import (
    DetailIntegrityError,
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectKind,
    EtWeek,
)
from .monthly_judgment import EtMonth, MonthlyJudgmentSummary
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
    def list_owned(self, owner_account_id: str) -> tuple[_QueryEntry, ...]: ...

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

    def list_owned(self, owner_account_id: str) -> tuple[_QueryEntry, ...]:
        owner_account_id = _uuid(owner_account_id, "owner_account_id")
        with self._lock:
            return tuple(
                entry
                for entry in self._entries.values()
                if entry.owner_account_id == owner_account_id
            )

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
    ) -> tuple[BacktestListItem, ...]:
        if bot_id is not None:
            bot_id = _uuid(bot_id, "bot_id")
        items = [
            BacktestListItem(
                run_id=entry.run.run_id,
                bot_id=entry.run.bot_id,
                status=entry.run.status,
                requested_at=entry.run.queued_at,
            )
            for entry in self._store.list_owned(owner_account_id)
            if bot_id is None or entry.run.bot_id == bot_id
        ]
        return tuple(sorted(items, key=lambda item: (item.requested_at, item.run_id), reverse=True))

    def overview(self, owner_account_id: str, run_id: str) -> BacktestOverview:
        run = self._store.get_owned(owner_account_id, run_id).run
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
