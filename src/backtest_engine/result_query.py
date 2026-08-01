"""Owner-scoped read models for official automatic backtest results."""

from __future__ import annotations

import copy
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import ContractValidationError, validate_backtest_request
from .detail_object_manifest import (
    DetailIntegrityError,
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectKind,
)
from .lifecycle import BacktestRun
from .monthly_judgment import EtMonth, MonthlyJudgmentSummary
from .result_snapshot import (
    PerformanceSummary,
    PositionAfter,
    ResultIntegrityError,
    ResultSnapshot,
    ResultSnapshotBuilder,
)


_STATUSES = frozenset({"QUEUED", "RUNNING", "COMPLETE", "FAILED", "UNAVAILABLE"})


class QueryValidationError(ValueError):
    """Raised when a query identity or projection input is malformed."""


class QueryNotFound(LookupError):
    """Owner-safe not-found response that does not reveal foreign runs."""


class QueryNotReady(RuntimeError):
    """Raised when result-only data is requested before successful completion."""


class QueryIntegrityError(RuntimeError):
    """Raised when immutable run, result, or detail identities disagree."""


@dataclass(frozen=True, slots=True)
class BacktestListItem:
    run_id: str
    strategy_version_id: str
    status: str
    requested_at: datetime


@dataclass(frozen=True, slots=True)
class BacktestOverview:
    run_id: str
    strategy_version_id: str
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
    strategy_version_id: str
    strategy_snapshot_hash: str
    compiled_plan_hash: str
    dataset_manifest_id: str
    dataset_hash: str
    input_bundle_fingerprint: str
    feature_materialization_version: str
    execution_policy_version: str
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
    owner_account_id: str
    run: BacktestRun
    result: ResultSnapshot | None = None
    details: DetailObjectBundle | None = None
    monthly: tuple[MonthlyJudgmentSummary, ...] = ()


class BacktestResultQueryStore(Protocol):
    def list_owned(self, owner_account_id: str) -> tuple[_QueryEntry, ...]: ...

    def get_owned(self, owner_account_id: str, run_id: str) -> _QueryEntry: ...


class InMemoryBacktestResultQueryStore:
    """Atomic local projection boundary for future RDB/object adapters."""

    def __init__(self) -> None:
        self._entries: dict[str, _QueryEntry] = {}
        self._lock = threading.RLock()

    def upsert_run(self, owner_account_id: str, run: BacktestRun) -> None:
        owner_account_id = _uuid(owner_account_id, "owner_account_id")
        _validate_run(run)
        if run.status == "COMPLETE":
            raise QueryIntegrityError(
                "COMPLETE projections must use publish_completed atomically"
            )
        with self._lock:
            existing = self._entries.get(run.backtest_run_id)
            self._validate_update(existing, owner_account_id, run)
            self._entries[run.backtest_run_id] = _QueryEntry(
                owner_account_id=owner_account_id,
                run=copy.deepcopy(run),
                result=existing.result if existing is not None else None,
                details=existing.details if existing is not None else None,
                monthly=existing.monthly if existing is not None else (),
            )

    def publish_completed(
        self,
        owner_account_id: str,
        run: BacktestRun,
        result: ResultSnapshot,
        details: DetailObjectBundle,
        monthly: tuple[MonthlyJudgmentSummary, ...],
    ) -> None:
        owner_account_id = _uuid(owner_account_id, "owner_account_id")
        monthly = tuple(monthly)
        _validate_completed(run, result, details, monthly)
        with self._lock:
            existing = self._entries.get(run.backtest_run_id)
            self._validate_update(
                existing,
                owner_account_id,
                run,
                publishing_completed=True,
            )
            candidate = _QueryEntry(
                owner_account_id=owner_account_id,
                run=copy.deepcopy(run),
                result=result,
                details=details,
                monthly=monthly,
            )
            if existing is not None and existing.result is not None:
                if existing == candidate:
                    return
                raise QueryIntegrityError(
                    "completed run already has a different immutable query result"
                )
            self._entries[run.backtest_run_id] = candidate

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
        owner_account_id: str,
        run: BacktestRun,
        *,
        publishing_completed: bool = False,
    ) -> None:
        if existing is None:
            return
        if existing.owner_account_id != owner_account_id:
            raise QueryIntegrityError("backtest owner cannot change")
        if existing.run.request != run.request:
            raise QueryIntegrityError("immutable backtest request cannot change")
        if run.version < existing.run.version:
            raise QueryIntegrityError("older query projection cannot replace a newer one")
        previous_status = existing.run.status
        if previous_status in {"COMPLETE", "FAILED", "UNAVAILABLE"}:
            if run.status != previous_status:
                raise QueryIntegrityError("terminal backtest status cannot change")
        allowed = {
            "QUEUED": {"QUEUED", "RUNNING", "FAILED", "UNAVAILABLE", "COMPLETE"},
            "RUNNING": {"RUNNING", "FAILED", "UNAVAILABLE", "COMPLETE"},
            "COMPLETE": {"COMPLETE"},
            "FAILED": {"FAILED"},
            "UNAVAILABLE": {"UNAVAILABLE"},
        }
        if run.status not in allowed[previous_status]:
            raise QueryIntegrityError(
                f"invalid query status transition: {previous_status} to {run.status}"
            )
        if run.status == "COMPLETE" and not publishing_completed:
            raise QueryIntegrityError(
                "COMPLETE projections must use publish_completed atomically"
            )


class BacktestResultQueryService:
    def __init__(self, store: BacktestResultQueryStore) -> None:
        self._store = store

    def list_runs(
        self,
        owner_account_id: str,
        *,
        strategy_version_id: str | None = None,
    ) -> tuple[BacktestListItem, ...]:
        if strategy_version_id is not None:
            strategy_version_id = _uuid(
                strategy_version_id, "strategy_version_id"
            )
        items = [
            _list_item(entry.run)
            for entry in self._store.list_owned(owner_account_id)
            if strategy_version_id is None
            or entry.run.request["strategy_version_id"] == strategy_version_id
        ]
        return tuple(
            sorted(
                items,
                key=lambda item: (item.requested_at, item.run_id),
                reverse=True,
            )
        )

    def overview(self, owner_account_id: str, run_id: str) -> BacktestOverview:
        entry = self._store.get_owned(owner_account_id, run_id)
        run = entry.run
        result = run.status_result or {}
        return BacktestOverview(
            run_id=run.backtest_run_id,
            strategy_version_id=str(run.request["strategy_version_id"]),
            status=run.status,
            requested_at=_timestamp(run.request["requested_at"], "requested_at"),
            started_at=_optional_timestamp(result.get("started_at"), "started_at"),
            finished_at=_optional_timestamp(
                result.get("completed_at")
                or result.get("failed_at")
                or result.get("decided_at"),
                "finished_at",
            ),
            reason_code=_reason_code(run),
            missing_requirements=_missing_requirements(run),
            result_manifest_id=(
                str(result["result_manifest_id"])
                if run.status == "COMPLETE"
                else None
            ),
        )

    def inputs_and_models(
        self, owner_account_id: str, run_id: str
    ) -> InputModelView:
        entry = self._store.get_owned(owner_account_id, run_id)
        request = entry.run.request
        snapshot = entry.result.run_snapshot if entry.result is not None else None
        return InputModelView(
            run_id=entry.run.backtest_run_id,
            strategy_version_id=str(request["strategy_version_id"]),
            strategy_snapshot_hash=str(request["strategy_snapshot_hash"]),
            compiled_plan_hash=str(request["compiled_plan_hash"]),
            dataset_manifest_id=str(request["dataset_manifest_id"]),
            dataset_hash=str(request["dataset_hash"]),
            input_bundle_fingerprint=str(request["input_bundle_fingerprint"]),
            feature_materialization_version=str(
                request["feature_materialization_version"]
            ),
            execution_policy_version=str(request["execution_policy_version"]),
            calculation_model_version=(
                snapshot.calculation_model_version if snapshot is not None else None
            ),
            cost_model_version=(
                snapshot.cost_model_version if snapshot is not None else None
            ),
            execution_model_version=(
                snapshot.execution_model_version if snapshot is not None else None
            ),
        )

    def performance(
        self, owner_account_id: str, run_id: str
    ) -> PerformanceSummary:
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
        if entry.run.status != "COMPLETE":
            raise QueryNotReady(
                f"backtest result is not available for status {entry.run.status}"
            )
        if entry.result is None or entry.details is None:
            raise QueryIntegrityError("complete run is missing immutable result artifacts")
        return entry


def _validate_run(run: BacktestRun) -> None:
    if not isinstance(run, BacktestRun):
        raise QueryValidationError("run must be a BacktestRun")
    if run.status not in _STATUSES:
        raise QueryValidationError("run status is unsupported")
    try:
        validate_backtest_request(run.request)
    except ContractValidationError as exc:
        raise QueryValidationError(str(exc)) from exc
    if run.backtest_run_id != run.request["backtest_run_id"]:
        raise QueryIntegrityError("run identity does not match immutable request")


def _validate_completed(
    run: BacktestRun,
    result: ResultSnapshot,
    details: DetailObjectBundle,
    monthly: tuple[MonthlyJudgmentSummary, ...],
) -> None:
    _validate_run(run)
    if run.status != "COMPLETE":
        raise QueryIntegrityError("only COMPLETE runs can publish result queries")
    try:
        ResultSnapshotBuilder.verify(result)
    except ResultIntegrityError as exc:
        raise QueryIntegrityError(f"result integrity failed: {exc}") from exc
    try:
        DetailObjectBuilder.verify(details)
    except DetailIntegrityError as exc:
        raise QueryIntegrityError(f"detail integrity failed: {exc}") from exc

    request = run.request
    snapshot = result.run_snapshot
    manifest = result.manifest
    detail_manifest = details.manifest
    status_result = run.status_result or {}
    expected = {
        "run identity": (run.backtest_run_id, snapshot.backtest_run_id),
        "strategy version": (
            str(request["strategy_version_id"]),
            snapshot.strategy_version_id,
        ),
        "input fingerprint": (
            str(request["input_bundle_fingerprint"]),
            snapshot.input_bundle_fingerprint,
        ),
        "result manifest status": (
            str(status_result.get("result_manifest_id")),
            manifest.result_manifest_id,
        ),
        "result manifest run": (manifest.backtest_run_id, run.backtest_run_id),
        "result manifest strategy": (
            manifest.strategy_version_id,
            snapshot.strategy_version_id,
        ),
        "detail result manifest": (
            detail_manifest.result_manifest_id,
            manifest.result_manifest_id,
        ),
        "detail run snapshot": (
            detail_manifest.run_snapshot_id,
            snapshot.snapshot_id,
        ),
        "detail run": (detail_manifest.backtest_run_id, run.backtest_run_id),
        "detail strategy": (
            detail_manifest.strategy_version_id,
            snapshot.strategy_version_id,
        ),
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
        record_ids_by_month[EtMonth.from_instant(record.occurred_at)].add(
            record.record_id
        )
    summary_by_month = {item.et_month: item for item in monthly}
    for month, record_ids in record_ids_by_month.items():
        summary = summary_by_month.get(month)
        if summary is None or set(summary.trade_record_ids) != record_ids:
            raise QueryIntegrityError(
                "monthly trade record identities do not match result records"
            )


def _list_item(run: BacktestRun) -> BacktestListItem:
    return BacktestListItem(
        run_id=run.backtest_run_id,
        strategy_version_id=str(run.request["strategy_version_id"]),
        status=run.status,
        requested_at=_timestamp(run.request["requested_at"], "requested_at"),
    )


def _reason_code(run: BacktestRun) -> str | None:
    result = run.status_result or {}
    if run.status == "FAILED":
        value = result.get("failure_code")
    elif run.status == "UNAVAILABLE":
        value = result.get("reason_code")
    else:
        return None
    return str(value) if value is not None else None


def _missing_requirements(run: BacktestRun) -> tuple[str, ...]:
    if run.status != "UNAVAILABLE":
        return ()
    values = (run.status_result or {}).get("missing_requirements", ())
    if not isinstance(values, list) or any(
        not isinstance(item, str) or not item for item in values
    ):
        raise QueryIntegrityError("unavailable missing requirements are invalid")
    return tuple(values)


def _read_month(entry: _QueryEntry, month: EtMonth) -> tuple[TradeDetailView, ...]:
    assert entry.details is not None
    summary = next((item for item in entry.monthly if item.et_month == month), None)
    objects = {
        item.descriptor.kind: item
        for item in entry.details.objects
        if item.descriptor.et_month == month
    }
    trade_object = objects.get(DetailObjectKind.TRADE_DETAIL)
    if summary is None and trade_object is None:
        return ()
    if summary is None or trade_object is None:
        raise QueryIntegrityError("monthly trade summary and detail object disagree")

    try:
        trade_rows = pq.read_table(
            pa.BufferReader(trade_object.parquet_bytes)
        ).to_pylist()
        position_object = objects.get(DetailObjectKind.POSITION_SNAPSHOT)
        position_rows = (
            pq.read_table(pa.BufferReader(position_object.parquet_bytes)).to_pylist()
            if position_object is not None
            else []
        )
    except Exception as exc:
        raise QueryIntegrityError("monthly detail Parquet cannot be read") from exc

    if {str(row["record_id"]) for row in trade_rows} != set(
        summary.trade_record_ids
    ):
        raise QueryIntegrityError("monthly Parquet record identities do not match")
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
                sorted(
                    positions[str(row["record_id"])],
                    key=lambda item: item.instrument_id,
                )
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


def _uuid(value: str, label: str) -> str:
    if not isinstance(value, str):
        raise QueryValidationError(f"{label} must be a UUID")
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise QueryValidationError(f"{label} must be a UUID") from exc


def _timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise QueryIntegrityError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise QueryIntegrityError(f"{label} must be a UTC timestamp") from exc
    return parsed


def _optional_timestamp(value: object, label: str) -> datetime | None:
    return None if value is None else _timestamp(value, label)


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))
