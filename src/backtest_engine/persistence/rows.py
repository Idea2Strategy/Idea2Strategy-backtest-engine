"""Row types for the canonical `backtest` tables.

One frozen dataclass per table, with exactly the canonical columns in canonical order.
These are deliberately dumb records: they carry no domain behaviour, so the later
stage that swaps the in-memory stores for these repositories only has to translate
between its aggregates and these rows.

Money is validated, never rounded. See `MoneyPrecisionError`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import UUID

from .errors import MoneyPrecisionError


__all__ = [
    "MONEY_PRECISION",
    "MONEY_SCALE",
    "RUN_INPUT_PIN_IDENTITY_FIELDS",
    "DetailManifestRow",
    "FailureConditionCountRow",
    "InputBundleRow",
    "InputDatasetRow",
    "InputFeatureMaterializationRow",
    "MonthlyJudgmentSummaryRow",
    "ObjectStatus",
    "PerformanceSummaryRow",
    "RunAttemptRow",
    "RunInputPinRow",
    "RunRow",
    "RunStatus",
    "StorageObjectRow",
    "WorkStatus",
    "row_to_params",
    "validate_money",
]


#: `numeric(24,8)`.
MONEY_PRECISION = 24
MONEY_SCALE = 8

#: What makes two `backtest.run_input_pins` offers the same pin. Everything except
#: `pinned_at`: two acceptances of the same request pin the same inputs, but they do
#: not happen at the same instant.
RUN_INPUT_PIN_IDENTITY_FIELDS: tuple[str, ...] = (
    "compiled_plan_checksum",
    "strategy_snapshot_hash",
    "dataset_manifest_id",
    "dataset_hash",
    "feature_materialization_version",
    "execution_policy_version",
)


class RunStatus(StrEnum):
    """`backtest.run_status`. `COMPLETED`, never `COMPLETE`."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNAVAILABLE = "UNAVAILABLE"


class WorkStatus(StrEnum):
    """`operations.work_status`, used by `backtest.run_attempts.status`."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    SKIPPED = "SKIPPED"


class ObjectStatus(StrEnum):
    """`storage.object_status`."""

    STAGED = "STAGED"
    AVAILABLE = "AVAILABLE"
    QUARANTINED = "QUARANTINED"
    SUPERSEDED = "SUPERSEDED"
    DELETED = "DELETED"


TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.UNAVAILABLE})

#: Canonical lifecycle. A run never leaves a terminal status.
RUN_STATUS_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.QUEUED: frozenset({RunStatus.RUNNING, RunStatus.FAILED, RunStatus.UNAVAILABLE}),
    RunStatus.RUNNING: frozenset({RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.UNAVAILABLE}),
    RunStatus.COMPLETED: frozenset(),
    RunStatus.FAILED: frozenset(),
    RunStatus.UNAVAILABLE: frozenset(),
}


def validate_money(value: Decimal, field: str) -> Decimal:
    """Reject a monetary value that `numeric(24,8)` cannot store exactly.

    PostgreSQL silently rounds an over-precise value to the column scale. Rounding is a
    reproducibility decision that belongs to the precision rules, so an unquantised
    value is an error here rather than a quiet mutation of the caller's number.
    """

    if not isinstance(value, Decimal):
        raise MoneyPrecisionError(f"{field} must be a Decimal, got {type(value).__name__}")
    if not value.is_finite():
        raise MoneyPrecisionError(f"{field} must be finite, got {value}")
    parts = value.as_tuple()
    exponent = parts.exponent
    if not isinstance(exponent, int):
        raise MoneyPrecisionError(f"{field} must be finite, got {value}")
    digits = parts.digits
    fractional = -exponent if exponent < 0 else 0
    if fractional > MONEY_SCALE:
        raise MoneyPrecisionError(
            f"{field}={value} has {fractional} fractional digits; numeric"
            f"({MONEY_PRECISION},{MONEY_SCALE}) would silently round it. Quantise first."
        )
    integral = len(digits) - fractional
    if integral > MONEY_PRECISION - MONEY_SCALE:
        raise MoneyPrecisionError(
            f"{field}={value} needs {integral} integral digits; numeric"
            f"({MONEY_PRECISION},{MONEY_SCALE}) allows {MONEY_PRECISION - MONEY_SCALE}"
        )
    return value


@dataclass(frozen=True, slots=True)
class RunRow:
    """`backtest.runs`."""

    id: UUID
    bot_id: UUID
    owner_account_id: UUID
    configuration_hash: str
    status: RunStatus
    evaluation_start: date
    evaluation_end: date
    initial_cash_amount: Decimal
    market_rules_version: str
    accounting_rules_version: str
    precision_rules_version: str
    fee_policy_id: UUID
    slippage_rate_bps: int
    buying_power_buffer_policy_id: UUID
    idempotency_key: str
    queued_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    failure_code: str | None = None
    result_hash: str | None = None
    #: `backtest.v1` COMPLETED -> `resultManifestId`. The only link from a completed
    #: run to the manifest that holds its result.
    result_manifest_id: UUID | None = None
    #: `backtest.v1` FAILED -> `retryable`. `None` means the run has not failed, which
    #: is a different fact from `False` ("it failed and re-queuing cannot help").
    retryable: bool | None = None
    #: `backtest.v1` UNAVAILABLE -> `missingRequirements`, in the order the worker
    #: sent them. The contract requires at least one entry alongside `reasonCode`, so
    #: an empty list is refused here rather than stored as "nothing was missing".
    missing_requirements: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        validate_money(self.initial_cash_amount, "initial_cash_amount")
        if self.evaluation_end < self.evaluation_start:
            raise ValueError("evaluation_end must not precede evaluation_start")
        if self.missing_requirements is not None:
            # A JSONB round trip returns a list; the row is frozen, so normalise once.
            requirements = tuple(self.missing_requirements)
            if not requirements:
                raise ValueError(
                    "missing_requirements must name at least one requirement; the "
                    "backtest.v1 UNAVAILABLE branch declares minItems 1, and an empty "
                    "list would read as 'nothing was missing'"
                )
            if any(not isinstance(item, str) or not item for item in requirements):
                raise ValueError("missing_requirements entries must be non-empty strings")
            object.__setattr__(self, "missing_requirements", requirements)


@dataclass(frozen=True, slots=True)
class RunAttemptRow:
    """`backtest.run_attempts`."""

    id: UUID
    run_id: UUID
    attempt_number: int
    worker_execution_key: str
    status: WorkStatus
    started_at: datetime
    completed_at: datetime | None = None
    failure_code: str | None = None

    def __post_init__(self) -> None:
        if self.attempt_number < 1:
            raise ValueError("attempt_number starts at 1")
        if not self.worker_execution_key.strip():
            raise ValueError("worker_execution_key must not be blank")


@dataclass(frozen=True, slots=True)
class RunInputPinRow:
    """`backtest.run_input_pins`. The request's pinned identifiers, written at accept.

    Every field is required. `runs.configuration_hash` is a digest *over* these values,
    so a blank one here would mean the fingerprint covers an empty string and the
    reproducibility boundary the API reports would be a fiction.
    """

    run_id: UUID
    compiled_plan_checksum: str
    strategy_snapshot_hash: str
    dataset_manifest_id: UUID
    dataset_hash: str
    feature_materialization_version: str
    execution_policy_version: str
    pinned_at: datetime

    def __post_init__(self) -> None:
        for name in (
            "compiled_plan_checksum",
            "strategy_snapshot_hash",
            "dataset_hash",
            "feature_materialization_version",
            "execution_policy_version",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} is NOT NULL in backtest.run_input_pins and must not be blank")


@dataclass(frozen=True, slots=True)
class InputBundleRow:
    """`backtest.input_bundles`. One per run, enforced by a unique constraint."""

    id: UUID
    run_id: UUID
    bundle_hash: str
    as_of_at: datetime
    locked_at: datetime


@dataclass(frozen=True, slots=True)
class InputDatasetRow:
    """`backtest.input_datasets`."""

    input_bundle_id: UUID
    dataset_manifest_id: UUID
    purpose_code: str
    locked_dataset_hash: str


@dataclass(frozen=True, slots=True)
class InputFeatureMaterializationRow:
    """`backtest.input_feature_materializations`."""

    input_bundle_id: UUID
    feature_materialization_id: UUID
    locked_result_hash: str


@dataclass(frozen=True, slots=True)
class MonthlyJudgmentSummaryRow:
    """`backtest.monthly_judgment_summaries`. All six canonical counters are required."""

    id: UUID
    run_id: UUID
    et_year_month: str
    evaluation_count: int
    active_branch_count: int
    trade_event_count: int
    data_gap_count: int
    triggered_count: int
    rejected_count: int
    summary_document: Mapping[str, Any]
    summary_hash: str

    def __post_init__(self) -> None:
        if len(self.et_year_month) != 7 or self.et_year_month[4] != "-":
            raise ValueError(f"et_year_month must be 'YYYY-MM', got {self.et_year_month!r}")
        for name in (
            "evaluation_count",
            "active_branch_count",
            "trade_event_count",
            "data_gap_count",
            "triggered_count",
            "rejected_count",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")


@dataclass(frozen=True, slots=True)
class FailureConditionCountRow:
    """`backtest.failure_condition_counts`."""

    id: UUID
    monthly_summary_id: UUID
    flow_or_branch_key: str
    first_failure_condition_key: str
    occurrence_count: int

    def __post_init__(self) -> None:
        if self.occurrence_count < 0:
            raise ValueError("occurrence_count must not be negative")


@dataclass(frozen=True, slots=True)
class PerformanceSummaryRow:
    """`backtest.performance_summaries`. Primary key is `run_id`."""

    run_id: UUID
    metric_catalog_version: str
    metrics_document: Mapping[str, Any]
    calculation_rules_version: str
    source_set_hash: str
    input_hash: str
    result_hash: str
    calculated_at: datetime


@dataclass(frozen=True, slots=True)
class DetailManifestRow:
    """`backtest.detail_manifests`. `week_start_date` is an ET Monday, not a month."""

    id: UUID
    run_id: UUID
    object_id: UUID
    record_type: str
    week_start_date: date
    period_start: datetime
    period_end: datetime
    part_number: int
    row_count: int
    schema_version: str
    source_set_hash: str
    supersedes_manifest_id: UUID | None
    detail_hash: str
    created_at: datetime

    def __post_init__(self) -> None:
        if self.week_start_date.weekday() != 0:
            raise ValueError(f"week_start_date must be a Monday (ET week boundary), got {self.week_start_date}")
        if self.part_number < 1:
            raise ValueError("part_number starts at 1")
        if self.row_count < 0:
            raise ValueError("row_count must not be negative")
        if self.period_end < self.period_start:
            raise ValueError("period_end must not precede period_start")


@dataclass(frozen=True, slots=True)
class StorageObjectRow:
    """`storage.objects`. Read-only from this repository."""

    id: UUID
    status: ObjectStatus
    storage_provider: str
    bucket_name: str
    object_key: str
    provider_version_id: str
    content_hash: str
    byte_size: int
    file_format: str
    compression_codec: str
    media_type: str
    schema_version: str
    row_count: int | None
    period_start: datetime | None
    period_end: datetime | None
    encryption_key_ref: str | None
    retention_policy_version: str
    retention_until: datetime | None
    legal_hold: bool
    created_at: datetime
    verified_at: datetime | None
    quarantined_at: datetime | None
    superseded_at: datetime | None
    deleted_at: datetime | None


def row_to_params(row: object) -> dict[str, Any]:
    """Flatten a row dataclass into SQLAlchemy Core insert parameters."""

    params: dict[str, Any] = {}
    for field in fields(row):  # type: ignore[arg-type]
        value = getattr(row, field.name)
        if isinstance(value, StrEnum):
            value = value.value
        elif isinstance(value, Mapping):
            value = dict(value)
        elif isinstance(value, tuple):
            # A tuple field always backs a `jsonb` column here; psycopg's JSON
            # adapter serialises a list, not a tuple.
            value = list(value)
        params[field.name] = value
    return params
