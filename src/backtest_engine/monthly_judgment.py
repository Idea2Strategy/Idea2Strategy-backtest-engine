"""ET-month judgment summaries without raw non-trade evaluation storage.

A run can evaluate its conditions on every one-minute bar of a quarter. Storing
those evaluations is neither useful nor permitted, so each ET month is reduced
to one ``backtest.monthly_judgment_summaries`` row: six counters that say what
the month *did*, plus the first-failure histogram that says why it did not
trade. No evaluation identity and no non-first condition outcome survives the
reduction.

The six canonical counters (spec 2.2) are independent populations, not slices
of one total:

``evaluation_count``
    Every evaluation in the month, data gaps included. The denominator.
``active_branch_count``
    Pro branches that were actually active across those evaluations. An
    inactive branch was never evaluated and is never counted.
``trade_event_count``
    Trade-detail records in the month, of any kind.
``data_gap_count``
    Evaluations that could not run because required market data was missing.
    Distinct from "ran and every condition passed".
``triggered_count``
    Evaluations that emitted a trade.
``rejected_count``
    Trade-detail records the execution model rejected.

``summary_document`` is the jsonb payload of that row and ``summary_hash`` is
its content address, so two runs of the same month either agree exactly or
report different hashes.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo

from backtest_engine.result_snapshot import ResultRecord, ResultRecordKind


ET_TIMEZONE_ID = "America/New_York"
ET = ZoneInfo(ET_TIMEZONE_ID)
SHA256 = re.compile(r"^[0-9a-f]{64}$")
SUMMARY_SCHEMA_VERSION = 1

#: The six counters `backtest.monthly_judgment_summaries` requires, in the
#: canonical order used by the summary document and `counters()`.
CANONICAL_COUNTERS = (
    "evaluation_count",
    "active_branch_count",
    "trade_event_count",
    "data_gap_count",
    "triggered_count",
    "rejected_count",
)


class MonthlyJudgmentValidationError(ValueError):
    """Raised when monthly judgment evidence is ambiguous or inconsistent."""


class MonthlyJudgmentIntegrityError(RuntimeError):
    """Raised when a stored summary document no longer matches its content address."""


class StrategyMode(str, Enum):
    BASIC = "BASIC"
    PRO = "PRO"


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise MonthlyJudgmentValidationError(f"{label} must be a non-empty string")
    return value


def _uuid(value: str, label: str) -> str:
    _text(value, label)
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise MonthlyJudgmentValidationError(f"{label} must be a UUID") from exc


def _hash(value: str, label: str) -> str:
    if not isinstance(value, str) or not SHA256.fullmatch(value):
        raise MonthlyJudgmentValidationError(
            f"{label} must be a lowercase SHA-256"
        )
    return value


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MonthlyJudgmentValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True, order=True)
class EtMonth:
    year: int
    month: int

    def __post_init__(self) -> None:
        if not isinstance(self.year, int) or isinstance(self.year, bool) or self.year < 1:
            raise MonthlyJudgmentValidationError("ET year must be a positive integer")
        if (
            not isinstance(self.month, int)
            or isinstance(self.month, bool)
            or not 1 <= self.month <= 12
        ):
            raise MonthlyJudgmentValidationError("ET month must be between 1 and 12")

    @classmethod
    def from_instant(cls, value: datetime) -> EtMonth:
        instant = _utc(value, "instant")
        local = instant.astimezone(ET)
        return cls(local.year, local.month)

    @property
    def key(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True, slots=True)
class ConditionOutcome:
    condition_id: str
    passed: bool

    def __post_init__(self) -> None:
        _text(self.condition_id, "condition_id")
        if not isinstance(self.passed, bool):
            raise MonthlyJudgmentValidationError("passed must be a bool")


def _outcomes(
    supplied: Iterable[ConditionOutcome], label: str
) -> tuple[ConditionOutcome, ...]:
    values = tuple(supplied)
    if any(not isinstance(item, ConditionOutcome) for item in values):
        raise MonthlyJudgmentValidationError(
            f"{label} must contain ConditionOutcome values"
        )
    identifiers = [item.condition_id for item in values]
    if len(set(identifiers)) != len(identifiers):
        raise MonthlyJudgmentValidationError(
            f"{label} condition_id values must be unique"
        )
    return values


@dataclass(frozen=True, slots=True)
class BranchEvaluation:
    branch_id: str
    active: bool
    outcomes: tuple[ConditionOutcome, ...]

    def __post_init__(self) -> None:
        _text(self.branch_id, "branch_id")
        if not isinstance(self.active, bool):
            raise MonthlyJudgmentValidationError("active must be a bool")
        object.__setattr__(
            self, "outcomes", _outcomes(self.outcomes, "branch outcomes")
        )


@dataclass(frozen=True, slots=True)
class JudgmentEvaluation:
    """One condition evaluation. Transient: only its aggregate survives.

    ``data_gap`` has no default. "The data was there and every condition
    passed" and "there was no data to evaluate" produce the same empty failure
    set, so the caller has to say which happened; inferring it would be a
    hidden default.
    """

    evaluation_id: str
    run_snapshot_id: str
    evaluated_at: datetime
    mode: StrategyMode
    trade_occurred: bool
    data_gap: bool
    basic_outcomes: tuple[ConditionOutcome, ...] = ()
    pro_branches: tuple[BranchEvaluation, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evaluation_id", _uuid(self.evaluation_id, "evaluation_id")
        )
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "evaluation.run_snapshot_id"),
        )
        object.__setattr__(
            self, "evaluated_at", _utc(self.evaluated_at, "evaluated_at")
        )
        if not isinstance(self.mode, StrategyMode):
            raise MonthlyJudgmentValidationError("mode is unsupported")
        if not isinstance(self.trade_occurred, bool):
            raise MonthlyJudgmentValidationError("trade_occurred must be a bool")
        if not isinstance(self.data_gap, bool):
            raise MonthlyJudgmentValidationError("data_gap must be a bool")

        basic = _outcomes(self.basic_outcomes, "basic_outcomes")
        branches = tuple(self.pro_branches)
        if any(not isinstance(item, BranchEvaluation) for item in branches):
            raise MonthlyJudgmentValidationError(
                "pro_branches must contain BranchEvaluation values"
            )
        branch_ids = [item.branch_id for item in branches]
        if len(set(branch_ids)) != len(branch_ids):
            raise MonthlyJudgmentValidationError(
                "pro branch_id values must be unique"
            )
        if self.mode is StrategyMode.BASIC and branches:
            raise MonthlyJudgmentValidationError(
                "Basic evaluations must not contain Pro branches"
            )
        if self.mode is StrategyMode.PRO and basic:
            raise MonthlyJudgmentValidationError(
                "Pro evaluations must not contain Basic outcomes"
            )
        if self.data_gap and (basic or branches):
            raise MonthlyJudgmentValidationError(
                "a data gap evaluation has no condition outcomes to report"
            )
        if self.data_gap and self.trade_occurred:
            raise MonthlyJudgmentValidationError(
                "a data gap evaluation cannot have produced a trade"
            )
        object.__setattr__(self, "basic_outcomes", basic)
        object.__setattr__(self, "pro_branches", branches)

    @property
    def active_branch_count(self) -> int:
        """Pro branches actually evaluated. Always 0 for a Basic evaluation."""
        return sum(1 for branch in self.pro_branches if branch.active)


@dataclass(frozen=True, slots=True)
class FirstFailureCount:
    mode: StrategyMode
    scope_id: str
    condition_id: str
    count: int

    def __post_init__(self) -> None:
        if not isinstance(self.mode, StrategyMode):
            raise MonthlyJudgmentValidationError("failure mode is unsupported")
        _text(self.scope_id, "failure scope_id")
        _text(self.condition_id, "failure condition_id")
        if not isinstance(self.count, int) or isinstance(self.count, bool) or self.count < 1:
            raise MonthlyJudgmentValidationError(
                "failure count must be a positive integer"
            )


@dataclass(frozen=True, slots=True)
class MonthlyJudgmentSummary:
    """One ``backtest.monthly_judgment_summaries`` row in domain form."""

    summary_id: str
    run_snapshot_id: str
    result_manifest_id: str
    et_month: EtMonth
    evaluation_count: int
    active_branch_count: int
    trade_event_count: int
    data_gap_count: int
    triggered_count: int
    rejected_count: int
    failure_counts: tuple[FirstFailureCount, ...]
    trade_record_ids: tuple[str, ...]
    summary_document: Mapping[str, Any]
    summary_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "summary_id", _uuid(self.summary_id, "summary_id")
        )
        object.__setattr__(
            self,
            "run_snapshot_id",
            _hash(self.run_snapshot_id, "summary.run_snapshot_id"),
        )
        object.__setattr__(
            self,
            "result_manifest_id",
            _uuid(self.result_manifest_id, "summary.result_manifest_id"),
        )
        if not isinstance(self.et_month, EtMonth):
            raise MonthlyJudgmentValidationError(
                "summary.et_month must be an EtMonth"
            )
        counts = tuple(self.failure_counts)
        if any(not isinstance(item, FirstFailureCount) for item in counts):
            raise MonthlyJudgmentValidationError(
                "failure_counts must contain FirstFailureCount values"
            )
        count_keys = [
            (item.mode, item.scope_id, item.condition_id) for item in counts
        ]
        if len(set(count_keys)) != len(count_keys):
            raise MonthlyJudgmentValidationError(
                "failure count keys must be unique"
            )
        trade_ids = tuple(
            _uuid(item, "trade_record_id") for item in self.trade_record_ids
        )
        if len(set(trade_ids)) != len(trade_ids):
            raise MonthlyJudgmentValidationError(
                "trade_record_id values must be unique"
            )
        object.__setattr__(self, "failure_counts", counts)
        object.__setattr__(self, "trade_record_ids", trade_ids)

        for name in CANONICAL_COUNTERS:
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise MonthlyJudgmentValidationError(
                    f"summary.{name} must be a non-negative integer"
                )
        if self.trade_event_count != len(trade_ids):
            raise MonthlyJudgmentValidationError(
                "trade_event_count must equal the number of linked trade records"
            )
        if self.rejected_count > self.trade_event_count:
            raise MonthlyJudgmentValidationError(
                "rejected_count cannot exceed trade_event_count"
            )
        for name in ("data_gap_count", "triggered_count"):
            if getattr(self, name) > self.evaluation_count:
                raise MonthlyJudgmentValidationError(
                    f"{name} cannot exceed evaluation_count"
                )
        if self.data_gap_count + self.triggered_count > self.evaluation_count:
            raise MonthlyJudgmentValidationError(
                "a data gap evaluation can never also be a triggered evaluation"
            )
        if not isinstance(self.summary_document, Mapping):
            raise MonthlyJudgmentValidationError(
                "summary_document must be a mapping"
            )
        object.__setattr__(
            self, "summary_document", MappingProxyType(dict(self.summary_document))
        )
        _hash(self.summary_hash, "summary_hash")

    def counters(self) -> dict[str, int]:
        """The six canonical counters, in canonical order."""
        return {name: getattr(self, name) for name in CANONICAL_COUNTERS}

    def as_record(self) -> dict[str, object]:
        """Return the aggregate relational record, never raw evaluations."""
        return {
            "summary_id": self.summary_id,
            "run_snapshot_id": self.run_snapshot_id,
            "result_manifest_id": self.result_manifest_id,
            "et_month": self.et_month.key,
            "timezone_id": ET_TIMEZONE_ID,
            **self.counters(),
            "failure_counts": [
                {
                    "mode": item.mode.value,
                    "scope_id": item.scope_id,
                    "condition_id": item.condition_id,
                    "count": item.count,
                }
                for item in self.failure_counts
            ],
            "trade_record_ids": list(self.trade_record_ids),
            "summary_hash": self.summary_hash,
        }


class MonthlyJudgmentBuilder:
    """Build deterministic ET-month aggregates from transient evaluations."""

    def build(
        self,
        run_snapshot_id: str,
        result_manifest_id: str,
        evaluations: Iterable[JudgmentEvaluation],
        trade_records: Iterable[ResultRecord],
    ) -> tuple[MonthlyJudgmentSummary, ...]:
        run_snapshot_id = _hash(run_snapshot_id, "run_snapshot_id")
        result_manifest_id = _uuid(result_manifest_id, "result_manifest_id")
        supplied_evaluations = tuple(evaluations)
        supplied_records = tuple(trade_records)

        if any(
            not isinstance(item, JudgmentEvaluation)
            for item in supplied_evaluations
        ):
            raise MonthlyJudgmentValidationError(
                "evaluations must contain JudgmentEvaluation values"
            )
        evaluation_ids = [item.evaluation_id for item in supplied_evaluations]
        if len(set(evaluation_ids)) != len(evaluation_ids):
            raise MonthlyJudgmentValidationError(
                "evaluation_id values must be unique"
            )
        if any(
            item.run_snapshot_id != run_snapshot_id
            for item in supplied_evaluations
        ):
            raise MonthlyJudgmentValidationError(
                "every evaluation must reference the run snapshot"
            )

        if any(not isinstance(item, ResultRecord) for item in supplied_records):
            raise MonthlyJudgmentValidationError(
                "trade_records must contain ResultRecord values"
            )
        record_ids = [item.record_id for item in supplied_records]
        if len(set(record_ids)) != len(record_ids):
            raise MonthlyJudgmentValidationError("record_id values must be unique")
        if any(item.run_snapshot_id != run_snapshot_id for item in supplied_records):
            raise MonthlyJudgmentValidationError(
                "every trade record must reference the run snapshot"
            )

        failures: dict[EtMonth, Counter[tuple[StrategyMode, str, str]]] = {}
        records: dict[EtMonth, list[tuple[datetime, str]]] = {}
        counters: dict[EtMonth, Counter[str]] = {}

        for evaluation in supplied_evaluations:
            month = EtMonth.from_instant(evaluation.evaluated_at)
            tally = counters.setdefault(month, Counter())
            tally["evaluation_count"] += 1
            tally["active_branch_count"] += evaluation.active_branch_count
            if evaluation.data_gap:
                tally["data_gap_count"] += 1
            if evaluation.trade_occurred:
                tally["triggered_count"] += 1
                # A triggered evaluation reached its trade; its condition
                # outcomes are not a "why it did not trade" explanation.
                continue
            for key in _first_failures(evaluation):
                failures.setdefault(month, Counter())[key] += 1

        for record in supplied_records:
            month = EtMonth.from_instant(record.occurred_at)
            records.setdefault(month, []).append(
                (record.occurred_at, record.record_id)
            )
            tally = counters.setdefault(month, Counter())
            tally["trade_event_count"] += 1
            if record.kind is ResultRecordKind.REJECTION:
                tally["rejected_count"] += 1

        summaries = []
        for month in sorted(set(counters) | set(records)):
            counts = tuple(
                FirstFailureCount(mode, scope_id, condition_id, count)
                for (mode, scope_id, condition_id), count in sorted(
                    failures.get(month, Counter()).items(),
                    key=lambda item: (
                        item[0][0].value,
                        item[0][1],
                        item[0][2],
                    ),
                )
            )
            trade_ids = tuple(
                record_id
                for _, record_id in sorted(records.get(month, []))
            )
            tally = counters.get(month, Counter())
            document = _summary_document(
                run_snapshot_id,
                result_manifest_id,
                month,
                tally,
                counts,
                trade_ids,
            )
            summary_hash = _document_hash(document)
            summaries.append(
                MonthlyJudgmentSummary(
                    summary_id=_summary_id(summary_hash),
                    run_snapshot_id=run_snapshot_id,
                    result_manifest_id=result_manifest_id,
                    et_month=month,
                    evaluation_count=tally["evaluation_count"],
                    active_branch_count=tally["active_branch_count"],
                    trade_event_count=tally["trade_event_count"],
                    data_gap_count=tally["data_gap_count"],
                    triggered_count=tally["triggered_count"],
                    rejected_count=tally["rejected_count"],
                    failure_counts=counts,
                    trade_record_ids=trade_ids,
                    summary_document=document,
                    summary_hash=summary_hash,
                )
            )
        return tuple(summaries)


def summary_from_document(
    document: Mapping[str, Any], summary_hash: str
) -> MonthlyJudgmentSummary:
    """Recover one summary from the `summary_document` jsonb and its `summary_hash`.

    `summary_document` is the whole aggregate — the six counters, the first-failure
    histogram and the month's trade record identities — and `summary_hash` is its
    content address. So a `backtest.monthly_judgment_summaries` row is sufficient on
    its own, and the durable read model does not need a second copy of the month.

    The hash is re-derived from the document before anything is returned: a row whose
    jsonb was edited in place no longer addresses its own content, and serving it would
    let a `trade_event_count` that never happened reach the API. That is
    `MonthlyJudgmentIntegrityError`, not a validation error, because the caller sent
    nothing wrong — the stored evidence is.
    """

    _hash(summary_hash, "summary_hash")
    if not isinstance(document, Mapping):
        raise MonthlyJudgmentValidationError("summary_document must be a mapping")
    recomputed = _document_hash(document)
    if recomputed != summary_hash:
        raise MonthlyJudgmentIntegrityError(
            f"monthly summary_document does not hash to its summary_hash "
            f"({recomputed} != {summary_hash})"
        )
    if document.get("schema_version") != SUMMARY_SCHEMA_VERSION:
        raise MonthlyJudgmentIntegrityError(
            f"monthly summary_document schema_version must be {SUMMARY_SCHEMA_VERSION}, "
            f"got {document.get('schema_version')!r}"
        )
    if document.get("timezone_id") != ET_TIMEZONE_ID:
        raise MonthlyJudgmentIntegrityError(
            f"monthly summary_document timezone_id must be {ET_TIMEZONE_ID}, "
            f"got {document.get('timezone_id')!r}"
        )

    try:
        year, _, month = str(document["et_year_month"]).partition("-")
        et_month = EtMonth(int(year), int(month))
        counters = {name: int(document[name]) for name in CANONICAL_COUNTERS}
        failure_counts = tuple(
            FirstFailureCount(
                mode=StrategyMode(item["mode"]),
                scope_id=item["flow_or_branch_key"],
                condition_id=item["first_failure_condition_key"],
                count=int(item["occurrence_count"]),
            )
            for item in document["failure_counts"]
        )
        return MonthlyJudgmentSummary(
            summary_id=_summary_id(summary_hash),
            run_snapshot_id=document["run_snapshot_id"],
            result_manifest_id=document["result_manifest_id"],
            et_month=et_month,
            failure_counts=failure_counts,
            trade_record_ids=tuple(document["trade_record_ids"]),
            summary_document=document,
            summary_hash=summary_hash,
            **counters,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise MonthlyJudgmentIntegrityError(
            f"monthly summary_document is not a readable summary: {exc}"
        ) from exc


def _first_failure(
    outcomes: tuple[ConditionOutcome, ...]
) -> ConditionOutcome | None:
    return next((item for item in outcomes if not item.passed), None)


def _first_failures(
    evaluation: JudgmentEvaluation,
) -> tuple[tuple[StrategyMode, str, str], ...]:
    if evaluation.mode is StrategyMode.BASIC:
        failed = _first_failure(evaluation.basic_outcomes)
        return (
            ((StrategyMode.BASIC, "BASIC", failed.condition_id),)
            if failed is not None
            else ()
        )

    result = []
    for branch in evaluation.pro_branches:
        if not branch.active:
            continue
        failed = _first_failure(branch.outcomes)
        if failed is not None:
            result.append((StrategyMode.PRO, branch.branch_id, failed.condition_id))
    return tuple(result)


def _summary_document(
    run_snapshot_id: str,
    result_manifest_id: str,
    month: EtMonth,
    tally: Counter[str],
    counts: tuple[FirstFailureCount, ...],
    trade_ids: tuple[str, ...],
) -> dict[str, Any]:
    """The ``monthly_judgment_summaries.summary_document`` jsonb payload.

    Field names inside ``failure_counts`` are the
    ``backtest.failure_condition_counts`` column names, so the document and the
    child rows cannot drift apart.
    """

    document: dict[str, Any] = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "run_snapshot_id": run_snapshot_id,
        "result_manifest_id": result_manifest_id,
        "et_year_month": month.key,
        "timezone_id": ET_TIMEZONE_ID,
    }
    document.update({name: tally[name] for name in CANONICAL_COUNTERS})
    document["failure_counts"] = [
        {
            "mode": item.mode.value,
            "flow_or_branch_key": item.scope_id,
            "first_failure_condition_key": item.condition_id,
            "occurrence_count": item.count,
        }
        for item in counts
    ]
    document["trade_record_ids"] = list(trade_ids)
    return document


def _document_hash(document: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        dict(document), sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _summary_id(summary_hash: str) -> str:
    """Content-addressed row id: the same month content always gets the same id."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"idea2strategy:d26:{summary_hash}"))
