"""ET-month judgment summaries without raw non-trade evaluation storage."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from zoneinfo import ZoneInfo

from backtest_engine.result_snapshot import ResultRecord


ET_TIMEZONE_ID = "America/New_York"
ET = ZoneInfo(ET_TIMEZONE_ID)
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class MonthlyJudgmentValidationError(ValueError):
    """Raised when monthly judgment evidence is ambiguous or inconsistent."""


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
    evaluation_id: str
    run_snapshot_id: str
    evaluated_at: datetime
    mode: StrategyMode
    trade_occurred: bool
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
        object.__setattr__(self, "basic_outcomes", basic)
        object.__setattr__(self, "pro_branches", branches)


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
    summary_id: str
    run_snapshot_id: str
    result_manifest_id: str
    et_month: EtMonth
    failure_counts: tuple[FirstFailureCount, ...]
    trade_record_ids: tuple[str, ...]

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

    def as_record(self) -> dict[str, object]:
        """Return the aggregate relational record, never raw evaluations."""
        return {
            "summary_id": self.summary_id,
            "run_snapshot_id": self.run_snapshot_id,
            "result_manifest_id": self.result_manifest_id,
            "et_month": self.et_month.key,
            "timezone_id": ET_TIMEZONE_ID,
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

        for evaluation in supplied_evaluations:
            if evaluation.trade_occurred:
                continue
            month = EtMonth.from_instant(evaluation.evaluated_at)
            for key in _first_failures(evaluation):
                failures.setdefault(month, Counter())[key] += 1

        for record in supplied_records:
            month = EtMonth.from_instant(record.occurred_at)
            records.setdefault(month, []).append(
                (record.occurred_at, record.record_id)
            )

        summaries = []
        for month in sorted(set(failures) | set(records)):
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
            summary_id = _summary_id(
                run_snapshot_id,
                result_manifest_id,
                month,
                counts,
                trade_ids,
            )
            summaries.append(
                MonthlyJudgmentSummary(
                    summary_id=summary_id,
                    run_snapshot_id=run_snapshot_id,
                    result_manifest_id=result_manifest_id,
                    et_month=month,
                    failure_counts=counts,
                    trade_record_ids=trade_ids,
                )
            )
        return tuple(summaries)


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


def _summary_id(
    run_snapshot_id: str,
    result_manifest_id: str,
    month: EtMonth,
    counts: tuple[FirstFailureCount, ...],
    trade_ids: tuple[str, ...],
) -> str:
    payload = {
        "run_snapshot_id": run_snapshot_id,
        "result_manifest_id": result_manifest_id,
        "et_month": month.key,
        "failure_counts": [
            [item.mode.value, item.scope_id, item.condition_id, item.count]
            for item in counts
        ],
        "trade_record_ids": list(trade_ids),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"idea2strategy:d26:{digest}"))
