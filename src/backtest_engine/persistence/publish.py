"""The atomic multi-table publish for a finished backtest run.

Publishing a run touches five tables: `performance_summaries`,
`monthly_judgment_summaries`, `failure_condition_counts`, `detail_manifests` and
`runs`. The sibling implementation performs these as N+3 independent writes with no
transaction, so a crash halfway through leaves a run that is neither RUNNING nor
consistently COMPLETED — for example two AVAILABLE manifests for the same week part.

`publish_completed_run` runs inside the caller's unit of work. If any step raises,
the whole transaction rolls back and the database is exactly as it was.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from .errors import PublishConflict
from .repositories import BacktestUnitOfWork
from .rows import (
    DetailManifestRow,
    FailureConditionCountRow,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunRow,
    WorkStatus,
)


__all__ = [
    "MonthlyJudgment",
    "RunPublication",
    "publish_completed_run",
    "publish_failed_run",
    "sum_monthly_counters",
]


@dataclass(frozen=True, slots=True)
class MonthlyJudgment:
    """One ET month summary plus its failure-condition breakdown."""

    summary: MonthlyJudgmentSummaryRow
    failure_counts: tuple[FailureConditionCountRow, ...] = ()

    def __post_init__(self) -> None:
        wrong_parent = [str(count.id) for count in self.failure_counts if count.monthly_summary_id != self.summary.id]
        if wrong_parent:
            raise PublishConflict(f"failure condition counts {wrong_parent} do not belong to summary {self.summary.id}")


@dataclass(frozen=True, slots=True)
class RunPublication:
    """Everything one completed run writes, as a single reviewable value."""

    run_id: UUID
    completed_at: datetime
    result_hash: str
    performance: PerformanceSummaryRow
    monthly: tuple[MonthlyJudgment, ...] = ()
    detail_manifests: tuple[DetailManifestRow, ...] = ()
    worker_execution_key: str | None = None
    attempt_id: UUID | None = None
    claim_token: UUID | None = None
    require_objects_available: bool = True

    def __post_init__(self) -> None:
        if self.performance.run_id != self.run_id:
            raise PublishConflict(f"performance summary belongs to run {self.performance.run_id}, not {self.run_id}")
        for judgment in self.monthly:
            if judgment.summary.run_id != self.run_id:
                raise PublishConflict(
                    f"monthly summary {judgment.summary.id} belongs to run {judgment.summary.run_id}, not {self.run_id}"
                )
        for manifest in self.detail_manifests:
            if manifest.run_id != self.run_id:
                raise PublishConflict(
                    f"detail manifest {manifest.id} belongs to run {manifest.run_id}, not {self.run_id}"
                )
        if self.performance.result_hash != self.result_hash:
            raise PublishConflict(
                "run result_hash and performance summary result_hash disagree: "
                f"{self.result_hash} != {self.performance.result_hash}"
            )
        if (self.attempt_id is None) != (self.claim_token is None):
            raise PublishConflict("attempt_id and claim_token must be supplied together")


def publish_completed_run(uow: BacktestUnitOfWork, publication: RunPublication) -> RunRow:
    """Write every result row and flip the run to COMPLETED, atomically.

    Must be called inside `BacktestPersistence.unit_of_work()`. Ordering is chosen so
    that a foreign-key or uniqueness violation surfaces before the run status changes,
    but correctness does not depend on the ordering: the transaction is the guarantee.
    """

    if publication.require_objects_available and publication.detail_manifests:
        uow.objects.require_available([manifest.object_id for manifest in publication.detail_manifests])

    uow.performance.insert(publication.performance)

    for judgment in publication.monthly:
        uow.monthly.insert_summary(judgment.summary)
        uow.monthly.insert_failure_counts(judgment.failure_counts)

    for manifest in publication.detail_manifests:
        uow.manifests.insert(manifest)

    if publication.attempt_id is not None and publication.claim_token is not None:
        uow.attempts.close_fenced(
            publication.attempt_id,
            publication.claim_token,
            status=WorkStatus.SUCCEEDED,
            terminal_reason_code=WorkStatus.SUCCEEDED.value,
        )
    elif publication.worker_execution_key is not None:
        uow.attempts.complete(
            publication.worker_execution_key,
            status=WorkStatus.SUCCEEDED,
            completed_at=publication.completed_at,
        )

    return uow.runs.mark_completed(publication.run_id, publication.completed_at, publication.result_hash)


def publish_failed_run(
    uow: BacktestUnitOfWork,
    run_id: UUID,
    *,
    completed_at: datetime,
    failure_code: str,
    worker_execution_key: str | None = None,
    unavailable: bool = False,
    attempt_status: WorkStatus = WorkStatus.FAILED,
) -> RunRow:
    """Record a terminal failure for a run and its attempt in one transaction.

    `unavailable=True` records `UNAVAILABLE` — the canonical status for "the inputs
    this run needs are not available", which is not the same thing as a failed run.
    """

    if worker_execution_key is not None:
        uow.attempts.complete(
            worker_execution_key,
            status=attempt_status,
            completed_at=completed_at,
            failure_code=failure_code,
        )
    if unavailable:
        return uow.runs.mark_unavailable(run_id, completed_at, failure_code)
    return uow.runs.mark_failed(run_id, completed_at, failure_code)


def sum_monthly_counters(monthly: Sequence[MonthlyJudgment]) -> dict[str, int]:
    """Totals across every published ET month; used by callers building a summary."""

    totals = {
        "evaluation_count": 0,
        "active_branch_count": 0,
        "trade_event_count": 0,
        "data_gap_count": 0,
        "triggered_count": 0,
        "rejected_count": 0,
    }
    for judgment in monthly:
        for name in totals:
            totals[name] += getattr(judgment.summary, name)
    return totals
