"""A multi-table publish is atomic: all of it lands, or none of it does."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from backtest_engine.persistence import (
    BacktestPersistence,
    MonthlyJudgment,
    PublishConflict,
    RowNotFound,
    RunPublication,
    RunRow,
    RunStatus,
    WorkStatus,
    publish_completed_run,
    publish_failed_run,
)

from .support import (
    STAGED_OBJECT_ID,
    make_attempt,
    make_detail_manifest,
    make_failure_count,
    make_monthly_summary,
    make_performance_summary,
    make_run,
)


pytestmark = pytest.mark.docker

RESULT_HASH = "c" * 64
STARTED_AT = datetime(2026, 3, 2, 14, 0, 1, tzinfo=UTC)
FINISHED_AT = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)


def _counts(persistence: BacktestPersistence) -> dict[str, int]:
    tables = (
        "backtest.performance_summaries",
        "backtest.monthly_judgment_summaries",
        "backtest.failure_condition_counts",
        "backtest.detail_manifests",
    )
    with persistence.unit_of_work() as uow:
        return {
            table: int(uow.connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()) for table in tables
        }


def _running_run(persistence: BacktestPersistence, key: str) -> tuple[RunRow, str]:
    run = make_run(idempotency_key=key)
    attempt = make_attempt(run.id)
    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.attempts.claim(attempt)
        uow.runs.mark_running(run.id, STARTED_AT)
    return run, attempt.worker_execution_key


def _publication(run_id: UUID, worker_key: str) -> RunPublication:
    summary = make_monthly_summary(run_id)
    return RunPublication(
        run_id=run_id,
        completed_at=FINISHED_AT,
        result_hash=RESULT_HASH,
        performance=make_performance_summary(run_id, result_hash=RESULT_HASH),
        monthly=(MonthlyJudgment(summary=summary, failure_counts=(make_failure_count(summary.id),)),),
        detail_manifests=(make_detail_manifest(run_id),),
        worker_execution_key=worker_key,
    )


def test_publish_writes_every_table_and_completes_the_run(
    persistence: BacktestPersistence,
) -> None:
    run, worker_key = _running_run(persistence, "PUBLISH:ok")
    publication = _publication(run.id, worker_key)

    with persistence.unit_of_work() as uow:
        completed = publish_completed_run(uow, publication)

    assert completed.status is RunStatus.COMPLETED
    assert completed.result_hash == RESULT_HASH
    assert _counts(persistence) == {
        "backtest.performance_summaries": 1,
        "backtest.monthly_judgment_summaries": 1,
        "backtest.failure_condition_counts": 1,
        "backtest.detail_manifests": 1,
    }

    with persistence.unit_of_work() as uow:
        attempt = uow.attempts.find_by_execution_key(worker_key)
    assert attempt is not None
    assert attempt.status is WorkStatus.SUCCEEDED
    assert attempt.completed_at == FINISHED_AT


def test_publish_rolls_back_completely_when_a_later_write_fails(
    persistence: BacktestPersistence,
) -> None:
    """A mid-transaction failure must not leave a half-published run.

    Two manifests collide on `(run_id, record_type, week_start_date, part_number)`. The
    performance summary and the monthly summary were already written when the collision
    is detected; the transaction must undo them and leave the run RUNNING.
    """

    run, worker_key = _running_run(persistence, "PUBLISH:rollback")
    summary = make_monthly_summary(run.id)
    first = make_detail_manifest(run.id)
    colliding = make_detail_manifest(
        run.id,
        manifest_id=uuid4(),
        object_id=STAGED_OBJECT_ID,
        detail_hash="d" * 64,
    )
    publication = RunPublication(
        run_id=run.id,
        completed_at=FINISHED_AT,
        result_hash=RESULT_HASH,
        performance=make_performance_summary(run.id, result_hash=RESULT_HASH),
        monthly=(MonthlyJudgment(summary=summary, failure_counts=(make_failure_count(summary.id),)),),
        detail_manifests=(first, colliding),
        worker_execution_key=worker_key,
        require_objects_available=False,
    )

    with pytest.raises(PublishConflict), persistence.unit_of_work() as uow:
        publish_completed_run(uow, publication)

    assert _counts(persistence) == {
        "backtest.performance_summaries": 0,
        "backtest.monthly_judgment_summaries": 0,
        "backtest.failure_condition_counts": 0,
        "backtest.detail_manifests": 0,
    }
    with persistence.unit_of_work() as uow:
        assert uow.runs.get(run.id).status is RunStatus.RUNNING
        attempt = uow.attempts.find_by_execution_key(worker_key)
    assert attempt is not None
    assert attempt.completed_at is None


def test_publish_rolls_back_when_an_arbitrary_error_interrupts_the_block(
    persistence: BacktestPersistence,
) -> None:
    """The transaction, not the ordering, is the guarantee. Crash after the last write."""

    run, worker_key = _running_run(persistence, "PUBLISH:crash")
    publication = _publication(run.id, worker_key)

    class Crash(RuntimeError):
        pass

    with pytest.raises(Crash), persistence.unit_of_work() as uow:
        publish_completed_run(uow, publication)
        raise Crash("worker died after writing every row")

    assert _counts(persistence) == {
        "backtest.performance_summaries": 0,
        "backtest.monthly_judgment_summaries": 0,
        "backtest.failure_condition_counts": 0,
        "backtest.detail_manifests": 0,
    }
    with persistence.unit_of_work() as uow:
        assert uow.runs.get(run.id).status is RunStatus.RUNNING


def test_publish_refuses_manifests_whose_object_is_not_available(
    persistence: BacktestPersistence,
) -> None:
    run, worker_key = _running_run(persistence, "PUBLISH:staged-object")
    summary = make_monthly_summary(run.id)
    publication = RunPublication(
        run_id=run.id,
        completed_at=FINISHED_AT,
        result_hash=RESULT_HASH,
        performance=make_performance_summary(run.id, result_hash=RESULT_HASH),
        monthly=(MonthlyJudgment(summary=summary),),
        detail_manifests=(make_detail_manifest(run.id, object_id=STAGED_OBJECT_ID),),
        worker_execution_key=worker_key,
    )

    with pytest.raises(RowNotFound, match="not AVAILABLE"), persistence.unit_of_work() as uow:
        publish_completed_run(uow, publication)

    assert _counts(persistence)["backtest.performance_summaries"] == 0


def test_failed_publish_records_the_attempt_and_the_run_together(
    persistence: BacktestPersistence,
) -> None:
    run, worker_key = _running_run(persistence, "PUBLISH:failed")

    with persistence.unit_of_work() as uow:
        failed = publish_failed_run(
            uow,
            run.id,
            completed_at=FINISHED_AT,
            failure_code="ENGINE_ERROR",
            worker_execution_key=worker_key,
        )

    assert failed.status is RunStatus.FAILED
    assert failed.failure_code == "ENGINE_ERROR"
    with persistence.unit_of_work() as uow:
        attempt = uow.attempts.find_by_execution_key(worker_key)
    assert attempt is not None
    assert attempt.status is WorkStatus.FAILED
    assert attempt.failure_code == "ENGINE_ERROR"


def test_unavailable_is_recorded_separately_from_failed(
    persistence: BacktestPersistence,
) -> None:
    run, worker_key = _running_run(persistence, "PUBLISH:unavailable")

    with persistence.unit_of_work() as uow:
        result = publish_failed_run(
            uow,
            run.id,
            completed_at=FINISHED_AT,
            failure_code="INPUT_DATA_UNAVAILABLE",
            worker_execution_key=worker_key,
            unavailable=True,
            attempt_status=WorkStatus.SKIPPED,
        )

    assert result.status is RunStatus.UNAVAILABLE
    with persistence.unit_of_work() as uow:
        attempt = uow.attempts.find_by_execution_key(worker_key)
    assert attempt is not None
    assert attempt.status is WorkStatus.SKIPPED
