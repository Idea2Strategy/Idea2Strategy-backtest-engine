"""The canonical unique constraints used as real concurrency controls.

Each test drives two *separate connections* where the failure mode is cross-process, so
an in-process lock could not make them pass.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import text

from backtest_engine.persistence import (
    AttemptNumberConflict,
    BacktestPersistence,
    DuplicateWorkerExecution,
    IdempotencyConflict,
    InvalidStatusTransition,
    PublishConflict,
    RunStatus,
    WorkStatus,
)

from .support import (
    make_attempt,
    make_detail_manifest,
    make_input_bundle,
    make_monthly_summary,
    make_performance_summary,
    make_run,
)


pytestmark = pytest.mark.docker


def _count(persistence: BacktestPersistence, table: str) -> int:
    with persistence.unit_of_work() as uow:
        return int(uow.connection.execute(text(f"SELECT count(*) FROM {table}")).scalar_one())


def test_duplicate_idempotency_key_returns_the_existing_run(
    persistence: BacktestPersistence,
) -> None:
    first = make_run(idempotency_key="BOT_CREATE:same")
    # A redelivery generates a fresh run id but the same material request.
    redelivery = make_run(run_id=uuid4(), idempotency_key="BOT_CREATE:same")

    with persistence.unit_of_work() as uow:
        stored, created = uow.runs.accept(first)
    assert created is True

    with persistence.unit_of_work() as uow:
        again, created_again = uow.runs.accept(redelivery)

    assert created_again is False
    assert again == stored
    assert again.id == first.id
    assert _count(persistence, "backtest.runs") == 1


def test_same_idempotency_key_with_a_different_request_is_a_conflict(
    persistence: BacktestPersistence,
) -> None:
    first = make_run(idempotency_key="BOT_CREATE:conflict")
    different = make_run(
        run_id=uuid4(),
        idempotency_key="BOT_CREATE:conflict",
        initial_cash_amount=Decimal("50000.00000000"),
    )

    with persistence.unit_of_work() as uow:
        uow.runs.accept(first)

    with persistence.unit_of_work() as uow, pytest.raises(IdempotencyConflict, match="differing"):
        uow.runs.accept(different)

    assert _count(persistence, "backtest.runs") == 1


def test_reusing_a_run_id_under_a_new_key_is_a_conflict(
    persistence: BacktestPersistence,
) -> None:
    first = make_run(idempotency_key="BOT_CREATE:id-reuse-1")
    second = make_run(run_id=first.id, idempotency_key="BOT_CREATE:id-reuse-2")

    with persistence.unit_of_work() as uow:
        uow.runs.accept(first)

    with persistence.unit_of_work() as uow, pytest.raises(IdempotencyConflict, match="run id"):
        uow.runs.accept(second)

    assert _count(persistence, "backtest.runs") == 1


def test_worker_execution_key_rejects_a_second_worker(
    persistence: BacktestPersistence,
) -> None:
    """The cross-process duplicate-worker control.

    Two workers on two connections derive the same execution key. The database, not a
    process-local lock, decides that only one of them owns the attempt.
    """

    run = make_run(idempotency_key="BOT_CREATE:worker")
    key = "BACKTEST_RUN_worker_ATTEMPT_1"
    worker_a = make_attempt(run.id, worker_execution_key=key)
    worker_b = make_attempt(run.id, worker_execution_key=key, attempt_id=uuid4())

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        claimed = uow.attempts.claim_exclusive(worker_a)

    with persistence.unit_of_work() as uow, pytest.raises(DuplicateWorkerExecution):
        uow.attempts.claim_exclusive(worker_b)

    assert _count(persistence, "backtest.run_attempts") == 1
    with persistence.unit_of_work() as uow:
        assert uow.attempts.list_for_run(run.id) == (claimed,)


def test_redelivery_of_the_same_execution_key_is_not_an_error(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="BOT_CREATE:redeliver")
    key = "BACKTEST_RUN_redeliver_ATTEMPT_1"
    attempt = make_attempt(run.id, worker_execution_key=key)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        first, created = uow.attempts.claim(attempt)
    assert created is True

    with persistence.unit_of_work() as uow:
        again, created_again = uow.attempts.claim(make_attempt(run.id, worker_execution_key=key, attempt_id=uuid4()))

    assert created_again is False
    assert again == first
    assert _count(persistence, "backtest.run_attempts") == 1


def test_execution_key_bound_to_another_run_is_rejected(
    persistence: BacktestPersistence,
) -> None:
    run_a = make_run(idempotency_key="BOT_CREATE:key-a")
    run_b = make_run(idempotency_key="BOT_CREATE:key-b")
    key = "SHARED_EXECUTION_KEY"

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run_a)
        uow.runs.accept(run_b)
        uow.attempts.claim(make_attempt(run_a.id, worker_execution_key=key))

    with persistence.unit_of_work() as uow, pytest.raises(DuplicateWorkerExecution, match="belongs to run"):
        uow.attempts.claim(make_attempt(run_b.id, worker_execution_key=key))


def test_attempt_number_cannot_fork(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="BOT_CREATE:fork")

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.attempts.claim(make_attempt(run.id, attempt_number=1, worker_execution_key="KEY-1"))

    with persistence.unit_of_work() as uow, pytest.raises(AttemptNumberConflict):
        uow.attempts.claim(make_attempt(run.id, attempt_number=1, worker_execution_key="KEY-2"))

    assert _count(persistence, "backtest.run_attempts") == 1


def test_input_bundle_is_unique_per_run(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="BOT_CREATE:bundle")
    bundle = make_input_bundle(run.id, bundle_hash="a" * 64)
    other = make_input_bundle(run.id, bundle_hash="b" * 64)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.inputs.lock(bundle)

    with persistence.unit_of_work() as uow, pytest.raises(PublishConflict, match="different input bundle"):
        uow.inputs.lock(other)

    assert _count(persistence, "backtest.input_bundles") == 1


def test_monthly_summary_is_unique_per_run_and_month(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="BOT_CREATE:month")
    summary = make_monthly_summary(run.id, summary_hash="a" * 64)
    same = make_monthly_summary(run.id, summary_hash="a" * 64)
    different = make_monthly_summary(run.id, summary_hash="b" * 64)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.monthly.insert_summary(summary)

    with persistence.unit_of_work() as uow:
        stored, created = uow.monthly.insert_summary(same)
    assert created is False
    assert stored.id == summary.id

    with persistence.unit_of_work() as uow, pytest.raises(PublishConflict, match="different summary"):
        uow.monthly.insert_summary(different)

    assert _count(persistence, "backtest.monthly_judgment_summaries") == 1


def test_detail_manifest_is_unique_per_week_part_and_per_object(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="BOT_CREATE:manifest")
    manifest = make_detail_manifest(run.id, detail_hash="a" * 64)
    same_part = make_detail_manifest(run.id, manifest_id=uuid4(), detail_hash="b" * 64)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.manifests.insert(manifest)

    with persistence.unit_of_work() as uow, pytest.raises(PublishConflict, match="different manifest"):
        uow.manifests.insert(same_part)

    # Same object, different week part: still rejected, by the object_id unique index.
    other_part = make_detail_manifest(run.id, manifest_id=uuid4(), part_number=2)
    with persistence.unit_of_work() as uow, pytest.raises(PublishConflict, match="already has manifest"):
        uow.manifests.insert(other_part)

    assert _count(persistence, "backtest.detail_manifests") == 1


def test_performance_summary_is_unique_per_run(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="BOT_CREATE:perf")

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.performance.insert(make_performance_summary(run.id, result_hash="a" * 64))

    with persistence.unit_of_work() as uow:
        stored, created = uow.performance.insert(make_performance_summary(run.id, result_hash="a" * 64))
    assert created is False
    assert stored.result_hash == "a" * 64

    with persistence.unit_of_work() as uow, pytest.raises(PublishConflict, match="different performance"):
        uow.performance.insert(make_performance_summary(run.id, result_hash="b" * 64))


def test_terminal_status_is_not_reversible(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="BOT_CREATE:terminal")
    finished = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.runs.mark_running(run.id, datetime(2026, 3, 2, 14, 0, 1, tzinfo=UTC))
        uow.runs.mark_completed(run.id, finished, "c" * 64)

    with persistence.unit_of_work() as uow, pytest.raises(InvalidStatusTransition):
        uow.runs.mark_running(run.id, datetime(2026, 3, 2, 15, 0, tzinfo=UTC))

    with persistence.unit_of_work() as uow:
        assert uow.runs.get(run.id).status is RunStatus.COMPLETED


def test_repeating_the_same_terminal_result_is_idempotent(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="BOT_CREATE:terminal-again")
    finished = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.runs.mark_running(run.id, datetime(2026, 3, 2, 14, 0, 1, tzinfo=UTC))
        uow.runs.mark_completed(run.id, finished, "c" * 64)

    with persistence.unit_of_work() as uow:
        again = uow.runs.mark_completed(run.id, finished, "c" * 64)

    assert again.status is RunStatus.COMPLETED
    assert again.result_hash == "c" * 64


def test_completing_an_attempt_twice_with_the_same_status_is_idempotent(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="BOT_CREATE:attempt-twice")
    attempt = make_attempt(run.id)
    finished = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.attempts.claim(attempt)
        uow.attempts.complete(attempt.worker_execution_key, status=WorkStatus.SUCCEEDED, completed_at=finished)

    with persistence.unit_of_work() as uow:
        again = uow.attempts.complete(attempt.worker_execution_key, status=WorkStatus.SUCCEEDED, completed_at=finished)
    assert again.status is WorkStatus.SUCCEEDED

    with persistence.unit_of_work() as uow, pytest.raises(InvalidStatusTransition):
        uow.attempts.complete(attempt.worker_execution_key, status=WorkStatus.FAILED, completed_at=finished)
