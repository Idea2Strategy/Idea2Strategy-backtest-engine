from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier
from uuid import UUID

import pytest
from sqlalchemy import text

from backtest_engine.persistence import (
    BacktestPersistence,
    RunAttemptRow,
    RunRow,
    RunStatus,
    StaleAttemptClaim,
    WorkStatus,
)

from .support import make_run


pytestmark = pytest.mark.docker


def _accept(persistence: BacktestPersistence, key: str) -> RunRow:
    run = make_run(idempotency_key=key)
    with persistence.unit_of_work() as uow:
        stored, _ = uow.runs.accept(run)
    return stored


def _expire(persistence: BacktestPersistence, attempt_id: UUID) -> None:
    with persistence.unit_of_work() as uow:
        uow.connection.execute(
            text(
                """UPDATE backtest.run_attempts
                      SET claimed_at = clock_timestamp() - interval '2 minutes',
                          last_heartbeat_at = clock_timestamp() - interval '2 minutes',
                          claim_expires_at = clock_timestamp() - interval '1 minute'
                    WHERE id = :attempt_id"""
            ),
            {"attempt_id": attempt_id},
        )


def test_duplicate_delivery_has_exactly_one_live_claim(persistence: BacktestPersistence) -> None:
    run = _accept(persistence, "FENCE:duplicate")
    with persistence.read_only() as uow:
        assert uow.runs.get(run.id).status is RunStatus.QUEUED
        assert uow.attempts.list_for_run(run.id) == ()
    with persistence.unit_of_work() as uow:
        first = uow.attempts.claim_fenced(
            run.id, worker_id="worker-a", execution_key="message-a", lease_duration=timedelta(minutes=1)
        )
    with persistence.unit_of_work() as uow:
        second = uow.attempts.claim_fenced(
            run.id, worker_id="worker-b", execution_key="message-a", lease_duration=timedelta(minutes=1)
        )
        attempts = uow.attempts.list_for_run(run.id)

    assert first is not None
    assert second is None
    assert len(attempts) == 1


def test_concurrent_duplicate_claims_have_one_database_winner(
    persistence: BacktestPersistence,
) -> None:
    run = _accept(persistence, "FENCE:concurrent-duplicate")
    barrier = Barrier(2)

    def claim(worker: str) -> RunAttemptRow | None:
        barrier.wait()
        with persistence.unit_of_work() as uow:
            return uow.attempts.claim_fenced(
                run.id,
                worker_id=worker,
                execution_key="message-concurrent",
                lease_duration=timedelta(minutes=1),
            )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(claim, ("worker-a", "worker-b")))

    assert sum(result is not None for result in results) == 1
    with persistence.read_only() as uow:
        assert len(uow.attempts.list_for_run(run.id)) == 1


def test_expired_claim_is_replaced_and_late_completion_is_fenced(
    persistence: BacktestPersistence,
) -> None:
    run = _accept(persistence, "FENCE:reclaim")
    with persistence.unit_of_work() as uow:
        first = uow.attempts.claim_fenced(
            run.id, worker_id="worker-a", execution_key="message-b", lease_duration=timedelta(minutes=1)
        )
    assert first is not None and first.claim_token is not None
    _expire(persistence, first.id)

    with persistence.unit_of_work() as uow:
        successor = uow.attempts.claim_fenced(
            run.id, worker_id="worker-b", execution_key="message-b", lease_duration=timedelta(minutes=1)
        )
    assert successor is not None
    assert successor.attempt_number == 2
    assert successor.previous_attempt_id == first.id

    with pytest.raises(StaleAttemptClaim), persistence.unit_of_work() as uow:
        uow.attempts.close_fenced(
            first.id,
            first.claim_token,
            status=WorkStatus.SUCCEEDED,
            terminal_reason_code="SUCCEEDED",
        )


def test_heartbeat_extends_the_database_lease(persistence: BacktestPersistence) -> None:
    run = _accept(persistence, "FENCE:heartbeat")
    with persistence.unit_of_work() as uow:
        claim = uow.attempts.claim_fenced(
            run.id, worker_id="worker-a", execution_key="message-c", lease_duration=timedelta(minutes=1)
        )
    assert claim is not None and claim.claim_token is not None
    original_expiry = claim.claim_expires_at

    with persistence.unit_of_work() as uow:
        extended = uow.attempts.heartbeat_fenced(
            claim.id, claim.claim_token, lease_duration=timedelta(minutes=2)
        )

    assert original_expiry is not None
    assert extended.claim_expires_at is not None
    assert extended.claim_expires_at > original_expiry


def test_cancellation_wins_over_success_from_the_live_claim(
    persistence: BacktestPersistence,
) -> None:
    run = _accept(persistence, "FENCE:cancel")
    with persistence.unit_of_work() as uow:
        claim = uow.attempts.claim_fenced(
            run.id, worker_id="worker-a", execution_key="message-d", lease_duration=timedelta(minutes=1)
        )
    assert claim is not None and claim.claim_token is not None

    with persistence.unit_of_work() as uow:
        requested = uow.runs.request_cancellation(run.id, reason_code="USER_REQUEST")
    assert requested.status is RunStatus.RUNNING

    with persistence.unit_of_work() as uow:
        closed = uow.attempts.close_fenced(
            claim.id,
            claim.claim_token,
            status=WorkStatus.SUCCEEDED,
            terminal_reason_code="SUCCEEDED",
        )
        cancelled = uow.runs.get(run.id)

    assert closed.status is WorkStatus.CANCELLED
    assert cancelled.status is RunStatus.CANCELLED
    assert cancelled.cancelled_at is not None
