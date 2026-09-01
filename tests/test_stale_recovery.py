from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Engine, text

from backtest_engine.persistence import BacktestPersistence, RunLane, RunStatus
from backtest_engine.recovery import (
    QueueDispatchPolicy,
    QueueLanePolicy,
    StaleRunRecovery,
)
from backtest_engine.wiring import PersistenceExecutionKeyStore
from backtest_engine.worker import worker_execution_key_for
from persistence.support import make_run


pytestmark = pytest.mark.docker


POLICY = QueueDispatchPolicy(
    {
        "BASIC": QueueLanePolicy(capacity=2, dispatch_timeout=timedelta(minutes=5)),
        "CUSTOM": QueueLanePolicy(capacity=1, dispatch_timeout=timedelta(minutes=10)),
        "COMPETITION": QueueLanePolicy(capacity=1, dispatch_timeout=timedelta(minutes=3)),
    }
)


def _run(
    persistence: BacktestPersistence,
    status: RunStatus = RunStatus.QUEUED,
    *,
    lane: RunLane = RunLane.CUSTOM,
) -> uuid.UUID:
    with persistence.unit_of_work() as uow:
        row, created = uow.runs.accept(
            make_run(
                idempotency_key=f"RECOVERY:{uuid.uuid4()}",
                status=status,
                lane=lane,
            )
        )
    assert created
    return row.id


def _state(engine: Engine, run_id: uuid.UUID) -> dict[str, object]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text("SELECT status, failure_code, cancellation_reason_code FROM backtest.runs WHERE id=:id"),
                {"id": run_id},
            )
            .mappings()
            .one()
        )


def _expire(engine: Engine, run_id: uuid.UUID) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE backtest.run_attempts SET started_at=clock_timestamp()-interval '3 minutes', "
                "claimed_at=clock_timestamp()-interval '3 minutes', "
                "last_heartbeat_at=clock_timestamp()-interval '2 minutes', "
                "claim_expires_at=clock_timestamp()-interval '1 minute' WHERE run_id=:id"
            ),
            {"id": run_id},
        )


def test_exhausted_queued_run_becomes_failed_with_no_sixth_attempt(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_id = _run(persistence)
    store = PersistenceExecutionKeyStore(persistence)
    key = worker_execution_key_for(str(run_id), "RECOVERY:exhausted")
    for attempt in range(1, 6):
        claim = store.claim(
            key,
            run_id=str(run_id),
            owner=f"worker-{attempt}",
            now=datetime.now(UTC),
            lease_duration=timedelta(minutes=1),
        )
        assert claim.acquired and claim.attempt_number == attempt
        store.release(key, now=datetime.now(UTC), claim=claim, reason_code="WORKER_TIMEOUT")

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.failed == 1
    assert _state(admin_engine, run_id)["status"] == "FAILED"
    assert _state(admin_engine, run_id)["failure_code"] == "MAX_ATTEMPTS_EXHAUSTED"
    with admin_engine.connect() as connection:
        count = connection.scalar(
            text("SELECT count(*) FROM backtest.run_attempts WHERE run_id=:id"),
            {"id": run_id},
        )
        assert count == 5


def test_expired_running_lease_is_requeued_but_live_heartbeat_is_never_recovered(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    expired_id = _run(persistence)
    live_id = _run(persistence)
    store = PersistenceExecutionKeyStore(persistence)
    for run_id, suffix in ((expired_id, "expired"), (live_id, "live")):
        store.claim(
            worker_execution_key_for(str(run_id), suffix),
            run_id=str(run_id),
            owner="worker",
            now=datetime.now(UTC),
            lease_duration=timedelta(minutes=10),
        )
    _expire(admin_engine, expired_id)
    with admin_engine.begin() as connection:
        connection.execute(
            text("UPDATE backtest.runs SET started_at=clock_timestamp()-interval '2 days' WHERE id=:id"),
            {"id": live_id},
        )

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.requeued == 1
    assert _state(admin_engine, expired_id)["status"] == "QUEUED"
    assert _state(admin_engine, live_id)["status"] == "RUNNING"


@pytest.mark.parametrize("max_attempts", [1, 5])
def test_heartbeat_renewed_after_candidate_read_prevents_requeue_or_failure(
    persistence: BacktestPersistence, admin_engine: Engine, max_attempts: int
) -> None:
    run_id = _run(persistence)
    store = PersistenceExecutionKeyStore(persistence)
    store.claim(
        worker_execution_key_for(str(run_id), "renewed-race"),
        run_id=str(run_id),
        owner="worker",
        now=datetime.now(UTC),
        lease_duration=timedelta(minutes=1),
    )
    _expire(admin_engine, run_id)

    class RenewBeforeCloseRecovery(StaleRunRecovery):
        def _close_expired_attempt(self, connection, row, now, *, status, reason):  # type: ignore[no-untyped-def]
            with admin_engine.begin() as heartbeat_connection:
                heartbeat_connection.execute(
                    text("""
                    UPDATE backtest.run_attempts
                       SET last_heartbeat_at=clock_timestamp(),
                           claim_expires_at=clock_timestamp()+interval '5 minutes'
                     WHERE id=:id
                """),
                    {"id": row["attempt_id"]},
                )
            return super()._close_expired_attempt(connection, row, now, status=status, reason=reason)

    report = RenewBeforeCloseRecovery(persistence, max_attempts=max_attempts, queue_policy=POLICY).recover_once()

    assert report.requeued == 0
    assert report.failed == 0
    assert _state(admin_engine, run_id)["status"] == "RUNNING"


def test_expired_running_cancellation_finishes_cancelled_not_failed(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_id = _run(persistence)
    store = PersistenceExecutionKeyStore(persistence)
    store.claim(
        worker_execution_key_for(str(run_id), "cancel"),
        run_id=str(run_id),
        owner="worker",
        now=datetime.now(UTC),
        lease_duration=timedelta(minutes=1),
    )
    with persistence.unit_of_work() as uow:
        uow.runs.request_cancellation(run_id, reason_code="USER_CANCELLED")
    _expire(admin_engine, run_id)

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.cancelled == 1
    assert _state(admin_engine, run_id) == {
        "status": "CANCELLED",
        "failure_code": None,
        "cancellation_reason_code": "USER_CANCELLED",
    }


def test_expired_running_deletion_finishes_cancelled_and_hidden(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_id = _run(persistence)
    store = PersistenceExecutionKeyStore(persistence)
    store.claim(
        worker_execution_key_for(str(run_id), "delete"),
        run_id=str(run_id),
        owner="worker",
        now=datetime.now(UTC),
        lease_duration=timedelta(minutes=1),
    )
    with persistence.unit_of_work() as uow:
        pending = uow.runs.request_deletion(run_id)
    assert pending.deletion_requested_at is not None
    assert pending.deleted_at is None
    _expire(admin_engine, run_id)

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.cancelled == 1
    with admin_engine.connect() as connection:
        row = (
            connection.execute(
                text("""
            SELECT status, deletion_requested_at, deleted_at
              FROM backtest.runs WHERE id=:id
        """),
                {"id": run_id},
            )
            .mappings()
            .one()
        )
    assert row["status"] == "CANCELLED"
    assert row["deleted_at"] is not None
    assert row["deleted_at"] >= row["deletion_requested_at"]


def test_never_dispatched_queued_run_fails_after_timeout_and_recovery_is_idempotent(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_id = _run(persistence)
    with admin_engine.begin() as connection:
        connection.execute(
            text("UPDATE backtest.runs SET queued_at=clock_timestamp()-interval '1 hour' WHERE id=:id"),
            {"id": run_id},
        )
    recovery = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY)

    first = recovery.recover_once()
    second = recovery.recover_once()

    assert first.failed == 1
    assert second.failed == 0
    assert _state(admin_engine, run_id)["failure_code"] == "QUEUE_DISPATCH_TIMEOUT"


def test_twenty_run_sequential_custom_backlog_survives_behind_a_live_lease(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_ids = [_run(persistence, lane=RunLane.CUSTOM) for _ in range(20)]
    store = PersistenceExecutionKeyStore(persistence)
    store.claim(
        worker_execution_key_for(str(run_ids[0]), "custom-live"),
        run_id=str(run_ids[0]),
        owner="custom-worker",
        now=datetime.now(UTC),
        lease_duration=timedelta(minutes=30),
    )
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE backtest.runs SET queued_at=clock_timestamp()-interval '2 hours' "
                "WHERE id=any(cast(:ids as uuid[]))"
            ),
            {"ids": run_ids},
        )

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.failed == 0
    with admin_engine.connect() as connection:
        states = (
            connection.execute(
                text(
                    "SELECT status,count(*) count FROM backtest.runs WHERE id=any(cast(:ids as uuid[])) GROUP BY status"
                ),
                {"ids": run_ids},
            )
            .mappings()
            .all()
        )
    assert {row["status"]: row["count"] for row in states} == {
        "QUEUED": 19,
        "RUNNING": 1,
    }


def test_truly_stale_lane_heads_are_recovered_on_their_prompt_lane_deadlines(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_ids = {
        lane.value: _run(persistence, lane=lane) for lane in (RunLane.BASIC, RunLane.CUSTOM, RunLane.COMPETITION)
    }
    with admin_engine.begin() as connection:
        for lane, run_id in run_ids.items():
            timeout = POLICY.for_lane(lane).dispatch_timeout
            connection.execute(
                text("UPDATE backtest.runs SET queued_at=clock_timestamp()-:age WHERE id=:id"),
                {"id": run_id, "age": timeout + timedelta(seconds=1)},
            )

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.failed == 3
    assert all(_state(admin_engine, run_id)["failure_code"] == "QUEUE_DISPATCH_TIMEOUT" for run_id in run_ids.values())


def test_lane_dispatch_grace_differs_without_a_global_two_hour_blind_spot(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    run_ids = {
        lane.value: _run(persistence, lane=lane) for lane in (RunLane.BASIC, RunLane.CUSTOM, RunLane.COMPETITION)
    }
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE backtest.runs SET queued_at=clock_timestamp()-interval '4 minutes' "
                "WHERE id=any(cast(:ids as uuid[]))"
            ),
            {"ids": list(run_ids.values())},
        )

    report = StaleRunRecovery(persistence, max_attempts=5, queue_policy=POLICY).recover_once()

    assert report.failed == 1
    assert _state(admin_engine, run_ids["COMPETITION"])["status"] == "FAILED"
    assert _state(admin_engine, run_ids["BASIC"])["status"] == "QUEUED"
    assert _state(admin_engine, run_ids["CUSTOM"])["status"] == "QUEUED"
