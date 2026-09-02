"""Lease-aware reconciliation for non-terminal backtest runs."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Any

from sqlalchemy import text

from .persistence import BacktestPersistence, create_backtest_engine


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    inspected: int = 0
    requeued: int = 0
    failed: int = 0
    cancelled: int = 0


@dataclass(frozen=True, slots=True)
class QueueLanePolicy:
    """How many queue heads can dispatch and how long each head gets to do so."""

    capacity: int
    dispatch_timeout: timedelta

    def __post_init__(self) -> None:
        if self.capacity < 1:
            raise ValueError("queue lane capacity must be positive")
        if self.dispatch_timeout <= timedelta(0):
            raise ValueError("queue lane dispatch timeout must be positive")


@dataclass(frozen=True, slots=True)
class QueueDispatchPolicy:
    """Lane-aware queue recovery contract.

    Only queue heads for currently available lane slots can become dispatch-stale.
    Work behind a live lease or an earlier queue head is backlog, not abandoned work.
    Once it reaches a head slot, its deadline is anchored to the most recent durable
    predecessor progress and the lane's short dispatch grace.
    """

    lanes: Mapping[str, QueueLanePolicy]

    def __post_init__(self) -> None:
        normalized = {str(key).upper(): value for key, value in self.lanes.items()}
        required = {"BASIC", "CUSTOM", "COMPETITION"}
        if set(normalized) != required:
            raise ValueError("queue dispatch policy must declare BASIC, CUSTOM, and COMPETITION")
        if any(not isinstance(value, QueueLanePolicy) for value in normalized.values()):
            raise TypeError("queue dispatch policy contains a non-lane policy")
        object.__setattr__(self, "lanes", MappingProxyType(normalized))

    def for_lane(self, lane: str) -> QueueLanePolicy:
        try:
            return self.lanes[str(lane).upper()]
        except KeyError as exc:
            raise ValueError(f"unsupported recovery lane: {lane}") from exc

    @classmethod
    def from_environment(cls, values: Mapping[str, str]) -> QueueDispatchPolicy:
        defaults = {
            "BASIC": (2, 300),
            "CUSTOM": (1, 600),
            "COMPETITION": (1, 180),
        }
        return cls(
            {
                lane: QueueLanePolicy(
                    capacity=int(values.get(f"BACKTEST_{lane}_MAX_CONCURRENCY", capacity)),
                    dispatch_timeout=timedelta(
                        seconds=int(
                            values.get(
                                f"BACKTEST_{lane}_QUEUE_DISPATCH_TIMEOUT_SECONDS",
                                timeout,
                            )
                        )
                    ),
                )
                for lane, (capacity, timeout) in defaults.items()
            }
        )


class StaleRunRecovery:
    """Reconcile only work whose durable lease no longer proves it is live."""

    def __init__(
        self,
        persistence: BacktestPersistence,
        *,
        max_attempts: int,
        queue_policy: QueueDispatchPolicy,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if not isinstance(queue_policy, QueueDispatchPolicy):
            raise TypeError("queue_policy must be a QueueDispatchPolicy")
        self._persistence = persistence
        self._max_attempts = max_attempts
        self._queue_policy = queue_policy

    def recover_once(self) -> RecoveryReport:
        counts = {"inspected": 0, "requeued": 0, "failed": 0, "cancelled": 0}
        with self._persistence.unit_of_work() as uow:
            now = uow.connection.scalar(text("SELECT clock_timestamp()"))
            assert isinstance(now, datetime)
            candidates = (
                uow.connection.execute(
                    text("""
                SELECT r.id, r.status, r.lane, r.queued_at, r.started_at,
                       r.cancellation_requested_at, r.cancellation_reason_code,
                       a.id AS attempt_id, a.attempt_number, a.status AS attempt_status,
                       a.completed_at AS attempt_completed_at,
                       a.claim_expires_at, a.last_heartbeat_at
                  FROM backtest.runs r
                  LEFT JOIN LATERAL (
                    SELECT id, attempt_number, status, completed_at,
                           claim_expires_at, last_heartbeat_at
                      FROM backtest.run_attempts
                     WHERE run_id = r.id
                     ORDER BY attempt_number DESC
                     LIMIT 1
                  ) a ON true
                 WHERE r.status IN ('QUEUED', 'RUNNING')
                 ORDER BY r.queued_at, r.id
                 FOR UPDATE OF r SKIP LOCKED
            """),
                )
                .mappings()
                .all()
            )
            for row in candidates:
                counts["inspected"] += 1
                self._recover_candidate(uow.connection, row, now, counts)
        return RecoveryReport(**counts)

    def _recover_candidate(
        self,
        connection: Any,
        row: Any,
        now: datetime,
        counts: dict[str, int],
    ) -> None:
        attempt_number = int(row["attempt_number"] or 0)
        lane_policy = self._queue_policy.for_lane(str(row["lane"]))
        attempt_running = str(row["attempt_status"] or "") == "RUNNING"
        lease_expires = row["claim_expires_at"]
        heartbeat = row["last_heartbeat_at"]
        live = attempt_running and lease_expires is not None and heartbeat is not None and lease_expires > now
        if live:
            return

        cancellation_requested = row["cancellation_requested_at"] is not None
        if cancellation_requested:
            closed = self._close_expired_attempt(
                connection, row, now, status="CANCELLED", reason="CANCELLED_BY_REQUEST"
            )
            # A heartbeat may renew the lease after the candidate SELECT but
            # before this guarded UPDATE. In that race the active worker still
            # owns the attempt and will observe the cancellation itself.
            if attempt_running and closed == 0:
                return
            changed = connection.execute(
                text("""
                UPDATE backtest.runs
                   SET status='CANCELLED', completed_at=:now, cancelled_at=:now,
                       cancellation_reason_code=COALESCE(cancellation_reason_code, 'USER_CANCELLED'),
                       deleted_at=CASE WHEN deletion_requested_at IS NOT NULL
                                       THEN GREATEST(:now, deletion_requested_at)
                                       ELSE deleted_at END
                 WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
            """),
                {"id": row["id"], "now": now},
            )
            counts["cancelled"] += changed.rowcount
            return

        if attempt_number >= self._max_attempts:
            closed = self._close_expired_attempt(connection, row, now, status="FAILED", reason="MAX_ATTEMPTS_EXHAUSTED")
            if attempt_running and closed == 0:
                return
            counts["failed"] += self._fail_run(connection, row["id"], now, "MAX_ATTEMPTS_EXHAUSTED")
            return

        if str(row["status"]) == "RUNNING":
            if row["attempt_id"] is None:
                progress_at = row["started_at"] or row["queued_at"]
                if progress_at + lane_policy.dispatch_timeout <= now:
                    counts["failed"] += self._fail_run(connection, row["id"], now, "WORKER_STATE_INCONSISTENT")
                return
            closed = self._close_expired_attempt(connection, row, now, status="FAILED", reason="LEASE_EXPIRED")
            if closed == 0:
                return
            changed = connection.execute(
                text("""
                UPDATE backtest.runs SET status='QUEUED'
                 WHERE id=:id AND status='RUNNING' AND cancellation_requested_at IS NULL
            """),
                {"id": row["id"]},
            )
            counts["requeued"] += changed.rowcount
            return

        lane_facts = self._current_lane_facts(connection, row, now)
        live_lane_attempts = int(lane_facts["live_lane_attempts"] or 0)
        available_slots = max(lane_policy.capacity - live_lane_attempts, 0)
        if int(lane_facts["queued_ahead"] or 0) >= available_slots:
            return
        progress_at = max(
            value
            for value in (
                row["queued_at"],
                row["attempt_completed_at"],
                lane_facts["lane_progress_at"],
            )
            if value is not None
        )
        if progress_at + lane_policy.dispatch_timeout <= now:
            counts["failed"] += self._fail_run(connection, row["id"], now, "QUEUE_DISPATCH_TIMEOUT")

    @staticmethod
    def _current_lane_facts(connection: Any, row: Any, now: datetime) -> Mapping[str, Any]:
        """Read queue position after all earlier mutations in this recovery pass."""
        return (
            connection.execute(
                text("""
                SELECT (SELECT count(*)
                          FROM backtest.runs q
                         WHERE q.lane=:lane AND q.status='QUEUED'
                           AND (q.queued_at < :queued_at
                                OR (q.queued_at=:queued_at AND q.id < :id)))
                           AS queued_ahead,
                       (SELECT count(*)
                          FROM backtest.runs active
                          JOIN LATERAL (
                            SELECT status, claim_expires_at, last_heartbeat_at
                              FROM backtest.run_attempts active_attempt
                             WHERE active_attempt.run_id=active.id
                             ORDER BY attempt_number DESC LIMIT 1
                          ) live_attempt ON true
                         WHERE active.lane=:lane AND active.status='RUNNING'
                           AND live_attempt.status='RUNNING'
                           AND live_attempt.claim_expires_at > :now
                           AND live_attempt.last_heartbeat_at IS NOT NULL)
                           AS live_lane_attempts,
                       (SELECT max(prior.completed_at)
                          FROM backtest.runs prior
                         WHERE prior.lane=:lane AND prior.completed_at IS NOT NULL
                           AND (prior.queued_at < :queued_at
                                OR (prior.queued_at=:queued_at AND prior.id < :id)))
                           AS lane_progress_at
            """),
                {
                    "id": row["id"],
                    "lane": row["lane"],
                    "queued_at": row["queued_at"],
                    "now": now,
                },
            )
            .mappings()
            .one()
        )

    @staticmethod
    def _close_expired_attempt(
        connection: Any,
        row: Any,
        now: datetime,
        *,
        status: str,
        reason: str,
    ) -> int:
        if row["attempt_id"] is None or str(row["attempt_status"] or "") != "RUNNING":
            return 0
        changed = connection.scalar(
            text(
                "SELECT backtest.recover_expired_run_attempt("
                ":attempt_id, :status, :reason)"
            ),
            {
                "attempt_id": row["attempt_id"],
                "status": status,
                "reason": reason,
            },
        )
        return int(changed or 0)

    @staticmethod
    def _fail_run(connection: Any, run_id: Any, now: datetime, reason: str) -> int:
        changed = connection.execute(
            text("""
            UPDATE backtest.runs
               SET status='FAILED', completed_at=:now, failure_code=:reason, retryable=false
             WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
        """),
            {"id": run_id, "now": now, "reason": reason},
        )
        return changed.rowcount


def run_once_from_environment(environment: dict[str, str] | None = None) -> RecoveryReport:
    values = os.environ if environment is None else environment
    database_url = values.get("BACKTEST_DATABASE_URL", "").strip()
    if not database_url:
        raise ValueError("BACKTEST_DATABASE_URL is required")
    engine = create_backtest_engine(database_url)
    persistence = BacktestPersistence(engine)
    try:
        return StaleRunRecovery(
            persistence,
            max_attempts=int(values.get("BACKTEST_MAX_RECEIVE_COUNT", "5")),
            queue_policy=QueueDispatchPolicy.from_environment(values),
        ).recover_once()
    finally:
        persistence.dispose()


def run_cli() -> None:
    print(json.dumps(asdict(run_once_from_environment()), sort_keys=True))


if __name__ == "__main__":  # pragma: no cover - exercised by local operations
    run_cli()
