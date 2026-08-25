"""Lease-aware reconciliation for non-terminal backtest runs."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

from .persistence import BacktestPersistence, create_backtest_engine


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    inspected: int = 0
    requeued: int = 0
    failed: int = 0
    cancelled: int = 0


class StaleRunRecovery:
    """Reconcile only work whose durable lease no longer proves it is live."""

    def __init__(
        self,
        persistence: BacktestPersistence,
        *,
        max_attempts: int,
        queued_timeout: timedelta,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if queued_timeout <= timedelta(0):
            raise ValueError("queued_timeout must be positive")
        self._persistence = persistence
        self._max_attempts = max_attempts
        self._queued_timeout = queued_timeout

    def recover_once(self) -> RecoveryReport:
        counts = {"inspected": 0, "requeued": 0, "failed": 0, "cancelled": 0}
        with self._persistence.unit_of_work() as uow:
            now = uow.connection.scalar(text("SELECT clock_timestamp()"))
            assert isinstance(now, datetime)
            candidates = uow.connection.execute(text("""
                SELECT r.id, r.status, r.queued_at, r.started_at,
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
            """)).mappings().all()
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
        attempt_running = str(row["attempt_status"] or "") == "RUNNING"
        lease_expires = row["claim_expires_at"]
        heartbeat = row["last_heartbeat_at"]
        live = (
            attempt_running
            and lease_expires is not None
            and heartbeat is not None
            and lease_expires > now
        )
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
            changed = connection.execute(text("""
                UPDATE backtest.runs
                   SET status='CANCELLED', completed_at=:now, cancelled_at=:now,
                       cancellation_reason_code=COALESCE(cancellation_reason_code, 'USER_CANCELLED')
                 WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
            """), {"id": row["id"], "now": now})
            counts["cancelled"] += changed.rowcount
            return

        if attempt_number >= self._max_attempts:
            closed = self._close_expired_attempt(
                connection, row, now, status="FAILED", reason="MAX_ATTEMPTS_EXHAUSTED"
            )
            if attempt_running and closed == 0:
                return
            counts["failed"] += self._fail_run(
                connection, row["id"], now, "MAX_ATTEMPTS_EXHAUSTED"
            )
            return

        if str(row["status"]) == "RUNNING":
            if row["attempt_id"] is None:
                progress_at = row["started_at"] or row["queued_at"]
                if progress_at + self._queued_timeout <= now:
                    counts["failed"] += self._fail_run(
                        connection, row["id"], now, "WORKER_STATE_INCONSISTENT"
                    )
                return
            closed = self._close_expired_attempt(
                connection, row, now, status="FAILED", reason="LEASE_EXPIRED"
            )
            if closed == 0:
                return
            changed = connection.execute(text("""
                UPDATE backtest.runs SET status='QUEUED'
                 WHERE id=:id AND status='RUNNING' AND cancellation_requested_at IS NULL
            """), {"id": row["id"]})
            counts["requeued"] += changed.rowcount
            return

        progress_at = row["attempt_completed_at"] or row["queued_at"]
        if progress_at + self._queued_timeout <= now:
            counts["failed"] += self._fail_run(
                connection, row["id"], now, "QUEUE_DISPATCH_TIMEOUT"
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
        changed = connection.execute(text(f"""
            UPDATE backtest.run_attempts
               SET status='{status}', completed_at=:now,
                   failure_code=:failure_code, terminal_reason_code=:reason
             WHERE id=:id AND status='RUNNING'
               AND (claim_expires_at IS NULL OR claim_expires_at <= :now)
        """), {
            "id": row["attempt_id"],
            "now": now,
            "failure_code": None if status == "CANCELLED" else reason,
            "reason": reason,
        })
        return changed.rowcount

    @staticmethod
    def _fail_run(connection: Any, run_id: Any, now: datetime, reason: str) -> int:
        changed = connection.execute(text("""
            UPDATE backtest.runs
               SET status='FAILED', completed_at=:now, failure_code=:reason, retryable=false
             WHERE id=:id AND status IN ('QUEUED', 'RUNNING')
        """), {"id": run_id, "now": now, "reason": reason})
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
            queued_timeout=timedelta(
                seconds=int(values.get("BACKTEST_QUEUED_TIMEOUT_SECONDS", "900"))
            ),
        ).recover_once()
    finally:
        persistence.dispose()


def run_cli() -> None:
    print(json.dumps(asdict(run_once_from_environment()), sort_keys=True))


if __name__ == "__main__":  # pragma: no cover - exercised by local operations
    run_cli()
