"""SQLAlchemy Core repositories for the canonical `backtest` tables.

Every repository is bound to a `Connection`, never to an engine, so the caller decides
the transaction boundary. `BacktestUnitOfWork` hands out a consistent set of
repositories that all share one transaction; see `engine.BacktestPersistence`.

The uniqueness constraints in the canonical schema are used as the concurrency
controls they are:

* `runs.idempotency_key` — a duplicate accept returns the existing run.
* `run_attempts.worker_execution_key` — a second process cannot start the same
  execution. This is the only control that works across processes; the in-process lock
  in the pre-rebuild code did nothing for the actual failure mode.
* `run_attempts (run_id, attempt_number)` — attempt numbers cannot fork.
* `input_bundles.run_id` — one reproducibility boundary per run.
* `monthly_judgment_summaries (run_id, et_year_month)` — one summary per ET month.
* `detail_manifests (run_id, record_type, week_start_date, part_number)` and
  `detail_manifests.object_id` — one manifest per week part and per stored object.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Row, Select, func, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .errors import (
    AttemptNumberConflict,
    DuplicateWorkerExecution,
    IdempotencyConflict,
    InvalidStatusTransition,
    PublishConflict,
    RowNotFound,
    StaleAttemptClaim,
)
from .rows import (
    RUN_INPUT_PIN_IDENTITY_FIELDS,
    RUN_STATUS_TRANSITIONS,
    DetailManifestRow,
    FailureConditionCountRow,
    InputBundleRow,
    InputDatasetRow,
    InputFeatureMaterializationRow,
    MonthlyJudgmentSummaryRow,
    ObjectStatus,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunInputPinRow,
    RunLane,
    RunRow,
    RunStatus,
    StorageObjectRow,
    WorkStatus,
    row_to_params,
)
from .tables import (
    detail_manifests,
    failure_condition_counts,
    input_bundles,
    input_datasets,
    input_feature_materializations,
    monthly_judgment_summaries,
    performance_summaries,
    run_attempts,
    run_input_pins,
    runs,
    storage_objects,
)


__all__ = [
    "BacktestUnitOfWork",
    "DetailManifestRepository",
    "InputBundleRepository",
    "MonthlyJudgmentRepository",
    "PerformanceSummaryRepository",
    "RunAttemptRepository",
    "RunInputPinRepository",
    "RunRepository",
    "StorageObjectReader",
    "StorageObjectRepository",
]


_ENUM_FIELDS: dict[type, dict[str, type]] = {
    RunRow: {"status": RunStatus, "lane": RunLane},
    RunAttemptRow: {"status": WorkStatus},
    StorageObjectRow: {"status": ObjectStatus},
}

#: Fields that make two accepts of the same idempotency key "the same request".
_RUN_IDENTITY_FIELDS = (
    "bot_id",
    "owner_account_id",
    "configuration_hash",
    "evaluation_start",
    "evaluation_end",
    "initial_cash_amount",
    "market_rules_version",
    "accounting_rules_version",
    "precision_rules_version",
    "fee_policy_id",
    "slippage_rate_bps",
    "buying_power_buffer_policy_id",
    "lane",
    "message_id",
    "canonical_payload_hash",
    "aggregate_sequence",
    "execution_policy_version",
    "idempotency_scope",
)

_RUN_REQUIRED_ENVELOPE_FIELDS = (
    "lane",
    "message_id",
    "canonical_payload_hash",
    "aggregate_sequence",
    "execution_policy_version",
    "idempotency_scope",
)


def _hydrate[RowT](row_type: type[RowT], mapping: Mapping[Any, Any]) -> RowT:
    data = dict(mapping)
    for name, enum_type in _ENUM_FIELDS.get(row_type, {}).items():
        value = data.get(name)
        if value is not None and not isinstance(value, enum_type):
            data[name] = enum_type(value)
    return row_type(**data)


def _first(result: Sequence[Row[Any]]) -> Mapping[Any, Any] | None:
    return result[0]._mapping if result else None


class _Repository:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    @property
    def connection(self) -> Connection:
        return self._connection

    def _fetch_one[RowT](self, statement: Select[Any], row_type: type[RowT]) -> RowT | None:
        found = self._connection.execute(statement).mappings().first()
        return None if found is None else _hydrate(row_type, found)

    def _fetch_all[RowT](self, statement: Select[Any], row_type: type[RowT]) -> tuple[RowT, ...]:
        return tuple(_hydrate(row_type, mapping) for mapping in self._connection.execute(statement).mappings().all())


class RunRepository(_Repository):
    """`backtest.runs`."""

    def accept(self, row: RunRow) -> tuple[RunRow, bool]:
        """Insert a run, or return the run an equal request already created.

        Returns `(run, created)`. A repeated request with the same idempotency key and
        the same material inputs is not an error — it is the at-least-once delivery the
        queue guarantees. A *different* request under the same key is a conflict.
        """

        missing = [field for field in _RUN_REQUIRED_ENVELOPE_FIELDS if getattr(row, field) is None]
        if missing:
            raise ValueError(f"run acceptance requires an explicit lane envelope; missing fields: {missing}")

        statement = pg_insert(runs).values(**row_to_params(row)).on_conflict_do_nothing().returning(*runs.c)
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(RunRow, inserted), True

        existing = self.find_by_idempotency_key(row.idempotency_key)
        if existing is None:
            raise IdempotencyConflict(f"run id {row.id} is already used by a different idempotency key")
        differing = [field for field in _RUN_IDENTITY_FIELDS if getattr(existing, field) != getattr(row, field)]
        if differing:
            raise IdempotencyConflict(
                f"idempotency_key {row.idempotency_key!r} was already used for a different "
                f"request; differing fields: {differing}"
            )
        return existing, False

    def get(self, run_id: UUID) -> RunRow:
        found = self._fetch_one(select(runs).where(runs.c.id == run_id), RunRow)
        if found is None:
            raise RowNotFound(f"backtest run not found: {run_id}")
        return found

    def get_owned(self, owner_account_id: UUID, run_id: UUID) -> RunRow:
        """Owner-scoped read. A foreign run is indistinguishable from a missing one."""

        found = self._fetch_one(
            select(runs).where(runs.c.id == run_id, runs.c.owner_account_id == owner_account_id),
            RunRow,
        )
        if found is None:
            raise RowNotFound(f"backtest run not found: {run_id}")
        return found

    def find_by_idempotency_key(self, idempotency_key: str) -> RunRow | None:
        return self._fetch_one(select(runs).where(runs.c.idempotency_key == idempotency_key), RunRow)

    def list_by_owner(
        self,
        owner_account_id: UUID,
        *,
        limit: int = 50,
        offset: int = 0,
        bot_id: UUID | None = None,
    ) -> tuple[RunRow, ...]:
        """One page of an owner's runs, newest first.

        `bot_id` filters in SQL, before `LIMIT`. Filtering the page afterwards would
        silently drop runs the caller can never page to.
        """

        if limit < 1:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must not be negative")
        statement = select(runs).where(runs.c.owner_account_id == owner_account_id)
        if bot_id is not None:
            statement = statement.where(runs.c.bot_id == bot_id)
        return self._fetch_all(
            statement.order_by(runs.c.queued_at.desc(), runs.c.id).limit(limit).offset(offset),
            RunRow,
        )

    def mark_running(self, run_id: UUID, started_at: datetime) -> RunRow:
        return self._transition(run_id, RunStatus.RUNNING, started_at=started_at)

    def mark_completed(
        self,
        run_id: UUID,
        completed_at: datetime,
        result_hash: str,
        *,
        result_manifest_id: UUID | None = None,
    ) -> RunRow:
        """`result_manifest_id` is the contract's `resultManifestId`.

        Keyword-only and defaulted to `None` so the two existing callers that predate
        the column keep compiling, but the production ingestion path always supplies
        it: `backtest.v1` lists it in COMPLETED's `required`, so an event without one
        never reaches here.
        """

        return self._transition(
            run_id,
            RunStatus.COMPLETED,
            completed_at=completed_at,
            result_hash=result_hash,
            failure_code=None,
            result_manifest_id=result_manifest_id,
        )

    def mark_failed(
        self,
        run_id: UUID,
        completed_at: datetime,
        failure_code: str,
        *,
        retryable: bool | None = None,
    ) -> RunRow:
        return self._transition(
            run_id,
            RunStatus.FAILED,
            completed_at=completed_at,
            failure_code=failure_code,
            retryable=retryable,
        )

    def mark_unavailable(
        self,
        run_id: UUID,
        completed_at: datetime,
        failure_code: str,
        *,
        missing_requirements: Sequence[str] | None = None,
    ) -> RunRow:
        return self._transition(
            run_id,
            RunStatus.UNAVAILABLE,
            completed_at=completed_at,
            failure_code=failure_code,
            missing_requirements=None if missing_requirements is None else list(missing_requirements),
        )

    def mark_cancelled(
        self,
        run_id: UUID,
        cancelled_at: datetime,
        reason_code: str,
    ) -> RunRow:
        current = self.get(run_id)
        return self._transition(
            run_id,
            RunStatus.CANCELLED,
            completed_at=cancelled_at,
            cancellation_requested_at=current.cancellation_requested_at or cancelled_at,
            cancellation_reason_code=current.cancellation_reason_code or reason_code,
            cancelled_at=cancelled_at,
        )

    def request_cancellation(self, run_id: UUID, *, reason_code: str) -> RunRow:
        """Serialize cancellation with claim and terminal publication using DB time."""
        if not reason_code.strip():
            raise ValueError("reason_code must not be blank")
        current = self._connection.execute(select(runs).where(runs.c.id == run_id).with_for_update()).mappings().first()
        if current is None:
            raise RowNotFound(f"backtest run not found: {run_id}")
        hydrated = _hydrate(RunRow, current)
        if hydrated.status in {
            RunStatus.COMPLETED,
            RunStatus.FAILED,
            RunStatus.CANCELLED,
            RunStatus.UNAVAILABLE,
        }:
            return hydrated
        now = self._connection.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        values: dict[str, Any] = {
            "cancellation_requested_at": now,
            "cancellation_reason_code": reason_code,
        }
        if hydrated.status is RunStatus.QUEUED:
            values.update(status=RunStatus.CANCELLED.value, cancelled_at=now, completed_at=now)
        updated = (
            self._connection.execute(update(runs).where(runs.c.id == run_id).values(**values).returning(*runs.c))
            .mappings()
            .one()
        )
        return _hydrate(RunRow, updated)

    def _transition(self, run_id: UUID, target: RunStatus, **values: Any) -> RunRow:
        sources = sorted(source.value for source, allowed in RUN_STATUS_TRANSITIONS.items() if target in allowed)
        statement = (
            update(runs)
            .where(runs.c.id == run_id, runs.c.status.in_(sources))
            .values(status=target.value, **values)
            .returning(*runs.c)
        )
        updated = _first(self._connection.execute(statement).all())
        if updated is not None:
            return _hydrate(RunRow, updated)

        current = self.get(run_id)
        if current.status is target:
            # At-least-once redelivery of the same terminal result. Already applied.
            return current
        raise InvalidStatusTransition(
            f"backtest run {run_id} is {current.status.value}; it cannot move to {target.value}"
        )


class RunAttemptRepository(_Repository):
    """`backtest.run_attempts` — the cross-process duplicate-worker control."""

    def claim_fenced(
        self,
        run_id: UUID,
        *,
        worker_id: str,
        execution_key: str,
        lease_duration: timedelta,
    ) -> RunAttemptRow | None:
        """Claim using database time and close an expired predecessor atomically."""
        if not worker_id.strip() or not execution_key.strip():
            raise ValueError("worker_id and execution_key must not be blank")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        run = (
            self._connection.execute(
                select(runs.c.status, runs.c.cancellation_requested_at).where(runs.c.id == run_id).with_for_update()
            )
            .mappings()
            .first()
        )
        if run is None:
            raise RowNotFound(f"backtest run not found: {run_id}")
        if (
            run["status"]
            in {
                RunStatus.COMPLETED.value,
                RunStatus.FAILED.value,
                RunStatus.CANCELLED.value,
                RunStatus.UNAVAILABLE.value,
            }
            or run["cancellation_requested_at"] is not None
        ):
            return None

        now = self._connection.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        latest = (
            self._connection.execute(
                select(run_attempts)
                .where(run_attempts.c.run_id == run_id)
                .order_by(run_attempts.c.attempt_number.desc())
                .limit(1)
                .with_for_update()
            )
            .mappings()
            .first()
        )
        previous_attempt_id: UUID | None = None
        next_number = 1
        if latest is not None:
            next_number = int(latest["attempt_number"]) + 1
            if latest["status"] in {
                WorkStatus.SUCCEEDED.value,
                WorkStatus.CANCELLED.value,
                WorkStatus.SKIPPED.value,
            }:
                return None
            if latest["status"] == WorkStatus.FAILED.value and latest["terminal_reason_code"] not in {
                "LEASE_EXPIRED",
                "RETRY_RELEASED",
            }:
                return None
            if latest["status"] == WorkStatus.RUNNING.value:
                expires_at = latest["claim_expires_at"]
                if expires_at is not None and expires_at > now:
                    return None
                closed = self._connection.execute(
                    update(run_attempts)
                    .where(
                        run_attempts.c.id == latest["id"],
                        run_attempts.c.claim_token == latest["claim_token"],
                        run_attempts.c.status == WorkStatus.RUNNING.value,
                        run_attempts.c.claim_expires_at <= now,
                    )
                    .values(
                        status=WorkStatus.FAILED.value,
                        completed_at=now,
                        failure_code="LEASE_EXPIRED",
                        terminal_reason_code="LEASE_EXPIRED",
                    )
                )
                if closed.rowcount != 1:
                    raise StaleAttemptClaim("expired attempt was reclaimed concurrently")
                previous_attempt_id = latest["id"]

        attempt_id = uuid4()
        claim_token = uuid4()
        attempt_key = f"{execution_key}:{next_number}"
        if len(attempt_key) > 160:
            raise ValueError("versioned worker execution key exceeds varchar(160)")
        inserted = (
            self._connection.execute(
                pg_insert(run_attempts)
                .values(
                    id=attempt_id,
                    run_id=run_id,
                    attempt_number=next_number,
                    worker_execution_key=attempt_key,
                    status=WorkStatus.RUNNING.value,
                    claim_token=claim_token,
                    worker_id=worker_id,
                    claimed_at=now,
                    claim_expires_at=now + lease_duration,
                    last_heartbeat_at=now,
                    previous_attempt_id=previous_attempt_id,
                    started_at=now,
                )
                .on_conflict_do_nothing()
                .returning(*run_attempts.c)
            )
            .mappings()
            .first()
        )
        if inserted is None:
            raise StaleAttemptClaim("attempt slot or execution key was claimed concurrently")
        changed = self._connection.execute(
            update(runs)
            .where(runs.c.id == run_id, runs.c.status == RunStatus.QUEUED.value)
            .values(status=RunStatus.RUNNING.value, started_at=func.coalesce(runs.c.started_at, now))
        )
        if changed.rowcount not in (0, 1):
            raise StaleAttemptClaim("run claim affected an unexpected row count")
        return _hydrate(RunAttemptRow, inserted)

    def heartbeat_fenced(self, attempt_id: UUID, claim_token: UUID, *, lease_duration: timedelta) -> RunAttemptRow:
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        now = self._connection.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        updated = (
            self._connection.execute(
                update(run_attempts)
                .where(
                    run_attempts.c.id == attempt_id,
                    run_attempts.c.claim_token == claim_token,
                    run_attempts.c.status == WorkStatus.RUNNING.value,
                    run_attempts.c.claim_expires_at > now,
                )
                .values(last_heartbeat_at=now, claim_expires_at=now + lease_duration)
                .returning(*run_attempts.c)
            )
            .mappings()
            .first()
        )
        if updated is None:
            raise StaleAttemptClaim("heartbeat matched no live attempt claim")
        return _hydrate(RunAttemptRow, updated)

    def latest_for_run(self, run_id: UUID) -> RunAttemptRow | None:
        return self._fetch_one(
            select(run_attempts)
            .where(run_attempts.c.run_id == run_id)
            .order_by(run_attempts.c.attempt_number.desc())
            .limit(1),
            RunAttemptRow,
        )

    def release_fenced(
        self,
        attempt_id: UUID,
        claim_token: UUID,
        *,
        terminal_reason_code: str,
        failure_code: str | None = None,
    ) -> RunAttemptRow:
        """Close exactly one live delivery and make its non-terminal run retryable."""
        attempt = (
            self._connection.execute(
                select(run_attempts.c.run_id).where(run_attempts.c.id == attempt_id).with_for_update()
            )
            .mappings()
            .first()
        )
        if attempt is None:
            raise StaleAttemptClaim("release matched no attempt")
        run_id = attempt["run_id"]
        self._connection.execute(select(runs.c.id).where(runs.c.id == run_id).with_for_update()).first()
        closed = self.close_fenced(
            attempt_id,
            claim_token,
            status=WorkStatus.FAILED,
            terminal_reason_code=terminal_reason_code,
            failure_code=failure_code,
        )
        self._connection.execute(
            update(runs)
            .where(
                runs.c.id == run_id,
                runs.c.status == RunStatus.RUNNING.value,
                runs.c.cancellation_requested_at.is_(None),
            )
            .values(status=RunStatus.QUEUED.value)
        )
        return closed

    def close_fenced(
        self,
        attempt_id: UUID,
        claim_token: UUID,
        *,
        status: WorkStatus,
        terminal_reason_code: str,
        failure_code: str | None = None,
    ) -> RunAttemptRow:
        if status in (WorkStatus.PENDING, WorkStatus.RUNNING):
            raise ValueError("close_fenced requires a terminal status")
        attempt = (
            self._connection.execute(
                select(run_attempts.c.run_id).where(run_attempts.c.id == attempt_id).with_for_update()
            )
            .mappings()
            .first()
        )
        if attempt is None:
            raise StaleAttemptClaim("terminal mutation matched no attempt")
        run = (
            self._connection.execute(
                select(runs.c.status, runs.c.cancellation_requested_at)
                .where(runs.c.id == attempt["run_id"])
                .with_for_update()
            )
            .mappings()
            .first()
        )
        if run is None:
            raise RowNotFound(f"backtest run not found: {attempt['run_id']}")
        now = self._connection.scalar(select(func.clock_timestamp()))
        assert isinstance(now, datetime)
        if run["cancellation_requested_at"] is not None and status is WorkStatus.SUCCEEDED:
            status = WorkStatus.CANCELLED
            terminal_reason_code = "CANCELLED_BY_REQUEST"
            failure_code = None
            self._connection.execute(
                update(runs)
                .where(runs.c.id == attempt["run_id"], runs.c.status == RunStatus.RUNNING.value)
                .values(status=RunStatus.CANCELLED.value, cancelled_at=now, completed_at=now)
            )
        updated = (
            self._connection.execute(
                update(run_attempts)
                .where(
                    run_attempts.c.id == attempt_id,
                    run_attempts.c.claim_token == claim_token,
                    run_attempts.c.status == WorkStatus.RUNNING.value,
                    run_attempts.c.claim_expires_at > now,
                )
                .values(
                    status=status.value,
                    completed_at=now,
                    terminal_reason_code=terminal_reason_code,
                    failure_code=failure_code,
                )
                .returning(*run_attempts.c)
            )
            .mappings()
            .first()
        )
        if updated is None:
            existing = self._fetch_one(
                select(run_attempts).where(run_attempts.c.id == attempt_id),
                RunAttemptRow,
            )
            if existing is not None and existing.claim_token == claim_token and existing.status is status:
                return existing
            raise StaleAttemptClaim("terminal mutation matched no live attempt claim")
        return _hydrate(RunAttemptRow, updated)

    def claim(self, row: RunAttemptRow) -> tuple[RunAttemptRow, bool]:
        """Claim an attempt. Returns `(attempt, created)`.

        `created is False` means this exact execution key already owns this attempt, so
        the caller is a redelivery of work that is already under way and must not run
        it a second time.
        """

        statement = (
            pg_insert(run_attempts).values(**row_to_params(row)).on_conflict_do_nothing().returning(*run_attempts.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(RunAttemptRow, inserted), True

        by_key = self.find_by_execution_key(row.worker_execution_key)
        if by_key is not None:
            if by_key.run_id == row.run_id and by_key.attempt_number == row.attempt_number:
                return by_key, False
            raise DuplicateWorkerExecution(
                f"worker_execution_key {row.worker_execution_key!r} already belongs to run "
                f"{by_key.run_id} attempt {by_key.attempt_number}"
            )

        by_number = self._fetch_one(
            select(run_attempts).where(
                run_attempts.c.run_id == row.run_id,
                run_attempts.c.attempt_number == row.attempt_number,
            ),
            RunAttemptRow,
        )
        if by_number is not None:
            raise AttemptNumberConflict(
                f"run {row.run_id} attempt {row.attempt_number} is already claimed by "
                f"execution key {by_number.worker_execution_key!r}"
            )
        raise DuplicateWorkerExecution(f"run attempt id {row.id} is already in use")

    def claim_exclusive(self, row: RunAttemptRow) -> RunAttemptRow:
        """Claim an attempt, rejecting a second worker for the same execution key."""

        attempt, created = self.claim(row)
        if not created:
            raise DuplicateWorkerExecution(
                f"worker_execution_key {row.worker_execution_key!r} is already claimed; "
                f"run {attempt.run_id} attempt {attempt.attempt_number} is already running"
            )
        return attempt

    def complete(
        self,
        worker_execution_key: str,
        *,
        status: WorkStatus,
        completed_at: datetime,
        failure_code: str | None = None,
    ) -> RunAttemptRow:
        if status in (WorkStatus.PENDING, WorkStatus.RUNNING):
            raise ValueError(f"{status.value} is not a completion status")
        statement = (
            update(run_attempts)
            .where(
                run_attempts.c.worker_execution_key == worker_execution_key,
                run_attempts.c.completed_at.is_(None),
            )
            .values(
                status=status.value,
                completed_at=completed_at,
                failure_code=failure_code,
                terminal_reason_code=(failure_code or status.value),
            )
            .returning(*run_attempts.c)
        )
        updated = _first(self._connection.execute(statement).all())
        if updated is not None:
            return _hydrate(RunAttemptRow, updated)

        existing = self.find_by_execution_key(worker_execution_key)
        if existing is None:
            raise RowNotFound(f"run attempt not found: {worker_execution_key}")
        if existing.status is status:
            return existing
        raise InvalidStatusTransition(
            f"run attempt {worker_execution_key} already completed as {existing.status.value}"
        )

    def find_by_execution_key(self, worker_execution_key: str) -> RunAttemptRow | None:
        return self._fetch_one(
            select(run_attempts).where(run_attempts.c.worker_execution_key == worker_execution_key),
            RunAttemptRow,
        )

    def list_for_run(self, run_id: UUID) -> tuple[RunAttemptRow, ...]:
        return self._fetch_all(
            select(run_attempts).where(run_attempts.c.run_id == run_id).order_by(run_attempts.c.attempt_number),
            RunAttemptRow,
        )

    def list_for_runs(self, run_ids: Sequence[UUID]) -> dict[UUID, tuple[RunAttemptRow, ...]]:
        """Attempts for a whole page of runs, in one query.

        The list endpoint reports an attempt count per row. Reading them with
        `list_for_run` in a loop would be one round trip per row, so the page cost
        would grow with the page size for a number that is a `count(*)` away.
        Runs with no attempts are simply absent from the result; the caller
        supplies the empty tuple, because "no attempts yet" is not "unknown".
        """

        if not run_ids:
            return {}
        rows = self._fetch_all(
            select(run_attempts)
            .where(run_attempts.c.run_id.in_(list(run_ids)))
            .order_by(run_attempts.c.run_id, run_attempts.c.attempt_number),
            RunAttemptRow,
        )
        grouped: dict[UUID, list[RunAttemptRow]] = {}
        for row in rows:
            grouped.setdefault(row.run_id, []).append(row)
        return {run_id: tuple(items) for run_id, items in grouped.items()}

    def next_attempt_number(self, run_id: UUID) -> int:
        """Advisory only. `(run_id, attempt_number)` is the real arbiter."""

        return len(self.list_for_run(run_id)) + 1


class RunInputPinRepository(_Repository):
    """`backtest.run_input_pins` — the request identifiers, pinned once at acceptance.

    Written in the same transaction as the `backtest.runs` insert, so a run either has
    its pins or does not exist. Idempotent on `run_id`, because acceptance itself is
    idempotent on `runs.idempotency_key`; a *different* pin set for a run that already
    has one means two materially different requests derived the same run id, which is
    the same conflict `RunRepository.accept` refuses.
    """

    def pin(self, row: RunInputPinRow) -> tuple[RunInputPinRow, bool]:
        statement = (
            pg_insert(run_input_pins).values(**row_to_params(row)).on_conflict_do_nothing().returning(*run_input_pins.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(RunInputPinRow, inserted), True

        existing = self.find(row.run_id)
        if existing is None:  # pragma: no cover - only reachable on a torn write
            raise PublishConflict(f"run input pins for run {row.run_id} vanished during insert")
        differing = [
            field for field in RUN_INPUT_PIN_IDENTITY_FIELDS if getattr(existing, field) != getattr(row, field)
        ]
        if differing:
            raise IdempotencyConflict(
                f"run {row.run_id} already pinned different request inputs; differing fields: {differing}"
            )
        return existing, False

    def find(self, run_id: UUID) -> RunInputPinRow | None:
        return self._fetch_one(select(run_input_pins).where(run_input_pins.c.run_id == run_id), RunInputPinRow)

    def get(self, run_id: UUID) -> RunInputPinRow:
        found = self.find(run_id)
        if found is None:
            raise RowNotFound(f"run input pins not found for run: {run_id}")
        return found

    def list_by_ids(self, run_ids: Sequence[UUID]) -> tuple[RunInputPinRow, ...]:
        """One query for a page of runs, so listing is not N+1."""

        if not run_ids:
            return ()
        return self._fetch_all(
            select(run_input_pins).where(run_input_pins.c.run_id.in_(list(run_ids))),
            RunInputPinRow,
        )


class InputBundleRepository(_Repository):
    """`backtest.input_bundles` and its two child tables."""

    def lock(
        self,
        bundle: InputBundleRow,
        datasets: Sequence[InputDatasetRow] = (),
        features: Sequence[InputFeatureMaterializationRow] = (),
    ) -> tuple[InputBundleRow, bool]:
        """Lock a run's reproducibility boundary. One bundle per run, ever."""

        statement = (
            pg_insert(input_bundles)
            .values(**row_to_params(bundle))
            .on_conflict_do_nothing()
            .returning(*input_bundles.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        created = inserted is not None
        if inserted is None:
            existing = self.get_by_run(bundle.run_id)
            if existing.bundle_hash != bundle.bundle_hash:
                raise PublishConflict(
                    f"run {bundle.run_id} already locked a different input bundle "
                    f"({existing.bundle_hash} != {bundle.bundle_hash})"
                )
            locked = existing
        else:
            locked = _hydrate(InputBundleRow, inserted)

        for dataset in datasets:
            self._connection.execute(
                pg_insert(input_datasets).values(**row_to_params(dataset)).on_conflict_do_nothing()
            )
        for feature in features:
            self._connection.execute(
                pg_insert(input_feature_materializations).values(**row_to_params(feature)).on_conflict_do_nothing()
            )
        return locked, created

    def get_by_run(self, run_id: UUID) -> InputBundleRow:
        found = self._fetch_one(select(input_bundles).where(input_bundles.c.run_id == run_id), InputBundleRow)
        if found is None:
            raise RowNotFound(f"input bundle not found for run: {run_id}")
        return found

    def datasets_for(self, input_bundle_id: UUID) -> tuple[InputDatasetRow, ...]:
        return self._fetch_all(
            select(input_datasets)
            .where(input_datasets.c.input_bundle_id == input_bundle_id)
            .order_by(input_datasets.c.dataset_manifest_id, input_datasets.c.purpose_code),
            InputDatasetRow,
        )

    def features_for(self, input_bundle_id: UUID) -> tuple[InputFeatureMaterializationRow, ...]:
        return self._fetch_all(
            select(input_feature_materializations)
            .where(input_feature_materializations.c.input_bundle_id == input_bundle_id)
            .order_by(input_feature_materializations.c.feature_materialization_id),
            InputFeatureMaterializationRow,
        )


class MonthlyJudgmentRepository(_Repository):
    """`backtest.monthly_judgment_summaries` and `backtest.failure_condition_counts`."""

    def insert_summary(self, row: MonthlyJudgmentSummaryRow) -> tuple[MonthlyJudgmentSummaryRow, bool]:
        statement = (
            pg_insert(monthly_judgment_summaries)
            .values(**row_to_params(row))
            .on_conflict_do_nothing()
            .returning(*monthly_judgment_summaries.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(MonthlyJudgmentSummaryRow, inserted), True

        existing = self.find_summary(row.run_id, row.et_year_month)
        if existing is None:
            raise PublishConflict(f"monthly summary id {row.id} is already in use")
        if existing.summary_hash != row.summary_hash:
            raise PublishConflict(
                f"run {row.run_id} month {row.et_year_month} already has a different summary "
                f"({existing.summary_hash} != {row.summary_hash})"
            )
        return existing, False

    def insert_failure_counts(self, rows: Sequence[FailureConditionCountRow]) -> int:
        inserted = 0
        for row in rows:
            statement = (
                pg_insert(failure_condition_counts)
                .values(**row_to_params(row))
                .on_conflict_do_nothing()
                .returning(failure_condition_counts.c.id)
            )
            if self._connection.execute(statement).first() is not None:
                inserted += 1
        return inserted

    def find_summary(self, run_id: UUID, et_year_month: str) -> MonthlyJudgmentSummaryRow | None:
        return self._fetch_one(
            select(monthly_judgment_summaries).where(
                monthly_judgment_summaries.c.run_id == run_id,
                monthly_judgment_summaries.c.et_year_month == et_year_month,
            ),
            MonthlyJudgmentSummaryRow,
        )

    def list_for_run(self, run_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]:
        return self._fetch_all(
            select(monthly_judgment_summaries)
            .where(monthly_judgment_summaries.c.run_id == run_id)
            .order_by(monthly_judgment_summaries.c.et_year_month),
            MonthlyJudgmentSummaryRow,
        )

    def failure_counts_for(self, monthly_summary_id: UUID) -> tuple[FailureConditionCountRow, ...]:
        return self._fetch_all(
            select(failure_condition_counts)
            .where(failure_condition_counts.c.monthly_summary_id == monthly_summary_id)
            .order_by(
                failure_condition_counts.c.flow_or_branch_key,
                failure_condition_counts.c.first_failure_condition_key,
            ),
            FailureConditionCountRow,
        )


class PerformanceSummaryRepository(_Repository):
    """`backtest.performance_summaries`. One immutable row per run."""

    def insert(self, row: PerformanceSummaryRow) -> tuple[PerformanceSummaryRow, bool]:
        statement = (
            pg_insert(performance_summaries)
            .values(**row_to_params(row))
            .on_conflict_do_nothing()
            .returning(*performance_summaries.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(PerformanceSummaryRow, inserted), True

        existing = self.find(row.run_id)
        if existing is None:  # pragma: no cover - only reachable on a torn write
            raise PublishConflict(f"performance summary for run {row.run_id} vanished")
        if existing.result_hash != row.result_hash:
            raise PublishConflict(
                f"run {row.run_id} already has a different performance summary "
                f"({existing.result_hash} != {row.result_hash})"
            )
        return existing, False

    def find(self, run_id: UUID) -> PerformanceSummaryRow | None:
        return self._fetch_one(
            select(performance_summaries).where(performance_summaries.c.run_id == run_id),
            PerformanceSummaryRow,
        )

    def get(self, run_id: UUID) -> PerformanceSummaryRow:
        found = self.find(run_id)
        if found is None:
            raise RowNotFound(f"performance summary not found for run: {run_id}")
        return found


class DetailManifestRepository(_Repository):
    """`backtest.detail_manifests`. ET Monday week + part, one row per stored object."""

    def insert(self, row: DetailManifestRow) -> tuple[DetailManifestRow, bool]:
        statement = (
            pg_insert(detail_manifests)
            .values(**row_to_params(row))
            .on_conflict_do_nothing()
            .returning(*detail_manifests.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(DetailManifestRow, inserted), True

        existing = self.find_part(row.run_id, row.record_type, row.week_start_date, row.part_number)
        if existing is not None:
            if existing.detail_hash != row.detail_hash:
                raise PublishConflict(
                    f"run {row.run_id} {row.record_type} week {row.week_start_date} part "
                    f"{row.part_number} already has a different manifest "
                    f"({existing.detail_hash} != {row.detail_hash})"
                )
            return existing, False

        by_object = self.find_by_object(row.object_id)
        if by_object is not None:
            raise PublishConflict(f"storage object {row.object_id} already has manifest {by_object.id}")
        raise PublishConflict(f"detail manifest id {row.id} is already in use")

    def find_part(
        self, run_id: UUID, record_type: str, week_start_date: date, part_number: int
    ) -> DetailManifestRow | None:
        return self._fetch_one(
            select(detail_manifests).where(
                detail_manifests.c.run_id == run_id,
                detail_manifests.c.record_type == record_type,
                detail_manifests.c.week_start_date == week_start_date,
                detail_manifests.c.part_number == part_number,
            ),
            DetailManifestRow,
        )

    def find_by_object(self, object_id: UUID) -> DetailManifestRow | None:
        return self._fetch_one(
            select(detail_manifests).where(detail_manifests.c.object_id == object_id),
            DetailManifestRow,
        )

    def list_for_run(self, run_id: UUID) -> tuple[DetailManifestRow, ...]:
        return self._fetch_all(
            select(detail_manifests)
            .where(detail_manifests.c.run_id == run_id)
            .order_by(
                detail_manifests.c.record_type,
                detail_manifests.c.week_start_date,
                detail_manifests.c.part_number,
            ),
            DetailManifestRow,
        )


class StorageObjectReader(_Repository):
    """Read-only access to `storage.objects`.

    Split from `StorageObjectRepository` so a caller that only needs to *read* object
    metadata cannot accidentally acquire the ability to publish one.
    """

    def find(self, object_id: UUID) -> StorageObjectRow | None:
        return self._fetch_one(select(storage_objects).where(storage_objects.c.id == object_id), StorageObjectRow)

    def get(self, object_id: UUID) -> StorageObjectRow:
        found = self.find(object_id)
        if found is None:
            raise RowNotFound(f"storage object not found: {object_id}")
        return found

    def find_by_key(
        self,
        *,
        storage_provider: str,
        bucket_name: str,
        object_key: str,
        provider_version_id: str,
    ) -> StorageObjectRow | None:
        return self._fetch_one(
            select(storage_objects).where(
                storage_objects.c.storage_provider == storage_provider,
                storage_objects.c.bucket_name == bucket_name,
                storage_objects.c.object_key == object_key,
                storage_objects.c.provider_version_id == provider_version_id,
            ),
            StorageObjectRow,
        )

    def find_result_snapshot_object(self, run_id: UUID, *, bucket_name: str) -> StorageObjectRow | None:
        """The immutable result-snapshot object one run published, or `None`.

        `backtest.*` has no column pointing at it: the JSON snapshot is registered in
        `storage.objects` only, and `runs.result_hash` is the *summary* digest, not the
        object's content hash. Its key is content-addressed under a run-scoped prefix
        (`result_snapshot.ResultSnapshotBuilder.build`), so the prefix is the only
        handle, and the bucket is required because object identity in
        `storage.objects` includes it — the same content in another deployment's
        bucket is a different row and must not be served here.

        A `LIKE` with no wildcard in the interpolated part: `run_id` is a `UUID`, whose
        text form cannot contain `%` or `_`.
        """

        prefix = f"backtest-results/{run_id}/%.json"
        rows = self._fetch_all(
            select(storage_objects)
            .where(
                storage_objects.c.bucket_name == bucket_name,
                storage_objects.c.object_key.like(prefix),
                storage_objects.c.status == ObjectStatus.AVAILABLE.value,
            )
            .order_by(storage_objects.c.created_at, storage_objects.c.id),
            StorageObjectRow,
        )
        if len(rows) > 1:
            raise PublishConflict(
                f"run {run_id} has {len(rows)} AVAILABLE result snapshot objects in bucket "
                f"{bucket_name!r}; an official result is immutable and has exactly one"
            )
        return rows[0] if rows else None

    def list_by_ids(self, object_ids: Sequence[UUID]) -> tuple[StorageObjectRow, ...]:
        if not object_ids:
            return ()
        return self._fetch_all(
            select(storage_objects)
            .where(storage_objects.c.id.in_(list(object_ids)))
            .order_by(storage_objects.c.created_at, storage_objects.c.id),
            StorageObjectRow,
        )

    def require_available(self, object_ids: Sequence[UUID]) -> tuple[StorageObjectRow, ...]:
        """Every named object must exist and be `AVAILABLE`, else `RowNotFound`."""

        found = self.list_by_ids(object_ids)
        by_id = {row.id: row for row in found}
        missing = [str(object_id) for object_id in object_ids if object_id not in by_id]
        if missing:
            raise RowNotFound(f"storage objects do not exist: {missing}")
        unavailable = [f"{row.id}={row.status.value}" for row in found if row.status is not ObjectStatus.AVAILABLE]
        if unavailable:
            raise RowNotFound(f"storage objects are not AVAILABLE: {unavailable}")
        return found


#: The subset of a `storage.objects` row that identifies the stored bytes. Two
#: registrations of the same object id must agree on all of it, or one of them is
#: describing different bytes under an id that is already taken.
_OBJECT_IDENTITY_FIELDS = (
    "storage_provider",
    "bucket_name",
    "object_key",
    "content_hash",
    "byte_size",
)


class StorageObjectRepository(StorageObjectReader):
    """Read/write access to `storage.objects`.

    Spec 2.5 requires every stored object to hold exactly one row here, and to become
    `AVAILABLE` **only after verification**. That ordering is enforced structurally
    rather than by convention:

    * `register()` refuses to insert anything but `STAGED`, so no code path can
      publish an object and verify it afterwards (or never).
    * `AVAILABLE` is reachable only through `mark_available()`, which is a conditional
      update from `STAGED`.

    Note the boundary this repository does *not* cross: it writes **rows**, never DDL.
    Spec 2.4 records that `DatabaseAccessPolicy` registers `storage` as SHARED while
    the checklist calls it D-owned, so this repository authors no `storage` migration
    and relies on the existing V1 definition. Writing rows to a shared schema is
    ordinary application traffic; creating tables in one is not.
    """

    def register(self, row: StorageObjectRow) -> tuple[StorageObjectRow, bool]:
        """Register a staged object. Returns `(row, inserted)`.

        Idempotent on `id`: re-registering an identical object is the at-least-once
        retry of an upload, not an error. Re-using an id for *different* bytes is a
        conflict, because the object id is what a detail manifest points at.
        """
        if row.status is not ObjectStatus.STAGED:
            raise PublishConflict(
                f"storage object {row.id} must be registered as STAGED, not "
                f"{row.status.value}; AVAILABLE is reachable only through "
                "mark_available() once the bytes have been verified"
            )
        if row.verified_at is not None:
            raise PublishConflict(f"storage object {row.id} cannot be registered as already verified")

        # No `index_elements`: `storage.objects` also carries a unique index on
        # (storage_provider, bucket_name, object_key, provider_version_id). Naming only
        # the primary key would let a re-registration raise IntegrityError from the
        # natural key instead of being recognised as the duplicate it is.
        statement = (
            pg_insert(storage_objects)
            .values(**row_to_params(row))
            .on_conflict_do_nothing()
            .returning(*storage_objects.c)
        )
        inserted = _first(self._connection.execute(statement).all())
        if inserted is not None:
            return _hydrate(StorageObjectRow, inserted), True

        existing = self.find(row.id) or self.find_by_key(
            storage_provider=row.storage_provider,
            bucket_name=row.bucket_name,
            object_key=row.object_key,
            provider_version_id=row.provider_version_id,
        )
        if existing is None:  # pragma: no cover - only reachable on a torn write
            raise PublishConflict(f"storage object {row.id} vanished during registration")
        if existing.id != row.id:
            raise PublishConflict(
                f"object key {row.object_key!r} is already registered as {existing.id}, "
                f"not {row.id}; one stored object has exactly one row"
            )
        differing = [field for field in _OBJECT_IDENTITY_FIELDS if getattr(existing, field) != getattr(row, field)]
        if differing:
            raise PublishConflict(
                f"storage object id {row.id} is already registered for different bytes; differing fields: {differing}"
            )
        return existing, False

    def mark_available(self, object_id: UUID, verified_at: datetime) -> StorageObjectRow:
        """Publish a verified object. Only `STAGED` may become `AVAILABLE`."""
        return self._transition(
            object_id,
            ObjectStatus.AVAILABLE,
            allowed_from=(ObjectStatus.STAGED,),
            verified_at=verified_at,
        )

    def quarantine(self, object_id: UUID, quarantined_at: datetime) -> StorageObjectRow:
        """Quarantine an object whose bytes failed verification.

        Reachable from `AVAILABLE` as well as `STAGED`: corruption can be discovered
        after publication, and the row must be able to say so.
        """
        return self._transition(
            object_id,
            ObjectStatus.QUARANTINED,
            allowed_from=(ObjectStatus.STAGED, ObjectStatus.AVAILABLE),
            quarantined_at=quarantined_at,
        )

    def _transition(
        self,
        object_id: UUID,
        target: ObjectStatus,
        *,
        allowed_from: Sequence[ObjectStatus],
        **values: Any,
    ) -> StorageObjectRow:
        statement = (
            update(storage_objects)
            .where(
                storage_objects.c.id == object_id,
                storage_objects.c.status.in_([source.value for source in allowed_from]),
            )
            .values(status=target.value, **values)
            .returning(*storage_objects.c)
        )
        updated = _first(self._connection.execute(statement).all())
        if updated is not None:
            return _hydrate(StorageObjectRow, updated)

        current = self.find(object_id)
        if current is None:
            raise RowNotFound(f"storage object not found: {object_id}")
        if current.status is target:
            # At-least-once redelivery of a verification that already landed.
            return current
        raise InvalidStatusTransition(
            f"storage object {object_id} is {current.status.value}; it cannot move to "
            f"{target.value} (allowed from {[s.value for s in allowed_from]})"
        )


class BacktestUnitOfWork:
    """Every repository below shares one connection, and therefore one transaction."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.runs = RunRepository(connection)
        self.attempts = RunAttemptRepository(connection)
        self.pins = RunInputPinRepository(connection)
        self.inputs = InputBundleRepository(connection)
        self.monthly = MonthlyJudgmentRepository(connection)
        self.performance = PerformanceSummaryRepository(connection)
        self.manifests = DetailManifestRepository(connection)
        self.objects = StorageObjectRepository(connection)
