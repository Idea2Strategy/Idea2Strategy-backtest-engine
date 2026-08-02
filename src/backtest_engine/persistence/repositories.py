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
from datetime import date, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Row, Select, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .errors import (
    AttemptNumberConflict,
    DuplicateWorkerExecution,
    IdempotencyConflict,
    InvalidStatusTransition,
    PublishConflict,
    RowNotFound,
)
from .rows import (
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
    "RunRepository",
    "StorageObjectReader",
    "StorageObjectRepository",
]


_ENUM_FIELDS: dict[type, dict[str, type]] = {
    RunRow: {"status": RunStatus},
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

    def list_by_owner(self, owner_account_id: UUID, *, limit: int = 50, offset: int = 0) -> tuple[RunRow, ...]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must not be negative")
        return self._fetch_all(
            select(runs)
            .where(runs.c.owner_account_id == owner_account_id)
            .order_by(runs.c.queued_at.desc(), runs.c.id)
            .limit(limit)
            .offset(offset),
            RunRow,
        )

    def mark_running(self, run_id: UUID, started_at: datetime) -> RunRow:
        return self._transition(run_id, RunStatus.RUNNING, started_at=started_at)

    def mark_completed(self, run_id: UUID, completed_at: datetime, result_hash: str) -> RunRow:
        return self._transition(
            run_id,
            RunStatus.COMPLETED,
            completed_at=completed_at,
            result_hash=result_hash,
            failure_code=None,
        )

    def mark_failed(self, run_id: UUID, completed_at: datetime, failure_code: str) -> RunRow:
        return self._transition(run_id, RunStatus.FAILED, completed_at=completed_at, failure_code=failure_code)

    def mark_unavailable(self, run_id: UUID, completed_at: datetime, failure_code: str) -> RunRow:
        return self._transition(run_id, RunStatus.UNAVAILABLE, completed_at=completed_at, failure_code=failure_code)

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
            .values(status=status.value, completed_at=completed_at, failure_code=failure_code)
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

    def next_attempt_number(self, run_id: UUID) -> int:
        """Advisory only. `(run_id, attempt_number)` is the real arbiter."""

        return len(self.list_for_run(run_id)) + 1


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
            raise PublishConflict(
                f"storage object {row.id} cannot be registered as already verified"
            )

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
        differing = [
            field
            for field in _OBJECT_IDENTITY_FIELDS
            if getattr(existing, field) != getattr(row, field)
        ]
        if differing:
            raise PublishConflict(
                f"storage object id {row.id} is already registered for different bytes; "
                f"differing fields: {differing}"
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
        self.inputs = InputBundleRepository(connection)
        self.monthly = MonthlyJudgmentRepository(connection)
        self.performance = PerformanceSummaryRepository(connection)
        self.manifests = DetailManifestRepository(connection)
        self.objects = StorageObjectRepository(connection)
