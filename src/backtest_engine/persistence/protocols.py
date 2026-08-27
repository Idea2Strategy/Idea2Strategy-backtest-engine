"""Protocols the later call-site swap depends on, and an honest compatibility note.

## What swaps mechanically

`InMemoryDetailManifestStore`, `InMemoryResultSnapshotStore` and the projection half of
`InMemoryBacktestResultQueryStore` are `put`/`get` boundaries over values that map onto
canonical rows. `DetailManifestRepository`, `PerformanceSummaryRepository` and
`RunRepository.list_by_owner`/`get_owned` cover them: the call site changes from a dict
lookup to a repository call inside a unit of work, and the owner-scoped read already
raises the same "not found rather than forbidden" shape.

## What does not, and why

`lifecycle.BacktestRunStore` cannot be satisfied by a repository over the canonical
schema as the schema stands today. Its `BacktestRun` aggregate carries four fields the
canonical `backtest.runs` table has no column for:

* `request` — the whole accepted request envelope. `runs` stores only
  `configuration_hash` and the pinned policy versions.
* `status_result` — the whole applied result envelope.
* `version` — an optimistic-concurrency generation.
* `dispatch_pending` — whether the queue publish has happened. `claim_dispatch()` and
  `release_dispatch()` are exactly the durable, cross-process control that the current
  in-memory flag only pretends to be.

## What `result_query` did about the same problem — resolved

`result_query.BacktestResultQueryStore` had the same problem one level up: its
`_QueryEntry` embeds a `ResultSnapshot` that holds the object *bytes*, which live in
object storage, not in PostgreSQL. That is now settled.
`result_query.DurableBacktestResultQueryStore` takes **option 2 below** and
reconstructs the projection on read; the reasoning and the price are written down in
that module's docstring. It stores no projection, so there is nothing here for it to
drift from. `backtest.run_input_pins` is now provider-owned acceptance evidence that
names the normalized input bundle. This consumer maps that provider schema; the
historical consumer-owned singular-pin contribution is retained only for audit and
excluded from current schema assembly.

## What is still open: `lifecycle.BacktestRunStore`

Two honest options for the *write* aggregate, both still out of scope here:

1. Add a `backtest.run_dispatches` table (or `runs.dispatch_state`/`runs.row_version`
   columns) in a reviewed `db/schema.dbml` change plus a new central migration, then
   implement `BacktestRunStore` over it.
2. Change the lifecycle aggregate so the request envelope is not part of durable run
   state, and reconstruct what is needed from `runs` + `input_bundles` +
   `run_input_pins` + `performance_summaries` + `detail_manifests` + the object store.

Until one of those is decided, a call site that needs `BacktestRunStore` semantics
should use `RunStore` below, which is the subset the canonical schema actually
supports.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Protocol
from uuid import UUID

from .rows import (
    DetailManifestRow,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunRow,
    StorageObjectRow,
    WorkStatus,
)


__all__ = [
    "AttemptStore",
    "DetailManifestStore",
    "MonthlyJudgmentStore",
    "PerformanceSummaryStore",
    "RunStore",
    "StorageObjectSource",
]


class RunStore(Protocol):
    """The durable subset of `lifecycle.BacktestRunStore` the canonical schema supports."""

    def accept(self, row: RunRow) -> tuple[RunRow, bool]: ...

    def get(self, run_id: UUID) -> RunRow: ...

    def get_owned(self, owner_account_id: UUID, run_id: UUID) -> RunRow: ...

    def list_by_owner(self, owner_account_id: UUID, *, limit: int = ..., offset: int = ...) -> tuple[RunRow, ...]: ...

    def mark_running(self, run_id: UUID, started_at: datetime) -> RunRow: ...

    def mark_completed(self, run_id: UUID, completed_at: datetime, result_hash: str) -> RunRow: ...

    def mark_failed(self, run_id: UUID, completed_at: datetime, failure_code: str) -> RunRow: ...

    def mark_unavailable(self, run_id: UUID, completed_at: datetime, failure_code: str) -> RunRow: ...

    def request_deletion(self, run_id: UUID, *, requested_at: datetime) -> RunRow: ...


class AttemptStore(Protocol):
    """Durable replacement for the in-process attempt lock in `attempt_coordinator`."""

    def claim(self, row: RunAttemptRow) -> tuple[RunAttemptRow, bool]: ...

    def claim_exclusive(self, row: RunAttemptRow) -> RunAttemptRow: ...

    def complete(
        self,
        worker_execution_key: str,
        *,
        status: WorkStatus,
        completed_at: datetime,
        failure_code: str | None = ...,
    ) -> RunAttemptRow: ...

    def list_for_run(self, run_id: UUID) -> tuple[RunAttemptRow, ...]: ...


class MonthlyJudgmentStore(Protocol):
    def insert_summary(self, row: MonthlyJudgmentSummaryRow) -> tuple[MonthlyJudgmentSummaryRow, bool]: ...

    def list_for_run(self, run_id: UUID) -> tuple[MonthlyJudgmentSummaryRow, ...]: ...


class PerformanceSummaryStore(Protocol):
    def insert(self, row: PerformanceSummaryRow) -> tuple[PerformanceSummaryRow, bool]: ...

    def get(self, run_id: UUID) -> PerformanceSummaryRow: ...


class DetailManifestStore(Protocol):
    def insert(self, row: DetailManifestRow) -> tuple[DetailManifestRow, bool]: ...

    def list_for_run(self, run_id: UUID) -> tuple[DetailManifestRow, ...]: ...


class StorageObjectSource(Protocol):
    """Read-only. There is deliberately no write method; see `repositories`."""

    def get(self, object_id: UUID) -> StorageObjectRow: ...

    def require_available(self, object_ids: Sequence[UUID]) -> tuple[StorageObjectRow, ...]: ...
