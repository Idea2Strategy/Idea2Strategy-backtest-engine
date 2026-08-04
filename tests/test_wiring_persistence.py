"""BT7: the two adapters that only exist against a real PostgreSQL.

``PersistenceExecutionKeyStore`` is the compare-and-swap that stops two workers
on two machines executing the same message twice, and
``PersistenceStorageObjectWritePort`` is the durable `storage.objects` writer the
object-store package deliberately left unbound. Neither can be shown to work in
process: the arbiter of the first is a unique index and the arbiter of the second
is a conditional UPDATE, so both are exercised here against the Testcontainers
PostgreSQL 16 from ``conftest`` and asserted with plain SQL on a separate,
unguarded engine.
"""

from __future__ import annotations

import hashlib
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest
from sqlalchemy import Engine, text

from backtest_engine.object_store import (
    LocalObjectStore,
    ObjectStoreConflict,
    StorageObjectRegistrar,
)
from backtest_engine.persistence import BacktestPersistence, ObjectStatus, RunStatus
from backtest_engine.wiring import (
    PersistenceExecutionKeyStore,
    PersistenceStorageObjectWritePort,
)
from backtest_engine.worker import ExecutionRecordStatus, worker_execution_key_for
from persistence.support import make_run


pytestmark = pytest.mark.docker


T0 = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
BODY = b"PAR1-fixture-object-body"
OTHER_BODY = b"PAR1-a-completely-different-object"
LEASE = timedelta(minutes=1)


@pytest.fixture
def run_id(persistence: BacktestPersistence) -> uuid.UUID:
    """A real `backtest.runs` row, because `run_attempts.run_id` is a foreign key."""
    row = make_run(idempotency_key=f"BT7:{uuid.uuid4()}", status=RunStatus.RUNNING)
    with persistence.unit_of_work() as uow:
        stored, created = uow.runs.accept(row)
    assert created
    return stored.id


@pytest.fixture
def store(persistence: BacktestPersistence) -> PersistenceExecutionKeyStore:
    return PersistenceExecutionKeyStore(persistence)


def attempt_rows(engine: Engine, run_id: uuid.UUID) -> list[dict[str, Any]]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(
                text(
                    "SELECT attempt_number, status, worker_execution_key, completed_at "
                    "FROM backtest.run_attempts WHERE run_id = :id ORDER BY attempt_number"
                ),
                {"id": run_id},
            ).mappings()
        ]


# ===========================================================================
# The execution-key compare-and-swap
# ===========================================================================


def test_the_first_claim_writes_a_durable_attempt_row(
    store: PersistenceExecutionKeyStore, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:first")

    claim = store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)

    assert claim.acquired is True
    assert claim.attempt_number == 1
    rows = attempt_rows(admin_engine, run_id)
    assert rows == [
        {
            "attempt_number": 1,
            "status": "RUNNING",
            "worker_execution_key": f"{key}:1",
            "completed_at": None,
        }
    ]


def test_only_one_of_many_concurrent_workers_wins_the_durable_cas(
    persistence: BacktestPersistence, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    """The unique index is the arbiter; an in-process lock could not do this."""
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:race")
    barrier = Barrier(8)

    def claim(index: int) -> bool:
        # A separate store per thread, as separate worker processes would have.
        local = PersistenceExecutionKeyStore(persistence)
        barrier.wait()
        return local.claim(key, run_id=str(run_id), owner=f"worker-{index}", now=T0, lease_duration=LEASE).acquired

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(claim, range(8)))

    assert results.count(True) == 1
    assert len(attempt_rows(admin_engine, run_id)) == 1


def test_a_second_worker_sees_the_first_as_in_progress(store: PersistenceExecutionKeyStore, run_id: uuid.UUID) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:held")
    store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)

    second = store.claim(key, run_id=str(run_id), owner="worker-b", now=T0, lease_duration=LEASE)

    assert second.acquired is False
    assert second.existing_status is ExecutionRecordStatus.IN_PROGRESS
    assert second.attempt_number == 1


def test_a_released_attempt_is_closed_and_retry_gets_a_fresh_fence(
    store: PersistenceExecutionKeyStore, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    """A retry never reuses the predecessor's fencing token or attempt row."""
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:retried")
    first = store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)

    store.release(key, now=T0 + timedelta(seconds=5), claim=first)
    assert attempt_rows(admin_engine, run_id)[0]["status"] == "FAILED"
    assert store.status(key) is ExecutionRecordStatus.FAILED

    reclaimed = store.claim(
        key, run_id=str(run_id), owner="worker-b", now=T0 + timedelta(seconds=6), lease_duration=LEASE
    )

    assert first.acquired and reclaimed.acquired
    assert first.attempt_number == 1
    assert reclaimed.attempt_number == 2
    rows = attempt_rows(admin_engine, run_id)
    assert rows[0]["completed_at"] is not None
    assert [{**row, "completed_at": None} for row in rows] == [
        {
            "attempt_number": 1,
            "status": "FAILED",
            "worker_execution_key": f"{key}:1",
            "completed_at": None,
        },
        {
            "attempt_number": 2,
            "status": "RUNNING",
            "worker_execution_key": f"{key}:2",
            "completed_at": None,
        },
    ]


def test_only_one_worker_wins_a_release_race(
    persistence: BacktestPersistence, store: PersistenceExecutionKeyStore, run_id: uuid.UUID
) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:reclaim-race")
    first = store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)
    store.release(key, now=T0, claim=first)
    barrier = Barrier(6)

    def reclaim(index: int) -> bool:
        local = PersistenceExecutionKeyStore(persistence)
        barrier.wait()
        return local.claim(key, run_id=str(run_id), owner=f"worker-{index}", now=T0, lease_duration=LEASE).acquired

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(reclaim, range(6)))

    assert results.count(True) == 1


@pytest.mark.parametrize(
    ("finished", "expected"),
    [
        (ExecutionRecordStatus.SUCCEEDED, ExecutionRecordStatus.SUCCEEDED),
        (ExecutionRecordStatus.FAILED, ExecutionRecordStatus.FAILED),
    ],
)
def test_a_finished_key_is_never_reclaimable(
    store: PersistenceExecutionKeyStore,
    run_id: uuid.UUID,
    finished: ExecutionRecordStatus,
    expected: ExecutionRecordStatus,
) -> None:
    key = worker_execution_key_for(str(run_id), f"OFFICIAL_BACKTEST:{finished.value}")
    claim = store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)
    store.finish(key, finished, now=T0 + timedelta(seconds=1), claim=claim)

    again = store.claim(key, run_id=str(run_id), owner="worker-b", now=T0 + timedelta(seconds=2), lease_duration=LEASE)

    assert again.acquired is False
    assert again.existing_status is expected
    assert store.status(key) is expected


def test_finish_refuses_a_non_terminal_status(store: PersistenceExecutionKeyStore, run_id: uuid.UUID) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:in-progress")
    store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)

    with pytest.raises(ValueError, match="terminal status"):
        store.finish(key, ExecutionRecordStatus.IN_PROGRESS, now=T0)


def test_the_status_of_an_unknown_key_is_none(store: PersistenceExecutionKeyStore, run_id: uuid.UUID) -> None:
    assert store.status(worker_execution_key_for(str(run_id), "never-claimed")) is None


def test_a_second_message_cannot_fork_a_live_run(
    store: PersistenceExecutionKeyStore, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    first = store.claim(
        worker_execution_key_for(str(run_id), "message-one"),
        run_id=str(run_id),
        owner="worker-a",
        now=T0,
        lease_duration=LEASE,
    )
    second = store.claim(
        worker_execution_key_for(str(run_id), "message-two"),
        run_id=str(run_id),
        owner="worker-a",
        now=T0,
        lease_duration=LEASE,
    )

    assert first.acquired is True
    assert second.acquired is False
    assert [row["attempt_number"] for row in attempt_rows(admin_engine, run_id)] == [1]


# ===========================================================================
# The durable storage.objects write port
# ===========================================================================


def _publish(
    port: PersistenceStorageObjectWritePort,
    root: Path,
    *,
    object_id: uuid.UUID,
    key: str,
    body: bytes,
) -> Any:
    registrar = StorageObjectRegistrar(LocalObjectStore(root, bucket_name="bt7-fixture"), port)
    return registrar.publish(
        object_id=object_id,
        object_key=key,
        data=body,
        schema_version="1.0.0",
        row_count=3,
        period_start=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        period_end=datetime(2024, 1, 2, 14, 50, tzinfo=UTC),
        created_at=T0,
        verified_at=T0,
        expected_content_hash=hashlib.sha256(body).hexdigest(),
    )


def object_row(engine: Engine, object_id: uuid.UUID) -> dict[str, Any]:
    with engine.connect() as connection:
        return dict(
            connection.execute(
                text(
                    "SELECT status, content_hash, byte_size, verified_at, compression_codec, "
                    "file_format, bucket_name, object_key FROM storage.objects WHERE id = :id"
                ),
                {"id": object_id},
            )
            .mappings()
            .one()
        )


def test_an_object_reaches_available_only_after_its_bytes_were_re_hashed(
    persistence: BacktestPersistence, admin_engine: Engine, tmp_path: Path
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = f"backtest-results/{uuid.uuid4()}/TRADE_DETAIL/week_start=2024-01-01/part=0001/{'a' * 64}.parquet"

    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)

    row = object_row(admin_engine, object_id)
    assert published.record.status is ObjectStatus.AVAILABLE
    assert row["status"] == "AVAILABLE"
    assert row["verified_at"] is not None
    assert row["content_hash"] == hashlib.sha256(BODY).hexdigest()
    assert row["byte_size"] == len(BODY)
    # Spec 2.2: explicit UNCOMPRESSED, never zstd.
    assert row["compression_codec"] == "UNCOMPRESSED"
    assert row["file_format"] == "PARQUET"


def test_republishing_the_same_object_is_idempotent_not_a_second_row(
    persistence: BacktestPersistence, admin_engine: Engine, tmp_path: Path
) -> None:
    """An at-least-once retry of an upload must reconcile, not duplicate."""
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = f"backtest-results/{uuid.uuid4()}/TRADE_DETAIL/week_start=2024-01-01/part=0001/{'b' * 64}.parquet"

    first = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)
    second = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)

    assert first.record == second.record
    with admin_engine.connect() as connection:
        count = connection.execute(
            text("SELECT count(*) FROM storage.objects WHERE object_key = :key"), {"key": key}
        ).scalar_one()
    assert count == 1


def test_reusing_an_object_id_for_different_bytes_is_refused(persistence: BacktestPersistence, tmp_path: Path) -> None:
    """The object id is what a detail manifest points at; it cannot be re-aimed."""
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    prefix = f"backtest-results/{uuid.uuid4()}/TRADE_DETAIL/week_start=2024-01-01/part=0001"
    _publish(port, tmp_path, object_id=object_id, key=f"{prefix}/{'c' * 64}.parquet", body=BODY)

    with pytest.raises(ObjectStoreConflict):
        _publish(
            port,
            tmp_path,
            object_id=object_id,
            key=f"{prefix}/{'d' * 64}.parquet",
            body=OTHER_BODY,
        )


def test_find_projects_the_persisted_row_back_onto_the_value_object(
    persistence: BacktestPersistence, tmp_path: Path
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = f"backtest-results/{uuid.uuid4()}/REPLAY_LEDGER/week_start=2024-01-01/part=0001/{'e' * 64}.parquet"
    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)

    found = port.find(object_id)

    assert found == published.record
    assert port.find(uuid.uuid4()) is None
