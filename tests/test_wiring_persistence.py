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
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier, Event
from typing import Any

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError

from backtest_engine.object_store import (
    LocalObjectStore,
    ObjectStoreConflict,
    S3ObjectStore,
    StorageObjectRegistrar,
)
from backtest_engine.persistence import (
    BacktestPersistence,
    ObjectStatus,
    RunStatus,
    create_backtest_engine,
)
from backtest_engine.wiring import (
    PersistenceExecutionKeyStore,
    PersistenceStorageObjectWritePort,
)
from backtest_engine.worker import ExecutionRecordStatus, worker_execution_key_for
from d_task5_chaos import wait_until
from persistence.support import make_detail_manifest, make_run


pytestmark = pytest.mark.docker


T0 = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
BODY = b"PAR1-fixture-object-body"
OTHER_BODY = b"PAR1-a-completely-different-object"
LEASE = timedelta(minutes=1)


def _canonical_cleanup_key(
    *,
    body: bytes = BODY,
    run_id: uuid.UUID | None = None,
    record_type: str = "TASK5_CLEANUP",
    part_number: int = 1,
) -> str:
    """One key inside the production-owned backtest result namespace."""

    return (
        f"backtest-results/{run_id or uuid.uuid4()}/{record_type}/"
        f"week_start=2024-01-01/part={part_number:04d}/"
        f"{hashlib.sha256(body).hexdigest()}.parquet"
    )


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

    store.release(
        key,
        now=T0 + timedelta(seconds=5),
        claim=first,
        reason_code="REQUIRED_DATA_UNAVAILABLE",
    )
    assert attempt_rows(admin_engine, run_id)[0]["status"] == "FAILED"
    assert store.status(key) is ExecutionRecordStatus.FAILED
    with admin_engine.connect() as connection:
        retry_reason = connection.execute(
            text(
                "SELECT failure_code, terminal_reason_code "
                "FROM backtest.run_attempts WHERE id = CAST(:id AS uuid)"
            ),
            {"id": first.attempt_id},
        ).mappings().one()
    assert dict(retry_reason) == {
        "failure_code": "REQUIRED_DATA_UNAVAILABLE",
        "terminal_reason_code": "RETRY_RELEASED",
    }

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


def test_terminal_failure_closes_attempt_and_run_in_one_persistence_operation(
    store: PersistenceExecutionKeyStore, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:exhausted")
    claim = store.claim(key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE)

    store.finish(
        key,
        ExecutionRecordStatus.FAILED,
        now=T0 + timedelta(seconds=2),
        claim=claim,
        reason_code="WORKER_TIMEOUT",
        run_id=str(run_id),
        run_failure_code="MAX_ATTEMPTS_EXHAUSTED",
    )

    with admin_engine.connect() as connection:
        run = connection.execute(
            text("SELECT status, failure_code, retryable FROM backtest.runs WHERE id = :id"),
            {"id": run_id},
        ).mappings().one()
        attempt = connection.execute(
            text(
                "SELECT status, failure_code, terminal_reason_code "
                "FROM backtest.run_attempts WHERE id = CAST(:id AS uuid)"
            ),
            {"id": claim.attempt_id},
        ).mappings().one()

    assert dict(run) == {
        "status": "FAILED",
        "failure_code": "MAX_ATTEMPTS_EXHAUSTED",
        "retryable": False,
    }
    assert dict(attempt) == {
        "status": "FAILED",
        "failure_code": "WORKER_TIMEOUT",
        "terminal_reason_code": "WORKER_TIMEOUT",
    }


def test_over_limit_repair_preserves_a_live_fenced_attempt(
    store: PersistenceExecutionKeyStore, run_id: uuid.UUID, admin_engine: Engine
) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:active-over-limit")
    claim = store.claim(
        key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE
    )
    assert claim.acquired

    status = store.record_run_failure(
        key, str(run_id), "MAX_ATTEMPTS_EXHAUSTED", now=T0 + timedelta(seconds=1)
    )

    assert status is ExecutionRecordStatus.IN_PROGRESS
    with admin_engine.connect() as connection:
        assert connection.scalar(
            text("SELECT status FROM backtest.runs WHERE id=:id"), {"id": run_id}
        ) == "RUNNING"
        assert connection.scalar(
            text("SELECT status FROM backtest.run_attempts WHERE id=CAST(:id AS uuid)"),
            {"id": claim.attempt_id},
        ) == "RUNNING"


def test_over_limit_repair_gives_expired_cancellation_precedence_over_failure(
    persistence: BacktestPersistence,
    store: PersistenceExecutionKeyStore,
    run_id: uuid.UUID,
    admin_engine: Engine,
) -> None:
    key = worker_execution_key_for(str(run_id), "OFFICIAL_BACKTEST:cancel-over-limit")
    claim = store.claim(
        key, run_id=str(run_id), owner="worker-a", now=T0, lease_duration=LEASE
    )
    with persistence.unit_of_work() as uow:
        uow.runs.request_cancellation(run_id, reason_code="USER_CANCELLED")
    with admin_engine.begin() as connection:
        connection.execute(text("""
            UPDATE backtest.run_attempts
               SET started_at=clock_timestamp()-interval '3 minutes',
                   claimed_at=clock_timestamp()-interval '3 minutes',
                   last_heartbeat_at=clock_timestamp()-interval '2 minutes',
                   claim_expires_at=clock_timestamp()-interval '1 minute'
             WHERE id=CAST(:id AS uuid)
        """), {"id": claim.attempt_id})

    status = store.record_run_failure(
        key, str(run_id), "MAX_ATTEMPTS_EXHAUSTED", now=T0 + timedelta(minutes=2)
    )

    assert status is ExecutionRecordStatus.CANCELLED
    with admin_engine.connect() as connection:
        run = connection.execute(
            text("SELECT status, failure_code, cancellation_reason_code FROM backtest.runs WHERE id=:id"),
            {"id": run_id},
        ).mappings().one()
    assert dict(run) == {
        "status": "CANCELLED",
        "failure_code": None,
        "cancellation_reason_code": "USER_CANCELLED",
    }


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


def test_cleanup_batch_fences_the_durable_provider_version(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = _canonical_cleanup_key(record_type="PROVIDER_VERSION_FENCE")
    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)

    with pytest.raises(
        ObjectStoreConflict, match="provider version"
    ), port.cleanup_batch(
        (
            replace(
                published.record,
                provider_version_id="different-provider-version",
            ),
        )
    ):
        pass

    assert object_row(admin_engine, object_id)["object_key"] == key


@pytest.mark.parametrize(
    ("field", "foreign_value"),
    [
        ("storage_provider", "FOREIGN_PROVIDER"),
        ("bucket_name", "foreign-bucket"),
    ],
)
def test_unregister_fences_the_durable_provider_and_bucket_identity(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
    field: str,
    foreign_value: str,
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = _canonical_cleanup_key(record_type="PROVIDER_BUCKET_FENCE")
    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)
    identity = {
        "storage_provider": published.record.storage_provider,
        "bucket_name": published.record.bucket_name,
        "object_key": published.record.object_key,
        "provider_version_id": published.record.provider_version_id,
        "content_hash": published.record.content_hash,
    }
    identity[field] = foreign_value

    with pytest.raises(ObjectStoreConflict):
        port.unregister(object_id, **identity)

    assert object_row(admin_engine, object_id)["object_key"] == key


def test_cleanup_batch_refuses_a_result_object_referenced_as_durable_evidence(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = _canonical_cleanup_key(record_type="DURABLE_EVIDENCE")
    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO operations.audit_events "
                "(id,actor_type,actor_id,action_type,target_domain,target_id,"
                "reason_code,correlation_id,idempotency_key,evidence_object_id,occurred_at) "
                "VALUES (:id,'SYSTEM',:actor,'TASK5_RESULT_REFERENCE','BACKTEST',"
                ":target,'TASK5_REFERENCE',:correlation,:key,:object_id,:occurred_at)"
            ),
            {
                "id": uuid.uuid4(),
                "actor": uuid.uuid4(),
                "target": uuid.uuid4(),
                "correlation": uuid.uuid4(),
                "key": f"TASK5:RESULT-REFERENCE:{uuid.uuid4()}",
                "object_id": object_id,
                "occurred_at": T0,
            },
        )

    with pytest.raises(
        ObjectStoreConflict, match="operations.audit_events"
    ), port.cleanup_batch((published.record,)):
        pass

    assert object_row(admin_engine, object_id)["object_key"] == key


def test_cleanup_batch_refuses_a_detail_object_referenced_by_a_manifest(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_id = uuid.uuid4()
    key = _canonical_cleanup_key(record_type="TRADE_DETAIL")
    published = _publish(port, tmp_path, object_id=object_id, key=key, body=BODY)
    run = make_run(idempotency_key=f"TASK5:DETAIL-REFERENCE:{uuid.uuid4()}")
    with persistence.unit_of_work() as uow:
        accepted, created = uow.runs.accept(run)
        assert created
        uow.manifests.insert(make_detail_manifest(accepted.id, object_id=object_id))

    with pytest.raises(
        ObjectStoreConflict, match="backtest.detail_manifests"
    ), port.cleanup_batch((published.record,)):
        pass

    assert object_row(admin_engine, object_id)["object_key"] == key


def test_live_catalog_exposes_every_canonical_storage_object_fk(
    admin_engine: Engine,
) -> None:
    with admin_engine.connect() as connection:
        canonical_references = {
            (row["source_table"], row["source_column"])
            for row in connection.execute(
                text(
                    "SELECT source_ns.nspname || '.' || source.relname AS source_table,"
                    "source_column.attname AS source_column "
                    "FROM pg_constraint fk "
                    "JOIN pg_class source ON source.oid=fk.conrelid "
                    "JOIN pg_namespace source_ns ON source_ns.oid=source.relnamespace "
                    "JOIN pg_class target ON target.oid=fk.confrelid "
                    "JOIN pg_namespace target_ns ON target_ns.oid=target.relnamespace "
                    "CROSS JOIN LATERAL unnest(fk.conkey) AS source_key(attnum) "
                    "JOIN pg_attribute source_column "
                    "ON source_column.attrelid=source.oid "
                    "AND source_column.attnum=source_key.attnum "
                    "WHERE fk.contype='f' AND target_ns.nspname='storage' "
                    "AND target.relname='objects'"
                )
            ).mappings()
        }

    assert canonical_references == {
        ("identity.account_sanction_events", "evidence_object_id"),
        ("market_data.dataset_objects", "object_id"),
        ("market_data.feature_snapshot_batches", "snapshot_object_id"),
        ("market_data.quality_incidents", "evidence_object_id"),
        ("bot.bot_events", "evidence_object_id"),
        ("backtest.detail_manifests", "object_id"),
        ("performance.series_manifests", "object_id"),
        ("operations.audit_events", "evidence_object_id"),
        ("operations.case_evidence_references", "storage_object_id"),
    }


def _future_fk_schema(
    admin_engine: Engine,
    *,
    object_id: uuid.UUID | None = None,
    with_constraint: bool = True,
) -> str:
    """Create one test-owned oddly-named future reference table."""

    schema = f"task5_fk_{uuid.uuid4().hex}"
    with admin_engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        connection.execute(
            text(
                f'CREATE TABLE "{schema}"."future object refs" '
                '("storage object id" uuid)'
            )
        )
        if with_constraint:
            connection.execute(
                text(
                    f'ALTER TABLE "{schema}"."future object refs" '
                    'ADD CONSTRAINT "future storage object fk" '
                    'FOREIGN KEY ("storage object id") REFERENCES storage.objects(id)'
                )
            )
        if object_id is not None:
            connection.execute(
                text(
                    f'INSERT INTO "{schema}"."future object refs" '
                    '("storage object id") VALUES (:object_id)'
                ),
                {"object_id": object_id},
            )
    return schema


def _drop_test_schema(admin_engine: Engine, schema: str) -> None:
    assert schema.startswith("task5_fk_")
    with admin_engine.begin() as connection:
        connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))


def _cleanup_owned_object(
    port: PersistenceStorageObjectWritePort,
    object_store: Any,
    record: Any,
) -> None:
    """Remove only the exact test-owned row/provider version, if still present."""

    current = port.find(record.object_id)
    if current is None:
        object_store.delete_if_matches(
            record.object_key,
            record.content_hash,
            record.provider_version_id,
        )
        return
    with port.cleanup_batch((current,)) as locked:
        for item in locked:
            if object_store.preflight_delete(
                item.object_key,
                item.content_hash,
                item.provider_version_id,
            ):
                object_store.delete_if_matches(
                    item.object_key,
                    item.content_hash,
                    item.provider_version_id,
                )


def test_canonical_backtest_role_uses_only_the_narrow_cleanup_capability(
    persistence: BacktestPersistence,
    backtest_role_persistence: BacktestPersistence,
    backtest_role_engine: Engine,
    admin_engine: Engine,
    s3: Any,
    bucket: str,
) -> None:
    """The production role cleans LocalStack without broad database privileges."""

    admin_port = PersistenceStorageObjectWritePort(persistence)
    runtime_port = PersistenceStorageObjectWritePort(backtest_role_persistence)
    object_store = S3ObjectStore(bucket, client=s3)
    published = StorageObjectRegistrar(object_store, admin_port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )

    with admin_engine.connect() as connection:
        identity_relation_oid = connection.scalar(
            text("SELECT 'identity.account_sanction_events'::regclass::oid")
        )

    with backtest_role_engine.connect() as connection:
        assert connection.execute(
            text(
                "SELECT current_user AS role_name,"
                "has_table_privilege(current_user,'storage.objects','DELETE') AS can_delete,"
                "has_table_privilege(current_user,"
                "CAST(:identity_relation_oid AS oid),'SELECT') AS can_read_identity,"
                "has_function_privilege(current_user,"
                "'storage.prepare_backtest_object_cleanup(jsonb)','EXECUTE') AS can_cleanup"
            ),
            {"identity_relation_oid": identity_relation_oid},
        ).mappings().one() == {
            "role_name": "idea2strategy_backtest",
            "can_delete": False,
            "can_read_identity": False,
            "can_cleanup": True,
        }

    with runtime_port.cleanup_batch((published.record,)) as locked:
        assert locked == (published.record,)
        assert object_store.delete_if_matches(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )

    assert runtime_port.find(published.record.object_id) is None
    assert not object_store.exists(published.record.object_key)
    with admin_engine.connect() as connection:
        assert connection.scalar(
            text("SELECT count(*) FROM storage.objects WHERE id=:id"),
            {"id": published.record.object_id},
        ) == 0


def test_runtime_unreadable_future_fk_fails_before_external_delete(
    persistence: BacktestPersistence,
    backtest_role_persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    """The definer inspects a future source the caller cannot SELECT."""

    admin_port = PersistenceStorageObjectWritePort(persistence)
    runtime_port = PersistenceStorageObjectWritePort(backtest_role_persistence)
    object_store = LocalObjectStore(
        tmp_path / "future-unreadable",
        bucket_name="task5-future-unreadable",
    )
    published = StorageObjectRegistrar(object_store, admin_port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type="FUTURE_UNREADABLE"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = _future_fk_schema(admin_engine, object_id=published.record.object_id)
    entered_external_delete = False
    try:
        with admin_engine.connect() as connection:
            assert not connection.scalar(
                text(
                    "SELECT has_table_privilege('idea2strategy_backtest',"
                    ":relation,'SELECT')"
                ),
                {"relation": f'{schema}."future object refs"'},
            )

        with (
            pytest.raises(ObjectStoreConflict, match="future object refs"),
            runtime_port.cleanup_batch((published.record,)),
        ):
            entered_external_delete = True

        assert entered_external_delete is False
        assert object_store.preflight_delete(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )
        assert object_row(admin_engine, published.record.object_id)["object_key"] == (
            published.record.object_key
        )
    finally:
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(admin_port, object_store, published.record)


def test_storage_row_delete_is_completed_before_external_delete(
    persistence: BacktestPersistence,
    backtest_role_persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    """A target-side delete rejection cannot arrive after bytes are gone."""

    admin_port = PersistenceStorageObjectWritePort(persistence)
    runtime_port = PersistenceStorageObjectWritePort(backtest_role_persistence)
    object_store = LocalObjectStore(
        tmp_path / "delete-trigger",
        bucket_name="task5-delete-trigger",
    )
    published = StorageObjectRegistrar(object_store, admin_port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type="DELETE_TRIGGER"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = f"task5_fk_{uuid.uuid4().hex}"
    entered_external_delete = False
    try:
        with admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            connection.execute(
                text(
                    f'CREATE FUNCTION "{schema}".reject_candidate_delete() '
                    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    "RAISE EXCEPTION 'task5 delete rejected'; END $$"
                )
            )
            connection.execute(
                text(
                    f'CREATE TRIGGER "task5 reject {uuid.uuid4().hex}" '
                    "BEFORE DELETE ON storage.objects FOR EACH ROW "
                    f"WHEN (OLD.id = '{published.record.object_id}') "
                    f'EXECUTE FUNCTION "{schema}".reject_candidate_delete()'
                )
            )

        with (
            pytest.raises(ObjectStoreConflict, match="delete rejected"),
            runtime_port.cleanup_batch((published.record,)),
        ):
            entered_external_delete = True
            object_store.delete_if_matches(
                published.record.object_key,
                published.record.content_hash,
                published.record.provider_version_id,
            )

        assert entered_external_delete is False
        assert object_store.preflight_delete(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )
        assert object_row(admin_engine, published.record.object_id)["object_key"] == (
            published.record.object_key
        )
    finally:
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(admin_port, object_store, published.record)


def test_cleanup_capability_refuses_an_unowned_object_namespace(
    persistence: BacktestPersistence,
    backtest_role_persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    """EXECUTE is not a disguised grant to delete arbitrary storage rows."""

    admin_port = PersistenceStorageObjectWritePort(persistence)
    runtime_port = PersistenceStorageObjectWritePort(backtest_role_persistence)
    object_store = LocalObjectStore(
        tmp_path / "unowned",
        bucket_name="task5-unowned",
    )
    published = StorageObjectRegistrar(object_store, admin_port).publish(
        object_id=uuid.uuid4(),
        object_key=f"pipeline-owned/{uuid.uuid4()}/{hashlib.sha256(BODY).hexdigest()}.parquet",
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    entered_external_delete = False
    try:
        with (
            pytest.raises(ObjectStoreConflict, match="canonical backtest"),
            runtime_port.cleanup_batch((published.record,)),
        ):
            entered_external_delete = True

        assert entered_external_delete is False
        assert object_store.preflight_delete(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )
        assert object_row(admin_engine, published.record.object_id)["object_key"] == (
            published.record.object_key
        )
    finally:
        with admin_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM storage.objects WHERE id=:id"),
                {"id": published.record.object_id},
            )
        object_store.delete_if_matches(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )


def test_future_storage_object_fk_blocks_every_external_delete(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    s3: Any,
    bucket: str,
) -> None:
    """A provider-owned FK added later is a fence without a code release here."""

    port = PersistenceStorageObjectWritePort(persistence)
    object_store = S3ObjectStore(bucket, client=s3)
    object_id = uuid.uuid4()
    key = _canonical_cleanup_key(record_type="FUTURE_FK")
    published = StorageObjectRegistrar(object_store, port).publish(
        object_id=object_id,
        object_key=key,
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=datetime(2024, 1, 2, 14, 30, tzinfo=UTC),
        period_end=datetime(2024, 1, 2, 14, 50, tzinfo=UTC),
        created_at=T0,
        verified_at=T0,
        expected_content_hash=hashlib.sha256(BODY).hexdigest(),
    )
    schema = _future_fk_schema(admin_engine, object_id=object_id)
    entered_external_delete = False
    caught: BaseException | None = None
    survived_exactly = False
    try:
        try:
            with port.cleanup_batch((published.record,)):
                entered_external_delete = True
                object_store.delete_if_matches(
                    published.record.object_key,
                    published.record.content_hash,
                    published.record.provider_version_id,
                )
        except BaseException as exc:  # asserted below as the typed boundary error
            caught = exc
        survived_exactly = object_store.preflight_delete(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )
        assert (
            type(caught),
            entered_external_delete,
            survived_exactly,
        ) == (ObjectStoreConflict, False, True)
    finally:
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(port, object_store, published.record)


def test_cleanup_table_lock_allows_an_unrelated_storage_object_insert(
    persistence: BacktestPersistence,
    tmp_path: Path,
) -> None:
    """The catalog fence must not stop ordinary storage publication traffic."""

    port = PersistenceStorageObjectWritePort(persistence)
    candidate_store = LocalObjectStore(tmp_path / "candidate", bucket_name="task5-candidate")
    unrelated_store = LocalObjectStore(tmp_path / "unrelated", bucket_name="task5-unrelated")
    candidate = StorageObjectRegistrar(candidate_store, port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type="UNRELATED_DML_CANDIDATE"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    unrelated_id = uuid.uuid4()

    def publish_unrelated() -> Any:
        return StorageObjectRegistrar(unrelated_store, port).publish(
            object_id=unrelated_id,
            object_key=_canonical_cleanup_key(
                body=OTHER_BODY,
                record_type="UNRELATED_DML_OTHER",
            ),
            data=OTHER_BODY,
            schema_version="1.0.0",
            row_count=3,
            period_start=T0,
            period_end=T0,
            created_at=T0,
            verified_at=T0,
        )

    with (
        ThreadPoolExecutor(max_workers=1) as pool,
        port.cleanup_batch((candidate.record,)) as locked,
    ):
        assert locked == (candidate.record,)
        unrelated = pool.submit(publish_unrelated).result(timeout=10)

    assert port.find(candidate.record.object_id) is None
    assert port.find(unrelated_id) == unrelated.record
    _cleanup_owned_object(port, candidate_store, candidate.record)
    _cleanup_owned_object(port, unrelated_store, unrelated.record)


def _blocked_relation_lock(
    admin_engine: Engine,
    *,
    application_name: str,
) -> str | None:
    with admin_engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT namespace.nspname || '.' || relation.relname || ':' || "
                "locks.mode FROM pg_locks locks "
                "JOIN pg_stat_activity activity ON activity.pid=locks.pid "
                "JOIN pg_class relation ON relation.oid=locks.relation "
                "JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace "
                "WHERE activity.application_name=:application_name "
                "AND NOT locks.granted "
                "ORDER BY namespace.nspname,relation.relname,locks.mode LIMIT 1"
            ),
            {"application_name": application_name},
        ).scalar_one_or_none()


def _blocked_backend_lock(
    admin_engine: Engine,
    *,
    application_name: str,
) -> str | None:
    with admin_engine.connect() as connection:
        return connection.execute(
            text(
                "SELECT locks.locktype || ':' || locks.mode FROM pg_locks locks "
                "JOIN pg_stat_activity activity ON activity.pid=locks.pid "
                "WHERE activity.application_name=:application_name "
                "AND NOT locks.granted ORDER BY locks.locktype,locks.mode LIMIT 1"
            ),
            {"application_name": application_name},
        ).scalar_one_or_none()


@pytest.mark.parametrize("operation", ["add", "drop", "rename"])
def test_inverse_fk_ddl_order_is_deadlock_free_and_fails_before_external_delete(
    postgres_url: str,
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
    operation: str,
) -> None:
    """DDL may own the source before cleanup asks for its target lock."""

    admin_port = PersistenceStorageObjectWritePort(persistence)
    object_store = LocalObjectStore(
        tmp_path / f"inverse-{operation}",
        bucket_name=f"task5-inverse-{operation}",
    )
    published = StorageObjectRegistrar(object_store, admin_port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type=f"INVERSE_{operation.upper()}"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = _future_fk_schema(admin_engine)
    if operation == "add":
        with admin_engine.begin() as connection:
            connection.execute(
                text(
                    f'ALTER TABLE "{schema}"."future object refs" '
                    'ADD COLUMN "second storage object id" uuid'
                )
            )
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'ADD CONSTRAINT "second future storage object fk" '
            'FOREIGN KEY ("second storage object id") REFERENCES storage.objects(id)'
        )
    elif operation == "drop":
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'DROP CONSTRAINT "future storage object fk"'
        )
    else:
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'RENAME CONSTRAINT "future storage object fk" '
            'TO "renamed future storage fk"'
        )

    cleanup_name = f"task5-inverse-cleanup-{operation}-{uuid.uuid4().hex}"
    cleanup_engine = create_backtest_engine(
        postgres_url,
        application_name=cleanup_name,
        connect_args={
            "options": "-c deadlock_timeout=100ms -c lock_timeout=5s",
        },
    )
    cleanup_port = PersistenceStorageObjectWritePort(
        BacktestPersistence(cleanup_engine)
    )
    source_locked = Event()
    allow_ddl_target = Event()
    entered_external_delete = False

    def mutate_after_source_lock() -> None:
        with admin_engine.begin() as connection:
            connection.execute(text("SET LOCAL deadlock_timeout='100ms'"))
            connection.execute(text("SET LOCAL statement_timeout='10s'"))
            connection.execute(
                text(
                    f'LOCK TABLE "{schema}"."future object refs" '
                    "IN ACCESS EXCLUSIVE MODE"
                )
            )
            source_locked.set()
            assert allow_ddl_target.wait(timeout=10)
            connection.execute(text(ddl))

    def cleanup() -> None:
        nonlocal entered_external_delete
        with cleanup_port.cleanup_batch((published.record,)):
            entered_external_delete = True

    pool = ThreadPoolExecutor(max_workers=2)
    ddl_future = pool.submit(mutate_after_source_lock)
    cleanup_future = None
    try:
        assert source_locked.wait(timeout=10), "DDL did not acquire its source lock"
        cleanup_future = pool.submit(cleanup)
        blocked = wait_until(
            lambda: _blocked_relation_lock(
                admin_engine,
                application_name=cleanup_name,
            ),
            description="cleanup to wait on the DDL-owned source before target locking",
            timeout_seconds=10,
        )
        assert blocked == f"{schema}.future object refs:AccessShareLock"

        allow_ddl_target.set()
        ddl_future.result(timeout=10)
        with pytest.raises(ObjectStoreConflict, match="catalog changed"):
            cleanup_future.result(timeout=10)
        assert entered_external_delete is False
        assert object_store.preflight_delete(
            published.record.object_key,
            published.record.content_hash,
            published.record.provider_version_id,
        )
    finally:
        allow_ddl_target.set()
        pool.shutdown(wait=True)
        cleanup_engine.dispose()
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(admin_port, object_store, published.record)


def test_unknown_source_fk_ddl_linearizes_after_cleanup_without_deadlock(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    """A source absent from the first fingerprint cannot form an inverse cycle."""

    port = PersistenceStorageObjectWritePort(persistence)
    object_store = LocalObjectStore(
        tmp_path / "unknown-source",
        bucket_name="task5-unknown-source",
    )
    published = StorageObjectRegistrar(object_store, port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type="UNKNOWN_SOURCE"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = _future_fk_schema(admin_engine, with_constraint=False)
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                f'INSERT INTO "{schema}"."future object refs" '
                '("storage object id") VALUES (:object_id)'
            ),
            {"object_id": published.record.object_id},
        )

    ddl_name = f"task5-unknown-source-ddl-{uuid.uuid4().hex}"
    source_locked = Event()
    allow_ddl_target = Event()
    cleanup_reached_external_boundary = Event()
    allow_cleanup_commit = Event()

    def add_unknown_fk() -> None:
        with admin_engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('application_name', :name, true)"),
                {"name": ddl_name},
            )
            connection.execute(text("SET LOCAL statement_timeout='10s'"))
            connection.execute(
                text(
                    f'LOCK TABLE "{schema}"."future object refs" '
                    "IN ACCESS EXCLUSIVE MODE"
                )
            )
            source_locked.set()
            assert allow_ddl_target.wait(timeout=10)
            connection.execute(
                text(
                    f'ALTER TABLE "{schema}"."future object refs" '
                    'ADD CONSTRAINT "new unknown storage object fk" '
                    'FOREIGN KEY ("storage object id") REFERENCES storage.objects(id)'
                )
            )

    def cleanup() -> None:
        with port.cleanup_batch((published.record,)) as locked:
            assert locked == (published.record,)
            cleanup_reached_external_boundary.set()
            assert allow_cleanup_commit.wait(timeout=10)
            assert object_store.delete_if_matches(
                published.record.object_key,
                published.record.content_hash,
                published.record.provider_version_id,
            )

    pool = ThreadPoolExecutor(max_workers=2)
    ddl_future = pool.submit(add_unknown_fk)
    cleanup_future = None
    try:
        assert source_locked.wait(timeout=10), "DDL did not acquire the unknown source lock"
        cleanup_future = pool.submit(cleanup)
        assert cleanup_reached_external_boundary.wait(timeout=10), (
            "cleanup waited on a source absent from its initial FK fingerprint"
        )

        allow_ddl_target.set()
        blocked = wait_until(
            lambda: _blocked_relation_lock(
                admin_engine,
                application_name=ddl_name,
            ),
            description="unknown-source ADD FK to wait on cleanup's target fence",
            timeout_seconds=10,
        )
        assert blocked == "storage.objects:ShareRowExclusiveLock"

        allow_cleanup_commit.set()
        cleanup_future.result(timeout=10)
        with pytest.raises(IntegrityError):
            ddl_future.result(timeout=10)
        assert port.find(published.record.object_id) is None
        assert not object_store.exists(published.record.object_key)
    finally:
        allow_ddl_target.set()
        allow_cleanup_commit.set()
        pool.shutdown(wait=True)
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(port, object_store, published.record)


@pytest.mark.parametrize(
    "operation",
    ["add", "drop", "rename"],
)
def test_cleanup_serializes_concurrent_storage_fk_ddl(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
    operation: str,
) -> None:
    """FK catalog shape cannot change between enumeration and external deletion."""

    port = PersistenceStorageObjectWritePort(persistence)
    object_store = LocalObjectStore(tmp_path / operation, bucket_name=f"task5-ddl-{operation}")
    published = StorageObjectRegistrar(object_store, port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type=f"DDL_{operation.upper()}"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = _future_fk_schema(
        admin_engine,
        with_constraint=operation != "add",
    )
    application_name = f"task5-fk-ddl-{operation}-{uuid.uuid4().hex}"
    ready = Event()
    if operation == "add":
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'ADD CONSTRAINT "future storage object fk" '
            'FOREIGN KEY ("storage object id") REFERENCES storage.objects(id)'
        )
    elif operation == "drop":
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'DROP CONSTRAINT "future storage object fk"'
        )
    else:
        ddl = (
            f'ALTER TABLE "{schema}"."future object refs" '
            'RENAME CONSTRAINT "future storage object fk" '
            'TO "renamed future storage fk"'
        )

    def mutate_catalog() -> None:
        with admin_engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('application_name', :name, true)"),
                {"name": application_name},
            )
            ready.set()
            connection.execute(text(ddl))

    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with port.cleanup_batch((published.record,)):
            future = pool.submit(mutate_catalog)
            assert ready.wait(timeout=10), "DDL worker never reached its boundary"

            def ddl_state() -> str | None:
                if future is not None and future.done():
                    return "completed"
                mode = _blocked_relation_lock(
                    admin_engine,
                    application_name=application_name,
                )
                return None if mode is None else f"blocked:{mode}"

            state = wait_until(
                ddl_state,
                description=f"{operation} FK DDL to block on cleanup catalog fence",
                timeout_seconds=10,
            )
            assert state.startswith("blocked:"), state
            assert state.split(":", 2)[1] in {
                "storage.objects",
                f"{schema}.future object refs",
            }
        assert future is not None
        future.result(timeout=10)
    finally:
        pool.shutdown(wait=True)
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(port, object_store, published.record)


def test_candidate_row_lock_serializes_a_concurrent_future_fk_insert(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
) -> None:
    """A new reference cannot land after reference scan but before unregister."""

    port = PersistenceStorageObjectWritePort(persistence)
    object_store = LocalObjectStore(tmp_path / "fk-insert", bucket_name="task5-fk-insert")
    published = StorageObjectRegistrar(object_store, port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type="CONCURRENT_FK_INSERT"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = _future_fk_schema(admin_engine)
    application_name = f"task5-fk-insert-{uuid.uuid4().hex}"
    ready = Event()

    def insert_reference() -> None:
        with admin_engine.begin() as connection:
            connection.execute(
                text("SELECT set_config('application_name', :name, true)"),
                {"name": application_name},
            )
            ready.set()
            connection.execute(
                text(
                    f'INSERT INTO "{schema}"."future object refs" '
                    '("storage object id") VALUES (:object_id)'
                ),
                {"object_id": published.record.object_id},
            )

    pool = ThreadPoolExecutor(max_workers=1)
    future = None
    try:
        with port.cleanup_batch((published.record,)):
            future = pool.submit(insert_reference)
            assert ready.wait(timeout=10), "reference writer never reached its boundary"

            def insert_state() -> str | None:
                if future is not None and future.done():
                    return "completed"
                mode = _blocked_backend_lock(
                    admin_engine,
                    application_name=application_name,
                )
                return None if mode is None else f"blocked:{mode}"

            state = wait_until(
                insert_state,
                description="future FK insert to wait on candidate row fence",
                timeout_seconds=10,
            )
            assert state.startswith("blocked:transactionid:"), state
        assert future is not None
        with pytest.raises(IntegrityError):
            future.result(timeout=10)
    finally:
        pool.shutdown(wait=True)
        _drop_test_schema(admin_engine, schema)
        _cleanup_owned_object(port, object_store, published.record)


@pytest.mark.parametrize(
    "shape",
    ["composite", "partition", "inheritance"],
)
def test_cleanup_fails_closed_for_unsupported_future_fk_shapes(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    tmp_path: Path,
    shape: str,
) -> None:
    port = PersistenceStorageObjectWritePort(persistence)
    object_store = LocalObjectStore(tmp_path / shape, bucket_name=f"task5-shape-{shape}")
    published = StorageObjectRegistrar(object_store, port).publish(
        object_id=uuid.uuid4(),
        object_key=_canonical_cleanup_key(record_type=f"SHAPE_{shape.upper()}"),
        data=BODY,
        schema_version="1.0.0",
        row_count=3,
        period_start=T0,
        period_end=T0,
        created_at=T0,
        verified_at=T0,
    )
    schema = f"task5_fk_{uuid.uuid4().hex}"
    target_constraint = f"task5_composite_{uuid.uuid4().hex}"
    try:
        with admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            if shape == "composite":
                connection.execute(
                    text(
                        "ALTER TABLE storage.objects "
                        f'ADD CONSTRAINT "{target_constraint}" '
                        "UNIQUE (id, storage_provider)"
                    )
                )
                connection.execute(
                    text(
                        f'CREATE TABLE "{schema}"."composite refs" ('
                        '"storage object id" uuid, "provider" varchar(32), '
                        'CONSTRAINT "composite storage fk" FOREIGN KEY '
                        '("storage object id", "provider") '
                        "REFERENCES storage.objects(id, storage_provider))"
                    )
                )
            elif shape == "partition":
                connection.execute(
                    text(
                        f'CREATE TABLE "{schema}"."partition refs" ('
                        '"storage object id" uuid REFERENCES storage.objects(id), '
                        '"shard" integer NOT NULL) PARTITION BY HASH ("shard")'
                    )
                )
            else:
                connection.execute(
                    text(
                        f'CREATE TABLE "{schema}"."parent refs" ('
                        '"storage object id" uuid REFERENCES storage.objects(id))'
                    )
                )
                connection.execute(
                        text(
                            f'CREATE TABLE "{schema}"."child refs" ('
                            f'"marker" integer) INHERITS ("{schema}"."parent refs")'
                        )
                    )

        entered_external_delete = False
        with (
            pytest.raises(ObjectStoreConflict, match="unsupported"),
            port.cleanup_batch((published.record,)),
        ):
            entered_external_delete = True
        assert entered_external_delete is False
        assert port.find(published.record.object_id) == published.record
    finally:
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            if shape == "composite":
                connection.execute(
                    text(
                        "ALTER TABLE storage.objects "
                        f'DROP CONSTRAINT IF EXISTS "{target_constraint}"'
                    )
                )
        _cleanup_owned_object(port, object_store, published.record)
