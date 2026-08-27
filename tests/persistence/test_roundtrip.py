"""Insert/read round-trip for every canonical `backtest.*` table, plus storage reads."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest

from backtest_engine.persistence import (
    BacktestPersistence,
    ObjectStatus,
    RowNotFound,
    RunStatus,
    WorkStatus,
)

from .support import (
    ACCOUNT_ID,
    AVAILABLE_OBJECT_ID,
    DATASET_MANIFEST_ID,
    FEATURE_MATERIALIZATION_ID,
    OTHER_ACCOUNT_ID,
    STAGED_OBJECT_ID,
    make_attempt,
    make_detail_manifest,
    make_failure_count,
    make_input_bundle,
    make_input_dataset,
    make_input_feature,
    make_monthly_summary,
    make_performance_summary,
    make_run,
)


pytestmark = pytest.mark.docker


def test_run_round_trip(persistence: BacktestPersistence) -> None:
    row = make_run(idempotency_key="ROUNDTRIP:run")

    with persistence.unit_of_work() as uow:
        stored, created = uow.runs.accept(row)

    assert created is True
    assert stored == row

    with persistence.unit_of_work() as uow:
        assert uow.runs.get(row.id) == row


def test_run_attempt_round_trip(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:attempt")
    attempt = make_attempt(run.id)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored, created = uow.attempts.claim(attempt)

    assert created is True
    assert stored == attempt

    with persistence.unit_of_work() as uow:
        assert uow.attempts.list_for_run(run.id) == (attempt,)
        assert uow.attempts.next_attempt_number(run.id) == 2


def test_input_bundle_and_children_round_trip(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:inputs")
    bundle = make_input_bundle(run.id)
    dataset = make_input_dataset(bundle.id)
    feature = make_input_feature(bundle.id)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored, created = uow.inputs.lock(bundle, [dataset], [feature])

    assert created is True
    assert stored == bundle

    with persistence.unit_of_work() as uow:
        assert uow.inputs.get_by_run(run.id) == bundle
        assert uow.inputs.datasets_for(bundle.id) == (dataset,)
        assert uow.inputs.features_for(bundle.id) == (feature,)
        assert dataset.dataset_manifest_id == DATASET_MANIFEST_ID
        assert feature.feature_materialization_id == FEATURE_MATERIALIZATION_ID


def test_monthly_summary_and_failure_counts_round_trip(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:monthly")
    summary = make_monthly_summary(run.id)
    counts = (
        make_failure_count(summary.id),
        make_failure_count(summary.id, first_failure_condition_key="VOLUME_TOO_LOW", occurrence_count=4),
    )

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored, created = uow.monthly.insert_summary(summary)
        inserted = uow.monthly.insert_failure_counts(counts)

    assert created is True
    assert stored == summary
    assert inserted == 2

    with persistence.unit_of_work() as uow:
        assert uow.monthly.list_for_run(run.id) == (summary,)
        assert set(uow.monthly.failure_counts_for(summary.id)) == set(counts)


def test_monthly_summary_keeps_all_six_canonical_counters(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:counters")
    summary = make_monthly_summary(
        run.id,
        evaluation_count=1,
        active_branch_count=2,
        trade_event_count=3,
        data_gap_count=4,
        triggered_count=5,
        rejected_count=6,
    )

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.monthly.insert_summary(summary)

    with persistence.unit_of_work() as uow:
        stored = uow.monthly.list_for_run(run.id)[0]

    assert (
        stored.evaluation_count,
        stored.active_branch_count,
        stored.trade_event_count,
        stored.data_gap_count,
        stored.triggered_count,
        stored.rejected_count,
    ) == (1, 2, 3, 4, 5, 6)
    assert stored.summary_document == {"orderIntents": 112, "skippedTriggers": 3}


def test_performance_summary_round_trip(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:performance")
    summary = make_performance_summary(run.id)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored, created = uow.performance.insert(summary)

    assert created is True
    assert stored == summary

    with persistence.unit_of_work() as uow:
        read = uow.performance.get(run.id)

    assert read.metrics_document["totalReturnPct"] == 12.64
    assert read.metric_catalog_version == "1.0.0"


def test_detail_manifest_round_trip(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:manifest")
    manifest = make_detail_manifest(run.id)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored, created = uow.manifests.insert(manifest)

    assert created is True
    assert stored == manifest
    assert stored.week_start_date == date(2024, 3, 4)
    assert stored.week_start_date.weekday() == 0

    with persistence.unit_of_work() as uow:
        assert uow.manifests.list_for_run(run.id) == (manifest,)
        assert uow.manifests.find_by_object(AVAILABLE_OBJECT_ID) == manifest


def test_storage_objects_are_readable(persistence: BacktestPersistence) -> None:
    with persistence.unit_of_work() as uow:
        available = uow.objects.get(AVAILABLE_OBJECT_ID)
        staged = uow.objects.get(STAGED_OBJECT_ID)
        by_key = uow.objects.find_by_key(
            storage_provider=available.storage_provider,
            bucket_name=available.bucket_name,
            object_key=available.object_key,
            provider_version_id=available.provider_version_id,
        )

    assert available.status is ObjectStatus.AVAILABLE
    assert available.compression_codec == "UNCOMPRESSED"
    assert available.file_format == "PARQUET"
    assert available.byte_size == 4096
    assert staged.status is ObjectStatus.STAGED
    assert by_key == available


def test_storage_object_reader_has_no_write_methods() -> None:
    from backtest_engine.persistence import StorageObjectReader

    public = {name for name in dir(StorageObjectReader) if not name.startswith("_")}

    assert public == {
        "connection",
        "find",
        "find_by_key",
        "find_result_snapshot_object",
        "get",
        "list_by_ids",
        "require_available",
    }


def test_require_available_rejects_a_staged_object(persistence: BacktestPersistence) -> None:
    with persistence.unit_of_work() as uow, pytest.raises(RowNotFound, match="not AVAILABLE"):
        uow.objects.require_available([AVAILABLE_OBJECT_ID, STAGED_OBJECT_ID])


def test_require_available_rejects_a_missing_object(persistence: BacktestPersistence) -> None:
    missing = uuid4()

    with persistence.unit_of_work() as uow, pytest.raises(RowNotFound, match="do not exist"):
        uow.objects.require_available([missing])


def test_owner_scoped_read_hides_foreign_runs(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:owner")

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)

    with persistence.unit_of_work() as uow:
        assert uow.runs.get_owned(ACCOUNT_ID, run.id) == run
        with pytest.raises(RowNotFound):
            uow.runs.get_owned(OTHER_ACCOUNT_ID, run.id)
        assert uow.runs.list_by_owner(OTHER_ACCOUNT_ID) == ()
        assert uow.runs.list_by_owner(ACCOUNT_ID) == (run,)


def test_owner_soft_delete_cancels_queued_run_and_preserves_internal_evidence(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:soft-delete")
    requested_at = datetime(2026, 8, 27, 9, 0, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        deleted = uow.runs.request_deletion(run.id, requested_at=requested_at)

    assert deleted.status is RunStatus.CANCELLED
    assert deleted.cancellation_reason_code == "USER_DELETED"
    assert deleted.deletion_requested_at == requested_at
    assert deleted.deleted_at == requested_at

    with persistence.unit_of_work() as uow:
        assert uow.runs.get(run.id).deleted_at == requested_at
        with pytest.raises(RowNotFound):
            uow.runs.get_owned(ACCOUNT_ID, run.id)
        assert uow.runs.list_by_owner(ACCOUNT_ID) == ()


def test_owner_soft_delete_waits_for_running_worker_then_hides_terminal_evidence(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:running-soft-delete")
    started_at = datetime(2026, 8, 27, 9, 0, tzinfo=UTC)
    requested_at = datetime(2026, 8, 27, 9, 1, tzinfo=UTC)
    cancelled_at = datetime(2026, 8, 27, 9, 2, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.runs.mark_running(run.id, started_at)
        pending = uow.runs.request_deletion(run.id, requested_at=requested_at)

    assert pending.status is RunStatus.RUNNING
    assert pending.cancellation_requested_at == requested_at
    assert pending.deletion_requested_at == requested_at
    assert pending.deleted_at is None

    with persistence.unit_of_work() as uow:
        cancelled = uow.runs.mark_cancelled(run.id, cancelled_at, "USER_DELETED")

    assert cancelled.status is RunStatus.CANCELLED
    assert cancelled.deleted_at == cancelled_at
    with persistence.unit_of_work() as uow:
        with pytest.raises(RowNotFound):
            uow.runs.get_owned(ACCOUNT_ID, run.id)
        assert uow.runs.get(run.id).deleted_at == cancelled_at


def test_lifecycle_transitions_use_the_canonical_completed_label(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:lifecycle")
    started = datetime(2026, 3, 2, 14, 0, 1, tzinfo=UTC)
    finished = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        running = uow.runs.mark_running(run.id, started)
        completed = uow.runs.mark_completed(run.id, finished, "c" * 64)

    assert running.status is RunStatus.RUNNING
    assert running.started_at == started
    assert completed.status is RunStatus.COMPLETED
    assert completed.status.value == "COMPLETED"
    assert completed.completed_at == finished
    assert completed.result_hash == "c" * 64


def test_attempt_completion_records_the_operations_work_status(
    persistence: BacktestPersistence,
) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:attempt-complete")
    attempt = make_attempt(run.id)
    finished = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        uow.attempts.claim(attempt)
        done = uow.attempts.complete(attempt.worker_execution_key, status=WorkStatus.SUCCEEDED, completed_at=finished)

    assert done.status is WorkStatus.SUCCEEDED
    assert done.completed_at == finished
    assert done.failure_code is None


def test_initial_cash_is_a_decimal_not_a_float(persistence: BacktestPersistence) -> None:
    run = make_run(idempotency_key="ROUNDTRIP:decimal")

    with persistence.unit_of_work() as uow:
        uow.runs.accept(run)
        stored = uow.runs.get(run.id)

    assert isinstance(stored.initial_cash_amount, Decimal)
    assert stored.initial_cash_amount == Decimal("100000.00000000")
