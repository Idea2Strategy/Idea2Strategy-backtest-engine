"""Row builders for the persistence tests.

The identifiers below are the ones `db/migration-contributions/fixtures/
backtest_reference_seed.sql.fixture` inserts, so every foreign key in a built row
resolves against a real upstream row.

Builders take explicit values and never recompute anything the production code
computes; determinism assertions in the tests use literals.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from backtest_engine.persistence import (
    DetailManifestRow,
    FailureConditionCountRow,
    InputBundleRow,
    InputDatasetRow,
    InputFeatureMaterializationRow,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunRow,
    RunStatus,
    WorkStatus,
)


ACCOUNT_ID = UUID("00000000-0000-4000-8000-0000000000a1")
OTHER_ACCOUNT_ID = UUID("00000000-0000-4000-8000-0000000000a9")
BOT_ID = UUID("00000000-0000-4000-8000-0000000000b1")
FEE_POLICY_ID = UUID("00000000-0000-4000-8000-0000000000f1")
BUFFER_POLICY_ID = UUID("00000000-0000-4000-8000-0000000000f2")
DATASET_MANIFEST_ID = UUID("00000000-0000-4000-8000-0000000000d1")
FEATURE_MATERIALIZATION_ID = UUID("00000000-0000-4000-8000-0000000000e1")
AVAILABLE_OBJECT_ID = UUID("00000000-0000-4000-8000-0000000000c1")
STAGED_OBJECT_ID = UUID("00000000-0000-4000-8000-0000000000c2")

QUEUED_AT = datetime(2026, 3, 2, 14, 0, tzinfo=UTC)
#: 2024-03-04 is a Monday: the canonical ET detail partition boundary.
WEEK_START = date(2024, 3, 4)

HASH_A = "a" * 64
HASH_B = "b" * 64


def make_run(
    *,
    run_id: UUID | None = None,
    idempotency_key: str = "BOT_CREATE:integration-1",
    owner_account_id: UUID = ACCOUNT_ID,
    status: RunStatus = RunStatus.QUEUED,
    initial_cash_amount: Decimal = Decimal("100000.00000000"),
    configuration_hash: str = HASH_A,
    slippage_rate_bps: int = 5,
    **overrides: Any,
) -> RunRow:
    values: dict[str, Any] = {
        "id": run_id or uuid4(),
        "bot_id": BOT_ID,
        "owner_account_id": owner_account_id,
        "configuration_hash": configuration_hash,
        "status": status,
        "evaluation_start": date(2024, 1, 1),
        "evaluation_end": date(2024, 12, 31),
        "initial_cash_amount": initial_cash_amount,
        "market_rules_version": "1.0.0",
        "accounting_rules_version": "1.0.0",
        "precision_rules_version": "precision:1.0.0",
        "fee_policy_id": FEE_POLICY_ID,
        "slippage_rate_bps": slippage_rate_bps,
        "buying_power_buffer_policy_id": BUFFER_POLICY_ID,
        "idempotency_key": idempotency_key,
        "queued_at": QUEUED_AT,
    }
    values.update(overrides)
    return RunRow(**values)


def make_attempt(
    run_id: UUID,
    *,
    attempt_number: int = 1,
    worker_execution_key: str | None = None,
    status: WorkStatus = WorkStatus.RUNNING,
    started_at: datetime | None = None,
    attempt_id: UUID | None = None,
) -> RunAttemptRow:
    return RunAttemptRow(
        id=attempt_id or uuid4(),
        run_id=run_id,
        attempt_number=attempt_number,
        worker_execution_key=(worker_execution_key or f"BACKTEST_RUN_{run_id}_ATTEMPT_{attempt_number}"),
        status=status,
        started_at=started_at or datetime(2026, 3, 2, 14, 0, 1, tzinfo=UTC),
    )


def make_input_bundle(run_id: UUID, *, bundle_id: UUID | None = None, bundle_hash: str = HASH_A) -> InputBundleRow:
    return InputBundleRow(
        id=bundle_id or uuid4(),
        run_id=run_id,
        bundle_hash=bundle_hash,
        as_of_at=QUEUED_AT,
        locked_at=QUEUED_AT,
    )


def make_input_dataset(bundle_id: UUID, *, purpose_code: str = "MARKET_INPUT") -> InputDatasetRow:
    return InputDatasetRow(
        input_bundle_id=bundle_id,
        dataset_manifest_id=DATASET_MANIFEST_ID,
        purpose_code=purpose_code,
        locked_dataset_hash=HASH_A,
    )


def make_input_feature(bundle_id: UUID) -> InputFeatureMaterializationRow:
    return InputFeatureMaterializationRow(
        input_bundle_id=bundle_id,
        feature_materialization_id=FEATURE_MATERIALIZATION_ID,
        locked_result_hash=HASH_B,
    )


def make_monthly_summary(
    run_id: UUID,
    *,
    summary_id: UUID | None = None,
    et_year_month: str = "2024-03",
    summary_hash: str = HASH_A,
    **counters: Any,
) -> MonthlyJudgmentSummaryRow:
    values: dict[str, Any] = {
        "evaluation_count": 8190,
        "active_branch_count": 412,
        "trade_event_count": 112,
        "data_gap_count": 3,
        "triggered_count": 112,
        "rejected_count": 37,
    }
    values.update(counters)
    return MonthlyJudgmentSummaryRow(
        id=summary_id or uuid4(),
        run_id=run_id,
        et_year_month=et_year_month,
        summary_document={"orderIntents": 112, "skippedTriggers": 3},
        summary_hash=summary_hash,
        **values,
    )


def make_failure_count(
    monthly_summary_id: UUID,
    *,
    flow_or_branch_key: str = "FLOW_MOMENTUM_ENTRY",
    first_failure_condition_key: str = "PRICE_BELOW_THRESHOLD",
    occurrence_count: int = 37,
) -> FailureConditionCountRow:
    return FailureConditionCountRow(
        id=uuid4(),
        monthly_summary_id=monthly_summary_id,
        flow_or_branch_key=flow_or_branch_key,
        first_failure_condition_key=first_failure_condition_key,
        occurrence_count=occurrence_count,
    )


def make_performance_summary(run_id: UUID, *, result_hash: str = HASH_A) -> PerformanceSummaryRow:
    return PerformanceSummaryRow(
        run_id=run_id,
        metric_catalog_version="1.0.0",
        metrics_document={
            "totalReturnPct": 12.64,
            "maxDrawdownPct": -3.21,
            "sharpe": 1.42,
            "winRatePct": 58.7,
        },
        calculation_rules_version="1.0.0",
        source_set_hash=HASH_B,
        input_hash=HASH_B,
        result_hash=result_hash,
        calculated_at=datetime(2026, 3, 2, 14, 5, tzinfo=UTC),
    )


def make_detail_manifest(
    run_id: UUID,
    *,
    manifest_id: UUID | None = None,
    object_id: UUID = AVAILABLE_OBJECT_ID,
    record_type: str = "ORDER_DECISIONS",
    week_start_date: date = WEEK_START,
    part_number: int = 1,
    detail_hash: str = HASH_A,
) -> DetailManifestRow:
    return DetailManifestRow(
        id=manifest_id or uuid4(),
        run_id=run_id,
        object_id=object_id,
        record_type=record_type,
        week_start_date=week_start_date,
        period_start=datetime(2024, 3, 4, 14, 30, tzinfo=UTC),
        period_end=datetime(2024, 3, 8, 21, 0, tzinfo=UTC),
        part_number=part_number,
        row_count=18420,
        schema_version="1.0.0",
        source_set_hash=HASH_B,
        supersedes_manifest_id=None,
        detail_hash=detail_hash,
        created_at=datetime(2026, 3, 2, 14, 5, tzinfo=UTC),
    )


def make_storage_object(
    *,
    object_id: UUID | None = None,
    status: Any = None,
    object_key: str | None = None,
    content_hash: str = HASH_A,
    byte_size: int = 4096,
    **overrides: Any,
) -> Any:
    """A `storage.objects` row as the object store would register it.

    Defaults to `STAGED`: spec 2.5 says an object becomes `AVAILABLE` only after
    verification, so the builder cannot hand a test a pre-published object by accident.
    """
    from backtest_engine.persistence import ObjectStatus, StorageObjectRow

    values: dict[str, Any] = {
        "id": object_id or uuid4(),
        "status": status or ObjectStatus.STAGED,
        "storage_provider": "LOCAL",
        "bucket_name": "idea2strategy-backtest",
        "object_key": object_key
        or f"backtest-results/{uuid4()}/ORDER_DECISIONS/week_start=2024-03-04/part=0001/a.parquet",
        "provider_version_id": "v1",
        "content_hash": content_hash,
        "byte_size": byte_size,
        "file_format": "PARQUET",
        "compression_codec": "UNCOMPRESSED",
        "media_type": "application/vnd.apache.parquet",
        "schema_version": "1.0.0",
        "row_count": 128,
        "period_start": datetime(2024, 3, 4, 14, 30, tzinfo=UTC),
        "period_end": datetime(2024, 3, 8, 21, 0, tzinfo=UTC),
        "encryption_key_ref": None,
        "retention_policy_version": "1.0.0",
        "retention_until": None,
        "legal_hold": False,
        "created_at": datetime(2026, 3, 2, 14, 5, tzinfo=UTC),
        "verified_at": None,
        "quarantined_at": None,
        "superseded_at": None,
        "deleted_at": None,
    }
    values.update(overrides)
    return StorageObjectRow(**values)
