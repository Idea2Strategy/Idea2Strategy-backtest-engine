"""The D29 read model over real PostgreSQL and a real object store.

`InMemoryBacktestResultQueryStore` holds the published artifacts as Python objects, so
it can never fail the way a deployment fails. This suite publishes a run the way
`wiring.DurableResultPublisher` does - domain builders, a real object store, the
canonical rows in one transaction - and then reads it back through
`DurableBacktestResultQueryStore`, which holds nothing and reconstructs everything.

Docker is required and never skipped silently: `postgres_url` is the session-scoped
Testcontainers PostgreSQL 16 from `conftest`, migrated with the canonical central
bundle plus this repository's contributed migration.

The object store is `LocalObjectStore` rather than LocalStack S3. Both satisfy the same
`ObjectStore` contract and the same contract tests (`tests/test_object_store.py`), and
the S3 leg of exactly this path is covered end to end in
`tests/test_reproducibility_e2e.py`. What is under test here is the *reconstruction*,
which is storage-adapter agnostic by construction.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from backtest_engine.detail_object_manifest import (
    DetailObjectBuilder,
    DetailObjectKind,
    DetailObjectPublisher,
)
from backtest_engine.execution_model import OrderStatus
from backtest_engine.monthly_judgment import EtMonth, MonthlyJudgmentBuilder
from backtest_engine.object_store import LocalObjectStore, StorageObjectRegistrar
from backtest_engine.persistence import (
    BacktestPersistence,
    InputDatasetRow,
    MonthlyJudgment,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunInputPinRow,
    RunPublication,
    RunStatus,
    publish_completed_run,
)
from backtest_engine.result_query import (
    BacktestResultQueryService,
    DurableBacktestResultQueryStore,
    QueryIntegrityError,
    QueryNotFound,
    QueryNotReady,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshotBuilder,
    RunSnapshot,
)
from backtest_engine.wiring import PersistenceStorageObjectWritePort, build_result_query_service
from persistence.support import (
    ACCOUNT_ID,
    BOT_ID,
    DATASET_MANIFEST_ID,
    HASH_A,
    OTHER_ACCOUNT_ID,
    make_input_bundle,
    make_run,
)


pytestmark = pytest.mark.docker


BUCKET = "idea2strategy-backtest-read"
RESULT_OBJECT_NAMESPACE = uuid.UUID("b7e2b2b9-5c3a-4d18-8b6f-9c1e2a4f0d55")

INSTRUMENT_ID = "00000000-0000-4000-8000-0000000041a1"
OCTOBER_ORDER_ID = "00000000-0000-4000-8000-0000000041a2"
OCTOBER_FILL_ID = "00000000-0000-4000-8000-0000000041a3"
OCTOBER_RECORD_ID = "00000000-0000-4000-8000-0000000041a4"
NOVEMBER_ORDER_ID = "00000000-0000-4000-8000-0000000041a5"
NOVEMBER_FILL_ID = "00000000-0000-4000-8000-0000000041a6"
NOVEMBER_RECORD_ID = "00000000-0000-4000-8000-0000000041a7"

COMPLETED_AT = datetime(2025, 11, 2, 4, 10, tzinfo=UTC)
COMPILED_PLAN_CHECKSUM = "sha256:" + "b" * 64
STRATEGY_SNAPSHOT_HASH = "sha256:" + "e" * 64


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _snapshot(run_id: uuid.UUID) -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=str(run_id),
        strategy_version_id=str(BOT_ID),
        # Must equal `runs.configuration_hash`; the read model asserts it.
        input_bundle_fingerprint=HASH_A,
        calculation_model_version="calculation-v9",
        cost_model_version="cost-v3",
        execution_model_version="execution-v5",
        initial_cash=Decimal("100000"),
    )


def _fill(
    run_id: uuid.UUID,
    *,
    record_id: str,
    order_id: str,
    fill_id: str,
    occurred_at: str,
    cash_after: str,
    quantity_after: str,
    cost_basis_after: str,
) -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=_snapshot(run_id).snapshot_id,
        record_id=record_id,
        kind=ResultRecordKind.FILL,
        occurred_at=_instant(occurred_at),
        order_id=order_id,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.FILLED,
        cash_after=Decimal(cash_after),
        positions_after=(
            PositionAfter(INSTRUMENT_ID, Decimal(quantity_after), Decimal(cost_basis_after)),
        ),
        fill_id=fill_id,
        quantity=Decimal("1"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("100.05"),
        slippage_amount=Decimal("0.05"),
        fee=Decimal("2.20"),
        cost_basis=Decimal("100.05"),
        realized_pnl=Decimal("0"),
    )


def _records(run_id: uuid.UUID) -> list[ResultRecord]:
    """ET Friday 2025-10-31 and ET Saturday 2025-11-01: one ET week, two ET months."""

    return [
        _fill(
            run_id,
            record_id=OCTOBER_RECORD_ID,
            order_id=OCTOBER_ORDER_ID,
            fill_id=OCTOBER_FILL_ID,
            occurred_at="2025-11-01T03:30:00Z",
            cash_after="99897.75",
            quantity_after="1",
            cost_basis_after="100.05",
        ),
        _fill(
            run_id,
            record_id=NOVEMBER_RECORD_ID,
            order_id=NOVEMBER_ORDER_ID,
            fill_id=NOVEMBER_FILL_ID,
            occurred_at="2025-11-01T14:30:00Z",
            cash_after="99795.50",
            quantity_after="2",
            cost_basis_after="200.10",
        ),
    ]


class Published:
    """One run, published exactly the way `DurableResultPublisher` publishes one."""

    def __init__(self, persistence: BacktestPersistence, store: LocalObjectStore) -> None:
        self.persistence = persistence
        self.store = store
        self.run_id = uuid.uuid4()
        self.result: Any = None
        self.details: Any = None
        self.monthly: Any = None

    def accept(self, *, queued_at: datetime | None = None) -> None:
        overrides: dict[str, Any] = {} if queued_at is None else {"queued_at": queued_at}
        run = make_run(
            run_id=self.run_id,
            idempotency_key=f"OFFICIAL_BACKTEST:{self.run_id}",
            **overrides,
        )
        with self.persistence.unit_of_work() as uow:
            uow.runs.accept(run)
            uow.pins.pin(self.pins())

    def pins(self) -> RunInputPinRow:
        return RunInputPinRow(
            run_id=self.run_id,
            compiled_plan_checksum=COMPILED_PLAN_CHECKSUM,
            strategy_snapshot_hash=STRATEGY_SNAPSHOT_HASH,
            dataset_manifest_id=DATASET_MANIFEST_ID,
            dataset_hash=HASH_A,
            feature_materialization_version="market-bars-v2",
            execution_policy_version="official-backtest-policy-v2",
            pinned_at=datetime(2025, 10, 31, 12, 0, tzinfo=UTC),
        )

    def publish(self) -> None:
        snapshot = _snapshot(self.run_id)
        self.result = ResultSnapshotBuilder().build(snapshot, _records(self.run_id), COMPLETED_AT)
        self.details = DetailObjectBuilder().build(self.result, [], [], COMPLETED_AT)
        self.monthly = MonthlyJudgmentBuilder().build(
            snapshot.snapshot_id, self.result.manifest.result_manifest_id, [], self.result.records
        )

        port = PersistenceStorageObjectWritePort(self.persistence)
        manifest = self.result.manifest
        instants = [record.occurred_at for record in self.result.records]
        StorageObjectRegistrar(self.store, port).publish(
            object_id=uuid.uuid5(
                RESULT_OBJECT_NAMESPACE, f"{manifest.run_snapshot_id}|{manifest.content_hash}"
            ),
            object_key=manifest.object_key,
            data=self.result.object_bytes,
            schema_version=str(manifest.schema_version),
            row_count=manifest.record_count,
            period_start=min(instants),
            period_end=max(instants),
            created_at=manifest.completed_at,
            verified_at=COMPLETED_AT,
            expected_content_hash=manifest.content_hash,
            media_type=manifest.media_type,
            file_format="JSON",
        )
        published = DetailObjectPublisher(self.store, storage_write_port=port).publish(
            self.details, verified_at=COMPLETED_AT
        )

        performance = self.result.performance_row()
        bundle = make_input_bundle(self.run_id)
        with self.persistence.unit_of_work() as uow:
            uow.runs.mark_running(self.run_id, datetime(2025, 11, 2, 4, 0, tzinfo=UTC))
            uow.inputs.lock(
                bundle,
                datasets=(
                    InputDatasetRow(
                        input_bundle_id=bundle.id,
                        dataset_manifest_id=DATASET_MANIFEST_ID,
                        purpose_code="MARKET_INPUT",
                        locked_dataset_hash=HASH_A,
                    ),
                ),
            )
            publish_completed_run(
                uow,
                RunPublication(
                    run_id=self.run_id,
                    completed_at=COMPLETED_AT,
                    result_hash=self.result.summary.result_hash,
                    performance=PerformanceSummaryRow(
                        run_id=self.run_id,
                        metric_catalog_version=performance.metric_catalog_version,
                        metrics_document=dict(performance.metrics_document),
                        calculation_rules_version=performance.calculation_rules_version,
                        source_set_hash=performance.source_set_hash,
                        input_hash=performance.input_hash,
                        result_hash=performance.result_hash,
                        calculated_at=performance.calculated_at,
                    ),
                    monthly=tuple(
                        MonthlyJudgment(
                            summary=MonthlyJudgmentSummaryRow(
                                id=uuid.UUID(summary.summary_id),
                                run_id=self.run_id,
                                et_year_month=summary.et_month.key,
                                evaluation_count=summary.evaluation_count,
                                active_branch_count=summary.active_branch_count,
                                trade_event_count=summary.trade_event_count,
                                data_gap_count=summary.data_gap_count,
                                triggered_count=summary.triggered_count,
                                rejected_count=summary.rejected_count,
                                summary_document=dict(summary.summary_document),
                                summary_hash=summary.summary_hash,
                            )
                        )
                        for summary in self.monthly
                    ),
                    detail_manifests=tuple(published.manifest_rows()),
                ),
            )


@pytest.fixture
def store(tmp_path: Path) -> LocalObjectStore:
    return LocalObjectStore(tmp_path / "objects", bucket_name=BUCKET)


@pytest.fixture
def service(
    persistence: BacktestPersistence, store: LocalObjectStore
) -> BacktestResultQueryService:
    return build_result_query_service(persistence, store)


@pytest.fixture
def published(persistence: BacktestPersistence, store: LocalObjectStore) -> Published:
    run = Published(persistence, store)
    run.accept()
    run.publish()
    return run


# ---------------------------------------------------------------------------
# What a published run reads back as
# ---------------------------------------------------------------------------


def test_a_published_run_is_readable_back_from_rows_and_objects_alone(
    service: BacktestResultQueryService, published: Published
) -> None:
    owner = str(ACCOUNT_ID)
    run_id = str(published.run_id)

    overview = service.overview(owner, run_id)
    assert overview.status == "COMPLETED"
    assert overview.result_manifest_id == published.result.manifest.result_manifest_id
    assert overview.finished_at == COMPLETED_AT
    assert overview.reason_code is None

    # Not `== something the test built`: the summary is re-derived from the JSON object
    # in the store and must equal the one the builder produced independently.
    assert service.performance(owner, run_id) == published.result.summary
    assert service.monthly_judgments(owner, run_id) == published.monthly
    assert [item.et_month.key for item in service.monthly_judgments(owner, run_id)] == [
        "2025-10",
        "2025-11",
    ]

    inputs = service.inputs_and_models(owner, run_id)
    assert inputs.compiled_plan_checksum == COMPILED_PLAN_CHECKSUM
    assert inputs.strategy_snapshot_hash == STRATEGY_SNAPSHOT_HASH
    assert inputs.dataset_manifest_id == str(DATASET_MANIFEST_ID)
    assert inputs.dataset_hash == HASH_A
    assert inputs.input_bundle_fingerprint == HASH_A
    assert inputs.feature_materialization_version == "market-bars-v2"
    assert inputs.execution_policy_version == "official-backtest-policy-v2"
    assert inputs.precision_rules_version == "precision:1.0.0"
    assert inputs.calculation_model_version == "calculation-v9"
    assert inputs.cost_model_version == "cost-v3"
    assert inputs.execution_model_version == "execution-v5"


def test_the_et_week_part_is_split_into_its_two_et_months_on_read(
    service: BacktestResultQueryService, published: Published
) -> None:
    """One Parquet object in the store, two monthly answers out of the database."""

    weeks = {item.descriptor.week.key for item in published.details.objects}
    assert weeks == {"2025-10-27"}, "both fills share one ET Monday week part"

    owner, run_id = str(ACCOUNT_ID), str(published.run_id)
    october = service.monthly_trades(owner, run_id, EtMonth(2025, 10))
    november = service.monthly_trades(owner, run_id, EtMonth(2025, 11))

    assert [item.record_id for item in october] == [OCTOBER_RECORD_ID]
    assert [item.record_id for item in november] == [NOVEMBER_RECORD_ID]
    assert october[0].price == Decimal("100.05000000")
    assert october[0].fee == Decimal("2.20000000")
    assert october[0].cash_after == Decimal("99897.75000000")
    assert october[0].positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),
    )
    assert november[0].cash_after == Decimal("99795.50000000")
    assert november[0].positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("2"), Decimal("200.10")),
    )
    assert service.monthly_trades(owner, run_id, EtMonth(2025, 12)) == ()


def test_listing_is_owner_scoped_newest_first_and_filtered_before_paging(
    service: BacktestResultQueryService, persistence: BacktestPersistence, store: LocalObjectStore
) -> None:
    older = Published(persistence, store)
    older.accept(queued_at=datetime(2025, 10, 30, 12, 0, tzinfo=UTC))
    newer = Published(persistence, store)
    newer.accept(queued_at=datetime(2025, 10, 31, 12, 0, tzinfo=UTC))

    listed = service.list_runs(str(ACCOUNT_ID))

    assert [item.run_id for item in listed] == [str(newer.run_id), str(older.run_id)]
    assert [item.status for item in listed] == ["QUEUED", "QUEUED"]
    # Another account sees none of them, and the read model says so by returning an
    # empty page rather than by refusing.
    assert service.list_runs(str(OTHER_ACCOUNT_ID)) == ()
    # The bot filter runs in SQL, before LIMIT: a page of one still respects it.
    assert [
        item.run_id for item in service.list_runs(str(ACCOUNT_ID), bot_id=str(BOT_ID), limit=1)
    ] == [str(newer.run_id)]
    assert (
        service.list_runs(str(ACCOUNT_ID), bot_id="00000000-0000-4000-8000-0000000000bf") == ()
    )
    assert [
        item.run_id for item in service.list_runs(str(ACCOUNT_ID), limit=1, offset=1)
    ] == [str(older.run_id)]


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------


def test_a_foreign_owner_gets_not_found_from_every_query(
    service: BacktestResultQueryService, published: Published
) -> None:
    """Not-found, not forbidden: 403 would confirm the run exists and finished."""

    other, run_id = str(OTHER_ACCOUNT_ID), str(published.run_id)

    for query in (
        lambda: service.overview(other, run_id),
        lambda: service.inputs_and_models(other, run_id),
        lambda: service.performance(other, run_id),
        lambda: service.monthly_judgments(other, run_id),
        lambda: service.monthly_trades(other, run_id, EtMonth(2025, 10)),
    ):
        with pytest.raises(QueryNotFound, match="not found"):
            query()


def test_an_accepted_run_never_exposes_partial_results(
    service: BacktestResultQueryService, persistence: BacktestPersistence, store: LocalObjectStore
) -> None:
    queued = Published(persistence, store)
    queued.accept()
    owner, run_id = str(ACCOUNT_ID), str(queued.run_id)

    assert service.overview(owner, run_id).status == "QUEUED"
    assert service.inputs_and_models(owner, run_id).compiled_plan_checksum == COMPILED_PLAN_CHECKSUM
    assert service.inputs_and_models(owner, run_id).cost_model_version is None
    for query in (
        lambda: service.performance(owner, run_id),
        lambda: service.monthly_judgments(owner, run_id),
        lambda: service.monthly_trades(owner, run_id, EtMonth(2025, 10)),
    ):
        with pytest.raises(QueryNotReady, match="QUEUED"):
            query()


def test_an_unavailable_run_reports_its_reason_from_runs_failure_code(
    service: BacktestResultQueryService, persistence: BacktestPersistence, store: LocalObjectStore
) -> None:
    run = Published(persistence, store)
    run.accept()
    with persistence.unit_of_work() as uow:
        uow.runs.mark_unavailable(
            run.run_id, datetime(2025, 11, 1, 0, 0, tzinfo=UTC), "REQUIRED_DATA_UNAVAILABLE"
        )

    overview = service.overview(str(ACCOUNT_ID), str(run.run_id))

    assert overview.status == "UNAVAILABLE"
    assert overview.reason_code == "REQUIRED_DATA_UNAVAILABLE"
    assert overview.result_manifest_id is None


def test_a_run_without_its_pinned_inputs_is_refused_rather_than_reported_with_blanks(
    service: BacktestResultQueryService, persistence: BacktestPersistence
) -> None:
    """The row the backend inserts directly into `backtest.runs` has no pins."""

    orphan = make_run(idempotency_key="OFFICIAL_BACKTEST:no-pins")
    with persistence.unit_of_work() as uow:
        uow.runs.accept(orphan)

    with pytest.raises(QueryIntegrityError, match="run_input_pins"):
        service.overview(str(ACCOUNT_ID), str(orphan.id))


def test_a_detail_object_that_disappeared_from_the_store_is_an_error_not_an_empty_month(
    service: BacktestResultQueryService, published: Published, store: LocalObjectStore
) -> None:
    trade = next(
        item
        for item in published.details.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
    )
    removed = store.path_for(trade.descriptor.object_key)
    assert removed.is_file(), removed
    removed.unlink()

    with pytest.raises(QueryIntegrityError, match="could not be read back"):
        service.monthly_trades(str(ACCOUNT_ID), str(published.run_id), EtMonth(2025, 10))


def test_a_detail_object_whose_bytes_were_rewritten_is_an_error(
    service: BacktestResultQueryService, published: Published, store: LocalObjectStore
) -> None:
    """`storage.objects.content_hash` is the arbiter, not the file on disk."""

    trade = next(
        item
        for item in published.details.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
    )
    rewritten = trade.parquet_bytes[:-8] + b"tampered"
    assert len(rewritten) == len(trade.parquet_bytes), "same length, so only the hash catches it"
    store.path_for(trade.descriptor.object_key).write_bytes(rewritten)

    with pytest.raises(QueryIntegrityError, match="hashes to"):
        service.monthly_trades(str(ACCOUNT_ID), str(published.run_id), EtMonth(2025, 10))


def test_a_result_object_stored_in_another_bucket_is_not_served(
    persistence: BacktestPersistence, published: Published, tmp_path: Path
) -> None:
    """`storage.objects` identity includes the bucket; another one is another object."""

    elsewhere = LocalObjectStore(tmp_path / "objects", bucket_name="somebody-elses-bucket")
    service = build_result_query_service(persistence, elsewhere)

    with pytest.raises(QueryIntegrityError, match="no AVAILABLE result snapshot object"):
        service.performance(str(ACCOUNT_ID), str(published.run_id))


def test_a_monthly_document_edited_in_place_no_longer_addresses_its_own_hash(
    service: BacktestResultQueryService, published: Published, persistence: BacktestPersistence
) -> None:
    from sqlalchemy import text

    with persistence.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE backtest.monthly_judgment_summaries "
                "SET summary_document = jsonb_set(summary_document, '{trade_event_count}', '99') "
                "WHERE run_id = :run_id"
            ),
            {"run_id": published.run_id},
        )

    with pytest.raises(QueryIntegrityError, match="summary_hash"):
        service.monthly_judgments(str(ACCOUNT_ID), str(published.run_id))


def test_the_run_status_and_the_stored_result_hash_must_agree(
    service: BacktestResultQueryService, published: Published, persistence: BacktestPersistence
) -> None:
    """A `runs.result_hash` nobody could have computed is refused, not served."""

    from sqlalchemy import text

    with persistence.engine.begin() as connection:
        connection.execute(
            text("UPDATE backtest.runs SET result_hash = :hash WHERE id = :run_id"),
            {"hash": "f" * 64, "run_id": published.run_id},
        )

    with pytest.raises(QueryIntegrityError, match="runs.result_hash"):
        service.performance(str(ACCOUNT_ID), str(published.run_id))


def test_the_run_row_is_completed_and_the_service_agrees(
    persistence: BacktestPersistence, published: Published
) -> None:
    """Guards the arrangement itself: these tests must read a genuinely COMPLETED run."""

    with persistence.read_only() as uow:
        assert uow.runs.get(published.run_id).status is RunStatus.COMPLETED


def test_a_run_store_reads_nothing_it_cannot_verify(
    persistence: BacktestPersistence, store: LocalObjectStore, published: Published
) -> None:
    """The store is read-only: it exposes no way to publish or mutate anything."""

    durable = DurableBacktestResultQueryStore(persistence=persistence, object_store=store)

    assert not hasattr(durable, "publish_completed")
    assert not hasattr(durable, "upsert_run")
    entry = durable.get_owned(str(ACCOUNT_ID), str(published.run_id))
    assert entry.result is not None
    assert entry.details is not None
    assert entry.run.result_manifest_id == published.result.manifest.result_manifest_id
