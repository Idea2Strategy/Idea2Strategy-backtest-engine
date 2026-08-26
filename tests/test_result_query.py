"""Owner-scoped result queries over ET-week detail objects (cards D03/D27, D29).

The read model consumes `RunProjection`, declared by `result_query` itself, so these
tests exercise the projection rules and the ET-week -> ET-month join without depending
on the write-side run aggregate or on B's request envelope.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.detail_object_manifest import (
    DetailObjectBuilder,
    DetailObjectKind,
    PerformancePoint,
)
from backtest_engine.execution_model import OrderStatus
from backtest_engine.monthly_judgment import EtMonth, MonthlyJudgmentBuilder
from backtest_engine.result_query import (
    BacktestResultQueryService,
    InMemoryBacktestResultQueryStore,
    QueryIntegrityError,
    QueryNotFound,
    QueryNotReady,
    QueryValidationError,
    RunDatasetInput,
    RunFeatureInput,
    RunInputs,
    RunProjection,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshotBuilder,
    RunSnapshot,
)


OWNER_ID = "00000000-0000-4000-8000-000000002901"
OTHER_OWNER_ID = "00000000-0000-4000-8000-000000002902"
RUN_ID = "00000000-0000-4000-8000-000000002903"
OTHER_RUN_ID = "00000000-0000-4000-8000-000000002904"
BOT_ID = "00000000-0000-4000-8000-000000002905"
OTHER_BOT_ID = "00000000-0000-4000-8000-000000002906"
DATASET_ID = "00000000-0000-4000-8000-000000002907"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000002908"
ORDER_ID = "00000000-0000-4000-8000-000000002909"
RECORD_ID = "00000000-0000-4000-8000-000000002910"
NOVEMBER_RECORD_ID = "00000000-0000-4000-8000-000000002920"
NOVEMBER_ORDER_ID = "00000000-0000-4000-8000-000000002921"
NOVEMBER_FILL_ID = "00000000-0000-4000-8000-000000002922"

#: `RunSnapshot.input_bundle_fingerprint` is a bare lowercase SHA-256, pinned here so
#: this read-model test does not depend on the request-envelope digest rules.
FINGERPRINT = "d" * 64

QUEUED_AT = "2025-11-01T04:00:00Z"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _inputs(fingerprint: str = FINGERPRINT) -> RunInputs:
    return RunInputs(
        compiled_plan_checksum="sha256:" + "b" * 64,
        strategy_snapshot_hash="sha256:" + "a" * 64,
        input_bundle_fingerprint=fingerprint,
        input_contract_version="backtest-request.v1",
        datasets=(RunDatasetInput(DATASET_ID, "MARKET_BARS", "c" * 64),),
        feature_materializations=(
            RunFeatureInput(
                "00000000-0000-4000-8000-000000002999", "e" * 64
            ),
        ),
        execution_policy_version="official-policy-v4",
        precision_rules_version="precision:1.0.0",
    )


def _run(
    status: str = "QUEUED",
    *,
    run_id: str = RUN_ID,
    bot_id: str = BOT_ID,
    owner_account_id: str = OWNER_ID,
    queued_at: str = QUEUED_AT,
    version: int = 2,
    result_manifest_id: str | None = None,
) -> RunProjection:
    extra: dict[str, object] = {}
    if status == "RUNNING":
        extra = {"started_at": _instant("2025-11-01T04:01:00Z")}
    elif status == "FAILED":
        extra = {
            "started_at": _instant("2025-11-01T04:01:00Z"),
            "finished_at": _instant("2025-11-01T04:02:00Z"),
            "failure_code": "WORKER_TIMEOUT",
            "retryable": True,
        }
    elif status == "UNAVAILABLE":
        extra = {
            "finished_at": _instant("2025-11-01T04:00:01Z"),
            "reason_code": "REQUIRED_DATA_MISSING",
            "missing_requirements": ("resolution:1m", "symbol:XYZ"),
        }
    elif status == "COMPLETED":
        extra = {
            "started_at": _instant("2025-11-01T04:01:00Z"),
            "finished_at": _instant("2025-11-01T04:10:00Z"),
            "result_manifest_id": result_manifest_id,
        }
    return RunProjection(
        run_id=run_id,
        bot_id=bot_id,
        owner_account_id=owner_account_id,
        status=status,
        queued_at=_instant(queued_at),
        inputs=_inputs(),
        version=version,
        **extra,  # type: ignore[arg-type]
    )


def _snapshot() -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=RUN_ID,
        strategy_version_id=BOT_ID,
        input_bundle_fingerprint=FINGERPRINT,
        calculation_model_version="calculation-v9",
        cost_model_version="cost-v3",
        execution_model_version="execution-v5",
        initial_cash=Decimal("10000"),
    )


def _record() -> ResultRecord:
    """ET Friday 2025-10-31 23:30 — October, in the ET week starting 2025-10-27."""

    return ResultRecord(
        run_snapshot_id=_snapshot().snapshot_id,
        record_id=RECORD_ID,
        kind=ResultRecordKind.FILL,
        occurred_at=_instant("2025-11-01T03:30:00Z"),
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.FILLED,
        cash_after=Decimal("9897.80"),
        positions_after=(PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),),
        fill_id="00000000-0000-4000-8000-000000002913",
        quantity=Decimal("1"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("100.05"),
        slippage_amount=Decimal("0.05"),
        fee=Decimal("2.20"),
        cost_basis=Decimal("100.05"),
        realized_pnl=Decimal("0"),
    )


def _november_record() -> ResultRecord:
    """ET Saturday 2025-11-01 10:30 — November, but the same ET week as `_record()`."""

    return ResultRecord(
        run_snapshot_id=_snapshot().snapshot_id,
        record_id=NOVEMBER_RECORD_ID,
        kind=ResultRecordKind.FILL,
        occurred_at=_instant("2025-11-01T14:30:00Z"),
        order_id=NOVEMBER_ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.FILLED,
        cash_after=Decimal("9795.55"),
        positions_after=(PositionAfter(INSTRUMENT_ID, Decimal("2"), Decimal("200.15")),),
        fill_id=NOVEMBER_FILL_ID,
        quantity=Decimal("1"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("100.05"),
        slippage_amount=Decimal("0.05"),
        fee=Decimal("2.20"),
        cost_basis=Decimal("100.10"),
        realized_pnl=Decimal("0"),
    )


def _artifacts(records: list[ResultRecord], built_at: str = "2025-11-02T04:10:00Z"):
    result = ResultSnapshotBuilder().build(_snapshot(), records, _instant(built_at))
    points = [
        PerformancePoint(
            point_id="00000000-0000-4000-8000-000000002930",
            run_snapshot_id=_snapshot().snapshot_id,
            occurred_at=_instant("2025-10-30T20:00:00Z"),
            metric_id="equity",
            value=Decimal("10000.00000000"),
        ),
        PerformancePoint(
            point_id="00000000-0000-4000-8000-000000002931",
            run_snapshot_id=_snapshot().snapshot_id,
            occurred_at=_instant("2025-10-31T20:00:00Z"),
            metric_id="equity",
            value=Decimal("10025.50000000"),
        ),
    ]
    details = DetailObjectBuilder().build(result, [], points, _instant(built_at))
    monthly = MonthlyJudgmentBuilder().build(
        _snapshot().snapshot_id, result.manifest.result_manifest_id, [], result.records
    )
    run = _run("COMPLETED", version=3, result_manifest_id=result.manifest.result_manifest_id)
    return run, result, details, monthly


def _completed_artifacts():
    return _artifacts([_record()], built_at="2025-11-01T04:10:00Z")


def _service() -> tuple[BacktestResultQueryService, InMemoryBacktestResultQueryStore]:
    store = InMemoryBacktestResultQueryStore()
    return BacktestResultQueryService(store), store


# --------------------------------------------------------------------------------
# owner scope
# --------------------------------------------------------------------------------


def test_lists_only_owned_runs_in_stable_latest_first_order() -> None:
    service, store = _service()
    store.upsert_run(
        _run(run_id=OTHER_RUN_ID, bot_id=OTHER_BOT_ID, queued_at="2025-11-01T05:00:00Z")
    )
    store.upsert_run(_run())
    store.upsert_run(
        _run(run_id="00000000-0000-4000-8000-000000002914", owner_account_id=OTHER_OWNER_ID)
    )

    listed = service.list_runs(OWNER_ID)

    assert [item.run_id for item in listed] == [OTHER_RUN_ID, RUN_ID]
    assert [item.status for item in listed] == ["QUEUED", "QUEUED"]
    assert [item.run_id for item in service.list_runs(OWNER_ID, bot_id=BOT_ID)] == [RUN_ID]
    assert all(item.run_id != "00000000-0000-4000-8000-000000002914" for item in listed)


def test_foreign_owner_receives_not_found_for_every_detail_query() -> None:
    service, store = _service()
    store.upsert_run(_run())

    for query in (
        lambda: service.overview(OTHER_OWNER_ID, RUN_ID),
        lambda: service.inputs_and_models(OTHER_OWNER_ID, RUN_ID),
        lambda: service.performance(OTHER_OWNER_ID, RUN_ID),
        lambda: service.monthly_judgments(OTHER_OWNER_ID, RUN_ID),
        lambda: service.monthly_trades(OTHER_OWNER_ID, RUN_ID, EtMonth(2025, 10)),
    ):
        with pytest.raises(QueryNotFound, match="not found"):
            query()


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING", "FAILED"])
def test_incomplete_or_failed_run_never_exposes_partial_results(status: str) -> None:
    service, store = _service()
    store.upsert_run(_run(status))

    assert service.overview(OWNER_ID, RUN_ID).status == status
    for query in (
        lambda: service.performance(OWNER_ID, RUN_ID),
        lambda: service.monthly_judgments(OWNER_ID, RUN_ID),
        lambda: service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 10)),
    ):
        with pytest.raises(QueryNotReady, match=status):
            query()


def test_unavailable_overview_returns_explicit_reason_without_performance() -> None:
    service, store = _service()
    store.upsert_run(_run("UNAVAILABLE"))

    overview = service.overview(OWNER_ID, RUN_ID)

    assert overview.status == "UNAVAILABLE"
    assert overview.reason_code == "REQUIRED_DATA_MISSING"
    assert overview.missing_requirements == ("resolution:1m", "symbol:XYZ")
    assert overview.result_manifest_id is None
    with pytest.raises(QueryNotReady, match="UNAVAILABLE"):
        service.performance(OWNER_ID, RUN_ID)


def test_failed_overview_preserves_provider_retryability_decision() -> None:
    service, store = _service()
    store.upsert_run(_run("FAILED"))

    overview = service.overview(OWNER_ID, RUN_ID)

    assert overview.reason_code == "WORKER_TIMEOUT"
    assert overview.retryable is True


# --------------------------------------------------------------------------------
# projection preconditions
# --------------------------------------------------------------------------------


def test_projection_rejects_states_that_cannot_be_rendered() -> None:
    with pytest.raises(QueryValidationError, match="status"):
        _run("DONE")
    with pytest.raises(QueryValidationError, match="reason_code"):
        RunProjection(
            run_id=RUN_ID,
            bot_id=BOT_ID,
            owner_account_id=OWNER_ID,
            status="UNAVAILABLE",
            queued_at=_instant(QUEUED_AT),
            inputs=_inputs(),
            finished_at=_instant(QUEUED_AT),
        )
    with pytest.raises(QueryValidationError, match="result_manifest_id"):
        RunProjection(
            run_id=RUN_ID,
            bot_id=BOT_ID,
            owner_account_id=OWNER_ID,
            status="RUNNING",
            queued_at=_instant(QUEUED_AT),
            inputs=_inputs(),
            result_manifest_id=DATASET_ID,
        )
    with pytest.raises(QueryValidationError, match="finished_at"):
        RunProjection(
            run_id=RUN_ID,
            bot_id=BOT_ID,
            owner_account_id=OWNER_ID,
            status="FAILED",
            queued_at=_instant(QUEUED_AT),
            inputs=_inputs(),
            failure_code="WORKER_TIMEOUT",
            retryable=True,
        )
    with pytest.raises(QueryValidationError, match="timezone-aware"):
        RunProjection(
            run_id=RUN_ID,
            bot_id=BOT_ID,
            owner_account_id=OWNER_ID,
            status="QUEUED",
            queued_at=datetime(2025, 11, 1, 4, 0),
            inputs=_inputs(),
        )
    with pytest.raises(QueryValidationError, match="locked_dataset_hash"):
        RunDatasetInput(DATASET_ID, "MARKET_BARS", "")


def test_inputs_and_models_preserve_locked_reproducibility_identity() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(run, result, details, monthly)

    view = service.inputs_and_models(OWNER_ID, RUN_ID)

    assert view.bot_id == BOT_ID
    assert view.strategy_snapshot_hash == "sha256:" + "a" * 64
    assert view.compiled_plan_checksum == "sha256:" + "b" * 64
    assert view.datasets[0].dataset_manifest_id == DATASET_ID
    assert view.datasets[0].locked_dataset_hash == "c" * 64
    assert view.input_bundle_fingerprint == FINGERPRINT
    assert view.input_contract_version == "backtest-request.v1"
    assert len(view.feature_materializations) == 1
    assert view.execution_policy_version == "official-policy-v4"
    assert view.precision_rules_version == "precision:1.0.0"
    assert view.calculation_model_version == "calculation-v9"
    assert view.cost_model_version == "cost-v3"
    assert view.execution_model_version == "execution-v5"


def test_run_inputs_accept_multiple_market_bar_dataset_pins() -> None:
    inputs = RunInputs(
        compiled_plan_checksum="sha256:" + "b" * 64,
        strategy_snapshot_hash="sha256:" + "a" * 64,
        input_bundle_fingerprint=FINGERPRINT,
        input_contract_version="backtest-request.v1",
        datasets=(
            RunDatasetInput(DATASET_ID, "MARKET_BARS", "c" * 64),
            RunDatasetInput("00000000-0000-4000-8000-000000000099", "MARKET_BARS", "f" * 64),
        ),
        feature_materializations=(),
        execution_policy_version="official-policy-v4",
        precision_rules_version="precision:1.0.0",
    )

    assert len(inputs.datasets) == 2
    assert inputs.market_bars.dataset_manifest_id == DATASET_ID


def test_complete_query_returns_performance_and_et_monthly_judgments() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(run, result, details, monthly)

    performance = service.performance(OWNER_ID, RUN_ID)
    judgments = service.monthly_judgments(OWNER_ID, RUN_ID)

    assert performance == result.summary
    assert [summary.et_month.key for summary in judgments] == ["2025-10"]
    assert judgments[0].trade_record_ids == (RECORD_ID,)


def test_performance_series_reads_the_official_equity_parquet_in_time_order() -> None:
    """Catches returning activity counts or summary endpoints as an equity curve."""
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(run, result, details, monthly)

    series = service.performance_series(OWNER_ID, RUN_ID)

    assert [(point.occurred_at.isoformat(), point.equity) for point in series.points] == [
        ("2025-10-30T20:00:00+00:00", Decimal("10000.00000000")),
        ("2025-10-31T20:00:00+00:00", Decimal("10025.50000000")),
    ]
    assert series.result_hash == result.summary.result_hash
    assert series.source_set_hash == result.summary.source_set_hash


# --------------------------------------------------------------------------------
# the ET week -> ET month join
# --------------------------------------------------------------------------------


def test_monthly_trades_read_verified_parquet_and_join_after_positions() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(run, result, details, monthly)

    trades = service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 10))

    assert len(trades) == 1
    trade = trades[0]
    assert trade.record_id == RECORD_ID
    assert trade.kind == "FILL"
    assert trade.base_price == Decimal("100")
    assert trade.price == Decimal("100.05")
    assert trade.slippage_amount == Decimal("0.05")
    assert trade.fee == Decimal("2.20")
    assert trade.realized_pnl == Decimal("0")
    assert trade.cash_after == Decimal("9897.80")
    assert trade.positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),
    )
    # The one detail part lives in the ET week starting Monday 2025-10-27, which runs
    # to Sunday 2025-11-02 and therefore overlaps November. Overlap is not membership.
    assert {item.descriptor.week.key for item in details.objects} == {"2025-10-27"}
    assert service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 11)) == ()


def test_a_single_et_week_part_is_split_across_the_two_months_it_spans() -> None:
    """ET week 2025-10-27 holds Friday's October trade and Saturday's November trade.

    One Parquet object, two monthly answers. A reader that treats the week partition as
    the month returns both rows for both months, or one of them for neither.
    """

    service, store = _service()
    run, result, details, monthly = _artifacts([_record(), _november_record()])
    store.publish_completed(run, result, details, monthly)

    trade_parts = [
        item.descriptor
        for item in details.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
    ]
    assert [(item.week.key, item.part_number, item.row_count) for item in trade_parts] == [
        ("2025-10-27", 1, 2)
    ], "both trades share one ET week part"

    october = service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 10))
    november = service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 11))

    assert [item.record_id for item in october] == [RECORD_ID]
    assert [item.record_id for item in november] == [NOVEMBER_RECORD_ID]
    assert october[0].positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),
    )
    assert november[0].positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("2"), Decimal("200.15")),
    )
    assert service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 12)) == ()


def test_monthly_trades_fail_closed_when_an_object_disappears_after_publication() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(run, result, details, monthly)
    entry = store.get_owned(OWNER_ID, RUN_ID)
    object.__setattr__(
        entry,
        "details",
        replace(
            details,
            objects=tuple(
                item
                for item in details.objects
                if item.descriptor.record_type is not DetailObjectKind.TRADE_DETAIL
            ),
        ),
    )

    with pytest.raises(QueryIntegrityError, match="do not match"):
        service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 10))


# --------------------------------------------------------------------------------
# publication integrity
# --------------------------------------------------------------------------------


def test_publish_fails_closed_when_result_identity_does_not_match_run() -> None:
    _, store = _service()
    run, result, details, monthly = _completed_artifacts()
    inconsistent = replace(run, inputs=_inputs(fingerprint="e" * 64))

    with pytest.raises(QueryIntegrityError, match="fingerprint"):
        store.publish_completed(inconsistent, result, details, monthly)


def test_publish_fails_closed_when_the_result_manifest_is_not_the_declared_one() -> None:
    _, store = _service()
    _, result, details, monthly = _completed_artifacts()
    other, other_result, _, _ = _artifacts([_record(), _november_record()])
    assert other_result.manifest.result_manifest_id != result.manifest.result_manifest_id

    with pytest.raises(QueryIntegrityError, match="result manifest"):
        store.publish_completed(other, result, details, monthly)


def test_publish_fails_closed_for_tampered_detail_object() -> None:
    _, store = _service()
    run, result, details, monthly = _completed_artifacts()
    tampered = replace(
        details,
        objects=(
            replace(
                details.objects[0],
                parquet_bytes=details.objects[0].parquet_bytes + b"tampered",
            ),
            *details.objects[1:],
        ),
    )

    with pytest.raises(QueryIntegrityError, match="detail"):
        store.publish_completed(run, result, tampered, monthly)


def test_publish_completed_is_idempotent_for_the_identical_projection() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()

    store.publish_completed(run, result, details, monthly)
    store.publish_completed(run, result, details, monthly)

    assert service.overview(OWNER_ID, RUN_ID).result_manifest_id == (
        result.manifest.result_manifest_id
    )
    # A second, independently built detail bundle for the same result is still a
    # different set of immutable objects, and must not silently replace the published
    # one.
    reissued = DetailObjectBuilder().build(result, [], [], _instant("2025-11-01T05:00:00Z"))
    assert reissued.manifest.manifest_hash != details.manifest.manifest_hash
    with pytest.raises(QueryIntegrityError, match="different immutable"):
        store.publish_completed(run, result, reissued, monthly)


def test_upsert_rejects_owner_change_and_older_projection() -> None:
    _, store = _service()
    run = _run()
    store.upsert_run(run)

    with pytest.raises(QueryIntegrityError, match="owner"):
        store.upsert_run(replace(run, owner_account_id=OTHER_OWNER_ID))
    with pytest.raises(QueryIntegrityError, match="older"):
        store.upsert_run(replace(run, version=1))
    with pytest.raises(QueryIntegrityError, match="inputs"):
        store.upsert_run(
            replace(run, inputs=_inputs(fingerprint="e" * 64), version=run.version + 1)
        )


def test_complete_projection_requires_atomic_artifact_publication() -> None:
    _, store = _service()
    run, _, _, _ = _completed_artifacts()

    with pytest.raises(QueryIntegrityError, match="publish_completed"):
        store.upsert_run(run)


def test_terminal_status_and_running_progress_cannot_be_reversed() -> None:
    _, store = _service()
    failed = _run("FAILED")
    store.upsert_run(failed)

    with pytest.raises(QueryIntegrityError, match="terminal"):
        store.upsert_run(
            replace(
                failed,
                status="UNAVAILABLE",
                failure_code=None,
                reason_code="REQUIRED_DATA_MISSING",
                retryable=None,
                version=failed.version + 1,
            )
        )

    _, other_store = _service()
    running = _run("RUNNING")
    other_store.upsert_run(running)
    with pytest.raises(QueryIntegrityError, match="transition"):
        other_store.upsert_run(replace(running, status="QUEUED", version=running.version + 1))


def test_a_queued_run_cannot_jump_straight_to_completed() -> None:
    """Matches `lifecycle.InMemoryBacktestRunStore._TRANSITIONS`: only RUNNING completes."""

    _, store = _service()
    store.upsert_run(_run("QUEUED"))
    run, result, details, monthly = _completed_artifacts()

    with pytest.raises(QueryIntegrityError, match="transition"):
        store.publish_completed(run, result, details, monthly)
