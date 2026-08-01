from __future__ import annotations

import copy
from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.contracts import compute_input_bundle_fingerprint
from backtest_engine.detail_object_manifest import DetailObjectBuilder
from backtest_engine.execution_model import OrderStatus
from backtest_engine.lifecycle import BacktestRun
from backtest_engine.monthly_judgment import EtMonth, MonthlyJudgmentBuilder
from backtest_engine.result_query import (
    BacktestResultQueryService,
    InMemoryBacktestResultQueryStore,
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


OWNER_ID = "00000000-0000-4000-8000-000000002901"
OTHER_OWNER_ID = "00000000-0000-4000-8000-000000002902"
RUN_ID = "00000000-0000-4000-8000-000000002903"
OTHER_RUN_ID = "00000000-0000-4000-8000-000000002904"
STRATEGY_ID = "00000000-0000-4000-8000-000000002905"
OTHER_STRATEGY_ID = "00000000-0000-4000-8000-000000002906"
DATASET_ID = "00000000-0000-4000-8000-000000002907"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000002908"
ORDER_ID = "00000000-0000-4000-8000-000000002909"
RECORD_ID = "00000000-0000-4000-8000-000000002910"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _request(
    *,
    run_id: str = RUN_ID,
    strategy_id: str = STRATEGY_ID,
    requested_at: str = "2025-11-01T04:00:00Z",
) -> dict[str, object]:
    request: dict[str, object] = {
        "contract_id": "com06.backtest-request",
        "schema_version": 1,
        "event_type": "BACKTEST_REQUESTED",
        "message_id": "00000000-0000-4000-8000-000000002911",
        "occurred_at": requested_at,
        "correlation_id": "00000000-0000-4000-8000-000000002912",
        "idempotency_key": f"backtest:{run_id}",
        "backtest_run_id": run_id,
        "strategy_version_id": strategy_id,
        "strategy_snapshot_hash": "a" * 64,
        "compiled_plan_hash": "b" * 64,
        "dataset_manifest_id": DATASET_ID,
        "dataset_hash": "c" * 64,
        "feature_materialization_version": "feature-v7",
        "execution_policy_version": "official-policy-v4",
        "requested_at": requested_at,
    }
    request["input_bundle_fingerprint"] = compute_input_bundle_fingerprint(request)
    return request


def _run(
    status: str = "QUEUED",
    *,
    run_id: str = RUN_ID,
    strategy_id: str = STRATEGY_ID,
    requested_at: str = "2025-11-01T04:00:00Z",
) -> BacktestRun:
    request = _request(
        run_id=run_id,
        strategy_id=strategy_id,
        requested_at=requested_at,
    )
    status_result: dict[str, object] | None = None
    if status == "QUEUED":
        status_result = {"queued_at": requested_at}
    elif status == "RUNNING":
        status_result = {
            "started_at": "2025-11-01T04:01:00Z",
            "attempt": 1,
        }
    elif status == "FAILED":
        status_result = {
            "failed_at": "2025-11-01T04:02:00Z",
            "failure_code": "WORKER_TIMEOUT",
            "retryable": False,
        }
    elif status == "UNAVAILABLE":
        status_result = {
            "decided_at": "2025-11-01T04:00:01Z",
            "reason_code": "REQUIRED_DATA_MISSING",
            "missing_requirements": ["resolution:1m", "symbol:XYZ"],
        }
    return BacktestRun(
        backtest_run_id=run_id,
        idempotency_key=str(request["idempotency_key"]),
        status=status,
        request=request,
        status_result=status_result,
        version=2,
        dispatch_pending=False,
    )


def _snapshot() -> RunSnapshot:
    request = _request()
    return RunSnapshot(
        backtest_run_id=RUN_ID,
        strategy_version_id=STRATEGY_ID,
        input_bundle_fingerprint=str(request["input_bundle_fingerprint"]),
        calculation_model_version="calculation-v9",
        cost_model_version="cost-v3",
        execution_model_version="execution-v5",
        initial_cash=Decimal("10000"),
    )


def _record() -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=_snapshot().snapshot_id,
        record_id=RECORD_ID,
        kind=ResultRecordKind.FILL,
        occurred_at=_instant("2025-11-01T03:30:00Z"),  # October ET
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.FILLED,
        cash_after=Decimal("9897.80"),
        positions_after=(
            PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),
        ),
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


def _completed_artifacts():
    result = ResultSnapshotBuilder().build(
        _snapshot(), [_record()], _instant("2025-11-01T04:10:00Z")
    )
    details = DetailObjectBuilder().build(
        result, [], [], _instant("2025-11-01T04:11:00Z")
    )
    monthly = MonthlyJudgmentBuilder().build(
        _snapshot().snapshot_id,
        result.manifest.result_manifest_id,
        [],
        result.records,
    )
    run = replace(
        _run("QUEUED"),
        status="COMPLETE",
        status_result={
            "completed_at": "2025-11-01T04:10:00Z",
            "result_manifest_id": result.manifest.result_manifest_id,
        },
        version=3,
    )
    return run, result, details, monthly


def _service() -> tuple[BacktestResultQueryService, InMemoryBacktestResultQueryStore]:
    store = InMemoryBacktestResultQueryStore()
    return BacktestResultQueryService(store), store


def test_lists_only_owned_runs_in_stable_latest_first_order() -> None:
    service, store = _service()
    store.upsert_run(
        OWNER_ID,
        _run(
            run_id=OTHER_RUN_ID,
            strategy_id=OTHER_STRATEGY_ID,
            requested_at="2025-11-01T05:00:00Z",
        ),
    )
    store.upsert_run(OWNER_ID, _run())
    foreign = replace(
        _run(run_id="00000000-0000-4000-8000-000000002914"),
        request=_request(run_id="00000000-0000-4000-8000-000000002914"),
    )
    store.upsert_run(OTHER_OWNER_ID, foreign)

    listed = service.list_runs(OWNER_ID)

    assert [item.run_id for item in listed] == [OTHER_RUN_ID, RUN_ID]
    assert [item.status for item in listed] == ["QUEUED", "QUEUED"]
    assert service.list_runs(OWNER_ID, strategy_version_id=STRATEGY_ID)[0].run_id == RUN_ID
    assert all(item.run_id != foreign.backtest_run_id for item in listed)


def test_foreign_owner_receives_not_found_for_every_detail_query() -> None:
    service, store = _service()
    store.upsert_run(OWNER_ID, _run())

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
    store.upsert_run(OWNER_ID, _run(status))

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
    store.upsert_run(OWNER_ID, _run("UNAVAILABLE"))

    overview = service.overview(OWNER_ID, RUN_ID)

    assert overview.status == "UNAVAILABLE"
    assert overview.reason_code == "REQUIRED_DATA_MISSING"
    assert overview.missing_requirements == ("resolution:1m", "symbol:XYZ")
    with pytest.raises(QueryNotReady, match="UNAVAILABLE"):
        service.performance(OWNER_ID, RUN_ID)


def test_inputs_and_models_preserve_locked_reproducibility_identity() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(OWNER_ID, run, result, details, monthly)

    view = service.inputs_and_models(OWNER_ID, RUN_ID)

    assert view.strategy_version_id == STRATEGY_ID
    assert view.strategy_snapshot_hash == "a" * 64
    assert view.compiled_plan_hash == "b" * 64
    assert view.dataset_manifest_id == DATASET_ID
    assert view.dataset_hash == "c" * 64
    assert view.input_bundle_fingerprint == _snapshot().input_bundle_fingerprint
    assert view.feature_materialization_version == "feature-v7"
    assert view.execution_policy_version == "official-policy-v4"
    assert view.calculation_model_version == "calculation-v9"
    assert view.cost_model_version == "cost-v3"
    assert view.execution_model_version == "execution-v5"


def test_complete_query_returns_performance_and_et_monthly_judgments() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(OWNER_ID, run, result, details, monthly)

    performance = service.performance(OWNER_ID, RUN_ID)
    judgments = service.monthly_judgments(OWNER_ID, RUN_ID)

    assert performance == result.summary
    assert [summary.et_month.key for summary in judgments] == ["2025-10"]
    assert judgments[0].trade_record_ids == (RECORD_ID,)


def test_monthly_trades_read_verified_parquet_and_join_after_positions() -> None:
    service, store = _service()
    run, result, details, monthly = _completed_artifacts()
    store.publish_completed(OWNER_ID, run, result, details, monthly)

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
    assert service.monthly_trades(OWNER_ID, RUN_ID, EtMonth(2025, 11)) == ()


def test_publish_fails_closed_when_result_identity_does_not_match_run() -> None:
    _, store = _service()
    run, result, details, monthly = _completed_artifacts()
    changed_request = copy.deepcopy(run.request)
    changed_request["strategy_version_id"] = OTHER_STRATEGY_ID
    changed_request["input_bundle_fingerprint"] = compute_input_bundle_fingerprint(
        changed_request
    )
    inconsistent = replace(run, request=changed_request)

    with pytest.raises(QueryIntegrityError, match="strategy version|fingerprint"):
        store.publish_completed(OWNER_ID, inconsistent, result, details, monthly)


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
        store.publish_completed(OWNER_ID, run, result, tampered, monthly)


def test_upsert_rejects_owner_change_and_older_projection() -> None:
    _, store = _service()
    run = _run()
    store.upsert_run(OWNER_ID, run)

    with pytest.raises(QueryIntegrityError, match="owner"):
        store.upsert_run(OTHER_OWNER_ID, run)
    with pytest.raises(QueryIntegrityError, match="older"):
        store.upsert_run(OWNER_ID, replace(run, version=1))


def test_complete_projection_requires_atomic_artifact_publication() -> None:
    _, store = _service()
    run, _, _, _ = _completed_artifacts()

    with pytest.raises(QueryIntegrityError, match="publish_completed"):
        store.upsert_run(OWNER_ID, run)


def test_terminal_status_and_running_progress_cannot_be_reversed() -> None:
    _, store = _service()
    failed = _run("FAILED")
    store.upsert_run(OWNER_ID, failed)

    with pytest.raises(QueryIntegrityError, match="terminal"):
        store.upsert_run(
            OWNER_ID,
            replace(failed, status="UNAVAILABLE", version=failed.version + 1),
        )

    _, other_store = _service()
    running = _run("RUNNING")
    other_store.upsert_run(OWNER_ID, running)
    with pytest.raises(QueryIntegrityError, match="transition"):
        other_store.upsert_run(
            OWNER_ID,
            replace(running, status="QUEUED", version=running.version + 1),
        )
