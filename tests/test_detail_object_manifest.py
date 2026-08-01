from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.detail_object_manifest import (
    DetailIntegrityError,
    DetailManifestConflict,
    DetailObjectBuilder,
    DetailObjectKind,
    DetailObjectValidationError,
    InMemoryDetailManifestStore,
    PerformancePoint,
    ReplayLedgerDetail,
)
from backtest_engine.execution_model import (
    LedgerDirection,
    LedgerEntry,
    LedgerTransaction,
    OrderStatus,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshotBuilder,
    RunSnapshot,
)


RUN_ID = "00000000-0000-4000-8000-000000001001"
STRATEGY_ID = "00000000-0000-4000-8000-000000001002"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000001003"
OTHER_INSTRUMENT_ID = "00000000-0000-4000-8000-000000001004"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run(fingerprint: str = "a") -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=RUN_ID,
        strategy_version_id=STRATEGY_ID,
        input_bundle_fingerprint=fingerprint * 64,
        calculation_model_version="calc-v1",
        cost_model_version="cost-v1",
        execution_model_version="execution-v1",
        initial_cash=Decimal("10000"),
    )


def _record(
    record_id: str,
    order_id: str,
    occurred_at: str,
    *,
    position: bool = False,
) -> ResultRecord:
    positions = (
        PositionAfter(INSTRUMENT_ID, Decimal("2"), Decimal("200")),
    ) if position else ()
    return ResultRecord(
        run_snapshot_id=_run().snapshot_id,
        record_id=record_id,
        kind=ResultRecordKind.ORDER,
        occurred_at=_instant(occurred_at),
        order_id=order_id,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.ACCEPTED,
        cash_after=Decimal("9800") if position else Decimal("10000"),
        positions_after=positions,
    )


def _result(records: list[ResultRecord] | None = None):
    if records is None:
        records = [
            _record(
                "00000000-0000-4000-8000-000000001011",
                "00000000-0000-4000-8000-000000001012",
                "2025-11-01T03:30:00Z",  # October ET
                position=True,
            ),
            _record(
                "00000000-0000-4000-8000-000000001013",
                "00000000-0000-4000-8000-000000001014",
                "2025-11-01T04:30:00Z",  # November ET
            ),
        ]
    return ResultSnapshotBuilder().build(
        _run(), records, _instant("2025-11-01T05:00:00Z")
    )


def _ledger(
    transaction_id: str = "00000000-0000-4000-8000-000000001021",
    posted_at: str = "2025-11-01T04:45:00Z",
) -> ReplayLedgerDetail:
    source_id = "00000000-0000-4000-8000-000000001022"
    transaction = LedgerTransaction(
        transaction_id=transaction_id,
        source_event_id=source_id,
        posted_at=_instant(posted_at),
        entries=(
            LedgerEntry(
                "00000000-0000-4000-8000-000000001023",
                "CASH",
                LedgerDirection.DEBIT,
                Decimal("100"),
                source_id,
            ),
            LedgerEntry(
                "00000000-0000-4000-8000-000000001024",
                "POSITION",
                LedgerDirection.CREDIT,
                Decimal("100"),
                source_id,
            ),
        ),
    )
    return ReplayLedgerDetail(_run().snapshot_id, transaction)


def _point(
    point_id: str = "00000000-0000-4000-8000-000000001031",
    occurred_at: str = "2025-11-01T04:50:00Z",
) -> PerformancePoint:
    return PerformancePoint(
        point_id=point_id,
        run_snapshot_id=_run().snapshot_id,
        occurred_at=_instant(occurred_at),
        metric_id="equity",
        value=Decimal("10123.45"),
        instrument_id=None,
    )


def test_builds_parquet_objects_partitioned_by_et_month_and_record_kind() -> None:
    bundle = DetailObjectBuilder().build(
        _result(), [_ledger()], [_point()], _instant("2025-11-01T05:00:00Z")
    )

    partitions = {
        (item.descriptor.et_month.key, item.descriptor.kind)
        for item in bundle.objects
    }
    assert partitions == {
        ("2025-10", DetailObjectKind.TRADE_DETAIL),
        ("2025-10", DetailObjectKind.POSITION_SNAPSHOT),
        ("2025-11", DetailObjectKind.TRADE_DETAIL),
        ("2025-11", DetailObjectKind.REPLAY_LEDGER),
        ("2025-11", DetailObjectKind.CALCULATION_SERIES),
    }
    assert all(item.parquet_bytes[:4] == b"PAR1" for item in bundle.objects)
    assert all(item.parquet_bytes[-4:] == b"PAR1" for item in bundle.objects)


def test_parquet_metadata_and_rows_preserve_exact_snapshot_evidence() -> None:
    result = _result()
    bundle = DetailObjectBuilder().build(
        result, [_ledger()], [_point()], _instant("2025-11-01T05:00:00Z")
    )
    trade = next(
        item
        for item in bundle.objects
        if item.descriptor.kind is DetailObjectKind.TRADE_DETAIL
        and item.descriptor.et_month.key == "2025-10"
    )
    parquet = pq.ParquetFile(pa.BufferReader(trade.parquet_bytes))
    metadata = parquet.schema_arrow.metadata

    assert metadata is not None
    assert metadata[b"run_snapshot_id"].decode() == _run().snapshot_id
    assert metadata[b"backtest_run_id"].decode() == RUN_ID
    assert metadata[b"strategy_version_id"].decode() == STRATEGY_ID
    assert metadata[b"result_manifest_id"].decode() == result.manifest.result_manifest_id
    assert metadata[b"et_month"] == b"2025-10"
    assert metadata[b"record_kind"] == b"TRADE_DETAIL"
    rows = parquet.read().to_pylist()
    assert rows[0]["record_id"] == result.records[0].record_id
    assert rows[0]["cash_after"] == "9800"


def test_relational_manifest_keeps_integrity_and_lineage_without_compact_summaries() -> None:
    result = _result()
    bundle = DetailObjectBuilder().build(
        result, [_ledger()], [_point()], _instant("2025-11-01T05:00:00Z")
    )

    record = bundle.manifest.as_record()
    assert record["run_snapshot_id"] == _run().snapshot_id
    assert record["backtest_run_id"] == RUN_ID
    assert record["strategy_version_id"] == STRATEGY_ID
    assert record["result_manifest_id"] == result.manifest.result_manifest_id
    assert record["manifest_hash"] == bundle.manifest.manifest_hash
    assert all(item["base_object_id"] is None for item in record["objects"])
    assert all(item["correction_of_object_id"] is None for item in record["objects"])
    rendered = repr(record)
    assert "performance_summary" not in rendered
    assert "monthly_judgment" not in rendered


def test_output_is_deterministic_regardless_of_input_order() -> None:
    ledger = _ledger()
    other_ledger = _ledger(
        "00000000-0000-4000-8000-000000001025", "2025-11-01T04:46:00Z"
    )
    other_ledger = replace(
        other_ledger,
        transaction=replace(
            other_ledger.transaction,
            entries=tuple(
                replace(
                    entry,
                    entry_id=f"00000000-0000-4000-8000-00000000102{index + 6}",
                )
                for index, entry in enumerate(other_ledger.transaction.entries)
            ),
        ),
    )
    point = _point()
    other_point = _point(
        "00000000-0000-4000-8000-000000001032", "2025-11-01T04:51:00Z"
    )
    builder = DetailObjectBuilder()

    first = builder.build(
        _result(), [ledger, other_ledger], [point, other_point], _instant("2025-11-01T05:00:00Z")
    )
    second = builder.build(
        _result(), [other_ledger, ledger], [other_point, point], _instant("2025-11-01T05:00:00Z")
    )

    assert first == second


def test_verification_fails_closed_for_tampered_or_missing_objects() -> None:
    bundle = DetailObjectBuilder().build(
        _result(), [_ledger()], [_point()], _instant("2025-11-01T05:00:00Z")
    )
    tampered = replace(
        bundle,
        objects=(
            replace(bundle.objects[0], parquet_bytes=bundle.objects[0].parquet_bytes + b"x"),
            *bundle.objects[1:],
        ),
    )

    with pytest.raises(DetailIntegrityError, match="hash|size"):
        DetailObjectBuilder.verify(tampered)
    with pytest.raises(DetailIntegrityError, match="missing"):
        DetailObjectBuilder.verify(replace(bundle, objects=bundle.objects[1:]))


def test_rejects_mixed_snapshots_duplicates_and_naive_times() -> None:
    result = _result()
    ledger = _ledger()
    point = _point()
    builder = DetailObjectBuilder()

    with pytest.raises(DetailObjectValidationError, match="run snapshot"):
        builder.build(
            result,
            [replace(ledger, run_snapshot_id="b" * 64)],
            [point],
            _instant("2025-11-01T05:00:00Z"),
        )
    with pytest.raises(DetailObjectValidationError, match="run snapshot"):
        builder.build(
            result,
            [ledger],
            [replace(point, run_snapshot_id="b" * 64)],
            _instant("2025-11-01T05:00:00Z"),
        )
    with pytest.raises(DetailObjectValidationError, match="transaction_id"):
        builder.build(
            result, [ledger, ledger], [point], _instant("2025-11-01T05:00:00Z")
        )
    with pytest.raises(DetailObjectValidationError, match="point_id"):
        builder.build(
            result, [ledger], [point, point], _instant("2025-11-01T05:00:00Z")
        )
    with pytest.raises(DetailObjectValidationError, match="timezone-aware"):
        replace(point, occurred_at=datetime(2025, 11, 1, 4, 50))
    with pytest.raises(DetailObjectValidationError, match="created_at"):
        builder.build(
            result,
            [ledger],
            [point],
            _instant("2025-11-01T04:00:00Z"),
        )


def test_empty_official_result_has_a_verified_manifest_without_fake_rows() -> None:
    result = _result([])

    bundle = DetailObjectBuilder().build(
        result, [], [], _instant("2025-11-01T05:00:00Z")
    )

    assert bundle.objects == ()
    assert bundle.manifest.objects == ()
    DetailObjectBuilder.verify(bundle)


def test_manifest_store_is_idempotent_and_rejects_conflicting_official_detail() -> None:
    result = _result()
    bundle = DetailObjectBuilder().build(
        result, [_ledger()], [_point()], _instant("2025-11-01T05:00:00Z")
    )
    changed = DetailObjectBuilder().build(
        result,
        [_ledger()],
        [_point("00000000-0000-4000-8000-000000001033", "2025-11-01T04:52:00Z")],
        _instant("2025-11-01T05:00:00Z"),
    )
    store = InMemoryDetailManifestStore()

    assert store.put(bundle) == bundle.manifest
    assert store.put(bundle) == bundle.manifest
    assert store.get(bundle.manifest.detail_manifest_id) == bundle
    with pytest.raises(DetailManifestConflict, match="result manifest"):
        store.put(changed)
