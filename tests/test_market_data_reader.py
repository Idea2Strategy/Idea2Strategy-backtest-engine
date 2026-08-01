from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.market_data import MarketDataValidationError, ParquetMarketDataReader
from d_market_data_testkit import write_small_market_bars


DATASET_HASH_FIELDS = (
    "content_hash",
    "object_kind",
    "partition_granularity",
    "partition_start",
    "partition_end",
    "period_start",
    "period_end",
    "shard_key",
    "part_number",
    "row_count",
    "schema_version",
)


def _canonical_dataset_hash(objects: list[dict[str, object]]) -> str:
    rows = [{key: item.get(key) for key in DATASET_HASH_FIELDS} for item in objects]
    rows.sort(
        key=lambda row: json.dumps(
            row, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
    )
    payload = json.dumps(
        rows, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _manifest_for(path: Path) -> dict[str, object]:
    object_metadata: dict[str, object] = {
        "storage_object_id": "33333333-3333-4333-8333-333333333333",
        "object_key": path.name,
        "content_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "object_kind": "PARQUET",
        "partition_granularity": "DAY",
        "partition_start": "2024-01-02",
        "partition_end": "2024-01-03",
        "period_start": "2024-01-02T14:30:00Z",
        "period_end": "2024-01-02T15:30:00Z",
        "shard_key": "s00-of-01",
        "part_number": 1,
        "row_count": 2,
        "schema_version": "market-bars-v2",
    }
    return {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": "11111111-1111-4111-8111-111111111111",
        "dataset_id": "22222222-2222-4222-8222-222222222222",
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": _canonical_dataset_hash([object_metadata]),
        "schema_id": "market-bars-v2",
        "period_start": "2024-01-02T14:30:00Z",
        "period_end": "2024-01-02T15:30:00Z",
        "available_at": "2024-01-03T01:00:00Z",
        "objects": [object_metadata],
    }


def test_d17_execution_policy_fixture_is_fully_pinned() -> None:
    policy = D17_EXECUTION_POLICY_FIXTURE

    assert policy.version == "official-backtest-policy-v1"
    assert policy.release_quarter == "2024-Q1"
    assert policy.period_start == datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc)
    assert policy.period_end == datetime(2024, 1, 2, 15, 30, tzinfo=timezone.utc)
    assert policy.fee_rate == Decimal("0.002")
    assert policy.slippage_rate == Decimal("0.0005")
    assert policy.timezone == "America/New_York"
    assert policy.session_calendar == "XNYS"
    assert policy.timestamp_unit == "us"
    assert policy.price_arrow_type == "double"
    assert policy.volume_arrow_type == "int64"
    assert policy.market_data_schema_version == "market-bars-v2"
    assert policy.calculation_model_version == "backtest-calculation-v1"


def test_reader_loads_verified_manifest_objects_in_event_order(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)

    table = ParquetMarketDataReader(tmp_path).read(
        manifest,
        D17_EXECUTION_POLICY_FIXTURE,
    )

    assert table.num_rows == 2
    assert table["provider_symbol"].to_pylist() == ["AAPL", "AAPL"]
    assert table["bar_start_at"].cast(pa.int64()).to_pylist() == [
        1_704_205_800_000_000,
        1_704_207_600_000_000,
    ]


def test_reader_rejects_object_bytes_that_do_not_match_manifest(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)
    fixture.path.write_bytes(fixture.path.read_bytes() + b"tampered")

    with pytest.raises(MarketDataValidationError, match="content_hash"):
        ParquetMarketDataReader(tmp_path).read(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
        )


def test_reader_rejects_bar_with_wrong_et_session_date(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    table = pq.read_table(fixture.path)
    session_index = table.schema.get_field_index("session_date_et")
    table = table.set_column(
        session_index,
        "session_date_et",
        pa.array([date(2024, 1, 3), date(2024, 1, 3)], type=pa.date32()),
    )
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _manifest_for(fixture.path)

    with pytest.raises(MarketDataValidationError, match="session_date_et"):
        ParquetMarketDataReader(tmp_path).read(
            deepcopy(manifest),
            D17_EXECUTION_POLICY_FIXTURE,
        )
