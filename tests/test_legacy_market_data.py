from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.legacy_market_data import (
    legacy_dataset_hash,
    legacy_period_within_policy,
    validate_legacy_market_loader_manifest,
)
from backtest_engine.market_data import MarketDataValidationError, ParquetMarketDataReader
from backtest_engine.object_store.paths import long_path
from backtest_engine.wiring import require_compatible_execution_window
from d_market_data_testkit import write_small_market_bars


FIXTURE = Path(__file__).parent / "fixtures" / "int03-development-market-manifest.json"
PERIOD_START = "2024-01-01T00:00:00Z"
PERIOD_END = "2024-02-01T00:00:00Z"


def _fixture() -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def test_exact_development_manifest_recomputes_the_legacy_loader_hash() -> None:
    manifest = _fixture()

    validate_legacy_market_loader_manifest(manifest)

    assert legacy_dataset_hash(manifest) == ("08a848a5f9aa1aac80e215c2d86bcf6d5f96c354400c7c16394dae9ffa9939af")
    assert sum(item["row_count"] for item in manifest["objects"]) == 546  # type: ignore[index,union-attr]


def test_exact_development_manifest_binds_to_the_one_month_et_policy() -> None:
    policy = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        version="development-official-backtest-2026-q3-v2",
        period_start=datetime(2024, 1, 1, 5, tzinfo=timezone.utc),
        period_end=datetime(2024, 2, 1, 5, tzinfo=timezone.utc),
        market_data_schema_version="market-bars/1",
    )

    require_compatible_execution_window(policy, _fixture(), XNYS_CALENDAR)


def test_legacy_manifest_may_be_one_segment_inside_a_longer_policy() -> None:
    policy = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        version="development-official-backtest-2026-q3-v3",
        period_start=datetime(2024, 1, 1, 5, tzinfo=timezone.utc),
        period_end=datetime(2025, 1, 1, 5, tzinfo=timezone.utc),
        market_data_schema_version="market-bars/1",
    )

    require_compatible_execution_window(policy, _fixture(), XNYS_CALENDAR)


def test_legacy_year_segment_may_extend_past_the_policy_end() -> None:
    assert legacy_period_within_policy(
        {"period_start": "2026-01-01T00:00:00Z", "period_end": "2027-01-01T00:00:00Z"},
        datetime(2016, 1, 1, 5, tzinfo=timezone.utc),
        datetime(2026, 7, 30, 4, tzinfo=timezone.utc),
        "America/New_York",
    )


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document["objects"][4].update(row_count=545),
        lambda document: document["objects"][4].update(provider_version_id=""),
        lambda document: document["objects"][4].update(object_kind="PARQUET"),
        lambda document: document["objects"][4].update(shard_key="s03-of-8"),
        lambda document: document.update(data_layer="RAW"),
    ],
)
def test_legacy_adapter_rejects_tampered_catalog_evidence(mutate: object) -> None:
    manifest = deepcopy(_fixture())
    mutate(manifest)  # type: ignore[operator]

    with pytest.raises(ValueError):
        validate_legacy_market_loader_manifest(manifest)


def _one_shard_manifest(path: Path) -> dict[str, object]:
    key = (
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        "resolution=30m/revision=00000001/year=2024/shard=00-of-01/"
        "manifest_id=11111111-1111-4111-8111-111111111111/part-00001.parquet"
    )
    metadata = {
        "storage_object_id": "22222222-2222-4222-8222-222222222222",
        "object_key": key,
        "content_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "object_kind": "MARKET_BARS",
        "partition_granularity": "YEAR",
        "partition_start": "2024-01-01",
        "partition_end": "2024-02-01",
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "shard_key": "s00-of-1",
        "part_number": 1,
        "row_count": 2,
        "schema_version": "market-bars/1",
    }
    manifest: dict[str, object] = {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": "11111111-1111-4111-8111-111111111111",
        "dataset_id": "11111111-1111-4111-8111-111111111111",
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": "",
        "schema_id": "market-bars/1",
        "provider_code": "ALPACA",
        "feed_code": "ALPACA_SIP_ALL_30M",
        "data_layer": "ADJUSTED",
        "resolution": "30m",
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "available_at": "2026-07-30T06:00:29Z",
        "objects": [metadata],
    }
    manifest["dataset_hash"] = legacy_dataset_hash(manifest)
    return manifest


def test_reader_consumes_legacy_parquet_without_weakening_the_canonical_path(
    tmp_path: Path,
) -> None:
    key_root = (
        tmp_path
        / "historical/provider=alpaca/feed=sip/adjustment=all/session=regular"
        / "resolution=30m/revision=00000001/year=2024/shard=00-of-01"
        / "manifest_id=11111111-1111-4111-8111-111111111111"
    )
    extended_key_root = Path(long_path(key_root))
    extended_key_root.mkdir(parents=True)
    fixture = write_small_market_bars(extended_key_root / "part-00001.parquet")
    table = pq.read_table(fixture.path).replace_schema_metadata(
        {b"schema_version": b"market-bars/1", b"processing_version": b"market-loader/1.0.0"}
    )
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _one_shard_manifest(fixture.path)
    policy = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        version="development-official-backtest-2026-q3-v2",
        period_start=datetime(2024, 1, 1, 5, tzinfo=timezone.utc),
        period_end=datetime(2024, 2, 1, 5, tzinfo=timezone.utc),
        market_data_schema_version="market-bars/1",
    )

    result = ParquetMarketDataReader(tmp_path).read(manifest, policy)

    assert result.num_rows == 2


def test_reader_consumes_one_legacy_segment_inside_a_longer_policy(tmp_path: Path) -> None:
    key_root = (
        tmp_path
        / "historical/provider=alpaca/feed=sip/adjustment=all/session=regular"
        / "resolution=30m/revision=00000001/year=2024/shard=00-of-01"
        / "manifest_id=11111111-1111-4111-8111-111111111111"
    )
    extended_key_root = Path(long_path(key_root))
    extended_key_root.mkdir(parents=True)
    fixture = write_small_market_bars(extended_key_root / "part-00001.parquet")
    table = pq.read_table(fixture.path).replace_schema_metadata(
        {b"schema_version": b"market-bars/1", b"processing_version": b"market-loader/1.0.0"}
    )
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _one_shard_manifest(fixture.path)
    policy = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        period_start=datetime(2024, 1, 1, 5, tzinfo=timezone.utc),
        period_end=datetime(2025, 1, 1, 5, tzinfo=timezone.utc),
        market_data_schema_version="market-bars/1",
    )

    assert ParquetMarketDataReader(tmp_path).read(manifest, policy).num_rows == 2


def test_composite_legacy_manifest_hashes_and_validates_multiple_source_years() -> None:
    manifest = _one_shard_manifest(Path(FIXTURE))
    composite_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    first = deepcopy(manifest["objects"][0])  # type: ignore[index]
    second = deepcopy(first)
    first.update(
        object_key=str(first["object_key"])
        .replace("year=2024", "year=2016")
        .replace(str(manifest["manifest_id"]), "11111111-1111-4111-8111-111111111111"),
        partition_start="2016-01-01",
        partition_end="2017-01-01",
        period_start="2016-01-01T00:00:00Z",
        period_end="2017-01-01T00:00:00Z",
    )
    second.update(
        object_key=str(second["object_key"])
        .replace("year=2024", "year=2017")
        .replace(str(manifest["manifest_id"]), "22222222-2222-4222-8222-222222222222"),
        partition_start="2017-01-01",
        partition_end="2018-01-01",
        period_start="2017-01-01T00:00:00Z",
        period_end="2018-01-01T00:00:00Z",
        storage_object_id="33333333-3333-4333-8333-333333333333",
    )
    manifest.update(
        manifest_id=composite_id,
        dataset_id=composite_id,
        composite=True,
        period_start="2016-01-01T00:00:00Z",
        period_end="2018-01-01T00:00:00Z",
        objects=[first, second],
    )
    manifest["dataset_hash"] = legacy_dataset_hash(manifest)

    validate_legacy_market_loader_manifest(manifest)

    assert len(str(manifest["dataset_hash"])) == 64


def test_canonical_manifest_cannot_claim_the_legacy_object_kind(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _one_shard_manifest(fixture.path)
    manifest["schema_id"] = "market-bars-v2"
    manifest["objects"][0]["partition_granularity"] = "DAY"  # type: ignore[index]

    with pytest.raises(MarketDataValidationError, match="object_kind"):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)
