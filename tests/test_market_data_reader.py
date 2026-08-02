from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.contracts import canonical_dataset_hash
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.market_data import MarketDataValidationError, ParquetMarketDataReader
from d_market_data_testkit import write_small_market_bars


# The manifest period must equal the policy's pinned evaluation window, which
# is now the whole 2024-Q1 ET calendar quarter.
PERIOD_START = "2024-01-01T05:00:00Z"
PERIOD_END = "2024-04-01T04:00:00Z"

# Object metadata with a fixed content_hash so the dataset-hash canonicalisation
# is pinned to a literal that cannot drift with the Parquet writer. The live
# manifest below reuses the same shape with the real object bytes.
PINNED_OBJECT: dict[str, object] = {
    "storage_object_id": "33333333-3333-4333-8333-333333333333",
    "object_key": "market-bars.parquet",
    "content_hash": "a" * 64,
    "object_kind": "PARQUET",
    "partition_granularity": "DAY",
    "partition_start": "2024-01-02",
    "partition_end": "2024-01-03",
    "period_start": PERIOD_START,
    "period_end": PERIOD_END,
    "shard_key": "s00-of-01",
    "part_number": 1,
    "row_count": 2,
    "schema_version": "market-bars-v2",
}
PINNED_DATASET_HASH = (
    "48874949cee30d4c87de042d94ee2e8db70c667a2334780e08430c91e9a422cc"
)


def _manifest_for(path: Path) -> dict[str, object]:
    object_metadata = dict(
        PINNED_OBJECT,
        object_key=path.name,
        content_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
    )
    return {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": "11111111-1111-4111-8111-111111111111",
        "dataset_id": "22222222-2222-4222-8222-222222222222",
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": canonical_dataset_hash([object_metadata]),
        "schema_id": "market-bars-v2",
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "available_at": "2024-04-02T01:00:00Z",
        "objects": [object_metadata],
    }


def test_manifest_dataset_hash_canonicalisation_is_pinned_to_a_literal() -> None:
    """If canonicalisation drifts, every stored dataset_hash silently rots."""
    assert canonical_dataset_hash([PINNED_OBJECT]) == PINNED_DATASET_HASH


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


def test_reader_rejects_manifest_whose_dataset_hash_disagrees_with_its_objects(
    tmp_path: Path,
) -> None:
    """The consumer re-derives the hash; it never trusts the declared field."""
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)
    manifest["objects"][0]["row_count"] = 3  # type: ignore[index]

    with pytest.raises(MarketDataValidationError, match="dataset_hash"):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"status": "PENDING"}, "status"),
        ({"objects": []}, "objects"),
        ({"revision": 0}, "revision"),
        ({"manifest_id": "not-a-uuid"}, "manifest_id"),
    ],
)
def test_reader_rejects_manifests_it_cannot_safely_consume(
    tmp_path: Path, mutation: dict[str, object], message: str
) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)
    manifest.update(mutation)

    with pytest.raises(MarketDataValidationError, match=message):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)


def test_producer_schema_validator_is_applied_when_supplied(tmp_path: Path) -> None:
    """Spec 2.1 keeps the dataset-manifest JSON Schema in the producer repo.

    The reader therefore validates only what it consumes and delegates the
    authoritative contract check to an injected validator.
    """
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)
    seen: list[object] = []

    def producer_validator(document: object) -> None:
        seen.append(document)
        raise ValueError("producer schema says no")

    reader = ParquetMarketDataReader(tmp_path, manifest_validator=producer_validator)
    with pytest.raises(MarketDataValidationError, match="producer schema says no"):
        reader.read(manifest, D17_EXECUTION_POLICY_FIXTURE)

    assert seen == [manifest]


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
