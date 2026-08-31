from __future__ import annotations

import hashlib
from copy import deepcopy
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.contracts import canonical_dataset_hash
from backtest_engine.event_clock import MarketEventClock
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.market_data import MarketDataValidationError, ParquetMarketDataReader
from backtest_engine.orchestrator import bar_events_from_batches
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
PINNED_DATASET_HASH = "48874949cee30d4c87de042d94ee2e8db70c667a2334780e08430c91e9a422cc"


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


def test_reader_validates_rows_against_the_manifest_not_a_shorter_run_window(
    tmp_path: Path,
) -> None:
    """Boundary partition rows outside this run remain valid immutable source data."""
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)
    shorter_run = replace(
        D17_EXECUTION_POLICY_FIXTURE,
        period_end=datetime(2024, 1, 2, 5, 0, tzinfo=timezone.utc),
    )

    batches = list(ParquetMarketDataReader(tmp_path).iter_batches(manifest, shorter_run))

    assert batches == []


def test_reader_streams_hashing_and_parquet_batches_without_whole_file_helpers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    manifest = _manifest_for(fixture.path)

    def whole_file_read_is_forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("whole-file Parquet reads are forbidden")

    monkeypatch.setattr(Path, "read_bytes", whole_file_read_is_forbidden)
    monkeypatch.setattr(pq, "read_table", whole_file_read_is_forbidden)

    batches = list(
        ParquetMarketDataReader(tmp_path, batch_size=1).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
        )
    )

    assert [batch.num_rows for batch in batches] == [1, 1]
    assert [row["provider_symbol"] for batch in batches for row in batch.to_pylist()] == [
        "AAPL",
        "AAPL",
    ]


def test_reader_filters_verified_batches_to_requested_plan_instruments(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    source = pq.read_table(fixture.path)
    other_id = "22222222-2222-4222-8222-222222222222"
    other = source.set_column(
        source.schema.get_field_index("instrument_id"),
        source.schema.field("instrument_id"),
        pa.array([other_id] * source.num_rows, type=pa.string()),
    )
    combined = pa.concat_tables([source, other])
    pq.write_table(combined, fixture.path, compression="zstd", version="2.6")
    manifest = _manifest_for(fixture.path)
    manifest["objects"][0]["row_count"] = combined.num_rows  # type: ignore[index]
    manifest["dataset_hash"] = canonical_dataset_hash(manifest["objects"])  # type: ignore[arg-type]
    requested_id = source["instrument_id"][0].as_py()

    batches = list(
        ParquetMarketDataReader(tmp_path, batch_size=3).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
            instrument_ids=frozenset({requested_id}),
        )
    )

    assert sum(batch.num_rows for batch in batches) == source.num_rows
    assert {row["instrument_id"] for batch in batches for row in batch.to_pylist()} == {requested_id}


def test_reader_decodes_full_columns_only_for_objects_containing_requested_instruments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_fixture = write_small_market_bars(tmp_path / "target.parquet")
    target = pq.read_table(target_fixture.path)
    other_id = "22222222-2222-4222-8222-222222222222"
    unrelated_source = pa.concat_tables([target, target])
    unrelated = unrelated_source.set_column(
        unrelated_source.schema.get_field_index("instrument_id"),
        unrelated_source.schema.field("instrument_id"),
        pa.array([other_id] * unrelated_source.num_rows, type=pa.string()),
    )
    unrelated_path = tmp_path / "unrelated.parquet"
    pq.write_table(unrelated, unrelated_path, compression="zstd", version="2.6")

    manifest = _manifest_for(target_fixture.path)
    unrelated_object = deepcopy(manifest["objects"][0])
    unrelated_object.update(
        {
            "storage_object_id": "99999999-9999-4999-8999-999999999999",
            "object_key": unrelated_path.name,
            "content_hash": hashlib.sha256(unrelated_path.read_bytes()).hexdigest(),
            "row_count": unrelated.num_rows,
            "shard_key": "s01-of-2",
        }
    )
    manifest["objects"][0]["shard_key"] = "s00-of-2"
    manifest["objects"] = [unrelated_object, manifest["objects"][0]]
    manifest["dataset_hash"] = canonical_dataset_hash(manifest["objects"])
    requested_id = target["instrument_id"][0].as_py()

    original = pq.ParquetFile.iter_batches

    def record_column_reads(parquet, *args, **kwargs):
        if parquet.metadata.num_rows == unrelated.num_rows:
            raise AssertionError("canonical unrelated shard decoded any columns")
        return original(parquet, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", record_column_reads)

    batches = list(
        ParquetMarketDataReader(tmp_path).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
            instrument_ids=frozenset({requested_id}),
        )
    )

    assert sum(batch.num_rows for batch in batches) == target.num_rows


def test_reader_hashes_only_canonical_shards_consumed_by_requested_instruments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target_fixture = write_small_market_bars(tmp_path / "target.parquet")
    target = pq.read_table(target_fixture.path)
    unrelated_path = tmp_path / "unrelated.parquet"
    pq.write_table(target, unrelated_path, compression="zstd", version="2.6")
    manifest = _manifest_for(target_fixture.path)
    unrelated_object = deepcopy(manifest["objects"][0])
    unrelated_object.update(
        {
            "storage_object_id": "99999999-9999-4999-8999-999999999999",
            "object_key": unrelated_path.name,
            "content_hash": hashlib.sha256(unrelated_path.read_bytes()).hexdigest(),
            "shard_key": "s01-of-2",
        }
    )
    manifest["objects"][0]["shard_key"] = "s00-of-2"
    manifest["objects"] = [unrelated_object, manifest["objects"][0]]
    manifest["dataset_hash"] = canonical_dataset_hash(manifest["objects"])
    requested_id = target["instrument_id"][0].as_py()
    hashed: list[str] = []
    original = ParquetMarketDataReader._content_hash

    def record_hash(path: Path) -> str:
        hashed.append(path.name)
        return original(path)

    monkeypatch.setattr(ParquetMarketDataReader, "_content_hash", staticmethod(record_hash))

    batches = list(
        ParquetMarketDataReader(tmp_path).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
            instrument_ids=frozenset({requested_id}),
        )
    )

    assert sum(batch.num_rows for batch in batches) == target.num_rows
    assert hashed == [target_fixture.path.name]


def test_reader_decodes_only_row_groups_whose_statistics_cover_requested_instrument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    target = pq.read_table(fixture.path)
    other_id = "22222222-2222-4222-8222-222222222222"
    other = target.set_column(
        target.schema.get_field_index("instrument_id"),
        target.schema.field("instrument_id"),
        pa.array([other_id] * target.num_rows, type=pa.string()),
    )
    combined = pa.concat_tables([target, other])
    pq.write_table(combined, fixture.path, compression="zstd", version="2.6", row_group_size=2)
    manifest = _manifest_for(fixture.path)
    manifest["objects"][0]["row_count"] = combined.num_rows  # type: ignore[index]
    manifest["dataset_hash"] = canonical_dataset_hash(manifest["objects"])  # type: ignore[arg-type]
    requested_id = target["instrument_id"][0].as_py()
    full_column_row_groups: list[list[int] | None] = []
    original = pq.ParquetFile.iter_batches

    def record_row_groups(parquet, *args, **kwargs):
        if kwargs.get("columns") is None:
            full_column_row_groups.append(kwargs.get("row_groups"))
        return original(parquet, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "iter_batches", record_row_groups)

    batches = list(
        ParquetMarketDataReader(tmp_path).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
            instrument_ids=frozenset({requested_id}),
        )
    )

    assert sum(batch.num_rows for batch in batches) == target.num_rows
    assert full_column_row_groups == [[0]]


def test_reader_accepts_eight_shard_instrument_major_data_and_clock_orders_events(
    tmp_path: Path,
) -> None:
    """Pin the actual INT03 publication shape through the production event boundary.

    Seven shards are empty and shard 04 contains two instrument-major series. Its
    timestamps reset once at the instrument boundary, exactly as the producer's
    canonical ``instrument_id, bar_start_at`` ordering requires. The event clock,
    not the bounded Parquet reader, establishes global market-time order.
    """
    fixture = write_small_market_bars(tmp_path / "source.parquet")
    source = pq.read_table(fixture.path)
    objects: list[dict[str, object]] = []
    for shard in range(8):
        path = tmp_path / f"shard-{shard:02d}.parquet"
        shard_table = source.slice(0, 0)
        if shard == 4:
            instruments = []
            for instrument, symbol in (
                ("11111111-1111-4111-8111-111111111111", "AAPL"),
                ("22222222-2222-4222-8222-222222222222", "MSFT"),
            ):
                table = source.set_column(
                    source.schema.get_field_index("instrument_id"),
                    source.schema.field("instrument_id"),
                    pa.array([instrument] * source.num_rows, type=pa.string()),
                )
                table = table.set_column(
                    table.schema.get_field_index("provider_symbol"),
                    table.schema.field("provider_symbol"),
                    pa.array([symbol] * table.num_rows, type=pa.string()),
                )
                instruments.append(table)
            shard_table = pa.concat_tables(instruments)
        shard_table = shard_table.replace_schema_metadata(
            {**(shard_table.schema.metadata or {}), b"shard_provenance": f"s{shard:02d}".encode()}
        )
        pq.write_table(shard_table, path, compression="zstd", version="2.6")
        objects.append(
            dict(
                PINNED_OBJECT,
                storage_object_id=f"33333333-3333-4333-8333-{shard + 1:012d}",
                object_key=path.name,
                content_hash=hashlib.sha256(path.read_bytes()).hexdigest(),
                shard_key=f"s{shard:02d}-of-08",
                row_count=shard_table.num_rows,
            )
        )
    manifest = _manifest_for(tmp_path / "shard-00.parquet")
    manifest["objects"] = objects
    manifest["dataset_hash"] = canonical_dataset_hash(objects)

    batches = list(
        ParquetMarketDataReader(tmp_path, batch_size=3).iter_batches(
            manifest,
            D17_EXECUTION_POLICY_FIXTURE,
        )
    )
    table = pa.Table.from_batches(batches)
    events = bar_events_from_batches(batches, data_kind="BAR", resolution="30m")

    assert table.num_rows == 4
    assert list(
        zip(
            table["instrument_id"].to_pylist(),
            table["bar_start_at"].cast(pa.int64()).to_pylist(),
            strict=True,
        )
    ) == sorted(
        zip(
            table["instrument_id"].to_pylist(),
            table["bar_start_at"].cast(pa.int64()).to_pylist(),
            strict=True,
        )
    )
    assert table["provider_symbol"].to_pylist() == ["AAPL", "AAPL", "MSFT", "MSFT"]
    assert len(events) == 4
    schedule = XNYS_CALENDAR.session_schedule(date(2024, 1, 1), date(2024, 3, 31))
    released = MarketEventClock(schedule, events).advance_to(datetime(2024, 1, 3, tzinfo=timezone.utc)).released_events
    assert [event.occurred_at for event in released] == sorted(event.occurred_at for event in released)


def test_reader_still_rejects_rows_out_of_order_inside_one_shard(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    table = pq.read_table(fixture.path).take(pa.array([1, 0]))
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _manifest_for(fixture.path)

    with pytest.raises(MarketDataValidationError, match="uniquely ordered"):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)


def test_reader_rejects_duplicate_instrument_timestamp_key(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    table = pq.read_table(fixture.path).take(pa.array([0, 0]))
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _manifest_for(fixture.path)

    with pytest.raises(MarketDataValidationError, match="uniquely ordered"):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)


def test_reader_rejects_instrument_order_regression(tmp_path: Path) -> None:
    fixture = write_small_market_bars(tmp_path / "market-bars.parquet")
    source = pq.read_table(fixture.path)
    later_instrument = source.set_column(
        source.schema.get_field_index("instrument_id"),
        source.schema.field("instrument_id"),
        pa.array(["22222222-2222-4222-8222-222222222222"] * source.num_rows),
    )
    table = pa.concat_tables([later_instrument, source]).take(pa.array([0, 2]))
    pq.write_table(table, fixture.path, compression="zstd", version="2.6")
    manifest = _manifest_for(fixture.path)

    with pytest.raises(MarketDataValidationError, match="uniquely ordered"):
        ParquetMarketDataReader(tmp_path).read(manifest, D17_EXECUTION_POLICY_FIXTURE)


def test_reader_verifies_every_object_before_yielding_any_rows(tmp_path: Path) -> None:
    first = write_small_market_bars(tmp_path / "first.parquet")
    second = write_small_market_bars(tmp_path / "second.parquet")
    objects = [
        dict(
            PINNED_OBJECT,
            storage_object_id=f"33333333-3333-4333-8333-{index:012d}",
            object_key=fixture.path.name,
            content_hash=hashlib.sha256(fixture.path.read_bytes()).hexdigest(),
            shard_key=f"s{index - 1:02d}-of-02",
        )
        for index, fixture in enumerate((first, second), start=1)
    ]
    manifest = _manifest_for(first.path)
    manifest["objects"] = objects
    manifest["dataset_hash"] = canonical_dataset_hash(objects)
    second.path.write_bytes(second.path.read_bytes() + b"tampered")

    batches = ParquetMarketDataReader(tmp_path).iter_batches(
        manifest,
        D17_EXECUTION_POLICY_FIXTURE,
    )
    with pytest.raises(MarketDataValidationError, match="content_hash"):
        next(batches)


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
