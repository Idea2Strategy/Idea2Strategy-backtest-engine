"""Canonical detail Parquet objects: ET Monday weeks, parts, UNCOMPRESSED (card D27).

The previous version of this file asserted an **ET month** partition, which the
canonical `backtest.detail_manifests` unique key
`(run_id, record_type, week_start_date, part_number)` cannot even address, and it
contained an assertion that could not fail (spec 4 flags lines 190-207:
`repr(record)` was searched for `"performance_summary"`, a string the manifest never
builds). Both are replaced here:

* the partition is the **ET Monday week**, proven at the boundary instant rather than
  by naming it,
* the manifest assertion is now against the canonical `DetailManifestRow` values.
"""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.detail_object_manifest import (
    COMPRESSION_CODEC,
    SCHEMA_VERSION,
    DetailIntegrityError,
    DetailManifestConflict,
    DetailObjectBuilder,
    DetailObjectKind,
    DetailObjectPublisher,
    DetailObjectValidationError,
    EtWeek,
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
from backtest_engine.object_store import (
    InMemoryStorageObjectRegistry,
    LocalObjectStore,
    ObjectStatus,
    StorageWriteNotAuthorized,
    UnauthorizedStorageObjectWritePort,
    long_path,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshotBuilder,
    RunSnapshot,
)


RUN_ID = "00000000-0000-4000-8000-000000001001"
OTHER_RUN_ID = "00000000-0000-4000-8000-0000000019ff"
STRATEGY_ID = "00000000-0000-4000-8000-000000001002"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000001003"

#: Two ET Monday weeks. `2025-11-01T03:30:00Z` is Friday 2025-10-31 23:30 in ET, so it
#: belongs to the week that started Monday 2025-10-27 even though it is November in UTC
#: — the exact case the old ET-month partition got wrong.
WEEK_A = date(2025, 10, 27)
WEEK_B = date(2025, 11, 3)

RECORD_A = "00000000-0000-4000-8000-000000001011"
ORDER_A = "00000000-0000-4000-8000-000000001012"
RECORD_B = "00000000-0000-4000-8000-000000001013"
ORDER_B = "00000000-0000-4000-8000-000000001014"
RECORD_C = "00000000-0000-4000-8000-000000001015"
ORDER_C = "00000000-0000-4000-8000-000000001016"

AT_A = "2025-11-01T03:30:00Z"  # ET Fri 2025-10-31 23:30 -> week 2025-10-27
AT_B = "2025-11-03T14:30:00Z"  # ET Mon 2025-11-03 09:30 -> week 2025-11-03
AT_C = "2025-11-03T15:30:00Z"  # ET Mon 2025-11-03 10:30 -> week 2025-11-03
CREATED_AT = "2025-11-03T21:00:00Z"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _run(fingerprint: str = "a", run_id: str = RUN_ID) -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=run_id,
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
    run_id: str = RUN_ID,
) -> ResultRecord:
    positions = (PositionAfter(INSTRUMENT_ID, Decimal("2"), Decimal("200")),) if position else ()
    return ResultRecord(
        run_snapshot_id=_run(run_id=run_id).snapshot_id,
        record_id=record_id,
        kind=ResultRecordKind.ORDER,
        occurred_at=_instant(occurred_at),
        order_id=order_id,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.ACCEPTED,
        cash_after=Decimal("9800") if position else Decimal("10000"),
        positions_after=positions,
    )


def _result(records: list[ResultRecord] | None = None, run_id: str = RUN_ID):
    if records is None:
        records = [
            _record(RECORD_A, ORDER_A, AT_A, position=True, run_id=run_id),
            _record(RECORD_B, ORDER_B, AT_B, run_id=run_id),
        ]
    return ResultSnapshotBuilder().build(_run(run_id=run_id), records, _instant(CREATED_AT))


def _ledger(
    transaction_id: str = "00000000-0000-4000-8000-000000001021",
    posted_at: str = "2025-11-03T15:00:00Z",
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
                Decimal("100.00000000"),
                source_id,
            ),
            LedgerEntry(
                "00000000-0000-4000-8000-000000001024",
                "SECURITY",
                LedgerDirection.CREDIT,
                Decimal("100.00000000"),
                source_id,
            ),
        ),
    )
    return ReplayLedgerDetail(_run().snapshot_id, transaction)


def _point(
    point_id: str = "00000000-0000-4000-8000-000000001031",
    occurred_at: str = "2025-11-03T20:00:00Z",
    value: str = "10123.45",
) -> PerformancePoint:
    return PerformancePoint(
        point_id=point_id,
        run_snapshot_id=_run().snapshot_id,
        occurred_at=_instant(occurred_at),
        metric_id="equity",
        value=Decimal(value),
        instrument_id=None,
    )


def _bundle(builder: DetailObjectBuilder | None = None):
    return (builder or DetailObjectBuilder()).build(
        _result(), [_ledger()], [_point()], _instant(CREATED_AT)
    )


# --------------------------------------------------------------------------------
# partitioning
# --------------------------------------------------------------------------------


def test_builds_parquet_objects_partitioned_by_et_monday_week_and_record_type() -> None:
    bundle = _bundle()

    partitions = {
        (item.descriptor.week.key, item.descriptor.record_type, item.descriptor.part_number)
        for item in bundle.objects
    }
    assert partitions == {
        ("2025-10-27", DetailObjectKind.TRADE_DETAIL, 1),
        ("2025-10-27", DetailObjectKind.POSITION_SNAPSHOT, 1),
        ("2025-11-03", DetailObjectKind.TRADE_DETAIL, 1),
        ("2025-11-03", DetailObjectKind.REPLAY_LEDGER, 1),
        ("2025-11-03", DetailObjectKind.CALCULATION_SERIES, 1),
    }
    assert all(item.parquet_bytes[:4] == b"PAR1" for item in bundle.objects)
    assert all(item.parquet_bytes[-4:] == b"PAR1" for item in bundle.objects)
    # No object may straddle the boundary: every week here is a Monday and the two
    # trade objects are in different weeks even though both instants are in UTC
    # November.
    assert {item.descriptor.week.start_date.weekday() for item in bundle.objects} == {0}


def test_detail_builder_writes_parts_through_parquet_writer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def whole_table_writer_is_forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("detail parts must use ParquetWriter")

    monkeypatch.setattr(pq, "write_table", whole_table_writer_is_forbidden)

    bundle = _bundle(DetailObjectBuilder(max_rows_per_part=1))

    assert bundle.objects
    assert all(item.parquet_bytes[:4] == b"PAR1" for item in bundle.objects)


@pytest.mark.parametrize(
    ("instant", "expected_week"),
    [
        ("2025-11-03T04:59:59Z", date(2025, 10, 27)),  # EST: ET Sun 2025-11-02 23:59:59
        ("2025-11-03T05:00:00Z", date(2025, 11, 3)),  # EST: ET Mon 2025-11-03 00:00:00
        ("2025-10-27T03:59:59Z", date(2025, 10, 20)),  # EDT: ET Sun 2025-10-26 23:59:59
        ("2025-10-27T04:00:00Z", date(2025, 10, 27)),  # EDT: ET Mon 2025-10-27 00:00:00
    ],
)
def test_week_boundary_is_et_midnight_monday_across_the_dst_change(
    instant: str, expected_week: date
) -> None:
    """The boundary is ET local midnight on Monday, not UTC and not a fixed offset.

    2025-11-02 is the EDT->EST change, so the two Monday boundaries in this test are at
    different UTC instants (04:00Z in October, 05:00Z in November). A UTC-week or
    fixed-offset implementation puts at least one of these four instants in the wrong
    object.
    """

    assert EtWeek.from_instant(_instant(instant)).start_date == expected_week


def test_a_week_is_split_into_ordered_parts_that_do_not_overlap_in_time() -> None:
    result = _result(
        [
            _record(RECORD_B, ORDER_B, AT_B),
            _record(RECORD_C, ORDER_C, AT_C),
        ]
    )

    bundle = DetailObjectBuilder(max_rows_per_part=1).build(result, [], [], _instant(CREATED_AT))

    parts = sorted(
        (item.descriptor for item in bundle.objects if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL),
        key=lambda item: item.part_number,
    )
    assert [item.part_number for item in parts] == [1, 2]
    assert {item.week.start_date for item in parts} == {WEEK_B}
    assert [item.row_count for item in parts] == [1, 1]
    assert parts[0].period_end == _instant(AT_B)
    assert parts[1].period_start == _instant(AT_C)
    assert parts[0].period_end < parts[1].period_start
    assert parts[0].object_key.split("/")[-2] == "part=0001"
    assert parts[1].object_key.split("/")[-2] == "part=0002"


def test_object_key_is_the_canonical_spec_2_5_backtest_result_key() -> None:
    bundle = _bundle()
    trade_a = next(
        item
        for item in bundle.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
        and item.descriptor.week.start_date == WEEK_A
    )

    content_hash = hashlib.sha256(trade_a.parquet_bytes).hexdigest()

    assert trade_a.descriptor.content_hash == content_hash
    assert trade_a.descriptor.object_key == (
        f"backtest-results/{RUN_ID}/TRADE_DETAIL/week_start=2025-10-27/part=0001/{content_hash}.parquet"
    )


# --------------------------------------------------------------------------------
# Parquet evidence
# --------------------------------------------------------------------------------


def test_parquet_is_written_uncompressed_and_says_so_in_its_footer() -> None:
    """`zstd` was a canonical-source violation; UNCOMPRESSED is asserted from the file."""

    bundle = _bundle()

    codecs = set()
    for item in bundle.objects:
        parquet = pq.ParquetFile(pa.BufferReader(item.parquet_bytes))
        for group in range(parquet.metadata.num_row_groups):
            row_group = parquet.metadata.row_group(group)
            for column in range(row_group.num_columns):
                codecs.add(row_group.column(column).compression)
        assert item.descriptor.compression_codec == "UNCOMPRESSED"
    assert codecs == {"UNCOMPRESSED"}
    assert COMPRESSION_CODEC == "UNCOMPRESSED"


def test_parquet_metadata_and_rows_preserve_exact_snapshot_evidence() -> None:
    result = _result()
    bundle = DetailObjectBuilder().build(result, [_ledger()], [_point()], _instant(CREATED_AT))
    trade = next(
        item
        for item in bundle.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
        and item.descriptor.week.start_date == WEEK_A
    )
    parquet = pq.ParquetFile(pa.BufferReader(trade.parquet_bytes))
    metadata = parquet.schema_arrow.metadata

    assert metadata is not None
    assert metadata[b"run_snapshot_id"].decode() == _run().snapshot_id
    assert metadata[b"backtest_run_id"].decode() == RUN_ID
    assert metadata[b"strategy_version_id"].decode() == STRATEGY_ID
    assert metadata[b"result_manifest_id"].decode() == result.manifest.result_manifest_id
    assert metadata[b"week_start_date"] == b"2025-10-27"
    assert metadata[b"record_type"] == b"TRADE_DETAIL"
    assert metadata[b"part_number"] == b"1"
    assert metadata[b"timezone_id"] == b"America/New_York"
    assert metadata[b"compression_codec"] == b"UNCOMPRESSED"
    assert metadata[b"schema_version"].decode() == SCHEMA_VERSION

    rows = parquet.read().to_pylist()
    assert [row["record_id"] for row in rows] == [RECORD_A]
    assert rows[0]["occurred_at"] == _instant(AT_A)
    # numeric(24,8) text, quantized exactly once through money.py (spec 2.3).
    assert rows[0]["cash_after"] == "9800.00000000"
    assert rows[0]["order_status"] == "ACCEPTED"


def test_position_rows_are_a_separate_record_type_in_the_same_week() -> None:
    bundle = _bundle()
    positions = next(
        item for item in bundle.objects if item.descriptor.record_type is DetailObjectKind.POSITION_SNAPSHOT
    )

    rows = pq.read_table(pa.BufferReader(positions.parquet_bytes)).to_pylist()

    assert positions.descriptor.week.start_date == WEEK_A
    assert rows == [
        {
            "record_id": RECORD_A,
            "occurred_at": _instant(AT_A),
            "instrument_id": INSTRUMENT_ID,
            "quantity": "2.00000000",
            "cost_basis": "200.00000000",
            "cash_after": "9800.00000000",
        }
    ]


# --------------------------------------------------------------------------------
# the relational manifest
# --------------------------------------------------------------------------------


def test_manifest_rows_are_canonical_detail_manifest_rows_keyed_by_week_and_part() -> None:
    """Replaces the old `repr(record)` assertion, which no input could make fail.

    This asserts the values the canonical table actually stores, including its unique
    key `(run_id, record_type, week_start_date, part_number)` and the `object_id` link
    into `storage.objects`.
    """

    result = _result()
    bundle = DetailObjectBuilder().build(result, [_ledger()], [_point()], _instant(CREATED_AT))

    rows = bundle.manifest.as_rows()

    assert len(rows) == len(bundle.objects)
    assert {(row.record_type, row.week_start_date, row.part_number) for row in rows} == {
        ("TRADE_DETAIL", WEEK_A, 1),
        ("POSITION_SNAPSHOT", WEEK_A, 1),
        ("TRADE_DETAIL", WEEK_B, 1),
        ("REPLAY_LEDGER", WEEK_B, 1),
        ("CALCULATION_SERIES", WEEK_B, 1),
    }
    # The canonical unique key and the one-object-per-manifest-row rule.
    keys = [(row.run_id, row.record_type, row.week_start_date, row.part_number) for row in rows]
    assert len(set(keys)) == len(keys)
    assert len({row.object_id for row in rows}) == len(rows)
    assert {row.run_id for row in rows} == {UUID(RUN_ID)}
    assert {row.schema_version for row in rows} == {SCHEMA_VERSION}
    assert all(row.row_count >= 1 for row in rows)
    assert all(row.period_start <= row.period_end for row in rows)
    assert all(row.supersedes_manifest_id is None for row in rows)

    by_key = {(row.record_type, row.week_start_date): row for row in rows}
    trade_a = by_key[("TRADE_DETAIL", WEEK_A)]
    assert trade_a.row_count == 1
    assert trade_a.period_start == _instant(AT_A)
    assert trade_a.period_end == _instant(AT_A)
    assert trade_a.object_id == UUID(
        next(
            item.descriptor.storage_object_id
            for item in bundle.objects
            if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
            and item.descriptor.week.start_date == WEEK_A
        )
    )
    ledger_row = by_key[("REPLAY_LEDGER", WEEK_B)]
    assert ledger_row.row_count == 2, "one manifest row counts both balanced ledger entries"


def test_manifest_hashes_are_pinned_and_move_when_any_evidence_moves() -> None:
    """Determinism against literals, not against a second call to the same code.

    `first == second` alone is satisfied by an implementation that returns a constant,
    so the expected digests are pinned here. They are the digests of the canonical
    model (ET Monday week + part_number + UNCOMPRESSED + numeric(24,8) text); the
    previous ET-month/zstd implementation produced different ones, which is the point.

    `source_set_hash` covers only source-row identities, so it is stable regardless of
    the Parquet writer. `manifest_hash` covers the object bytes, so it is pinned
    together with the `pyarrow==25.0.0` pin in `pyproject.toml`: if that pin moves, the
    published object bytes move too and this literal has to be re-derived deliberately
    rather than silently.
    """

    assert pa.__version__ == "25.0.0", "the pinned manifest_hash below is for this writer"

    bundle = _bundle()

    assert bundle.manifest.source_set_hash == (
        "9ab1e323e31b226640236d334e5b41c75d6131ab7a0696a68c7ec8edad62a4c8"
    )
    assert bundle.manifest.manifest_hash == (
        "f4975077ae1495b54ead05c52d1cb298b98b360f9b2e82e6d571eeb4fe57b55f"
    )
    assert bundle.manifest.detail_manifest_id == "d0b29960-e5be-5e6f-b5ea-8e2d7546ee93"
    assert {
        (item.descriptor.record_type.value, item.descriptor.week.key): item.descriptor.content_hash
        for item in bundle.objects
    } == {
        ("TRADE_DETAIL", "2025-10-27"): "9ba23b7a204dddff5f40c74beb66f0176d888e473490ac1bf65728e5f62e2414",
        ("POSITION_SNAPSHOT", "2025-10-27"): "df25642d74818f3480835b1ca1540821261c98aa5fece58a197f5e0d27fac7d5",
        ("TRADE_DETAIL", "2025-11-03"): "d8dee7f722f424eea2bdbb09ce8924a5195568241ef67e55ef88807244883a2d",
        ("REPLAY_LEDGER", "2025-11-03"): "f5126d3687b40937674b758a12454b1cbc89883515f7419aa7951ebcceb70b7f",
        ("CALCULATION_SERIES", "2025-11-03"): "b47e746a768dd685fe0331cbb80412b442666fd248cbae475d9e73a150adf460",
    }

    # ... and the digest is a function of the evidence: one changed metric value moves
    # the part hash, the manifest hash and the manifest identity.
    changed = DetailObjectBuilder().build(
        _result(), [_ledger()], [_point(value="10123.46")], _instant(CREATED_AT)
    )
    assert changed.manifest.manifest_hash != bundle.manifest.manifest_hash
    assert changed.manifest.source_set_hash == bundle.manifest.source_set_hash, (
        "the same source rows keep the same source_set_hash; only the values moved"
    )
    assert changed.manifest.detail_manifest_id != bundle.manifest.detail_manifest_id


def test_output_is_deterministic_regardless_of_input_order() -> None:
    ledger = _ledger()
    other_ledger = _ledger("00000000-0000-4000-8000-000000001025", "2025-11-03T15:01:00Z")
    other_ledger = replace(
        other_ledger,
        transaction=replace(
            other_ledger.transaction,
            entries=tuple(
                replace(entry, entry_id=f"00000000-0000-4000-8000-00000000102{index + 6}")
                for index, entry in enumerate(other_ledger.transaction.entries)
            ),
        ),
    )
    point = _point()
    other_point = _point("00000000-0000-4000-8000-000000001032", "2025-11-03T20:01:00Z")
    builder = DetailObjectBuilder()

    first = builder.build(
        _result(), [ledger, other_ledger], [point, other_point], _instant(CREATED_AT)
    )
    second = builder.build(
        _result(), [other_ledger, ledger], [other_point, point], _instant(CREATED_AT)
    )

    assert first == second
    assert first.manifest.manifest_hash == second.manifest.manifest_hash
    assert [item.descriptor.content_hash for item in first.objects] == [
        item.descriptor.content_hash for item in second.objects
    ]


# --------------------------------------------------------------------------------
# supersede chain
# --------------------------------------------------------------------------------


def test_reissued_detail_records_a_supersede_chain_only_where_bytes_changed() -> None:
    original = _bundle()
    reissued = DetailObjectBuilder().build(
        _result(),
        [_ledger()],
        [_point(value="10200.00")],
        _instant("2025-11-04T21:00:00Z"),
        supersedes=original,
    )

    previous = {
        (item.descriptor.record_type, item.descriptor.week): item.descriptor for item in original.objects
    }
    corrected = [
        item.descriptor for item in reissued.objects if item.descriptor.correction_of_object_id is not None
    ]

    assert [item.record_type for item in corrected] == [DetailObjectKind.CALCULATION_SERIES]
    correction = corrected[0]
    superseded = previous[(DetailObjectKind.CALCULATION_SERIES, EtWeek(WEEK_B))]
    assert correction.correction_of_object_id == superseded.storage_object_id
    assert correction.base_object_id == superseded.storage_object_id
    assert correction.supersedes_manifest_id == superseded.detail_manifest_id
    assert correction.content_hash != superseded.content_hash

    # Untouched weeks are not corrections and keep their published identity.
    trade_a = next(
        item.descriptor
        for item in reissued.objects
        if item.descriptor.record_type is DetailObjectKind.TRADE_DETAIL
        and item.descriptor.week.start_date == WEEK_A
    )
    assert trade_a.correction_of_object_id is None
    assert trade_a.base_object_id is None
    assert trade_a.storage_object_id == previous[(DetailObjectKind.TRADE_DETAIL, EtWeek(WEEK_A))].storage_object_id
    assert reissued.manifest.supersedes_manifest_id == original.manifest.detail_manifest_id


def test_a_third_issue_keeps_pointing_at_the_original_base_object() -> None:
    original = _bundle()
    second = DetailObjectBuilder().build(
        _result(),
        [_ledger()],
        [_point(value="10200.00")],
        _instant("2025-11-04T21:00:00Z"),
        supersedes=original,
    )
    third = DetailObjectBuilder().build(
        _result(),
        [_ledger()],
        [_point(value="10300.00")],
        _instant("2025-11-05T21:00:00Z"),
        supersedes=second,
    )

    def series(bundle):
        return next(
            item.descriptor
            for item in bundle.objects
            if item.descriptor.record_type is DetailObjectKind.CALCULATION_SERIES
        )

    assert series(third).base_object_id == series(original).storage_object_id
    assert series(third).correction_of_object_id == series(second).storage_object_id
    assert series(third).supersedes_manifest_id == series(second).detail_manifest_id


def test_a_correction_cannot_supersede_another_run_or_travel_backwards() -> None:
    foreign = DetailObjectBuilder().build(
        _result(run_id=OTHER_RUN_ID), [], [], _instant(CREATED_AT)
    )
    later = DetailObjectBuilder().build(
        _result(), [_ledger()], [_point()], _instant("2025-11-05T21:00:00Z")
    )

    assert foreign.manifest.backtest_run_id == OTHER_RUN_ID
    with pytest.raises(DetailObjectValidationError, match="same backtest run"):
        DetailObjectBuilder().build(
            _result(),
            [_ledger()],
            [_point()],
            _instant("2025-11-06T21:00:00Z"),
            supersedes=foreign,
        )
    with pytest.raises(DetailObjectValidationError, match="predate"):
        DetailObjectBuilder().build(
            _result(),
            [_ledger()],
            [_point(value="10200.00")],
            _instant("2025-11-04T21:00:00Z"),
            supersedes=later,
        )


def test_a_tampered_predecessor_cannot_be_used_as_a_supersede_target() -> None:
    original = _bundle()
    tampered = replace(
        original,
        objects=(
            replace(original.objects[0], parquet_bytes=original.objects[0].parquet_bytes + b"x"),
            *original.objects[1:],
        ),
    )

    with pytest.raises(DetailIntegrityError):
        DetailObjectBuilder().build(
            _result(),
            [_ledger()],
            [_point(value="10200.00")],
            _instant("2025-11-04T21:00:00Z"),
            supersedes=tampered,
        )


# --------------------------------------------------------------------------------
# fail-closed verification
# --------------------------------------------------------------------------------


def test_verification_fails_closed_for_tampered_or_missing_objects() -> None:
    bundle = _bundle()
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


def test_verification_rejects_a_part_relabelled_into_another_week_or_part() -> None:
    bundle = _bundle()
    first = bundle.objects[0]

    moved_week = replace(
        first,
        descriptor=replace(first.descriptor, week=EtWeek(date(2025, 12, 1))),
    )
    with pytest.raises(DetailIntegrityError):
        DetailObjectBuilder.verify(replace(bundle, objects=(moved_week, *bundle.objects[1:])))

    moved_part = replace(first, descriptor=replace(first.descriptor, part_number=7))
    with pytest.raises(DetailIntegrityError):
        DetailObjectBuilder.verify(replace(bundle, objects=(moved_part, *bundle.objects[1:])))


def test_verification_rejects_a_compressed_body_even_with_matching_hashes() -> None:
    """A recompressed object is a different canonical object, not a cheaper one."""

    bundle = _bundle()
    original = bundle.objects[0]
    table = pq.read_table(pa.BufferReader(original.parquet_bytes))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False, version="2.6")
    zstd_bytes = sink.getvalue().to_pybytes()

    forged = replace(
        original,
        descriptor=replace(
            original.descriptor,
            content_hash=hashlib.sha256(zstd_bytes).hexdigest(),
            byte_size=len(zstd_bytes),
        ),
        parquet_bytes=zstd_bytes,
    )

    with pytest.raises(DetailIntegrityError):
        DetailObjectBuilder.verify(replace(bundle, objects=(forged, *bundle.objects[1:])))


def test_rejects_mixed_snapshots_duplicates_and_naive_times() -> None:
    result = _result()
    ledger = _ledger()
    point = _point()
    builder = DetailObjectBuilder()

    with pytest.raises(DetailObjectValidationError, match="run snapshot"):
        builder.build(result, [replace(ledger, run_snapshot_id="b" * 64)], [point], _instant(CREATED_AT))
    with pytest.raises(DetailObjectValidationError, match="run snapshot"):
        builder.build(result, [ledger], [replace(point, run_snapshot_id="b" * 64)], _instant(CREATED_AT))
    with pytest.raises(DetailObjectValidationError, match="transaction_id"):
        builder.build(result, [ledger, ledger], [point], _instant(CREATED_AT))
    with pytest.raises(DetailObjectValidationError, match="point_id"):
        builder.build(result, [ledger], [point, point], _instant(CREATED_AT))
    with pytest.raises(DetailObjectValidationError, match="timezone-aware"):
        replace(point, occurred_at=datetime(2025, 11, 3, 15, 0))
    with pytest.raises(DetailObjectValidationError, match="created_at"):
        builder.build(result, [ledger], [point], _instant("2025-11-03T16:00:00Z"))


def test_empty_official_result_has_a_verified_manifest_without_fake_rows() -> None:
    result = _result([])

    bundle = DetailObjectBuilder().build(result, [], [], _instant(CREATED_AT))

    assert bundle.objects == ()
    assert bundle.manifest.objects == ()
    assert bundle.manifest.as_rows() == ()
    DetailObjectBuilder.verify(bundle)


def test_manifest_store_is_idempotent_and_rejects_conflicting_official_detail() -> None:
    result = _result()
    bundle = DetailObjectBuilder().build(result, [_ledger()], [_point()], _instant(CREATED_AT))
    changed = DetailObjectBuilder().build(
        result,
        [_ledger()],
        [_point("00000000-0000-4000-8000-000000001033", "2025-11-03T20:02:00Z")],
        _instant(CREATED_AT),
    )
    store = InMemoryDetailManifestStore()

    assert store.put(bundle) == bundle.manifest
    assert store.put(bundle) == bundle.manifest
    assert store.get(bundle.manifest.detail_manifest_id) == bundle
    with pytest.raises(DetailManifestConflict, match="result manifest"):
        store.put(changed)


# --------------------------------------------------------------------------------
# publishing: object store + storage.objects registration
# --------------------------------------------------------------------------------


VERIFIED_AT = datetime(2025, 11, 3, 22, 0, tzinfo=UTC)


def test_publish_writes_every_part_at_its_canonical_key_and_registers_one_row(tmp_path) -> None:
    bundle = _bundle()
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    registry = InMemoryStorageObjectRegistry()

    published = DetailObjectPublisher(store, storage_write_port=registry).publish(
        bundle, verified_at=VERIFIED_AT
    )

    assert len(published.objects) == len(bundle.objects)
    for item in published.objects:
        descriptor = item.descriptor
        on_disk = Path(long_path((tmp_path / "objects").joinpath(*descriptor.object_key.split("/"))))
        assert on_disk.read_bytes() == next(
            stored.parquet_bytes for stored in bundle.objects if stored.descriptor == descriptor
        )
        assert item.storage_object.status is ObjectStatus.AVAILABLE
        assert item.storage_object.verified_at == VERIFIED_AT
        assert item.storage_object.compression_codec == "UNCOMPRESSED"
        assert item.storage_object.object_key == descriptor.object_key

    # storage.objects: exactly one row per object, no duplicates, all verified.
    assert len(registry.rows()) == len(bundle.objects)
    assert {row.status for row in registry.rows()} == {ObjectStatus.AVAILABLE}
    assert len({row.object_key for row in registry.rows()}) == len(bundle.objects)
    assert registry.register_calls == len(bundle.objects)


def test_publish_is_idempotent_and_still_leaves_exactly_one_row(tmp_path) -> None:
    bundle = _bundle()
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    registry = InMemoryStorageObjectRegistry()
    publisher = DetailObjectPublisher(store, storage_write_port=registry)

    first = publisher.publish(bundle, verified_at=VERIFIED_AT)
    second = publisher.publish(bundle, verified_at=VERIFIED_AT)

    assert first == second
    assert len(registry.rows()) == len(bundle.objects)
    assert registry.register_calls == 2 * len(bundle.objects), "idempotent at the row, not at the call"


def test_a_row_is_staged_before_verification_and_only_then_available(tmp_path) -> None:
    """AVAILABLE is a post-verification state; the STAGED row must exist first."""

    bundle = _bundle()
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    registry = InMemoryStorageObjectRegistry()
    seen: list[ObjectStatus] = []
    original_register = registry.register

    def spy(record):
        seen.append(record.status)
        return original_register(record)

    registry.register = spy  # type: ignore[method-assign]

    DetailObjectPublisher(store, storage_write_port=registry).publish(bundle, verified_at=VERIFIED_AT)

    assert seen == [ObjectStatus.STAGED] * len(bundle.objects)
    assert {row.status for row in registry.rows()} == {ObjectStatus.AVAILABLE}


def test_publish_quarantines_and_raises_when_a_stored_object_is_tampered(tmp_path) -> None:
    bundle = _bundle()
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")
    registry = InMemoryStorageObjectRegistry()
    target = bundle.objects[0].descriptor

    class _TamperingStore(LocalObjectStore):
        def put(self, object_key, data):
            receipt = super().put(object_key, data)
            if object_key == target.object_key:
                path = self.path_for(object_key)
                path.write_bytes(b"PAR1" + b"corrupted" + b"PAR1")
            return receipt

    tampering = _TamperingStore(tmp_path / "objects", bucket_name="backtest-local")

    with pytest.raises(DetailIntegrityError, match="verification"):
        DetailObjectPublisher(tampering, storage_write_port=registry).publish(
            bundle, verified_at=VERIFIED_AT
        )

    rows = registry.rows()
    assert [row.status for row in rows] == [ObjectStatus.QUARANTINED]
    assert rows[0].object_key == target.object_key
    assert rows[0].verified_at is None
    assert store is not tampering


def test_publish_fails_closed_when_the_object_vanishes_before_verification(tmp_path) -> None:
    bundle = _bundle()
    registry = InMemoryStorageObjectRegistry()

    class _AmnesiacStore(LocalObjectStore):
        def put(self, object_key, data):
            receipt = super().put(object_key, data)
            self.path_for(object_key).unlink()
            return receipt

    with pytest.raises(DetailIntegrityError, match="verification"):
        DetailObjectPublisher(
            _AmnesiacStore(tmp_path / "objects", bucket_name="backtest-local"),
            storage_write_port=registry,
        ).publish(bundle, verified_at=VERIFIED_AT)

    assert [row.status for row in registry.rows()] == [ObjectStatus.QUARANTINED]


def test_publish_through_the_unauthorised_port_refuses_instead_of_pretending(tmp_path) -> None:
    bundle = _bundle()
    store = LocalObjectStore(tmp_path / "objects", bucket_name="backtest-local")

    with pytest.raises(StorageWriteNotAuthorized, match="SHARED"):
        DetailObjectPublisher(store, storage_write_port=UnauthorizedStorageObjectWritePort()).publish(
            bundle, verified_at=VERIFIED_AT
        )
