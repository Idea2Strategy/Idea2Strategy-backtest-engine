"""Rebuilding published evidence from what durable storage actually keeps.

The durable read model (`result_query.DurableBacktestResultQueryStore`) holds no copy
of the immutable artifacts: it re-derives them from `backtest.*` rows plus the bytes in
the object store. These three entry points are that derivation, and each one is
required to fail closed rather than return a plausible-looking partial answer:

* `ResultSnapshotBuilder.rebuild` — the JSON result object back into a `ResultSnapshot`.
* `reassemble_detail_bundle` — `detail_manifests` + `storage.objects` + the Parquet
  bytes back into a `DetailObjectBundle`.
* `summary_from_document` — one `monthly_judgment_summaries` row back into a
  `MonthlyJudgmentSummary`.

Every assertion below is against a value the *builder* produced independently, never
against a value the test handed in.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.detail_object_manifest import (
    DetailIntegrityError,
    DetailObjectBuilder,
    DetailObjectKind,
    reassemble_detail_bundle,
)
from backtest_engine.execution_model import OrderStatus
from backtest_engine.monthly_judgment import (
    MonthlyJudgmentBuilder,
    MonthlyJudgmentIntegrityError,
    summary_from_document,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultIntegrityError,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshotBuilder,
    RunSnapshot,
)


RUN_ID = "00000000-0000-4000-8000-0000000031a1"
BOT_ID = "00000000-0000-4000-8000-0000000031a2"
INSTRUMENT_ID = "00000000-0000-4000-8000-0000000031a3"
ORDER_ID = "00000000-0000-4000-8000-0000000031a4"
FILL_ID = "00000000-0000-4000-8000-0000000031a5"
RECORD_ID = "00000000-0000-4000-8000-0000000031a6"
REJECTED_ORDER_ID = "00000000-0000-4000-8000-0000000031a7"
REJECTED_RECORD_ID = "00000000-0000-4000-8000-0000000031a8"

FINGERPRINT = "7" * 64
COMPLETED_AT = "2025-11-02T04:10:00Z"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


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


def _fill_record() -> ResultRecord:
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
        fill_id=FILL_ID,
        quantity=Decimal("1"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("100.05"),
        slippage_amount=Decimal("0.05"),
        fee=Decimal("2.20"),
        cost_basis=Decimal("100.05"),
        realized_pnl=Decimal("0"),
    )


def _rejection_record() -> ResultRecord:
    """A non-FILL record: it carries `reason_code` and no fill columns at all.

    ET Saturday 2025-11-01 10:30 — a *November* row inside the same ET Monday week as
    the October fill, so the two records also exercise the week/month split.
    """

    return ResultRecord(
        run_snapshot_id=_snapshot().snapshot_id,
        record_id=REJECTED_RECORD_ID,
        kind=ResultRecordKind.REJECTION,
        occurred_at=_instant("2025-11-01T14:30:00Z"),
        order_id=REJECTED_ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.REJECTED,
        cash_after=Decimal("9897.80"),
        positions_after=(PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),),
        reason_code="BUYING_POWER_EXCEEDED",
    )


def _published():
    result = ResultSnapshotBuilder().build(
        _snapshot(), [_fill_record(), _rejection_record()], _instant(COMPLETED_AT)
    )
    details = DetailObjectBuilder().build(result, [], [], _instant(COMPLETED_AT))
    return result, details


# --------------------------------------------------------------------------------
# the result snapshot object
# --------------------------------------------------------------------------------


def test_rebuild_recovers_the_published_result_snapshot_from_its_bytes_alone() -> None:
    result, _ = _published()

    rebuilt = ResultSnapshotBuilder.rebuild(result.object_bytes, _instant(COMPLETED_AT))

    assert rebuilt == result
    assert rebuilt.manifest.content_hash == result.manifest.content_hash
    assert rebuilt.summary.result_hash == result.summary.result_hash
    # The non-FILL record must survive with its reason and without invented fill data.
    rejection = next(item for item in rebuilt.records if item.record_id == REJECTED_RECORD_ID)
    assert rejection.reason_code == "BUYING_POWER_EXCEEDED"
    assert rejection.fill_id is None
    assert rejection.quantity is None
    fill = next(item for item in rebuilt.records if item.record_id == RECORD_ID)
    assert fill.fee == Decimal("2.20")
    assert fill.price == Decimal("100.05")
    assert fill.positions_after == (
        PositionAfter(INSTRUMENT_ID, Decimal("1"), Decimal("100.05")),
    )


def test_rebuild_refuses_bytes_that_are_not_a_result_object() -> None:
    result, _ = _published()

    with pytest.raises(ResultIntegrityError, match="not valid JSON"):
        ResultSnapshotBuilder.rebuild(result.object_bytes + b"tampered", _instant(COMPLETED_AT))
    with pytest.raises(ResultIntegrityError, match="schema_version"):
        ResultSnapshotBuilder.rebuild(b'{"schema_version":2,"run_snapshot":{},"records":[]}', _instant(COMPLETED_AT))
    with pytest.raises(ResultIntegrityError, match="cannot be parsed"):
        ResultSnapshotBuilder.rebuild(b'{"schema_version":1,"records":[]}', _instant(COMPLETED_AT))


def test_the_completion_instant_is_an_input_of_the_result_hash_not_of_the_object() -> None:
    """`calculated_at` is not inside the object, so the caller must supply the right one.

    `performance_summaries.calculated_at` is that value. Supplying a different instant
    produces the same bytes and a *different* `result_hash`, which is what the durable
    store cross-checks against `performance_summaries.result_hash`.
    """

    result, _ = _published()

    drifted = ResultSnapshotBuilder.rebuild(result.object_bytes, _instant("2025-11-02T05:10:00Z"))

    assert drifted.object_bytes == result.object_bytes
    assert drifted.summary.result_hash != result.summary.result_hash
    assert drifted.manifest.result_manifest_id != result.manifest.result_manifest_id


def test_an_edited_object_rebuilds_into_a_different_content_hash() -> None:
    """A self-consistent edit is caught by the content address, not by the parse.

    Changing one fee and re-serialising is still a valid result object, so `rebuild`
    cannot reject it on its own — and must not pretend to. What it does guarantee is
    that the manifest it returns describes *these* bytes, so the caller comparing
    `manifest.content_hash` with `storage.objects.content_hash` sees the difference.
    """

    result, _ = _published()
    edited = result.object_bytes.replace(b'"fee":"2.2"', b'"fee":"2.3"')
    assert edited != result.object_bytes, "the canonical fee text is not what this test edits"

    rebuilt = ResultSnapshotBuilder.rebuild(edited, _instant(COMPLETED_AT))

    assert rebuilt.manifest.content_hash != result.manifest.content_hash
    assert rebuilt.summary.result_hash != result.summary.result_hash
    assert rebuilt.summary.total_fees == Decimal("2.30000000")


# --------------------------------------------------------------------------------
# the detail bundle
# --------------------------------------------------------------------------------


def _parts(details):
    return [(item.descriptor, item.parquet_bytes) for item in details.objects]


def test_reassemble_recovers_the_published_detail_bundle() -> None:
    result, details = _published()

    rebuilt = reassemble_detail_bundle(
        result_manifest_id=result.manifest.result_manifest_id,
        run_snapshot_id=result.run_snapshot.snapshot_id,
        backtest_run_id=RUN_ID,
        strategy_version_id=BOT_ID,
        created_at=_instant(COMPLETED_AT),
        parts=_parts(details),
    )

    assert rebuilt == details
    assert rebuilt.manifest.detail_manifest_id == details.manifest.detail_manifest_id
    assert rebuilt.manifest.manifest_hash == details.manifest.manifest_hash
    assert {item.descriptor.record_type for item in rebuilt.objects} == {
        DetailObjectKind.TRADE_DETAIL,
        DetailObjectKind.POSITION_SNAPSHOT,
    }


def test_reassemble_is_order_independent_because_it_sorts_canonically() -> None:
    """The database returns rows in whatever order the query asked for."""

    result, details = _published()
    shuffled = list(reversed(_parts(details)))

    rebuilt = reassemble_detail_bundle(
        result_manifest_id=result.manifest.result_manifest_id,
        run_snapshot_id=result.run_snapshot.snapshot_id,
        backtest_run_id=RUN_ID,
        strategy_version_id=BOT_ID,
        created_at=_instant(COMPLETED_AT),
        parts=shuffled,
    )

    assert rebuilt.manifest.manifest_hash == details.manifest.manifest_hash


def test_reassemble_fails_closed_when_a_part_is_missing_or_its_bytes_changed() -> None:
    result, details = _published()
    parts = _parts(details)

    with pytest.raises(DetailIntegrityError, match="detail object size does not match"):
        reassemble_detail_bundle(
            result_manifest_id=result.manifest.result_manifest_id,
            run_snapshot_id=result.run_snapshot.snapshot_id,
            backtest_run_id=RUN_ID,
            strategy_version_id=BOT_ID,
            created_at=_instant(COMPLETED_AT),
            parts=[(parts[0][0], parts[0][1] + b"x"), *parts[1:]],
        )

    # A part that vanished changes the bundle's own identity rather than silently
    # yielding a shorter month.
    dropped = reassemble_detail_bundle(
        result_manifest_id=result.manifest.result_manifest_id,
        run_snapshot_id=result.run_snapshot.snapshot_id,
        backtest_run_id=RUN_ID,
        strategy_version_id=BOT_ID,
        created_at=_instant(COMPLETED_AT),
        parts=parts[:1],
    )
    assert dropped.manifest.manifest_hash != details.manifest.manifest_hash


def test_reassemble_refuses_a_part_created_at_a_different_instant() -> None:
    result, details = _published()
    parts = _parts(details)
    drifted = replace(parts[0][0], created_at=_instant("2025-11-02T04:11:00Z"))

    with pytest.raises(DetailIntegrityError, match="created_at"):
        reassemble_detail_bundle(
            result_manifest_id=result.manifest.result_manifest_id,
            run_snapshot_id=result.run_snapshot.snapshot_id,
            backtest_run_id=RUN_ID,
            strategy_version_id=BOT_ID,
            created_at=_instant(COMPLETED_AT),
            parts=[(drifted, parts[0][1]), *parts[1:]],
        )


# --------------------------------------------------------------------------------
# the monthly judgment row
# --------------------------------------------------------------------------------


def test_summary_from_document_recovers_the_published_month() -> None:
    result, _ = _published()
    built = MonthlyJudgmentBuilder().build(
        result.run_snapshot.snapshot_id,
        result.manifest.result_manifest_id,
        [],
        result.records,
    )
    assert [item.et_month.key for item in built] == ["2025-10", "2025-11"]

    for original in built:
        recovered = summary_from_document(original.summary_document, original.summary_hash)
        assert recovered == original

    october = summary_from_document(built[0].summary_document, built[0].summary_hash)
    assert october.et_month.key == "2025-10"
    assert october.trade_event_count == 1
    assert october.rejected_count == 0
    assert october.trade_record_ids == (RECORD_ID,)
    november = summary_from_document(built[1].summary_document, built[1].summary_hash)
    assert november.trade_record_ids == (REJECTED_RECORD_ID,)
    assert november.rejected_count == 1


def test_summary_from_document_refuses_a_document_that_no_longer_hashes_to_its_row() -> None:
    result, _ = _published()
    built = MonthlyJudgmentBuilder().build(
        result.run_snapshot.snapshot_id, result.manifest.result_manifest_id, [], result.records
    )
    tampered = dict(built[0].summary_document)
    tampered["trade_event_count"] = 99

    with pytest.raises(MonthlyJudgmentIntegrityError, match="summary_hash"):
        summary_from_document(tampered, built[0].summary_hash)
