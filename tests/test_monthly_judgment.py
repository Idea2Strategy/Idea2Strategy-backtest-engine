from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

import pytest

from backtest_engine.execution_model import OrderStatus
from backtest_engine.monthly_judgment import (
    BranchEvaluation,
    ConditionOutcome,
    JudgmentEvaluation,
    MonthlyJudgmentBuilder,
    MonthlyJudgmentValidationError,
    StrategyMode,
)
from backtest_engine.result_snapshot import ResultRecord, ResultRecordKind


RUN_SNAPSHOT_ID = "a" * 64
RESULT_MANIFEST_ID = "00000000-0000-4000-8000-000000000901"
INSTRUMENT_ID = "00000000-0000-4000-8000-000000000902"
ORDER_ID = "00000000-0000-4000-8000-000000000903"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _evaluation(
    evaluation_id: str,
    evaluated_at: str,
    *,
    mode: StrategyMode = StrategyMode.BASIC,
    trade_occurred: bool = False,
    data_gap: bool = False,
    basic: tuple[ConditionOutcome, ...] = (),
    branches: tuple[BranchEvaluation, ...] = (),
) -> JudgmentEvaluation:
    return JudgmentEvaluation(
        evaluation_id=evaluation_id,
        run_snapshot_id=RUN_SNAPSHOT_ID,
        evaluated_at=_instant(evaluated_at),
        mode=mode,
        trade_occurred=trade_occurred,
        data_gap=data_gap,
        basic_outcomes=basic,
        pro_branches=branches,
    )


def _record(record_id: str, occurred_at: str) -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=RUN_SNAPSHOT_ID,
        record_id=record_id,
        kind=ResultRecordKind.ORDER,
        occurred_at=_instant(occurred_at),
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.ACCEPTED,
        cash_after=Decimal("10000"),
        positions_after=(),
    )


def _rejection(record_id: str, occurred_at: str) -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=RUN_SNAPSHOT_ID,
        record_id=record_id,
        kind=ResultRecordKind.REJECTION,
        occurred_at=_instant(occurred_at),
        order_id=ORDER_ID,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.REJECTED,
        cash_after=Decimal("10000"),
        positions_after=(),
        reason_code="STRATEGY_BUDGET_EXCEEDED",
    )


def test_basic_counts_only_the_first_failure_in_each_et_month() -> None:
    february = _evaluation(
        "00000000-0000-4000-8000-000000000911",
        "2025-03-01T04:30:00Z",  # 2025-02-28 23:30 EST
        basic=(
            ConditionOutcome("market-open", True),
            ConditionOutcome("price-breakout", False),
            ConditionOutcome("volume", False),
        ),
    )
    march = _evaluation(
        "00000000-0000-4000-8000-000000000912",
        "2025-03-01T05:30:00Z",  # 2025-03-01 00:30 EST
        basic=(ConditionOutcome("market-open", False),),
    )

    summaries = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [march, february], []
    )

    assert [summary.et_month.key for summary in summaries] == ["2025-02", "2025-03"]
    assert [(item.scope_id, item.condition_id, item.count) for item in summaries[0].failure_counts] == [
        ("BASIC", "price-breakout", 1)
    ]
    assert [(item.scope_id, item.condition_id, item.count) for item in summaries[1].failure_counts] == [
        ("BASIC", "market-open", 1)
    ]


def test_pro_counts_first_failure_for_each_active_branch_only() -> None:
    evaluation = _evaluation(
        "00000000-0000-4000-8000-000000000913",
        "2025-07-10T14:00:00Z",
        mode=StrategyMode.PRO,
        branches=(
            BranchEvaluation(
                "long",
                True,
                (
                    ConditionOutcome("trend", False),
                    ConditionOutcome("liquidity", False),
                ),
            ),
            BranchEvaluation(
                "short",
                True,
                (ConditionOutcome("trend", True), ConditionOutcome("borrow", False)),
            ),
            BranchEvaluation(
                "unused",
                False,
                (ConditionOutcome("should-not-count", False),),
            ),
        ),
    )

    summary = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [evaluation], []
    )[0]

    assert [(item.scope_id, item.condition_id, item.count) for item in summary.failure_counts] == [
        ("long", "trend", 1),
        ("short", "borrow", 1),
    ]


def test_trade_details_are_linked_to_the_same_snapshot_and_et_month() -> None:
    records = [
        _record("00000000-0000-4000-8000-000000000921", "2025-11-01T03:30:00Z"),
        _record("00000000-0000-4000-8000-000000000922", "2025-11-01T04:30:00Z"),
    ]

    summaries = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [], records
    )

    assert [summary.et_month.key for summary in summaries] == ["2025-10", "2025-11"]
    assert summaries[0].trade_record_ids == (records[0].record_id,)
    assert summaries[1].trade_record_ids == (records[1].record_id,)
    assert all(summary.result_manifest_id == RESULT_MANIFEST_ID for summary in summaries)


def test_non_trade_evaluation_identity_and_raw_outcomes_are_not_persisted() -> None:
    evaluation = _evaluation(
        "00000000-0000-4000-8000-000000000914",
        "2025-07-10T14:00:00Z",
        basic=(ConditionOutcome("first", False), ConditionOutcome("secret-later", False)),
    )

    record = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [evaluation], []
    )[0].as_record()

    rendered = repr(record)
    assert evaluation.evaluation_id not in rendered
    assert "secret-later" not in rendered
    assert "evaluations" not in record


def test_output_is_deterministic_and_aggregates_matching_failures() -> None:
    evaluations = [
        _evaluation(
            "00000000-0000-4000-8000-000000000915",
            "2025-07-10T14:00:00Z",
            basic=(ConditionOutcome("price", False),),
        ),
        _evaluation(
            "00000000-0000-4000-8000-000000000916",
            "2025-07-11T14:00:00Z",
            basic=(ConditionOutcome("price", False),),
        ),
    ]
    records = [_record("00000000-0000-4000-8000-000000000923", "2025-07-10T15:00:00Z")]
    builder = MonthlyJudgmentBuilder()

    first = builder.build(RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, records)
    second = builder.build(
        RUN_SNAPSHOT_ID,
        RESULT_MANIFEST_ID,
        list(reversed(evaluations)),
        list(reversed(records)),
    )

    assert first == second
    assert first[0].failure_counts[0].count == 2


def test_trade_evaluation_and_all_pass_non_trade_do_not_create_raw_rows() -> None:
    """No failure row, but the month is still counted.

    Pre-rebuild this returned ``()``: a month in which the strategy ran and
    triggered was indistinguishable from a month in which it never ran. The six
    canonical counters exist precisely to tell those apart, so the month now
    yields exactly one summary carrying zero failure counts.
    """

    evaluations = [
        _evaluation(
            "00000000-0000-4000-8000-000000000917",
            "2025-07-10T14:00:00Z",
            trade_occurred=True,
            basic=(ConditionOutcome("passed", True),),
        ),
        _evaluation(
            "00000000-0000-4000-8000-000000000918",
            "2025-07-10T14:01:00Z",
            basic=(ConditionOutcome("passed", True),),
        ),
    ]

    summaries = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, []
    )

    assert len(summaries) == 1
    assert summaries[0].failure_counts == ()
    assert summaries[0].trade_record_ids == ()
    assert summaries[0].evaluation_count == 2
    assert summaries[0].triggered_count == 1


def test_a_month_with_no_evaluations_and_no_trades_has_no_summary() -> None:
    assert MonthlyJudgmentBuilder().build(RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [], []) == ()


# ---------------------------------------------------------------------------
# D26: the six canonical counters, the summary document and its hash
# ---------------------------------------------------------------------------


def _july_population() -> tuple[list[JudgmentEvaluation], list[ResultRecord]]:
    """4 evaluations and 2 trade events, all inside ET 2025-07.

    Hand-counted expectations:
      evaluation_count    4  (every evaluation, gaps included)
      active_branch_count 2  ('long' and 'short'; 'unused' is inactive)
      trade_event_count   2  (the ORDER and the REJECTION detail records)
      data_gap_count      1  (evaluation C)
      triggered_count     1  (evaluation A)
      rejected_count      1  (the REJECTION detail record)
    """

    evaluations = [
        _evaluation(
            "00000000-0000-4000-8000-000000000941",
            "2025-07-10T14:00:00Z",
            trade_occurred=True,
            basic=(ConditionOutcome("price", True),),
        ),
        _evaluation(
            "00000000-0000-4000-8000-000000000942",
            "2025-07-10T14:01:00Z",
            basic=(ConditionOutcome("price", False),),
        ),
        _evaluation(
            "00000000-0000-4000-8000-000000000943",
            "2025-07-10T14:02:00Z",
            data_gap=True,
        ),
        _evaluation(
            "00000000-0000-4000-8000-000000000944",
            "2025-07-11T14:00:00Z",
            mode=StrategyMode.PRO,
            branches=(
                BranchEvaluation("long", True, (ConditionOutcome("trend", False),)),
                BranchEvaluation("short", True, (ConditionOutcome("trend", True),)),
                BranchEvaluation("unused", False, (ConditionOutcome("trend", False),)),
            ),
        ),
    ]
    records = [
        _record("00000000-0000-4000-8000-000000000951", "2025-07-10T15:00:00Z"),
        _rejection("00000000-0000-4000-8000-000000000952", "2025-07-10T15:01:00Z"),
    ]
    return evaluations, records


def test_summary_reports_all_six_canonical_counters() -> None:
    evaluations, records = _july_population()

    summary = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, records
    )[0]

    assert summary.et_month.key == "2025-07"
    assert summary.evaluation_count == 4
    assert summary.active_branch_count == 2
    assert summary.trade_event_count == 2
    assert summary.data_gap_count == 1
    assert summary.triggered_count == 1
    assert summary.rejected_count == 1
    # A data gap is not a condition failure, and an inactive branch is not
    # evaluated, so neither contributes a first-failure row.
    assert [
        (item.mode.value, item.scope_id, item.condition_id, item.count)
        for item in summary.failure_counts
    ] == [("BASIC", "BASIC", "price", 1), ("PRO", "long", "trend", 1)]


def test_summary_document_is_aggregate_only_and_its_hash_is_pinned() -> None:
    evaluations, records = _july_population()

    summary = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, records
    )[0]

    assert dict(summary.summary_document) == {
        "schema_version": 1,
        "run_snapshot_id": RUN_SNAPSHOT_ID,
        "result_manifest_id": RESULT_MANIFEST_ID,
        "et_year_month": "2025-07",
        "timezone_id": "America/New_York",
        "evaluation_count": 4,
        "active_branch_count": 2,
        "trade_event_count": 2,
        "data_gap_count": 1,
        "triggered_count": 1,
        "rejected_count": 1,
        "failure_counts": [
            {
                "mode": "BASIC",
                "flow_or_branch_key": "BASIC",
                "first_failure_condition_key": "price",
                "occurrence_count": 1,
            },
            {
                "mode": "PRO",
                "flow_or_branch_key": "long",
                "first_failure_condition_key": "trend",
                "occurrence_count": 1,
            },
        ],
        "trade_record_ids": [
            "00000000-0000-4000-8000-000000000951",
            "00000000-0000-4000-8000-000000000952",
        ],
    }
    # Content address of exactly the document above. Pinned as a literal so a
    # change to the counted material is a reviewed change; re-deriving it in
    # the test would only prove the implementation agrees with itself.
    assert summary.summary_hash == (
        "939a2c36926d076793e23b77f79fc79ef8ef6506aea55df3d2e3997f33df7731"
    )
    # uuid5(NAMESPACE_URL, "idea2strategy:d26:<summary_hash>"): the row id is
    # the content address, so the same month content always reconciles.
    assert summary.summary_id == "a8ef4bdc-069e-5dc1-9f34-2891a50165ff"

    rendered = repr(dict(summary.summary_document))
    for evaluation in evaluations:
        assert evaluation.evaluation_id not in rendered


def test_counters_move_independently_of_one_another() -> None:
    """Each counter must be driven by its own population, not by a shared total."""

    evaluations, records = _july_population()

    without_gap = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID,
        RESULT_MANIFEST_ID,
        [item for item in evaluations if not item.data_gap],
        records,
    )[0]
    assert without_gap.evaluation_count == 3
    assert without_gap.data_gap_count == 0
    assert without_gap.triggered_count == 1
    assert without_gap.trade_event_count == 2
    assert without_gap.summary_hash != _july_summary_hash()

    without_rejection = MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID,
        RESULT_MANIFEST_ID,
        evaluations,
        records[:1],
    )[0]
    assert without_rejection.trade_event_count == 1
    assert without_rejection.rejected_count == 0
    assert without_rejection.evaluation_count == 4


def _july_summary_hash() -> str:
    evaluations, records = _july_population()
    return (
        MonthlyJudgmentBuilder()
        .build(RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, records)[0]
        .summary_hash
    )


def test_a_data_gap_evaluation_cannot_also_report_conditions_or_a_trade() -> None:
    with pytest.raises(MonthlyJudgmentValidationError, match="data gap"):
        _evaluation(
            "00000000-0000-4000-8000-000000000961",
            "2025-07-10T14:00:00Z",
            data_gap=True,
            basic=(ConditionOutcome("price", False),),
        )
    with pytest.raises(MonthlyJudgmentValidationError, match="data gap"):
        _evaluation(
            "00000000-0000-4000-8000-000000000962",
            "2025-07-10T14:00:00Z",
            data_gap=True,
            trade_occurred=True,
        )
    with pytest.raises(MonthlyJudgmentValidationError, match="data_gap must be a bool"):
        _evaluation(
            "00000000-0000-4000-8000-000000000963",
            "2025-07-10T14:00:00Z",
            data_gap="yes",  # type: ignore[arg-type]
        )


def test_rejects_duplicate_or_mixed_snapshot_inputs_and_naive_time() -> None:
    evaluation = _evaluation(
        "00000000-0000-4000-8000-000000000919",
        "2025-07-10T14:00:00Z",
        basic=(ConditionOutcome("price", False),),
    )
    builder = MonthlyJudgmentBuilder()

    with pytest.raises(MonthlyJudgmentValidationError, match="evaluation_id"):
        builder.build(RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [evaluation, evaluation], [])
    with pytest.raises(MonthlyJudgmentValidationError, match="run snapshot"):
        builder.build(
            "b" * 64, RESULT_MANIFEST_ID, [evaluation], []
        )
    with pytest.raises(MonthlyJudgmentValidationError, match="record_id"):
        record = _record(
            "00000000-0000-4000-8000-000000000924", "2025-07-10T15:00:00Z"
        )
        builder.build(RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, [], [record, record])
    with pytest.raises(MonthlyJudgmentValidationError, match="run snapshot"):
        builder.build(
            RUN_SNAPSHOT_ID,
            RESULT_MANIFEST_ID,
            [],
            [replace(_record("00000000-0000-4000-8000-000000000925", "2025-07-10T15:00:00Z"), run_snapshot_id="b" * 64)],
        )
    with pytest.raises(MonthlyJudgmentValidationError, match="timezone-aware"):
        replace(evaluation, evaluated_at=datetime(2025, 7, 10, 14, 0))


def test_rejects_mode_shape_and_duplicate_pro_branch() -> None:
    with pytest.raises(MonthlyJudgmentValidationError, match="Basic"):
        _evaluation(
            "00000000-0000-4000-8000-000000000931",
            "2025-07-10T14:00:00Z",
            branches=(BranchEvaluation("branch", True, ()),),
        )
    with pytest.raises(MonthlyJudgmentValidationError, match="Pro"):
        _evaluation(
            "00000000-0000-4000-8000-000000000932",
            "2025-07-10T14:00:00Z",
            mode=StrategyMode.PRO,
            basic=(ConditionOutcome("condition", False),),
        )
    branch = BranchEvaluation("branch", True, ())
    with pytest.raises(MonthlyJudgmentValidationError, match="branch_id"):
        _evaluation(
            "00000000-0000-4000-8000-000000000933",
            "2025-07-10T14:00:00Z",
            mode=StrategyMode.PRO,
            branches=(branch, branch),
        )
