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
    basic: tuple[ConditionOutcome, ...] = (),
    branches: tuple[BranchEvaluation, ...] = (),
) -> JudgmentEvaluation:
    return JudgmentEvaluation(
        evaluation_id=evaluation_id,
        run_snapshot_id=RUN_SNAPSHOT_ID,
        evaluated_at=_instant(evaluated_at),
        mode=mode,
        trade_occurred=trade_occurred,
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

    assert MonthlyJudgmentBuilder().build(
        RUN_SNAPSHOT_ID, RESULT_MANIFEST_ID, evaluations, []
    ) == ()


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
