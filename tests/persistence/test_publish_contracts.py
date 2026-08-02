"""Publish invariants that need no database.

Split out of `test_atomic_publish.py` so the Docker-free suite still covers them.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

import pytest

from backtest_engine.persistence import (
    MonthlyJudgment,
    PublishConflict,
    RunPublication,
    sum_monthly_counters,
)

from .support import (
    AVAILABLE_OBJECT_ID,
    make_detail_manifest,
    make_failure_count,
    make_monthly_summary,
    make_performance_summary,
)


RESULT_HASH = "c" * 64
FINISHED_AT = datetime(2026, 3, 2, 14, 5, tzinfo=UTC)


def test_publication_rejects_rows_belonging_to_another_run() -> None:
    run_id = uuid4()
    other = uuid4()

    with pytest.raises(PublishConflict, match="performance summary belongs to run"):
        RunPublication(
            run_id=run_id,
            completed_at=FINISHED_AT,
            result_hash=RESULT_HASH,
            performance=make_performance_summary(other, result_hash=RESULT_HASH),
        )


def test_publication_rejects_a_result_hash_that_disagrees_with_the_summary() -> None:
    run_id = uuid4()

    with pytest.raises(PublishConflict, match="result_hash"):
        RunPublication(
            run_id=run_id,
            completed_at=FINISHED_AT,
            result_hash=RESULT_HASH,
            performance=make_performance_summary(run_id, result_hash="e" * 64),
        )


def test_sum_monthly_counters_totals_every_canonical_counter() -> None:
    run_id = uuid4()
    january = make_monthly_summary(
        run_id,
        et_year_month="2024-01",
        evaluation_count=10,
        active_branch_count=1,
        trade_event_count=2,
        data_gap_count=3,
        triggered_count=4,
        rejected_count=5,
    )
    february = make_monthly_summary(
        run_id,
        et_year_month="2024-02",
        evaluation_count=20,
        active_branch_count=2,
        trade_event_count=4,
        data_gap_count=6,
        triggered_count=8,
        rejected_count=10,
    )

    totals = sum_monthly_counters([MonthlyJudgment(january), MonthlyJudgment(february)])

    assert totals == {
        "evaluation_count": 30,
        "active_branch_count": 3,
        "trade_event_count": 6,
        "data_gap_count": 9,
        "triggered_count": 12,
        "rejected_count": 15,
    }


def test_failure_counts_must_belong_to_their_summary() -> None:
    run_id = uuid4()
    summary = make_monthly_summary(run_id)

    with pytest.raises(PublishConflict, match="do not belong"):
        MonthlyJudgment(summary=summary, failure_counts=(make_failure_count(uuid4()),))


def test_week_start_must_be_a_monday() -> None:
    with pytest.raises(ValueError, match="Monday"):
        make_detail_manifest(uuid4(), week_start_date=date(2024, 3, 5))


def test_available_object_id_is_the_seeded_one() -> None:
    assert str(AVAILABLE_OBJECT_ID) == "00000000-0000-4000-8000-0000000000c1"
