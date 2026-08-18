from datetime import UTC, datetime

from backtest_engine.basic_runtime import (
    BasicDecisionStatus,
    BasicInstrumentDecision,
    BasicStepTrace,
    PlanEvaluation,
)
from backtest_engine.event_clock import MarketSessionStatus
from backtest_engine.wiring import _condition_outcomes


def test_condition_outcomes_are_unique_when_buy_and_sell_reuse_a_step_id() -> None:
    instrument_id = "aa268aa6-9401-49d0-a2d4-a2a490df7d84"

    def decision(flow_id: str, side: str) -> BasicInstrumentDecision:
        return BasicInstrumentDecision(
            flow_id=flow_id,
            instrument_id=instrument_id,
            side=side,
            status=BasicDecisionStatus.CONDITION_NOT_MET,
            trace=(BasicStepTrace("condition-1", False, "CONDITION_NOT_MET", {}),),
            first_failure_step_id="condition-1",
            first_failure_reason="CONDITION_NOT_MET",
        )

    evaluation = PlanEvaluation(
        evaluation_id="00000000-0000-4000-8000-000000000001",
        occurred_at=datetime(2024, 1, 2, 15, 0, tzinfo=UTC),
        session_status=MarketSessionStatus.REGULAR_OPEN,
        decisions=(decision("buy-flow", "BUY"), decision("sell-flow", "SELL")),
        candidates=(),
    )

    outcomes = _condition_outcomes(evaluation)

    assert [item.condition_id for item in outcomes] == [
        f"buy-flow|{instrument_id}|condition-1",
        f"sell-flow|{instrument_id}|condition-1",
    ]
