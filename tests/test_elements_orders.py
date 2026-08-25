"""The terminal ``EMIT_ORDER_CANDIDATE`` element (card D22).

Every expectation is a pinned literal. Nothing here recomputes the production
assembly to obtain its own oracle.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction
from typing import Any

import pytest

from backtest_engine.elements import (
    ElementCompatibilityError,
    ElementEvaluationError,
    PlanLoadFailure,
    PlanStep,
)
from backtest_engine.elements.orders import (
    SUPPORTED_ALLOCATION_MODES,
    SUPPORTED_ORDER_TYPES,
    SUPPORTED_SIDES,
    TERMINAL_OPERATION,
    OrderCandidate,
    emit_order_candidate,
)


INSTRUMENT = "00000000-0000-4000-8000-000000000301"
DECIDED_AT = datetime.fromisoformat("2025-11-28T14:45:00+00:00")
CLOSES_AT = datetime.fromisoformat("2025-11-28T18:00:00+00:00")

BUY_STEP = PlanStep(
    sequence=3,
    operation="EMIT_ORDER_CANDIDATE",
    arguments={"allocation": "EQUAL", "orderType": "MARKET", "side": "BUY"},
)

V2_BUY_STEP = PlanStep(
    sequence=3,
    operation="EMIT_ORDER_CANDIDATE",
    arguments={
        "allocation": "EQUAL",
        "orderType": "MARKET",
        "timeInForce": "DAY",
        "side": "BUY",
        "orderPercent": "25",
        "maxPositionPercent": "40",
        "executionMode": "1회만",
        "waitMode": "조건 재충족",
        "waitInterval": "1",
        "maxExecutions": "1",
    },
)


def _emit(**overrides: Any) -> OrderCandidate:
    arguments: dict[str, Any] = {
        "evaluation_id": "evaluation-1",
        "instrument_id": INSTRUMENT,
        "partition_key": "partition-1",
        "flow_id": "flow-1",
        "budget_cap_bps": 10000,
        "allocation": Fraction(1, 2),
        "reference_price": Decimal("94.00000000"),
        "decided_at": DECIDED_AT,
        "eligible_at": DECIDED_AT,
        "session_date_et": date(2025, 11, 28),
        "session_closes_at": CLOSES_AT,
    }
    step = overrides.pop("step", BUY_STEP)
    arguments.update(overrides)
    return emit_order_candidate(step, **arguments)


def _step(**arguments: str) -> PlanStep:
    return PlanStep(sequence=3, operation="EMIT_ORDER_CANDIDATE", arguments=arguments)


def test_the_terminal_vocabulary_is_pinned() -> None:
    assert TERMINAL_OPERATION == "EMIT_ORDER_CANDIDATE"
    assert SUPPORTED_SIDES == ("BUY", "SELL")
    assert SUPPORTED_ORDER_TYPES == ("MARKET",)
    assert SUPPORTED_ALLOCATION_MODES == ("EQUAL",)


def test_emits_every_field_the_execution_layer_needs() -> None:
    candidate = _emit()

    assert candidate.evaluation_id == "evaluation-1"
    assert candidate.instrument_id == INSTRUMENT
    assert candidate.partition_key == "partition-1"
    assert candidate.flow_id == "flow-1"
    # side / orderType come from the step, not from the caller.
    assert candidate.side == "BUY"
    assert candidate.order_type == "MARKET"
    assert candidate.allocation == Fraction(1, 2)
    assert candidate.reference_price == Decimal("94.00000000")
    assert candidate.decided_at == DECIDED_AT
    assert candidate.eligible_at == DECIDED_AT
    assert candidate.session_date_et == date(2025, 11, 28)
    assert candidate.session_closes_at == CLOSES_AT
    assert candidate.budget_cap_bps == 10000
    assert candidate.max_position_percent == Decimal("100")


def test_v2_terminal_requires_and_emits_the_per_instrument_position_cap() -> None:
    candidate = _emit(step=V2_BUY_STEP)

    assert candidate.max_position_percent == Decimal("40")


@pytest.mark.parametrize("cap", ["0", "100.1", "-1", "many"])
def test_v2_terminal_refuses_an_invalid_per_instrument_position_cap(cap: str) -> None:
    step = PlanStep(
        sequence=V2_BUY_STEP.sequence,
        operation=V2_BUY_STEP.operation,
        arguments={**V2_BUY_STEP.arguments, "maxPositionPercent": cap},
    )

    with pytest.raises((ElementCompatibilityError, ElementEvaluationError)):
        _emit(step=step)


def test_the_side_comes_from_the_step_and_a_sell_carries_no_allocation() -> None:
    candidate = _emit(
        step=_step(allocation="EQUAL", orderType="MARKET", side="SELL"),
        allocation=None,
    )

    assert candidate.side == "SELL"
    assert candidate.allocation is None


def test_a_non_terminal_step_can_never_emit() -> None:
    load = PlanStep(
        sequence=1,
        operation="LOAD_FEATURE",
        arguments={"feature": "RSI_14", "resolution": "1m"},
    )

    with pytest.raises(ElementEvaluationError, match="EMIT_ORDER_CANDIDATE"):
        _emit(step=load)


@pytest.mark.parametrize(
    ("arguments", "detail"),
    [
        ({"allocation": "WEIGHTED", "orderType": "MARKET", "side": "BUY"}, "WEIGHTED"),
        ({"allocation": "EQUAL", "orderType": "LIMIT", "side": "BUY"}, "LIMIT"),
        ({"allocation": "EQUAL", "orderType": "MARKET", "side": "SHORT"}, "SHORT"),
    ],
)
def test_an_argument_value_this_build_cannot_execute_is_rejected(
    arguments: dict[str, str], detail: str
) -> None:
    with pytest.raises(ElementCompatibilityError) as failure:
        _emit(step=_step(**arguments), allocation=None)

    assert failure.value.failure is PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT
    assert detail in str(failure.value)


def test_a_missing_terminal_argument_is_rejected_by_name() -> None:
    with pytest.raises(ElementCompatibilityError) as failure:
        _emit(step=_step(allocation="EQUAL", side="BUY"))

    assert failure.value.failure is PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT
    assert "orderType" in str(failure.value)


def test_an_equal_allocated_buy_without_a_share_is_never_emitted_as_a_default() -> None:
    with pytest.raises(ElementEvaluationError, match="allocation share"):
        _emit(allocation=None)


def test_a_sell_may_not_carry_a_buy_allocation_share() -> None:
    with pytest.raises(ElementEvaluationError, match="allocation share"):
        _emit(
            step=_step(allocation="EQUAL", orderType="MARKET", side="SELL"),
            allocation=Fraction(1, 2),
        )


@pytest.mark.parametrize(
    "allocation", [Fraction(0, 1), Fraction(-1, 2), Fraction(3, 2)]
)
def test_an_allocation_outside_zero_to_one_is_rejected(allocation: Fraction) -> None:
    with pytest.raises(ElementEvaluationError, match="allocation"):
        _emit(allocation=allocation)


def test_an_unquantized_reference_price_never_reaches_the_execution_layer() -> None:
    with pytest.raises(ElementEvaluationError, match="reference_price"):
        _emit(reference_price=Decimal("94"))


def test_a_non_positive_reference_price_is_rejected() -> None:
    with pytest.raises(ElementEvaluationError, match="reference_price"):
        _emit(reference_price=Decimal("0.00000000"))


def test_eligibility_may_not_precede_the_decision() -> None:
    with pytest.raises(ElementEvaluationError, match="eligible_at"):
        _emit(eligible_at=datetime.fromisoformat("2025-11-28T14:44:00+00:00"))


def test_a_decision_at_or_after_the_session_close_can_never_fill() -> None:
    with pytest.raises(ElementEvaluationError, match="session_closes_at"):
        _emit(decided_at=CLOSES_AT, eligible_at=CLOSES_AT)


def test_a_naive_instant_is_rejected() -> None:
    with pytest.raises(ElementEvaluationError, match="decided_at"):
        _emit(decided_at=datetime(2025, 11, 28, 14, 45))


@pytest.mark.parametrize("budget_cap_bps", [0, -1, 10001])
def test_a_budget_cap_outside_the_contract_range_is_rejected(
    budget_cap_bps: int,
) -> None:
    with pytest.raises(ElementEvaluationError, match="budget_cap_bps"):
        _emit(budget_cap_bps=budget_cap_bps)


def test_the_session_date_must_be_a_date_not_an_instant() -> None:
    with pytest.raises(ElementEvaluationError, match="session_date_et"):
        _emit(session_date_et=datetime(2025, 11, 28, tzinfo=DECIDED_AT.tzinfo))


def test_a_candidate_is_immutable() -> None:
    candidate = _emit()

    with pytest.raises(AttributeError):
        candidate.allocation = Fraction(1, 1)  # type: ignore[misc]
