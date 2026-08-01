from __future__ import annotations

import copy
import json
from fractions import Fraction
from pathlib import Path

import pytest

from backtest_engine.basic_runtime import (
    BasicDecisionStatus,
    BasicPlanCompatibilityError,
    BasicPlanRuntime,
    BasicStepOutcome,
)


FIXTURE_PATH = (
    Path(__file__).parent / "fixtures/contracts/basic-compiled-plan.v1.json"
)
FIRST = "00000000-0000-4000-8000-000000000301"
SECOND = "00000000-0000-4000-8000-000000000302"


def _document() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _runtime(calls: list[str] | None = None) -> BasicPlanRuntime:
    recorded = calls if calls is not None else []

    def market_open(parameters: object, instrument_input: object) -> BasicStepOutcome:
        del parameters
        values = dict(instrument_input)
        recorded.append(f"trigger:{values['instrument_id']}")
        return (
            BasicStepOutcome.passed("MARKET_OPEN")
            if values["market_open"]
            else BasicStepOutcome.failed("MARKET_CLOSED")
        )

    def rsi(parameters: object, instrument_input: object) -> BasicStepOutcome:
        config = dict(parameters)
        values = dict(instrument_input)
        recorded.append(f"condition:{values['instrument_id']}")
        return (
            BasicStepOutcome.passed("THRESHOLD_MET")
            if values["rsi_14"] < config["threshold"]
            else BasicStepOutcome.failed("THRESHOLD_NOT_MET")
        )

    return BasicPlanRuntime({"MARKET_OPEN": market_open, "RSI": rsi})


def test_executes_producer_plan_with_c14_sequential_and_equal_semantics() -> None:
    calls: list[str] = []
    runtime = _runtime(calls)
    plan = runtime.load(_document())

    result = runtime.execute(
        plan,
        {
            SECOND: {"instrument_id": SECOND, "market_open": True, "rsi_14": 40},
            FIRST: {"instrument_id": FIRST, "market_open": True, "rsi_14": 20},
        },
    )

    assert [decision.instrument_id for decision in result.decisions] == [FIRST, SECOND]
    accepted, rejected = result.decisions
    assert accepted.status is BasicDecisionStatus.CANDIDATE
    assert accepted.buy_allocation == Fraction(1, 1)
    assert [trace.step_id for trace in accepted.trace] == ["trigger", "condition"]
    assert rejected.status is BasicDecisionStatus.CONDITION_NOT_MET
    assert rejected.first_failure_step_id == "condition"
    assert rejected.first_failure_reason == "THRESHOLD_NOT_MET"
    assert [trace.step_id for trace in rejected.trace] == ["trigger", "condition"]
    assert calls == [
        f"trigger:{FIRST}",
        f"condition:{FIRST}",
        f"trigger:{SECOND}",
        f"condition:{SECOND}",
    ]


def test_stops_at_first_failed_trigger_and_allocates_only_passing_buy_candidates() -> None:
    calls: list[str] = []
    runtime = _runtime(calls)

    result = runtime.execute(
        runtime.load(_document()),
        {
            FIRST: {"instrument_id": FIRST, "market_open": False, "rsi_14": 20},
            SECOND: {"instrument_id": SECOND, "market_open": True, "rsi_14": 20},
        },
    )

    assert result.decisions[0].first_failure_step_id == "trigger"
    assert result.decisions[0].first_failure_reason == "MARKET_CLOSED"
    assert result.decisions[0].buy_allocation is None
    assert result.decisions[1].buy_allocation == Fraction(1, 1)
    assert f"condition:{FIRST}" not in calls


def test_assigns_exact_equal_fraction_to_every_passing_buy_candidate() -> None:
    runtime = _runtime()

    result = runtime.execute(
        runtime.load(_document()),
        {
            FIRST: {"instrument_id": FIRST, "market_open": True, "rsi_14": 20},
            SECOND: {"instrument_id": SECOND, "market_open": True, "rsi_14": 25},
        },
    )

    assert [decision.buy_allocation for decision in result.decisions] == [
        Fraction(1, 2),
        Fraction(1, 2),
    ]


def test_records_missing_input_and_isolates_evaluator_errors() -> None:
    def broken(parameters: object, instrument_input: object) -> BasicStepOutcome:
        del parameters, instrument_input
        raise RuntimeError("feature unavailable")

    runtime = BasicPlanRuntime(
        {
            "MARKET_OPEN": lambda parameters, instrument_input: BasicStepOutcome.passed(
                "MARKET_OPEN"
            ),
            "RSI": broken,
        }
    )

    result = runtime.execute(
        runtime.load(_document()),
        {FIRST: {"instrument_id": FIRST}},
    )

    failed, missing = result.decisions
    assert failed.status is BasicDecisionStatus.CONDITION_ERROR
    assert failed.first_failure_reason == "CONDITION_EVALUATION_ERROR"
    assert failed.trace[-1].evidence["errorType"] == "RuntimeError"
    assert missing.status is BasicDecisionStatus.INPUT_MISSING
    assert missing.first_failure_step_id == "$input"


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update(schemaVersion="basic-compiled-plan.v2"), "schemaVersion"),
        (lambda value: value.update(compilerVersion="basic-compiler:2.0.0"), "compilerVersion"),
        (
            lambda value: value["policyVersions"].update(
                evaluationPolicyVersion="basic-evaluation:2.0.0"
            ),
            "evaluationPolicyVersion",
        ),
        (
            lambda value: value["flows"][0].update(evaluationMode="PARALLEL"),
            "evaluationMode",
        ),
        (
            lambda value: value["flows"][0].update(allocationMode="WEIGHTED"),
            "allocationMode",
        ),
        (
            lambda value: value["flows"][0]["steps"][1].update(
                elementCode="PRO_SCRIPT"
            ),
            "unsupported elementCode",
        ),
        (
            lambda value: value["flows"][0]["connections"][0].update(
                toBlockId="order"
            ),
            "connections",
        ),
    ],
)
def test_rejects_incompatible_meaning_without_substitution(mutation, message: str) -> None:
    document = copy.deepcopy(_document())
    mutation(document)

    with pytest.raises(BasicPlanCompatibilityError, match=message):
        _runtime().load(document)


def test_assigns_exact_equal_fractions_and_preserves_sell_meaning() -> None:
    document = _document()
    document["flows"][0]["container"] = "SELL"
    document["flows"][0]["steps"][-1]["elementCode"] = "SELL_ORDER"
    runtime = _runtime()

    result = runtime.execute(
        runtime.load(document),
        {
            FIRST: {"instrument_id": FIRST, "market_open": True, "rsi_14": 20},
            SECOND: {"instrument_id": SECOND, "market_open": True, "rsi_14": 20},
        },
    )

    assert all(decision.status is BasicDecisionStatus.CANDIDATE for decision in result.decisions)
    assert all(decision.side == "SELL" for decision in result.decisions)
    assert all(decision.buy_allocation is None for decision in result.decisions)
