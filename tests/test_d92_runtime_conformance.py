"""D92 -- D's runtime, run over the language-neutral conformance fixture.

The fixture is `conformance/strategy-bot-runtime/v1/basic-executor-conformance.v1.json`.
It is not a Python fixture: it is the single set of bytes the Java trading runtime
is meant to be checked against too, and `conformance/README.md` states exactly what
the trading-engine-side test has to do with it.

What this module is careful *not* to do
---------------------------------------
It never recomputes an expectation. Every value asserted comes out of the fixture,
and every value in the fixture is a literal with a written derivation. A test that
computed the RSI from the closes and compared it with the runtime's answer would be
testing that two copies of the same formula agree.

It also never asserts only that the runtime is self-consistent. Where a decision has
an order, the order is asserted as a list of literals; where a step must not run, the
evaluator is one that fails the test if it is called.

`executorCases` are driven through `BasicPlanRuntime._evaluate_instrument` and
`_allocate_equally` via a scripted element catalog rather than through a compiled
plan, for the same reason the Java fixture does: those cases are about the executor
(ordering, short-circuit, status mapping, allocation), not about the element layer.
`elementCases` and `featureVectors` cover the element layer separately, so between
them every clause of the fixture is exercised against production code.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from backtest_engine.basic_runtime import (
    BasicDecisionStatus,
    BasicPlanFlow,
    BasicPlanRuntime,
    _allocate_equally,
)
from backtest_engine.elements import (
    ElementEvaluation,
    ElementEvaluationError,
    ElementInputMissing,
    ElementSpec,
    InstrumentInput,
    InstrumentSeries,
    PlanStep,
    SeriesBar,
    StepOutcome,
    element_catalog,
    resolution_period,
)
from backtest_engine.elements.features import feature_definition


FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "conformance"
    / "strategy-bot-runtime"
    / "v1"
    / "basic-executor-conformance.v1.json"
)

FIXTURE: dict[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

IMPLEMENTATION_DEFINED = "$IMPLEMENTATION_DEFINED"


def _ids(entries: list[dict[str, Any]], key: str) -> list[str]:
    return [str(entry[key]) for entry in entries]


# ===========================================================================
# The fixture itself must stay a fixture
# ===========================================================================


def test_the_fixture_declares_the_versions_this_build_implements() -> None:
    """A fixture written for a different catalog would silently prove nothing."""
    from backtest_engine.basic_runtime import PLAN_SCHEMA_VERSION, RUNTIME_SCHEMA_VERSION
    from backtest_engine.elements.features import FEATURE_CATALOG_VERSION

    assert FIXTURE["conformanceVersion"] == "basic-executor-conformance.v1"
    assert FIXTURE["runtimeSchemaVersion"] == RUNTIME_SCHEMA_VERSION == "strategy-bot-runtime.v1"
    assert FIXTURE["planSchemaVersion"] == PLAN_SCHEMA_VERSION == "basic-compiled-plan.v1"
    assert FIXTURE["featureCatalogVersion"] == FEATURE_CATALOG_VERSION == "features:1.0.0"
    assert element_catalog(FIXTURE["elementCatalogVersion"]).version == "basic-elements:2026-07-31"


def test_the_fixture_covers_every_behaviour_card_d92_names() -> None:
    """A conformance suite that quietly lost a case is worse than none."""
    case_ids = set(_ids(FIXTURE["executorCases"], "caseId"))

    assert {
        "candidate-emitted",
        "condition-not-met-short-circuits",
        "condition-error-short-circuits",
        "input-missing-for-an-instrument-with-no-input-at-all",
        "equal-allocation-non-divisible-three-candidates",
        "equal-allocation-counts-only-survivors",
        "instrument-iteration-order-within-a-flow",
        "instrument-iteration-order-discriminates-signed-from-unsigned",
    } <= case_ids
    assert len(FIXTURE["featureVectors"]) >= 6
    assert {"LOAD_FEATURE", "COMPARE"} <= {
        case["operation"] for case in FIXTURE["elementCases"]
    }


def test_the_reason_codes_are_the_ones_the_java_executor_declares() -> None:
    """Transcribed from `BasicStrategyExecutor`'s two private constants."""
    from backtest_engine.elements.core import CONDITION_ERROR_REASON, INPUT_MISSING_REASON

    codes = FIXTURE["reasonCodes"]
    assert codes["conditionError"] == CONDITION_ERROR_REASON == "CONDITION_EVALUATION_ERROR"
    assert codes["inputMissing"] == INPUT_MISSING_REASON == "INSTRUMENT_INPUT_MISSING"
    assert codes["missingInputStepId"] == "$input"


# ===========================================================================
# featureVectors -- the arithmetic
# ===========================================================================


def _series(
    instrument_id: str,
    *,
    data_kind: str,
    resolution: str,
    first_bar_starts_at: datetime,
    closes: list[str],
) -> InstrumentSeries:
    period = resolution_period(resolution)
    return InstrumentSeries(
        instrument_id=instrument_id,
        data_kind=data_kind,
        resolution=resolution,
        bars=tuple(
            SeriesBar(
                instrument_id=instrument_id,
                resolution=resolution,
                starts_at=first_bar_starts_at + period * index,
                ends_at=first_bar_starts_at + period * (index + 1),
                close=Decimal(close),
                volume=Decimal(1000),
            )
            for index, close in enumerate(closes)
        ),
    )


@pytest.mark.parametrize(
    "vector",
    FIXTURE["featureVectors"],
    ids=_ids(FIXTURE["featureVectors"], "vectorId"),
)
def test_feature_vector(vector: dict[str, Any]) -> None:
    definition = feature_definition(vector["featureId"])
    assert definition is not None, vector["featureId"]
    window = _series(
        "00000000-0000-4000-8000-000000000301",
        data_kind=definition.data_kind,
        resolution="1m",
        first_bar_starts_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
        closes=vector["closes"],
    ).bars

    value = definition.compute(window)

    # `f"{value:f}"` and not `str(value)`: the rendered form is what reaches the step
    # trace and the reproducibility hash, so it is the form the fixture pins.
    assert f"{value:f}" == vector["expectedValue"]


def test_the_feature_definition_matches_the_fixtures_declaration() -> None:
    declared = FIXTURE["features"][0]
    definition = feature_definition(declared["featureId"])
    assert definition is not None

    assert definition.definition_version == declared["definitionVersion"]
    assert definition.method == declared["method"]
    assert definition.data_kind == declared["dataKind"]
    assert definition.periods == declared["periods"]
    assert definition.required_bars == declared["requiredBars"] == 15
    assert definition.value_scale == declared["arithmetic"]["resultScale"] == 8


def test_the_declared_resolutions_and_their_periods_are_the_ones_implemented() -> None:
    declared = FIXTURE["features"][0]
    from backtest_engine.elements.core import ISO8601_RESOLUTIONS, RESOLUTION_PERIODS

    assert list(declared["supportedResolutions"]) == list(RESOLUTION_PERIODS)
    for token, iso in declared["resolutionPeriods"].items():
        assert ISO8601_RESOLUTIONS[iso] == token
        assert resolution_period(token) == RESOLUTION_PERIODS[token]


# ===========================================================================
# elementCases -- LOAD_FEATURE and COMPARE
# ===========================================================================


ELEMENT_INSTRUMENT = "00000000-0000-4000-8000-000000000301"


def _evaluation_for(case: dict[str, Any]) -> ElementEvaluation:
    declared = case.get("series")
    series: tuple[InstrumentSeries, ...] = ()
    if declared is not None:
        series = (
            _series(
                ELEMENT_INSTRUMENT,
                data_kind=declared["dataKind"],
                resolution=declared["resolution"],
                first_bar_starts_at=_instant(declared["firstBarStartsAt"]),
                closes=declared["closes"],
            ),
        )
    evaluation = ElementEvaluation(
        instrument_id=ELEMENT_INSTRUMENT,
        as_of=_instant(case["asOf"]) if case.get("asOf") else _instant("2024-01-02T14:45:00Z"),
        inputs=InstrumentInput(instrument_id=ELEMENT_INSTRUMENT, series=series),
    )
    if case.get("operand") is not None:
        evaluation.record(str(case["operandSource"]), Decimal(str(case["operand"])))
    return evaluation


def _instant(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


@pytest.mark.parametrize(
    "case", FIXTURE["elementCases"], ids=_ids(FIXTURE["elementCases"], "caseId")
)
def test_element_case(case: dict[str, Any]) -> None:
    catalog = element_catalog(FIXTURE["elementCatalogVersion"])
    step = PlanStep(sequence=1, operation=case["operation"], arguments=dict(case["arguments"]))
    evaluation = _evaluation_for(case)
    expected = case["expected"]

    if expected["outcome"] == "INPUT_MISSING":
        with pytest.raises(ElementInputMissing) as raised:
            catalog.evaluate(step, evaluation)
        assert raised.value.reason_code == expected["reasonCode"]
        assert raised.value.input_reason == expected["inputReason"]
        assert dict(raised.value.evidence) == expected["evidence"]
        return

    if expected["outcome"] == "EVALUATION_ERROR":
        with pytest.raises(ElementEvaluationError) as failure:
            catalog.evaluate(step, evaluation)
        assert failure.value.reason_code == expected["reasonCode"]
        return

    outcome = catalog.evaluate(step, evaluation)

    assert outcome.is_passed is (expected["outcome"] == "PASSED")
    assert outcome.reason_code == expected["reasonCode"]
    assert dict(outcome.evidence) == expected["evidence"]


# ===========================================================================
# executorCases -- ordering, short-circuit, status mapping, allocation
# ===========================================================================


class _ScriptedSteps:
    """A catalog whose step outcomes come from the fixture, one per instrument.

    This is the Python counterpart of handing `BasicConditionStep` a
    `Function<BasicInstrumentInput, BasicConditionOutcome>` in Java: the executor's
    own behaviour is what is under test, so the condition results are given rather
    than computed. `MUST_NOT_BE_EVALUATED` fails the test on call, so
    short-circuiting is proved by the evaluator never running and not merely by a
    short trace.
    """

    def __init__(self, script: Mapping[str, list[dict[str, Any]]]) -> None:
        self._script = {key: list(value) for key, value in script.items()}
        self.evaluated: list[tuple[str, str]] = []
        #: The script is per instrument *per flow*: the same instrument may appear in
        #: two flows and must replay the same outcomes in each. Counting globally
        #: would run flow-b's first step against flow-a's second entry.
        self.current_flow = ""
        self._per_flow: dict[tuple[str, str], int] = {}

    def spec(self, operation: str) -> ElementSpec:  # pragma: no cover - not consulted
        raise AssertionError(f"the scripted catalog has no spec for {operation!r}")

    def evaluate(self, step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
        instrument_id = evaluation.instrument_id
        self.evaluated.append((instrument_id, _step_id(step)))
        key = (self.current_flow, instrument_id)
        index = self._per_flow.get(key, 0)
        self._per_flow[key] = index + 1
        entry = self._script[instrument_id][index]
        outcome = entry["outcome"]
        if outcome == "MUST_NOT_BE_EVALUATED":
            raise AssertionError(
                f"step {_step_id(step)} was evaluated for {instrument_id}, but the "
                "conformance fixture requires the instrument to have short-circuited "
                "before reaching it"
            )
        if outcome == "RAISES_EVALUATION_ERROR":
            raise ElementEvaluationError("scripted conformance failure")
        if outcome == "RAISES_INPUT_MISSING":
            raise ElementInputMissing(
                "scripted conformance input gap", input_reason="FEATURE_WARMUP_INCOMPLETE"
            )
        return StepOutcome(
            outcome == "PASSED", entry["reasonCode"], dict(entry.get("evidence", {}))
        )


def _step_id(step: PlanStep) -> str:
    return f"step-{step.sequence}:{step.operation}"


def _plan_step(step_id: str) -> PlanStep:
    sequence, operation = step_id.split(":", 1)
    return PlanStep(
        sequence=int(sequence.removeprefix("step-")), operation=operation, arguments={}
    )


def _instrument_input(instrument_id: str) -> InstrumentInput:
    """A minimal input. The scripted catalog never reads it; its presence is the fact."""
    return InstrumentInput(
        instrument_id=instrument_id,
        series=(
            _series(
                instrument_id,
                data_kind="ADJUSTED_BAR",
                resolution="1m",
                first_bar_starts_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
                closes=["100"],
            ),
        ),
    )


def _run_case(case: dict[str, Any]) -> tuple[list[Any], _ScriptedSteps]:
    """Drive the production executor loop over one fixture case."""
    runtime = BasicPlanRuntime()
    catalog = _ScriptedSteps(case["stepOutcomes"])
    without_input = set(case.get("instrumentsWithoutInput", ()))
    as_of = datetime(2024, 1, 2, 14, 45, tzinfo=timezone.utc)

    decisions: list[Any] = []
    for declared in case["flows"]:
        catalog.current_flow = str(declared["flowId"])
        # The fixture has always declared conditionSteps per flow, because a container is
        # what holds an AND chain. Root #202 made the runtime read them from there too,
        # so the harness hands them to the flow rather than to the plan.
        flow = BasicPlanFlow(
            partition_key="partition-1",
            flow_id=declared["flowId"],
            budget_cap_bps=10000,
            side=declared["side"],
            instrument_ids=tuple(declared["instrumentIds"]),
            condition_steps=tuple(_plan_step(item) for item in declared["conditionSteps"]),
        )
        plan = _ScriptedPlan(
            catalog=catalog,
            condition_steps=tuple(_plan_step(item) for item in declared["conditionSteps"]),
        )
        flow_decisions = []
        evaluations: dict[str, ElementEvaluation] = {}
        for instrument_id in sorted(flow.instrument_ids, key=UUID):
            supplied = None if instrument_id in without_input else _instrument_input(instrument_id)
            flow_decisions.append(
                runtime._evaluate_instrument(
                    plan, flow, instrument_id, supplied, as_of, evaluations
                )
            )
        decisions.extend(_allocate_equally(flow_decisions, declared["side"]))
    return decisions, catalog


class _ScriptedPlan:
    """The two attributes `_evaluate_instrument` reads off a loaded plan."""

    def __init__(self, *, catalog: Any, condition_steps: tuple[PlanStep, ...]) -> None:
        self.catalog = catalog
        self.condition_steps = condition_steps
        self.reference_series = ("ADJUSTED_BAR", "1m")


@pytest.mark.parametrize(
    "case", FIXTURE["executorCases"], ids=_ids(FIXTURE["executorCases"], "caseId")
)
def test_executor_case(case: dict[str, Any]) -> None:
    decisions, catalog = _run_case(case)
    expected = case["expectedDecisions"]

    assert len(decisions) == len(expected), [
        (item.flow_id, item.instrument_id, item.status.value) for item in decisions
    ]
    for produced, want in zip(decisions, expected, strict=True):
        assert produced.flow_id == want["flowId"]
        assert produced.instrument_id == want["instrumentId"]
        assert produced.side == want["side"]
        assert produced.status is BasicDecisionStatus(want["status"])
        assert produced.first_failure_step_id == want["firstFailureStepId"]
        assert produced.first_failure_reason == want["firstFailureReason"]
        _assert_allocation(produced.buy_allocation, want["buyAllocation"])
        _assert_trace(produced.trace, want["trace"])

    if "expectedEvaluationOrder" in case:
        seen: list[str] = []
        for instrument_id, _ in catalog.evaluated:
            if instrument_id not in seen:
                seen.append(instrument_id)
        assert seen == case["expectedEvaluationOrder"]


def _assert_allocation(produced: Fraction | None, want: dict[str, int] | None) -> None:
    if want is None:
        assert produced is None
        return
    assert produced is not None
    assert isinstance(produced, Fraction), type(produced)
    assert produced.numerator == want["numerator"]
    assert produced.denominator == want["denominator"]


def _assert_trace(produced: tuple[Any, ...], want: list[dict[str, Any]]) -> None:
    assert [item.step_id for item in produced] == [item["stepId"] for item in want]
    for entry, expected in zip(produced, want, strict=True):
        assert entry.passed is expected["passed"]
        assert entry.reason_code == expected["reasonCode"]
        _assert_evidence(dict(entry.evidence), expected)


def _assert_evidence(produced: dict[str, str], expected: dict[str, Any]) -> None:
    wanted: dict[str, Any] = expected["evidence"]
    for key, value in wanted.items():
        assert key in produced, f"evidence is missing {key!r}: {produced}"
        if value == IMPLEMENTATION_DEFINED:
            assert produced[key], f"evidence {key!r} must not be empty"
            continue
        assert produced[key] == value
    if not expected.get("additionalEvidenceKeysPermitted", False):
        assert set(produced) == set(wanted), (
            f"unexpected evidence keys {sorted(set(produced) - set(wanted))}"
        )


# ===========================================================================
# The divergence the fixture records must still be a real divergence
# ===========================================================================


def _java_uuid_key(value: str) -> tuple[int, int]:
    """`java.util.UUID.compareTo`: two SIGNED 64-bit longs, most significant first."""
    raw = UUID(value).bytes
    return (
        int.from_bytes(raw[:8], "big", signed=True),
        int.from_bytes(raw[8:], "big", signed=True),
    )


def test_the_recorded_ordering_divergence_is_real_and_still_diverges() -> None:
    """If Java's rule ever agreed with the fixture, the divergence entry is stale.

    The `orderUnderSignedComparison` field is what a Java implementation using
    `Comparator.naturalOrder()` produces. It is recomputed here from the signed
    comparison rule rather than trusted, so the fixture cannot record a divergence
    that does not exist -- and the day someone fixes the Java side, this test says
    the entry can be deleted.
    """
    case = next(
        item
        for item in FIXTURE["executorCases"]
        if item.get("discriminatesKnownDivergence") == "instrument-iteration-order"
    )
    declared = case["flows"][0]["instrumentIds"]

    unsigned = sorted(declared, key=UUID)
    signed = sorted(declared, key=_java_uuid_key)

    assert unsigned == case["expectedEvaluationOrder"]
    assert signed == case["orderUnderSignedComparison"]
    assert unsigned != signed, (
        "the discriminating case no longer discriminates: pick ids that straddle 0x80"
    )


def test_the_unsigned_rule_is_the_one_the_canonical_text_form_gives() -> None:
    """The justification for choosing unsigned order, asserted rather than asserted-in-prose."""
    declared = next(
        item
        for item in FIXTURE["executorCases"]
        if item.get("discriminatesKnownDivergence") == "instrument-iteration-order"
    )["flows"][0]["instrumentIds"]

    assert sorted(declared, key=UUID) == sorted(declared)


def test_every_recorded_divergence_names_a_side_an_impact_and_a_status() -> None:
    """A divergence entry with no impact or no status is a note, not a report."""
    assert FIXTURE["knownDivergences"], "the fixture records no divergences at all"
    for entry in FIXTURE["knownDivergences"]:
        assert entry["id"]
        assert entry["severity"] in {"MATERIAL", "COSMETIC", "LOW", "OBSERVATION"}
        assert entry["python"]
        assert entry["java"]
        assert entry["impact"]
        assert entry["status"]


def test_the_mid_step_input_missing_divergence_is_real() -> None:
    """D really does produce INPUT_MISSING with a non-empty trace from inside a step.

    That is the behaviour `knownDivergences[mid-step-input-missing]` describes, and
    C's executor -- which reaches INPUT_MISSING only for a null input, with an empty
    trace -- cannot produce it. Asserted here so the entry cannot go stale silently.
    """
    case = {
        "flows": [
            {
                "flowId": "flow-a",
                "side": "BUY",
                "instrumentIds": ["00000000-0000-4000-8000-000000000301"],
                "conditionSteps": ["step-1:LOAD_FEATURE"],
            }
        ],
        "stepOutcomes": {
            "00000000-0000-4000-8000-000000000301": [{"outcome": "RAISES_INPUT_MISSING"}]
        },
    }

    decisions, _ = _run_case(case)

    assert len(decisions) == 1
    only = decisions[0]
    assert only.status is BasicDecisionStatus.INPUT_MISSING
    assert only.first_failure_step_id == "step-1:LOAD_FEATURE"
    assert only.first_failure_reason == "INSTRUMENT_INPUT_MISSING"
    # The trace is NOT empty, which is precisely what C's executor cannot produce.
    assert len(only.trace) == 1
    assert only.trace[0].evidence["inputReason"] == "FEATURE_WARMUP_INCOMPLETE"


# ===========================================================================
# The fixture is shared bytes, so its digest is part of the contract
# ===========================================================================


def test_the_recorded_digest_matches_the_fixture_bytes() -> None:
    """Both languages must be able to prove they read the same bytes.

    The Java test asserts the same digest against the same file, so a fixture edited
    on one side without the other noticing fails on both.
    """
    import hashlib

    recorded = (FIXTURE_PATH.parent / "basic-executor-conformance.v1.json.sha256").read_text(
        encoding="utf-8"
    )
    digest, _, name = recorded.strip().partition("  ")

    assert name.strip() == FIXTURE_PATH.name
    assert digest == hashlib.sha256(FIXTURE_PATH.read_bytes()).hexdigest()


def test_the_fixture_is_read_as_bytes_not_regenerated() -> None:
    """The file on disk is the artefact; nothing in this repository writes it."""
    package = Path(__file__).resolve().parents[1] / "src"
    offenders = [
        path.name
        for path in package.rglob("*.py")
        if "basic-executor-conformance" in path.read_text(encoding="utf-8")
    ]

    assert offenders == [], (
        "production code references the conformance fixture; it must be a test input "
        f"only, otherwise the fixture and the implementation are one artefact: {offenders}"
    )


def test_bar_windowing_matches_the_declared_convention() -> None:
    """`[S, S+R)`, completed when `S+R <= T`. The look-ahead rule, pinned."""
    assert FIXTURE["conventions"]["barWindow"].startswith(
        "A bar declared with startsAt S at resolution R spans [S, S+R)."
    )
    series = _series(
        ELEMENT_INSTRUMENT,
        data_kind="ADJUSTED_BAR",
        resolution="1m",
        first_bar_starts_at=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
        closes=["100", "101"],
    )
    boundary = datetime(2024, 1, 2, 14, 31, tzinfo=timezone.utc)

    assert len(series.completed_through(boundary)) == 1
    assert len(series.completed_through(boundary - timedelta(seconds=1))) == 0
    assert len(series.completed_through(boundary + timedelta(minutes=1))) == 2
