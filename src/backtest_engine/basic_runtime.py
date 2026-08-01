"""Basic compiled-plan compatibility runtime shared with trading semantics."""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from fractions import Fraction
from types import MappingProxyType
from typing import Any


PLAN_SCHEMA_VERSION = "basic-compiled-plan.v1"
COMPILER_VERSION = "basic-compiler:1.0.0"
POLICY_VERSIONS = {
    "evaluationPolicyVersion": "basic-evaluation:1.0.0",
    "allocationPolicyVersion": "basic-allocation:1.0.0",
    "orderCandidatePolicyVersion": "basic-order-candidate:1.0.0",
}
ORDER_ELEMENTS = {"BUY": "BUY_ORDER", "SELL": "SELL_ORDER"}


class BasicPlanCompatibilityError(ValueError):
    """Raised when a compiled plan cannot preserve supported Basic meaning."""


class BasicDecisionStatus(str, Enum):
    CANDIDATE = "CANDIDATE"
    CONDITION_NOT_MET = "CONDITION_NOT_MET"
    CONDITION_ERROR = "CONDITION_ERROR"
    INPUT_MISSING = "INPUT_MISSING"


@dataclass(frozen=True, slots=True)
class BasicStepOutcome:
    is_passed: bool
    reason_code: str
    evidence: Mapping[str, str]

    def __post_init__(self) -> None:
        if not self.reason_code:
            raise ValueError("reason_code must not be empty")
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))

    @classmethod
    def passed(
        cls, reason_code: str, evidence: Mapping[str, str] | None = None
    ) -> BasicStepOutcome:
        return cls(True, reason_code, evidence or {})

    @classmethod
    def failed(
        cls, reason_code: str, evidence: Mapping[str, str] | None = None
    ) -> BasicStepOutcome:
        return cls(False, reason_code, evidence or {})


@dataclass(frozen=True, slots=True)
class BasicStepTrace:
    step_id: str
    passed: bool
    reason_code: str
    evidence: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))


@dataclass(frozen=True, slots=True)
class BasicPlanStep:
    sequence: int
    step_id: str
    element_code: str
    parameters: Mapping[str, Any]

    def __post_init__(self) -> None:
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))


@dataclass(frozen=True, slots=True)
class BasicPlanFlow:
    flow_id: str
    side: str
    instrument_ids: tuple[str, ...]
    condition_steps: tuple[BasicPlanStep, ...]


@dataclass(frozen=True, slots=True)
class BasicCompiledPlan:
    schema_version: str
    compiler_version: str
    flows: tuple[BasicPlanFlow, ...]


@dataclass(frozen=True, slots=True)
class BasicInstrumentDecision:
    flow_id: str
    instrument_id: str
    side: str
    status: BasicDecisionStatus
    trace: tuple[BasicStepTrace, ...]
    first_failure_step_id: str | None = None
    first_failure_reason: str | None = None
    buy_allocation: Fraction | None = None


@dataclass(frozen=True, slots=True)
class BasicExecutionResult:
    decisions: tuple[BasicInstrumentDecision, ...]


StepEvaluator = Callable[[Mapping[str, Any], Mapping[str, Any]], BasicStepOutcome]


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise BasicPlanCompatibilityError(f"{label} must be an object")
    return value


def _sequence(value: object, label: str) -> Sequence[Any]:
    if not isinstance(value, list):
        raise BasicPlanCompatibilityError(f"{label} must be an array")
    return value


def _text(document: Mapping[str, Any], field: str, label: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value:
        raise BasicPlanCompatibilityError(f"{label}.{field} must be a non-empty string")
    return value


class BasicPlanRuntime:
    """Loads B plans and evaluates them with C14-compatible deterministic rules."""

    def __init__(self, evaluators: Mapping[str, StepEvaluator]) -> None:
        if not evaluators:
            raise ValueError("evaluators must not be empty")
        if any(not code or not callable(evaluator) for code, evaluator in evaluators.items()):
            raise ValueError("evaluators must map element codes to callables")
        self._evaluators = MappingProxyType(dict(evaluators))

    def load(self, document: Mapping[str, Any]) -> BasicCompiledPlan:
        root = _mapping(document, "plan")
        schema_version = _text(root, "schemaVersion", "plan")
        if schema_version != PLAN_SCHEMA_VERSION:
            raise BasicPlanCompatibilityError(
                f"schemaVersion must be {PLAN_SCHEMA_VERSION}"
            )
        compiler_version = _text(root, "compilerVersion", "plan")
        if compiler_version != COMPILER_VERSION:
            raise BasicPlanCompatibilityError(
                f"compilerVersion must be {COMPILER_VERSION}"
            )

        policies = _mapping(root.get("policyVersions"), "plan.policyVersions")
        for field, supported in POLICY_VERSIONS.items():
            if policies.get(field) != supported:
                raise BasicPlanCompatibilityError(f"{field} must be {supported}")

        raw_flows = _sequence(root.get("flows"), "plan.flows")
        if not raw_flows:
            raise BasicPlanCompatibilityError("plan.flows must not be empty")
        flows = tuple(self._load_flow(value, index) for index, value in enumerate(raw_flows))
        if len({flow.flow_id for flow in flows}) != len(flows):
            raise BasicPlanCompatibilityError("flow keys must be unique")
        return BasicCompiledPlan(schema_version, compiler_version, flows)

    def _load_flow(self, value: object, index: int) -> BasicPlanFlow:
        label = f"plan.flows[{index}]"
        flow = _mapping(value, label)
        flow_id = _text(flow, "key", label)
        side = _text(flow, "container", label)
        if side not in ORDER_ELEMENTS:
            raise BasicPlanCompatibilityError(f"{label}.container is unsupported")
        if flow.get("evaluationMode") != "INDEPENDENT":
            raise BasicPlanCompatibilityError(
                f"{label}.evaluationMode must be INDEPENDENT"
            )
        if flow.get("allocationMode") != "EQUAL":
            raise BasicPlanCompatibilityError(f"{label}.allocationMode must be EQUAL")

        instrument_ids = tuple(
            self._instrument_id(raw, f"{label}.instrumentIds[{position}]")
            for position, raw in enumerate(
                _sequence(flow.get("instrumentIds"), f"{label}.instrumentIds")
            )
        )
        if not instrument_ids or len(set(instrument_ids)) != len(instrument_ids):
            raise BasicPlanCompatibilityError(
                f"{label}.instrumentIds must be non-empty and unique"
            )

        steps = tuple(
            self._load_step(raw, position, label)
            for position, raw in enumerate(
                _sequence(flow.get("steps"), f"{label}.steps")
            )
        )
        if len(steps) < 2 or len({step.step_id for step in steps}) != len(steps):
            raise BasicPlanCompatibilityError(
                f"{label}.steps must contain unique evaluation and order steps"
            )
        expected_order = ORDER_ELEMENTS[side]
        if steps[-1].element_code != expected_order:
            raise BasicPlanCompatibilityError(
                f"{label}.steps must end with {expected_order}"
            )
        for step in steps[:-1]:
            if step.element_code in ORDER_ELEMENTS.values():
                raise BasicPlanCompatibilityError(
                    f"{label} contains an order before the final step"
                )
            if step.element_code not in self._evaluators:
                raise BasicPlanCompatibilityError(
                    f"unsupported elementCode: {step.element_code}"
                )
        self._validate_connections(flow, steps, label)
        return BasicPlanFlow(flow_id, side, instrument_ids, steps[:-1])

    @staticmethod
    def _instrument_id(value: object, label: str) -> str:
        if not isinstance(value, str):
            raise BasicPlanCompatibilityError(f"{label} must be a UUID")
        try:
            return str(uuid.UUID(value))
        except ValueError as exc:
            raise BasicPlanCompatibilityError(f"{label} must be a UUID") from exc

    @staticmethod
    def _load_step(value: object, position: int, flow_label: str) -> BasicPlanStep:
        label = f"{flow_label}.steps[{position}]"
        step = _mapping(value, label)
        sequence = step.get("sequence")
        if sequence != position + 1:
            raise BasicPlanCompatibilityError(
                f"{flow_label}.steps sequence must be contiguous"
            )
        return BasicPlanStep(
            sequence=sequence,
            step_id=_text(step, "key", label),
            element_code=_text(step, "elementCode", label),
            parameters=_mapping(step.get("parameters"), f"{label}.parameters"),
        )

    @staticmethod
    def _validate_connections(
        flow: Mapping[str, Any], steps: tuple[BasicPlanStep, ...], label: str
    ) -> None:
        connections = _sequence(flow.get("connections"), f"{label}.connections")
        if len(connections) != len(steps) - 1:
            raise BasicPlanCompatibilityError(
                f"{label}.connections must form one sequential flow"
            )
        for index, raw in enumerate(connections):
            connection = _mapping(raw, f"{label}.connections[{index}]")
            if (
                connection.get("fromBlockId") != steps[index].step_id
                or connection.get("toBlockId") != steps[index + 1].step_id
            ):
                raise BasicPlanCompatibilityError(
                    f"{label}.connections must follow step sequence"
                )

    def execute(
        self,
        plan: BasicCompiledPlan,
        instrument_inputs: Mapping[str, Mapping[str, Any]],
    ) -> BasicExecutionResult:
        decisions: list[BasicInstrumentDecision] = []
        for flow in plan.flows:
            flow_decisions = [
                self._evaluate_instrument(
                    flow, instrument_id, instrument_inputs.get(instrument_id)
                )
                for instrument_id in sorted(flow.instrument_ids, key=uuid.UUID)
            ]
            decisions.extend(self._allocate(flow_decisions))
        return BasicExecutionResult(tuple(decisions))

    def _evaluate_instrument(
        self,
        flow: BasicPlanFlow,
        instrument_id: str,
        instrument_input: Mapping[str, Any] | None,
    ) -> BasicInstrumentDecision:
        if instrument_input is None:
            return BasicInstrumentDecision(
                flow.flow_id,
                instrument_id,
                flow.side,
                BasicDecisionStatus.INPUT_MISSING,
                (),
                "$input",
                "INSTRUMENT_INPUT_MISSING",
            )

        trace: list[BasicStepTrace] = []
        for step in flow.condition_steps:
            try:
                outcome = self._evaluators[step.element_code](
                    step.parameters, instrument_input
                )
                if not isinstance(outcome, BasicStepOutcome):
                    raise TypeError("evaluator must return BasicStepOutcome")
                trace.append(
                    BasicStepTrace(
                        step.step_id,
                        outcome.is_passed,
                        outcome.reason_code,
                        outcome.evidence,
                    )
                )
                if not outcome.is_passed:
                    return BasicInstrumentDecision(
                        flow.flow_id,
                        instrument_id,
                        flow.side,
                        BasicDecisionStatus.CONDITION_NOT_MET,
                        tuple(trace),
                        step.step_id,
                        outcome.reason_code,
                    )
            except Exception as failure:
                trace.append(
                    BasicStepTrace(
                        step.step_id,
                        False,
                        "CONDITION_EVALUATION_ERROR",
                        {"errorType": type(failure).__name__},
                    )
                )
                return BasicInstrumentDecision(
                    flow.flow_id,
                    instrument_id,
                    flow.side,
                    BasicDecisionStatus.CONDITION_ERROR,
                    tuple(trace),
                    step.step_id,
                    "CONDITION_EVALUATION_ERROR",
                )

        return BasicInstrumentDecision(
            flow.flow_id,
            instrument_id,
            flow.side,
            BasicDecisionStatus.CANDIDATE,
            tuple(trace),
        )

    @staticmethod
    def _allocate(
        decisions: list[BasicInstrumentDecision],
    ) -> tuple[BasicInstrumentDecision, ...]:
        if not decisions or decisions[0].side != "BUY":
            return tuple(decisions)
        candidate_count = sum(
            decision.status is BasicDecisionStatus.CANDIDATE
            for decision in decisions
        )
        if candidate_count == 0:
            return tuple(decisions)
        share = Fraction(1, candidate_count)
        return tuple(
            replace(decision, buy_allocation=share)
            if decision.status is BasicDecisionStatus.CANDIDATE
            else decision
            for decision in decisions
        )
