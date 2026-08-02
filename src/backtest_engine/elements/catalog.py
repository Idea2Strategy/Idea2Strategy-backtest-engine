"""The versioned element catalog and the evaluators B's plan operations need.

B's ``basic-compiled-plan.v1`` declares ``elementCatalogVersion``. That string
selects an :class:`ElementCatalog` here: the set of operations, the arguments
each accepts, the enumerated values each argument may take, and the feature
definition version each feature name resolves to.

Nothing is resolved by convention. An operation, argument, argument value,
feature or catalog version this build does not implement raises
:class:`~backtest_engine.elements.core.ElementCompatibilityError` at *load*
time, so an unrecognised element can never be silently skipped at replay time.

Operation semantics (normative for card D92)
--------------------------------------------

``LOAD_FEATURE {feature, resolution}``
    Reads the instrument's ``(definition.data_kind, resolution)`` series, takes
    the last ``definition.required_bars`` bars completed at or before the
    evaluation instant, computes the feature, and records it as the operand for
    the next comparison. Always passes when it produces a value
    (``reason_code = "FEATURE_LOADED"``). If the series is absent or too short
    it raises ``ElementInputMissing`` - it never yields a substituted value and
    never reports "condition false".

``COMPARE {operator, threshold}``
    Compares the operand produced by the preceding ``LOAD_FEATURE`` against
    ``threshold``, parsed as an exact decimal, using ``operator`` in
    ``LT, LTE, GT, GTE, EQ, NEQ``. Numeric comparison, never string comparison:
    ``EQ`` with ``"20"`` and with ``"20.00000000"`` both hold for an operand of
    20. Passes with ``"COMPARE_TRUE"``, fails with ``"COMPARE_FALSE"``. A
    comparison with no preceding value is a plan-structure defect and raises
    ``ElementEvaluationError``.

``EMIT_ORDER_CANDIDATE {allocation, orderType, side}``
    Terminal. It is not evaluated per instrument: it declares what the flow
    emits for the instruments that survived every preceding step. The loader
    reads ``side`` (``BUY``/``SELL``), ``allocation`` (``EQUAL``) and
    ``orderType`` (``MARKET``) from it. Calling :meth:`ElementCatalog.evaluate`
    on it is a programming error.
"""

from __future__ import annotations

import operator as operator_module
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from types import MappingProxyType

from backtest_engine.elements.core import (
    SUPPORTED_RESOLUTIONS,
    ElementCompatibilityError,
    ElementEvaluation,
    ElementEvaluationError,
    ElementInputMissing,
    PlanLoadFailure,
    PlanStep,
    StepEvaluator,
    StepOutcome,
)
from backtest_engine.elements.features import FEATURE_REGISTRY, feature_definition


__all__ = [
    "COMPARISON_OPERATORS",
    "ELEMENT_CATALOGS",
    "ELEMENT_CATALOG_VERSIONS",
    "ElementCatalog",
    "ElementSpec",
    "element_catalog",
    "supported_feature_versions",
]


COMPARISON_OPERATORS: Mapping[str, Callable[[Decimal, Decimal], bool]] = MappingProxyType(
    {
        "LT": operator_module.lt,
        "LTE": operator_module.le,
        "GT": operator_module.gt,
        "GTE": operator_module.ge,
        "EQ": operator_module.eq,
        "NEQ": operator_module.ne,
    }
)

_TERMINAL_SIDES = ("BUY", "SELL")
_TERMINAL_ALLOCATIONS = ("EQUAL",)
_TERMINAL_ORDER_TYPES = ("MARKET",)


def _reject(failure: PlanLoadFailure, detail: str) -> ElementCompatibilityError:
    return ElementCompatibilityError(failure, detail)


def _parse_decimal(step: PlanStep, name: str) -> Decimal:
    raw = step.argument(name)
    try:
        value = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise _reject(
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            f"{step.operation} argument {name!r} must be a decimal number, got {raw!r}",
        ) from exc
    if not value.is_finite():
        raise _reject(
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            f"{step.operation} argument {name!r} must be finite, got {raw!r}",
        )
    return value


def _evaluate_load_feature(step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
    feature_id = step.argument("feature")
    resolution = step.argument("resolution")
    definition = feature_definition(feature_id)
    if definition is None:  # pragma: no cover - validate_step rejects this first
        raise _reject(
            PlanLoadFailure.UNSUPPORTED_FEATURE,
            f"feature {feature_id!r} is not implemented by this build",
        )

    series = evaluation.inputs.series_for(definition.data_kind, resolution)
    if series is None:
        raise ElementInputMissing(
            f"instrument {evaluation.instrument_id} has no "
            f"{definition.data_kind}/{resolution} series for {feature_id}",
            input_reason="FEATURE_SERIES_MISSING",
            evidence={
                "feature": feature_id,
                "dataKind": definition.data_kind,
                "resolution": resolution,
            },
        )

    completed = series.completed_through(evaluation.as_of)
    if len(completed) < definition.required_bars:
        raise ElementInputMissing(
            f"{feature_id} needs {definition.required_bars} completed "
            f"{resolution} bars at {evaluation.as_of.isoformat()}, "
            f"{len(completed)} are available",
            input_reason="FEATURE_WARMUP_INCOMPLETE",
            evidence={
                "feature": feature_id,
                "resolution": resolution,
                "requiredBars": str(definition.required_bars),
                "availableBars": str(len(completed)),
                "asOf": evaluation.as_of.isoformat(),
            },
        )

    window = completed[-definition.required_bars :]
    value = definition.compute(window)
    evaluation.record(feature_id, value)
    return StepOutcome.passed(
        "FEATURE_LOADED",
        {
            "feature": feature_id,
            "featureVersion": definition.definition_version,
            "resolution": resolution,
            "value": f"{value:f}",
            "asOf": evaluation.as_of.isoformat(),
            "windowFrom": window[0].starts_at.isoformat(),
            "windowThrough": window[-1].ends_at.isoformat(),
        },
    )


def _evaluate_compare(step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
    name = step.argument("operator")
    comparison = COMPARISON_OPERATORS.get(name)
    if comparison is None:  # pragma: no cover - validate_step rejects this first
        raise _reject(
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            f"COMPARE operator {name!r} is not supported",
        )
    threshold = _parse_decimal(step, "threshold")
    operand = evaluation.require_operand("COMPARE")
    evidence = {
        "operator": name,
        "operand": f"{operand:f}",
        "threshold": step.argument("threshold"),
        "source": evaluation.last_value_source or "",
    }
    if comparison(operand, threshold):
        return StepOutcome.passed("COMPARE_TRUE", evidence)
    return StepOutcome.failed("COMPARE_FALSE", evidence)


def _evaluate_terminal(step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
    del evaluation
    raise ElementEvaluationError(
        f"{step.operation} is a terminal element: the plan loader consumes it, "
        "it is never evaluated per instrument"
    )


@dataclass(frozen=True, slots=True)
class ElementSpec:
    """What one operation accepts and what it does to the evaluation state."""

    operation: str
    required_arguments: tuple[str, ...]
    enumerations: Mapping[str, tuple[str, ...]]
    decimal_arguments: tuple[str, ...]
    feature_arguments: tuple[str, ...]
    terminal: bool
    produces_value: bool
    consumes_value: bool
    evaluator: StepEvaluator


@dataclass(frozen=True, slots=True)
class ElementCatalog:
    """The element set pinned by one ``elementCatalogVersion``."""

    version: str
    specs: Mapping[str, ElementSpec]
    feature_versions: Mapping[str, str]
    canonical_feature_ids: Mapping[str, str]
    """B's ``requiredFeature.featureId`` UUID -> this catalog's feature name.

    B addresses a feature by canonical UUID in ``requiredFeatures`` but by
    element-catalog name in a ``LOAD_FEATURE`` step argument. Both are B's own
    vocabulary; this table is the pinned join between them. It is deliberately
    part of the catalog rather than a module constant, so a new
    ``elementCatalogVersion`` can re-point a UUID without silently changing the
    meaning of plans already compiled against the old one.
    """

    @property
    def operations(self) -> tuple[str, ...]:
        return tuple(self.specs)

    def spec(self, operation: str) -> ElementSpec:
        try:
            return self.specs[operation]
        except KeyError as exc:
            raise _reject(
                PlanLoadFailure.UNSUPPORTED_ELEMENT,
                f"operation {operation!r} is not in element catalog {self.version}",
            ) from exc

    def validate_step(self, step: PlanStep) -> ElementSpec:
        """Reject anything this catalog version cannot execute. Load-time only."""
        spec = self.spec(step.operation)
        supplied = set(step.arguments)
        required = set(spec.required_arguments)
        missing = sorted(required - supplied)
        if missing:
            raise _reject(
                PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                f"{step.operation} step {step.sequence} is missing required "
                f"argument(s): {', '.join(missing)}",
            )
        unknown = sorted(supplied - required)
        if unknown:
            raise _reject(
                PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                f"{step.operation} step {step.sequence} carries argument(s) this "
                f"build does not implement: {', '.join(unknown)}",
            )
        for name, allowed in spec.enumerations.items():
            value = step.arguments[name]
            if value not in allowed:
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                    f"{step.operation} argument {name}={value!r} is not one of "
                    + ", ".join(allowed),
                )
        for name in spec.decimal_arguments:
            _parse_decimal(step, name)
        for name in spec.feature_arguments:
            self.require_feature(step.arguments[name])
        return spec

    def require_feature(self, feature_id: str) -> str:
        """The definition version this catalog pins for ``feature_id``."""
        try:
            return self.feature_versions[feature_id]
        except KeyError as exc:
            raise _reject(
                PlanLoadFailure.UNSUPPORTED_FEATURE,
                f"feature {feature_id!r} is not in element catalog {self.version}",
            ) from exc

    def require_canonical_feature(self, canonical_feature_id: str) -> str:
        """The catalog feature name B's ``featureId`` UUID refers to.

        No inference from a name and no default: an unknown UUID is refused, so
        a feature this build has never heard of can never be quietly treated as
        a feature it does implement.
        """
        try:
            return self.canonical_feature_ids[canonical_feature_id]
        except KeyError as exc:
            raise _reject(
                PlanLoadFailure.UNSUPPORTED_FEATURE,
                f"requiredFeature.featureId {canonical_feature_id!r} is not in "
                f"element catalog {self.version}",
            ) from exc

    def evaluate(self, step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
        return self.spec(step.operation).evaluator(step, evaluation)


_BASIC_ELEMENTS_2026_07_31 = ElementCatalog(
    version="basic-elements:2026-07-31",
    specs=MappingProxyType(
        {
            "LOAD_FEATURE": ElementSpec(
                operation="LOAD_FEATURE",
                required_arguments=("feature", "resolution"),
                enumerations=MappingProxyType({"resolution": SUPPORTED_RESOLUTIONS}),
                decimal_arguments=(),
                feature_arguments=("feature",),
                terminal=False,
                produces_value=True,
                consumes_value=False,
                evaluator=_evaluate_load_feature,
            ),
            "COMPARE": ElementSpec(
                operation="COMPARE",
                required_arguments=("operator", "threshold"),
                enumerations=MappingProxyType(
                    {"operator": tuple(COMPARISON_OPERATORS)}
                ),
                decimal_arguments=("threshold",),
                feature_arguments=(),
                terminal=False,
                produces_value=False,
                consumes_value=True,
                evaluator=_evaluate_compare,
            ),
            "EMIT_ORDER_CANDIDATE": ElementSpec(
                operation="EMIT_ORDER_CANDIDATE",
                required_arguments=("allocation", "orderType", "side"),
                enumerations=MappingProxyType(
                    {
                        "allocation": _TERMINAL_ALLOCATIONS,
                        "orderType": _TERMINAL_ORDER_TYPES,
                        "side": _TERMINAL_SIDES,
                    }
                ),
                decimal_arguments=(),
                feature_arguments=(),
                terminal=True,
                produces_value=False,
                consumes_value=False,
                evaluator=_evaluate_terminal,
            ),
        }
    ),
    feature_versions=MappingProxyType(
        {
            "RSI_14": FEATURE_REGISTRY["RSI_14"].definition_version,
        }
    ),
    canonical_feature_ids=MappingProxyType(
        {
            # Transcribed from B's published fixture requiredFeatures[0].
            "00000000-0000-4000-8000-000000000401": "RSI_14",
        }
    ),
)


ELEMENT_CATALOGS: Mapping[str, ElementCatalog] = MappingProxyType(
    {_BASIC_ELEMENTS_2026_07_31.version: _BASIC_ELEMENTS_2026_07_31}
)

ELEMENT_CATALOG_VERSIONS: tuple[str, ...] = tuple(ELEMENT_CATALOGS)


def element_catalog(version: str) -> ElementCatalog:
    """Resolve ``plan.elementCatalogVersion``. No fallback, no nearest match."""
    try:
        return ELEMENT_CATALOGS[version]
    except KeyError as exc:
        raise _reject(
            PlanLoadFailure.ELEMENT_CATALOG_VERSION_UNSUPPORTED,
            f"elementCatalogVersion {version!r} is not implemented by this build; "
            "implemented: " + ", ".join(ELEMENT_CATALOG_VERSIONS),
        ) from exc


def supported_feature_versions() -> dict[str, str]:
    """Feature versions this build actually implements.

    This is the D counterpart of
    ``ExecutionPlanCompatibility.supportedFeatureVersions()``: it comes from the
    *implementation registry*, not from a catalog entry, so a catalog that pins
    a version the code does not implement is a ``FEATURE_VERSION_MISMATCH``
    rather than a silent agreement with itself.
    """
    return {
        definition.feature_id: definition.definition_version
        for definition in FEATURE_REGISTRY.values()
    }
