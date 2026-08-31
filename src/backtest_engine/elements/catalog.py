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
from dataclasses import dataclass, field, replace
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation
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
    resolution_period,
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
_PRODUCTION_RESOLUTIONS = ("30m", "1h", "4h", "1d")
_EXECUTION_MODES = ("1회만", "주기마다", "대기 후 재진입", "대기 후 재실행")
_WAIT_MODES = ("조건 재충족", "N봉 이후", "N거래일 이후")
_MATH = Context(prec=18, rounding=ROUND_HALF_UP)


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

    if evaluation.inputs.require_pinned_features:
        pinned = evaluation.inputs.feature_series_for(feature_id, resolution)
        if pinned is None:
            raise ElementInputMissing(
                f"instrument {evaluation.instrument_id} has no pinned {feature_id}/{resolution} feature series",
                input_reason="FEATURE_SERIES_MISSING",
                evidence={
                    "feature": feature_id,
                    "resolution": resolution,
                    "asOf": evaluation.as_of.isoformat(),
                },
            )
        value = pinned.value_at(evaluation.as_of)
        evaluation.record(feature_id, value)
        return StepOutcome.passed(
            "FEATURE_LOADED",
            {
                "feature": feature_id,
                "featureVersion": definition.definition_version,
                "resolution": resolution,
                "value": f"{value:f}",
                "asOf": evaluation.as_of.isoformat(),
                "source": "PINNED_FEATURE_OUTPUT",
            },
        )

    series = evaluation.inputs.series_for(definition.data_kind, resolution)
    if series is None:
        raise ElementInputMissing(
            f"instrument {evaluation.instrument_id} has no {definition.data_kind}/{resolution} series for {feature_id}",
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


def _value(evaluation: ElementEvaluation, key: str, operation: str) -> str:
    raw = evaluation.inputs.values.get(key)
    if raw is None or not raw.strip():
        raise ElementInputMissing(
            f"{operation} needs runtime input {key}",
            input_reason="FEATURE_WARMUP_INCOMPLETE",
            evidence={"operation": operation, "source": key, "requiredBars": "1"},
        )
    return raw


def _decimal_value(evaluation: ElementEvaluation, key: str, operation: str) -> Decimal:
    raw = _value(evaluation, key, operation)
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ElementEvaluationError(f"{operation} runtime input {key} must be a decimal number, got {raw!r}") from exc
    if not value.is_finite():
        raise ElementEvaluationError(f"{operation} runtime input {key} must be finite, got {raw!r}")
    return value


def _series_values(evaluation: ElementEvaluation, key: str, required: int, operation: str) -> list[Decimal]:
    raw = _value(evaluation, key, operation)
    values: list[Decimal] = []
    for item in raw.split(","):
        if not item:
            continue
        try:
            value = Decimal(item)
        except InvalidOperation as exc:
            raise ElementEvaluationError(
                f"{operation} runtime input {key} must contain decimal numbers, got {item!r}"
            ) from exc
        if not value.is_finite():
            raise ElementEvaluationError(f"{operation} runtime input {key} must contain finite numbers, got {item!r}")
        values.append(value)
    if len(values) < required:
        raise ElementInputMissing(
            f"{operation} needs {required} values from {key}, got {len(values)}",
            input_reason="FEATURE_WARMUP_INCOMPLETE",
            evidence={
                "operation": operation,
                "source": key,
                "requiredBars": str(required),
                "availableBars": str(len(values)),
            },
        )
    return values


def _plain(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _average(values: list[Decimal]) -> Decimal:
    return _MATH.divide(sum(values, Decimal(0)), Decimal(len(values)))


def _trailing_average(values: list[Decimal], period: int, offset: int = 0) -> Decimal:
    end = len(values) - offset
    return _average(values[end - period : end])


def _percent(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator == 0:
        raise ElementEvaluationError("percentage denominator must not be zero")
    return _MATH.divide(_MATH.multiply(numerator, Decimal(100)), denominator)


def _outcome(operation: str, passed: bool, evidence: Mapping[str, str]) -> StepOutcome:
    reason = f"{operation}_{'TRUE' if passed else 'FALSE'}"
    return StepOutcome.passed(reason, evidence) if passed else StepOutcome.failed(reason, evidence)


def _compared(
    operation: str,
    operator_name: str,
    left: Decimal,
    right: Decimal,
    evidence: Mapping[str, str] | None = None,
) -> StepOutcome:
    comparison = COMPARISON_OPERATORS[operator_name]
    details = dict(evidence or {})
    details.update({"operator": operator_name, "left": _plain(left), "right": _plain(right)})
    return _outcome(operation, comparison(left, right), details)


def _reference_price(
    step: PlanStep,
    evaluation: ElementEvaluation,
    reference: str,
    closes: list[Decimal],
) -> Decimal:
    operation = step.operation
    if reference == "PREVIOUS_CLOSE":
        if len(closes) < 2:
            return _series_values(evaluation, f"closes.{step.argument('resolution')}", 2, operation)[-2]
        return closes[-2]
    if reference == "SESSION_OPEN":
        return _decimal_value(evaluation, "session.open", operation)
    if reference == "AVERAGE_ENTRY_PRICE":
        return _decimal_value(evaluation, "position.averageEntryPrice", operation)
    prefix, _, raw_period = reference.partition("_")
    try:
        period = int(raw_period)
    except ValueError as exc:
        raise ElementEvaluationError(f"unsupported price reference {reference}") from exc
    resolution = step.argument("resolution")
    values = _series_values(
        evaluation, f"closes.{resolution}", period + (1 if prefix in {"HIGH", "LOW"} else 0), operation
    )
    if prefix == "SMA":
        return _trailing_average(values, period)
    window = values[-period - 1 : -1]
    if prefix == "HIGH":
        return max(window)
    if prefix == "LOW":
        return min(window)
    raise ElementEvaluationError(f"unsupported price reference {reference}")


def _rsi(closes: list[Decimal], period: int, offset: int) -> Decimal:
    end = len(closes) - offset
    start = end - period - 1
    gains = Decimal(0)
    losses = Decimal(0)
    for index in range(start + 1, end):
        change = closes[index] - closes[index - 1]
        if change > 0:
            gains += change
        else:
            losses += abs(change)
    if losses == 0:
        return Decimal(0) if gains == 0 else Decimal(100)
    relative_strength = _MATH.divide(gains, losses)
    return Decimal(100) - _MATH.divide(Decimal(100), Decimal(1) + relative_strength)


def _ema(values: list[Decimal], period: int) -> list[Decimal]:
    alpha = _MATH.divide(Decimal(2), Decimal(period + 1))
    current = values[0]
    result = [current]
    for value in values[1:]:
        current = _MATH.add(_MATH.multiply(value, alpha), _MATH.multiply(current, Decimal(1) - alpha))
        result.append(current)
    return result


def _evaluate_catalog_operation(step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
    operation = step.operation
    resolution = step.arguments.get("resolution")
    if (
        resolution is not None
        and operation not in {"HOLDING_PERIOD", "SCHEDULE"}
        and evaluation.inputs.values.get(f"bar.closed.{resolution}", "false").lower() != "true"
    ):
        return StepOutcome.failed(
            "WAITING_FOR_BAR_CLOSE",
            {"operation": operation, "resolution": resolution},
        )

    if operation == "PRICE_COMPARE":
        closes = _series_values(evaluation, f"closes.{resolution}", 1, operation)
        reference = step.argument("reference")
        return _compared(
            operation,
            step.argument("operator"),
            closes[-1],
            _reference_price(step, evaluation, reference, closes),
            {"reference": reference, "resolution": resolution or ""},
        )
    if operation == "PRICE_CHANGE_PERCENT":
        closes = _series_values(evaluation, f"closes.{resolution}", 1, operation)
        base = step.argument("base")
        change = _percent(
            closes[-1] - _reference_price(step, evaluation, base, closes),
            _reference_price(step, evaluation, base, closes),
        )
        threshold = _parse_decimal(step, "thresholdPercent")
        direction = step.argument("direction")
        passed = change >= threshold if direction == "UP" else change <= -threshold
        return _outcome(
            operation,
            passed,
            {
                "direction": direction,
                "changePercent": _plain(change),
                "thresholdPercent": _plain(threshold),
                "base": base,
            },
        )
    if operation == "VOLUME_COMPARE":
        reference = step.argument("reference")
        period = int(step.argument("period"))
        volumes = _series_values(
            evaluation, f"volumes.{resolution}", 2 if reference == "PREVIOUS_VOLUME" else period + 1, operation
        )
        expected = volumes[-2] if reference == "PREVIOUS_VOLUME" else _average(volumes[-period - 1 : -1])
        expected = _MATH.multiply(expected, _parse_decimal(step, "multiplier"))
        return _compared(
            operation,
            step.argument("operator"),
            volumes[-1],
            expected,
            {"reference": reference, "period": str(period), "multiplier": step.argument("multiplier")},
        )
    if operation == "STREAK":
        bars = int(step.argument("bars"))
        closes = _series_values(evaluation, f"closes.{resolution}", bars + 1, operation)
        direction = step.argument("direction")
        count = 0
        for current, previous in zip(reversed(closes[1:]), reversed(closes[:-1]), strict=True):
            if (direction == "UP" and current > previous) or (direction == "DOWN" and current < previous):
                count += 1
            else:
                break
        return _outcome(
            operation, count >= bars, {"direction": direction, "streak": str(count), "requiredBars": str(bars)}
        )
    if operation == "SMA_CROSS":
        short, long = int(step.argument("shortPeriod")), int(step.argument("longPeriod"))
        closes = _series_values(evaluation, f"closes.{resolution}", long + 1, operation)
        ps, pl = _trailing_average(closes, short, 1), _trailing_average(closes, long, 1)
        cs, cl = _trailing_average(closes, short), _trailing_average(closes, long)
        direction = step.argument("direction")
        passed = ps <= pl and cs > cl if direction == "UP" else ps >= pl and cs < cl
        return _outcome(operation, passed, {"direction": direction, "short": _plain(cs), "long": _plain(cl)})
    if operation == "RSI_CROSS":
        rsi_resolution = step.argument("resolution")
        pinned = evaluation.inputs.feature_series_for("RSI_14", rsi_resolution)
        if pinned is None:
            raise ElementInputMissing(
                f"instrument {evaluation.instrument_id} has no pinned RSI_14/{rsi_resolution} feature series",
                input_reason="FEATURE_SERIES_MISSING",
                evidence={
                    "feature": "RSI_14",
                    "resolution": rsi_resolution,
                    "asOf": evaluation.as_of.isoformat(),
                },
            )
        current = pinned.value_at(evaluation.as_of)
        previous = pinned.value_at(evaluation.as_of - resolution_period(rsi_resolution))
        threshold = _parse_decimal(step, "threshold")
        direction = step.argument("direction")
        passed = previous <= threshold < current if direction == "UP" else previous >= threshold > current
        return _outcome(
            operation,
            passed,
            {
                "direction": direction,
                "previous": _plain(previous),
                "current": _plain(current),
                "threshold": _plain(threshold),
                "source": "PINNED_FEATURE_OUTPUT",
            },
        )
    if operation == "MACD_CROSS":
        fast, slow, signal = (
            int(step.argument("fastPeriod")),
            int(step.argument("slowPeriod")),
            int(step.argument("signalPeriod")),
        )
        closes = _series_values(evaluation, f"closes.{resolution}", slow + signal + 2, operation)
        macd = [a - b for a, b in zip(_ema(closes, fast), _ema(closes, slow), strict=True)]
        histogram = [a - b for a, b in zip(macd, _ema(macd, signal), strict=True)]
        previous, current = histogram[-2], histogram[-1]
        direction = step.argument("direction")
        passed = previous <= 0 < current if direction == "UP" else previous >= 0 > current
        return _outcome(
            operation,
            passed,
            {"direction": direction, "previousHistogram": _plain(previous), "histogram": _plain(current)},
        )
    if operation == "BOLLINGER_REVERSAL":
        period = int(step.argument("period"))
        deviations = _parse_decimal(step, "deviations")
        closes = _series_values(evaluation, f"closes.{resolution}", period + 1, operation)

        def band(offset: int) -> tuple[Decimal, Decimal]:
            end = len(closes) - offset
            window = closes[end - period : end]
            mean = _average(window)
            variance = _MATH.divide(
                sum((_MATH.power(value - mean, 2) for value in window), Decimal(0)), Decimal(period)
            )
            width = _MATH.multiply(_MATH.sqrt(variance), deviations)
            return mean - width, mean + width

        previous_band, current_band = band(1), band(0)
        previous, current = closes[-2], closes[-1]
        direction = step.argument("direction")
        passed = (
            previous <= previous_band[0] and current > current_band[0]
            if direction == "UP"
            else previous >= previous_band[1] and current < current_band[1]
        )
        return _outcome(
            operation, passed, {"direction": direction, "previous": _plain(previous), "current": _plain(current)}
        )
    if operation == "POSITION_RETURN":
        value = _decimal_value(evaluation, "position.returnPercent", operation)
        threshold = _parse_decimal(step, "thresholdPercent")
        direction = step.argument("direction")
        return _outcome(
            operation,
            value >= threshold if direction == "PROFIT" else value <= -threshold,
            {"direction": direction, "returnPercent": _plain(value), "thresholdPercent": _plain(threshold)},
        )
    if operation == "HOLDING_PERIOD":
        unit, amount = step.argument("unit"), int(step.argument("amount"))
        if unit == "SESSION_CLOSE":
            passed = evaluation.inputs.values.get("session.close", "false").lower() == "true"
        elif unit == "BAR":
            passed = _decimal_value(evaluation, f"position.holdingBars.{resolution}", operation) >= amount
        else:
            passed = _decimal_value(evaluation, "position.holdingTradingDays", operation) >= amount
        return _outcome(operation, passed, {"unit": unit, "amount": str(amount), "resolution": resolution or ""})
    if operation in {"PEAK_RETURN", "DRAWDOWN_FROM_PEAK"}:
        key = "position.peakReturnPercent" if operation == "PEAK_RETURN" else "position.drawdownPercent"
        return _compared(
            operation,
            step.argument("operator"),
            _decimal_value(evaluation, key, operation),
            _parse_decimal(step, "thresholdPercent"),
        )
    if operation == "SCHEDULE":
        cycle, interval = step.argument("cycle"), int(step.argument("interval"))
        values = evaluation.inputs.values
        new_day = values.get("schedule.newTradingDay", "false").lower() == "true"
        day_index = int(values.get("schedule.tradingDayIndex", "0"))
        passed = {
            "EVERY_TRADING_DAY": new_day,
            "WEEK_FIRST_TRADING_DAY": values.get("schedule.weekFirstTradingDay", "false").lower() == "true",
            "MONTH_FIRST_TRADING_DAY": values.get("schedule.monthFirstTradingDay", "false").lower() == "true",
            "MONTH_LAST_TRADING_DAY": values.get("schedule.monthLastTradingDay", "false").lower() == "true",
            "EVERY_N_TRADING_DAYS": new_day and interval > 0 and (day_index - 1) % interval == 0,
        }[cycle]
        return _outcome(operation, passed, {"cycle": cycle, "interval": str(interval)})
    raise ElementEvaluationError(f"unsupported catalog operation {operation}")


def _evaluate_terminal(step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome:
    del evaluation
    raise ElementEvaluationError(
        f"{step.operation} is a terminal element: the plan loader consumes it, it is never evaluated per instrument"
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
    minimum_arguments: Mapping[str, Decimal] = field(default_factory=lambda: MappingProxyType({}))
    exclusive_minimum_arguments: Mapping[str, Decimal] = field(default_factory=lambda: MappingProxyType({}))
    maximum_arguments: Mapping[str, Decimal] = field(default_factory=lambda: MappingProxyType({}))
    integer_arguments: tuple[str, ...] = ()


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
    canonical_feature_resolutions: Mapping[str, str]
    """Canonical feature UUID -> exact bar resolution pinned by that definition."""
    execution_gate: bool = False
    """Whether terminal executionMode/wait/maxExecutions semantics are declared."""

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
                f"{step.operation} step {step.sequence} is missing required argument(s): {', '.join(missing)}",
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
                    f"{step.operation} argument {name}={value!r} is not one of " + ", ".join(allowed),
                )
        decimal_values = {name: _parse_decimal(step, name) for name in spec.decimal_arguments}
        for name, minimum in spec.minimum_arguments.items():
            if decimal_values[name] < minimum:
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                    f"{step.operation} argument {name} must be at least {minimum}, got {step.arguments[name]!r}",
                )
        for name, minimum in spec.exclusive_minimum_arguments.items():
            if decimal_values[name] <= minimum:
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                    f"{step.operation} argument {name} must be greater than {minimum}, got {step.arguments[name]!r}",
                )
        for name, maximum in spec.maximum_arguments.items():
            if decimal_values[name] > maximum:
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                    f"{step.operation} argument {name} must be at most {maximum}, got {step.arguments[name]!r}",
                )
        for name in spec.integer_arguments:
            if decimal_values[name] != decimal_values[name].to_integral_value():
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                    f"{step.operation} argument {name} must be an integer, got {step.arguments[name]!r}",
                )
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
                f"requiredFeature.featureId {canonical_feature_id!r} is not in element catalog {self.version}",
            ) from exc

    def require_canonical_feature_resolution(self, canonical_feature_id: str) -> str | None:
        """Return the exact resolution pinned by a canonical feature UUID, when constrained."""
        return self.canonical_feature_resolutions.get(canonical_feature_id)

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
                enumerations=MappingProxyType({"operator": tuple(COMPARISON_OPERATORS)}),
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
            # Production definition seeded by the shared pipeline catalog.
            "0f1b0000-0000-4000-8000-000000000001": "RSI_14",
            # Published cross-runtime conformance fixture.
            "00000000-0000-4000-8000-000000000401": "RSI_14",
        }
    ),
    canonical_feature_resolutions=MappingProxyType(
        {
            "0f1b0000-0000-4000-8000-000000000001": "1m",
            "00000000-0000-4000-8000-000000000401": "1m",
        }
    ),
)


def _production_spec(
    operation: str,
    required: tuple[str, ...],
    *,
    enumerations: Mapping[str, tuple[str, ...]] | None = None,
    decimals: tuple[str, ...] = (),
) -> ElementSpec:
    return ElementSpec(
        operation=operation,
        required_arguments=required,
        enumerations=MappingProxyType(dict(enumerations or {})),
        decimal_arguments=decimals,
        feature_arguments=(),
        terminal=False,
        produces_value=False,
        consumes_value=False,
        evaluator=_evaluate_catalog_operation,
    )


_BASIC_ELEMENTS_2026_08_07 = ElementCatalog(
    version="basic-elements:2026-08-07",
    execution_gate=True,
    specs=MappingProxyType(
        {
            "PRICE_COMPARE": _production_spec(
                "PRICE_COMPARE",
                ("resolution", "operator", "reference"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "operator": tuple(COMPARISON_OPERATORS),
                    "reference": (
                        "PREVIOUS_CLOSE",
                        "SESSION_OPEN",
                        "AVERAGE_ENTRY_PRICE",
                        "SMA_5",
                        "SMA_20",
                        "SMA_60",
                        "HIGH_5",
                        "HIGH_20",
                        "HIGH_60",
                        "LOW_5",
                        "LOW_20",
                        "LOW_60",
                    ),
                },
            ),
            "PRICE_CHANGE_PERCENT": _production_spec(
                "PRICE_CHANGE_PERCENT",
                ("resolution", "base", "direction", "thresholdPercent"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "base": ("PREVIOUS_CLOSE", "SESSION_OPEN", "AVERAGE_ENTRY_PRICE"),
                    "direction": ("UP", "DOWN"),
                },
                decimals=("thresholdPercent",),
            ),
            "VOLUME_COMPARE": _production_spec(
                "VOLUME_COMPARE",
                ("resolution", "operator", "reference", "period", "multiplier"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "operator": tuple(COMPARISON_OPERATORS),
                    "reference": ("PREVIOUS_VOLUME", "AVERAGE_VOLUME"),
                    "period": ("1", "5", "20", "60"),
                    "multiplier": ("1", "2", "3"),
                },
                decimals=("multiplier",),
            ),
            "STREAK": _production_spec(
                "STREAK",
                ("resolution", "direction", "bars"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "direction": ("UP", "DOWN"),
                    "bars": ("2", "3", "5", "10", "20", "30"),
                },
            ),
            "SMA_CROSS": _production_spec(
                "SMA_CROSS",
                ("resolution", "direction", "shortPeriod", "longPeriod"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "direction": ("UP", "DOWN"),
                    "shortPeriod": ("5", "20", "60"),
                    "longPeriod": ("20", "60", "120"),
                },
            ),
            "RSI_CROSS": _production_spec(
                "RSI_CROSS",
                ("resolution", "direction", "period", "threshold"),
                enumerations={"resolution": _PRODUCTION_RESOLUTIONS, "direction": ("UP", "DOWN"), "period": ("14",)},
                decimals=("threshold",),
            ),
            "MACD_CROSS": _production_spec(
                "MACD_CROSS",
                ("resolution", "direction", "fastPeriod", "slowPeriod", "signalPeriod"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "direction": ("UP", "DOWN"),
                    "fastPeriod": ("12",),
                    "slowPeriod": ("26",),
                    "signalPeriod": ("9",),
                },
            ),
            "BOLLINGER_REVERSAL": _production_spec(
                "BOLLINGER_REVERSAL",
                ("resolution", "direction", "period", "deviations"),
                enumerations={
                    "resolution": _PRODUCTION_RESOLUTIONS,
                    "direction": ("UP", "DOWN"),
                    "period": ("20",),
                    "deviations": ("2",),
                },
                decimals=("deviations",),
            ),
            "POSITION_RETURN": _production_spec(
                "POSITION_RETURN",
                ("direction", "thresholdPercent"),
                enumerations={"direction": ("PROFIT", "LOSS")},
                decimals=("thresholdPercent",),
            ),
            "HOLDING_PERIOD": _production_spec(
                "HOLDING_PERIOD",
                ("unit", "amount", "resolution"),
                enumerations={
                    "unit": ("SESSION_CLOSE", "BAR", "TRADING_DAY"),
                    "amount": ("0", "1", "5", "20"),
                    "resolution": _PRODUCTION_RESOLUTIONS,
                },
            ),
            "PEAK_RETURN": _production_spec(
                "PEAK_RETURN",
                ("operator", "thresholdPercent"),
                enumerations={"operator": tuple(COMPARISON_OPERATORS)},
                decimals=("thresholdPercent",),
            ),
            "DRAWDOWN_FROM_PEAK": _production_spec(
                "DRAWDOWN_FROM_PEAK",
                ("operator", "thresholdPercent"),
                enumerations={"operator": tuple(COMPARISON_OPERATORS)},
                decimals=("thresholdPercent",),
            ),
            "SCHEDULE": _production_spec(
                "SCHEDULE",
                ("cycle", "interval", "resolution"),
                enumerations={
                    "cycle": (
                        "EVERY_TRADING_DAY",
                        "WEEK_FIRST_TRADING_DAY",
                        "MONTH_FIRST_TRADING_DAY",
                        "MONTH_LAST_TRADING_DAY",
                        "EVERY_N_TRADING_DAYS",
                    ),
                    "resolution": _PRODUCTION_RESOLUTIONS,
                },
                decimals=("interval",),
            ),
            "EMIT_ORDER_CANDIDATE": ElementSpec(
                operation="EMIT_ORDER_CANDIDATE",
                required_arguments=(
                    "allocation",
                    "orderType",
                    "timeInForce",
                    "side",
                    "orderPercent",
                    "executionMode",
                    "waitMode",
                    "waitInterval",
                    "maxExecutions",
                ),
                enumerations=MappingProxyType(
                    {
                        "allocation": _TERMINAL_ALLOCATIONS,
                        "orderType": _TERMINAL_ORDER_TYPES,
                        "timeInForce": ("DAY",),
                        "side": _TERMINAL_SIDES,
                        "executionMode": _EXECUTION_MODES,
                        "waitMode": _WAIT_MODES,
                    }
                ),
                decimal_arguments=("orderPercent", "waitInterval", "maxExecutions"),
                feature_arguments=(),
                terminal=True,
                produces_value=False,
                consumes_value=False,
                evaluator=_evaluate_terminal,
            ),
        }
    ),
    feature_versions=MappingProxyType({}),
    canonical_feature_ids=MappingProxyType({}),
    canonical_feature_resolutions=MappingProxyType({}),
)

# The active production catalog aligns the raw bar, RSI materialization, pinned feature input,
# and evaluation clock to the selected 30m/1h/4h/1d resolution. The legacy catalog remains
# immutable for historical replay only.
_BASIC_ELEMENTS_2026_08_08 = ElementCatalog(
    version="basic-elements:2026-08-08",
    execution_gate=True,
    specs=_BASIC_ELEMENTS_2026_08_07.specs,
    feature_versions=MappingProxyType({"RSI_14": FEATURE_REGISTRY["RSI_14"].definition_version}),
    canonical_feature_ids=MappingProxyType(
        {
            "4b1c6801-0259-5176-a857-0e5ea923d898": "RSI_14",
            "2e18c093-5d4e-5d9a-bd22-b7e5679f1a3e": "RSI_14",
            "1b2785bd-20f0-50a2-ae96-6a1f7bad74b9": "RSI_14",
            "eddfb2d4-8586-5260-8fc9-9c8125990270": "RSI_14",
        }
    ),
    canonical_feature_resolutions=MappingProxyType(
        {
            "4b1c6801-0259-5176-a857-0e5ea923d898": "30m",
            "2e18c093-5d4e-5d9a-bd22-b7e5679f1a3e": "1h",
            "1b2785bd-20f0-50a2-ae96-6a1f7bad74b9": "4h",
            "eddfb2d4-8586-5260-8fc9-9c8125990270": "1d",
        }
    ),
)


def _v2_specs() -> Mapping[str, ElementSpec]:
    specs = dict(_BASIC_ELEMENTS_2026_08_08.specs)
    zero = Decimal(0)
    hundred = Decimal(100)
    specs["PRICE_CHANGE_PERCENT"] = replace(
        specs["PRICE_CHANGE_PERCENT"],
        minimum_arguments=MappingProxyType({"thresholdPercent": zero}),
    )
    specs["RSI_CROSS"] = replace(
        specs["RSI_CROSS"],
        minimum_arguments=MappingProxyType({"threshold": zero}),
        maximum_arguments=MappingProxyType({"threshold": hundred}),
    )
    specs["POSITION_RETURN"] = replace(
        specs["POSITION_RETURN"],
        minimum_arguments=MappingProxyType({"thresholdPercent": zero}),
        maximum_arguments=MappingProxyType({"thresholdPercent": hundred}),
    )
    specs["HOLDING_PERIOD"] = _production_spec(
        "HOLDING_PERIOD",
        ("unit", "amount", "resolution"),
        enumerations={
            "unit": ("SESSION_CLOSE", "BAR", "TRADING_DAY"),
            "resolution": _PRODUCTION_RESOLUTIONS,
        },
        decimals=("amount",),
    )
    specs["HOLDING_PERIOD"] = replace(
        specs["HOLDING_PERIOD"],
        minimum_arguments=MappingProxyType({"amount": zero}),
        integer_arguments=("amount",),
    )
    for operation in ("PEAK_RETURN", "DRAWDOWN_FROM_PEAK"):
        specs[operation] = replace(
            specs[operation],
            minimum_arguments=MappingProxyType({"thresholdPercent": zero}),
            maximum_arguments=MappingProxyType({"thresholdPercent": hundred}),
        )
    specs["SCHEDULE"] = replace(
        specs["SCHEDULE"],
        exclusive_minimum_arguments=MappingProxyType({"interval": zero}),
        integer_arguments=("interval",),
    )
    specs["EMIT_ORDER_CANDIDATE"] = ElementSpec(
        operation="EMIT_ORDER_CANDIDATE",
        required_arguments=(
            "allocation",
            "orderType",
            "timeInForce",
            "side",
            "orderPercent",
            "maxPositionPercent",
            "executionMode",
            "waitMode",
            "waitInterval",
            "maxExecutions",
        ),
        enumerations=MappingProxyType(
            {
                "allocation": _TERMINAL_ALLOCATIONS,
                "orderType": _TERMINAL_ORDER_TYPES,
                "timeInForce": ("DAY",),
                "side": _TERMINAL_SIDES,
                "executionMode": _EXECUTION_MODES,
                "waitMode": _WAIT_MODES,
            }
        ),
        decimal_arguments=(
            "orderPercent",
            "maxPositionPercent",
            "waitInterval",
            "maxExecutions",
        ),
        feature_arguments=(),
        terminal=True,
        produces_value=False,
        consumes_value=False,
        evaluator=_evaluate_terminal,
        exclusive_minimum_arguments=MappingProxyType(
            {
                "orderPercent": zero,
                "maxPositionPercent": zero,
                "waitInterval": zero,
                "maxExecutions": zero,
            }
        ),
        maximum_arguments=MappingProxyType(
            {
                "orderPercent": hundred,
                "maxPositionPercent": hundred,
            }
        ),
        integer_arguments=("waitInterval", "maxExecutions"),
    )
    return MappingProxyType(specs)


_BASIC_ELEMENTS_2026_08_25 = ElementCatalog(
    version="basic-elements:2026-08-25",
    execution_gate=True,
    specs=_v2_specs(),
    feature_versions=_BASIC_ELEMENTS_2026_08_08.feature_versions,
    canonical_feature_ids=MappingProxyType(
        {
            "ec37984b-6605-5560-8ea0-774c5b8e9626": "RSI_14",
            "85f4f80f-be4e-d9dc-bd52-d4781ba5f30f": "RSI_14",
            "65a5aaf5-f536-820f-119a-239b0aec0de7": "RSI_14",
            "647a5fd6-98ed-0617-d4b2-844748d54fac": "RSI_14",
        }
    ),
    canonical_feature_resolutions=MappingProxyType(
        {
            "ec37984b-6605-5560-8ea0-774c5b8e9626": "30m",
            "85f4f80f-be4e-d9dc-bd52-d4781ba5f30f": "1h",
            "65a5aaf5-f536-820f-119a-239b0aec0de7": "4h",
            "647a5fd6-98ed-0617-d4b2-844748d54fac": "1d",
        }
    ),
)


ELEMENT_CATALOGS: Mapping[str, ElementCatalog] = MappingProxyType(
    {
        _BASIC_ELEMENTS_2026_07_31.version: _BASIC_ELEMENTS_2026_07_31,
        _BASIC_ELEMENTS_2026_08_07.version: _BASIC_ELEMENTS_2026_08_07,
        _BASIC_ELEMENTS_2026_08_08.version: _BASIC_ELEMENTS_2026_08_08,
        _BASIC_ELEMENTS_2026_08_25.version: _BASIC_ELEMENTS_2026_08_25,
    }
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
    return {definition.feature_id: definition.definition_version for definition in FEATURE_REGISTRY.values()}
