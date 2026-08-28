from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from backtest_engine.elements import (
    ElementCompatibilityError,
    ElementEvaluation,
    ElementEvaluationError,
    ElementInputMissing,
    InstrumentInput,
    PinnedFeatureSeries,
    PinnedFeatureValue,
    PlanStep,
    element_catalog,
)


FIXTURE = Path(__file__).parent / "fixtures/contracts/basic-element-conformance.v1.json"
INSTRUMENT = "00000000-0000-4000-8000-000000000301"
AS_OF = datetime(2026, 8, 26, 20, 0, tzinfo=timezone.utc)
TERMINAL_ARGUMENTS = {
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
}


def test_v2_catalog_accepts_every_compiled_operation_and_argument_from_the_corpus() -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    catalog = element_catalog(document["catalogVersion"])

    assert len(document["cases"]) == 14
    assert {case["operation"] for case in document["cases"]} == set(catalog.operations)

    for case in document["cases"]:
        arguments = dict(case["arguments"])
        if case["operation"] == "EMIT_ORDER_CANDIDATE":
            arguments.update(
                allocation="EQUAL",
                orderType="MARKET",
                timeInForce="DAY",
                side="BUY",
            )
        catalog.validate_step(PlanStep(sequence=1, operation=case["operation"], arguments=arguments))


def _values(operation: str, passed: bool) -> dict[str, str]:
    common = {"bar.closed.30m": "true", "bar.closed.1h": "true", "bar.closed.4h": "true", "bar.closed.1d": "true"}
    scenarios = {
        "PRICE_COMPARE": {"closes.30m": "100,101" if passed else "100,99"},
        "PRICE_CHANGE_PERCENT": {"closes.1h": "100,103.5" if passed else "100,102"},
        "VOLUME_COMPARE": {"volumes.4h": ",".join(["1000"] * 20 + (["2000"] if passed else ["1500"]))},
        "STREAK": {"closes.1d": "100,101,102,103" if passed else "100,101,100,103"},
        "SMA_CROSS": {"closes.30m": ",".join((["100"] * 16 + ["90"] * 4 + ["200"]) if passed else ["100"] * 21)},
        "MACD_CROSS": {"closes.4h": ",".join((["100"] * 37 + ["110"]) if passed else ["100"] * 38)},
        "BOLLINGER_REVERSAL": {"closes.1d": ",".join((["100"] * 19 + ["80", "100"]) if passed else ["100"] * 21)},
        "POSITION_RETURN": {"position.returnPercent": "-5.1" if passed else "-4.9"},
        "HOLDING_PERIOD": {"position.holdingTradingDays": "10" if passed else "9"},
        "PEAK_RETURN": {"position.peakReturnPercent": "15.1" if passed else "14.9"},
        "DRAWDOWN_FROM_PEAK": {"position.drawdownPercent": "7.1" if passed else "0.9"},
        "SCHEDULE": {
            "schedule.newTradingDay": "true",
            "schedule.tradingDayIndex": "1" if passed else "2",
            "schedule.weekFirstTradingDay": "false",
            "schedule.monthFirstTradingDay": "false",
            "schedule.monthLastTradingDay": "false",
        },
    }
    return {**common, **scenarios.get(operation, {})}


def _evaluation(operation: str, passed: bool) -> ElementEvaluation:
    features = ()
    if operation == "RSI_CROSS":
        period = timedelta(hours=1)
        features = (
            PinnedFeatureSeries(
                feature_id="RSI_14",
                instrument_id=INSTRUMENT,
                resolution="1h",
                values=(
                    PinnedFeatureValue(
                        AS_OF - period * 2, Decimal("29.00000000") if passed else Decimal("31.00000000")
                    ),
                    PinnedFeatureValue(AS_OF - period, Decimal("31.00000000") if passed else Decimal("32.00000000")),
                ),
            ),
        )
    return ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=AS_OF,
        inputs=InstrumentInput(
            instrument_id=INSTRUMENT,
            series=(),
            feature_series=features,
            require_pinned_features=bool(features),
            values=_values(operation, passed),
        ),
    )


@pytest.mark.parametrize("passed", [True, False])
def test_every_condition_has_real_true_and_false_runtime_outcomes(passed: bool) -> None:
    document = json.loads(FIXTURE.read_text(encoding="utf-8"))
    catalog = element_catalog(document["catalogVersion"])

    for case in document["cases"]:
        if case["operation"] == "EMIT_ORDER_CANDIDATE":
            continue
        step = PlanStep(sequence=1, operation=case["operation"], arguments=case["arguments"])
        outcome = catalog.evaluate(step, _evaluation(case["operation"], passed))
        assert outcome.is_passed is passed, case["operation"]


def test_missing_history_is_unavailable_and_an_open_bar_waits_instead_of_becoming_a_signal() -> None:
    catalog = element_catalog("basic-elements:2026-08-25")
    price = PlanStep(
        sequence=1,
        operation="PRICE_COMPARE",
        arguments={"resolution": "30m", "operator": "GT", "reference": "PREVIOUS_CLOSE"},
    )
    missing = ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=AS_OF,
        inputs=InstrumentInput(instrument_id=INSTRUMENT, series=(), values={"bar.closed.30m": "true"}),
    )
    with pytest.raises(ElementInputMissing):
        catalog.evaluate(price, missing)

    open_bar = ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=AS_OF,
        inputs=InstrumentInput(
            instrument_id=INSTRUMENT,
            series=(),
            values={"bar.closed.30m": "false", "closes.30m": "100,101"},
        ),
    )
    outcome = catalog.evaluate(price, open_bar)
    assert outcome.is_passed is False
    assert outcome.reason_code == "WAITING_FOR_BAR_CLOSE"


@pytest.mark.parametrize(
    ("operation", "arguments", "invalid_name", "invalid_value"),
    [
        (
            "PRICE_CHANGE_PERCENT",
            {"resolution": "30m", "base": "PREVIOUS_CLOSE", "direction": "UP", "thresholdPercent": "1"},
            "thresholdPercent",
            "-0.00000001",
        ),
        (
            "RSI_CROSS",
            {"resolution": "30m", "direction": "UP", "period": "14", "threshold": "50"},
            "threshold",
            "-0.00000001",
        ),
        (
            "RSI_CROSS",
            {"resolution": "30m", "direction": "UP", "period": "14", "threshold": "50"},
            "threshold",
            "100.00000001",
        ),
        ("POSITION_RETURN", {"direction": "PROFIT", "thresholdPercent": "5"}, "thresholdPercent", "100.00000001"),
        ("HOLDING_PERIOD", {"unit": "BAR", "amount": "1", "resolution": "30m"}, "amount", "-1"),
        ("HOLDING_PERIOD", {"unit": "BAR", "amount": "1", "resolution": "30m"}, "amount", "1.5"),
        ("PEAK_RETURN", {"operator": "GTE", "thresholdPercent": "5"}, "thresholdPercent", "-0.00000001"),
        ("DRAWDOWN_FROM_PEAK", {"operator": "GTE", "thresholdPercent": "5"}, "thresholdPercent", "100.00000001"),
        ("SCHEDULE", {"cycle": "EVERY_N_TRADING_DAYS", "interval": "1", "resolution": "1d"}, "interval", "0"),
        ("SCHEDULE", {"cycle": "EVERY_N_TRADING_DAYS", "interval": "1", "resolution": "1d"}, "interval", "1.5"),
        ("EMIT_ORDER_CANDIDATE", TERMINAL_ARGUMENTS.copy(), "orderPercent", "0"),
        ("EMIT_ORDER_CANDIDATE", TERMINAL_ARGUMENTS.copy(), "maxPositionPercent", "100.00000001"),
        ("EMIT_ORDER_CANDIDATE", TERMINAL_ARGUMENTS.copy(), "waitInterval", "0"),
        ("EMIT_ORDER_CANDIDATE", TERMINAL_ARGUMENTS.copy(), "maxExecutions", "1.5"),
    ],
)
def test_v2_plan_loader_rejects_every_numeric_value_immediately_outside_the_published_boundary(
    operation: str,
    arguments: dict[str, str],
    invalid_name: str,
    invalid_value: str,
) -> None:
    arguments[invalid_name] = invalid_value

    with pytest.raises(ElementCompatibilityError, match=invalid_name):
        element_catalog("basic-elements:2026-08-25").validate_step(
            PlanStep(sequence=1, operation=operation, arguments=arguments)
        )


@pytest.mark.parametrize("malformed", ["not-a-number", "NaN", "Infinity", "-Infinity"])
def test_malformed_or_non_finite_market_values_fail_with_a_typed_runtime_error(malformed: str) -> None:
    step = PlanStep(
        sequence=1,
        operation="PRICE_COMPARE",
        arguments={"resolution": "30m", "operator": "GT", "reference": "PREVIOUS_CLOSE"},
    )
    evaluation = ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=AS_OF,
        inputs=InstrumentInput(
            instrument_id=INSTRUMENT,
            series=(),
            values={"bar.closed.30m": "true", "closes.30m": f"100,{malformed}"},
        ),
    )

    with pytest.raises(ElementEvaluationError, match="PRICE_COMPARE.*closes.30m"):
        element_catalog("basic-elements:2026-08-25").evaluate(step, evaluation)


def _evaluate_values(operation: str, arguments: dict[str, str], values: dict[str, str]) -> ElementEvaluation:
    resolution = arguments.get("resolution")
    runtime_values = dict(values)
    if resolution and operation not in {"HOLDING_PERIOD", "SCHEDULE"}:
        runtime_values[f"bar.closed.{resolution}"] = "true"
    return ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=AS_OF,
        inputs=InstrumentInput(instrument_id=INSTRUMENT, series=(), values=runtime_values),
    )


@pytest.mark.parametrize(
    ("operator", "left", "right", "expected"),
    [
        ("LT", "99", "100", True),
        ("LT", "100", "100", False),
        ("LTE", "100", "100", True),
        ("LTE", "101", "100", False),
        ("GT", "101", "100", True),
        ("GT", "100", "100", False),
        ("GTE", "100", "100", True),
        ("GTE", "99", "100", False),
        ("EQ", "100.00000000", "100", True),
        ("EQ", "99.99999999", "100", False),
        ("NEQ", "99.99999999", "100", True),
        ("NEQ", "100", "100.00000000", False),
    ],
)
def test_price_comparison_exercises_all_operators_at_and_immediately_around_equality(
    operator: str, left: str, right: str, expected: bool
) -> None:
    step = PlanStep(
        sequence=1,
        operation="PRICE_COMPARE",
        arguments={"resolution": "30m", "operator": operator, "reference": "PREVIOUS_CLOSE"},
    )

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        step, _evaluate_values(step.operation, dict(step.arguments), {"closes.30m": f"{right},{left}"})
    )

    assert outcome.is_passed is expected


@pytest.mark.parametrize("resolution", ["30m", "1h", "4h", "1d"])
def test_each_published_resolution_reads_only_its_own_series(resolution: str) -> None:
    step = PlanStep(
        sequence=1,
        operation="PRICE_COMPARE",
        arguments={"resolution": resolution, "operator": "GT", "reference": "PREVIOUS_CLOSE"},
    )

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        step,
        _evaluate_values(step.operation, dict(step.arguments), {f"closes.{resolution}": "100,101"}),
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize(
    ("reference", "operator", "closes", "extra"),
    [
        ("PREVIOUS_CLOSE", "GT", "100,101", {}),
        ("SESSION_OPEN", "GT", "101", {"session.open": "100"}),
        ("AVERAGE_ENTRY_PRICE", "GT", "101", {"position.averageEntryPrice": "100"}),
        ("SMA_5", "GT", ",".join(["100"] * 4 + ["110"]), {}),
        ("SMA_20", "GT", ",".join(["100"] * 19 + ["110"]), {}),
        ("SMA_60", "GT", ",".join(["100"] * 59 + ["110"]), {}),
        ("HIGH_5", "GT", ",".join(["100"] * 5 + ["110"]), {}),
        ("HIGH_20", "GT", ",".join(["100"] * 20 + ["110"]), {}),
        ("HIGH_60", "GT", ",".join(["100"] * 60 + ["110"]), {}),
        ("LOW_5", "LT", ",".join(["100"] * 5 + ["90"]), {}),
        ("LOW_20", "LT", ",".join(["100"] * 20 + ["90"]), {}),
        ("LOW_60", "LT", ",".join(["100"] * 60 + ["90"]), {}),
    ],
)
def test_every_price_reference_uses_its_declared_source(
    reference: str, operator: str, closes: str, extra: dict[str, str]
) -> None:
    step = PlanStep(
        sequence=1,
        operation="PRICE_COMPARE",
        arguments={"resolution": "30m", "operator": operator, "reference": reference},
    )

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        step,
        _evaluate_values(step.operation, dict(step.arguments), {"closes.30m": closes, **extra}),
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize(
    ("operation", "direction", "arguments", "values", "features"),
    [
        (
            "PRICE_CHANGE_PERCENT",
            "UP",
            {"resolution": "30m", "base": "PREVIOUS_CLOSE", "thresholdPercent": "5"},
            {"closes.30m": "100,105"},
            (),
        ),
        (
            "PRICE_CHANGE_PERCENT",
            "DOWN",
            {"resolution": "30m", "base": "PREVIOUS_CLOSE", "thresholdPercent": "5"},
            {"closes.30m": "100,95"},
            (),
        ),
        ("STREAK", "UP", {"resolution": "1d", "bars": "3"}, {"closes.1d": "100,101,102,103"}, ()),
        ("STREAK", "DOWN", {"resolution": "1d", "bars": "3"}, {"closes.1d": "103,102,101,100"}, ()),
        (
            "SMA_CROSS",
            "UP",
            {"resolution": "30m", "shortPeriod": "5", "longPeriod": "20"},
            {"closes.30m": ",".join(["100"] * 16 + ["90"] * 4 + ["200"])},
            (),
        ),
        (
            "SMA_CROSS",
            "DOWN",
            {"resolution": "30m", "shortPeriod": "5", "longPeriod": "20"},
            {"closes.30m": ",".join(["100"] * 16 + ["110"] * 4 + ["0"])},
            (),
        ),
        (
            "MACD_CROSS",
            "UP",
            {"resolution": "4h", "fastPeriod": "12", "slowPeriod": "26", "signalPeriod": "9"},
            {"closes.4h": ",".join(["100"] * 37 + ["110"])},
            (),
        ),
        (
            "MACD_CROSS",
            "DOWN",
            {"resolution": "4h", "fastPeriod": "12", "slowPeriod": "26", "signalPeriod": "9"},
            {"closes.4h": ",".join(["100"] * 37 + ["90"])},
            (),
        ),
        (
            "BOLLINGER_REVERSAL",
            "UP",
            {"resolution": "1d", "period": "20", "deviations": "2"},
            {"closes.1d": ",".join(["100"] * 19 + ["80", "100"])},
            (),
        ),
        (
            "BOLLINGER_REVERSAL",
            "DOWN",
            {"resolution": "1d", "period": "20", "deviations": "2"},
            {"closes.1d": ",".join(["100"] * 19 + ["120", "100"])},
            (),
        ),
        (
            "RSI_CROSS",
            "UP",
            {"resolution": "1h", "period": "14", "threshold": "30"},
            {},
            ("29", "31"),
        ),
        (
            "RSI_CROSS",
            "DOWN",
            {"resolution": "1h", "period": "14", "threshold": "70"},
            {},
            ("71", "69"),
        ),
    ],
)
def test_every_directional_condition_triggers_on_the_exact_declared_transition(
    operation: str,
    direction: str,
    arguments: dict[str, str],
    values: dict[str, str],
    features: tuple[str, ...],
) -> None:
    arguments = {**arguments, "direction": direction}
    evaluation = _evaluate_values(operation, arguments, values)
    if features:
        period = timedelta(hours=1)
        evaluation = ElementEvaluation(
            instrument_id=INSTRUMENT,
            as_of=AS_OF,
            inputs=InstrumentInput(
                instrument_id=INSTRUMENT,
                series=(),
                feature_series=(
                    PinnedFeatureSeries(
                        feature_id="RSI_14",
                        instrument_id=INSTRUMENT,
                        resolution="1h",
                        values=(
                            PinnedFeatureValue(AS_OF - period * 2, Decimal(f"{features[0]}.00000000")),
                            PinnedFeatureValue(AS_OF - period, Decimal(f"{features[1]}.00000000")),
                        ),
                    ),
                ),
                require_pinned_features=True,
                values={"bar.closed.1h": "true"},
            ),
        )

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation=operation, arguments=arguments), evaluation
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize(
    ("cycle", "values"),
    [
        ("EVERY_TRADING_DAY", {"schedule.newTradingDay": "true"}),
        ("WEEK_FIRST_TRADING_DAY", {"schedule.weekFirstTradingDay": "true"}),
        ("MONTH_FIRST_TRADING_DAY", {"schedule.monthFirstTradingDay": "true"}),
        ("MONTH_LAST_TRADING_DAY", {"schedule.monthLastTradingDay": "true"}),
        ("EVERY_N_TRADING_DAYS", {"schedule.newTradingDay": "true", "schedule.tradingDayIndex": "6"}),
    ],
)
def test_every_schedule_cycle_has_a_real_true_runtime_path(cycle: str, values: dict[str, str]) -> None:
    arguments = {"cycle": cycle, "interval": "5", "resolution": "1d"}
    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation="SCHEDULE", arguments=arguments),
        _evaluate_values("SCHEDULE", arguments, values),
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize(
    ("unit", "amount", "resolution", "values"),
    [
        ("SESSION_CLOSE", "0", "1d", {"session.close": "true"}),
        ("BAR", "5", "4h", {"position.holdingBars.4h": "5"}),
        ("TRADING_DAY", "5", "1d", {"position.holdingTradingDays": "5"}),
    ],
)
def test_every_holding_period_unit_passes_at_its_exact_boundary(
    unit: str, amount: str, resolution: str, values: dict[str, str]
) -> None:
    arguments = {"unit": unit, "amount": amount, "resolution": resolution}
    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation="HOLDING_PERIOD", arguments=arguments),
        _evaluate_values("HOLDING_PERIOD", arguments, values),
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize("period", ["1", "5", "20", "60"])
@pytest.mark.parametrize("multiplier", ["1", "2", "3"])
@pytest.mark.parametrize("reference", ["PREVIOUS_VOLUME", "AVERAGE_VOLUME"])
def test_every_volume_reference_period_and_multiplier_combination_uses_the_declared_window(
    period: str, multiplier: str, reference: str
) -> None:
    count = int(period)
    previous = ["100"] * max(count, 1)
    current = str(100 * int(multiplier))
    arguments = {
        "resolution": "4h",
        "operator": "GTE",
        "reference": reference,
        "period": period,
        "multiplier": multiplier,
    }

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation="VOLUME_COMPARE", arguments=arguments),
        _evaluate_values(
            "VOLUME_COMPARE",
            arguments,
            {"volumes.4h": ",".join([*previous, current])},
        ),
    )

    assert outcome.is_passed is True


@pytest.mark.parametrize(
    ("operation", "operator", "value", "threshold", "expected"),
    [
        ("PEAK_RETURN", "LT", "9.99999999", "10", True),
        ("PEAK_RETURN", "LTE", "10", "10", True),
        ("PEAK_RETURN", "GT", "10.00000001", "10", True),
        ("PEAK_RETURN", "GTE", "10", "10", True),
        ("PEAK_RETURN", "EQ", "10.00000000", "10", True),
        ("PEAK_RETURN", "NEQ", "9.99999999", "10", True),
        ("DRAWDOWN_FROM_PEAK", "LT", "9.99999999", "10", True),
        ("DRAWDOWN_FROM_PEAK", "LTE", "10", "10", True),
        ("DRAWDOWN_FROM_PEAK", "GT", "10.00000001", "10", True),
        ("DRAWDOWN_FROM_PEAK", "GTE", "10", "10", True),
        ("DRAWDOWN_FROM_PEAK", "EQ", "10.00000000", "10", True),
        ("DRAWDOWN_FROM_PEAK", "NEQ", "9.99999999", "10", True),
    ],
)
def test_position_threshold_conditions_cover_all_operators_at_decimal_precision(
    operation: str, operator: str, value: str, threshold: str, expected: bool
) -> None:
    key = "position.peakReturnPercent" if operation == "PEAK_RETURN" else "position.drawdownPercent"
    arguments = {"operator": operator, "thresholdPercent": threshold}

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation=operation, arguments=arguments),
        _evaluate_values(operation, arguments, {key: value}),
    )

    assert outcome.is_passed is expected


@pytest.mark.parametrize(
    ("direction", "value", "threshold", "expected"),
    [
        ("PROFIT", "5", "5", True),
        ("PROFIT", "4.99999999", "5", False),
        ("LOSS", "-5", "5", True),
        ("LOSS", "-4.99999999", "5", False),
    ],
)
def test_position_return_honors_profit_and_loss_boundaries(
    direction: str, value: str, threshold: str, expected: bool
) -> None:
    arguments = {"direction": direction, "thresholdPercent": threshold}

    outcome = element_catalog("basic-elements:2026-08-25").evaluate(
        PlanStep(sequence=1, operation="POSITION_RETURN", arguments=arguments),
        _evaluate_values("POSITION_RETURN", arguments, {"position.returnPercent": value}),
    )

    assert outcome.is_passed is expected
