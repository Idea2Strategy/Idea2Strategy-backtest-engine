from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from backtest_engine.elements import (
    ElementEvaluation,
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
        catalog.validate_step(
            PlanStep(sequence=1, operation=case["operation"], arguments=arguments)
        )


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
        features = (PinnedFeatureSeries(
            feature_id="RSI_14",
            instrument_id=INSTRUMENT,
            resolution="1h",
            values=(
                PinnedFeatureValue(AS_OF - period * 2, Decimal("29.00000000") if passed else Decimal("31.00000000")),
                PinnedFeatureValue(AS_OF - period, Decimal("31.00000000") if passed else Decimal("32.00000000")),
            ),
        ),)
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
