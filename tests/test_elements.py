"""Step evaluators and the versioned element/feature catalog (card D21/D22).

RSI expectations are computed by hand from the pinned formula in the module
docstring of :mod:`backtest_engine.elements.features`; nothing here calls the
production routine to obtain its own oracle.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from backtest_engine.elements import (
    ELEMENT_CATALOG_VERSIONS,
    FEATURE_CATALOG_VERSION,
    RSI_14_DEFINITION,
    ElementCompatibilityError,
    ElementEvaluation,
    ElementEvaluationError,
    ElementInputMissing,
    InstrumentInput,
    InstrumentSeries,
    PinnedFeatureSeries,
    PinnedFeatureValue,
    PlanLoadFailure,
    PlanStep,
    SeriesBar,
    element_catalog,
    resolution_period,
    supported_feature_versions,
)


CATALOG_VERSION = "basic-elements:2026-07-31"
INSTRUMENT = "00000000-0000-4000-8000-000000000301"
ORIGIN = datetime.fromisoformat("2025-11-28T14:30:00+00:00")


def _bars(closes: list[str], *, resolution: str = "1m") -> tuple[SeriesBar, ...]:
    period = resolution_period(resolution)
    return tuple(
        SeriesBar(
            instrument_id=INSTRUMENT,
            resolution=resolution,
            starts_at=ORIGIN + period * index,
            ends_at=ORIGIN + period * (index + 1),
            close=Decimal(value),
            volume=Decimal(1000),
        )
        for index, value in enumerate(closes)
    )


def _evaluation(
    closes: list[str],
    *,
    as_of: datetime | None = None,
    data_kind: str = "ADJUSTED_BAR",
    resolution: str = "1m",
) -> ElementEvaluation:
    bars = _bars(closes, resolution=resolution)
    series = InstrumentSeries(
        instrument_id=INSTRUMENT,
        data_kind=data_kind,
        resolution=resolution,
        bars=bars,
    )
    return ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=as_of if as_of is not None else bars[-1].ends_at,
        inputs=InstrumentInput(instrument_id=INSTRUMENT, series=(series,)),
    )


def _step(sequence: int, operation: str, **arguments: str) -> PlanStep:
    return PlanStep(sequence=sequence, operation=operation, arguments=arguments)


LOAD_RSI = _step(1, "LOAD_FEATURE", feature="RSI_14", resolution="1m")


# ---------------------------------------------------------------------------
# Feature definition
# ---------------------------------------------------------------------------


def test_rsi_14_definition_is_pinned_and_declared() -> None:
    assert RSI_14_DEFINITION.feature_id == "RSI_14"
    assert RSI_14_DEFINITION.definition_version == "rsi:1.0.0"
    assert RSI_14_DEFINITION.method == "SIMPLE_AVERAGE_BOUNDED_WINDOW"
    assert RSI_14_DEFINITION.periods == 14
    assert RSI_14_DEFINITION.required_bars == 15
    assert RSI_14_DEFINITION.data_kind == "ADJUSTED_BAR"
    assert RSI_14_DEFINITION.value_scale == 8
    assert FEATURE_CATALOG_VERSION == "features:1.0.0"


@pytest.mark.parametrize(
    ("closes", "expected"),
    [
        # 14 changes of +1: sum(gain)=14, sum(loss)=0 -> RSI = 100.
        ([str(100 + step) for step in range(15)], "100.00000000"),
        # 14 changes of -1: sum(gain)=0, sum(loss)=14 -> RS = 0 -> RSI = 0.
        ([str(114 - step) for step in range(15)], "0.00000000"),
        # No movement at all: both averages 0 -> pinned undefined value 50.
        (["100"] * 15, "50.00000000"),
        # 7 x (+2) and 7 x (-1): avgGain=1, avgLoss=0.5, RS=2,
        # RSI = 100 - 100/3 = 66.666... -> 66.66666667 at 8dp HALF_EVEN.
        (
            [
                "100",
                "102",
                "101",
                "103",
                "102",
                "104",
                "103",
                "105",
                "104",
                "106",
                "105",
                "107",
                "106",
                "108",
                "107",
            ],
            "66.66666667",
        ),
        # +1 then -2 then twelve flat changes: avgGain=1/14, avgLoss=2/14,
        # RS=0.5, RSI = 100 - 100/1.5 = 33.333... -> 33.33333333 at 8dp.
        (["100", "101"] + ["99"] * 13, "33.33333333"),
    ],
)
def test_rsi_14_matches_the_pinned_formula(closes: list[str], expected: str) -> None:
    outcome = element_catalog(CATALOG_VERSION).evaluate(LOAD_RSI, _evaluation(closes))

    assert outcome.is_passed is True
    assert outcome.reason_code == "FEATURE_LOADED"
    assert outcome.evidence["value"] == expected
    assert outcome.evidence["feature"] == "RSI_14"
    assert outcome.evidence["resolution"] == "1m"


def test_rsi_uses_only_bars_completed_at_or_before_the_evaluation_instant() -> None:
    # A 16th bar that closes after `as_of` must not shift the answer.
    closes = [
        "100",
        "102",
        "101",
        "103",
        "102",
        "104",
        "103",
        "105",
        "104",
        "106",
        "105",
        "107",
        "106",
        "108",
        "107",
    ]
    bounded = _evaluation(closes)
    with_future_bar = _evaluation([*closes, "999"], as_of=bounded.as_of)
    catalog = element_catalog(CATALOG_VERSION)

    assert catalog.evaluate(LOAD_RSI, bounded).evidence["value"] == "66.66666667"
    assert catalog.evaluate(LOAD_RSI, with_future_bar).evidence["value"] == "66.66666667"


def test_rsi_window_is_bounded_so_older_history_cannot_change_the_value() -> None:
    # A bounded window is what lets C's realtime runtime agree with a replay
    # that started at a different instant: only the last 15 bars matter.
    closes = [
        "100",
        "102",
        "101",
        "103",
        "102",
        "104",
        "103",
        "105",
        "104",
        "106",
        "105",
        "107",
        "106",
        "108",
        "107",
    ]
    catalog = element_catalog(CATALOG_VERSION)
    long_history = _evaluation(["7", "913", "41", "268", *closes])

    assert catalog.evaluate(LOAD_RSI, long_history).evidence["value"] == "66.66666667"


def test_evidence_records_the_exact_window_boundary() -> None:
    closes = [str(100 + step) for step in range(15)]
    evaluation = _evaluation(closes)

    outcome = element_catalog(CATALOG_VERSION).evaluate(LOAD_RSI, evaluation)

    assert outcome.evidence["windowFrom"] == "2025-11-28T14:30:00+00:00"
    assert outcome.evidence["windowThrough"] == "2025-11-28T14:45:00+00:00"
    assert outcome.evidence["asOf"] == "2025-11-28T14:45:00+00:00"


# ---------------------------------------------------------------------------
# Typed input failures
# ---------------------------------------------------------------------------


def test_short_history_is_a_typed_input_miss_not_a_false_condition() -> None:
    evaluation = _evaluation([str(100 + step) for step in range(14)])

    with pytest.raises(ElementInputMissing) as failure:
        element_catalog(CATALOG_VERSION).evaluate(LOAD_RSI, evaluation)

    assert failure.value.reason_code == "INSTRUMENT_INPUT_MISSING"
    assert failure.value.input_reason == "FEATURE_WARMUP_INCOMPLETE"
    assert failure.value.evidence["requiredBars"] == "15"
    assert failure.value.evidence["availableBars"] == "14"


def test_absent_series_is_a_typed_input_miss_naming_the_series() -> None:
    evaluation = _evaluation([str(100 + step) for step in range(15)], resolution="5m")

    with pytest.raises(ElementInputMissing) as failure:
        element_catalog(CATALOG_VERSION).evaluate(LOAD_RSI, evaluation)

    assert failure.value.reason_code == "INSTRUMENT_INPUT_MISSING"
    assert failure.value.input_reason == "FEATURE_SERIES_MISSING"
    assert failure.value.evidence["dataKind"] == "ADJUSTED_BAR"
    assert failure.value.evidence["resolution"] == "1m"


def test_no_bar_completed_yet_is_an_input_miss_rather_than_an_empty_success() -> None:
    evaluation = _evaluation(
        [str(100 + step) for step in range(15)],
        as_of=ORIGIN,
    )

    with pytest.raises(ElementInputMissing) as failure:
        element_catalog(CATALOG_VERSION).evaluate(LOAD_RSI, evaluation)

    assert failure.value.input_reason == "FEATURE_WARMUP_INCOMPLETE"
    assert failure.value.evidence["availableBars"] == "0"


# ---------------------------------------------------------------------------
# COMPARE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("operator", "threshold", "expected_pass", "expected_reason"),
    [
        ("LT", "30", True, "COMPARE_TRUE"),
        ("LT", "20", False, "COMPARE_FALSE"),
        ("LTE", "20", True, "COMPARE_TRUE"),
        ("GT", "20", False, "COMPARE_FALSE"),
        ("GTE", "20", True, "COMPARE_TRUE"),
        ("EQ", "20.00000000", True, "COMPARE_TRUE"),
        ("NEQ", "20", False, "COMPARE_FALSE"),
    ],
)
def test_compare_uses_the_value_loaded_by_the_preceding_step(
    operator: str, threshold: str, expected_pass: bool, expected_reason: str
) -> None:
    # 14 changes of -1 from 114 gives RSI 0; a hand-picked series is easier to
    # reason about, so drive COMPARE from an explicitly seeded operand.
    catalog = element_catalog(CATALOG_VERSION)
    evaluation = _evaluation(["100"] * 15)
    evaluation.record("RSI_14", Decimal("20.00000000"))

    outcome = catalog.evaluate(_step(2, "COMPARE", operator=operator, threshold=threshold), evaluation)

    assert outcome.is_passed is expected_pass
    assert outcome.reason_code == expected_reason
    assert outcome.evidence["operand"] == "20.00000000"
    assert outcome.evidence["threshold"] == threshold
    assert outcome.evidence["operator"] == operator


def test_rsi_below_thirty_is_the_fixture_trigger() -> None:
    # sum(gain)=1, sum(loss)=7 -> RS = 1/7, RSI = 100 - 100/(8/7) = 12.5.
    oversold = ["100", "101", "100", "99", "98", "97", "96", "95", "94"] + ["94"] * 6
    catalog = element_catalog(CATALOG_VERSION)
    compare = _step(2, "COMPARE", operator="LT", threshold="30")

    triggered = _evaluation(oversold)
    loaded = catalog.evaluate(LOAD_RSI, triggered)
    fired = catalog.evaluate(compare, triggered)

    # sum(gain)=1, sum(loss)=2 -> RSI = 33.33333333, above the 30 threshold.
    quiet = _evaluation(["100", "101"] + ["99"] * 13)
    catalog.evaluate(LOAD_RSI, quiet)
    held = catalog.evaluate(compare, quiet)

    assert loaded.evidence["value"] == "12.50000000"
    assert fired.is_passed is True
    assert fired.reason_code == "COMPARE_TRUE"
    assert held.is_passed is False
    assert held.reason_code == "COMPARE_FALSE"


def test_compare_without_a_loaded_operand_is_an_evaluation_error() -> None:
    evaluation = _evaluation(["100"] * 15)

    with pytest.raises(ElementEvaluationError) as failure:
        element_catalog(CATALOG_VERSION).evaluate(_step(2, "COMPARE", operator="LT", threshold="30"), evaluation)

    assert failure.value.reason_code == "CONDITION_EVALUATION_ERROR"
    assert "operand" in str(failure.value)


# ---------------------------------------------------------------------------
# Catalog compatibility
# ---------------------------------------------------------------------------


def test_the_published_catalog_version_is_the_one_b_emits() -> None:
    assert ELEMENT_CATALOG_VERSIONS == (
        "basic-elements:2026-07-31",
        "basic-elements:2026-08-07",
        "basic-elements:2026-08-08",
    )
    catalog = element_catalog(CATALOG_VERSION)
    assert catalog.version == CATALOG_VERSION
    assert sorted(catalog.operations) == [
        "COMPARE",
        "EMIT_ORDER_CANDIDATE",
        "LOAD_FEATURE",
    ]
    assert catalog.feature_versions == {"RSI_14": "rsi:1.0.0"}
    assert supported_feature_versions() == {"RSI_14": "rsi:1.0.0"}


def test_production_catalog_exposes_every_ui_operation_and_only_new_resolutions() -> None:
    catalog = element_catalog("basic-elements:2026-08-08")
    assert set(catalog.operations) == {
        "PRICE_COMPARE",
        "PRICE_CHANGE_PERCENT",
        "VOLUME_COMPARE",
        "STREAK",
        "SMA_CROSS",
        "RSI_CROSS",
        "MACD_CROSS",
        "BOLLINGER_REVERSAL",
        "POSITION_RETURN",
        "HOLDING_PERIOD",
        "PEAK_RETURN",
        "DRAWDOWN_FROM_PEAK",
        "SCHEDULE",
        "EMIT_ORDER_CANDIDATE",
    }
    for _operation, spec in catalog.specs.items():
        if "resolution" in spec.enumerations:
            assert spec.enumerations["resolution"] == ("30m", "1h", "4h", "1d")


def _pinned_rsi_series(
    *, resolution: str = "30m", previous: str = "49.00000000", current: str = "51.00000000",
) -> PinnedFeatureSeries:
    period = resolution_period(resolution)
    return PinnedFeatureSeries(
        feature_id="RSI_14",
        instrument_id=INSTRUMENT,
        resolution=resolution,
        values=(
            PinnedFeatureValue(bar_start_at=ORIGIN - period * 2, value=Decimal(previous)),
            PinnedFeatureValue(bar_start_at=ORIGIN - period, value=Decimal(current)),
        ),
    )


def test_rsi_cross_consumes_the_official_pinned_series_instead_of_recomputing_closes() -> None:
    inputs = InstrumentInput(
        instrument_id=INSTRUMENT,
        series=(),
        feature_series=(_pinned_rsi_series(),),
        require_pinned_features=True,
        values={"bar.closed.30m": "true"},
    )
    evaluation = ElementEvaluation(instrument_id=INSTRUMENT, as_of=ORIGIN, inputs=inputs)
    step = _step(
        1, "RSI_CROSS", resolution="30m", direction="UP", period="14", threshold="50",
    )

    outcome = element_catalog("basic-elements:2026-08-08").evaluate(step, evaluation)

    assert outcome.is_passed is True
    assert outcome.evidence["previous"] == "49"
    assert outcome.evidence["current"] == "51"
    assert outcome.evidence["source"] == "PINNED_FEATURE_OUTPUT"


def test_rsi_cross_fails_closed_when_the_pinned_series_has_a_gap() -> None:
    period = resolution_period("30m")
    inputs = InstrumentInput(
        instrument_id=INSTRUMENT,
        series=(),
        feature_series=(PinnedFeatureSeries(
            feature_id="RSI_14",
            instrument_id=INSTRUMENT,
            resolution="30m",
            values=(PinnedFeatureValue(
                bar_start_at=ORIGIN - period,
                value=Decimal("51.00000000"),
            ),),
        ),),
        require_pinned_features=True,
        values={"bar.closed.30m": "true"},
    )
    evaluation = ElementEvaluation(instrument_id=INSTRUMENT, as_of=ORIGIN, inputs=inputs)

    with pytest.raises(ElementInputMissing) as failure:
        element_catalog("basic-elements:2026-08-08").evaluate(
            _step(1, "RSI_CROSS", resolution="30m", direction="UP", period="14", threshold="50"),
            evaluation,
        )

    assert failure.value.input_reason == "FEATURE_SERIES_DATA_GAP"


@pytest.mark.parametrize(
    ("operation", "arguments"),
    [
        ("PRICE_COMPARE", {"resolution": "30m", "operator": "GT", "reference": "PREVIOUS_CLOSE"}),
        (
            "PRICE_CHANGE_PERCENT",
            {"resolution": "30m", "base": "PREVIOUS_CLOSE", "direction": "UP", "thresholdPercent": "1"},
        ),
        (
            "VOLUME_COMPARE",
            {"resolution": "30m", "operator": "GT", "reference": "PREVIOUS_VOLUME", "period": "1", "multiplier": "1"},
        ),
        ("STREAK", {"resolution": "30m", "direction": "UP", "bars": "2"}),
        ("SMA_CROSS", {"resolution": "30m", "direction": "UP", "shortPeriod": "5", "longPeriod": "20"}),
        ("RSI_CROSS", {"resolution": "30m", "direction": "UP", "period": "14", "threshold": "50"}),
        (
            "MACD_CROSS",
            {"resolution": "30m", "direction": "UP", "fastPeriod": "12", "slowPeriod": "26", "signalPeriod": "9"},
        ),
        ("BOLLINGER_REVERSAL", {"resolution": "30m", "direction": "UP", "period": "20", "deviations": "2"}),
        ("POSITION_RETURN", {"direction": "PROFIT", "thresholdPercent": "1"}),
        ("HOLDING_PERIOD", {"unit": "BAR", "amount": "1", "resolution": "30m"}),
        ("PEAK_RETURN", {"operator": "GTE", "thresholdPercent": "1"}),
        ("DRAWDOWN_FROM_PEAK", {"operator": "LTE", "thresholdPercent": "2"}),
        ("SCHEDULE", {"cycle": "EVERY_TRADING_DAY", "interval": "1", "resolution": "30m"}),
    ],
)
def test_every_production_condition_has_an_executable_evaluator(operation: str, arguments: dict[str, str]) -> None:
    closes = ",".join(str(value) for value in range(1, 160))
    volumes = ",".join(str(value) for value in range(100, 259))
    values = {
        "bar.closed.30m": "true",
        "closes.30m": closes,
        "volumes.30m": volumes,
        "session.open": "1",
        "session.close": "true",
        "position.averageEntryPrice": "100",
        "position.returnPercent": "5",
        "position.peakReturnPercent": "8",
        "position.drawdownPercent": "1",
        "position.holdingBars.30m": "3",
        "position.holdingTradingDays": "2",
        "schedule.newTradingDay": "true",
        "schedule.tradingDayIndex": "1",
        "schedule.weekFirstTradingDay": "true",
        "schedule.monthFirstTradingDay": "true",
        "schedule.monthLastTradingDay": "false",
    }
    pinned_features = (_pinned_rsi_series(),) if operation == "RSI_CROSS" else ()
    evaluation = ElementEvaluation(
        instrument_id=INSTRUMENT,
        as_of=ORIGIN,
        inputs=InstrumentInput(
            instrument_id=INSTRUMENT,
            series=(),
            feature_series=pinned_features,
            require_pinned_features=bool(pinned_features),
            values=values,
        ),
    )
    step = PlanStep(sequence=1, operation=operation, arguments=arguments)
    catalog = element_catalog("basic-elements:2026-08-08")
    catalog.validate_step(step)
    assert catalog.evaluate(step, evaluation).reason_code


def test_unknown_catalog_version_fails_loudly_instead_of_defaulting() -> None:
    with pytest.raises(ElementCompatibilityError) as failure:
        element_catalog("basic-elements:2099-01-01")

    assert failure.value.failure is PlanLoadFailure.ELEMENT_CATALOG_VERSION_UNSUPPORTED
    assert "basic-elements:2099-01-01" in str(failure.value)


@pytest.mark.parametrize(
    ("operation", "arguments", "failure", "detail"),
    [
        ("PRO_SCRIPT", {}, PlanLoadFailure.UNSUPPORTED_ELEMENT, "PRO_SCRIPT"),
        (
            "LOAD_FEATURE",
            {"feature": "RSI_14"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "resolution",
        ),
        (
            "LOAD_FEATURE",
            {"feature": "RSI_14", "resolution": "1m", "lookahead": "1"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "lookahead",
        ),
        (
            "LOAD_FEATURE",
            {"feature": "MACD", "resolution": "1m"},
            PlanLoadFailure.UNSUPPORTED_FEATURE,
            "MACD",
        ),
        (
            "LOAD_FEATURE",
            {"feature": "RSI_14", "resolution": "3s"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "3s",
        ),
        (
            "COMPARE",
            {"operator": "APPROX", "threshold": "30"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "APPROX",
        ),
        (
            "COMPARE",
            {"operator": "LT", "threshold": "thirty"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "threshold",
        ),
        (
            "EMIT_ORDER_CANDIDATE",
            {"allocation": "WEIGHTED", "orderType": "MARKET", "side": "BUY"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "WEIGHTED",
        ),
        (
            "EMIT_ORDER_CANDIDATE",
            {"allocation": "EQUAL", "orderType": "LIMIT", "side": "BUY"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "LIMIT",
        ),
        (
            "EMIT_ORDER_CANDIDATE",
            {"allocation": "EQUAL", "orderType": "MARKET", "side": "SHORT"},
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "SHORT",
        ),
    ],
)
def test_unsupported_elements_and_arguments_are_rejected_at_load_time(
    operation: str,
    arguments: dict[str, str],
    failure: PlanLoadFailure,
    detail: str,
) -> None:
    catalog = element_catalog(CATALOG_VERSION)

    with pytest.raises(ElementCompatibilityError) as raised:
        catalog.validate_step(_step(1, operation, **arguments))

    assert raised.value.failure is failure
    assert detail in str(raised.value)


def test_the_fixture_steps_validate_unchanged() -> None:
    catalog = element_catalog(CATALOG_VERSION)

    catalog.validate_step(_step(1, "LOAD_FEATURE", feature="RSI_14", resolution="1m"))
    catalog.validate_step(_step(2, "COMPARE", operator="LT", threshold="30"))
    catalog.validate_step(
        _step(
            3,
            "EMIT_ORDER_CANDIDATE",
            allocation="EQUAL",
            orderType="MARKET",
            side="BUY",
        )
    )

    assert catalog.spec("EMIT_ORDER_CANDIDATE").terminal is True
    assert catalog.spec("LOAD_FEATURE").terminal is False
    assert catalog.spec("LOAD_FEATURE").produces_value is True
    assert catalog.spec("COMPARE").consumes_value is True


def test_evaluating_the_terminal_order_element_is_a_programming_error() -> None:
    catalog = element_catalog(CATALOG_VERSION)
    step = _step(3, "EMIT_ORDER_CANDIDATE", allocation="EQUAL", orderType="MARKET", side="BUY")

    with pytest.raises(ElementEvaluationError, match="terminal"):
        catalog.evaluate(step, _evaluation(["100"] * 15))


# ---------------------------------------------------------------------------
# Series and resolution invariants
# ---------------------------------------------------------------------------


def test_resolution_periods_are_pinned() -> None:
    assert resolution_period("1m") == timedelta(minutes=1)
    assert resolution_period("5m") == timedelta(minutes=5)
    assert resolution_period("15m") == timedelta(minutes=15)
    assert resolution_period("30m") == timedelta(minutes=30)
    assert resolution_period("1h") == timedelta(hours=1)
    assert resolution_period("4h") == timedelta(hours=4)
    assert resolution_period("1d") == timedelta(days=1)
    with pytest.raises(ElementCompatibilityError, match="2h"):
        resolution_period("2h")


def test_series_rejects_out_of_order_and_mismatched_bars() -> None:
    ordered = _bars(["100", "101", "102"])

    with pytest.raises(ElementEvaluationError, match="ascending"):
        InstrumentSeries(
            instrument_id=INSTRUMENT,
            data_kind="ADJUSTED_BAR",
            resolution="1m",
            bars=(ordered[1], ordered[0], ordered[2]),
        )
    with pytest.raises(ElementEvaluationError, match="resolution"):
        InstrumentSeries(
            instrument_id=INSTRUMENT,
            data_kind="ADJUSTED_BAR",
            resolution="5m",
            bars=ordered,
        )
    with pytest.raises(ElementEvaluationError, match="instrument"):
        InstrumentSeries(
            instrument_id="00000000-0000-4000-8000-000000000302",
            data_kind="ADJUSTED_BAR",
            resolution="1m",
            bars=ordered,
        )


def test_bar_rejects_a_span_that_is_not_its_declared_resolution() -> None:
    with pytest.raises(ElementEvaluationError, match="resolution"):
        SeriesBar(
            instrument_id=INSTRUMENT,
            resolution="1m",
            starts_at=ORIGIN,
            ends_at=ORIGIN + timedelta(minutes=2),
            close=Decimal("100"),
            volume=Decimal(1),
        )


def test_instrument_input_rejects_two_series_with_the_same_identity() -> None:
    series = InstrumentSeries(
        instrument_id=INSTRUMENT,
        data_kind="ADJUSTED_BAR",
        resolution="1m",
        bars=_bars(["100", "101"]),
    )

    with pytest.raises(ElementEvaluationError, match="unique"):
        InstrumentInput(instrument_id=INSTRUMENT, series=(series, series))
