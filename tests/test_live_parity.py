"""Executable specification for where the backtest must agree with the live runtime.

A released bot and its official backtest are two implementations of one strategy, and the
product's whole claim is that the second predicts the first. Every test here states a rule
both must obey, phrased from the backtest's side, and each one failed before the parity fix
it guards.

The fixture dates are deliberate. 2025-11-28 is the day after Thanksgiving: the session runs
14:30-18:00 UTC, a 13:00 ET early close rather than the usual 16:00. A rule that assumes a
fixed session length passes on 2025-12-01 and fails here.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction
from types import SimpleNamespace

import pytest

from backtest_engine.basic_runtime import (
    LIVE_SERIES_BARS,
    BasicDecisionStatus,
    BasicInstrumentDecision,
    BasicPlanReplay,
    _published_values,
    _ReplayExecutionState,
    bar_closed_event,
)
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.elements.core import (
    ElementInputMissing,
    PinnedFeatureSeries,
    PinnedFeatureValue,
)
from backtest_engine.elements.orders import OrderCandidate


INSTRUMENT = "00000000-0000-4000-8000-000000000301"

EARLY_CLOSE_DAY = date(2025, 11, 28)
REGULAR_DAY = date(2025, 12, 1)


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _series(resolution: str, *bar_starts: str) -> PinnedFeatureSeries:
    return PinnedFeatureSeries(
        feature_id="RSI_14",
        instrument_id=INSTRUMENT,
        resolution=resolution,
        values=tuple(
            PinnedFeatureValue(bar_start_at=_utc(moment), value=Decimal("55.00000000")) for moment in bar_starts
        ),
    )


class TestPinnedSeriesReadsShortSessionBars:
    """A pinned feature resolves for the bar that closed at the boundary, short or not.

    A 6.5-hour session does not divide evenly by 1h, 4h, or 24h, so the last bar of every
    session is shorter than its nominal period. Requiring ``boundary - period`` to land
    exactly on the bar's start made those bars unreadable, which took ``4h`` and ``1d``
    strategies out of the backtest entirely while they ran normally in production.
    """

    def test_a_daily_bar_spanning_only_the_session_resolves_at_its_close(self) -> None:
        # The 1d bar of a regular session covers 14:30-21:00 UTC: 6.5 hours, not 24.
        series = _series("1d", "2025-11-26T14:30:00+00:00", "2025-12-01T14:30:00+00:00")

        assert series.value_at(_utc("2025-12-01T21:00:00+00:00")) == Decimal("55.00000000")

    def test_a_daily_bar_resolves_on_an_early_close_day_too(self) -> None:
        series = _series("1d", "2025-11-28T14:30:00+00:00")

        assert series.value_at(_utc("2025-11-28T18:00:00+00:00")) == Decimal("55.00000000")

    def test_the_short_second_four_hour_bar_of_a_session_resolves(self) -> None:
        # 4h bars of a regular session are 14:30-18:30 and then 18:30-21:00, only 2.5 hours.
        series = _series("4h", "2025-12-01T14:30:00+00:00", "2025-12-01T18:30:00+00:00")

        assert series.value_at(_utc("2025-12-01T21:00:00+00:00")) == Decimal("55.00000000")

    def test_the_short_last_hourly_bar_of_a_session_resolves(self) -> None:
        # The last 1h bar of a regular session is 20:30-21:00, only 30 minutes.
        series = _series("1h", "2025-12-01T19:30:00+00:00", "2025-12-01T20:30:00+00:00")

        assert series.value_at(_utc("2025-12-01T21:00:00+00:00")) == Decimal("55.00000000")

    def test_a_bar_on_the_regular_grid_still_resolves(self) -> None:
        series = _series("30m", "2025-12-01T20:00:00+00:00", "2025-12-01T20:30:00+00:00")

        assert series.value_at(_utc("2025-12-01T21:00:00+00:00")) == Decimal("55.00000000")


class TestPinnedSeriesStillReportsGaps:
    """Admitting short bars must not let a stale value stand in for a missing one."""

    def test_a_missing_bar_is_a_gap_rather_than_the_previous_value(self) -> None:
        # The 20:30 bar never arrived, so nothing covers the 21:00 boundary.
        series = _series("30m", "2025-12-01T19:30:00+00:00", "2025-12-01T20:00:00+00:00")

        with pytest.raises(ElementInputMissing) as failure:
            series.value_at(_utc("2025-12-01T21:00:00+00:00"))

        assert failure.value.input_reason == "FEATURE_SERIES_DATA_GAP"

    def test_a_missing_daily_bar_is_a_gap_rather_than_yesterdays_value(self) -> None:
        series = _series("1d", "2025-11-26T14:30:00+00:00")

        with pytest.raises(ElementInputMissing) as failure:
            series.value_at(_utc("2025-12-01T21:00:00+00:00"))

        assert failure.value.input_reason == "FEATURE_SERIES_DATA_GAP"

    def test_a_value_from_a_bar_that_has_not_closed_is_not_visible(self) -> None:
        series = _series("30m", "2025-12-01T21:00:00+00:00")

        with pytest.raises(ElementInputMissing):
            series.value_at(_utc("2025-12-01T21:00:00+00:00"))


def _schedule_values(trading_date: date, instant: str, earlier_bars: tuple[str, ...] = ()) -> dict[str, str]:
    """The schedule inputs the replay publishes at one instant.

    ``_schedule_values`` reads nothing but the pinned schedule, so a stub carrying one is a
    complete collaborator for it.
    """
    schedule = XNYS_CALENDAR.session_schedule(date(2025, 11, 24), date(2025, 12, 31))
    replay = SimpleNamespace(clock=SimpleNamespace(schedule=schedule))
    events = tuple(SimpleNamespace(occurred_at=_utc(moment), payload={}) for moment in earlier_bars)
    return BasicPlanReplay._schedule_values(replay, _utc(instant), events)


class TestSchedulePeriodFlags:
    """A "first trading day of the period" flag is true once per day, on its first bar.

    Live publishes each of these only when the trading day itself just changed. Publishing
    the day's property on every bar instead made a 30m clock fire a month-open rule thirteen
    times a day in the backtest and once in production.
    """

    def test_the_first_bar_of_a_month_opening_session_sets_the_month_flag(self) -> None:
        values = _schedule_values(REGULAR_DAY, "2025-12-01T15:00:00+00:00")

        assert values["schedule.newTradingDay"] == "true"
        assert values["schedule.monthFirstTradingDay"] == "true"

    def test_a_later_bar_of_the_same_session_does_not_repeat_the_month_flag(self) -> None:
        values = _schedule_values(
            REGULAR_DAY,
            "2025-12-01T15:30:00+00:00",
            earlier_bars=("2025-12-01T15:00:00+00:00",),
        )

        assert values["schedule.newTradingDay"] == "false"
        assert values["schedule.monthFirstTradingDay"] == "false"

    def test_a_later_bar_of_the_same_session_does_not_repeat_the_week_flag(self) -> None:
        values = _schedule_values(
            REGULAR_DAY,
            "2025-12-01T15:30:00+00:00",
            earlier_bars=("2025-12-01T15:00:00+00:00",),
        )

        assert values["schedule.weekFirstTradingDay"] == "false"

    def test_the_first_bar_of_a_weeks_opening_session_sets_the_week_flag(self) -> None:
        values = _schedule_values(REGULAR_DAY, "2025-12-01T15:00:00+00:00")

        assert values["schedule.weekFirstTradingDay"] == "true"

    def test_a_bar_from_the_previous_session_does_not_suppress_the_flag(self) -> None:
        # The visible window spans sessions; only bars from *this* session count as earlier.
        values = _schedule_values(
            REGULAR_DAY,
            "2025-12-01T15:00:00+00:00",
            earlier_bars=("2025-11-28T17:00:00+00:00",),
        )

        assert values["schedule.newTradingDay"] == "true"
        assert values["schedule.monthFirstTradingDay"] == "true"


class TestSessionCloseFollowsTheCalendar:
    """``session.close`` is the session's own close, not a fixed hour of the clock."""

    def test_the_early_close_bar_closes_the_session(self) -> None:
        values = _schedule_values(EARLY_CLOSE_DAY, "2025-11-28T18:00:00+00:00")

        assert values["session.close"] == "true"

    def test_a_bar_before_the_early_close_does_not_close_the_session(self) -> None:
        values = _schedule_values(EARLY_CLOSE_DAY, "2025-11-28T17:30:00+00:00")

        assert values["session.close"] == "false"

    def test_the_regular_close_bar_closes_the_session(self) -> None:
        values = _schedule_values(REGULAR_DAY, "2025-12-01T21:00:00+00:00")

        assert values["session.close"] == "true"

    def test_the_hour_that_closes_a_regular_day_does_not_close_an_early_day(self) -> None:
        # 18:00 UTC is 13:00 ET. On a regular session that is mid-afternoon, and the only
        # thing distinguishing it from the early-close case above is the calendar.
        values = _schedule_values(REGULAR_DAY, "2025-12-01T18:00:00+00:00")

        assert values["session.close"] == "false"


def _candidate(
    *,
    flow_id: str = "flow-1",
    instrument_id: str = INSTRUMENT,
    execution_mode: str = "1회만",
    wait_mode: str = "조건 재충족",
    wait_interval: int = 1,
    max_executions: int = 1,
    session_date_et: date = REGULAR_DAY,
) -> OrderCandidate:
    return OrderCandidate(
        evaluation_id="00000000-0000-4000-8000-000000000001",
        instrument_id=instrument_id,
        partition_key="partition-1",
        flow_id=flow_id,
        side="BUY",
        order_type="MARKET",
        allocation=Fraction(1, 1),
        reference_price=Decimal("100.00000000"),
        decided_at=_utc("2025-12-01T15:00:00+00:00"),
        eligible_at=_utc("2025-12-01T15:00:00+00:00"),
        session_date_et=session_date_et,
        session_closes_at=_utc("2025-12-01T21:00:00+00:00"),
        budget_cap_bps=10_000,
        order_percent=Decimal("100"),
        execution_mode=execution_mode,
        wait_mode=wait_mode,
        wait_interval=wait_interval,
        max_executions=max_executions,
    )


class TestExecutionGateMatchesLive:
    """The gate's own arithmetic, which must stay identical to ``EvaluatingBotRuntime``."""

    def test_one_shot_admits_the_first_candidate_and_refuses_the_next(self) -> None:
        state = _ReplayExecutionState()

        assert [state.accepts(_candidate()) for _ in range(2)] == [True, False]

    def test_an_admitted_attempt_is_not_refunded_by_a_downstream_non_fill(self) -> None:
        """The durable live gate restores immutable intents, not broker fill outcomes."""
        state = _ReplayExecutionState()

        assert state.accepts(_candidate()) is True
        # Risk rejection, expiry, cancellation and partial fill occur after this boundary.
        # There is deliberately no rollback transition: retrying the same signal would create a
        # second order attempt and make worker redelivery alter strategy meaning.
        assert state.executions == 1
        assert state.accepts(_candidate()) is False

    def test_active_catalog_applies_one_shot_at_the_replay_boundary(self) -> None:
        """A catalog publication must not silently turn off an execution semantic."""
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace(catalog=SimpleNamespace(execution_gate=True))
        replay._execution_states = {}
        candidate = _candidate()
        replay._session_index_by_date = {candidate.session_date_et: 0}

        assert replay._apply_execution_policy((), (candidate,)) == (candidate,)
        assert replay._apply_execution_policy((), (candidate,)) == ()

    def test_plan_without_gate_capability_bypasses_the_execution_gate(self) -> None:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace()
        replay._execution_states = {}
        replay._session_index_by_date = {}
        candidate = _candidate()

        assert replay._apply_execution_policy((), (candidate,)) == (candidate,)
        assert replay._execution_states == {}

    def test_non_candidate_decision_rearms_the_matching_condition_gate(self) -> None:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace(catalog=SimpleNamespace(execution_gate=True))
        replay._execution_states = {}
        candidate = _candidate(execution_mode="대기 후 재진입", max_executions=3)
        replay._session_index_by_date = {candidate.session_date_et: 0}

        assert replay._apply_execution_policy((), (candidate,)) == (candidate,)
        assert replay._apply_execution_policy((), (candidate,)) == ()

        missed = BasicInstrumentDecision(
            flow_id=candidate.flow_id,
            instrument_id=candidate.instrument_id,
            side="BUY",
            status=BasicDecisionStatus.CONDITION_NOT_MET,
            trace=(),
        )
        assert replay._apply_execution_policy((missed,), ()) == ()
        assert replay._apply_execution_policy((), (candidate,)) == (candidate,)

    def test_condition_can_be_false_before_its_first_candidate(self) -> None:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace(catalog=SimpleNamespace(execution_gate=True))
        replay._execution_states = {}
        replay._session_index_by_date = {REGULAR_DAY: 0}
        candidate = _candidate(execution_mode="대기 후 재진입", max_executions=3)
        missed = BasicInstrumentDecision(
            flow_id=candidate.flow_id,
            instrument_id=candidate.instrument_id,
            side="BUY",
            status=BasicDecisionStatus.CONDITION_NOT_MET,
            trace=(),
        )

        assert replay._apply_execution_policy((missed,), ()) == ()
        assert replay._apply_execution_policy((), (candidate,)) == (candidate,)

    def test_gate_state_is_scoped_by_flow_and_instrument(self) -> None:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace(catalog=SimpleNamespace(execution_gate=True))
        replay._execution_states = {}
        replay._session_index_by_date = {REGULAR_DAY: 0}
        other = _candidate(flow_id="flow-2", instrument_id="00000000-0000-4000-8000-000000000302")
        first = _candidate()

        assert replay._apply_execution_policy((), (first, other)) == (first, other)
        assert set(replay._execution_states) == {
            (first.flow_id, first.instrument_id),
            (other.flow_id, other.instrument_id),
        }

    def test_replay_gate_receives_the_official_session_index(self) -> None:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay.plan = SimpleNamespace(catalog=SimpleNamespace(execution_gate=True))
        replay._execution_states = {}
        replay._session_index_by_date = {
            date(2025, 12, 1): 0,
            date(2025, 12, 2): 1,
            date(2025, 12, 3): 2,
        }

        def at(day: int) -> OrderCandidate:
            return _candidate(
                execution_mode="대기 후 재진입",
                wait_mode="N거래일 이후",
                wait_interval=2,
                max_executions=3,
                session_date_et=date(2025, 12, day),
            )

        first, second, third = at(1), at(2), at(3)
        assert replay._apply_execution_policy((), (first,)) == (first,)
        assert replay._apply_execution_policy((), (second,)) == ()
        assert replay._apply_execution_policy((), (third,)) == (third,)

    def test_cycle_mode_admits_up_to_the_declared_limit(self) -> None:
        state = _ReplayExecutionState()
        candidate = _candidate(execution_mode="주기마다", max_executions=3)

        assert [state.accepts(candidate) for _ in range(4)] == [True, True, True, False]

    def test_bar_wait_needs_the_interval_to_elapse(self) -> None:
        state = _ReplayExecutionState()
        candidate = _candidate(
            execution_mode="대기 후 재진입",
            wait_mode="N봉 이후",
            wait_interval=2,
            max_executions=3,
        )
        state.accepts(candidate)

        assert state.accepts(candidate) is False
        assert state.accepts(candidate) is True

    def test_trading_day_wait_counts_weekdays_between_sessions(self) -> None:
        state = _ReplayExecutionState()
        session_index = {
            session.trading_date_et: index
            for index, session in enumerate(
                XNYS_CALENDAR.session_schedule(date(2025, 12, 1), date(2025, 12, 3)).sessions
            )
        }

        def at(session_date: date) -> OrderCandidate:
            return _candidate(
                execution_mode="대기 후 재진입",
                wait_mode="N거래일 이후",
                wait_interval=2,
                max_executions=3,
                session_date_et=session_date,
            )

        state.accepts(at(date(2025, 12, 1)), session_index=session_index[date(2025, 12, 1)])

        assert state.accepts(at(date(2025, 12, 2)), session_index=session_index[date(2025, 12, 2)]) is False
        assert state.accepts(at(date(2025, 12, 3)), session_index=session_index[date(2025, 12, 3)]) is True

    def test_trading_day_wait_does_not_count_a_weekday_exchange_holiday(self) -> None:
        state = _ReplayExecutionState()
        session_index = {
            session.trading_date_et: index
            for index, session in enumerate(
                XNYS_CALENDAR.session_schedule(date(2025, 12, 24), date(2025, 12, 29)).sessions
            )
        }

        def at(session_date: date) -> OrderCandidate:
            return _candidate(
                execution_mode="대기 후 재진입",
                wait_mode="N거래일 이후",
                wait_interval=2,
                max_executions=3,
                session_date_et=session_date,
            )

        state.accepts(at(date(2025, 12, 24)), session_index=session_index[date(2025, 12, 24)])

        # Christmas is a Thursday but XNYS is closed. Friday is only the first
        # elapsed trading session; Monday is the second.
        assert state.accepts(at(date(2025, 12, 26)), session_index=session_index[date(2025, 12, 26)]) is False
        assert state.accepts(at(date(2025, 12, 29)), session_index=session_index[date(2025, 12, 29)]) is True

    def test_condition_rearm_requires_the_condition_to_fail_first(self) -> None:
        state = _ReplayExecutionState()
        candidate = _candidate(execution_mode="대기 후 재진입", max_executions=3)
        state.accepts(candidate)

        assert state.accepts(candidate) is False

        state.condition_rearmed = True

        assert state.accepts(candidate) is True


class TestExecutionGateIsScopedToOnePositionCycle:
    """``1회만`` counts within one position, not once per backtest.

    Live drops an instrument's gates whenever its position snapshot stops matching, so a
    strategy that buys, sells, and buys again trades repeatedly. Counting for the whole
    replay backtested that same strategy as a single trade.
    """

    @staticmethod
    def _replay() -> BasicPlanReplay:
        replay = BasicPlanReplay.__new__(BasicPlanReplay)
        replay._execution_states = {}
        replay._position_identities = {}
        return replay

    @staticmethod
    def _holding(average: str, opened_at: str = "2025-12-01T14:30:00+00:00") -> dict[str, dict[str, str]]:
        return {
            INSTRUMENT: {
                "position.averageEntryPrice": average,
                "position.openedAt": opened_at,
            }
        }

    def test_closing_the_position_releases_the_gate(self) -> None:
        replay = self._replay()
        replay._retire_closed_position_gates(self._holding("100"))
        replay._execution_states[("flow-1", INSTRUMENT)] = _ReplayExecutionState(executions=1)

        replay._retire_closed_position_gates({})

        assert replay._execution_states == {}

    def test_re_entering_in_a_new_position_cycle_releases_the_gate(self) -> None:
        replay = self._replay()
        replay._retire_closed_position_gates(self._holding("100"))
        replay._execution_states[("flow-1", INSTRUMENT)] = _ReplayExecutionState(executions=1)

        replay._retire_closed_position_gates(self._holding("104", opened_at="2025-12-02T14:30:00+00:00"))

        assert replay._execution_states == {}

    def test_adding_to_the_same_position_cycle_keeps_the_gate(self) -> None:
        replay = self._replay()
        replay._retire_closed_position_gates(self._holding("100"))
        state = _ReplayExecutionState(executions=1)
        replay._execution_states[("flow-1", INSTRUMENT)] = state

        replay._retire_closed_position_gates(self._holding("104"))

        assert replay._execution_states == {("flow-1", INSTRUMENT): state}

    def test_holding_the_same_position_keeps_the_gate(self) -> None:
        replay = self._replay()
        replay._retire_closed_position_gates(self._holding("100"))
        state = _ReplayExecutionState(executions=1)
        replay._execution_states[("flow-1", INSTRUMENT)] = state

        replay._retire_closed_position_gates(self._holding("100"))

        assert replay._execution_states == {("flow-1", INSTRUMENT): state}

    def test_another_instruments_gate_survives_this_ones_close(self) -> None:
        other = "00000000-0000-4000-8000-000000000302"
        replay = self._replay()
        replay._retire_closed_position_gates(
            {
                INSTRUMENT: {
                    "position.averageEntryPrice": "100",
                    "position.openedAt": "2025-12-01T14:30:00+00:00",
                },
                other: {
                    "position.averageEntryPrice": "200",
                    "position.openedAt": "2025-12-01T14:30:00+00:00",
                },
            }
        )
        kept = _ReplayExecutionState(executions=1)
        replay._execution_states[("flow-1", INSTRUMENT)] = _ReplayExecutionState(executions=1)
        replay._execution_states[("flow-1", other)] = kept

        replay._retire_closed_position_gates(
            {
                other: {
                    "position.averageEntryPrice": "200",
                    "position.openedAt": "2025-12-01T14:30:00+00:00",
                }
            }
        )

        assert replay._execution_states == {("flow-1", other): kept}


class TestTheVisibleWindowMatchesLive:
    """A strategy sees a bounded rolling window, because the live runtime only has one.

    ``MACD_CROSS`` reads an EMA, an unbounded recursion whose value depends on every earlier
    bar. An uncapped replay and a live runtime holding 180 bars therefore compute different
    histograms for the same instant and never converge -- and the histogram is compared against
    zero, so near a crossing the difference decides the trade.
    """

    @staticmethod
    def _values(count: int) -> dict[str, str]:
        events = tuple(
            bar_closed_event(
                event_id=f"bar-{index:04d}",
                instrument_id=INSTRUMENT,
                data_kind="ADJUSTED_BAR",
                resolution="30m",
                starts_at=_utc("2025-12-01T14:30:00+00:00") + timedelta(minutes=30 * index),
                close=Decimal(100 + index),
                volume=Decimal(1000),
                source_sequence=index + 1,
            )
            for index in range(count)
        )
        return _published_values(
            INSTRUMENT,
            {(INSTRUMENT, "30m"): list(events)},
            events[-1].occurred_at,
            runtime_values={},
            shared_values={},
        )

    @classmethod
    def _closes(cls, count: int) -> str:
        return cls._values(count)["closes.30m"]

    def test_only_catalog_consumed_series_are_materialized(self) -> None:
        assert set(self._values(2)) == {"closes.30m", "volumes.30m", "bar.closed.30m"}

    def test_a_short_history_is_shown_whole(self) -> None:
        assert len(self._closes(40).split(",")) == 40

    def test_a_history_at_the_bound_is_shown_whole(self) -> None:
        assert len(self._closes(LIVE_SERIES_BARS).split(",")) == LIVE_SERIES_BARS

    def test_a_longer_history_is_truncated_to_the_bound(self) -> None:
        assert len(self._closes(LIVE_SERIES_BARS + 50).split(",")) == LIVE_SERIES_BARS

    def test_the_newest_bars_are_the_ones_kept(self) -> None:
        """Truncation drops the oldest bars; the newest close must still be last."""
        closes = self._closes(LIVE_SERIES_BARS + 50).split(",")

        assert Decimal(closes[-1]) == Decimal(100 + LIVE_SERIES_BARS + 49)
        assert Decimal(closes[0]) == Decimal(100 + 50)
