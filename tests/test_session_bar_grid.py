"""The official session-aligned bar grid, walked backwards on the real XNYS schedule.

Bars are anchored at each session open and the last bar of a session ends at its close, so
1h and 4h sessions end with a short bar, a 1d bar is the whole session, and an early-close
day simply has fewer bars. Nights, weekends and holidays hold no bar at all.

The fortnight around Thanksgiving 2025 holds all of those shapes: Thursday 2025-11-27 is
closed and Friday 2025-11-28 closes at 13:00 ET (18:00Z).
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.event_clock import EventClockValidationError, OfficialSessionSchedule


SCHEDULE = XNYS_CALENDAR.session_schedule(date(2025, 11, 17), date(2025, 12, 12))

PERIODS = {
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
}


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _grid(schedule: OfficialSessionSchedule, period: timedelta) -> list[tuple[datetime, datetime]]:
    bars = []
    for session in schedule.sessions:
        start = session.opens_at
        while start < session.closes_at:
            bars.append((start, min(start + period, session.closes_at)))
            start += period
    return bars


@pytest.mark.parametrize(
    ("resolution", "instant", "expected"),
    [
        # Inside a session the previous bar is simply one period earlier.
        ("30m", "2025-12-01T15:00:00+00:00", "2025-12-01T14:30:00+00:00"),
        ("1h", "2025-12-01T16:30:00+00:00", "2025-12-01T15:30:00+00:00"),
        ("4h", "2025-12-01T18:30:00+00:00", "2025-12-01T14:30:00+00:00"),
        # Monday's open looks back over the weekend to the early-close Friday.
        ("30m", "2025-12-01T14:30:00+00:00", "2025-11-28T17:30:00+00:00"),
        ("1h", "2025-12-01T14:30:00+00:00", "2025-11-28T17:30:00+00:00"),
        ("4h", "2025-12-01T14:30:00+00:00", "2025-11-28T14:30:00+00:00"),
        ("1d", "2025-12-01T14:30:00+00:00", "2025-11-28T14:30:00+00:00"),
        # Friday's open looks back over the Thanksgiving closure to Wednesday.
        ("30m", "2025-11-28T14:30:00+00:00", "2025-11-26T20:30:00+00:00"),
        ("1d", "2025-11-28T14:30:00+00:00", "2025-11-26T14:30:00+00:00"),
        # Overnight, the previous bar is the short last bar of the session before.
        ("1h", "2025-12-02T14:30:00+00:00", "2025-12-01T20:30:00+00:00"),
        ("4h", "2025-12-02T14:30:00+00:00", "2025-12-01T18:30:00+00:00"),
        # A bar still in progress at the instant is not completed and is not counted.
        ("30m", "2025-12-01T14:45:00+00:00", "2025-11-28T17:30:00+00:00"),
        ("1h", "2025-12-01T15:45:00+00:00", "2025-12-01T14:30:00+00:00"),
    ],
)
def test_the_previous_completed_bar_crosses_nights_weekends_and_holidays(
    resolution: str, instant: str, expected: str
) -> None:
    assert SCHEDULE.bar_start_n_bars_before(PERIODS[resolution], _utc(instant), 1) == _utc(expected)


@pytest.mark.parametrize(
    ("resolution", "bars", "expected"),
    [
        # A regular session holds 13 x 30m, 7 x 1h, 2 x 4h and 1 x 1d bars.
        ("30m", 13, "2025-12-01T14:30:00+00:00"),
        ("1h", 7, "2025-12-01T14:30:00+00:00"),
        ("4h", 2, "2025-12-01T14:30:00+00:00"),
        ("1d", 1, "2025-12-01T14:30:00+00:00"),
        # The early-close Friday holds 7 x 30m, 4 x 1h and a single 3.5h bar.
        ("30m", 14, "2025-11-28T17:30:00+00:00"),
        ("30m", 20, "2025-11-28T14:30:00+00:00"),
        ("1h", 11, "2025-11-28T14:30:00+00:00"),
        ("4h", 3, "2025-11-28T14:30:00+00:00"),
        ("4h", 4, "2025-11-26T18:30:00+00:00"),
        ("1d", 2, "2025-11-28T14:30:00+00:00"),
        ("1d", 3, "2025-11-26T14:30:00+00:00"),
    ],
)
def test_walking_back_counts_short_and_early_close_bars_as_one_bar_each(
    resolution: str, bars: int, expected: str
) -> None:
    tuesday_open = _utc("2025-12-02T14:30:00+00:00")

    assert SCHEDULE.bar_start_n_bars_before(PERIODS[resolution], tuesday_open, bars) == _utc(expected)


@pytest.mark.parametrize("resolution", list(PERIODS))
@pytest.mark.parametrize("bars", [1, 2, 4, 13, 21, 37, 61])
def test_exactly_the_requested_number_of_completed_bars_lies_in_between(resolution: str, bars: int) -> None:
    period = PERIODS[resolution]
    instant = _utc("2025-12-12T21:00:00+00:00")

    start = SCHEDULE.bar_start_n_bars_before(period, instant, bars)

    grid = _grid(SCHEDULE, period)
    completed_in_between = [bar for bar in grid if start is not None and start <= bar[0] and bar[1] <= instant]
    if len([bar for bar in grid if bar[1] <= instant]) < bars:
        assert start is None
    else:
        assert len(completed_in_between) == bars


def test_a_schedule_with_too_few_bars_answers_none_rather_than_guessing() -> None:
    first_session = SCHEDULE.sessions[0]

    assert SCHEDULE.bar_start_n_bars_before(PERIODS["1d"], first_session.opens_at, 1) is None
    assert SCHEDULE.bar_start_n_bars_before(PERIODS["30m"], first_session.closes_at, 14) is None


@pytest.mark.parametrize(("period", "bars"), [(timedelta(0), 1), (timedelta(minutes=30), 0)])
def test_a_degenerate_request_is_refused(period: timedelta, bars: int) -> None:
    with pytest.raises(EventClockValidationError):
        SCHEDULE.bar_start_n_bars_before(period, _utc("2025-12-01T14:30:00+00:00"), bars)
