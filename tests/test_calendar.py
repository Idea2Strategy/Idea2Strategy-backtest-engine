"""XNYS pinned session calendar (BT5 / card D22).

The expected values here are **not** produced by the production code. Trading
dates, holiday dates and early closes are the published NYSE calendar; the 2025
trading-day count is derived independently in the test that asserts it.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone

import pytest

from backtest_engine.calendar import (
    XNYS_CALENDAR,
    XNYS_CALENDAR_VERSION,
    CalendarCoverageError,
    CalendarValidationError,
    PinnedSessionCalendar,
    SessionHours,
    SessionType,
)
from backtest_engine.event_clock import MarketEventClock, MarketSessionStatus


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _day(value: str) -> date:
    return date.fromisoformat(value)


def test_calendar_version_is_pinned_and_fits_the_canonical_column() -> None:
    # market_data.trading_sessions.calendar_version is varchar(40) and the
    # unique key is (exchange_mic, session_date, calendar_version).
    assert XNYS_CALENDAR_VERSION == "xnys-2016-2026:1.0.0"
    assert XNYS_CALENDAR.calendar_version == XNYS_CALENDAR_VERSION
    assert len(XNYS_CALENDAR.calendar_version) <= 40
    assert XNYS_CALENDAR.exchange_mic == "XNYS"
    assert XNYS_CALENDAR.covered_from == _day("2016-01-01")
    assert XNYS_CALENDAR.covered_through == _day("2026-12-31")


@pytest.mark.parametrize(
    ("session_date", "session_type", "opens_at", "closes_at"),
    [
        # Regular winter session: 09:30-16:00 EST = 14:30-21:00Z.
        ("2026-01-05", SessionType.REGULAR, "2026-01-05T14:30:00Z", "2026-01-05T21:00:00Z"),
        # Regular summer session: 09:30-16:00 EDT = 13:30-20:00Z.
        ("2026-07-06", SessionType.REGULAR, "2026-07-06T13:30:00Z", "2026-07-06T20:00:00Z"),
        # Friday after Thanksgiving 2025: 09:30-13:00 EST = 14:30-18:00Z.
        ("2025-11-28", SessionType.EARLY_CLOSE, "2025-11-28T14:30:00Z", "2025-11-28T18:00:00Z"),
        # Christmas Eve 2024 (Tuesday): 09:30-13:00 EST = 14:30-18:00Z.
        ("2024-12-24", SessionType.EARLY_CLOSE, "2024-12-24T14:30:00Z", "2024-12-24T18:00:00Z"),
        # 3 July 2025 (Thursday before Independence Day Friday): early close in EDT.
        ("2025-07-03", SessionType.EARLY_CLOSE, "2025-07-03T13:30:00Z", "2025-07-03T17:00:00Z"),
    ],
)
def test_open_sessions_carry_exact_dst_correct_boundaries(
    session_date: str, session_type: SessionType, opens_at: str, closes_at: str
) -> None:
    record = XNYS_CALENDAR.session_on(_day(session_date))

    assert record.session_type is session_type
    assert record.opens_at == _utc(opens_at)
    assert record.closes_at == _utc(closes_at)
    assert record.exchange_mic == "XNYS"
    assert record.calendar_version == XNYS_CALENDAR_VERSION


@pytest.mark.parametrize(
    "session_date",
    [
        "2024-03-29",  # Good Friday
        "2025-01-01",  # New Year's Day
        "2025-01-09",  # National Day of Mourning, President Carter
        "2025-11-27",  # Thanksgiving
        "2025-11-29",  # Saturday
        "2025-11-30",  # Sunday
        "2026-04-03",  # Good Friday
        "2026-07-03",  # Independence Day observed (4 July 2026 is a Saturday)
        "2026-11-26",  # Thanksgiving
    ],
)
def test_closed_dates_have_no_boundaries(session_date: str) -> None:
    record = XNYS_CALENDAR.session_on(_day(session_date))

    assert record.session_type is SessionType.CLOSED
    assert record.opens_at is None
    assert record.closes_at is None
    assert record.to_official_session() is None


def test_maximum_range_calendar_pins_pre_2024_one_off_closure() -> None:
    record = XNYS_CALENDAR.session_on(_day("2018-12-05"))

    assert record.session_type is SessionType.CLOSED
    assert record.opens_at is None
    assert record.closes_at is None


def test_thursday_before_a_saturday_independence_day_is_a_full_session() -> None:
    # 4 July 2026 is a Saturday, so the NYSE closes Friday 3 July and does not
    # add an early close on Thursday 2 July.
    record = XNYS_CALENDAR.session_on(_day("2026-07-02"))

    assert record.session_type is SessionType.REGULAR
    assert record.closes_at == _utc("2026-07-02T20:00:00Z")


@pytest.mark.parametrize(
    ("session_date", "opens_at"),
    [
        # US DST ends on the first Sunday of November 2025 (2 November).
        ("2025-10-31", "2025-10-31T13:30:00Z"),
        ("2025-11-03", "2025-11-03T14:30:00Z"),
        # US DST starts on the second Sunday of March 2026 (8 March).
        ("2026-03-06", "2026-03-06T14:30:00Z"),
        ("2026-03-09", "2026-03-09T13:30:00Z"),
    ],
)
def test_open_times_follow_the_et_dst_transitions(session_date: str, opens_at: str) -> None:
    assert XNYS_CALENDAR.session_on(_day(session_date)).opens_at == _utc(opens_at)


def test_2025_has_the_published_number_of_trading_days_and_early_closes() -> None:
    # 2025 has 261 weekdays (52 weeks x 5, plus Wednesday 1 January) and 11
    # weekday holidays: 1 Jan, 9 Jan, 20 Jan, 17 Feb, 18 Apr, 26 May, 19 Jun,
    # 4 Jul, 1 Sep, 27 Nov, 25 Dec. 261 - 11 = 250 trading days.
    records = XNYS_CALENDAR.records(_day("2025-01-01"), _day("2025-12-31"))

    assert len(records) == 365
    open_records = [item for item in records if item.session_type is not SessionType.CLOSED]
    assert len(open_records) == 250
    early = [item for item in open_records if item.session_type is SessionType.EARLY_CLOSE]
    assert [item.session_date for item in early] == [
        _day("2025-07-03"),
        _day("2025-11-28"),
        _day("2025-12-24"),
    ]


def test_2024_has_the_published_number_of_trading_days_and_early_closes() -> None:
    # 2024 is a leap year starting Monday and ending Tuesday: 52 x 5 + 2 = 262
    # weekdays, less 10 weekday holidays (1 Jan, 15 Jan, 19 Feb, 29 Mar, 27 May,
    # 19 Jun, 4 Jul, 2 Sep, 28 Nov, 25 Dec) = 252 trading days, the NYSE count.
    records = XNYS_CALENDAR.records(_day("2024-01-01"), _day("2024-12-31"))

    assert len(records) == 366
    open_records = [item for item in records if item.session_type is not SessionType.CLOSED]
    assert len(open_records) == 252
    early = [item for item in open_records if item.session_type is SessionType.EARLY_CLOSE]
    assert [item.session_date for item in early] == [
        _day("2024-07-03"),
        _day("2024-11-29"),
        _day("2024-12-24"),
    ]


def test_2026_has_the_published_number_of_trading_days_and_early_closes() -> None:
    # 2026 starts and ends on a Thursday: 52 x 5 + 1 = 261 weekdays, less 10
    # weekday holidays (1 Jan, 19 Jan, 16 Feb, 3 Apr, 25 May, 19 Jun, 3 Jul,
    # 7 Sep, 26 Nov, 25 Dec) = 251 trading days. Only two early closes: the
    # Independence Day holiday falls on Friday 3 July, so there is no July one.
    records = XNYS_CALENDAR.records(_day("2026-01-01"), _day("2026-12-31"))

    assert len(records) == 365
    open_records = [item for item in records if item.session_type is not SessionType.CLOSED]
    assert len(open_records) == 251
    early = [item for item in open_records if item.session_type is SessionType.EARLY_CLOSE]
    assert [item.session_date for item in early] == [
        _day("2026-11-27"),
        _day("2026-12-24"),
    ]


def test_records_are_canonical_trading_session_rows() -> None:
    row = XNYS_CALENDAR.session_on(_day("2025-11-28")).row()

    assert row == {
        "exchange_mic": "XNYS",
        "session_date": _day("2025-11-28"),
        "opens_at": _utc("2025-11-28T14:30:00Z"),
        "closes_at": _utc("2025-11-28T18:00:00Z"),
        "session_type": "EARLY_CLOSE",
        "calendar_version": "xnys-2016-2026:1.0.0",
    }


def test_unique_key_columns_are_unique_across_the_pinned_range() -> None:
    records = XNYS_CALENDAR.records()
    keys = {(item.exchange_mic, item.session_date, item.calendar_version) for item in records}

    assert len(keys) == len(records)
    assert len(records) == 4018


def test_dates_outside_the_pinned_coverage_fail_closed() -> None:
    with pytest.raises(CalendarCoverageError, match="2015-12-31"):
        XNYS_CALENDAR.session_on(_day("2015-12-31"))
    with pytest.raises(CalendarCoverageError, match="2027-01-04"):
        XNYS_CALENDAR.session_on(_day("2027-01-04"))
    with pytest.raises(CalendarCoverageError, match="coverage"):
        XNYS_CALENDAR.records(_day("2015-12-01"), _day("2016-01-05"))


def test_schedule_feeds_the_event_clock_with_matching_coverage() -> None:
    schedule = XNYS_CALENDAR.session_schedule(_day("2025-11-26"), _day("2025-12-01"))

    assert schedule.covered_from_et == _day("2025-11-26")
    assert schedule.covered_through_et == _day("2025-12-01")
    assert [item.trading_date_et for item in schedule.sessions] == [
        _day("2025-11-26"),
        _day("2025-11-28"),
        _day("2025-12-01"),
    ]

    clock = MarketEventClock(schedule, [])

    assert clock.advance_to(_utc("2025-11-27T15:00:00Z")).status is MarketSessionStatus.MARKET_CLOSED
    assert clock.advance_to(_utc("2025-11-28T17:59:59Z")).status is MarketSessionStatus.REGULAR_OPEN
    assert clock.advance_to(_utc("2025-11-28T18:00:00Z")).status is MarketSessionStatus.POST_MARKET
    assert (
        clock.advance_to(_utc("2025-12-02T15:00:00Z")).status
        is MarketSessionStatus.CALENDAR_UNAVAILABLE
    )


def test_session_status_outside_coverage_is_not_silently_closed() -> None:
    schedule = XNYS_CALENDAR.session_schedule(_day("2025-11-28"), _day("2025-11-28"))
    clock = MarketEventClock(schedule, [])

    assert clock.advance_to(_utc("2025-11-27T15:00:00Z")).status is (
        MarketSessionStatus.CALENDAR_UNAVAILABLE
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"exchange_mic": "NYSEX"}, "exchange_mic"),
        ({"calendar_version": "x" * 41}, "calendar_version"),
        ({"closed_dates": frozenset({_day("2030-01-01")})}, "outside"),
        ({"early_close_dates": frozenset({_day("2024-01-01")})}, "both closed and early-closing"),
        (
            {"early_close_hours": SessionHours(time(9, 30), time(16, 30))},
            "early close must precede",
        ),
        ({"covered_through": _day("2023-01-01")}, "coverage"),
    ],
)
def test_rejects_incoherent_pinned_calendar_data(kwargs: dict[str, object], message: str) -> None:
    base = {
        "exchange_mic": "XNYS",
        "calendar_version": "test:1.0.0",
        "timezone_name": "America/New_York",
        "covered_from": _day("2024-01-01"),
        "covered_through": _day("2024-12-31"),
        "regular_hours": SessionHours(time(9, 30), time(16, 0)),
        "early_close_hours": SessionHours(time(9, 30), time(13, 0)),
        "closed_dates": frozenset({_day("2024-01-01")}),
        "early_close_dates": frozenset({_day("2024-07-03")}),
    }
    base.update(kwargs)

    with pytest.raises(CalendarValidationError, match=message):
        PinnedSessionCalendar(**base)  # type: ignore[arg-type]


def test_official_sessions_are_utc_and_ordered() -> None:
    sessions = XNYS_CALENDAR.trading_sessions(_day("2024-12-23"), _day("2024-12-27"))

    assert [item.trading_date_et for item in sessions] == [
        _day("2024-12-23"),
        _day("2024-12-24"),
        _day("2024-12-26"),
        _day("2024-12-27"),
    ]
    assert all(item.opens_at.tzinfo == timezone.utc for item in sessions)
    assert sessions[1].closes_at == _utc("2024-12-24T18:00:00Z")
