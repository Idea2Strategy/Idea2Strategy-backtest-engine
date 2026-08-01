from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backtest_engine.event_clock import (
    EventClockValidationError,
    MarketDataEvent,
    MarketEventClock,
    MarketSessionStatus,
    OfficialSessionSchedule,
    OfficialTradingSession,
)


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _session(
    trading_date: str = "2025-11-28",
    opens_at: str = "2025-11-28T14:30:00Z",
    closes_at: str = "2025-11-28T18:00:00Z",
) -> OfficialTradingSession:
    return OfficialTradingSession(
        trading_date_et=date.fromisoformat(trading_date),
        opens_at=_utc(opens_at),
        closes_at=_utc(closes_at),
    )


def _event(
    event_id: str,
    occurred_at: str,
    available_at: str,
    source_sequence: int,
) -> MarketDataEvent:
    return MarketDataEvent(
        event_id=event_id,
        instrument_id="00000000-0000-4000-8000-000000000301",
        occurred_at=_utc(occurred_at),
        available_at=_utc(available_at),
        source_sequence=source_sequence,
        event_type="BAR_CLOSED",
        payload={"close": "100.00"},
    )


def _clock(
    sessions: list[OfficialTradingSession],
    events: list[MarketDataEvent],
    covered_from: str = "2025-11-28",
    covered_through: str = "2025-11-28",
) -> MarketEventClock:
    return MarketEventClock(
        OfficialSessionSchedule(
            covered_from_et=date.fromisoformat(covered_from),
            covered_through_et=date.fromisoformat(covered_through),
            sessions=tuple(sessions),
        ),
        events,
    )


def test_uses_supplied_dst_and_early_close_boundaries_with_close_exclusive() -> None:
    winter = OfficialTradingSession(
        date.fromisoformat("2026-01-05"),
        _utc("2026-01-05T14:30:00Z"),
        _utc("2026-01-05T21:00:00Z"),
    )
    summer = OfficialTradingSession(
        date.fromisoformat("2026-07-06"),
        _utc("2026-07-06T13:30:00Z"),
        _utc("2026-07-06T20:00:00Z"),
    )
    early_close = _session()
    clock = _clock(
        [summer, early_close, winter],
        [],
        covered_from="2025-11-28",
        covered_through="2026-07-06",
    )

    assert clock.advance_to(_utc("2026-01-05T14:29:59Z")).status is MarketSessionStatus.PRE_MARKET
    assert clock.advance_to(_utc("2026-01-05T14:30:00Z")).status is MarketSessionStatus.REGULAR_OPEN
    assert clock.advance_to(_utc("2026-01-05T20:59:59Z")).status is MarketSessionStatus.REGULAR_OPEN
    assert clock.advance_to(_utc("2026-01-05T21:00:00Z")).status is MarketSessionStatus.POST_MARKET

    later_clock = _clock(
        [summer, early_close],
        [],
        covered_from="2025-11-28",
        covered_through="2026-07-06",
    )
    assert later_clock.advance_to(_utc("2026-07-06T13:30:00Z")).status is MarketSessionStatus.REGULAR_OPEN
    assert later_clock.advance_to(_utc("2026-07-06T20:00:00Z")).status is MarketSessionStatus.POST_MARKET

    early_clock = _clock([early_close], [])
    assert early_clock.advance_to(_utc("2025-11-28T17:59:59Z")).status is MarketSessionStatus.REGULAR_OPEN
    assert early_clock.advance_to(_utc("2025-11-28T18:00:00Z")).status is MarketSessionStatus.POST_MARKET


def test_treats_dates_without_an_official_session_as_market_closed() -> None:
    clock = _clock(
        [_session()],
        [],
        covered_from="2025-11-27",
        covered_through="2025-11-28",
    )

    snapshot = clock.advance_to(_utc("2025-11-27T16:00:00Z"))

    assert snapshot.status is MarketSessionStatus.MARKET_CLOSED
    assert snapshot.session is None


def test_fails_closed_outside_the_pinned_calendar_coverage() -> None:
    clock = _clock([_session()], [])

    snapshot = clock.advance_to(_utc("2025-11-27T16:00:00Z"))

    assert snapshot.status is MarketSessionStatus.CALENDAR_UNAVAILABLE
    assert snapshot.session is None


def test_releases_only_available_events_in_deterministic_official_order() -> None:
    first = _event(
        "first",
        "2025-11-28T14:30:00Z",
        "2025-11-28T14:31:00Z",
        30,
    )
    same_time_second = _event(
        "same-time-second",
        "2025-11-28T14:31:00Z",
        "2025-11-28T14:32:00Z",
        20,
    )
    same_time_first = _event(
        "same-time-first",
        "2025-11-28T14:31:00Z",
        "2025-11-28T14:32:00Z",
        10,
    )
    clock = _clock(
        [_session()],
        [same_time_second, first, same_time_first],
    )

    before_available = clock.advance_to(_utc("2025-11-28T14:30:59Z"))
    first_release = clock.advance_to(_utc("2025-11-28T14:31:00Z"))
    second_release = clock.advance_to(_utc("2025-11-28T14:32:00Z"))

    assert before_available.released_events == ()
    assert before_available.visible_events == ()
    assert [event.event_id for event in first_release.released_events] == ["first"]
    assert [event.event_id for event in first_release.visible_events] == ["first"]
    assert [event.event_id for event in second_release.released_events] == [
        "same-time-first",
        "same-time-second",
    ]
    assert [event.event_id for event in second_release.visible_events] == [
        "first",
        "same-time-first",
        "same-time-second",
    ]


def test_advances_to_next_availability_without_exposing_later_events() -> None:
    first = _event(
        "first",
        "2025-11-28T14:30:00Z",
        "2025-11-28T14:31:00Z",
        1,
    )
    second = _event(
        "second",
        "2025-11-28T14:31:00Z",
        "2025-11-28T14:32:00Z",
        2,
    )
    clock = _clock([_session()], [second, first])

    first_snapshot = clock.advance_to_next_event()
    second_snapshot = clock.advance_to_next_event()

    assert first_snapshot is not None
    assert first_snapshot.now == first.available_at
    assert first_snapshot.released_events == (first,)
    assert first_snapshot.visible_events == (first,)
    assert second_snapshot is not None
    assert second_snapshot.now == second.available_at
    assert second_snapshot.released_events == (second,)
    assert second_snapshot.visible_events == (first, second)
    assert clock.advance_to_next_event() is None


def test_allows_last_bar_to_become_available_at_the_exclusive_close() -> None:
    last_bar = _event(
        "last-bar",
        "2025-11-28T17:59:00Z",
        "2025-11-28T18:00:00Z",
        1,
    )
    clock = _clock([_session()], [last_bar])

    before_close = clock.advance_to(_utc("2025-11-28T17:59:59Z"))
    at_close = clock.advance_to(_utc("2025-11-28T18:00:00Z"))

    assert before_close.released_events == ()
    assert at_close.status is MarketSessionStatus.POST_MARKET
    assert at_close.released_events == (last_bar,)


@pytest.mark.parametrize(
    ("event", "message"),
    [
        (
            _event(
                "look-ahead",
                "2025-11-28T14:32:00Z",
                "2025-11-28T14:31:00Z",
                1,
            ),
            "available_at must not precede occurred_at",
        ),
        (
            _event(
                "pre-market",
                "2025-11-28T14:29:59Z",
                "2025-11-28T14:30:00Z",
                1,
            ),
            "outside an official regular session",
        ),
        (
            _event(
                "at-close",
                "2025-11-28T18:00:00Z",
                "2025-11-28T18:00:00Z",
                1,
            ),
            "outside an official regular session",
        ),
    ],
)
def test_rejects_lookahead_and_events_outside_regular_session(
    event: MarketDataEvent, message: str
) -> None:
    with pytest.raises(EventClockValidationError, match=message):
        _clock([_session()], [event])


def test_rejects_duplicate_official_order_and_clock_reversal() -> None:
    first = _event(
        "first",
        "2025-11-28T14:30:00Z",
        "2025-11-28T14:31:00Z",
        7,
    )
    duplicate_order = _event(
        "other",
        "2025-11-28T14:30:30Z",
        "2025-11-28T14:31:00Z",
        7,
    )

    with pytest.raises(EventClockValidationError, match="official order"):
        _clock([_session()], [first, duplicate_order])

    clock = _clock([_session()], [first])
    clock.advance_to(_utc("2025-11-28T14:31:00Z"))
    with pytest.raises(EventClockValidationError, match="must not move backward"):
        clock.advance_to(_utc("2025-11-28T14:30:59Z"))


def test_rejects_session_whose_boundaries_do_not_match_its_et_date() -> None:
    with pytest.raises(EventClockValidationError, match="trading_date_et"):
        OfficialTradingSession(
            date.fromisoformat("2025-11-29"),
            _utc("2025-11-28T14:30:00Z"),
            _utc("2025-11-28T18:00:00Z"),
        )


def test_requires_timezone_aware_utc_clock_values() -> None:
    with pytest.raises(EventClockValidationError, match="timezone-aware"):
        OfficialTradingSession(
            date.fromisoformat("2025-11-28"),
            datetime(2025, 11, 28, 14, 30),
            datetime(2025, 11, 28, 18, 0),
        )

    aware = _utc("2025-11-28T14:31:00Z")
    assert aware.tzinfo == timezone.utc
