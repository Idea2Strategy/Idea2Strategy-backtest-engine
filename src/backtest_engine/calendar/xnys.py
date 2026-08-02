"""The pinned XNYS (New York Stock Exchange) session calendar.

``backtest_engine.execution_policy.ExecutionPolicy.session_calendar`` is
``"XNYS"``. This module is the data that name resolves to.

Provenance and pinning
----------------------
The dates below are transcribed from the NYSE published holiday and early-close
schedule for 2024, 2025 and 2026 and are **frozen**. They are never fetched at
run time. Correcting or extending them is a new ``calendar_version``, never an
edit of an existing one, because
``market_data.trading_sessions (exchange_mic, session_date, calendar_version)``
is the reproducibility key for every replay that already ran.

Holiday rules the pinned dates implement (all observed dates, i.e. a Saturday
holiday moves to the preceding Friday and a Sunday holiday to the following
Monday):

* New Year's Day - 1 January
* Martin Luther King Jr. Day - third Monday of January
* Washington's Birthday - third Monday of February
* Good Friday - the Friday before Easter Sunday
* Memorial Day - last Monday of May
* Juneteenth National Independence Day - 19 June
* Independence Day - 4 July
* Labor Day - first Monday of September
* Thanksgiving Day - fourth Thursday of November
* Christmas Day - 25 December

Plus one non-recurring closure: Thursday 9 January 2025, the National Day of
Mourning for President Jimmy Carter.

Early closes are 13:00 ET and follow the NYSE convention:

* the trading day before Independence Day, when Independence Day itself is a
  regular trading day (so 2026 has none: 4 July 2026 is a Saturday, the holiday
  is observed on Friday 3 July, and no early close is added on Thursday 2 July)
* the Friday after Thanksgiving
* Christmas Eve, when it is a weekday and Christmas Day is a trading day

Session hours are 09:30-16:00 ET (13:00 ET on an early close). ET daylight
saving is applied by ``zoneinfo``/``tzdata``, not by a stored UTC offset, so the
16:00 ET close is 21:00Z in winter and 20:00Z in summer.
"""

from __future__ import annotations

from datetime import date, time

from backtest_engine.calendar.sessions import PinnedSessionCalendar, SessionHours


__all__ = [
    "XNYS_CALENDAR",
    "XNYS_CALENDAR_VERSION",
    "XNYS_CLOSED_DATES",
    "XNYS_COVERED_FROM",
    "XNYS_COVERED_THROUGH",
    "XNYS_EARLY_CLOSE_DATES",
    "XNYS_EARLY_CLOSE_HOURS",
    "XNYS_REGULAR_HOURS",
]


XNYS_CALENDAR_VERSION = "xnys-2024-2026:1.0.0"
"""Written to ``market_data.trading_sessions.calendar_version`` (varchar(40))."""

XNYS_COVERED_FROM = date(2024, 1, 1)
XNYS_COVERED_THROUGH = date(2026, 12, 31)

XNYS_REGULAR_HOURS = SessionHours(opens=time(9, 30), closes=time(16, 0))
XNYS_EARLY_CLOSE_HOURS = SessionHours(opens=time(9, 30), closes=time(13, 0))


def _days(*values: str) -> tuple[date, ...]:
    return tuple(date.fromisoformat(value) for value in values)


XNYS_CLOSED_DATES = frozenset(
    _days(
        # -- 2024 -------------------------------------------------------------
        "2024-01-01",  # New Year's Day (Monday)
        "2024-01-15",  # Martin Luther King Jr. Day
        "2024-02-19",  # Washington's Birthday
        "2024-03-29",  # Good Friday (Easter 31 March 2024)
        "2024-05-27",  # Memorial Day
        "2024-06-19",  # Juneteenth (Wednesday)
        "2024-07-04",  # Independence Day (Thursday)
        "2024-09-02",  # Labor Day
        "2024-11-28",  # Thanksgiving Day
        "2024-12-25",  # Christmas Day (Wednesday)
        # -- 2025 -------------------------------------------------------------
        "2025-01-01",  # New Year's Day (Wednesday)
        "2025-01-09",  # National Day of Mourning, President Jimmy Carter
        "2025-01-20",  # Martin Luther King Jr. Day
        "2025-02-17",  # Washington's Birthday
        "2025-04-18",  # Good Friday (Easter 20 April 2025)
        "2025-05-26",  # Memorial Day
        "2025-06-19",  # Juneteenth (Thursday)
        "2025-07-04",  # Independence Day (Friday)
        "2025-09-01",  # Labor Day
        "2025-11-27",  # Thanksgiving Day
        "2025-12-25",  # Christmas Day (Thursday)
        # -- 2026 -------------------------------------------------------------
        "2026-01-01",  # New Year's Day (Thursday)
        "2026-01-19",  # Martin Luther King Jr. Day
        "2026-02-16",  # Washington's Birthday
        "2026-04-03",  # Good Friday (Easter 5 April 2026)
        "2026-05-25",  # Memorial Day
        "2026-06-19",  # Juneteenth (Friday)
        "2026-07-03",  # Independence Day observed (4 July 2026 is a Saturday)
        "2026-09-07",  # Labor Day
        "2026-11-26",  # Thanksgiving Day
        "2026-12-25",  # Christmas Day (Friday)
    )
)

XNYS_EARLY_CLOSE_DATES = frozenset(
    _days(
        # -- 2024 -------------------------------------------------------------
        "2024-07-03",  # day before Independence Day (Thursday 4 July)
        "2024-11-29",  # Friday after Thanksgiving
        "2024-12-24",  # Christmas Eve (Tuesday)
        # -- 2025 -------------------------------------------------------------
        "2025-07-03",  # day before Independence Day (Friday 4 July)
        "2025-11-28",  # Friday after Thanksgiving
        "2025-12-24",  # Christmas Eve (Wednesday)
        # -- 2026 -------------------------------------------------------------
        # No July early close: the 2026 Independence Day holiday is observed on
        # Friday 3 July, so Thursday 2 July is a full session.
        "2026-11-27",  # Friday after Thanksgiving
        "2026-12-24",  # Christmas Eve (Thursday)
    )
)

XNYS_CALENDAR = PinnedSessionCalendar(
    exchange_mic="XNYS",
    calendar_version=XNYS_CALENDAR_VERSION,
    timezone_name="America/New_York",
    covered_from=XNYS_COVERED_FROM,
    covered_through=XNYS_COVERED_THROUGH,
    regular_hours=XNYS_REGULAR_HOURS,
    early_close_hours=XNYS_EARLY_CLOSE_HOURS,
    closed_dates=XNYS_CLOSED_DATES,
    early_close_dates=XNYS_EARLY_CLOSE_DATES,
)
