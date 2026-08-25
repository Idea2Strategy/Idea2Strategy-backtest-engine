"""The pinned XNYS (New York Stock Exchange) session calendar.

``backtest_engine.execution_policy.ExecutionPolicy.session_calendar`` is
``"XNYS"``. This module is the data that name resolves to.

Provenance and pinning
----------------------
The dates below are transcribed from the immutable market-data session evidence
for 2016 through July 2026 and the published remainder of 2026 and are **frozen**. They are never fetched at
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


XNYS_CALENDAR_VERSION = "xnys-2016-2026:1.0.0"
"""Written to ``market_data.trading_sessions.calendar_version`` (varchar(40))."""

XNYS_COVERED_FROM = date(2016, 1, 1)
XNYS_COVERED_THROUGH = date(2026, 12, 31)

XNYS_REGULAR_HOURS = SessionHours(opens=time(9, 30), closes=time(16, 0))
XNYS_EARLY_CLOSE_HOURS = SessionHours(opens=time(9, 30), closes=time(13, 0))


def _days(*values: str) -> tuple[date, ...]:
    return tuple(date.fromisoformat(value) for value in values)


XNYS_CLOSED_DATES = frozenset(
    _days(
        # -- 2016-2023: pinned from the stored 30-minute session evidence ----
        "2016-01-01", "2016-01-18", "2016-02-15", "2016-03-25", "2016-05-30",
        "2016-07-04", "2016-09-05", "2016-11-24", "2016-12-26",
        "2017-01-02", "2017-01-16", "2017-02-20", "2017-04-14", "2017-05-29",
        "2017-07-04", "2017-09-04", "2017-11-23", "2017-12-25",
        "2018-01-01", "2018-01-15", "2018-02-19", "2018-03-30", "2018-05-28",
        "2018-07-04", "2018-09-03", "2018-11-22", "2018-12-05", "2018-12-25",
        "2019-01-01", "2019-01-21", "2019-02-18", "2019-04-19", "2019-05-27",
        "2019-07-04", "2019-09-02", "2019-11-28", "2019-12-25",
        "2020-01-01", "2020-01-20", "2020-02-17", "2020-04-10", "2020-05-25",
        "2020-07-03", "2020-09-07", "2020-11-26", "2020-12-25",
        "2021-01-01", "2021-01-18", "2021-02-15", "2021-04-02", "2021-05-31",
        "2021-07-05", "2021-09-06", "2021-11-25", "2021-12-24",
        "2022-01-17", "2022-02-21", "2022-04-15", "2022-05-30", "2022-06-20",
        "2022-07-04", "2022-09-05", "2022-11-24", "2022-12-26",
        "2023-01-02", "2023-01-16", "2023-02-20", "2023-04-07", "2023-05-29",
        "2023-06-19", "2023-07-04", "2023-09-04", "2023-11-23", "2023-12-25",
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
        # -- 2016-2023: actual last stored bar begins at 12:30 ET ------------
        "2016-11-25",
        "2017-07-03", "2017-11-24",
        "2018-07-03", "2018-11-23", "2018-12-24",
        "2019-07-03", "2019-11-29", "2019-12-24",
        "2020-11-27", "2020-12-24",
        "2021-11-26",
        "2022-11-25",
        "2023-07-03", "2023-11-24",
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
