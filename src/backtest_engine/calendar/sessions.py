"""Pinned exchange session calendars.

``backtest.runs`` reproducibility requires that the session boundaries used by a
replay are *data*, not a computation performed at run time against a live
source. This module is the data structure; :mod:`backtest_engine.calendar.xnys`
is the pinned XNYS instance.

A calendar is identified by ``(exchange_mic, calendar_version)`` and emits one
row per calendar date, matching the canonical
``market_data.trading_sessions`` unique key
``(exchange_mic, session_date, calendar_version)`` in ``db/schema.dbml``:

===================  ==========================================================
``exchange_mic``     ``char(4)``
``session_date``     ``date``
``opens_at``         ``timestamptz``, NULL on a closed date
``closes_at``        ``timestamptz``, NULL on a closed date
``session_type``     ``REGULAR`` | ``EARLY_CLOSE`` | ``CLOSED``
``calendar_version`` ``varchar(40)``
===================  ==========================================================

Boundaries are declared as *local exchange wall-clock times* and converted with
the IANA zone, so daylight saving is handled by the zone database rather than by
a hand-written offset table. Nothing here reads the network or the clock.

Requests for a date outside the pinned coverage raise
:class:`CalendarCoverageError`. They are never answered with "closed": an
unpinned date is an unknown date, and a backtest that silently treats unknown as
closed produces a reproducible-looking but wrong result.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from enum import Enum
from zoneinfo import ZoneInfo

from backtest_engine.event_clock import OfficialSessionSchedule, OfficialTradingSession


__all__ = [
    "CalendarCoverageError",
    "CalendarValidationError",
    "PinnedSessionCalendar",
    "SessionHours",
    "SessionRecord",
    "SessionType",
]


CALENDAR_VERSION_MAX_LENGTH = 40
"""``market_data.trading_sessions.calendar_version`` is ``varchar(40)``."""

_SATURDAY = 5


class CalendarValidationError(ValueError):
    """Raised when pinned calendar data cannot describe a reproducible session."""


class CalendarCoverageError(CalendarValidationError):
    """Raised when a date is outside the calendar's pinned coverage."""


class SessionType(str, Enum):
    """``market_data.trading_sessions.session_type`` values used by this repo."""

    REGULAR = "REGULAR"
    EARLY_CLOSE = "EARLY_CLOSE"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class SessionHours:
    """Local exchange wall-clock open and close of a regular trading session."""

    opens: time
    closes: time

    def __post_init__(self) -> None:
        for label, value in (("opens", self.opens), ("closes", self.closes)):
            if not isinstance(value, time) or value.tzinfo is not None:
                raise CalendarValidationError(
                    f"SessionHours.{label} must be a naive local wall-clock time"
                )
        if self.opens >= self.closes:
            raise CalendarValidationError("SessionHours.opens must precede closes")


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """One pinned ``market_data.trading_sessions`` row."""

    exchange_mic: str
    session_date: date
    opens_at: datetime | None
    closes_at: datetime | None
    session_type: SessionType
    calendar_version: str

    def __post_init__(self) -> None:
        closed = self.session_type is SessionType.CLOSED
        if closed != (self.opens_at is None):
            raise CalendarValidationError(
                "opens_at must be NULL exactly on a CLOSED session_date"
            )
        if closed != (self.closes_at is None):
            raise CalendarValidationError(
                "closes_at must be NULL exactly on a CLOSED session_date"
            )

    def row(self) -> dict[str, object]:
        """The canonical column mapping, ready for ``market_data.trading_sessions``."""
        return {
            "exchange_mic": self.exchange_mic,
            "session_date": self.session_date,
            "opens_at": self.opens_at,
            "closes_at": self.closes_at,
            "session_type": self.session_type.value,
            "calendar_version": self.calendar_version,
        }

    def to_official_session(self) -> OfficialTradingSession | None:
        """The :mod:`backtest_engine.event_clock` view, or ``None`` when closed."""
        if self.opens_at is None or self.closes_at is None:
            return None
        return OfficialTradingSession(
            trading_date_et=self.session_date,
            opens_at=self.opens_at,
            closes_at=self.closes_at,
        )


def _dates(first: date, last: date) -> Iterator[date]:
    current = first
    while current <= last:
        yield current
        current += timedelta(days=1)


@dataclass(frozen=True, slots=True)
class PinnedSessionCalendar:
    """An immutable, fully enumerable session calendar for one exchange.

    ``closed_dates`` and ``early_close_dates`` are the pinned data; every other
    weekday in the coverage window is a regular session. Weekends are closed by
    definition and are not listed.
    """

    exchange_mic: str
    calendar_version: str
    timezone_name: str
    covered_from: date
    covered_through: date
    regular_hours: SessionHours
    early_close_hours: SessionHours
    closed_dates: frozenset[date]
    early_close_dates: frozenset[date]

    def __post_init__(self) -> None:
        if not isinstance(self.exchange_mic, str) or len(self.exchange_mic) != 4:
            raise CalendarValidationError(
                f"exchange_mic must be a 4-character MIC, got {self.exchange_mic!r}"
            )
        if (
            not isinstance(self.calendar_version, str)
            or not self.calendar_version
            or len(self.calendar_version) > CALENDAR_VERSION_MAX_LENGTH
        ):
            raise CalendarValidationError(
                "calendar_version must be 1-"
                f"{CALENDAR_VERSION_MAX_LENGTH} characters, got "
                f"{self.calendar_version!r}"
            )
        if self.covered_from > self.covered_through:
            raise CalendarValidationError(
                "calendar coverage must not end before it starts: "
                f"{self.covered_from} > {self.covered_through}"
            )
        if self.early_close_hours.closes >= self.regular_hours.closes:
            raise CalendarValidationError(
                "an early close must precede the regular close: "
                f"{self.early_close_hours.closes} >= {self.regular_hours.closes}"
            )
        if self.early_close_hours.opens != self.regular_hours.opens:
            raise CalendarValidationError(
                "an early-closing session must open at the regular open"
            )
        overlap = self.closed_dates & self.early_close_dates
        if overlap:
            raise CalendarValidationError(
                "dates cannot be both closed and early-closing: "
                + ", ".join(item.isoformat() for item in sorted(overlap))
            )
        for label, values in (
            ("closed_dates", self.closed_dates),
            ("early_close_dates", self.early_close_dates),
        ):
            outside = sorted(
                item
                for item in values
                if not self.covered_from <= item <= self.covered_through
            )
            if outside:
                raise CalendarValidationError(
                    f"{label} outside the pinned coverage: "
                    + ", ".join(item.isoformat() for item in outside)
                )
            weekend = sorted(item for item in values if item.weekday() >= _SATURDAY)
            if weekend:
                raise CalendarValidationError(
                    f"{label} must not list weekend dates: "
                    + ", ".join(item.isoformat() for item in weekend)
                )
        # Fail here rather than on the first lookup if the zone is unavailable.
        ZoneInfo(self.timezone_name)

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone_name)

    def covers(self, session_date: date) -> bool:
        return self.covered_from <= session_date <= self.covered_through

    def session_type_on(self, session_date: date) -> SessionType:
        self._require_covered(session_date)
        if session_date.weekday() >= _SATURDAY or session_date in self.closed_dates:
            return SessionType.CLOSED
        if session_date in self.early_close_dates:
            return SessionType.EARLY_CLOSE
        return SessionType.REGULAR

    def session_on(self, session_date: date) -> SessionRecord:
        """The pinned row for one date. Raises outside the coverage window."""
        session_type = self.session_type_on(session_date)
        if session_type is SessionType.CLOSED:
            return SessionRecord(
                exchange_mic=self.exchange_mic,
                session_date=session_date,
                opens_at=None,
                closes_at=None,
                session_type=session_type,
                calendar_version=self.calendar_version,
            )
        hours = (
            self.early_close_hours
            if session_type is SessionType.EARLY_CLOSE
            else self.regular_hours
        )
        return SessionRecord(
            exchange_mic=self.exchange_mic,
            session_date=session_date,
            opens_at=self._instant(session_date, hours.opens),
            closes_at=self._instant(session_date, hours.closes),
            session_type=session_type,
            calendar_version=self.calendar_version,
        )

    def records(
        self, first: date | None = None, last: date | None = None
    ) -> tuple[SessionRecord, ...]:
        """Every row, closed dates included, for an inclusive date range."""
        first, last = self._range(first, last)
        return tuple(self.session_on(day) for day in _dates(first, last))

    def trading_sessions(
        self, first: date | None = None, last: date | None = None
    ) -> tuple[OfficialTradingSession, ...]:
        """Only the dates on which the exchange opens, in chronological order."""
        first, last = self._range(first, last)
        sessions = []
        for day in _dates(first, last):
            session = self.session_on(day).to_official_session()
            if session is not None:
                sessions.append(session)
        return tuple(sessions)

    def session_schedule(
        self, first: date | None = None, last: date | None = None
    ) -> OfficialSessionSchedule:
        """The :class:`~backtest_engine.event_clock.OfficialSessionSchedule` view.

        Coverage is reported as the requested range, not as the range that
        happens to contain sessions, so the event clock reports
        ``CALENDAR_UNAVAILABLE`` outside it instead of ``MARKET_CLOSED``.
        """
        first, last = self._range(first, last)
        return OfficialSessionSchedule(
            covered_from_et=first,
            covered_through_et=last,
            sessions=self.trading_sessions(first, last),
        )

    def _range(self, first: date | None, last: date | None) -> tuple[date, date]:
        resolved_first = self.covered_from if first is None else first
        resolved_last = self.covered_through if last is None else last
        if resolved_first > resolved_last:
            raise CalendarCoverageError(
                f"requested range ends before it starts: {resolved_first} > {resolved_last}"
            )
        self._require_covered(resolved_first)
        self._require_covered(resolved_last)
        return resolved_first, resolved_last

    def _require_covered(self, session_date: date) -> None:
        if not isinstance(session_date, date) or isinstance(session_date, datetime):
            raise CalendarValidationError("session_date must be a date, not a datetime")
        if not self.covers(session_date):
            raise CalendarCoverageError(
                f"{session_date.isoformat()} is outside the pinned "
                f"{self.exchange_mic} coverage "
                f"{self.covered_from.isoformat()}..{self.covered_through.isoformat()} "
                f"({self.calendar_version})"
            )

    def _instant(self, session_date: date, local_time: time) -> datetime:
        local = datetime.combine(session_date, local_time, tzinfo=self.zone)
        return local.astimezone(timezone.utc)
