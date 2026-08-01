"""Deterministic ET-session clock for look-ahead-safe backtest events."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from enum import Enum
from types import MappingProxyType
from typing import Any
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


class EventClockValidationError(ValueError):
    """Raised when a pinned session or event stream is not replay-safe."""


class MarketSessionStatus(str, Enum):
    PRE_MARKET = "PRE_MARKET"
    REGULAR_OPEN = "REGULAR_OPEN"
    POST_MARKET = "POST_MARKET"
    MARKET_CLOSED = "MARKET_CLOSED"
    CALENDAR_UNAVAILABLE = "CALENDAR_UNAVAILABLE"


def _utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise EventClockValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise EventClockValidationError(f"{label} must be a non-empty string")
    return value


@dataclass(frozen=True, slots=True)
class OfficialTradingSession:
    """One immutable official regular session, including early closes."""

    trading_date_et: date
    opens_at: datetime
    closes_at: datetime

    def __post_init__(self) -> None:
        opens_at = _utc(self.opens_at, "opens_at")
        closes_at = _utc(self.closes_at, "closes_at")
        if opens_at >= closes_at:
            raise EventClockValidationError("opens_at must precede closes_at")
        if (
            opens_at.astimezone(ET).date() != self.trading_date_et
            or closes_at.astimezone(ET).date() != self.trading_date_et
        ):
            raise EventClockValidationError(
                "session boundaries must match trading_date_et"
            )
        object.__setattr__(self, "opens_at", opens_at)
        object.__setattr__(self, "closes_at", closes_at)

    def contains(self, instant: datetime) -> bool:
        assessed_at = _utc(instant, "instant")
        return self.opens_at <= assessed_at < self.closes_at


@dataclass(frozen=True, slots=True)
class OfficialSessionSchedule:
    """Pinned official-calendar coverage and its regular sessions."""

    covered_from_et: date
    covered_through_et: date
    sessions: tuple[OfficialTradingSession, ...]

    def __post_init__(self) -> None:
        if self.covered_from_et > self.covered_through_et:
            raise EventClockValidationError(
                "covered_from_et must not follow covered_through_et"
            )
        sessions = tuple(self.sessions)
        if any(not isinstance(session, OfficialTradingSession) for session in sessions):
            raise EventClockValidationError(
                "sessions must contain OfficialTradingSession values"
            )
        object.__setattr__(self, "sessions", sessions)


@dataclass(frozen=True, slots=True)
class MarketDataEvent:
    """A market fact separated into occurrence and safe availability times."""

    event_id: str
    instrument_id: str
    occurred_at: datetime
    available_at: datetime
    source_sequence: int
    event_type: str
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        _text(self.event_id, "event_id")
        instrument_id = _text(self.instrument_id, "instrument_id")
        try:
            instrument_id = str(uuid.UUID(instrument_id))
        except ValueError as exc:
            raise EventClockValidationError("instrument_id must be a UUID") from exc
        _text(self.event_type, "event_type")
        if not isinstance(self.source_sequence, int) or self.source_sequence < 1:
            raise EventClockValidationError("source_sequence must be positive")
        if not isinstance(self.payload, Mapping):
            raise EventClockValidationError("payload must be an object")
        object.__setattr__(self, "instrument_id", instrument_id)
        object.__setattr__(self, "occurred_at", _utc(self.occurred_at, "occurred_at"))
        object.__setattr__(self, "available_at", _utc(self.available_at, "available_at"))
        object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))


@dataclass(frozen=True, slots=True)
class MarketClockSnapshot:
    now: datetime
    status: MarketSessionStatus
    session: OfficialTradingSession | None
    released_events: tuple[MarketDataEvent, ...]
    visible_events: tuple[MarketDataEvent, ...]


class MarketEventClock:
    """Advances monotonically and reveals only events available by that instant."""

    def __init__(
        self,
        schedule: OfficialSessionSchedule,
        events: Iterable[MarketDataEvent],
    ) -> None:
        if not isinstance(schedule, OfficialSessionSchedule):
            raise EventClockValidationError(
                "schedule must be an OfficialSessionSchedule"
            )
        self._schedule = schedule
        ordered_sessions = tuple(
            sorted(schedule.sessions, key=lambda item: item.opens_at)
        )
        self._sessions_by_date: dict[date, OfficialTradingSession] = {}
        previous: OfficialTradingSession | None = None
        for session in ordered_sessions:
            if not isinstance(session, OfficialTradingSession):
                raise EventClockValidationError(
                    "sessions must contain OfficialTradingSession values"
                )
            if session.trading_date_et in self._sessions_by_date:
                raise EventClockValidationError(
                    "official sessions must have unique trading_date_et values"
                )
            if not (
                schedule.covered_from_et
                <= session.trading_date_et
                <= schedule.covered_through_et
            ):
                raise EventClockValidationError(
                    "official session is outside calendar coverage"
                )
            if previous is not None and session.opens_at < previous.closes_at:
                raise EventClockValidationError("official sessions must not overlap")
            self._sessions_by_date[session.trading_date_et] = session
            previous = session

        supplied_events = tuple(events)
        self._validate_events(supplied_events)
        self._events = tuple(
            sorted(
                supplied_events,
                key=lambda event: (
                    event.available_at,
                    event.source_sequence,
                    event.event_id,
                ),
            )
        )
        self._cursor = 0
        self._now: datetime | None = None
        self._visible: list[MarketDataEvent] = []

    def _validate_events(self, events: tuple[MarketDataEvent, ...]) -> None:
        event_ids: set[str] = set()
        official_order: set[tuple[datetime, int]] = set()
        for event in events:
            if not isinstance(event, MarketDataEvent):
                raise EventClockValidationError(
                    "events must contain MarketDataEvent values"
                )
            if event.event_id in event_ids:
                raise EventClockValidationError("event_id values must be unique")
            event_ids.add(event.event_id)
            if event.available_at < event.occurred_at:
                raise EventClockValidationError(
                    "available_at must not precede occurred_at"
                )
            order_key = (event.available_at, event.source_sequence)
            if order_key in official_order:
                raise EventClockValidationError(
                    "events must have a unique official order"
                )
            official_order.add(order_key)

            trading_date = event.occurred_at.astimezone(ET).date()
            session = self._sessions_by_date.get(trading_date)
            if session is None or not session.contains(event.occurred_at):
                raise EventClockValidationError(
                    "event occurred_at is outside an official regular session"
                )

    def advance_to(self, instant: datetime) -> MarketClockSnapshot:
        target = _utc(instant, "instant")
        if self._now is not None and target < self._now:
            raise EventClockValidationError("simulation clock must not move backward")

        released: list[MarketDataEvent] = []
        while (
            self._cursor < len(self._events)
            and self._events[self._cursor].available_at <= target
        ):
            event = self._events[self._cursor]
            released.append(event)
            self._visible.append(event)
            self._cursor += 1
        self._now = target

        session, status = self._assess_session(target)
        return MarketClockSnapshot(
            now=target,
            status=status,
            session=session,
            released_events=tuple(released),
            visible_events=tuple(self._visible),
        )

    def advance_to_next_event(self) -> MarketClockSnapshot | None:
        if self._cursor >= len(self._events):
            return None
        return self.advance_to(self._events[self._cursor].available_at)

    def _assess_session(
        self, assessed_at: datetime
    ) -> tuple[OfficialTradingSession | None, MarketSessionStatus]:
        market_date = assessed_at.astimezone(ET).date()
        if not (
            self._schedule.covered_from_et
            <= market_date
            <= self._schedule.covered_through_et
        ):
            return None, MarketSessionStatus.CALENDAR_UNAVAILABLE
        session = self._sessions_by_date.get(market_date)
        if session is None:
            return None, MarketSessionStatus.MARKET_CLOSED
        if assessed_at < session.opens_at:
            return session, MarketSessionStatus.PRE_MARKET
        if assessed_at < session.closes_at:
            return session, MarketSessionStatus.REGULAR_OPEN
        return session, MarketSessionStatus.POST_MARKET
