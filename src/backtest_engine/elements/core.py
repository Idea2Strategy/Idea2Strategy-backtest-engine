"""Shared types for the Basic element evaluators.

These live below :mod:`backtest_engine.basic_runtime` so the runtime can import
the evaluators without the evaluators importing the runtime.

Failure model
-------------
Three, and only three, outcomes can leave an element:

``StepOutcome(is_passed=True)``
    The step's meaning held.
``StepOutcome(is_passed=False)``
    The step's meaning did not hold. The runtime turns this into
    ``CONDITION_NOT_MET`` and keeps the element's own ``reason_code``.
:class:`ElementInputMissing`
    The pinned input the element needs does not exist for this instrument. The
    runtime turns this into ``INPUT_MISSING`` with C's literal reason code
    ``INSTRUMENT_INPUT_MISSING`` and records the specific cause
    (``input_reason``) in the step trace, so the contract-level code stays
    C-compatible without hiding why.
:class:`ElementEvaluationError`
    Anything else. The runtime turns this into ``CONDITION_ERROR`` with C's
    literal reason code ``CONDITION_EVALUATION_ERROR``.

A missing input is deliberately *not* reported as "condition false": a strategy
whose data never arrived did not decline to trade, and collapsing the two would
make an unavailable dataset look like a valid negative result.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Protocol
from uuid import UUID


__all__ = [
    "ISO8601_RESOLUTIONS",
    "RESOLUTION_PERIODS",
    "SUPPORTED_RESOLUTIONS",
    "ElementCompatibilityError",
    "ElementError",
    "ElementEvaluation",
    "ElementEvaluationError",
    "ElementInputMissing",
    "InstrumentInput",
    "InstrumentSeries",
    "PinnedFeatureSeries",
    "PinnedFeatureValue",
    "PlanLoadFailure",
    "PlanStep",
    "SeriesBar",
    "StepEvaluator",
    "StepOutcome",
    "bar_resolution",
    "resolution_period",
]


CONDITION_ERROR_REASON = "CONDITION_EVALUATION_ERROR"
"""``BasicStrategyExecutor.CONDITION_ERROR`` in the trading runtime."""

INPUT_MISSING_REASON = "INSTRUMENT_INPUT_MISSING"
"""``BasicStrategyExecutor.INPUT_MISSING`` in the trading runtime."""


RESOLUTION_PERIODS: Mapping[str, timedelta] = MappingProxyType(
    {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
)
"""Normalized periods this build accepts from compiled plans and datasets.

``1d`` is the backend contract's normalized ``PT24H`` token. Its market-data
completion boundary is still aligned to the official session by the
orchestrator; the fixed duration here is used only for plan checksums and
bar-count warm-up arithmetic.
"""

SUPPORTED_RESOLUTIONS: tuple[str, ...] = tuple(RESOLUTION_PERIODS)


ISO8601_RESOLUTIONS: Mapping[str, str] = MappingProxyType(
    {
        "PT1M": "1m",
        "PT5M": "5m",
        "PT15M": "15m",
        "PT30M": "30m",
        "PT1H": "1h",
        "PT4H": "4h",
        "PT24H": "1d",
    }
)
"""B's ``requiredFeature.resolution`` vocabulary mapped to this build's tokens.

B declares a data-requirement resolution as a *normalized* ISO-8601 duration
(``java.time.Duration.parse(value).toString().equals(value)``), while a
``LOAD_FEATURE`` step argument uses the short element-catalog token. The two
vocabularies are both B's and both authoritative; this table is the only place
they are related, and it is a lookup, not a parser. ``PT60M`` and ``PT1H`` are
the same duration but only ``PT1H`` is normalized, so only ``PT1H`` appears -
accepting both would let two different documents carry the same meaning and
therefore two different ``planChecksum`` values for one plan.
"""

_UNMAPPED_RESOLUTIONS = set(ISO8601_RESOLUTIONS.values()) - set(RESOLUTION_PERIODS)
if _UNMAPPED_RESOLUTIONS:  # pragma: no cover - guards a bad edit to the tables above
    raise RuntimeError(
        "ISO8601_RESOLUTIONS maps to tokens with no period: "
        + ", ".join(sorted(_UNMAPPED_RESOLUTIONS))
    )


class PlanLoadFailure(str, Enum):
    """Why a compiled plan could not be loaded.

    The first three mirror
    ``com.idea2strategy.trading.strategy.runtime.plan.ExecutionPlanLoadFailure``
    one-for-one so the two runtimes report the same condition by the same name
    (card D92).
    """

    PLAN_SCHEMA_VERSION_MISMATCH = "PLAN_SCHEMA_VERSION_MISMATCH"
    FEATURE_VERSION_MISMATCH = "FEATURE_VERSION_MISMATCH"
    RUNTIME_SCHEMA_VERSION_MISMATCH = "RUNTIME_SCHEMA_VERSION_MISMATCH"
    # D-specific: the trading runtime receives an already-adapted plan snapshot
    # and therefore has no equivalent of these.
    PLAN_CONTRACT_INVALID = "PLAN_CONTRACT_INVALID"
    PLAN_INTEGRITY_MISMATCH = "PLAN_INTEGRITY_MISMATCH"
    PLAN_STRUCTURE_INVALID = "PLAN_STRUCTURE_INVALID"
    COMPILER_VERSION_MISMATCH = "COMPILER_VERSION_MISMATCH"
    ELEMENT_CATALOG_VERSION_UNSUPPORTED = "ELEMENT_CATALOG_VERSION_UNSUPPORTED"
    INSTRUMENT_CATALOG_VERSION_UNSUPPORTED = "INSTRUMENT_CATALOG_VERSION_UNSUPPORTED"
    UNSUPPORTED_ELEMENT = "UNSUPPORTED_ELEMENT"
    UNSUPPORTED_ELEMENT_ARGUMENT = "UNSUPPORTED_ELEMENT_ARGUMENT"
    UNSUPPORTED_FEATURE = "UNSUPPORTED_FEATURE"


class ElementError(Exception):
    """Base class for every typed element failure."""

    reason_code = CONDITION_ERROR_REASON

    def __init__(self, message: str, evidence: Mapping[str, str] | None = None) -> None:
        super().__init__(message)
        self.evidence: Mapping[str, str] = MappingProxyType(dict(evidence or {}))


class ElementEvaluationError(ElementError):
    """The element could not be evaluated. Maps to ``CONDITION_ERROR``."""


class ElementInputMissing(ElementError):
    """A required pinned input is absent. Maps to ``INPUT_MISSING``."""

    reason_code = INPUT_MISSING_REASON

    def __init__(
        self,
        message: str,
        *,
        input_reason: str,
        evidence: Mapping[str, str] | None = None,
    ) -> None:
        if not input_reason:
            raise ValueError("input_reason must not be empty")
        super().__init__(message, evidence)
        self.input_reason = input_reason


class ElementCompatibilityError(ValueError):
    """An element, argument or catalog version this build does not implement."""

    def __init__(self, failure: PlanLoadFailure, detail: str) -> None:
        super().__init__(f"{failure.value}: {detail}")
        self.failure = failure
        self.detail = detail


def bar_resolution(iso_duration: str) -> str:
    """Translate B's normalized ISO-8601 requirement resolution to a bar token."""
    try:
        return ISO8601_RESOLUTIONS[iso_duration]
    except KeyError as exc:
        raise ElementCompatibilityError(
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            f"requiredFeature.resolution {iso_duration!r} is not one of "
            + ", ".join(sorted(ISO8601_RESOLUTIONS)),
        ) from exc


def resolution_period(resolution: str) -> timedelta:
    """The exact bar duration of a supported resolution token."""
    try:
        return RESOLUTION_PERIODS[resolution]
    except KeyError as exc:
        raise ElementCompatibilityError(
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            f"resolution {resolution!r} is not one of "
            + ", ".join(SUPPORTED_RESOLUTIONS),
        ) from exc


def _utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ElementEvaluationError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _uuid(value: str, label: str) -> str:
    try:
        return str(UUID(value))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ElementEvaluationError(f"{label} must be a UUID") from exc


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, Decimal) or not value.is_finite():
        raise ElementEvaluationError(f"{label} must be a finite Decimal")
    return value


@dataclass(frozen=True, slots=True)
class StepOutcome:
    """One step's verdict. Structurally identical to C's ``BasicConditionOutcome``."""

    is_passed: bool
    reason_code: str
    evidence: Mapping[str, str]

    def __post_init__(self) -> None:
        if not self.reason_code:
            raise ValueError("reason_code must not be empty")
        evidence = dict(self.evidence)
        if any(not isinstance(value, str) for value in evidence.values()):
            raise ValueError("evidence values must be strings")
        object.__setattr__(self, "evidence", MappingProxyType(evidence))

    @classmethod
    def passed(
        cls, reason_code: str, evidence: Mapping[str, str] | None = None
    ) -> StepOutcome:
        return cls(True, reason_code, evidence or {})

    @classmethod
    def failed(
        cls, reason_code: str, evidence: Mapping[str, str] | None = None
    ) -> StepOutcome:
        return cls(False, reason_code, evidence or {})


@dataclass(frozen=True, slots=True)
class PlanStep:
    """One entry of B's flat ``steps[]`` array.

    ``arguments`` values are strings: the ``strategy-bot.v1`` schema declares
    ``arguments`` as an object of strings, so every numeric argument is parsed
    here rather than trusted as a JSON number.
    """

    sequence: int
    operation: str
    arguments: Mapping[str, str]

    def __post_init__(self) -> None:
        if not isinstance(self.sequence, int) or isinstance(self.sequence, bool):
            raise ElementEvaluationError("step sequence must be an integer")
        if self.sequence < 1:
            raise ElementEvaluationError("step sequence must be positive")
        if not self.operation:
            raise ElementEvaluationError("step operation must not be empty")
        arguments = dict(self.arguments)
        if any(not isinstance(value, str) for value in arguments.values()):
            raise ElementEvaluationError("step arguments must be strings")
        object.__setattr__(self, "arguments", MappingProxyType(arguments))

    def argument(self, name: str) -> str:
        try:
            return self.arguments[name]
        except KeyError as exc:
            raise ElementCompatibilityError(
                PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
                f"{self.operation} step {self.sequence} is missing "
                f"required argument {name!r}",
            ) from exc


@dataclass(frozen=True, slots=True)
class SeriesBar:
    """One completed bar of a pinned market-data series."""

    instrument_id: str
    resolution: str
    starts_at: datetime
    ends_at: datetime
    close: Decimal
    volume: Decimal
    session_truncated: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_id", _uuid(self.instrument_id, "instrument_id"))
        starts_at = _utc(self.starts_at, "starts_at")
        ends_at = _utc(self.ends_at, "ends_at")
        period = resolution_period(self.resolution)
        actual_period = ends_at - starts_at
        if actual_period <= timedelta(0) or (
            actual_period != period
            and (not self.session_truncated or actual_period > period)
        ):
            raise ElementEvaluationError(
                f"bar must span its declared resolution {self.resolution} ({period}) "
                f"or be a shorter session-truncated bar, got {actual_period}"
            )
        if not isinstance(self.session_truncated, bool):
            raise ElementEvaluationError("session_truncated must be boolean")
        object.__setattr__(self, "starts_at", starts_at)
        object.__setattr__(self, "ends_at", ends_at)
        if _decimal(self.close, "close") <= 0:
            raise ElementEvaluationError("close must be positive")
        if _decimal(self.volume, "volume") < 0:
            raise ElementEvaluationError("volume must not be negative")


@dataclass(frozen=True, slots=True)
class InstrumentSeries:
    """A look-ahead-safe, strictly ordered bar series for one instrument.

    Gaps are *not* rejected here. Whether a gap is tolerable is decided once, by
    :class:`backtest_engine.data_availability.DataAvailabilityAssessor`, against
    the pinned manifest coverage; re-deciding it per feature would be a second,
    silently different policy.
    """

    instrument_id: str
    data_kind: str
    resolution: str
    bars: tuple[SeriesBar, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_id", _uuid(self.instrument_id, "instrument_id"))
        if not self.data_kind:
            raise ElementEvaluationError("data_kind must not be empty")
        resolution_period(self.resolution)
        bars = tuple(self.bars)
        previous: SeriesBar | None = None
        for bar in bars:
            if not isinstance(bar, SeriesBar):
                raise ElementEvaluationError("bars must contain SeriesBar values")
            if bar.instrument_id != self.instrument_id:
                raise ElementEvaluationError(
                    "every bar must belong to the series instrument: "
                    f"{bar.instrument_id} != {self.instrument_id}"
                )
            if bar.resolution != self.resolution:
                raise ElementEvaluationError(
                    "every bar must carry the series resolution: "
                    f"{bar.resolution} != {self.resolution}"
                )
            if previous is not None and bar.starts_at <= previous.starts_at:
                raise ElementEvaluationError(
                    "series bars must be in strictly ascending starts_at order"
                )
            previous = bar
        object.__setattr__(self, "bars", bars)

    @property
    def identity(self) -> tuple[str, str]:
        return self.data_kind, self.resolution

    def completed_through(self, as_of: datetime) -> tuple[SeriesBar, ...]:
        """Bars whose period has finished at or before ``as_of``.

        A bar is usable only once it is complete; using the bar that contains
        the evaluation instant would read the future.
        """
        boundary = _utc(as_of, "as_of")
        return tuple(bar for bar in self.bars if bar.ends_at <= boundary)


@dataclass(frozen=True, slots=True)
class PinnedFeatureValue:
    """One decoded value from an immutable historical feature object."""

    bar_start_at: datetime
    value: Decimal

    def __post_init__(self) -> None:
        object.__setattr__(self, "bar_start_at", _utc(self.bar_start_at, "bar_start_at"))
        value = _decimal(self.value, "value")
        if value.as_tuple().exponent != -8:
            raise ElementEvaluationError("pinned feature values must have decimal scale 8")
        object.__setattr__(self, "value", value)


@dataclass(frozen=True, slots=True)
class PinnedFeatureSeries:
    """A version-verified feature series injected into ``LOAD_FEATURE``.

    A value belongs to the source bar beginning at ``bar_start_at`` and becomes
    visible only when that bar has completed. The exact preceding instant is
    required so a missing bar cannot silently reuse a stale value.
    """

    feature_id: str
    instrument_id: str
    resolution: str
    values: tuple[PinnedFeatureValue, ...]

    def __post_init__(self) -> None:
        if not self.feature_id:
            raise ElementEvaluationError("feature_id must not be empty")
        object.__setattr__(self, "instrument_id", _uuid(self.instrument_id, "instrument_id"))
        resolution_period(self.resolution)
        values = tuple(self.values)
        previous: PinnedFeatureValue | None = None
        for item in values:
            if not isinstance(item, PinnedFeatureValue):
                raise ElementEvaluationError("values must contain PinnedFeatureValue entries")
            if previous is not None and item.bar_start_at <= previous.bar_start_at:
                raise ElementEvaluationError(
                    "feature values must be strictly increasing and unique by bar_start_at"
                )
            previous = item
        object.__setattr__(self, "values", values)

    @property
    def identity(self) -> tuple[str, str]:
        return self.feature_id, self.resolution

    def value_at(self, as_of: datetime) -> Decimal:
        boundary = _utc(as_of, "as_of")
        expected_start = boundary - resolution_period(self.resolution)
        for item in reversed(self.values):
            if item.bar_start_at == expected_start:
                return item.value
            if item.bar_start_at < expected_start:
                break
        raise ElementInputMissing(
            f"pinned {self.feature_id}/{self.resolution} has a data gap at "
            f"{boundary.isoformat()}",
            input_reason="FEATURE_SERIES_DATA_GAP",
            evidence={
                "feature": self.feature_id,
                "resolution": self.resolution,
                "asOf": boundary.isoformat(),
                "expectedBarStartAt": expected_start.isoformat(),
            },
        )


@dataclass(frozen=True, slots=True)
class InstrumentInput:
    """Everything one instrument contributes to one evaluation."""

    instrument_id: str
    series: tuple[InstrumentSeries, ...]
    feature_series: tuple[PinnedFeatureSeries, ...] = ()
    require_pinned_features: bool = False
    values: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "instrument_id", _uuid(self.instrument_id, "instrument_id"))
        series = tuple(self.series)
        identities = [item.identity for item in series]
        if len(set(identities)) != len(identities):
            raise ElementEvaluationError(
                "(data_kind, resolution) must be unique within an instrument input"
            )
        if any(item.instrument_id != self.instrument_id for item in series):
            raise ElementEvaluationError(
                "every series must belong to the input instrument"
            )
        object.__setattr__(self, "series", series)
        feature_series = tuple(self.feature_series)
        feature_identities = [item.identity for item in feature_series]
        if len(set(feature_identities)) != len(feature_identities):
            raise ElementEvaluationError(
                "(feature_id, resolution) must be unique within an instrument input"
            )
        if any(item.instrument_id != self.instrument_id for item in feature_series):
            raise ElementEvaluationError(
                "every feature series must belong to the input instrument"
            )
        object.__setattr__(self, "feature_series", feature_series)
        values = dict(self.values)
        if any(not isinstance(key, str) or not isinstance(value, str) for key, value in values.items()):
            raise ElementEvaluationError("instrument input values must map strings to strings")
        object.__setattr__(self, "values", MappingProxyType(values))

    def series_for(self, data_kind: str, resolution: str) -> InstrumentSeries | None:
        for item in self.series:
            if item.identity == (data_kind, resolution):
                return item
        return None

    def feature_series_for(
        self, feature_id: str, resolution: str
    ) -> PinnedFeatureSeries | None:
        for item in self.feature_series:
            if item.identity == (feature_id, resolution):
                return item
        return None


@dataclass(slots=True)
class ElementEvaluation:
    """The mutable per-instrument scratchpad one plan evaluation writes into.

    ``last_value`` is the operand ``COMPARE`` consumes: B's plan is a flat
    sequence, so a comparison always refers to the value the preceding
    ``LOAD_FEATURE`` produced.
    """

    instrument_id: str
    as_of: datetime
    inputs: InstrumentInput
    values: dict[str, Decimal] = field(default_factory=dict)
    last_value: Decimal | None = None
    last_value_source: str | None = None

    def __post_init__(self) -> None:
        self.instrument_id = _uuid(self.instrument_id, "instrument_id")
        self.as_of = _utc(self.as_of, "as_of")
        if not isinstance(self.inputs, InstrumentInput):
            raise ElementEvaluationError("inputs must be an InstrumentInput")
        if self.inputs.instrument_id != self.instrument_id:
            raise ElementEvaluationError(
                "instrument input does not belong to the evaluated instrument"
            )

    def record(self, feature_id: str, value: Decimal) -> None:
        self.values[feature_id] = value
        self.last_value = value
        self.last_value_source = feature_id

    def require_operand(self, operation: str) -> Decimal:
        if self.last_value is None:
            raise ElementEvaluationError(
                f"{operation} has no operand: no preceding LOAD_FEATURE produced a value"
            )
        return self.last_value


class StepEvaluator(Protocol):
    """The callable an element catalog registers for one operation."""

    def __call__(self, step: PlanStep, evaluation: ElementEvaluation) -> StepOutcome: ...


def evidence_of(pairs: Sequence[tuple[str, str]]) -> Mapping[str, str]:
    return MappingProxyType(dict(pairs))
