"""Deterministic D24 data sufficiency and unavailable assessment."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum


class AvailabilityValidationError(ValueError):
    """Raised when pinned data coverage cannot be assessed safely."""


class AvailabilityStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


class SkipStage(str, Enum):
    EVALUATION = "EVALUATION"
    ORDER_TRIGGER = "ORDER_TRIGGER"
    FILL = "FILL"


ALL_SKIP_STAGES = (
    SkipStage.EVALUATION,
    SkipStage.ORDER_TRIGGER,
    SkipStage.FILL,
)


def _utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise AvailabilityValidationError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise AvailabilityValidationError(f"{label} must be a non-empty string")
    return value


def _uuid(value: str, label: str) -> str:
    _text(value, label)
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise AvailabilityValidationError(f"{label} must be a UUID") from exc


@dataclass(frozen=True, slots=True, order=True)
class TimeInterval:
    """One UTC half-open interval: ``[starts_at, ends_at)``."""

    starts_at: datetime
    ends_at: datetime

    def __post_init__(self) -> None:
        starts_at = _utc(self.starts_at, "starts_at")
        ends_at = _utc(self.ends_at, "ends_at")
        if starts_at >= ends_at:
            raise AvailabilityValidationError(
                "interval starts_at must precede ends_at"
            )
        object.__setattr__(self, "starts_at", starts_at)
        object.__setattr__(self, "ends_at", ends_at)

    def contains(self, instant: datetime) -> bool:
        assessed_at = _utc(instant, "instant")
        return self.starts_at <= assessed_at < self.ends_at

    def intersection(self, other: TimeInterval) -> TimeInterval | None:
        start = max(self.starts_at, other.starts_at)
        end = min(self.ends_at, other.ends_at)
        return TimeInterval(start, end) if start < end else None


@dataclass(frozen=True, slots=True)
class DataRequirement:
    requirement_id: str
    instrument_id: str
    data_kind: str
    resolution: str
    warmup_from: datetime
    evaluation_from: datetime
    evaluation_through: datetime

    def __post_init__(self) -> None:
        _text(self.requirement_id, "requirement_id")
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        _text(self.data_kind, "data_kind")
        _text(self.resolution, "resolution")
        warmup_from = _utc(self.warmup_from, "warmup_from")
        evaluation_from = _utc(self.evaluation_from, "evaluation_from")
        evaluation_through = _utc(
            self.evaluation_through, "evaluation_through"
        )
        if not warmup_from <= evaluation_from < evaluation_through:
            raise AvailabilityValidationError(
                "requirement boundaries must satisfy "
                "warmup_from <= evaluation_from < evaluation_through"
            )
        object.__setattr__(self, "warmup_from", warmup_from)
        object.__setattr__(self, "evaluation_from", evaluation_from)
        object.__setattr__(self, "evaluation_through", evaluation_through)

    @property
    def required_interval(self) -> TimeInterval:
        return TimeInterval(self.warmup_from, self.evaluation_through)

    @property
    def evaluation_interval(self) -> TimeInterval:
        return TimeInterval(self.evaluation_from, self.evaluation_through)

    @property
    def warmup_interval(self) -> TimeInterval | None:
        if self.warmup_from == self.evaluation_from:
            return None
        return TimeInterval(self.warmup_from, self.evaluation_from)


@dataclass(frozen=True, slots=True)
class DataObservation:
    requirement_id: str
    instrument_id: str
    data_kind: str
    resolution: str
    available_intervals: tuple[TimeInterval, ...]
    verified: bool = True
    listed_at: datetime | None = None

    def __post_init__(self) -> None:
        _text(self.requirement_id, "requirement_id")
        object.__setattr__(
            self, "instrument_id", _uuid(self.instrument_id, "instrument_id")
        )
        _text(self.data_kind, "data_kind")
        _text(self.resolution, "resolution")
        intervals = tuple(self.available_intervals)
        if any(not isinstance(item, TimeInterval) for item in intervals):
            raise AvailabilityValidationError(
                "available_intervals must contain TimeInterval values"
            )
        if not isinstance(self.verified, bool):
            raise AvailabilityValidationError("verified must be boolean")
        listed_at = (
            _utc(self.listed_at, "listed_at")
            if self.listed_at is not None
            else None
        )
        object.__setattr__(self, "available_intervals", intervals)
        object.__setattr__(self, "listed_at", listed_at)


@dataclass(frozen=True, slots=True)
class MissingRequirement:
    requirement_id: str
    reason_code: str
    interval: TimeInterval | None = None

    @property
    def contract_value(self) -> str:
        return f"{self.requirement_id}:{self.reason_code}"


@dataclass(frozen=True, slots=True)
class SkipInterval:
    requirement_id: str
    interval: TimeInterval
    stages: tuple[SkipStage, ...] = ALL_SKIP_STAGES
    expirations_continue: bool = True
    interpolation_allowed: bool = False
    retroactive_replay_allowed: bool = False


@dataclass(frozen=True, slots=True)
class AvailabilityAssessment:
    status: AvailabilityStatus
    skip_intervals: tuple[SkipInterval, ...]
    missing_requirements: tuple[MissingRequirement, ...]

    def is_stage_allowed(self, stage: SkipStage, instant: datetime) -> bool:
        if not isinstance(stage, SkipStage):
            raise AvailabilityValidationError("stage is unsupported")
        assessed_at = _utc(instant, "instant")
        if self.status is AvailabilityStatus.UNAVAILABLE:
            return False
        return not any(
            stage in skipped.stages and skipped.interval.contains(assessed_at)
            for skipped in self.skip_intervals
        )

    def unavailable_contract_fields(self) -> dict[str, object]:
        if self.status is not AvailabilityStatus.UNAVAILABLE:
            raise AvailabilityValidationError(
                "contract fields require an UNAVAILABLE assessment"
            )
        return {
            "reason_code": "REQUIRED_DATA_UNAVAILABLE",
            "missing_requirements": sorted(
                item.contract_value for item in self.missing_requirements
            ),
        }


class DataAvailabilityAssessor:
    """Compares exact locked series coverage without imputation or fallback."""

    def assess(
        self,
        requirements: Iterable[DataRequirement],
        observations: Iterable[DataObservation],
    ) -> AvailabilityAssessment:
        supplied_requirements = tuple(requirements)
        supplied_observations = tuple(observations)
        if not supplied_requirements:
            raise AvailabilityValidationError("requirements must not be empty")
        if any(
            not isinstance(item, DataRequirement)
            for item in supplied_requirements
        ):
            raise AvailabilityValidationError(
                "requirements must contain DataRequirement values"
            )
        if any(
            not isinstance(item, DataObservation)
            for item in supplied_observations
        ):
            raise AvailabilityValidationError(
                "observations must contain DataObservation values"
            )
        if len({item.requirement_id for item in supplied_requirements}) != len(
            supplied_requirements
        ):
            raise AvailabilityValidationError(
                "requirement_id values must be unique"
            )
        if len({item.requirement_id for item in supplied_observations}) != len(
            supplied_observations
        ):
            raise AvailabilityValidationError(
                "observation requirement_id values must be unique"
            )

        observations_by_id = {
            item.requirement_id: item for item in supplied_observations
        }
        missing: list[MissingRequirement] = []
        skipped: list[SkipInterval] = []
        evaluation_coverage: list[tuple[TimeInterval, ...]] = []

        for requirement in sorted(
            supplied_requirements, key=lambda item: item.requirement_id
        ):
            observation = observations_by_id.get(requirement.requirement_id)
            if observation is None:
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id, "OBSERVATION_MISSING"
                    )
                )
                continue
            if not self._identity_matches(requirement, observation):
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id,
                        "SERIES_IDENTITY_MISMATCH",
                    )
                )
                continue
            if not observation.verified:
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id, "SERIES_UNVERIFIED"
                    )
                )
                continue
            if (
                observation.listed_at is not None
                and observation.listed_at > requirement.evaluation_from
            ):
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id,
                        "INSTRUMENT_LISTED_AFTER_EVALUATION_START",
                        TimeInterval(
                            requirement.evaluation_from,
                            min(
                                observation.listed_at,
                                requirement.evaluation_through,
                            ),
                        ),
                    )
                )
                continue

            normalized = _normalize_intervals(
                observation.available_intervals, requirement.required_interval
            )
            if not normalized:
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id, "REQUIRED_SERIES_ABSENT"
                    )
                )
                continue

            warmup = requirement.warmup_interval
            if warmup is not None:
                warmup_gaps = _gaps(warmup, normalized)
                if warmup_gaps:
                    missing.append(
                        MissingRequirement(
                            requirement.requirement_id,
                            "WARMUP_COVERAGE_MISSING",
                            warmup_gaps[0],
                        )
                    )
                    continue

            evaluation = _normalize_intervals(
                normalized, requirement.evaluation_interval
            )
            if not evaluation:
                missing.append(
                    MissingRequirement(
                        requirement.requirement_id,
                        "EVALUATION_COVERAGE_ABSENT",
                        requirement.evaluation_interval,
                    )
                )
                continue
            evaluation_coverage.append(evaluation)
            skipped.extend(
                SkipInterval(requirement.requirement_id, gap)
                for gap in _gaps(requirement.evaluation_interval, evaluation)
            )

        if missing:
            return AvailabilityAssessment(
                AvailabilityStatus.UNAVAILABLE,
                (),
                tuple(sorted(missing, key=_missing_key)),
            )

        if not _common_intervals(evaluation_coverage):
            return AvailabilityAssessment(
                AvailabilityStatus.UNAVAILABLE,
                (),
                (
                    MissingRequirement(
                        "$strategy", "NO_COMMON_REPLAYABLE_INTERVAL"
                    ),
                ),
            )

        ordered_skips = tuple(sorted(skipped, key=_skip_key))
        status = (
            AvailabilityStatus.DEGRADED
            if ordered_skips
            else AvailabilityStatus.AVAILABLE
        )
        return AvailabilityAssessment(status, ordered_skips, ())

    @staticmethod
    def _identity_matches(
        requirement: DataRequirement, observation: DataObservation
    ) -> bool:
        return (
            requirement.instrument_id == observation.instrument_id
            and requirement.data_kind == observation.data_kind
            and requirement.resolution == observation.resolution
        )


def _normalize_intervals(
    intervals: Iterable[TimeInterval], clip: TimeInterval
) -> tuple[TimeInterval, ...]:
    clipped = [
        intersection
        for interval in intervals
        if (intersection := interval.intersection(clip)) is not None
    ]
    if not clipped:
        return ()
    clipped.sort(key=lambda item: (item.starts_at, item.ends_at))
    merged: list[TimeInterval] = [clipped[0]]
    for current in clipped[1:]:
        previous = merged[-1]
        if current.starts_at <= previous.ends_at:
            merged[-1] = TimeInterval(
                previous.starts_at, max(previous.ends_at, current.ends_at)
            )
        else:
            merged.append(current)
    return tuple(merged)


def _gaps(
    target: TimeInterval, available: Iterable[TimeInterval]
) -> tuple[TimeInterval, ...]:
    normalized = _normalize_intervals(available, target)
    cursor = target.starts_at
    missing: list[TimeInterval] = []
    for interval in normalized:
        if cursor < interval.starts_at:
            missing.append(TimeInterval(cursor, interval.starts_at))
        cursor = max(cursor, interval.ends_at)
    if cursor < target.ends_at:
        missing.append(TimeInterval(cursor, target.ends_at))
    return tuple(missing)


def _common_intervals(
    coverage_sets: list[tuple[TimeInterval, ...]],
) -> tuple[TimeInterval, ...]:
    if not coverage_sets:
        return ()
    common = coverage_sets[0]
    for coverage in coverage_sets[1:]:
        intersections = [
            intersection
            for left in common
            for right in coverage
            if (intersection := left.intersection(right)) is not None
        ]
        if not intersections:
            return ()
        clip = TimeInterval(
            min(item.starts_at for item in intersections),
            max(item.ends_at for item in intersections),
        )
        common = _normalize_intervals(intersections, clip)
    return common


def _missing_key(
    item: MissingRequirement,
) -> tuple[str, str, datetime, datetime]:
    start = item.interval.starts_at if item.interval else datetime.min.replace(
        tzinfo=timezone.utc
    )
    end = item.interval.ends_at if item.interval else start
    return item.requirement_id, item.reason_code, start, end


def _skip_key(item: SkipInterval) -> tuple[datetime, datetime, str]:
    return (
        item.interval.starts_at,
        item.interval.ends_at,
        item.requirement_id,
    )
