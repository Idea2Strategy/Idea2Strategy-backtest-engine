from __future__ import annotations

from datetime import datetime

import pytest

from backtest_engine.contracts import build_backtest_result_event
from backtest_engine.data_availability import (
    AvailabilityStatus,
    AvailabilityValidationError,
    DataAvailabilityAssessor,
    DataObservation,
    DataRequirement,
    SkipStage,
    TimeInterval,
)


INSTRUMENT = "00000000-0000-4000-8000-000000000601"
OTHER_INSTRUMENT = "00000000-0000-4000-8000-000000000602"


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _interval(start: str, end: str) -> TimeInterval:
    return TimeInterval(_utc(start), _utc(end))


def _requirement(
    requirement_id: str = "bars-1m-primary",
    *,
    instrument_id: str = INSTRUMENT,
    data_kind: str = "ADJUSTED_BAR",
    resolution: str = "1m",
    warmup_from: str = "2025-11-28T14:28:00Z",
    evaluation_from: str = "2025-11-28T14:30:00Z",
    evaluation_through: str = "2025-11-28T14:35:00Z",
) -> DataRequirement:
    return DataRequirement(
        requirement_id=requirement_id,
        instrument_id=instrument_id,
        data_kind=data_kind,
        resolution=resolution,
        warmup_from=_utc(warmup_from),
        evaluation_from=_utc(evaluation_from),
        evaluation_through=_utc(evaluation_through),
    )


def _observation(
    requirement_id: str = "bars-1m-primary",
    *,
    instrument_id: str = INSTRUMENT,
    data_kind: str = "ADJUSTED_BAR",
    resolution: str = "1m",
    intervals: tuple[TimeInterval, ...] | None = None,
    verified: bool = True,
    listed_at: str | None = "2020-01-02T14:30:00Z",
) -> DataObservation:
    return DataObservation(
        requirement_id=requirement_id,
        instrument_id=instrument_id,
        data_kind=data_kind,
        resolution=resolution,
        available_intervals=intervals
        if intervals is not None
        else (_interval("2025-11-28T14:28:00Z", "2025-11-28T14:35:00Z"),),
        verified=verified,
        listed_at=_utc(listed_at) if listed_at else None,
    )


def test_full_exact_coverage_is_available() -> None:
    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], [_observation()]
    )

    assert assessment.status is AvailabilityStatus.AVAILABLE
    assert assessment.skip_intervals == ()
    assert assessment.missing_requirements == ()
    assert assessment.is_stage_allowed(
        SkipStage.FILL, _utc("2025-11-28T14:32:00Z")
    )


def test_short_gap_degrades_only_the_exact_interval_without_retroactive_replay() -> None:
    observation = _observation(
        intervals=(
            _interval("2025-11-28T14:28:00Z", "2025-11-28T14:32:00Z"),
            _interval("2025-11-28T14:33:00Z", "2025-11-28T14:35:00Z"),
        )
    )

    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], [observation]
    )

    assert assessment.status is AvailabilityStatus.DEGRADED
    assert len(assessment.skip_intervals) == 1
    skipped = assessment.skip_intervals[0]
    assert skipped.interval == _interval(
        "2025-11-28T14:32:00Z", "2025-11-28T14:33:00Z"
    )
    assert skipped.requirement_id == "bars-1m-primary"
    assert skipped.stages == (
        SkipStage.EVALUATION,
        SkipStage.ORDER_TRIGGER,
        SkipStage.FILL,
    )
    assert skipped.expirations_continue is True
    assert skipped.interpolation_allowed is False
    assert skipped.retroactive_replay_allowed is False
    assert not assessment.is_stage_allowed(
        SkipStage.EVALUATION, _utc("2025-11-28T14:32:00Z")
    )
    assert assessment.is_stage_allowed(
        SkipStage.EVALUATION, _utc("2025-11-28T14:33:00Z")
    )


def test_adjacent_and_overlapping_coverage_is_normalized_without_a_fake_gap() -> None:
    observation = _observation(
        intervals=(
            _interval("2025-11-28T14:31:00Z", "2025-11-28T14:35:00Z"),
            _interval("2025-11-28T14:28:00Z", "2025-11-28T14:31:00Z"),
            _interval("2025-11-28T14:30:00Z", "2025-11-28T14:32:00Z"),
        )
    )

    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], [observation]
    )

    assert assessment.status is AvailabilityStatus.AVAILABLE
    assert assessment.skip_intervals == ()


@pytest.mark.parametrize(
    ("observation", "reason"),
    [
        (None, "OBSERVATION_MISSING"),
        (_observation(intervals=()), "REQUIRED_SERIES_ABSENT"),
        (_observation(verified=False), "SERIES_UNVERIFIED"),
        (_observation(resolution="5m"), "SERIES_IDENTITY_MISMATCH"),
    ],
)
def test_structural_missing_data_is_unavailable(
    observation: DataObservation | None, reason: str
) -> None:
    observations = [] if observation is None else [observation]

    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], observations
    )

    assert assessment.status is AvailabilityStatus.UNAVAILABLE
    assert assessment.skip_intervals == ()
    assert [item.reason_code for item in assessment.missing_requirements] == [reason]
    assert not assessment.is_stage_allowed(
        SkipStage.EVALUATION, _utc("2025-11-28T14:32:00Z")
    )


def test_missing_warmup_blocks_start_instead_of_substituting_later_data() -> None:
    observation = _observation(
        intervals=(
            _interval("2025-11-28T14:29:00Z", "2025-11-28T14:35:00Z"),
        )
    )

    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], [observation]
    )

    assert assessment.status is AvailabilityStatus.UNAVAILABLE
    missing = assessment.missing_requirements[0]
    assert missing.reason_code == "WARMUP_COVERAGE_MISSING"
    assert missing.interval == _interval(
        "2025-11-28T14:28:00Z", "2025-11-28T14:29:00Z"
    )


def test_instrument_listed_after_required_start_makes_the_whole_run_unavailable() -> None:
    observation = _observation(listed_at="2025-11-28T14:31:00Z")

    assessment = DataAvailabilityAssessor().assess(
        [_requirement()], [observation]
    )

    assert assessment.status is AvailabilityStatus.UNAVAILABLE
    assert assessment.missing_requirements[0].reason_code == (
        "INSTRUMENT_LISTED_AFTER_EVALUATION_START"
    )


def test_disjoint_required_series_have_no_common_replayable_interval() -> None:
    primary = _requirement()
    secondary = _requirement(
        "indicator-5m-secondary",
        instrument_id=OTHER_INSTRUMENT,
        resolution="5m",
        warmup_from="2025-11-28T14:30:00Z",
    )
    observations = [
        _observation(
            intervals=(
                _interval("2025-11-28T14:28:00Z", "2025-11-28T14:32:00Z"),
            )
        ),
        _observation(
            "indicator-5m-secondary",
            instrument_id=OTHER_INSTRUMENT,
            resolution="5m",
            intervals=(
                _interval("2025-11-28T14:32:00Z", "2025-11-28T14:35:00Z"),
            ),
        ),
    ]

    assessment = DataAvailabilityAssessor().assess(
        [secondary, primary], list(reversed(observations))
    )

    assert assessment.status is AvailabilityStatus.UNAVAILABLE
    assert assessment.missing_requirements[0].reason_code == (
        "NO_COMMON_REPLAYABLE_INTERVAL"
    )


def test_one_instrument_gap_does_not_end_the_run_when_common_time_remains() -> None:
    primary = _requirement()
    secondary = _requirement(
        "bars-1m-secondary",
        instrument_id=OTHER_INSTRUMENT,
        warmup_from="2025-11-28T14:30:00Z",
    )
    observations = [
        _observation(),
        _observation(
            "bars-1m-secondary",
            instrument_id=OTHER_INSTRUMENT,
            intervals=(
                _interval("2025-11-28T14:30:00Z", "2025-11-28T14:31:00Z"),
                _interval("2025-11-28T14:32:00Z", "2025-11-28T14:35:00Z"),
            ),
        ),
    ]

    assessment = DataAvailabilityAssessor().assess(
        [primary, secondary], observations
    )

    assert assessment.status is AvailabilityStatus.DEGRADED
    assert assessment.missing_requirements == ()
    assert not assessment.is_stage_allowed(
        SkipStage.ORDER_TRIGGER, _utc("2025-11-28T14:31:30Z")
    )
    assert assessment.is_stage_allowed(
        SkipStage.ORDER_TRIGGER, _utc("2025-11-28T14:32:00Z")
    )


def test_unavailable_contract_fields_are_sorted_and_publish_as_backtest_v1() -> None:
    """The fields must drop straight into the published ``backtest.v1`` event.

    Not a shape assertion in isolation: the real contract builder validates the
    assembled document against the JSON Schema, so a snake_case regression or a
    renamed field fails here rather than at the message broker.
    """
    requirements = [
        _requirement("z-missing"),
        _requirement("a-missing", instrument_id=OTHER_INSTRUMENT),
    ]
    assessment = DataAvailabilityAssessor().assess(requirements, [])

    fields = assessment.unavailable_contract_fields()

    assert fields == {
        "reasonCode": "REQUIRED_DATA_UNAVAILABLE",
        "missingRequirements": [
            "a-missing:OBSERVATION_MISSING",
            "z-missing:OBSERVATION_MISSING",
        ],
    }
    event = build_backtest_result_event(
        status="UNAVAILABLE",
        backtest_run_id="00000000-0000-4000-8000-000000000703",
        bot_id="00000000-0000-4000-8000-000000000704",
        owner_account_id="00000000-0000-4000-8000-000000000705",
        expected_snapshot_hash="sha256:" + "a" * 64,
        input_bundle_fingerprint="sha256:" + "b" * 64,
        execution_policy_version="official-backtest-policy-v1",
        message_id="00000000-0000-4000-8000-000000000701",
        occurred_at="2025-11-28T14:30:00Z",
        correlation_id="00000000-0000-4000-8000-000000000702",
        decidedAt="2025-11-28T14:30:00Z",
        **fields,
    )

    assert event["metadata"]["messageType"] == "BACKTEST_UNAVAILABLE"
    assert event["missingRequirements"] == fields["missingRequirements"]


def test_assessment_is_independent_of_requirement_observation_and_interval_order() -> None:
    requirements = [
        _requirement(),
        _requirement(
            "bars-1m-secondary",
            instrument_id=OTHER_INSTRUMENT,
            warmup_from="2025-11-28T14:30:00Z",
        ),
    ]
    observations = [
        _observation(
            intervals=(
                _interval("2025-11-28T14:33:00Z", "2025-11-28T14:35:00Z"),
                _interval("2025-11-28T14:28:00Z", "2025-11-28T14:32:00Z"),
            )
        ),
        _observation(
            "bars-1m-secondary",
            instrument_id=OTHER_INSTRUMENT,
            intervals=(
                _interval("2025-11-28T14:30:00Z", "2025-11-28T14:35:00Z"),
            ),
        ),
    ]

    first = DataAvailabilityAssessor().assess(requirements, observations)
    second = DataAvailabilityAssessor().assess(
        list(reversed(requirements)), list(reversed(observations))
    )

    assert first == second


def test_rejects_naive_boundaries_duplicates_and_empty_requirements() -> None:
    with pytest.raises(AvailabilityValidationError, match="timezone-aware"):
        TimeInterval(
            datetime(2025, 11, 28, 14, 30),
            _utc("2025-11-28T14:31:00Z"),
        )

    assessor = DataAvailabilityAssessor()
    with pytest.raises(AvailabilityValidationError, match="requirements"):
        assessor.assess([], [])
    with pytest.raises(AvailabilityValidationError, match="requirement_id"):
        assessor.assess([_requirement(), _requirement()], [_observation()])
    with pytest.raises(AvailabilityValidationError, match="observation"):
        assessor.assess(
            [_requirement()], [_observation(), _observation()]
        )
