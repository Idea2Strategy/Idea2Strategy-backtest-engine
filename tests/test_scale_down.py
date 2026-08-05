from __future__ import annotations

from collections.abc import Iterable
from threading import Event
from typing import Any

from backtest_engine.scale_down import (
    BacktestActivity,
    InstanceScaleDownController,
    PostgresSqsActivityProbe,
    QueueActivity,
    ScaleDownConfigurationError,
    controller_from_env,
)


def activity(*, queued: int = 0, running: int = 0, claims: int = 0, queue_depth: int = 0) -> BacktestActivity:
    return BacktestActivity(
        queued,
        running,
        claims,
        tuple(QueueActivity(f"queue-{index}", queue_depth, 0, 0) for index in range(3)),
    )


class ScriptedProbe:
    def __init__(self, observations: Iterable[BacktestActivity | Exception]) -> None:
        self._observations = iter(observations)

    def observe(self) -> BacktestActivity:
        observation = next(self._observations)
        if isinstance(observation, Exception):
            raise observation
        return observation


class RecordingCapacity:
    def __init__(self) -> None:
        self.names: list[str] = []

    def set_desired_zero(self, asg_name: str) -> None:
        self.names.append(asg_name)


class ImmediateEvent(Event):
    def wait(self, timeout: float | None = None) -> bool:
        return False


def controller(
    observations: Iterable[BacktestActivity | Exception], capacity: RecordingCapacity
) -> InstanceScaleDownController:
    return InstanceScaleDownController(
        probe=ScriptedProbe(observations),
        capacity=capacity,
        asg_name="i2s-dev-backtest",
        poll_seconds=1,
    )


def test_two_consecutive_all_idle_observations_scale_the_exact_group_to_zero() -> None:
    capacity = RecordingCapacity()

    assert controller((activity(), activity()), capacity).run(ImmediateEvent()) is True

    assert capacity.names == ["i2s-dev-backtest"]


def test_database_work_and_every_sqs_depth_dimension_block_termination() -> None:
    for busy in (
        activity(queued=1),
        activity(running=1),
        activity(claims=1),
        BacktestActivity(0, 0, 0, (QueueActivity("basic", 0, 1, 0),)),
        BacktestActivity(0, 0, 0, (QueueActivity("basic", 0, 0, 1),)),
    ):
        capacity = RecordingCapacity()
        gate = controller((activity(), busy, activity(), activity()), capacity)
        assert gate.run(ImmediateEvent()) is True
        assert capacity.names == ["i2s-dev-backtest"]


def test_probe_error_resets_the_two_observation_stabilization_gate() -> None:
    capacity = RecordingCapacity()
    gate = controller((activity(), RuntimeError("stale telemetry"), activity(), activity()), capacity)

    assert gate.run(ImmediateEvent()) is True
    assert capacity.names == ["i2s-dev-backtest"]


def test_environment_gate_is_disabled_by_default() -> None:
    assert (
        controller_from_env(
            {},
            engine=object(),  # type: ignore[arg-type]
            sqs_client=object(),
            autoscaling_client=object(),
            queue_urls=(),
            request_queue_urls=(),
        )
        is None
    )


def test_invalid_environment_gate_fails_closed() -> None:
    import pytest

    with pytest.raises(ScaleDownConfigurationError, match="true or false"):
        controller_from_env(
            {"BACKTEST_SCALE_DOWN_ENABLED": "maybe"},
            engine=object(),  # type: ignore[arg-type]
            sqs_client=object(),
            autoscaling_client=object(),
            queue_urls=(),
            request_queue_urls=(),
        )


class FakeResult:
    def mappings(self) -> FakeResult:
        return self

    def one(self) -> dict[str, int]:
        return {"queued_runs": 0, "running_runs": 0, "live_claims": 0}


class FakeConnection:
    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _statement: object) -> FakeResult:
        return FakeResult()


class FakeEngine:
    def connect(self) -> FakeConnection:
        return FakeConnection()


class FakeSqs:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def get_queue_attributes(self, **kwargs: Any) -> dict[str, dict[str, str]]:
        self.calls.append(kwargs)
        return {
            "Attributes": {
                "ApproximateNumberOfMessages": "1" if kwargs["QueueUrl"] == "request-custom" else "0",
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateNumberOfMessagesDelayed": "0",
            }
        }


def test_probe_observes_all_execution_and_request_queue_depth_dimensions() -> None:
    sqs = FakeSqs()
    probe = PostgresSqsActivityProbe(
        engine=FakeEngine(),  # type: ignore[arg-type]
        sqs_client=sqs,
        queue_urls=("job-basic", "job-custom", "job-competition"),
        request_queue_urls=("request-basic", "request-custom", "request-competition"),
    )

    observation = probe.observe()

    assert len(observation.queues) == 6
    assert observation.idle is False
    assert [call["QueueUrl"] for call in sqs.calls] == [
        "job-basic",
        "job-custom",
        "job-competition",
        "request-basic",
        "request-custom",
        "request-competition",
    ]
    assert all(
        call["AttributeNames"]
        == [
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ]
        for call in sqs.calls
    )


def test_scale_down_requires_three_distinct_request_queues_separate_from_jobs() -> None:
    import pytest

    with pytest.raises(ScaleDownConfigurationError, match="exactly three request queue URLs"):
        PostgresSqsActivityProbe(
            engine=FakeEngine(),  # type: ignore[arg-type]
            sqs_client=FakeSqs(),
            queue_urls=("job-basic", "job-custom", "job-competition"),
            request_queue_urls=(),
        )
    with pytest.raises(ScaleDownConfigurationError, match="must be distinct from execution"):
        PostgresSqsActivityProbe(
            engine=FakeEngine(),  # type: ignore[arg-type]
            sqs_client=FakeSqs(),
            queue_urls=("job-basic", "job-custom", "job-competition"),
            request_queue_urls=("request-basic", "request-custom", "job-basic"),
        )


class FakeAutoscaling:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def set_desired_capacity(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)


def test_boto3_capacity_port_uses_exact_name_zero_and_no_cooldown() -> None:
    from backtest_engine.scale_down import Boto3DesiredCapacityPort

    client = FakeAutoscaling()
    Boto3DesiredCapacityPort(client).set_desired_zero("i2s-dev-backtest")

    assert client.calls == [
        {
            "AutoScalingGroupName": "i2s-dev-backtest",
            "DesiredCapacity": 0,
            "HonorCooldown": False,
        }
    ]
