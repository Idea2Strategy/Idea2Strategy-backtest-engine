"""Fail-closed, instance-local scale-to-zero control for the backtest ASG.

SQS metrics alone never authorize termination. The worker already has private
database and queue access, so it observes both planes twice before asking Auto
Scaling to set the one exact configured group to desired capacity zero.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from threading import Event
from typing import Any, Protocol

from sqlalchemy import Engine, text


class ScaleDownConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class QueueActivity:
    queue_url: str
    visible: int
    not_visible: int
    delayed: int

    @property
    def idle(self) -> bool:
        return self.visible == self.not_visible == self.delayed == 0


@dataclass(frozen=True, slots=True)
class BacktestActivity:
    queued_runs: int
    running_runs: int
    live_claims: int
    queues: tuple[QueueActivity, ...]

    @property
    def idle(self) -> bool:
        return (
            self.queued_runs == 0
            and self.running_runs == 0
            and self.live_claims == 0
            and bool(self.queues)
            and all(queue.idle for queue in self.queues)
        )


class ActivityProbe(Protocol):
    def observe(self) -> BacktestActivity: ...


class DesiredCapacityPort(Protocol):
    def set_desired_zero(self, asg_name: str) -> None: ...


class InstanceScaleDownController:
    """Require consecutive all-idle observations; every error resets the gate."""

    def __init__(
        self,
        *,
        probe: ActivityProbe,
        capacity: DesiredCapacityPort,
        asg_name: str,
        poll_seconds: float,
        required_idle_observations: int = 2,
    ) -> None:
        if not asg_name.strip():
            raise ScaleDownConfigurationError("BACKTEST_ASG_NAME must not be blank")
        if poll_seconds <= 0:
            raise ScaleDownConfigurationError("scale-down poll interval must be positive")
        if required_idle_observations < 2:
            raise ScaleDownConfigurationError("scale-down requires at least two idle observations")
        self._probe = probe
        self._capacity = capacity
        self._asg_name = asg_name
        self._poll_seconds = poll_seconds
        self._required = required_idle_observations

    def run(self, stop: Event) -> bool:
        consecutive_idle = 0
        while not stop.wait(self._poll_seconds):
            try:
                observation = self._probe.observe()
                consecutive_idle = consecutive_idle + 1 if observation.idle else 0
                if consecutive_idle < self._required:
                    continue
                self._capacity.set_desired_zero(self._asg_name)
                return True
            except Exception:
                # Missing/stale/unparseable telemetry and AWS errors are not proof
                # of idleness. Re-observe both planes from the beginning.
                consecutive_idle = 0
        return False


class PostgresSqsActivityProbe:
    _ATTRIBUTES = (
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
    )

    def __init__(
        self,
        *,
        engine: Engine,
        sqs_client: Any,
        queue_urls: Sequence[str],
        request_queue_urls: Sequence[str],
    ) -> None:
        execution_urls = tuple(queue_url.strip() for queue_url in queue_urls)
        request_urls = tuple(queue_url.strip() for queue_url in request_queue_urls)
        if len(execution_urls) != 3 or any(not queue_url for queue_url in execution_urls):
            raise ScaleDownConfigurationError("scale-down requires exactly three lane queue URLs")
        if len(set(execution_urls)) != 3:
            raise ScaleDownConfigurationError("scale-down lane queue URLs must be distinct")
        if len(request_urls) != 3 or any(not queue_url for queue_url in request_urls):
            raise ScaleDownConfigurationError("scale-down requires exactly three request queue URLs")
        if len(set(request_urls)) != 3:
            raise ScaleDownConfigurationError("scale-down request queue URLs must be distinct")
        if set(execution_urls) & set(request_urls):
            raise ScaleDownConfigurationError(
                "scale-down request queue URLs must be distinct from execution queue URLs"
            )
        self._engine = engine
        self._sqs = sqs_client
        self._queue_urls = execution_urls + request_urls

    def observe(self) -> BacktestActivity:
        with self._engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        """SELECT count(*) FILTER (WHERE status = 'QUEUED') AS queued_runs,
                              count(*) FILTER (WHERE status = 'RUNNING') AS running_runs,
                              (SELECT count(*) FROM backtest.run_attempts
                                WHERE status = 'RUNNING'
                                  AND claim_expires_at > clock_timestamp()) AS live_claims
                         FROM backtest.runs"""
                    )
                )
                .mappings()
                .one()
            )
        queues = tuple(self._queue_activity(queue_url) for queue_url in self._queue_urls)
        return BacktestActivity(
            queued_runs=int(row["queued_runs"]),
            running_runs=int(row["running_runs"]),
            live_claims=int(row["live_claims"]),
            queues=queues,
        )

    def _queue_activity(self, queue_url: str) -> QueueActivity:
        response = self._sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=list(self._ATTRIBUTES))
        attributes = response.get("Attributes")
        if not isinstance(attributes, Mapping):
            raise RuntimeError("SQS queue attributes are missing")
        values: list[int] = []
        for name in self._ATTRIBUTES:
            raw = attributes.get(name)
            if raw is None:
                raise RuntimeError(f"SQS queue attribute is missing: {name}")
            value = int(raw)
            if value < 0:
                raise RuntimeError(f"SQS queue attribute is negative: {name}")
            values.append(value)
        return QueueActivity(queue_url, *values)


class Boto3DesiredCapacityPort:
    def __init__(self, autoscaling_client: Any) -> None:
        self._autoscaling = autoscaling_client

    def set_desired_zero(self, asg_name: str) -> None:
        self._autoscaling.set_desired_capacity(
            AutoScalingGroupName=asg_name,
            DesiredCapacity=0,
            HonorCooldown=False,
        )


def controller_from_env(
    environ: Mapping[str, str],
    *,
    engine: Engine,
    sqs_client: Any,
    autoscaling_client: Any,
    queue_urls: Sequence[str],
    request_queue_urls: Sequence[str],
) -> InstanceScaleDownController | None:
    enabled = environ.get("BACKTEST_SCALE_DOWN_ENABLED", "false").strip().lower()
    if enabled in {"", "false"}:
        return None
    if enabled != "true":
        raise ScaleDownConfigurationError("BACKTEST_SCALE_DOWN_ENABLED must be true or false")
    asg_name = environ.get("BACKTEST_ASG_NAME", "")
    try:
        poll_seconds = float(environ.get("BACKTEST_SCALE_DOWN_POLL_SECONDS", "60"))
    except ValueError as exc:
        raise ScaleDownConfigurationError("BACKTEST_SCALE_DOWN_POLL_SECONDS must be numeric") from exc
    return InstanceScaleDownController(
        probe=PostgresSqsActivityProbe(
            engine=engine,
            sqs_client=sqs_client,
            queue_urls=queue_urls,
            request_queue_urls=request_queue_urls,
        ),
        capacity=Boto3DesiredCapacityPort(autoscaling_client),
        asg_name=asg_name,
        poll_seconds=poll_seconds,
        required_idle_observations=2,
    )
