from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from backtest_engine.backtest_request_intake import RequestLane
from backtest_engine.worker import (
    BacktestLane,
    BacktestLaneScheduler,
    BacktestWorker,
    ExecutionRecordStatus,
    HandledMessage,
    InMemoryExecutionKeyStore,
    JobOutcome,
    JobResult,
    MessageDisposition,
    WorkerConfig,
    WorkerConfigurationError,
    _lane_configs_from_env,
    _request_configs_from_env,
)


T0 = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _message(lane: BacktestLane, index: int) -> dict[str, Any]:
    return {
        "MessageId": f"{lane.value}-{index}",
        "ReceiptHandle": f"receipt-{lane.value}-{index}",
        "Body": json.dumps(
            {
                "backtestRunId": f"00000000-0000-4000-8000-{index:012d}",
                "idempotencyKey": f"{lane.value}:{index}",
            }
        ),
        "Attributes": {"ApproximateReceiveCount": "1"},
    }


class BlockingLaneWorker:
    def __init__(self, lane: BacktestLane, count: int, release: threading.Event) -> None:
        self.lane = lane
        self.messages = deque(_message(lane, index) for index in range(count))
        self.release = release
        self.received: list[str] = []
        self.started: list[str] = []

    def receive_one(self) -> Mapping[str, Any] | None:
        if not self.messages:
            return None
        message = self.messages.popleft()
        self.received.append(str(message["MessageId"]))
        return message

    def handle_message(self, message: Mapping[str, Any]) -> HandledMessage:
        message_id = str(message["MessageId"])
        self.started.append(message_id)
        assert self.release.wait(5)
        return HandledMessage(
            message_id,
            MessageDisposition.DELETED,
            None,
            f"key:{message_id}",
        )


class FailFirstReceiveLaneWorker(BlockingLaneWorker):
    def __init__(
        self,
        lane: BacktestLane,
        count: int,
        release: threading.Event,
        *,
        failures: int = 1,
    ) -> None:
        super().__init__(lane, count, release)
        self.receive_attempts = 0
        self.failures = failures

    def receive_one(self) -> Mapping[str, Any] | None:
        self.receive_attempts += 1
        if self.receive_attempts <= self.failures:
            raise OSError("simulated transient SQS receive failure")
        return super().receive_one()


def _wait_until(predicate: Any, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.005)
    raise AssertionError("condition did not become true before timeout")


def test_lane_scheduler_enforces_two_one_one_and_leaves_excess_queued() -> None:
    release = threading.Event()
    workers = {
        BacktestLane.BASIC: BlockingLaneWorker(BacktestLane.BASIC, 3, release),
        BacktestLane.CUSTOM: BlockingLaneWorker(BacktestLane.CUSTOM, 2, release),
        BacktestLane.COMPETITION: BlockingLaneWorker(BacktestLane.COMPETITION, 2, release),
    }
    scheduler = BacktestLaneScheduler(workers=workers)

    scheduler.poll_once()
    _wait_until(lambda: sum(len(worker.started) for worker in workers.values()) == 4)

    assert len(workers[BacktestLane.BASIC].started) == 2
    assert len(workers[BacktestLane.CUSTOM].started) == 1
    assert len(workers[BacktestLane.COMPETITION].started) == 1
    assert len(workers[BacktestLane.BASIC].messages) == 1
    assert len(workers[BacktestLane.CUSTOM].messages) == 1
    assert len(workers[BacktestLane.COMPETITION].messages) == 1
    assert scheduler.active_count == 4

    # No slot is available, so another poll must not receive (and hide) more work.
    scheduler.poll_once()
    assert [len(worker.received) for worker in workers.values()] == [2, 1, 1]

    release.set()
    scheduler.wait_for_idle(timeout=2)


def test_lane_environment_builds_three_queue_configs_with_two_one_one_limits() -> None:
    environ = {
        "BACKTEST_WORKER_ID": "worker-a",
        "BACKTEST_JOB_HANDLER": "example:handler",
        "BACKTEST_EXECUTION_KEY_STORE": "example:store",
        **{
            f"BACKTEST_{lane.value.upper()}_{suffix}": f"https://sqs.local/{lane.value}/{suffix.lower()}"
            for lane in BacktestLane
            for suffix in ("QUEUE_URL", "DLQ_URL")
        },
    }

    configs, limits, global_limit = _lane_configs_from_env(environ)

    assert set(configs) == set(BacktestLane)
    assert {lane: config.max_messages for lane, config in configs.items()} == dict.fromkeys(BacktestLane, 1)
    assert limits == {
        BacktestLane.BASIC: 2,
        BacktestLane.CUSTOM: 1,
        BacktestLane.COMPETITION: 1,
    }
    assert global_limit == 4


def test_lane_environment_rejects_partial_queue_configuration() -> None:
    with pytest.raises(WorkerConfigurationError, match="COMPETITION_DLQ_URL"):
        _lane_configs_from_env(
            {
                "BACKTEST_WORKER_ID": "worker-a",
                "BACKTEST_JOB_HANDLER": "example:handler",
                "BACKTEST_EXECUTION_KEY_STORE": "example:store",
                **{
                    f"BACKTEST_{lane.value.upper()}_{suffix}": "https://sqs.local/queue"
                    for lane in BacktestLane
                    for suffix in ("QUEUE_URL", "DLQ_URL")
                    if not (lane is BacktestLane.COMPETITION and suffix == "DLQ_URL")
                },
            }
        )


def test_request_intake_queues_must_be_complete_and_separate_from_execution_queues() -> None:
    environ = {
        "BACKTEST_BASIC_QUEUE_URL": "https://sqs/jobs-basic",
        "BACKTEST_CUSTOM_QUEUE_URL": "https://sqs/jobs-custom",
        "BACKTEST_COMPETITION_QUEUE_URL": "https://sqs/jobs-competition",
        "BACKTEST_BASIC_REQUEST_QUEUE_URL": "https://sqs/requests-basic",
        "BACKTEST_BASIC_REQUEST_DLQ_URL": "https://sqs/requests-basic-dlq",
        "BACKTEST_CUSTOM_REQUEST_QUEUE_URL": "https://sqs/requests-custom",
        "BACKTEST_CUSTOM_REQUEST_DLQ_URL": "https://sqs/requests-custom-dlq",
        "BACKTEST_COMPETITION_REQUEST_QUEUE_URL": "https://sqs/requests-competition",
        "BACKTEST_COMPETITION_REQUEST_DLQ_URL": "https://sqs/requests-competition-dlq",
        "BACKTEST_REQUEST_HANDLER": "backtest_engine.production:backtest_request_handler",
        "BACKTEST_REQUEST_RECEIPT_STORE": "backtest_engine.production:postgres_request_receipt_store",
    }

    configs = _request_configs_from_env(environ)

    assert configs[RequestLane.BASIC].queue_url == "https://sqs/requests-basic"
    assert configs[RequestLane.CUSTOM].queue_url == "https://sqs/requests-custom"
    assert configs[RequestLane.COMPETITION].queue_url == "https://sqs/requests-competition"

    environ["BACKTEST_CUSTOM_REQUEST_QUEUE_URL"] = environ["BACKTEST_CUSTOM_QUEUE_URL"]
    with pytest.raises(WorkerConfigurationError, match="must be distinct"):
        _request_configs_from_env(environ)


def test_lane_scheduler_round_robins_nonempty_lanes_without_starvation() -> None:
    release = threading.Event()
    workers = {
        BacktestLane.BASIC: BlockingLaneWorker(BacktestLane.BASIC, 20, release),
        BacktestLane.CUSTOM: BlockingLaneWorker(BacktestLane.CUSTOM, 1, release),
        BacktestLane.COMPETITION: BlockingLaneWorker(BacktestLane.COMPETITION, 1, release),
    }
    scheduler = BacktestLaneScheduler(workers=workers)

    scheduler.poll_once()
    _wait_until(lambda: sum(len(worker.started) for worker in workers.values()) == 4)

    assert scheduler.receive_order[:4] == (
        BacktestLane.BASIC,
        BacktestLane.CUSTOM,
        BacktestLane.COMPETITION,
        BacktestLane.BASIC,
    )
    release.set()
    scheduler.wait_for_idle(timeout=2)


def test_lane_scheduler_isolates_receive_failure_and_retries_after_capped_backoff() -> None:
    release = threading.Event()
    now = [100.0]
    basic = FailFirstReceiveLaneWorker(BacktestLane.BASIC, 1, release)
    custom = BlockingLaneWorker(BacktestLane.CUSTOM, 1, release)
    scheduler = BacktestLaneScheduler(
        workers={BacktestLane.BASIC: basic, BacktestLane.CUSTOM: custom},
        lane_limits={BacktestLane.BASIC: 1, BacktestLane.CUSTOM: 1},
        global_limit=2,
        receive_backoff_seconds=1.0,
        max_receive_backoff_seconds=2.0,
        monotonic=lambda: now[0],
    )

    scheduler.poll_once()
    _wait_until(lambda: custom.started == ["custom-0"])
    assert basic.receive_attempts == 1

    scheduler.poll_once()
    assert basic.receive_attempts == 1

    release.set()
    scheduler.wait_for_idle(timeout=2)
    now[0] += 1.0
    scheduler.poll_once()
    scheduler.wait_for_idle(timeout=2)

    assert basic.receive_attempts == 2
    assert basic.started == ["basic-0"]


def test_lane_scheduler_exponential_receive_backoff_stops_at_the_configured_cap() -> None:
    release = threading.Event()
    now = [100.0]
    worker = FailFirstReceiveLaneWorker(
        BacktestLane.BASIC,
        0,
        release,
        failures=4,
    )
    scheduler = BacktestLaneScheduler(
        workers={BacktestLane.BASIC: worker},
        lane_limits={BacktestLane.BASIC: 1},
        global_limit=1,
        receive_backoff_seconds=1.0,
        max_receive_backoff_seconds=2.0,
        monotonic=lambda: now[0],
    )

    scheduler.poll_once()
    now[0] += 0.999
    scheduler.poll_once()
    assert worker.receive_attempts == 1

    now[0] += 0.001
    scheduler.poll_once()
    now[0] += 1.999
    scheduler.poll_once()
    assert worker.receive_attempts == 2

    now[0] += 0.001
    scheduler.poll_once()
    now[0] += 2.0
    scheduler.poll_once()

    assert worker.receive_attempts == 4


class BlockingFinishStore(InMemoryExecutionKeyStore):
    def __init__(self) -> None:
        super().__init__()
        self.finish_started = threading.Event()
        self.allow_finish = threading.Event()

    def finish(
        self,
        key: str,
        status: ExecutionRecordStatus,
        *,
        now: datetime,
        claim=None,
    ) -> None:
        self.finish_started.set()
        assert self.allow_finish.wait(5)
        super().finish(key, status, now=now, claim=claim)


class OneMessageSqs:
    def __init__(self, message: Mapping[str, Any]) -> None:
        self.message = message
        self.received = False
        self.deleted: list[str] = []
        self.visibility_changes: list[int] = []

    def receive_message(self, **kwargs: Any) -> Mapping[str, Any]:
        if self.received:
            return {}
        self.received = True
        return {"Messages": [self.message]}

    def delete_message(self, **kwargs: Any) -> None:
        self.deleted.append(str(kwargs["ReceiptHandle"]))

    def change_message_visibility(self, **kwargs: Any) -> None:
        self.visibility_changes.append(int(kwargs["VisibilityTimeout"]))

    def send_message(self, **kwargs: Any) -> None:
        return None


class FailingExecutionStore(InMemoryExecutionKeyStore):
    def __init__(self, stage: str) -> None:
        super().__init__()
        self.stage = stage

    def claim(self, *args: Any, **kwargs: Any):
        if self.stage == "claim":
            raise OSError("simulated transient claim failure")
        return super().claim(*args, **kwargs)

    def finish(self, *args: Any, **kwargs: Any) -> None:
        if self.stage == "finish":
            raise OSError("simulated transient finish failure")
        return super().finish(*args, **kwargs)


@pytest.mark.parametrize("stage", ["claim", "finish"])
def test_scheduler_isolates_completed_future_infrastructure_failure_and_leaves_message_unacked(
    stage: str,
) -> None:
    failed_message = _message(BacktestLane.BASIC, 11)
    failed_client = OneMessageSqs(failed_message)
    failed_worker = BacktestWorker(
        client=failed_client,
        config=WorkerConfig(
            queue_url="https://sqs.local/basic",
            dead_letter_queue_url="https://sqs.local/basic-dlq",
            worker_id="lane-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=30),
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=10),
        ),
        handler=lambda job, context: JobOutcome(JobResult.SUCCEEDED),
        store=FailingExecutionStore(stage),
        clock=lambda: T0,
    )
    release = threading.Event()
    healthy_worker = BlockingLaneWorker(BacktestLane.CUSTOM, 1, release)
    scheduler = BacktestLaneScheduler(
        workers={BacktestLane.BASIC: failed_worker, BacktestLane.CUSTOM: healthy_worker},
        lane_limits={BacktestLane.BASIC: 1, BacktestLane.CUSTOM: 1},
        global_limit=2,
    )

    scheduler.poll_once()
    _wait_until(lambda: healthy_worker.started == ["custom-0"])
    release.set()
    completed = scheduler.wait_for_idle(timeout=2)

    assert [item.message_id for item in completed] == ["custom-0"]
    assert failed_client.deleted == []
    assert failed_client.visibility_changes == []


def test_scheduler_acknowledges_only_after_durable_finish() -> None:
    message = _message(BacktestLane.BASIC, 1)
    client = OneMessageSqs(message)
    store = BlockingFinishStore()
    worker = BacktestWorker(
        client=client,
        config=WorkerConfig(
            queue_url="https://sqs.local/basic",
            dead_letter_queue_url="https://sqs.local/basic-dlq",
            worker_id="lane-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=30),
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=10),
        ),
        handler=lambda job, context: JobOutcome(JobResult.SUCCEEDED),
        store=store,
        clock=lambda: T0,
    )
    scheduler = BacktestLaneScheduler(
        workers={BacktestLane.BASIC: worker},
        lane_limits={BacktestLane.BASIC: 1},
        global_limit=1,
    )

    scheduler.poll_once()
    assert store.finish_started.wait(2)
    assert client.deleted == []

    store.allow_finish.set()
    completed = scheduler.wait_for_idle(timeout=2)

    assert [item.disposition for item in completed] == [MessageDisposition.DELETED]
    assert client.deleted == [message["ReceiptHandle"]]


def test_scheduler_retry_releases_the_claim_and_returns_message_to_its_lane(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _message(BacktestLane.CUSTOM, 2)
    client = OneMessageSqs(message)

    class RetryReasonStore(InMemoryExecutionKeyStore):
        released_reason_code: str | None = None

        def release(
            self,
            key: str,
            *,
            now: datetime,
            claim: Any = None,
            reason_code: str | None = None,
        ) -> None:
            self.released_reason_code = reason_code
            super().release(key, now=now, claim=claim, reason_code=reason_code)

    store = RetryReasonStore()
    caplog.set_level(logging.INFO, logger="backtest_engine.worker")
    worker = BacktestWorker(
        client=client,
        config=WorkerConfig(
            queue_url="https://sqs.local/custom",
            dead_letter_queue_url="https://sqs.local/custom-dlq",
            worker_id="lane-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=30),
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=10),
        ),
        handler=lambda job, context: JobOutcome(JobResult.RETRY, reason_code="TRANSIENT_DEPENDENCY"),
        store=store,
        clock=lambda: T0,
    )
    scheduler = BacktestLaneScheduler(
        workers={BacktestLane.CUSTOM: worker},
        lane_limits={BacktestLane.CUSTOM: 1},
        global_limit=1,
    )

    scheduler.poll_once()
    completed = scheduler.wait_for_idle(timeout=2)

    assert [item.disposition for item in completed] == [MessageDisposition.RETURNED]
    assert client.visibility_changes == [0]
    assert client.deleted == []
    assert store.released_reason_code == "TRANSIENT_DEPENDENCY"
    assert "reason_code=TRANSIENT_DEPENDENCY" in caplog.text


def test_handler_exception_emits_traceback_and_a_stable_retry_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _message(BacktestLane.BASIC, 3)
    client = OneMessageSqs(message)

    def fail(_job: Mapping[str, Any], _context: Any) -> JobOutcome:
        raise OSError("simulated data read failure")

    worker = BacktestWorker(
        client=client,
        config=WorkerConfig(
            queue_url="https://sqs.local/basic",
            dead_letter_queue_url="https://sqs.local/basic-dlq",
            worker_id="lane-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=30),
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=10),
        ),
        handler=fail,
        store=InMemoryExecutionKeyStore(),
        clock=lambda: T0,
    )
    caplog.set_level(logging.INFO, logger="backtest_engine.worker")

    handled = worker.handle_message(message)

    assert handled.disposition is MessageDisposition.RETURNED
    assert handled.reason_code == "HANDLER_ERROR:OSError"
    assert "backtest job handler raised" in caplog.text
    assert "simulated data read failure" in caplog.text
    assert "reason_code=HANDLER_ERROR:OSError" in caplog.text
