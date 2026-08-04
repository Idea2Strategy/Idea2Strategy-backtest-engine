from __future__ import annotations

import json
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
        "BACKTEST_CUSTOM_REQUEST_QUEUE_URL": "https://sqs/requests-custom",
        "BACKTEST_CUSTOM_REQUEST_DLQ_URL": "https://sqs/requests-custom-dlq",
        "BACKTEST_COMPETITION_REQUEST_QUEUE_URL": "https://sqs/requests-competition",
        "BACKTEST_COMPETITION_REQUEST_DLQ_URL": "https://sqs/requests-competition-dlq",
        "BACKTEST_REQUEST_HANDLER": "backtest_engine.production:backtest_request_handler",
        "BACKTEST_REQUEST_RECEIPT_STORE": "backtest_engine.production:postgres_request_receipt_store",
    }

    configs = _request_configs_from_env(environ)

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


def test_scheduler_retry_releases_the_claim_and_returns_message_to_its_lane() -> None:
    message = _message(BacktestLane.CUSTOM, 2)
    client = OneMessageSqs(message)
    store = InMemoryExecutionKeyStore()
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
