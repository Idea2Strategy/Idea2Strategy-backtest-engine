"""BT4 / D18 + D29: the real SQS consumer and its attempt-durability CAS.

The previous ``tests/test_worker.py`` was deleted by spec section 4 because it
proved nothing about a worker whose entire body was ``Event().wait()``.

The queue tests here run against **LocalStack SQS** (real boto3, real
long-poll, real visibility timeouts, real dead-letter routing). They are
behind the existing ``docker`` marker, so the default ``-m 'not docker'`` run
stays Docker-free; run them with ``-m docker``.

The compare-and-swap tests need no queue and always run.
"""

from __future__ import annotations

import contextlib
import json
import os
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pytest

from backtest_engine.worker import (
    WORKER_EXECUTION_KEY_MAX_LENGTH,
    BacktestWorker,
    ExecutionRecordStatus,
    InMemoryExecutionKeyStore,
    JobContext,
    JobOutcome,
    JobResult,
    MessageDisposition,
    WorkerConfig,
    WorkerConfigurationError,
    _runtime_sqs_client,
    worker_execution_key_for,
)


SQS_ENDPOINT = os.environ.get("BACKTEST_TEST_SQS_ENDPOINT", "http://127.0.0.1:24566")
RUN_ID = "55555555-5555-4555-8555-555555555555"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_sqs_client_uses_the_explicit_runtime_region(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def client(service: str, **kwargs: object) -> object:
        captured.update(service=service, **kwargs)
        return object()

    monkeypatch.setattr(boto3, "client", client)

    _runtime_sqs_client(
        {
            "AWS_REGION": "ap-northeast-2",
            "AWS_ENDPOINT_URL_SQS": "http://localstack:4566",
        }
    )

    assert captured == {
        "service": "sqs",
        "endpoint_url": "http://localstack:4566",
        "region_name": "ap-northeast-2",
    }


def _body(run_id: str = RUN_ID, idempotency_key: str = "OFFICIAL_BACKTEST:bt4") -> str:
    return json.dumps(
        {
            "metadata": {"messageType": "OFFICIAL_BACKTEST_REQUESTED"},
            "backtestRunId": run_id,
            "idempotencyKey": idempotency_key,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


# ==========================================================================
# D29 -- compare-and-swap on worker_execution_key (no queue required)
# ==========================================================================


def test_execution_key_is_stable_across_redeliveries_and_fits_the_column() -> None:
    """The key must dedupe *redeliveries*, so the receive count cannot be in it."""
    first = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    second = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")

    assert first == second
    assert first == f"BACKTEST_RUN:{RUN_ID}:OFFICIAL_BACKTEST:bt4"
    assert len(first) <= WORKER_EXECUTION_KEY_MAX_LENGTH

    # backtest.run_attempts.worker_execution_key is varchar(160): a long
    # idempotency key must be folded, not truncated into a collision.
    long_key = worker_execution_key_for(RUN_ID, "X" * 400)
    assert len(long_key) <= WORKER_EXECUTION_KEY_MAX_LENGTH
    assert long_key != worker_execution_key_for(RUN_ID, "Y" * 400)


def test_only_one_of_many_concurrent_claims_wins_the_cas() -> None:
    store = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "concurrent")
    barrier = threading.Barrier(16)

    def claim(index: int) -> bool:
        barrier.wait()
        return store.claim(key, run_id=RUN_ID, owner=f"worker-{index}", now=T0).acquired

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(claim, range(16)))

    assert results.count(True) == 1
    assert store.status(key) is ExecutionRecordStatus.IN_PROGRESS


def test_a_redelivered_message_cannot_double_execute_a_finished_attempt() -> None:
    store = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "finished")

    first = store.claim(key, run_id=RUN_ID, owner="worker-a", now=T0)
    store.finish(key, ExecutionRecordStatus.SUCCEEDED, now=T0 + timedelta(seconds=5))
    second = store.claim(key, run_id=RUN_ID, owner="worker-b", now=T0 + timedelta(seconds=6))

    assert first.acquired
    assert not second.acquired
    assert second.existing_status is ExecutionRecordStatus.SUCCEEDED
    assert store.attempt_number(key) == 1


def test_a_retryable_release_lets_the_next_attempt_reclaim_with_a_new_number() -> None:
    store = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "retried")

    store.claim(key, run_id=RUN_ID, owner="worker-a", now=T0)
    store.release(key, now=T0 + timedelta(seconds=5))
    reclaimed = store.claim(key, run_id=RUN_ID, owner="worker-b", now=T0 + timedelta(seconds=6))

    assert reclaimed.acquired
    assert store.attempt_number(key) == 2
    assert store.owner(key) == "worker-b"


def test_permanently_failed_key_is_not_reclaimable() -> None:
    store = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "poisoned")

    store.claim(key, run_id=RUN_ID, owner="worker-a", now=T0)
    store.finish(key, ExecutionRecordStatus.FAILED, now=T0 + timedelta(seconds=1))
    again = store.claim(key, run_id=RUN_ID, owner="worker-b", now=T0 + timedelta(seconds=2))

    assert not again.acquired
    assert again.existing_status is ExecutionRecordStatus.FAILED


# ==========================================================================
# Configuration
# ==========================================================================


def _config(**overrides: Any) -> WorkerConfig:
    base: dict[str, Any] = {
        "queue_url": "https://sqs.local/queue",
        "dead_letter_queue_url": "https://sqs.local/dlq",
        "worker_id": "bt4-worker",
        "max_receive_count": 3,
        "visibility_timeout": timedelta(seconds=30),
        "wait_time": timedelta(seconds=20),
        "max_messages": 1,
        "heartbeat_interval": timedelta(seconds=10),
    }
    base.update(overrides)
    return WorkerConfig(**base)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"queue_url": ""}, "queue_url"),
        ({"dead_letter_queue_url": ""}, "dead_letter_queue_url"),
        ({"max_receive_count": 0}, "max_receive_count"),
        ({"wait_time": timedelta(seconds=21)}, "wait_time"),
        ({"max_messages": 11}, "max_messages"),
        ({"max_messages": 0}, "max_messages"),
        ({"visibility_timeout": timedelta(seconds=5)}, "heartbeat_interval"),
        ({"worker_id": "  "}, "worker_id"),
    ],
)
def test_configuration_is_validated(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(WorkerConfigurationError, match=message):
        _config(**overrides)


# ==========================================================================
# LocalStack SQS
# ==========================================================================


def _sqs_client() -> Any:
    import boto3

    return boto3.client(
        "sqs",
        endpoint_url=SQS_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


@pytest.fixture(scope="module")
def sqs() -> Any:
    boto3 = pytest.importorskip("boto3")
    assert boto3 is not None
    try:
        client = _sqs_client()
        client.list_queues()
    except Exception as exc:  # pragma: no cover - depends on the developer's machine
        pytest.skip(f"LocalStack SQS is not reachable at {SQS_ENDPOINT}: {exc}")
    return client


@pytest.fixture
def queues(sqs: Any) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:12]
    main = sqs.create_queue(
        QueueName=f"bt4-main-{suffix}",
        Attributes={"VisibilityTimeout": "30", "ReceiveMessageWaitTimeSeconds": "0"},
    )["QueueUrl"]
    dlq = sqs.create_queue(QueueName=f"bt4-dlq-{suffix}")["QueueUrl"]
    yield main, dlq
    for url in (main, dlq):
        with contextlib.suppress(Exception):  # best-effort cleanup
            sqs.delete_queue(QueueUrl=url)


def _visible(sqs: Any, queue_url: str) -> int:
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"])


class RecordingHandler:
    def __init__(self, outcome: JobOutcome | None = None) -> None:
        self.jobs: list[Mapping[str, Any]] = []
        self.contexts: list[JobContext] = []
        self._outcome = outcome or JobOutcome(JobResult.SUCCEEDED, result_hash="d" * 64)

    def __call__(self, job: Mapping[str, Any], context: JobContext) -> JobOutcome:
        self.jobs.append(job)
        self.contexts.append(context)
        return self._outcome


def _worker(
    sqs: Any,
    queues: tuple[str, str],
    handler: Any,
    store: InMemoryExecutionKeyStore | None = None,
    **overrides: Any,
) -> BacktestWorker:
    main, dlq = queues
    settings: dict[str, Any] = {
        "queue_url": main,
        "dead_letter_queue_url": dlq,
        "wait_time": timedelta(seconds=1),
        "visibility_timeout": timedelta(seconds=30),
        "heartbeat_interval": timedelta(seconds=10),
    }
    settings.update(overrides)
    return BacktestWorker(
        client=sqs,
        config=_config(**settings),
        handler=handler,
        store=store or InMemoryExecutionKeyStore(),
    )


@pytest.mark.docker
def test_long_poll_delivers_the_job_and_deletes_it_after_success(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler()
    worker = _worker(sqs, queues, handler)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert len(handler.jobs) == 1
    assert handler.jobs[0]["backtestRunId"] == RUN_ID
    assert handler.contexts[0].worker_execution_key == worker_execution_key_for(
        RUN_ID, "OFFICIAL_BACKTEST:bt4"
    )
    assert handler.contexts[0].receive_count == 1
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 0


@pytest.mark.docker
def test_at_least_once_redelivery_executes_the_job_exactly_once(
    sqs: Any, queues: tuple[str, str]
) -> None:
    """A duplicate of an already-succeeded key is acknowledged, not re-run."""
    main, _ = queues
    handler = RecordingHandler()
    store = InMemoryExecutionKeyStore()
    worker = _worker(sqs, queues, handler, store)

    sqs.send_message(QueueUrl=main, MessageBody=_body())
    first = worker.poll_once()
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    second = worker.poll_once()

    assert [item.disposition for item in first] == [MessageDisposition.DELETED]
    assert [item.disposition for item in second] == [MessageDisposition.DELETED]
    assert [item.reason_code for item in second] == ["DUPLICATE_ALREADY_SUCCEEDED"]
    assert len(handler.jobs) == 1
    assert _visible(sqs, main) == 0


@pytest.mark.docker
def test_visibility_is_extended_while_the_job_is_still_running(
    sqs: Any, queues: tuple[str, str]
) -> None:
    """Without heartbeats a 3s visibility timeout would redeliver mid-run."""
    main, _ = queues
    observed: list[int] = []

    def slow_handler(job: Mapping[str, Any], context: JobContext) -> JobOutcome:
        deadline = time.monotonic() + 6.0
        while time.monotonic() < deadline:
            time.sleep(0.25)
            observed.append(_visible(sqs, main))
        return JobOutcome(JobResult.SUCCEEDED, result_hash="e" * 64)

    sqs.send_message(QueueUrl=main, MessageBody=_body())
    worker = _worker(
        sqs,
        queues,
        slow_handler,
        visibility_timeout=timedelta(seconds=3),
        heartbeat_interval=timedelta(seconds=1),
    )

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert observed, "handler never sampled the queue"
    assert max(observed) == 0, "message became visible again while still in flight"
    assert worker.heartbeat_count >= 3


@pytest.mark.docker
def test_retryable_failure_returns_the_message_for_immediate_redelivery(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(JobOutcome(JobResult.RETRY, reason_code="OBJECT_STORE_TEMPORARY"))
    store = InMemoryExecutionKeyStore()
    worker = _worker(sqs, queues, handler, store)

    first = worker.poll_once()
    second = worker.poll_once()

    assert [item.disposition for item in first] == [MessageDisposition.RETURNED]
    assert [item.disposition for item in second] == [MessageDisposition.RETURNED]
    assert [context.receive_count for context in handler.contexts] == [1, 2]
    # The CAS record was released, so the retry is a *new* attempt, not a
    # blocked duplicate.
    assert store.attempt_number(handler.contexts[0].worker_execution_key) == 2
    assert _visible(sqs, dlq) == 0


@pytest.mark.docker
def test_message_past_max_receive_count_is_routed_to_the_dead_letter_queue(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(JobOutcome(JobResult.RETRY, reason_code="TRANSIENT"))
    worker = _worker(sqs, queues, handler, max_receive_count=2)

    dispositions = [
        item.disposition for _ in range(3) for item in worker.poll_once()
    ]

    assert dispositions == [
        MessageDisposition.RETURNED,
        MessageDisposition.RETURNED,
        MessageDisposition.DEAD_LETTERED,
    ]
    assert len(handler.jobs) == 2, "the job must not run again past max receives"
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 1
    dead = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=1, MessageAttributeNames=["All"])
    attributes = dead["Messages"][0]["MessageAttributes"]
    assert attributes["DeadLetterReason"]["StringValue"] == "MAX_RECEIVE_COUNT_EXCEEDED"


@pytest.mark.docker
def test_unparseable_message_is_dead_lettered_without_invoking_the_handler(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody="{not json")
    handler = RecordingHandler()
    worker = _worker(sqs, queues, handler)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == ["MESSAGE_NOT_PARSEABLE"]
    assert handler.jobs == []
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 1


@pytest.mark.docker
def test_permanent_job_failure_is_dead_lettered_and_recorded(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(
        JobOutcome(JobResult.PERMANENT_FAILURE, reason_code="REQUIRED_DATA_UNAVAILABLE")
    )
    store = InMemoryExecutionKeyStore()
    worker = _worker(sqs, queues, handler, store)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == ["REQUIRED_DATA_UNAVAILABLE"]
    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    assert store.status(key) is ExecutionRecordStatus.FAILED
    assert _visible(sqs, dlq) == 1


@pytest.mark.docker
def test_graceful_shutdown_finishes_the_in_flight_message_then_returns(
    sqs: Any, queues: tuple[str, str]
) -> None:
    main, _ = queues
    started = threading.Event()
    finished = threading.Event()

    def slow_handler(job: Mapping[str, Any], context: JobContext) -> JobOutcome:
        started.set()
        time.sleep(2.0)
        finished.set()
        return JobOutcome(JobResult.SUCCEEDED, result_hash="f" * 64)

    sqs.send_message(QueueUrl=main, MessageBody=_body())
    worker = _worker(sqs, queues, slow_handler)
    thread = threading.Thread(target=worker.run, daemon=True)
    thread.start()

    assert started.wait(timeout=20), "worker never picked the message up"
    worker.request_stop()
    thread.join(timeout=30)

    assert not thread.is_alive(), "run() did not return after request_stop()"
    assert finished.is_set(), "shutdown interrupted an in-flight job"
    assert worker.stopped
    assert _visible(sqs, main) == 0


@pytest.mark.docker
def test_a_second_worker_cannot_double_execute_a_message_in_flight_elsewhere(
    sqs: Any, queues: tuple[str, str]
) -> None:
    """The CAS, not the visibility timeout, is what makes this safe."""
    main, _ = queues
    shared = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    shared.claim(key, run_id=RUN_ID, owner="other-worker", now=T0)

    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler()
    worker = _worker(sqs, queues, handler, shared)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.RETURNED]
    assert [item.reason_code for item in handled] == ["EXECUTION_KEY_HELD"]
    assert handler.jobs == []
