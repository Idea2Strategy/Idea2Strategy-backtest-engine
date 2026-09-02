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
import logging
import threading
import time
import uuid
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pytest
from sqlalchemy import Engine, text

from backtest_engine import worker as worker_module
from backtest_engine.persistence import BacktestPersistence
from backtest_engine.recovery import QueueDispatchPolicy, StaleRunRecovery
from backtest_engine.wiring import PersistenceExecutionKeyStore
from backtest_engine.worker import (
    WORKER_EXECUTION_KEY_MAX_LENGTH,
    BacktestLane,
    BacktestLaneScheduler,
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
from d_task5_chaos import evidence_result, record_evidence, task5_run_id, wait_until
from persistence.support import make_run


RUN_ID = "55555555-5555-4555-8555-555555555555"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("environ", "expected_level"),
    [
        ({}, logging.INFO),
        ({"BACKTEST_LOG_LEVEL": "debug"}, logging.DEBUG),
    ],
)
def test_worker_entrypoint_configures_runtime_logging(
    monkeypatch: pytest.MonkeyPatch,
    environ: Mapping[str, str],
    expected_level: int,
) -> None:
    configured: list[dict[str, object]] = []
    monkeypatch.setattr(worker_module.logging, "basicConfig", lambda **values: configured.append(values))

    level_name = worker_module._configure_logging(environ)

    assert level_name == logging.getLevelName(expected_level)
    assert configured == [
        {
            "level": expected_level,
            "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            "force": True,
        }
    ]


def test_worker_entrypoint_rejects_an_unknown_log_level() -> None:
    with pytest.raises(WorkerConfigurationError, match="BACKTEST_LOG_LEVEL"):
        worker_module._configure_logging({"BACKTEST_LOG_LEVEL": "chatty"})


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
# LocalStack SQS (the shared fixture owns an isolated Testcontainers LocalStack)
# ==========================================================================


def _visible(sqs: Any, queue_url: str) -> int:
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return int(attributes["ApproximateNumberOfMessages"])


def _queue_counts(sqs: Any, queue_url: str) -> tuple[int, int]:
    attributes = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    return (
        int(attributes["ApproximateNumberOfMessages"]),
        int(attributes["ApproximateNumberOfMessagesNotVisible"]),
    )


class RecordingHandler:
    def __init__(self, outcome: JobOutcome | None = None) -> None:
        self.jobs: list[Mapping[str, Any]] = []
        self.contexts: list[JobContext] = []
        self._outcome = outcome or JobOutcome(JobResult.SUCCEEDED, result_hash="d" * 64)

    def __call__(self, job: Mapping[str, Any], context: JobContext) -> JobOutcome:
        self.jobs.append(job)
        self.contexts.append(context)
        return self._outcome


class RecordingQueue:
    def __init__(self) -> None:
        self.sent: list[dict[str, object]] = []
        self.deleted: list[dict[str, object]] = []
        self.visibility: list[dict[str, object]] = []

    def receive_message(self, **_kwargs: object) -> dict[str, object]:
        return {"Messages": []}

    def send_message(self, **kwargs: object) -> None:
        self.sent.append(kwargs)

    def delete_message(self, **kwargs: object) -> None:
        self.deleted.append(kwargs)

    def change_message_visibility(self, **kwargs: object) -> None:
        self.visibility.append(kwargs)


def _delivery(receive_count: int) -> dict[str, object]:
    return {
        "MessageId": f"message-{receive_count}",
        "ReceiptHandle": f"receipt-{receive_count}",
        "Body": _body(),
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


def test_final_retry_is_failed_and_dead_lettered_without_a_sixth_delivery() -> None:
    queue = RecordingQueue()
    handler = RecordingHandler(JobOutcome(JobResult.RETRY, reason_code="WORKER_TIMEOUT"))
    store = InMemoryExecutionKeyStore()
    worker = BacktestWorker(client=queue, config=_config(max_receive_count=5), handler=handler, store=store)

    handled = worker.handle_message(_delivery(5))

    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    assert handled.disposition is MessageDisposition.DEAD_LETTERED
    assert handled.reason_code == "MAX_ATTEMPTS_EXHAUSTED:WORKER_TIMEOUT"
    assert store.status(key) is ExecutionRecordStatus.FAILED
    assert store.run_terminal_failure(RUN_ID) == "MAX_ATTEMPTS_EXHAUSTED"
    assert len(handler.jobs) == 1
    assert queue.visibility == []
    assert queue.sent[0]["MessageAttributes"]["DeadLetterReason"]["StringValue"] == (
        "MAX_ATTEMPTS_EXHAUSTED:WORKER_TIMEOUT"
    )


def test_addressable_over_limit_delivery_repairs_run_before_dlq_without_execution() -> None:
    queue = RecordingQueue()
    handler = RecordingHandler()
    store = InMemoryExecutionKeyStore()
    worker = BacktestWorker(client=queue, config=_config(max_receive_count=5), handler=handler, store=store)

    handled = worker.handle_message(_delivery(6))

    assert handled.disposition is MessageDisposition.DEAD_LETTERED
    assert handled.reason_code == "MAX_ATTEMPTS_EXHAUSTED"
    assert store.run_terminal_failure(RUN_ID) == "MAX_ATTEMPTS_EXHAUSTED"
    assert handler.jobs == []


def test_over_limit_duplicate_never_fails_an_active_heartbeating_attempt() -> None:
    queue = RecordingQueue()
    handler = RecordingHandler()
    store = InMemoryExecutionKeyStore()
    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    active = store.claim(
        key,
        run_id=RUN_ID,
        owner="original-worker",
        now=datetime.now(timezone.utc),
        lease_duration=timedelta(seconds=30),
    )
    assert active.acquired
    worker = BacktestWorker(client=queue, config=_config(max_receive_count=5), handler=handler, store=store)

    handled = worker.handle_message(_delivery(6))

    assert handled.disposition is MessageDisposition.RETURNED
    assert handled.reason_code == "EXECUTION_KEY_HELD"
    assert store.status(key) is ExecutionRecordStatus.IN_PROGRESS
    assert store.run_terminal_failure(RUN_ID) is None
    assert handler.jobs == []
    assert queue.sent == []
    assert queue.deleted == []


@pytest.mark.docker
def test_cancellation_committed_before_claim_acknowledges_the_delivery_without_an_attempt(
    persistence: BacktestPersistence,
    admin_engine: Engine,
) -> None:
    """A cancelled queue entry is terminal work, not a held execution key.

    Removing the terminal-run projection from ``PersistenceExecutionKeyStore.claim``
    must make this test return the message to SQS and leave it redeliverable.
    """

    run = make_run(
        id=uuid.UUID(task5_run_id("cancellation-before-claim")),
        idempotency_key="TASK5:CANCEL-BEFORE-CLAIM",
    )
    with persistence.unit_of_work() as uow:
        accepted, created = uow.runs.accept(run)
        assert created
        cancelled = uow.runs.request_cancellation(
            accepted.id,
            reason_code="USER_CANCELLED",
        )
    assert cancelled.status.value == "CANCELLED"

    queue = RecordingQueue()
    handler = RecordingHandler()
    worker = BacktestWorker(
        client=queue,
        config=_config(),
        handler=handler,
        store=PersistenceExecutionKeyStore(persistence),
    )
    delivery = {
        "MessageId": "task5-cancel-before-claim",
        "ReceiptHandle": "task5-cancel-before-claim-receipt",
        "Body": _body(
            str(accepted.id),
            "TASK5:CANCEL-BEFORE-CLAIM",
        ),
        "Attributes": {"ApproximateReceiveCount": "1"},
    }

    handled = worker.handle_message(delivery)

    assert handled.disposition is MessageDisposition.DELETED
    assert handled.reason_code == "DUPLICATE_ALREADY_CANCELLED"
    assert handler.jobs == []
    assert len(queue.deleted) == 1
    assert queue.visibility == []
    with admin_engine.connect() as connection:
        attempt_count = connection.scalar(
            text("SELECT count(*) FROM backtest.run_attempts WHERE run_id=:run_id"),
            {"run_id": accepted.id},
        )
    assert attempt_count == 0
    recovery = StaleRunRecovery(
        persistence,
        max_attempts=3,
        queue_policy=QueueDispatchPolicy.from_environment({}),
    ).recover_once()
    assert recovery.requeued == recovery.failed == recovery.cancelled == 0
    with admin_engine.connect() as connection:
        nonterminal = connection.scalar(text("SELECT count(*) FROM backtest.runs WHERE status IN ('QUEUED','RUNNING')"))
    assert nonterminal == 0
    record_evidence(
        evidence_result(
            scenario="task5-cancellation-race-claim",
            terminal_state="CANCELLED",
            duration_seconds=0,
            run_id=str(accepted.id),
            attempt_lineage=("no-attempt-before-claim",),
            failure_reason="USER_CANCELLED",
            observations={
                "disposition": handled.disposition.value,
                "reason": handled.reason_code,
                "attempt_count": attempt_count,
                "nonterminal_after_recovery": nonterminal,
            },
        )
    )


@pytest.mark.docker
def test_task5_real_sqs_lane_saturation_preserves_2_1_1_fairness_and_live_leases(
    sqs: Any,
    persistence: BacktestPersistence,
    admin_engine: Engine,
) -> None:
    """Real SQS backlog stays visible above the BASIC/CUSTOM/COMPETITION caps."""

    suffix = uuid.uuid4().hex[:12]
    queue_urls: dict[BacktestLane, tuple[str, str]] = {}
    created_urls: list[str] = []
    for lane in BacktestLane:
        main = sqs.create_queue(
            QueueName=f"task5-{lane.value}-main-{suffix}",
            Attributes={"VisibilityTimeout": "30", "ReceiveMessageWaitTimeSeconds": "0"},
        )["QueueUrl"]
        dead = sqs.create_queue(QueueName=f"task5-{lane.value}-dlq-{suffix}")["QueueUrl"]
        queue_urls[lane] = (main, dead)
        created_urls.extend((main, dead))

    release = threading.Event()
    lock = threading.Lock()
    active = dict.fromkeys(BacktestLane, 0)
    peaks = dict.fromkeys(BacktestLane, 0)
    starts: list[BacktestLane] = []

    def handler_for(lane: BacktestLane) -> Any:
        def handle(_job: Mapping[str, Any], _context: JobContext) -> JobOutcome:
            with lock:
                active[lane] += 1
                peaks[lane] = max(peaks[lane], active[lane])
                starts.append(lane)
            assert release.wait(timeout=30), f"{lane.value} saturation gate was never released"
            with lock:
                active[lane] -= 1
            return JobOutcome(JobResult.SUCCEEDED, result_hash="a" * 64)

        return handle

    workers = {
        lane: _worker(
            sqs,
            queue_urls[lane],
            handler_for(lane),
            worker_id=f"task5-{lane.value}-worker",
            wait_time=timedelta(0),
        )
        for lane in BacktestLane
    }
    scheduler = BacktestLaneScheduler(
        workers=workers,
        idle_wait_seconds=0.05,
        receive_backoff_seconds=0.05,
        max_receive_backoff_seconds=0.1,
    )

    sentinel = make_run(
        id=uuid.UUID(task5_run_id("lane-saturation-live-sentinel")),
        idempotency_key="TASK5:LANE-SATURATION:LIVE-SENTINEL",
    )
    with persistence.unit_of_work() as uow:
        accepted_sentinel, created = uow.runs.accept(sentinel)
    assert created
    sentinel_key = worker_execution_key_for(str(accepted_sentinel.id), accepted_sentinel.idempotency_key)
    sentinel_store = PersistenceExecutionKeyStore(persistence)
    sentinel_claim = sentinel_store.claim(
        sentinel_key,
        run_id=str(accepted_sentinel.id),
        owner="task5-live-sentinel-worker",
        now=datetime.now(timezone.utc),
        lease_duration=timedelta(minutes=5),
    )
    assert sentinel_claim.acquired

    completed: list[Any] = []
    try:
        for lane in BacktestLane:
            for index in range(4):
                sqs.send_message(
                    QueueUrl=queue_urls[lane][0],
                    MessageBody=_body(
                        task5_run_id(f"lane-{lane.value}-{index}"),
                        f"TASK5:LANE:{lane.value.upper()}:{index}",
                    ),
                )

        scheduler.poll_once()
        wait_until(
            lambda: len(starts) == 4,
            description="all four 2/1/1 lane slots to start",
        )
        expected_counts = {
            BacktestLane.BASIC: (2, 2),
            BacktestLane.CUSTOM: (3, 1),
            BacktestLane.COMPETITION: (3, 1),
        }
        wait_until(
            lambda: all(_queue_counts(sqs, queue_urls[lane][0]) == counts for lane, counts in expected_counts.items()),
            description="excess lane work to remain visibly queued",
        )
        assert peaks == {
            BacktestLane.BASIC: 2,
            BacktestLane.CUSTOM: 1,
            BacktestLane.COMPETITION: 1,
        }
        assert set(starts) == set(BacktestLane)

        recovery = StaleRunRecovery(
            persistence,
            max_attempts=3,
            queue_policy=QueueDispatchPolicy.from_environment({}),
        ).recover_once()
        live = sql = None
        with admin_engine.connect() as connection:
            live = (
                connection.execute(
                    text(
                        "SELECT r.status,a.status AS attempt_status "
                        "FROM backtest.runs r JOIN backtest.run_attempts a ON a.run_id=r.id "
                        "WHERE r.id=:id"
                    ),
                    {"id": accepted_sentinel.id},
                )
                .mappings()
                .one()
            )
            sql = connection.scalar(
                text("SELECT count(*) FROM backtest.runs WHERE id=:id AND status IN ('QUEUED','RUNNING')"),
                {"id": accepted_sentinel.id},
            )
        assert dict(live) == {"status": "RUNNING", "attempt_status": "RUNNING"}
        assert sql == 1
        assert recovery.requeued == recovery.failed == recovery.cancelled == 0

        release.set()

        def drain() -> bool:
            completed.extend(scheduler.poll_once())
            return (
                len(completed) == 12
                and scheduler.active_count == 0
                and all(_queue_counts(sqs, queue_urls[lane][0]) == (0, 0) for lane in BacktestLane)
            )

        wait_until(drain, description="all saturated lane jobs to finish", timeout_seconds=60)
        assert all(item.disposition is MessageDisposition.DELETED for item in completed)
        assert all(_visible(sqs, queue_urls[lane][1]) == 0 for lane in BacktestLane)

        with persistence.unit_of_work() as uow:
            uow.runs.request_cancellation(accepted_sentinel.id, reason_code="USER_CANCELLED")
        sentinel_store.finish(
            sentinel_key,
            ExecutionRecordStatus.CANCELLED,
            now=datetime.now(timezone.utc),
            claim=sentinel_claim,
            reason_code="USER_CANCELLED",
            run_id=str(accepted_sentinel.id),
        )
        record_evidence(
            evidence_result(
                scenario="task5-lane-saturation-fairness",
                terminal_state="COMPLETED",
                duration_seconds=0,
                run_id=task5_run_id("lane-saturation-fairness"),
                attempt_lineage=tuple(f"{lane.value}:{index + 1}" for lane in BacktestLane for index in range(4)),
                observations={
                    "limits": {"basic": 2, "custom": 1, "competition": 1},
                    "peak": {lane.value: peaks[lane] for lane in BacktestLane},
                    "completed": len(completed),
                    "live_lease_preserved": True,
                },
                resource_peak={lane.value: peaks[lane] for lane in BacktestLane},
            )
        )
    finally:
        release.set()
        with contextlib.suppress(Exception):
            scheduler.request_stop()
            scheduler.wait_for_idle(timeout=30)
        for url in created_urls:
            with contextlib.suppress(Exception):
                sqs.delete_queue(QueueUrl=url)


def test_transient_durable_heartbeat_error_is_retried_without_abandoning_handler() -> None:
    class TransientHeartbeatStore(InMemoryExecutionKeyStore):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        def heartbeat(self, key, claim, *, lease_duration):  # type: ignore[no-untyped-def]
            self.calls += 1
            if self.calls == 1:
                raise OSError("temporary database interruption")
            return super().heartbeat(key, claim, lease_duration=lease_duration)

    queue = RecordingQueue()
    store = TransientHeartbeatStore()

    def slow_success(_job: Mapping[str, Any], _context: JobContext) -> JobOutcome:
        time.sleep(0.2)
        return JobOutcome(JobResult.SUCCEEDED, result_hash="a" * 64)

    worker = BacktestWorker(
        client=queue,
        config=_config(
            visibility_timeout=timedelta(seconds=1),
            heartbeat_interval=timedelta(milliseconds=50),
        ),
        handler=slow_success,
        store=store,
    )

    handled = worker.handle_message(_delivery(1))

    assert handled.disposition is MessageDisposition.DELETED
    assert store.calls >= 2
    assert len(queue.deleted) == 1


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
def test_long_poll_delivers_the_job_and_deletes_it_after_success(sqs: Any, queues: tuple[str, str]) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler()
    worker = _worker(sqs, queues, handler)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert len(handler.jobs) == 1
    assert handler.jobs[0]["backtestRunId"] == RUN_ID
    assert handler.contexts[0].worker_execution_key == worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    assert handler.contexts[0].receive_count == 1
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 0


@pytest.mark.docker
def test_at_least_once_redelivery_executes_the_job_exactly_once(sqs: Any, queues: tuple[str, str]) -> None:
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
def test_visibility_is_extended_while_the_job_is_still_running(sqs: Any, queues: tuple[str, str]) -> None:
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
def test_retryable_failure_returns_the_message_for_immediate_redelivery(sqs: Any, queues: tuple[str, str]) -> None:
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
def test_final_allowed_receive_is_failed_and_routed_to_the_dead_letter_queue(sqs: Any, queues: tuple[str, str]) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(JobOutcome(JobResult.RETRY, reason_code="TRANSIENT"))
    worker = _worker(sqs, queues, handler, max_receive_count=2)

    dispositions = [item.disposition for _ in range(2) for item in worker.poll_once()]

    assert dispositions == [
        MessageDisposition.RETURNED,
        MessageDisposition.DEAD_LETTERED,
    ]
    assert len(handler.jobs) == 2, "the job must not run again past max receives"
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 1
    dead = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=1, MessageAttributeNames=["All"])
    attributes = dead["Messages"][0]["MessageAttributes"]
    assert attributes["DeadLetterReason"]["StringValue"] == "MAX_ATTEMPTS_EXHAUSTED:TRANSIENT"


@pytest.mark.docker
def test_unparseable_message_is_dead_lettered_without_invoking_the_handler(sqs: Any, queues: tuple[str, str]) -> None:
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
def test_permanent_job_failure_is_dead_lettered_and_recorded(sqs: Any, queues: tuple[str, str]) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(JobOutcome(JobResult.PERMANENT_FAILURE, reason_code="REQUIRED_DATA_UNAVAILABLE"))
    store = InMemoryExecutionKeyStore()
    worker = _worker(sqs, queues, handler, store)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == ["REQUIRED_DATA_UNAVAILABLE"]
    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    assert store.status(key) is ExecutionRecordStatus.FAILED
    assert _visible(sqs, dlq) == 1


@pytest.mark.docker
def test_cancelled_job_is_acknowledged_without_dead_lettering(sqs: Any, queues: tuple[str, str]) -> None:
    main, dlq = queues
    sqs.send_message(QueueUrl=main, MessageBody=_body())
    handler = RecordingHandler(JobOutcome(JobResult.CANCELLED, reason_code="USER_CANCELLED"))
    store = InMemoryExecutionKeyStore()
    worker = _worker(sqs, queues, handler, store)

    handled = worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert [item.reason_code for item in handled] == ["USER_CANCELLED"]
    key = worker_execution_key_for(RUN_ID, "OFFICIAL_BACKTEST:bt4")
    assert store.status(key) is ExecutionRecordStatus.CANCELLED
    assert _visible(sqs, main) == 0
    assert _visible(sqs, dlq) == 0


@pytest.mark.docker
def test_graceful_shutdown_finishes_the_in_flight_message_then_returns(sqs: Any, queues: tuple[str, str]) -> None:
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
def test_a_second_worker_cannot_double_execute_a_message_in_flight_elsewhere(sqs: Any, queues: tuple[str, str]) -> None:
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
