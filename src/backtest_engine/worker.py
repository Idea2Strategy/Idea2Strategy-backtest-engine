"""D18 -- the durable SQS consumer, and D29's attempt-durability CAS.

What the previous 26-line stub admitted it did not do, and this module does:

* **long-poll receive** with an explicit visibility timeout and receive count;
* **visibility heartbeats** so a long replay is not redelivered mid-flight;
* **at-least-once with idempotent processing**, enforced by a compare-and-swap
  on ``backtest.run_attempts.worker_execution_key`` (unique in
  ``db/schema.dbml``) -- a redelivered message cannot double-execute;
* **explicit dead-letter routing** once ``ApproximateReceiveCount`` passes the
  configured maximum, and for messages that can never parse;
* **graceful shutdown** that finishes the in-flight message and then returns.

The queue client is the boto3 SQS interface. The execution-key store is a
narrow :class:`typing.Protocol`; :class:`InMemoryExecutionKeyStore` is the
reference implementation and the Postgres-backed one is BT-e's to supply.
"""

from __future__ import annotations

import gc
import hashlib
import importlib
import json
import logging
import os
import signal
import threading
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Protocol


__all__ = [
    "WORKER_EXECUTION_KEY_MAX_LENGTH",
    "WORKER_VERSION",
    "BacktestLane",
    "BacktestLaneScheduler",
    "BacktestWorker",
    "ExecutionClaim",
    "ExecutionKeyStore",
    "ExecutionRecordStatus",
    "HandledMessage",
    "InMemoryExecutionKeyStore",
    "JobContext",
    "JobHandler",
    "JobOutcome",
    "JobResult",
    "MessageDisposition",
    "SqsClient",
    "WorkerConfig",
    "WorkerConfigurationError",
    "load_factory",
    "run",
    "worker_execution_key_for",
]


WORKER_VERSION = "backtest-worker:1.0.0"

#: ``backtest.run_attempts.worker_execution_key`` is ``varchar(160)``.
WORKER_EXECUTION_KEY_MAX_LENGTH = 160

_KEY_PREFIX = "BACKTEST_RUN:"

#: SQS caps a single long poll at 20 seconds and a batch at 10 messages.
_MAX_WAIT = timedelta(seconds=20)
_MAX_BATCH = 10

_LOGGER = logging.getLogger(__name__)


class WorkerConfigurationError(ValueError):
    """Raised when the worker is asked to run with unusable settings."""


class JobResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    RETRY = "RETRY"
    PERMANENT_FAILURE = "PERMANENT_FAILURE"


class ExecutionRecordStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


class MessageDisposition(StrEnum):
    DELETED = "DELETED"
    RETURNED = "RETURNED"
    DEAD_LETTERED = "DEAD_LETTERED"


class BacktestLane(StrEnum):
    """Infrastructure work lanes; request classification remains upstream-owned."""

    BASIC = "basic"
    CUSTOM = "custom"
    COMPETITION = "competition"


def worker_execution_key_for(run_id: str, idempotency_key: str) -> str:
    """The CAS key for one *message*, stable across every redelivery of it.

    Deliberately free of the receive count and of any worker identity: those
    change between deliveries of the same job, and a key that changed with them
    would let a redelivery execute a second time.

    Long inputs are folded into a digest rather than truncated, because
    truncating two distinct keys to ``varchar(160)`` would make them collide and
    silently suppress a legitimate second run.
    """
    if not run_id or not idempotency_key:
        raise WorkerConfigurationError("run_id and idempotency_key must not be empty")
    key = f"{_KEY_PREFIX}{run_id}:{idempotency_key}"
    if len(key) <= WORKER_EXECUTION_KEY_MAX_LENGTH:
        return key
    digest = hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
    folded = f"{_KEY_PREFIX}{run_id}:sha256-{digest}"
    if len(folded) > WORKER_EXECUTION_KEY_MAX_LENGTH:  # pragma: no cover - run_id is a UUID
        raise WorkerConfigurationError("run_id is too long to form an execution key")
    return folded


# ==========================================================================
# Protocols
# ==========================================================================


class SqsClient(Protocol):
    """The boto3 SQS surface this worker uses. LocalStack satisfies it too."""

    def receive_message(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def delete_message(self, **kwargs: Any) -> Any: ...

    def change_message_visibility(self, **kwargs: Any) -> Any: ...

    def send_message(self, **kwargs: Any) -> Any: ...


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    acquired: bool
    attempt_number: int
    existing_status: ExecutionRecordStatus | None = None
    attempt_id: str | None = None
    claim_token: str | None = None


class ExecutionKeyStore(Protocol):
    """Compare-and-swap on ``worker_execution_key``.

    Must be satisfied by BT-e's persistence layer, where ``claim`` is an
    ``INSERT ... ON CONFLICT (worker_execution_key) DO NOTHING`` whose affected
    row count *is* the CAS result.
    """

    def claim(
        self,
        key: str,
        *,
        run_id: str,
        owner: str,
        now: datetime,
        lease_duration: timedelta | None = None,
    ) -> ExecutionClaim: ...

    def heartbeat(
        self, key: str, claim: ExecutionClaim, *, lease_duration: timedelta
    ) -> str | None: ...

    def release(
        self,
        key: str,
        *,
        now: datetime,
        claim: ExecutionClaim | None = None,
        reason_code: str | None = None,
    ) -> None: ...

    def finish(
        self,
        key: str,
        status: ExecutionRecordStatus,
        *,
        now: datetime,
        claim: ExecutionClaim | None = None,
        reason_code: str | None = None,
        run_id: str | None = None,
        run_failure_code: str | None = None,
    ) -> None: ...

    def record_run_failure(
        self, key: str, run_id: str, failure_code: str, *, now: datetime
    ) -> ExecutionRecordStatus: ...

    def recover_stale(self, *, max_attempts: int, queue_policy: Any) -> Any: ...

    def status(self, key: str) -> ExecutionRecordStatus | None: ...


@dataclass(slots=True)
class _ExecutionRecord:
    run_id: str
    owner: str
    attempt_number: int
    status: ExecutionRecordStatus
    updated_at: datetime


class InMemoryExecutionKeyStore:
    """Thread-safe reference CAS, mirroring the unique-index semantics exactly."""

    def __init__(self) -> None:
        self._records: dict[str, _ExecutionRecord] = {}
        #: Attempt numbers survive a release, so a retry is attempt N+1 rather
        #: than a second attempt 1. ``backtest.run_attempts`` keys on
        #: ``(run_id, attempt_number)``, so reusing 1 would collide.
        self._released_attempts: dict[str, int] = {}
        self._run_failures: dict[str, str] = {}
        self._lock = threading.Lock()

    def claim(
        self,
        key: str,
        *,
        run_id: str,
        owner: str,
        now: datetime,
        lease_duration: timedelta | None = None,
    ) -> ExecutionClaim:
        with self._lock:
            record = self._records.get(key)
            if record is None:
                attempt_number = self._released_attempts.pop(key, 0) + 1
                self._records[key] = _ExecutionRecord(
                    run_id, owner, attempt_number, ExecutionRecordStatus.IN_PROGRESS, now
                )
                return ExecutionClaim(acquired=True, attempt_number=attempt_number)
            # IN_PROGRESS means another worker holds it; SUCCEEDED and FAILED
            # are both terminal. None of the three may be re-executed.
            return ExecutionClaim(
                acquired=False,
                attempt_number=record.attempt_number,
                existing_status=record.status,
            )

    def heartbeat(
        self, key: str, claim: ExecutionClaim, *, lease_duration: timedelta
    ) -> str | None:
        return None

    def release(
        self,
        key: str,
        *,
        now: datetime,
        claim: ExecutionClaim | None = None,
        reason_code: str | None = None,
    ) -> None:
        """Hand a retryable attempt back so the next delivery can re-claim it."""
        with self._lock:
            record = self._records.pop(key, None)
            if record is not None:
                self._released_attempts[key] = record.attempt_number

    def finish(
        self,
        key: str,
        status: ExecutionRecordStatus,
        *,
        now: datetime,
        claim: ExecutionClaim | None = None,
        reason_code: str | None = None,
        run_id: str | None = None,
        run_failure_code: str | None = None,
    ) -> None:
        if status is ExecutionRecordStatus.IN_PROGRESS:
            raise ValueError("finish requires a terminal status")
        with self._lock:
            record = self._records.get(key)
            if record is None:
                raise KeyError(f"no execution record for {key}")
            record.status = status
            record.updated_at = now
            if status is ExecutionRecordStatus.FAILED and run_id is not None:
                self._run_failures[run_id] = run_failure_code or reason_code or "EXECUTION_FAILED"

    def record_run_failure(
        self, key: str, run_id: str, failure_code: str, *, now: datetime
    ) -> ExecutionRecordStatus:
        del now
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                if record.status is ExecutionRecordStatus.IN_PROGRESS:
                    return ExecutionRecordStatus.IN_PROGRESS
                return record.status
            self._run_failures.setdefault(run_id, failure_code)
            return ExecutionRecordStatus.FAILED

    def run_terminal_failure(self, run_id: str) -> str | None:
        with self._lock:
            return self._run_failures.get(run_id)

    def recover_stale(self, *, max_attempts: int, queue_policy: Any) -> None:
        del max_attempts, queue_policy

    def status(self, key: str) -> ExecutionRecordStatus | None:
        with self._lock:
            record = self._records.get(key)
            return record.status if record else None

    def attempt_number(self, key: str) -> int | None:
        with self._lock:
            record = self._records.get(key)
            if record is not None:
                return record.attempt_number
            return self._released_attempts.get(key)

    def owner(self, key: str) -> str | None:
        with self._lock:
            record = self._records.get(key)
            return record.owner if record else None


@dataclass(frozen=True, slots=True)
class JobContext:
    worker_execution_key: str
    attempt_number: int
    receive_count: int
    message_id: str
    worker_id: str
    attempt_id: str | None = None
    claim_token: str | None = None
    cancellation_reason: Callable[[], str | None] | None = None


class _CancellationSignal:
    def __init__(self) -> None:
        self._reason: str | None = None
        self._lock = threading.Lock()

    def request(self, reason: str) -> None:
        with self._lock:
            self._reason = reason

    def reason(self) -> str | None:
        with self._lock:
            return self._reason


@dataclass(frozen=True, slots=True)
class JobOutcome:
    result: JobResult
    reason_code: str | None = None
    result_hash: str | None = None

    def __post_init__(self) -> None:
        if self.result is not JobResult.SUCCEEDED and not self.reason_code:
            raise ValueError("a non-successful outcome must carry a reason_code")


class JobHandler(Protocol):
    def __call__(self, job: Mapping[str, Any], context: JobContext) -> JobOutcome: ...


@dataclass(frozen=True, slots=True)
class HandledMessage:
    message_id: str
    disposition: MessageDisposition
    reason_code: str | None
    worker_execution_key: str | None


# ==========================================================================
# Configuration
# ==========================================================================


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    queue_url: str
    dead_letter_queue_url: str
    worker_id: str
    max_receive_count: int
    visibility_timeout: timedelta
    wait_time: timedelta
    max_messages: int
    heartbeat_interval: timedelta

    def __post_init__(self) -> None:
        for name in ("queue_url", "dead_letter_queue_url", "worker_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise WorkerConfigurationError(f"{name} must not be blank")
        if self.max_receive_count < 1:
            raise WorkerConfigurationError("max_receive_count must be at least 1")
        if not 1 <= self.max_messages <= _MAX_BATCH:
            raise WorkerConfigurationError(f"max_messages must be between 1 and {_MAX_BATCH}")
        if not timedelta(0) <= self.wait_time <= _MAX_WAIT:
            raise WorkerConfigurationError("wait_time must be between 0 and 20 seconds (the SQS long-poll cap)")
        if self.visibility_timeout <= timedelta(0):
            raise WorkerConfigurationError("visibility_timeout must be positive")
        if self.heartbeat_interval <= timedelta(0):
            raise WorkerConfigurationError("heartbeat_interval must be positive")
        if self.heartbeat_interval >= self.visibility_timeout:
            raise WorkerConfigurationError(
                "heartbeat_interval must be shorter than visibility_timeout, "
                "otherwise the message is redelivered before the first extension"
            )


# ==========================================================================
# The worker
# ==========================================================================


class BacktestWorker:
    """A durable, idempotent, at-least-once SQS consumer for backtest jobs."""

    def __init__(
        self,
        *,
        client: SqsClient,
        config: WorkerConfig,
        handler: JobHandler,
        store: ExecutionKeyStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._handler = handler
        self._store = store
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = threading.Event()
        self._heartbeat_count = 0

    # -- lifecycle --------------------------------------------------------

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    @property
    def heartbeat_count(self) -> int:
        return self._heartbeat_count

    def request_stop(self, *_: object) -> None:
        """Signal shutdown. The in-flight message is always finished first."""
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            self.poll_once()

    # -- one poll cycle ---------------------------------------------------

    def poll_once(self) -> tuple[HandledMessage, ...]:
        messages = self._receive()
        return tuple(self._handle(message) for message in messages)

    def receive_one(self) -> Mapping[str, Any] | None:
        """Receive at most one message without blocking.

        The multi-lane scheduler uses a non-blocking receive so an empty lane
        cannot hold every other lane behind a 20-second SQS long poll. It also
        calls this only after reserving both a lane slot and a global slot, so
        work beyond the configured concurrency stays visible in SQS.
        """
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=0,
            VisibilityTimeout=int(self._config.visibility_timeout.total_seconds()),
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        messages = list(response.get("Messages", []))
        return messages[0] if messages else None

    def handle_message(self, message: Mapping[str, Any]) -> HandledMessage:
        """Handle one previously received message using the durable lifecycle."""
        return self._handle(message)

    def _receive(self) -> list[Mapping[str, Any]]:
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=self._config.max_messages,
            WaitTimeSeconds=int(self._config.wait_time.total_seconds()),
            VisibilityTimeout=int(self._config.visibility_timeout.total_seconds()),
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        return list(response.get("Messages", []))

    def _handle(self, message: Mapping[str, Any]) -> HandledMessage:
        message_id = str(message.get("MessageId", ""))
        receipt = str(message["ReceiptHandle"])
        receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))

        try:
            job = json.loads(str(message.get("Body", "")))
            if not isinstance(job, Mapping):
                raise TypeError("message body must be a JSON object")
            run_id = str(job["backtestRunId"])
            idempotency_key = str(job["idempotencyKey"])
            key = worker_execution_key_for(run_id, idempotency_key)
        except Exception:
            return self._dead_letter(message, receipt, "MESSAGE_NOT_PARSEABLE", message_id, None)

        if receive_count > self._config.max_receive_count:
            # Parse first so an addressable legacy message that already crossed
            # the limit repairs its durable run instead of disappearing into DLQ.
            existing = self._store.record_run_failure(
                key, run_id, "MAX_ATTEMPTS_EXHAUSTED", now=self._clock()
            )
            if existing is ExecutionRecordStatus.IN_PROGRESS:
                return HandledMessage(
                    message_id, MessageDisposition.RETURNED, "EXECUTION_KEY_HELD", key
                )
            if existing in (ExecutionRecordStatus.SUCCEEDED, ExecutionRecordStatus.CANCELLED):
                self._delete(receipt)
                return HandledMessage(
                    message_id, MessageDisposition.DELETED,
                    f"DUPLICATE_ALREADY_{existing.value}", key,
                )
            return self._dead_letter(
                message, receipt, "MAX_ATTEMPTS_EXHAUSTED", message_id, key
            )

        claim = self._store.claim(
            key,
            run_id=run_id,
            owner=self._config.worker_id,
            now=self._clock(),
            lease_duration=self._config.visibility_timeout,
        )
        if not claim.acquired:
            return self._on_duplicate(message, receipt, claim, message_id, key)

        cancellation = _CancellationSignal()
        context = JobContext(
            worker_execution_key=key,
            attempt_number=claim.attempt_number,
            receive_count=receive_count,
            message_id=message_id,
            worker_id=self._config.worker_id,
            attempt_id=claim.attempt_id,
            claim_token=claim.claim_token,
            cancellation_reason=cancellation.reason,
        )
        outcome = self._invoke(job, context, receipt, key, claim, cancellation)
        self._release_attempt_resources()

        if outcome.result is JobResult.SUCCEEDED:
            self._store.finish(key, ExecutionRecordStatus.SUCCEEDED, now=self._clock(), claim=claim)
            self._delete(receipt)
            return HandledMessage(message_id, MessageDisposition.DELETED, None, key)

        if outcome.result is JobResult.CANCELLED:
            self._store.finish(
                key,
                ExecutionRecordStatus.CANCELLED,
                now=self._clock(),
                claim=claim,
                reason_code=outcome.reason_code,
                run_id=run_id,
            )
            self._delete(receipt)
            return HandledMessage(message_id, MessageDisposition.DELETED, outcome.reason_code, key)

        if outcome.result is JobResult.RETRY:
            # Release the CAS record so the redelivery is a *new* attempt, then
            # make the message immediately visible again.
            reason_code = outcome.reason_code or "RETRY_REQUESTED"
            _LOGGER.info(
                "backtest job retry released run_id=%s attempt=%s receive_count=%s "
                "reason_code=%s message_id=%s",
                run_id,
                context.attempt_number,
                context.receive_count,
                reason_code,
                message_id,
            )
            if receive_count >= self._config.max_receive_count:
                terminal_reason = f"MAX_ATTEMPTS_EXHAUSTED:{reason_code}"
                self._store.finish(
                    key,
                    ExecutionRecordStatus.FAILED,
                    now=self._clock(),
                    claim=claim,
                    reason_code=reason_code,
                    run_id=run_id,
                    run_failure_code="MAX_ATTEMPTS_EXHAUSTED",
                )
                return self._dead_letter(
                    message, receipt, terminal_reason, message_id, key
                )
            self._store.release(
                key,
                now=self._clock(),
                claim=claim,
                reason_code=reason_code,
            )
            self._return_to_queue(receipt)
            return HandledMessage(message_id, MessageDisposition.RETURNED, reason_code, key)

        self._store.finish(
            key,
            ExecutionRecordStatus.FAILED,
            now=self._clock(),
            claim=claim,
            reason_code=outcome.reason_code,
            run_id=run_id,
            run_failure_code=outcome.reason_code,
        )
        return self._dead_letter(message, receipt, outcome.reason_code or "PERMANENT_FAILURE", message_id, key)

    @staticmethod
    def _release_attempt_resources() -> None:
        """Return large per-attempt Arrow buffers before polling the next job."""
        gc.collect()
        try:
            pyarrow = importlib.import_module("pyarrow")
            pyarrow.default_memory_pool().release_unused()
        except (ImportError, AttributeError):  # pragma: no cover - optional runtime detail
            return

    def _invoke(
        self,
        job: Mapping[str, Any],
        context: JobContext,
        receipt: str,
        key: str,
        claim: ExecutionClaim,
        cancellation: _CancellationSignal,
    ) -> JobOutcome:
        """Run the handler with a visibility heartbeat alongside it."""
        done = threading.Event()
        beat = threading.Thread(
            target=self._beat,
            args=(receipt, done, key, claim, cancellation),
            daemon=True,
            name="bt4-heartbeat",
        )
        beat.start()
        try:
            return self._handler(job, context)
        except Exception as exc:
            _LOGGER.exception(
                "backtest job handler raised run_id=%s attempt=%s receive_count=%s message_id=%s",
                job.get("backtestRunId"),
                context.attempt_number,
                context.receive_count,
                context.message_id,
            )
            return JobOutcome(JobResult.RETRY, reason_code=f"HANDLER_ERROR:{type(exc).__name__}")
        finally:
            done.set()
            beat.join(timeout=self._config.visibility_timeout.total_seconds())

    def _beat(
        self,
        receipt: str,
        done: threading.Event,
        key: str,
        claim: ExecutionClaim,
        cancellation: _CancellationSignal,
    ) -> None:
        interval = self._config.heartbeat_interval.total_seconds()
        timeout = int(self._config.visibility_timeout.total_seconds())
        while not done.wait(interval):
            reason: str | None = None
            for retry in range(3):
                try:
                    reason = self._store.heartbeat(
                        key, claim, lease_duration=self._config.visibility_timeout
                    )
                    break
                except Exception:
                    if retry == 2:
                        _LOGGER.exception(
                            "durable backtest heartbeat failed after retries key=%s", key
                        )
                        cancellation.request("WORKER_HEARTBEAT_UNAVAILABLE")
                        return
                    if done.wait(min(1.0, max(0.05, interval / 10) * (retry + 1))):
                        return
            if reason is not None:
                cancellation.request(reason)
            try:
                self._client.change_message_visibility(
                    QueueUrl=self._config.queue_url,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=timeout,
                )
            except Exception:
                # Durable fencing remains authoritative even if this receipt is
                # stale. Keep heartbeating Postgres so a redelivery cannot start
                # a second execution while the original handler is healthy.
                _LOGGER.warning(
                    "SQS visibility heartbeat failed; durable lease remains active key=%s",
                    key,
                    exc_info=True,
                )
            self._heartbeat_count += 1

    def _on_duplicate(
        self,
        message: Mapping[str, Any],
        receipt: str,
        claim: ExecutionClaim,
        message_id: str,
        key: str,
    ) -> HandledMessage:
        if claim.existing_status is ExecutionRecordStatus.SUCCEEDED:
            # At-least-once delivery of work that is already done: acknowledge
            # it, do not run it again.
            self._delete(receipt)
            return HandledMessage(
                message_id,
                MessageDisposition.DELETED,
                "DUPLICATE_ALREADY_SUCCEEDED",
                key,
            )
        if claim.existing_status is ExecutionRecordStatus.CANCELLED:
            self._delete(receipt)
            return HandledMessage(
                message_id,
                MessageDisposition.DELETED,
                "DUPLICATE_ALREADY_CANCELLED",
                key,
            )
        if claim.existing_status is ExecutionRecordStatus.FAILED:
            return self._dead_letter(message, receipt, "DUPLICATE_ALREADY_FAILED", message_id, key)
        # Another worker holds the key. Leave the message for the queue to
        # redeliver after its visibility timeout, without resetting it to 0.
        return HandledMessage(message_id, MessageDisposition.RETURNED, "EXECUTION_KEY_HELD", key)

    # -- queue effects ----------------------------------------------------

    def _delete(self, receipt: str) -> None:
        self._client.delete_message(QueueUrl=self._config.queue_url, ReceiptHandle=receipt)

    def _return_to_queue(self, receipt: str) -> None:
        self._client.change_message_visibility(
            QueueUrl=self._config.queue_url, ReceiptHandle=receipt, VisibilityTimeout=0
        )

    def _dead_letter(
        self,
        message: Mapping[str, Any],
        receipt: str,
        reason_code: str,
        message_id: str,
        key: str | None,
    ) -> HandledMessage:
        attributes: dict[str, Any] = {
            "DeadLetterReason": {"DataType": "String", "StringValue": reason_code},
            "SourceQueueUrl": {
                "DataType": "String",
                "StringValue": self._config.queue_url,
            },
            "WorkerId": {"DataType": "String", "StringValue": self._config.worker_id},
        }
        if key:
            attributes["WorkerExecutionKey"] = {
                "DataType": "String",
                "StringValue": key,
            }
        self._client.send_message(
            QueueUrl=self._config.dead_letter_queue_url,
            MessageBody=str(message.get("Body", "")),
            MessageAttributes=attributes,
        )
        self._delete(receipt)
        return HandledMessage(message_id, MessageDisposition.DEAD_LETTERED, reason_code, key)


class LaneWorker(Protocol):
    """Worker surface required by :class:`BacktestLaneScheduler`."""

    def receive_one(self) -> Mapping[str, Any] | None: ...

    def handle_message(self, message: Mapping[str, Any]) -> HandledMessage: ...


class BacktestLaneScheduler:
    """Bounded, fair scheduler for three independent SQS backtest lanes.

    The default budget is ``basic=2``, ``custom=1``, ``competition=1`` with a
    global maximum of four executions. The scheduler never receives a message
    unless both budgets have a free slot. Consequently excess requests remain
    queued rather than becoming invisible while waiting for a local thread.

    Lane assignment is deliberately not inferred from a message body here.
    The upstream publisher owns request classification and sends each request
    to the corresponding queue; this class only shares compute fairly.
    """

    DEFAULT_LIMITS: Mapping[BacktestLane, int] = {
        BacktestLane.BASIC: 2,
        BacktestLane.CUSTOM: 1,
        BacktestLane.COMPETITION: 1,
    }

    def __init__(
        self,
        *,
        workers: Mapping[BacktestLane, LaneWorker],
        lane_limits: Mapping[BacktestLane, int] | None = None,
        global_limit: int = 4,
        idle_wait_seconds: float = 5.0,
        receive_backoff_seconds: float = 0.25,
        max_receive_backoff_seconds: float = 5.0,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        limits = dict(lane_limits or self.DEFAULT_LIMITS)
        if not workers:
            raise WorkerConfigurationError("at least one lane worker is required")
        if global_limit < 1:
            raise WorkerConfigurationError("global_limit must be at least 1")
        if idle_wait_seconds <= 0:
            raise WorkerConfigurationError("idle_wait_seconds must be positive")
        if receive_backoff_seconds <= 0:
            raise WorkerConfigurationError("receive_backoff_seconds must be positive")
        if max_receive_backoff_seconds < receive_backoff_seconds:
            raise WorkerConfigurationError(
                "max_receive_backoff_seconds must be at least receive_backoff_seconds"
            )
        if set(workers) != set(limits):
            raise WorkerConfigurationError("lane workers and lane limits must name the same lanes")
        if any(limit < 1 for limit in limits.values()):
            raise WorkerConfigurationError("every lane limit must be at least 1")
        if global_limit > sum(limits.values()):
            raise WorkerConfigurationError("global_limit cannot exceed the sum of lane limits")

        self._workers = dict(workers)
        self._limits = limits
        self._global_limit = global_limit
        self._idle_wait_seconds = idle_wait_seconds
        self._receive_backoff_seconds = receive_backoff_seconds
        self._max_receive_backoff_seconds = max_receive_backoff_seconds
        self._monotonic = monotonic or time.monotonic
        self._lanes = tuple(workers)
        self._cursor = 0
        self._active_by_lane = dict.fromkeys(self._lanes, 0)
        self._futures: dict[Future[HandledMessage], BacktestLane] = {}
        self._executor = ThreadPoolExecutor(max_workers=global_limit, thread_name_prefix="backtest-lane")
        self._stop = threading.Event()
        self._receive_order: list[BacktestLane] = []
        self._failure_count = dict.fromkeys(self._lanes, 0)
        self._retry_at = dict.fromkeys(self._lanes, 0.0)

    @property
    def active_count(self) -> int:
        return len(self._futures)

    @property
    def receive_order(self) -> tuple[BacktestLane, ...]:
        return tuple(self._receive_order)

    def request_stop(self, *_: object) -> None:
        """Stop receiving new work; already received work remains drainable."""
        self._stop.set()

    def poll_once(self) -> tuple[HandledMessage, ...]:
        completed = list(self._reap_completed())
        if self._stop.is_set():
            return tuple(completed)

        empty_lanes: set[BacktestLane] = set()
        while len(self._futures) < self._global_limit:
            lane = self._next_eligible_lane(empty_lanes)
            if lane is None:
                break
            try:
                message = self._workers[lane].receive_one()
            except Exception:
                self._record_lane_failure(lane, "receive")
                empty_lanes.add(lane)
                continue
            self._clear_lane_failure(lane)
            if message is None:
                empty_lanes.add(lane)
                continue
            self._receive_order.append(lane)
            self._active_by_lane[lane] += 1
            future = self._executor.submit(self._workers[lane].handle_message, message)
            self._futures[future] = lane
        return tuple(completed)

    def wait_for_idle(self, *, timeout: float | None = None) -> tuple[HandledMessage, ...]:
        """Wait for work already received by the scheduler, without polling SQS."""
        deadline = None if timeout is None else time.monotonic() + timeout
        completed: list[HandledMessage] = []
        while self._futures:
            completed.extend(self._reap_completed())
            if not self._futures:
                break
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("backtest lane scheduler did not become idle")
            time.sleep(0.005)
        return tuple(completed)

    def run(self) -> None:
        try:
            while not self._stop.is_set():
                before = len(self._futures)
                completed = self.poll_once()
                if not completed and len(self._futures) == before:
                    wait_seconds = 0.05 if self._futures else self._next_wait_seconds()
                    self._stop.wait(wait_seconds)
        finally:
            self.wait_for_idle()
            self._executor.shutdown(wait=True)

    def _next_eligible_lane(self, empty_lanes: set[BacktestLane]) -> BacktestLane | None:
        for offset in range(len(self._lanes)):
            index = (self._cursor + offset) % len(self._lanes)
            lane = self._lanes[index]
            if lane in empty_lanes:
                continue
            if self._retry_at[lane] > self._monotonic():
                continue
            if self._active_by_lane[lane] >= self._limits[lane]:
                continue
            self._cursor = (index + 1) % len(self._lanes)
            return lane
        return None

    def _reap_completed(self) -> tuple[HandledMessage, ...]:
        completed: list[HandledMessage] = []
        for future, lane in tuple(self._futures.items()):
            if not future.done():
                continue
            del self._futures[future]
            self._active_by_lane[lane] -= 1
            try:
                completed.append(future.result())
            except Exception:
                # The message was received with a visibility timeout but was not
                # acknowledged.  Isolate the failed lane and let SQS redeliver it
                # after visibility expiry instead of terminating all three lanes.
                self._record_lane_failure(lane, "handle")
        return tuple(completed)

    def _record_lane_failure(self, lane: BacktestLane, operation: str) -> None:
        failures = self._failure_count[lane] + 1
        self._failure_count[lane] = failures
        delay = min(
            self._max_receive_backoff_seconds,
            self._receive_backoff_seconds * (2 ** min(failures - 1, 30)),
        )
        self._retry_at[lane] = self._monotonic() + delay
        _LOGGER.exception(
            "backtest %s lane %s failed; retrying lane after %.3f seconds",
            lane.value,
            operation,
            delay,
        )

    def _clear_lane_failure(self, lane: BacktestLane) -> None:
        self._failure_count[lane] = 0
        self._retry_at[lane] = 0.0

    def _next_wait_seconds(self) -> float:
        now = self._monotonic()
        retry_delays = [retry_at - now for retry_at in self._retry_at.values() if retry_at > now]
        if not retry_delays:
            return self._idle_wait_seconds
        return max(0.001, min(self._idle_wait_seconds, min(retry_delays)))


# ==========================================================================
# Entry point
# ==========================================================================


_REQUIRED_ENV = (
    "BACKTEST_QUEUE_URL",
    "BACKTEST_DLQ_URL",
    "BACKTEST_WORKER_ID",
    "BACKTEST_JOB_HANDLER",
    "BACKTEST_EXECUTION_KEY_STORE",
)

_LANE_LIMIT_DEFAULTS: Mapping[BacktestLane, int] = {
    BacktestLane.BASIC: 2,
    BacktestLane.CUSTOM: 1,
    BacktestLane.COMPETITION: 1,
}

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"


def _configure_logging(environ: Mapping[str, str]) -> str:
    """Configure the dedicated console process before any worker components start."""
    level_name = environ.get("BACKTEST_LOG_LEVEL", "INFO").strip().upper() or "INFO"
    level = logging.getLevelNamesMapping().get(level_name)
    if not isinstance(level, int):
        raise WorkerConfigurationError(
            f"BACKTEST_LOG_LEVEL must be a Python logging level name, got {level_name!r}"
        )
    logging.basicConfig(level=level, format=_LOG_FORMAT, force=True)
    return level_name


def _config_from_env(environ: Mapping[str, str]) -> WorkerConfig:
    missing = [name for name in _REQUIRED_ENV if not environ.get(name)]
    if missing:
        raise WorkerConfigurationError("missing required environment settings: " + ", ".join(sorted(missing)))
    return WorkerConfig(
        queue_url=environ["BACKTEST_QUEUE_URL"],
        dead_letter_queue_url=environ["BACKTEST_DLQ_URL"],
        worker_id=environ["BACKTEST_WORKER_ID"],
        max_receive_count=int(environ.get("BACKTEST_MAX_RECEIVE_COUNT", "5")),
        visibility_timeout=timedelta(seconds=int(environ.get("BACKTEST_VISIBILITY_TIMEOUT_SECONDS", "300"))),
        wait_time=timedelta(seconds=int(environ.get("BACKTEST_WAIT_SECONDS", "20"))),
        max_messages=int(environ.get("BACKTEST_MAX_MESSAGES", "1")),
        heartbeat_interval=timedelta(seconds=int(environ.get("BACKTEST_HEARTBEAT_SECONDS", "60"))),
    )


def _lane_configs_from_env(
    environ: Mapping[str, str],
) -> tuple[dict[BacktestLane, WorkerConfig], dict[BacktestLane, int], int]:
    """Read the deployment's three queue URLs and bounded concurrency budget."""
    required = [
        "BACKTEST_WORKER_ID",
        "BACKTEST_JOB_HANDLER",
        "BACKTEST_EXECUTION_KEY_STORE",
    ]
    for lane in BacktestLane:
        prefix = f"BACKTEST_{lane.value.upper()}"
        required.extend((f"{prefix}_QUEUE_URL", f"{prefix}_DLQ_URL"))
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise WorkerConfigurationError("missing required lane environment settings: " + ", ".join(sorted(missing)))

    configs: dict[BacktestLane, WorkerConfig] = {}
    limits: dict[BacktestLane, int] = {}
    for lane, default_limit in _LANE_LIMIT_DEFAULTS.items():
        prefix = f"BACKTEST_{lane.value.upper()}"
        configs[lane] = WorkerConfig(
            queue_url=environ[f"{prefix}_QUEUE_URL"],
            dead_letter_queue_url=environ[f"{prefix}_DLQ_URL"],
            worker_id=environ["BACKTEST_WORKER_ID"],
            max_receive_count=int(environ.get("BACKTEST_MAX_RECEIVE_COUNT", "5")),
            visibility_timeout=timedelta(seconds=int(environ.get("BACKTEST_VISIBILITY_TIMEOUT_SECONDS", "300"))),
            # The scheduler polls lanes without blocking and admits one message
            # per reserved slot. These values retain a valid standalone config.
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=int(environ.get("BACKTEST_HEARTBEAT_SECONDS", "60"))),
        )
        limits[lane] = int(environ.get(f"{prefix}_MAX_CONCURRENCY", str(default_limit)))
    global_limit = int(environ.get("BACKTEST_MAX_TOTAL_CONCURRENCY", "4"))
    # Reuse the scheduler's validation so environment errors fail before the
    # process starts receiving messages. Avoid constructing an executor here.
    if global_limit < 1:
        raise WorkerConfigurationError("global_limit must be at least 1")
    if any(limit < 1 for limit in limits.values()):
        raise WorkerConfigurationError("every lane limit must be at least 1")
    if global_limit > sum(limits.values()):
        raise WorkerConfigurationError("global_limit cannot exceed the sum of lane limits")
    return configs, limits, global_limit


def _request_configs_from_env(environ: Mapping[str, str]) -> dict[Any, Any]:
    """Build producer-envelope consumers and prevent request/job queue aliasing."""

    from .backtest_request_intake import RequestIntakeConfig, RequestLane

    required = ["BACKTEST_REQUEST_HANDLER", "BACKTEST_REQUEST_RECEIPT_STORE"]
    for lane in RequestLane:
        prefix = f"BACKTEST_{lane.value}_REQUEST"
        required.extend((f"{prefix}_QUEUE_URL", f"{prefix}_DLQ_URL"))
    missing = [name for name in required if not environ.get(name)]
    if missing:
        raise WorkerConfigurationError(
            "missing required request intake settings: " + ", ".join(sorted(missing))
        )

    execution_urls = {
        environ.get(f"BACKTEST_{lane.value.upper()}_QUEUE_URL", "")
        for lane in BacktestLane
    }
    request_urls = {
        environ[f"BACKTEST_{lane.value}_REQUEST_QUEUE_URL"] for lane in RequestLane
    }
    request_dlq_urls = {
        environ[f"BACKTEST_{lane.value}_REQUEST_DLQ_URL"] for lane in RequestLane
    }
    request_all_urls = request_urls | request_dlq_urls
    if (
        "" in execution_urls
        or len(execution_urls) != len(BacktestLane)
        or len(request_all_urls) != len(RequestLane) * 2
    ):
        raise WorkerConfigurationError(
            "request intake requires distinct Basic, Custom and Competition source and DLQ queues"
        )
    if execution_urls & request_all_urls:
        raise WorkerConfigurationError(
            "producer request queues and execution job queues must be distinct"
        )

    configs = {}
    for lane in RequestLane:
        prefix = f"BACKTEST_{lane.value}_REQUEST"
        configs[lane] = RequestIntakeConfig(
            lane=lane,
            queue_url=environ[f"{prefix}_QUEUE_URL"],
            dead_letter_queue_url=environ[f"{prefix}_DLQ_URL"],
            consumer_id=f"backtest-{lane.value.lower()}-request-v1",
            max_receive_count=int(
                environ.get("BACKTEST_REQUEST_MAX_RECEIVE_COUNT", "5")
            ),
            visibility_timeout=timedelta(
                seconds=int(
                    environ.get("BACKTEST_REQUEST_VISIBILITY_TIMEOUT_SECONDS", "300")
                )
            ),
            wait_time=timedelta(
                seconds=int(environ.get("BACKTEST_REQUEST_WAIT_SECONDS", "5"))
            ),
        )
    return configs


def load_factory(target: str, setting: str) -> Any:
    """Resolve ``package.module:factory`` and call it.

    The wiring that binds the orchestrator to the real execution model, result
    snapshot, object store and repositories is an integration-step artefact.
    Requiring it to be named explicitly is deliberate: a default wired in here
    would be exactly the hidden policy the spec forbids.
    """
    module_name, _, attribute = target.partition(":")
    if not module_name or not attribute:
        raise WorkerConfigurationError(f"{setting} must be 'package.module:factory'")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise WorkerConfigurationError(f"{setting}={target!r} names an unimportable module: {exc}") from exc
    try:
        factory = getattr(module, attribute)
    except AttributeError as exc:
        raise WorkerConfigurationError(f"{setting}={target!r} names no attribute {attribute!r}") from exc
    return factory()


def _runtime_sqs_client(environ: Mapping[str, str]) -> Any:
    import boto3

    return boto3.client(
        "sqs",
        endpoint_url=environ.get("AWS_ENDPOINT_URL_SQS")
        or environ.get("AWS_ENDPOINT_URL"),
        region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
    )


def _stale_recovery_loop(
    store: ExecutionKeyStore,
    stop: threading.Event,
    *,
    interval: timedelta,
    max_attempts: int,
    queue_policy: Any,
) -> None:
    while not stop.is_set():
        try:
            report = store.recover_stale(
                max_attempts=max_attempts, queue_policy=queue_policy
            )
            if report is not None:
                _LOGGER.info("backtest stale recovery completed report=%s", report)
        except Exception:
            _LOGGER.exception("backtest stale recovery pass failed")
        stop.wait(interval.total_seconds())


def run() -> None:
    log_level = _configure_logging(os.environ)
    lane_mode = any(os.environ.get(f"BACKTEST_{lane.value.upper()}_QUEUE_URL") for lane in BacktestLane)
    if lane_mode:
        configs, lane_limits, global_limit = _lane_configs_from_env(os.environ)
    else:
        config = _config_from_env(os.environ)
    _LOGGER.info(
        "backtest worker starting worker_id=%s lane_mode=%s log_level=%s",
        os.environ.get("BACKTEST_WORKER_ID", ""),
        lane_mode,
        log_level,
    )
    handler: JobHandler = load_factory(os.environ["BACKTEST_JOB_HANDLER"], "BACKTEST_JOB_HANDLER")
    # No in-memory fallback. `InMemoryExecutionKeyStore` is a process-local dictionary;
    # a deployment that got it by leaving one variable unset would silently lose the
    # cross-process duplicate-worker control this whole module exists to provide, and
    # two workers would execute the same message twice.
    store: ExecutionKeyStore = load_factory(os.environ["BACKTEST_EXECUTION_KEY_STORE"], "BACKTEST_EXECUTION_KEY_STORE")

    recovery_interval = timedelta(
        seconds=int(os.environ.get("BACKTEST_RECOVERY_INTERVAL_SECONDS", "60"))
    )
    from .recovery import QueueDispatchPolicy

    queue_policy = QueueDispatchPolicy.from_environment(os.environ)
    max_attempts = int(os.environ.get("BACKTEST_MAX_RECEIVE_COUNT", "5"))
    if recovery_interval <= timedelta(0):
        raise WorkerConfigurationError("backtest recovery interval must be positive")
    recovery_stop = threading.Event()
    recovery_thread = threading.Thread(
        target=_stale_recovery_loop,
        args=(store, recovery_stop),
        kwargs={
            "interval": recovery_interval,
            "max_attempts": max_attempts,
            "queue_policy": queue_policy,
        },
        daemon=True,
        name="backtest-stale-recovery",
    )
    recovery_thread.start()

    client = _runtime_sqs_client(os.environ)
    request_intake_stop = threading.Event()
    request_threads: list[threading.Thread] = []
    request_configs: dict[Any, Any] = {}
    request_mode = any(
        os.environ.get(f"BACKTEST_{lane}_REQUEST_QUEUE_URL")
        for lane in ("BASIC", "CUSTOM", "COMPETITION")
    )
    if request_mode:
        from .backtest_request_intake import BacktestRequestIntake

        request_configs = _request_configs_from_env(os.environ)
        request_handler = load_factory(
            os.environ["BACKTEST_REQUEST_HANDLER"], "BACKTEST_REQUEST_HANDLER"
        )
        request_receipts = load_factory(
            os.environ["BACKTEST_REQUEST_RECEIPT_STORE"],
            "BACKTEST_REQUEST_RECEIPT_STORE",
        )
        for request_lane, request_config in request_configs.items():
            intake = BacktestRequestIntake(
                client=client,
                config=request_config,
                handler=request_handler,
                receipts=request_receipts,
            )
            thread = threading.Thread(
                target=intake.run,
                args=(request_intake_stop,),
                daemon=True,
                name=f"backtest-{request_lane.value.lower()}-request-intake",
            )
            thread.start()
            request_threads.append(thread)
    scale_down_stop = threading.Event()
    scale_down_thread: threading.Thread | None = None
    scale_down_engine = None
    if os.environ.get("BACKTEST_SCALE_DOWN_ENABLED", "false").strip().lower() not in {"", "false"}:
        import boto3

        if not lane_mode:
            raise WorkerConfigurationError("instance scale-down requires the three-lane worker mode")
        if not request_mode:
            raise WorkerConfigurationError(
                "instance scale-down requires all three producer request queues"
            )
        from .backtest_request_intake import RequestLane
        from .persistence import create_backtest_engine
        from .scale_down import controller_from_env

        database_url = os.environ.get("BACKTEST_DATABASE_URL", "").strip()
        if not database_url:
            raise WorkerConfigurationError("BACKTEST_DATABASE_URL is required for instance scale-down")
        scale_down_engine = create_backtest_engine(database_url)
        controller = controller_from_env(
            os.environ,
            engine=scale_down_engine,
            sqs_client=client,
            autoscaling_client=boto3.client("autoscaling", endpoint_url=os.environ.get("AWS_ENDPOINT_URL")),
            queue_urls=[configs[lane].queue_url for lane in BacktestLane],
            request_queue_urls=[request_configs[lane].queue_url for lane in RequestLane],
        )
        assert controller is not None
        scale_down_thread = threading.Thread(
            target=controller.run,
            args=(scale_down_stop,),
            daemon=True,
            name="backtest-scale-down",
        )
        scale_down_thread.start()
    if lane_mode:
        workers = {
            lane: BacktestWorker(
                client=client,
                config=lane_config,
                handler=handler,
                store=store,
            )
            for lane, lane_config in configs.items()
        }
        scheduler = BacktestLaneScheduler(
            workers=workers,
            lane_limits=lane_limits,
            global_limit=global_limit,
            idle_wait_seconds=float(os.environ.get("BACKTEST_SCHEDULER_IDLE_SECONDS", "5")),
        )

        def request_all_stop(*_: Any) -> None:
            request_intake_stop.set()
            scale_down_stop.set()
            scheduler.request_stop()

        signal.signal(signal.SIGINT, request_all_stop)
        signal.signal(signal.SIGTERM, request_all_stop)
        try:
            scheduler.run()
        finally:
            request_intake_stop.set()
            scale_down_stop.set()
            recovery_stop.set()
            if scale_down_thread is not None:
                scale_down_thread.join(timeout=5)
            if scale_down_engine is not None:
                scale_down_engine.dispose()
            for thread in request_threads:
                thread.join(timeout=6)
            recovery_thread.join(timeout=5)
        return

    worker = BacktestWorker(client=client, config=config, handler=handler, store=store)
    def request_single_stop(*args: Any) -> None:
        request_intake_stop.set()
        worker.request_stop(*args)

    signal.signal(signal.SIGINT, request_single_stop)
    signal.signal(signal.SIGTERM, request_single_stop)
    try:
        worker.run()
    finally:
        request_intake_stop.set()
        recovery_stop.set()
        for thread in request_threads:
            thread.join(timeout=6)
        recovery_thread.join(timeout=5)
