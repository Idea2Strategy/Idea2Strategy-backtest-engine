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

import hashlib
import importlib
import json
import os
import signal
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any, Protocol


__all__ = [
    "WORKER_EXECUTION_KEY_MAX_LENGTH",
    "WORKER_VERSION",
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


class WorkerConfigurationError(ValueError):
    """Raised when the worker is asked to run with unusable settings."""


class JobResult(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    RETRY = "RETRY"
    PERMANENT_FAILURE = "PERMANENT_FAILURE"


class ExecutionRecordStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class MessageDisposition(StrEnum):
    DELETED = "DELETED"
    RETURNED = "RETURNED"
    DEAD_LETTERED = "DEAD_LETTERED"


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


class ExecutionKeyStore(Protocol):
    """Compare-and-swap on ``worker_execution_key``.

    Must be satisfied by BT-e's persistence layer, where ``claim`` is an
    ``INSERT ... ON CONFLICT (worker_execution_key) DO NOTHING`` whose affected
    row count *is* the CAS result.
    """

    def claim(
        self, key: str, *, run_id: str, owner: str, now: datetime
    ) -> ExecutionClaim: ...

    def release(self, key: str, *, now: datetime) -> None: ...

    def finish(
        self, key: str, status: ExecutionRecordStatus, *, now: datetime
    ) -> None: ...

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
        self._lock = threading.Lock()

    def claim(
        self, key: str, *, run_id: str, owner: str, now: datetime
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

    def release(self, key: str, *, now: datetime) -> None:
        """Hand a retryable attempt back so the next delivery can re-claim it."""
        with self._lock:
            record = self._records.pop(key, None)
            if record is not None:
                self._released_attempts[key] = record.attempt_number

    def finish(
        self, key: str, status: ExecutionRecordStatus, *, now: datetime
    ) -> None:
        if status is ExecutionRecordStatus.IN_PROGRESS:
            raise ValueError("finish requires a terminal status")
        with self._lock:
            record = self._records.get(key)
            if record is None:
                raise KeyError(f"no execution record for {key}")
            record.status = status
            record.updated_at = now

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
            raise WorkerConfigurationError(
                f"max_messages must be between 1 and {_MAX_BATCH}"
            )
        if not timedelta(0) <= self.wait_time <= _MAX_WAIT:
            raise WorkerConfigurationError(
                "wait_time must be between 0 and 20 seconds (the SQS long-poll cap)"
            )
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
        receive_count = int(
            message.get("Attributes", {}).get("ApproximateReceiveCount", "1")
        )

        if receive_count > self._config.max_receive_count:
            # Explicit routing, not a redrive policy: the reason travels with
            # the message so the DLQ is triageable.
            return self._dead_letter(
                message, receipt, "MAX_RECEIVE_COUNT_EXCEEDED", message_id, None
            )

        try:
            job = json.loads(str(message.get("Body", "")))
            if not isinstance(job, Mapping):
                raise TypeError("message body must be a JSON object")
            run_id = str(job["backtestRunId"])
            idempotency_key = str(job["idempotencyKey"])
            key = worker_execution_key_for(run_id, idempotency_key)
        except Exception:
            return self._dead_letter(
                message, receipt, "MESSAGE_NOT_PARSEABLE", message_id, None
            )

        claim = self._store.claim(
            key, run_id=run_id, owner=self._config.worker_id, now=self._clock()
        )
        if not claim.acquired:
            return self._on_duplicate(message, receipt, claim, message_id, key)

        context = JobContext(
            worker_execution_key=key,
            attempt_number=claim.attempt_number,
            receive_count=receive_count,
            message_id=message_id,
            worker_id=self._config.worker_id,
        )
        outcome = self._invoke(job, context, receipt)

        if outcome.result is JobResult.SUCCEEDED:
            self._store.finish(key, ExecutionRecordStatus.SUCCEEDED, now=self._clock())
            self._delete(receipt)
            return HandledMessage(message_id, MessageDisposition.DELETED, None, key)

        if outcome.result is JobResult.RETRY:
            # Release the CAS record so the redelivery is a *new* attempt, then
            # make the message immediately visible again.
            self._store.release(key, now=self._clock())
            self._return_to_queue(receipt)
            return HandledMessage(
                message_id, MessageDisposition.RETURNED, outcome.reason_code, key
            )

        self._store.finish(key, ExecutionRecordStatus.FAILED, now=self._clock())
        return self._dead_letter(
            message, receipt, outcome.reason_code or "PERMANENT_FAILURE", message_id, key
        )

    def _invoke(
        self, job: Mapping[str, Any], context: JobContext, receipt: str
    ) -> JobOutcome:
        """Run the handler with a visibility heartbeat alongside it."""
        done = threading.Event()
        beat = threading.Thread(
            target=self._beat, args=(receipt, done), daemon=True, name="bt4-heartbeat"
        )
        beat.start()
        try:
            return self._handler(job, context)
        except Exception as exc:
            return JobOutcome(
                JobResult.RETRY, reason_code=f"HANDLER_ERROR:{type(exc).__name__}"
            )
        finally:
            done.set()
            beat.join(timeout=self._config.visibility_timeout.total_seconds())

    def _beat(self, receipt: str, done: threading.Event) -> None:
        interval = self._config.heartbeat_interval.total_seconds()
        timeout = int(self._config.visibility_timeout.total_seconds())
        while not done.wait(interval):
            try:
                self._client.change_message_visibility(
                    QueueUrl=self._config.queue_url,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=timeout,
                )
            except Exception:
                # The message has already been deleted or its receipt expired;
                # the outer call is authoritative either way.
                return
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
        if claim.existing_status is ExecutionRecordStatus.FAILED:
            return self._dead_letter(
                message, receipt, "DUPLICATE_ALREADY_FAILED", message_id, key
            )
        # Another worker holds the key. Leave the message for the queue to
        # redeliver after its visibility timeout, without resetting it to 0.
        return HandledMessage(
            message_id, MessageDisposition.RETURNED, "EXECUTION_KEY_HELD", key
        )

    # -- queue effects ----------------------------------------------------

    def _delete(self, receipt: str) -> None:
        self._client.delete_message(
            QueueUrl=self._config.queue_url, ReceiptHandle=receipt
        )

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
        return HandledMessage(
            message_id, MessageDisposition.DEAD_LETTERED, reason_code, key
        )


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


def _config_from_env(environ: Mapping[str, str]) -> WorkerConfig:
    missing = [name for name in _REQUIRED_ENV if not environ.get(name)]
    if missing:
        raise WorkerConfigurationError(
            "missing required environment settings: " + ", ".join(sorted(missing))
        )
    return WorkerConfig(
        queue_url=environ["BACKTEST_QUEUE_URL"],
        dead_letter_queue_url=environ["BACKTEST_DLQ_URL"],
        worker_id=environ["BACKTEST_WORKER_ID"],
        max_receive_count=int(environ.get("BACKTEST_MAX_RECEIVE_COUNT", "5")),
        visibility_timeout=timedelta(
            seconds=int(environ.get("BACKTEST_VISIBILITY_TIMEOUT_SECONDS", "300"))
        ),
        wait_time=timedelta(seconds=int(environ.get("BACKTEST_WAIT_SECONDS", "20"))),
        max_messages=int(environ.get("BACKTEST_MAX_MESSAGES", "1")),
        heartbeat_interval=timedelta(
            seconds=int(environ.get("BACKTEST_HEARTBEAT_SECONDS", "60"))
        ),
    )


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


def run() -> None:
    config = _config_from_env(os.environ)
    handler: JobHandler = load_factory(
        os.environ["BACKTEST_JOB_HANDLER"], "BACKTEST_JOB_HANDLER"
    )
    # No in-memory fallback. `InMemoryExecutionKeyStore` is a process-local dictionary;
    # a deployment that got it by leaving one variable unset would silently lose the
    # cross-process duplicate-worker control this whole module exists to provide, and
    # two workers would execute the same message twice.
    store: ExecutionKeyStore = load_factory(
        os.environ["BACKTEST_EXECUTION_KEY_STORE"], "BACKTEST_EXECUTION_KEY_STORE"
    )

    import boto3

    client = boto3.client("sqs", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
    worker = BacktestWorker(
        client=client, config=config, handler=handler, store=store
    )
    signal.signal(signal.SIGINT, worker.request_stop)
    signal.signal(signal.SIGTERM, worker.request_stop)
    worker.run()
