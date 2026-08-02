"""D91 -- the contract-driven path from a strategy release to an official run.

The ownership violation this module replaces
--------------------------------------------
``backend/modules/backend-persistence/src/main/java/com/idea2strategy/backend/
persistence/strategy/ImmutableStrategyReleaseJooqCommandAdapter.java`` currently
does this, at line 207, inside the release transaction::

    dsl.execute(
            "insert into backtest.runs "
                    + "(id, bot_id, owner_account_id, configuration_hash, status, evaluation_start, "
                    + "evaluation_end, initial_cash_amount, market_rules_version, accounting_rules_version, "
                    + "precision_rules_version, fee_policy_id, slippage_rate_bps, "
                    + "buying_power_buffer_policy_id, idempotency_key, queued_at) "
                    + "values (?, ?, ?, ?, 'QUEUED', ?::date, ?::date, ?, ?, ?, ?, ?, 5, ?, ?, "
                    + "?::timestamptz) on conflict (idempotency_key) do nothing",
            ...);

Three separate defects live in those nine lines:

1. **Ownership.** ``DatabaseAccessPolicy.SCHEMA_OWNERS`` (line 39) registers
   ``backtest`` to ``MigrationOwner.BACKTEST``, and ``allowsBacktest`` (lines
   119-129) is the only role permitted to ``INSERT`` there. ``allows`` for
   ``BACKEND`` (line 72) grants writes only to tables ``ownsBackendTable``
   accepts, which does not include ``backtest``. The backend is writing a
   schema its own policy forbids it.
2. **A hidden default.** ``slippage_rate_bps`` is the SQL literal ``5``. It is
   not read from the execution policy, so every official run ever created by
   this path is measured at 5 bps whatever the published policy says, and the
   run row silently disagrees with the ``fee_policy_id`` beside it.
3. **A second source of truth for run identity.** ``on conflict
   (idempotency_key) do nothing`` makes the backend the arbiter of "exactly
   once", while D's own acceptance path has the same responsibility. Two
   arbiters is one too many: whichever writes first decides the run's columns,
   and the other's are silently discarded.

The replacement needs no new contract, because the same method *already* writes
the outbox row that carries it::

    insert into operations.outbox_messages
      (id, owner_domain, aggregate_id, aggregate_sequence, event_type,
       event_schema_version, payload_document, idempotency_key, created_at)
    values (?, 'strategy-bot', ?, 1, ?, ?, ?::jsonb, ?, ?::timestamptz)
    on conflict (idempotency_key) do nothing

``payload_document`` is B's ``OFFICIAL_BACKTEST_REQUESTED`` message verbatim and
``operations`` is a backend-owned schema, so that write is legitimate. The
required backend change is therefore a **deletion**: drop the
``insert into backtest.runs`` statement and the ``runExists`` probe that guards
it, keep the outbox row, and let a relay publish it onto the release queue this
module consumes. Exactly-once then has one arbiter -- ``backtest.runs``'s unique
``idempotency_key``, enforced by the owner of the table -- and
``slippage_rate_bps`` comes from the published :class:`ExecutionPolicy` like
every other policy column on the row.

What this module is
-------------------
An SQS consumer for that relay. It is deliberately *not*
:class:`~backtest_engine.worker.BacktestWorker`: that worker consumes D's own
job messages and coordinates attempts, while this one consumes B's contract and
does nothing but accept it. Sharing one consumer would mean one queue carrying
two contracts.

Idempotency is not implemented here. It belongs to
:meth:`~backtest_engine.lifecycle.BacktestLifecycleService.accept`, whose run id
is ``uuid5`` of B's own ``metadata.idempotencyKey`` and whose gateway holds the
unique constraint. A redelivery therefore converges on the same row without this
module keeping any state of its own -- which is what makes it safe to run more
than one consumer.

Retry policy
------------
Only one failure class is retryable: a required input that is not resolvable
*yet*. A bot the owner directory has not replicated, an execution policy not yet
published, a dataset manifest still being written -- all of those can become
resolvable, so the message goes back to the queue. Everything else (a contract
violation, an unsupported contract version, an unparseable body, a message of
the wrong type, an idempotency-key collision on different content) can never
succeed on a later delivery and is dead-lettered immediately, with the reason
travelling as a message attribute so the queue is triageable.
"""

from __future__ import annotations

import json
import signal
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from .contracts import (
    OFFICIAL_BACKTEST_MESSAGE_TYPE,
    ContractValidationError,
    UnsupportedContractVersion,
)
from .lifecycle import (
    BacktestLifecycleService,
    IdempotencyConflict,
    LifecycleError,
    RequestNotSatisfiable,
)
from .worker import SqsClient


__all__ = [
    "INTAKE_VERSION",
    "IntakeConfig",
    "IntakeConfigurationError",
    "IntakeDisposition",
    "IntakeOutcome",
    "OfficialBacktestIntake",
    "run",
]


INTAKE_VERSION = "backtest-release-intake:1.0.0"

#: SQS caps a single long poll at 20 seconds and a batch at 10 messages.
_MAX_WAIT = timedelta(seconds=20)
_MAX_BATCH = 10


class IntakeConfigurationError(ValueError):
    """Raised when the intake is asked to run with unusable settings."""


class IntakeDisposition(StrEnum):
    """What happened to one delivery."""

    #: The release became a new ``backtest.runs`` row and a queued job.
    ACCEPTED_CREATED = "ACCEPTED_CREATED"
    #: The release was already accepted. No second run, no second job.
    ACCEPTED_DUPLICATE = "ACCEPTED_DUPLICATE"
    #: Left for the queue to redeliver; a required input was not resolvable yet.
    RETURNED = "RETURNED"
    #: Routed to the dead-letter queue; a later delivery cannot succeed.
    DEAD_LETTERED = "DEAD_LETTERED"


@dataclass(frozen=True, slots=True)
class IntakeOutcome:
    message_id: str
    disposition: IntakeDisposition
    reason_code: str | None = None
    backtest_run_id: uuid.UUID | None = None
    created: bool = False
    dispatched: bool = False

    def __post_init__(self) -> None:
        accepted = self.disposition in (
            IntakeDisposition.ACCEPTED_CREATED,
            IntakeDisposition.ACCEPTED_DUPLICATE,
        )
        if accepted and self.backtest_run_id is None:
            raise IntakeConfigurationError(
                "an accepted release must name the run it addressed"
            )
        if not accepted and not self.reason_code:
            raise IntakeConfigurationError(
                "a release that was not accepted must carry a reason code"
            )
        if self.created and self.disposition is not IntakeDisposition.ACCEPTED_CREATED:
            raise IntakeConfigurationError(
                "only ACCEPTED_CREATED may report that it created the run"
            )


@dataclass(frozen=True, slots=True)
class IntakeConfig:
    queue_url: str
    dead_letter_queue_url: str
    consumer_id: str
    max_receive_count: int
    visibility_timeout: timedelta
    wait_time: timedelta
    max_messages: int

    def __post_init__(self) -> None:
        for name in ("queue_url", "dead_letter_queue_url", "consumer_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise IntakeConfigurationError(f"{name} must not be blank")
        if self.queue_url == self.dead_letter_queue_url:
            raise IntakeConfigurationError(
                "the dead-letter queue must not be the queue being consumed, "
                "otherwise poison is redelivered forever"
            )
        if self.max_receive_count < 1:
            raise IntakeConfigurationError("max_receive_count must be at least 1")
        if not 1 <= self.max_messages <= _MAX_BATCH:
            raise IntakeConfigurationError(
                f"max_messages must be between 1 and {_MAX_BATCH}"
            )
        if not timedelta(0) <= self.wait_time <= _MAX_WAIT:
            raise IntakeConfigurationError(
                "wait_time must be between 0 and 20 seconds (the SQS long-poll cap)"
            )
        if self.visibility_timeout <= timedelta(0):
            raise IntakeConfigurationError("visibility_timeout must be positive")


class OfficialBacktestIntake:
    """Turns B's published release message into exactly one official run."""

    def __init__(
        self,
        *,
        client: SqsClient,
        config: IntakeConfig,
        lifecycle: BacktestLifecycleService,
    ) -> None:
        self._client = client
        self._config = config
        self._lifecycle = lifecycle
        self._stop = threading.Event()

    # -- lifecycle --------------------------------------------------------

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def request_stop(self, *_: object) -> None:
        """Signal shutdown. The in-flight batch is always finished first."""
        self._stop.set()

    def run(self) -> None:  # pragma: no cover - the loop is one call per poll
        while not self._stop.is_set():
            self.poll_once()

    # -- one poll cycle ---------------------------------------------------

    def poll_once(self) -> tuple[IntakeOutcome, ...]:
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=self._config.max_messages,
            WaitTimeSeconds=int(self._config.wait_time.total_seconds()),
            VisibilityTimeout=int(self._config.visibility_timeout.total_seconds()),
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        return tuple(self.handle(message) for message in response.get("Messages", []))

    def handle(self, message: Mapping[str, Any]) -> IntakeOutcome:
        """Apply one delivery. Public so a test can drive the decision table."""
        message_id = str(message.get("MessageId", ""))
        receipt = str(message["ReceiptHandle"])
        receive_count = int(
            message.get("Attributes", {}).get("ApproximateReceiveCount", "1")
        )
        body = str(message.get("Body", ""))

        if receive_count > self._config.max_receive_count:
            return self._dead_letter(
                body, receipt, "MAX_RECEIVE_COUNT_EXCEEDED", message_id
            )

        try:
            request = self._parse(body)
        except _PoisonMessage as poison:
            return self._dead_letter(body, receipt, poison.reason_code, message_id)

        try:
            accepted = self._lifecycle.accept(request)
        except UnsupportedContractVersion:
            # Rejected outright, never substituted: a version this build does
            # not implement means the message's fields may not mean what they
            # appear to mean.
            return self._dead_letter(
                body, receipt, "UNSUPPORTED_CONTRACT_VERSION", message_id
            )
        except ContractValidationError:
            return self._dead_letter(body, receipt, "CONTRACT_VIOLATION", message_id)
        except IdempotencyConflict:
            # B reused an idempotency key for materially different content. A
            # later delivery of the same bytes conflicts identically.
            return self._dead_letter(body, receipt, "IDEMPOTENCY_CONFLICT", message_id)
        except RequestNotSatisfiable as exc:
            return self._not_yet(body, receipt, exc.reason_code, message_id, receive_count)
        except LifecycleError as exc:
            # An unclassified lifecycle failure is treated as transient rather
            # than discarded: losing a release is worse than retrying one.
            return self._not_yet(
                body,
                receipt,
                f"INTAKE_ERROR:{type(exc).__name__}",
                message_id,
                receive_count,
            )

        self._delete(receipt)
        return IntakeOutcome(
            message_id=message_id,
            disposition=(
                IntakeDisposition.ACCEPTED_CREATED
                if accepted.created
                else IntakeDisposition.ACCEPTED_DUPLICATE
            ),
            reason_code=None if accepted.created else "DUPLICATE_RELEASE_DELIVERY",
            backtest_run_id=accepted.run.backtest_run_id,
            created=accepted.created,
            dispatched=accepted.dispatched,
        )

    # -- parsing -----------------------------------------------------------

    @staticmethod
    def _parse(body: str) -> Mapping[str, Any]:
        try:
            document = json.loads(body)
        except (TypeError, ValueError) as exc:
            raise _PoisonMessage("MESSAGE_NOT_PARSEABLE") from exc
        if not isinstance(document, Mapping):
            raise _PoisonMessage("MESSAGE_NOT_PARSEABLE")
        metadata = document.get("metadata")
        message_type = metadata.get("messageType") if isinstance(metadata, Mapping) else None
        if message_type != OFFICIAL_BACKTEST_MESSAGE_TYPE:
            # The release queue carries one contract. A `bot-run-command`
            # arriving here is a routing fault, not a malformed backtest
            # request, and the two need different operator responses.
            raise _PoisonMessage("UNEXPECTED_MESSAGE_TYPE")
        return document

    # -- queue effects -----------------------------------------------------

    def _not_yet(
        self,
        body: str,
        receipt: str,
        reason_code: str,
        message_id: str,
        receive_count: int,
    ) -> IntakeOutcome:
        """Retry while the budget lasts, then dead-letter with the *real* reason.

        Dead-lettering on ``MAX_RECEIVE_COUNT_EXCEEDED`` would tell an operator
        only that something failed repeatedly; the reason that actually blocked
        every attempt is what they need.
        """
        if receive_count >= self._config.max_receive_count:
            return self._dead_letter(body, receipt, reason_code, message_id)
        self._client.change_message_visibility(
            QueueUrl=self._config.queue_url, ReceiptHandle=receipt, VisibilityTimeout=0
        )
        return IntakeOutcome(
            message_id=message_id,
            disposition=IntakeDisposition.RETURNED,
            reason_code=reason_code,
        )

    def _delete(self, receipt: str) -> None:
        self._client.delete_message(
            QueueUrl=self._config.queue_url, ReceiptHandle=receipt
        )

    def _dead_letter(
        self, body: str, receipt: str, reason_code: str, message_id: str
    ) -> IntakeOutcome:
        self._client.send_message(
            QueueUrl=self._config.dead_letter_queue_url,
            MessageBody=body,
            MessageAttributes={
                "DeadLetterReason": {"DataType": "String", "StringValue": reason_code},
                "SourceQueueUrl": {
                    "DataType": "String",
                    "StringValue": self._config.queue_url,
                },
                "ConsumerId": {
                    "DataType": "String",
                    "StringValue": self._config.consumer_id,
                },
                "IntakeVersion": {"DataType": "String", "StringValue": INTAKE_VERSION},
                "DeadLetteredAt": {
                    "DataType": "String",
                    "StringValue": datetime.now(tz=timezone.utc).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                },
            },
        )
        self._delete(receipt)
        return IntakeOutcome(
            message_id=message_id,
            disposition=IntakeDisposition.DEAD_LETTERED,
            reason_code=reason_code,
        )


class _PoisonMessage(Exception):
    """A body no later delivery can make acceptable."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


def run() -> None:  # pragma: no cover - process entry point
    """Entry point for a release-intake consumer.

    Deliberately unimplemented for the same reason ``api.run`` is: a real
    deployment needs a database URL, a queue URL, an owner directory, a compiled
    plan source and a published execution policy catalog. Inventing defaults for
    those is how the pre-rebuild build shipped an API backed by a dictionary.
    """
    raise NotImplementedError(
        "backtest-release-intake has no runnable default wiring yet: build the "
        "BacktestLifecycleService with PersistenceRunGateway plus the real "
        "OwnerDirectory, CompiledPlanSource, DatasetManifestSource and "
        "ExecutionPolicyCatalog, then serve OfficialBacktestIntake(...).run() "
        f"from your deployment entry point. {signal.SIGTERM!r} should call "
        "request_stop()."
    )
