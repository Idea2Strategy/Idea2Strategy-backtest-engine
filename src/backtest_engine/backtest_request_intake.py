"""Consumer boundary for backend ``backtest-request.v1`` Outbox messages.

Custom and competition requests travel on distinct SQS queues.  This intake keeps
that routing boundary explicit, verifies the raw Outbox transport envelope before
deserialising it, and records a message-id receipt before invoking a business
handler.  The handler is deliberately a port: resolving the custom period or the
locked competition plan belongs to the lifecycle service, not to SQS plumbing.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

from jsonschema.exceptions import best_match
from sqlalchemy import Engine, text

from .contracts import (
    ContractValidationError,
    UnsupportedContractVersion,
    _validator,
    validate_official_backtest_request,
)
from .worker import SqsClient


__all__ = [
    "BACKTEST_REQUEST_CONTRACT_VERSION",
    "BacktestRequestIntake",
    "InMemoryRequestReceiptStore",
    "PostgresRequestReceiptStore",
    "RequestClaim",
    "RequestClaimDisposition",
    "RequestHandler",
    "RequestIntakeConfig",
    "RequestIntakeDisposition",
    "RequestIntakeOutcome",
    "RequestLane",
    "RequestProcessingError",
    "RequestReceiptStore",
    "TransportEnvelope",
    "validate_backtest_request",
]


BACKTEST_REQUEST_CONTRACT_VERSION = "backtest-request.v1"
BACKTEST_REQUEST_SCHEMA = (
    "https://contracts.idea2strategy.io/backtest-request/v1/backtest-request.schema.json"
)
_OWNER_DOMAINS = {
    "BASIC": "strategy-bot",
    "CUSTOM": "backtest-request",
    "COMPETITION": "backtest-request",
}
_HASH_PATTERN_LENGTH = 64
_LOGGER = logging.getLogger(__name__)


class RequestLane(StrEnum):
    BASIC = "BASIC"
    CUSTOM = "CUSTOM"
    COMPETITION = "COMPETITION"

    @property
    def event_type(self) -> str:
        return (
            "OFFICIAL_BACKTEST_REQUESTED"
            if self is RequestLane.BASIC
            else f"{self.value}_BACKTEST_REQUESTED"
        )


class RequestIntakeDisposition(StrEnum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    RETURNED = "RETURNED"
    DEAD_LETTERED = "DEAD_LETTERED"


class RequestClaimDisposition(StrEnum):
    CLAIMED = "CLAIMED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    CONFLICT = "CONFLICT"
    BUSY = "BUSY"


class RequestProcessingError(RuntimeError):
    """A stable handler failure that states whether redelivery can help."""

    def __init__(self, reason_code: str, *, retryable: bool) -> None:
        if not reason_code or len(reason_code) > 80:
            raise ValueError("reason_code must contain 1..80 characters")
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class TransportEnvelope:
    event_type: str
    contract_version: str
    owner_domain: str
    aggregate_id: uuid.UUID
    aggregate_sequence: int
    message_id: uuid.UUID
    producer_idempotency_key: str
    outbox_idempotency_key: str
    payload_hash: str


@dataclass(frozen=True, slots=True)
class RequestClaim:
    disposition: RequestClaimDisposition


@dataclass(frozen=True, slots=True)
class RequestIntakeOutcome:
    message_id: str
    disposition: RequestIntakeDisposition
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class RequestIntakeConfig:
    lane: RequestLane
    queue_url: str
    dead_letter_queue_url: str
    consumer_id: str
    max_receive_count: int
    visibility_timeout: timedelta
    wait_time: timedelta

    def __post_init__(self) -> None:
        for field in ("queue_url", "dead_letter_queue_url", "consumer_id"):
            value = getattr(self, field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field} must not be blank")
        if self.queue_url == self.dead_letter_queue_url:
            raise ValueError("queue_url and dead_letter_queue_url must differ")
        if self.max_receive_count < 1:
            raise ValueError("max_receive_count must be positive")
        if self.visibility_timeout <= timedelta(0):
            raise ValueError("visibility_timeout must be positive")
        if not timedelta(0) <= self.wait_time <= timedelta(seconds=20):
            raise ValueError("wait_time must be between zero and twenty seconds")


class RequestHandler(Protocol):
    def __call__(self, request: Mapping[str, Any], lane: RequestLane) -> None: ...


class RequestReceiptStore(Protocol):
    """Durable message receipt and aggregate ordering compare-and-set boundary."""

    def claim(
        self,
        *,
        handler_id: str,
        envelope: TransportEnvelope,
        claimed_by: str,
        now: datetime,
        claim_expires_at: datetime,
    ) -> RequestClaim: ...

    def complete(
        self, *, handler_id: str, message_id: uuid.UUID, payload_hash: str, now: datetime
    ) -> None: ...

    def retry(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None: ...

    def fail(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None: ...


@dataclass(slots=True)
class _Receipt:
    payload_hash: str
    aggregate_id: uuid.UUID
    aggregate_sequence: int
    status: str
    claim_expires_at: datetime | None


class InMemoryRequestReceiptStore:
    """Thread-safe reference semantics for the production receipt adapter."""

    def __init__(self) -> None:
        self._receipts: dict[tuple[str, uuid.UUID], _Receipt] = {}
        self._latest: dict[tuple[str, uuid.UUID], int] = {}
        self._lock = threading.RLock()

    @property
    def completed_message_ids(self) -> set[uuid.UUID]:
        with self._lock:
            return {
                message_id
                for (_, message_id), receipt in self._receipts.items()
                if receipt.status == "COMPLETED"
            }

    def claim(
        self,
        *,
        handler_id: str,
        envelope: TransportEnvelope,
        claimed_by: str,
        now: datetime,
        claim_expires_at: datetime,
    ) -> RequestClaim:
        del claimed_by
        key = (handler_id, envelope.message_id)
        aggregate_key = (handler_id, envelope.aggregate_id)
        with self._lock:
            existing = self._receipts.get(key)
            if existing is not None:
                if existing.payload_hash != envelope.payload_hash:
                    return RequestClaim(RequestClaimDisposition.CONFLICT)
                if existing.status == "COMPLETED":
                    return RequestClaim(RequestClaimDisposition.DUPLICATE)
                if (
                    existing.status == "PROCESSING"
                    and existing.claim_expires_at is not None
                    and existing.claim_expires_at > now
                ):
                    return RequestClaim(RequestClaimDisposition.BUSY)
            latest = self._latest.get(aggregate_key, 0)
            if existing is None and envelope.aggregate_sequence <= latest:
                return RequestClaim(RequestClaimDisposition.STALE)
            self._receipts[key] = _Receipt(
                payload_hash=envelope.payload_hash,
                aggregate_id=envelope.aggregate_id,
                aggregate_sequence=envelope.aggregate_sequence,
                status="PROCESSING",
                claim_expires_at=claim_expires_at,
            )
            self._latest[aggregate_key] = max(latest, envelope.aggregate_sequence)
            return RequestClaim(RequestClaimDisposition.CLAIMED)

    def complete(
        self, *, handler_id: str, message_id: uuid.UUID, payload_hash: str, now: datetime
    ) -> None:
        del now
        self._transition(handler_id, message_id, payload_hash, "COMPLETED")

    def retry(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None:
        del reason_code, now
        self._transition(handler_id, message_id, payload_hash, "RETRYABLE_FAILURE")

    def fail(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None:
        del reason_code, now
        self._transition(handler_id, message_id, payload_hash, "PERMANENT_FAILURE")

    def _transition(
        self, handler_id: str, message_id: uuid.UUID, payload_hash: str, status: str
    ) -> None:
        with self._lock:
            receipt = self._receipts[(handler_id, message_id)]
            if receipt.payload_hash != payload_hash:
                raise RuntimeError("receipt payload hash changed while claimed")
            receipt.status = status
            receipt.claim_expires_at = None


class PostgresRequestReceiptStore:
    """PostgreSQL CAS over the canonical Outbox consumer receipt table.

    The advisory transaction lock serializes claims for one handler/aggregate,
    including two previously unseen message IDs.  Without it both transactions
    could observe the same latest sequence and accept an older event after a newer
    one.  The Outbox row is re-read as authoritative evidence; forged SQS
    attributes cannot create a receipt for different bytes or ordering metadata.
    """

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def claim(
        self,
        *,
        handler_id: str,
        envelope: TransportEnvelope,
        claimed_by: str,
        now: datetime,
        claim_expires_at: datetime,
    ) -> RequestClaim:
        parameters = {
            "handler_id": handler_id,
            "message_id": envelope.message_id,
            "payload_hash": envelope.payload_hash,
            "producer_key": envelope.producer_idempotency_key,
            "aggregate_id": envelope.aggregate_id,
            "aggregate_sequence": envelope.aggregate_sequence,
            "claimed_by": claimed_by,
            "now": now,
            "claim_expires_at": claim_expires_at,
            "claim_token": uuid.uuid4(),
        }
        with self._engine.begin() as connection:
            connection.execute(
                text(
                    "SELECT pg_advisory_xact_lock(hashtextextended("
                    "CAST(:handler_id AS text) || ':' || CAST(:aggregate_id AS text), 0))"
                ),
                parameters,
            )
            source = connection.execute(
                text(
                    """
                    SELECT owner_domain, aggregate_id, aggregate_sequence, event_type,
                           event_schema_version, producer_idempotency_key, payload_hash
                      FROM operations.outbox_messages
                     WHERE id = :message_id
                    """
                ),
                parameters,
            ).mappings().first()
            if source is None or any(
                (
                    str(source["aggregate_id"]) != str(envelope.aggregate_id),
                    int(source["aggregate_sequence"]) != envelope.aggregate_sequence,
                    str(source["owner_domain"]) != envelope.owner_domain,
                    str(source["event_type"]) != envelope.event_type,
                    str(source["event_schema_version"]) != envelope.contract_version,
                    str(source["producer_idempotency_key"])
                    != envelope.producer_idempotency_key,
                    str(source["payload_hash"]) != envelope.payload_hash,
                )
            ):
                return RequestClaim(RequestClaimDisposition.CONFLICT)

            receipt = connection.execute(
                text(
                    """
                    SELECT payload_hash, status::text, claim_expires_at
                      FROM operations.outbox_consumer_receipts
                     WHERE consumer_handler_id = :handler_id
                       AND outbox_message_id = :message_id
                     FOR UPDATE
                    """
                ),
                parameters,
            ).mappings().first()
            if receipt is not None:
                if str(receipt["payload_hash"]) != envelope.payload_hash:
                    return RequestClaim(RequestClaimDisposition.CONFLICT)
                status = str(receipt["status"])
                if status in {"COMPLETED", "PERMANENT_FAILURE"}:
                    return RequestClaim(RequestClaimDisposition.DUPLICATE)
                expires = receipt["claim_expires_at"]
                if status == "PROCESSING" and expires is not None and expires > now:
                    return RequestClaim(RequestClaimDisposition.BUSY)
                connection.execute(
                    text(
                        """
                        UPDATE operations.outbox_consumer_receipts
                           SET status = 'PROCESSING', claim_token = :claim_token,
                               claimed_by = :claimed_by, claimed_at = :now,
                               claim_expires_at = :claim_expires_at,
                               receive_attempt_count = receive_attempt_count + 1,
                               last_received_at = :now, failure_code = NULL
                         WHERE consumer_handler_id = :handler_id
                           AND outbox_message_id = :message_id
                        """
                    ),
                    parameters,
                )
                return RequestClaim(RequestClaimDisposition.CLAIMED)

            latest = connection.execute(
                text(
                    """
                    SELECT COALESCE(MAX(o.aggregate_sequence), 0)
                      FROM operations.outbox_consumer_receipts r
                      JOIN operations.outbox_messages o ON o.id = r.outbox_message_id
                     WHERE r.consumer_handler_id = :handler_id
                       AND o.aggregate_id = :aggregate_id
                    """
                ),
                parameters,
            ).scalar_one()
            if envelope.aggregate_sequence <= int(latest):
                return RequestClaim(RequestClaimDisposition.STALE)
            connection.execute(
                text(
                    """
                    INSERT INTO operations.outbox_consumer_receipts
                        (consumer_handler_id, outbox_message_id,
                         producer_idempotency_key, payload_hash, status,
                         claim_token, claimed_by, claimed_at, claim_expires_at,
                         receive_attempt_count, first_received_at, last_received_at)
                    VALUES
                        (:handler_id, :message_id, :producer_key, :payload_hash,
                         'PROCESSING', :claim_token, :claimed_by, :now,
                         :claim_expires_at, 1, :now, :now)
                    """
                ),
                parameters,
            )
            return RequestClaim(RequestClaimDisposition.CLAIMED)

    def complete(
        self, *, handler_id: str, message_id: uuid.UUID, payload_hash: str, now: datetime
    ) -> None:
        self._transition(
            handler_id=handler_id,
            message_id=message_id,
            payload_hash=payload_hash,
            status="COMPLETED",
            reason_code=None,
            now=now,
        )

    def retry(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None:
        self._transition(
            handler_id=handler_id,
            message_id=message_id,
            payload_hash=payload_hash,
            status="RETRYABLE_FAILURE",
            reason_code=reason_code,
            now=now,
        )

    def fail(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        reason_code: str,
        now: datetime,
    ) -> None:
        self._transition(
            handler_id=handler_id,
            message_id=message_id,
            payload_hash=payload_hash,
            status="PERMANENT_FAILURE",
            reason_code=reason_code,
            now=now,
        )

    def _transition(
        self,
        *,
        handler_id: str,
        message_id: uuid.UUID,
        payload_hash: str,
        status: str,
        reason_code: str | None,
        now: datetime,
    ) -> None:
        completed_at = now if status == "COMPLETED" else None
        with self._engine.begin() as connection:
            changed = connection.execute(
                text(
                    """
                    UPDATE operations.outbox_consumer_receipts
                       SET status = CAST(:status AS operations.consumer_receipt_status),
                           claim_token = NULL, claimed_by = NULL, claimed_at = NULL,
                           claim_expires_at = NULL, last_received_at = :now,
                           completed_at = :completed_at, failure_code = :reason_code
                     WHERE consumer_handler_id = :handler_id
                       AND outbox_message_id = :message_id
                       AND payload_hash = :payload_hash
                       AND status = 'PROCESSING'
                    """
                ),
                {
                    "handler_id": handler_id,
                    "message_id": message_id,
                    "payload_hash": payload_hash,
                    "status": status,
                    "reason_code": reason_code,
                    "completed_at": completed_at,
                    "now": now,
                },
            ).rowcount
        if changed != 1:
            raise RuntimeError("request receipt transition lost its claim or payload identity")


def _sha256_prefixed(material: str) -> str:
    return "sha256:" + hashlib.sha256(material.encode()).hexdigest()


def _java_name_uuid(material: str) -> uuid.UUID:
    digest = hashlib.md5(material.encode(), usedforsecurity=False).digest()
    return uuid.UUID(bytes=digest, version=3)


def validate_backtest_request(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate shape plus the producer's content-bound identities."""

    if not isinstance(document, Mapping):
        raise ContractValidationError("backtest request must be a JSON object")
    instance = dict(document)
    metadata = instance.get("metadata")
    if not isinstance(metadata, Mapping):
        raise ContractValidationError("backtest request metadata must be an object")
    actual_version = metadata.get("contractVersion")
    if actual_version == "strategy-bot.v1":
        request = validate_official_backtest_request(instance)
        if request.get("lane") != RequestLane.BASIC.value:
            raise ContractValidationError("official backtest request lane must be BASIC")
        if request.get("aggregateSequence") != 1:
            raise ContractValidationError("official backtest aggregateSequence must be 1")
        return request
    if actual_version != BACKTEST_REQUEST_CONTRACT_VERSION:
        raise UnsupportedContractVersion(
            f"backtest request declares {actual_version!r}; only "
            f"{BACKTEST_REQUEST_CONTRACT_VERSION!r} is supported"
        )
    error = best_match(_validator(BACKTEST_REQUEST_SCHEMA).iter_errors(instance))
    if error is not None:
        location = ".".join(str(part) for part in error.absolute_path) or "(root)"
        raise ContractValidationError(f"backtest request.{location}: {error.message}")

    try:
        event_type = str(metadata["messageType"])
        idempotency_key = str(metadata["idempotencyKey"])
        message_id = uuid.UUID(str(metadata["messageId"]))
        occurred_at = datetime.fromisoformat(str(metadata["occurredAt"]).replace("Z", "+00:00"))
        if occurred_at.tzinfo is None:
            raise ValueError("occurredAt must include an offset")
        if Decimal(str(instance["initialCashAmount"])) <= 0:
            raise ValueError("initialCashAmount must be positive")
        if event_type == RequestLane.CUSTOM.event_type:
            for field in ("requestingAccountId", "botId", "runId", "datasetManifestId"):
                uuid.UUID(str(instance[field]))
            start = date.fromisoformat(str(instance["periodStart"]))
            end = date.fromisoformat(str(instance["periodEnd"]))
            if end < start:
                raise ContractValidationError("periodEnd must not precede periodStart")
            request_material = "\n".join(
                str(instance[field])
                for field in (
                    "requestingAccountId",
                    "botId",
                    "datasetManifestId",
                    "expectedDatasetHash",
                    "periodStart",
                    "periodEnd",
                    "expectedSnapshotHash",
                    "compiledPlanChecksum",
                    "instrumentCatalogVersion",
                    "initialCashAmount",
                    "assumptionsVersion",
                    "executionPolicyVersion",
                )
            )
            # The client idempotency key is deliberately not repeated in the
            # public envelope, so its one-way producer digest cannot be
            # recomputed. The canonical Outbox row is checked by the receipt
            # store before dispatch instead.
            expected_key = None
        else:
            for field in ("runId", "roomId", "participationId", "botId"):
                uuid.UUID(str(instance[field]))
            periods = instance["periods"]
            if not isinstance(periods, list) or len(periods) != 1:
                raise ContractValidationError(
                    "competition runtime envelope must contain exactly one period"
                )
            period = periods[0]
            if Decimal(str(period["importanceWeight"])) <= 0:
                raise ValueError("importanceWeight must be positive")
            request_fields = [
                str(instance[field])
                for field in (
                    "roomId",
                    "participationId",
                    "botId",
                    "planVersion",
                    "planHash",
                    "expectedSnapshotHash",
                    "compiledPlanChecksum",
                    "assumptionsVersion",
                    "executionPolicyVersion",
                    "scoringTemplateVersionId",
                    "roomRulesHash",
                    "initialCashAmount",
                    "currencyCode",
                )
            ]
            request_fields.extend(
                str(period[field])
                for field in (
                    "evaluationPeriodId",
                    "periodSequence",
                    "evaluationStart",
                    "evaluationEnd",
                    "importanceWeight",
                    "inputSetHash",
                )
            )
            for dataset in sorted(
                period["datasets"],
                key=lambda item: (item["purposeCode"], item["datasetManifestId"]),
            ):
                request_fields.extend(
                    str(dataset[field])
                    for field in (
                        "datasetManifestId",
                        "purposeCode",
                        "expectedDatasetHash",
                    )
                )
            for feature in sorted(
                period["featureMaterializations"],
                key=lambda item: item["featureMaterializationId"],
            ):
                request_fields.extend(
                    str(feature[field])
                    for field in ("featureMaterializationId", "lockedResultHash")
                )
            request_material = "\n".join(request_fields)
            expected_key = _sha256_prefixed(
                "\n".join(
                    (
                        "COMPETITION_PERIOD",
                        str(instance["roomId"]),
                        str(instance["participationId"]),
                        str(period["evaluationPeriodId"]),
                        str(instance["planHash"]),
                    )
                )
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise ContractValidationError(f"backtest request semantic field is invalid: {exc}") from exc

    if instance["requestHash"] != _sha256_prefixed(request_material):
        raise ContractValidationError("requestHash does not match the canonical request fields")
    if expected_key is not None and idempotency_key != expected_key:
        raise ContractValidationError("idempotencyKey does not match competition identity")
    if str(metadata["correlationId"]) != str(instance["runId"]):
        raise ContractValidationError("correlationId does not match runId")
    expected_message_id = _java_name_uuid(f"{event_type}:{idempotency_key}")
    if message_id != expected_message_id:
        raise ContractValidationError("messageId does not match event type and idempotency key")
    return instance


def _attribute(attributes: Mapping[str, Any], name: str) -> str:
    value = attributes.get(name)
    if not isinstance(value, Mapping):
        raise ValueError(f"missing message attribute {name}")
    if value.get("DataType") != "String":
        raise ValueError(f"message attribute {name} must be a String")
    text = value.get("StringValue")
    if not isinstance(text, str) or not text:
        raise ValueError(f"message attribute {name} must not be blank")
    return text


def _transport(message: Mapping[str, Any], body: str) -> TransportEnvelope:
    attributes = message.get("MessageAttributes")
    if not isinstance(attributes, Mapping):
        raise ValueError("MessageAttributes are required")
    payload_hash = _attribute(attributes, "payloadHash")
    if len(payload_hash) != _HASH_PATTERN_LENGTH or any(
        character not in "0123456789abcdef" for character in payload_hash
    ):
        raise ValueError("payloadHash must be 64 lowercase hexadecimal characters")
    sequence = int(_attribute(attributes, "aggregateSequence"))
    if sequence < 1:
        raise ValueError("aggregateSequence must be positive")
    return TransportEnvelope(
        event_type=_attribute(attributes, "eventType"),
        contract_version=_attribute(attributes, "contractVersion"),
        owner_domain=_attribute(attributes, "ownerDomain"),
        aggregate_id=uuid.UUID(_attribute(attributes, "aggregateId")),
        aggregate_sequence=sequence,
        message_id=uuid.UUID(_attribute(attributes, "messageId")),
        producer_idempotency_key=_attribute(attributes, "idempotencyKey"),
        outbox_idempotency_key=_attribute(attributes, "outboxIdempotencyKey"),
        payload_hash=payload_hash,
    )


class BacktestRequestIntake:
    """Validate, order and dispatch one Custom or Competition request queue."""

    def __init__(
        self,
        *,
        client: SqsClient,
        config: RequestIntakeConfig,
        handler: RequestHandler,
        receipts: RequestReceiptStore,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._handler = handler
        self._receipts = receipts
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def poll_once(self) -> tuple[RequestIntakeOutcome, ...]:
        response = self._client.receive_message(
            QueueUrl=self._config.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=int(self._config.wait_time.total_seconds()),
            VisibilityTimeout=int(self._config.visibility_timeout.total_seconds()),
            MessageSystemAttributeNames=["ApproximateReceiveCount"],
            MessageAttributeNames=["All"],
        )
        return tuple(self.handle(message) for message in response.get("Messages", []))

    def run(self, stop: threading.Event) -> None:
        """Poll until the shared worker shutdown event is set."""

        while not stop.is_set():
            try:
                self.poll_once()
            except Exception:  # pragma: no cover - SDK/network boundary
                _LOGGER.exception("backtest request intake poll failed; retrying")
                stop.wait(1)

    def handle(self, message: Mapping[str, Any]) -> RequestIntakeOutcome:
        sqs_message_id = str(message.get("MessageId", ""))
        receipt_handle = str(message["ReceiptHandle"])
        receive_count = int(message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))
        body = str(message.get("Body", ""))
        if receive_count > self._config.max_receive_count:
            return self._dead_letter(
                body, receipt_handle, "MAX_RECEIVE_COUNT_EXCEEDED", sqs_message_id
            )
        try:
            envelope = _transport(message, body)
        except (TypeError, ValueError):
            return self._dead_letter(
                body, receipt_handle, "TRANSPORT_ENVELOPE_INVALID", sqs_message_id
            )
        actual_hash = hashlib.sha256(body.encode()).hexdigest()
        if envelope.payload_hash != actual_hash:
            return self._dead_letter(
                body, receipt_handle, "PAYLOAD_HASH_MISMATCH", sqs_message_id
            )
        try:
            decoded = json.loads(body)
            request = validate_backtest_request(decoded)
        except UnsupportedContractVersion:
            return self._dead_letter(
                body, receipt_handle, "UNSUPPORTED_CONTRACT_VERSION", sqs_message_id
            )
        except (json.JSONDecodeError, ContractValidationError, TypeError):
            return self._dead_letter(
                body, receipt_handle, "CONTRACT_VIOLATION", sqs_message_id
            )
        mismatch = self._transport_mismatch(envelope, request)
        if mismatch is not None:
            return self._dead_letter(body, receipt_handle, mismatch, sqs_message_id)

        now = self._clock()
        claim = self._receipts.claim(
            handler_id=self._config.consumer_id,
            envelope=envelope,
            claimed_by=self._config.consumer_id,
            now=now,
            claim_expires_at=now + self._config.visibility_timeout,
        )
        if claim.disposition is RequestClaimDisposition.CONFLICT:
            return self._dead_letter(
                body, receipt_handle, "IDEMPOTENCY_CONFLICT", sqs_message_id
            )
        if claim.disposition is RequestClaimDisposition.DUPLICATE:
            self._delete(receipt_handle)
            return RequestIntakeOutcome(sqs_message_id, RequestIntakeDisposition.DUPLICATE)
        if claim.disposition is RequestClaimDisposition.STALE:
            self._delete(receipt_handle)
            return RequestIntakeOutcome(
                sqs_message_id, RequestIntakeDisposition.STALE, "STALE_AGGREGATE_SEQUENCE"
            )
        if claim.disposition is RequestClaimDisposition.BUSY:
            return self._return(receipt_handle, sqs_message_id, "MESSAGE_ALREADY_PROCESSING")

        try:
            self._handler(request, self._config.lane)
        except RequestProcessingError as exc:
            if exc.retryable and receive_count < self._config.max_receive_count:
                self._receipts.retry(
                    handler_id=self._config.consumer_id,
                    message_id=envelope.message_id,
                    payload_hash=envelope.payload_hash,
                    reason_code=exc.reason_code,
                    now=self._clock(),
                )
                return self._return(receipt_handle, sqs_message_id, exc.reason_code)
            self._receipts.fail(
                handler_id=self._config.consumer_id,
                message_id=envelope.message_id,
                payload_hash=envelope.payload_hash,
                reason_code=exc.reason_code,
                now=self._clock(),
            )
            return self._dead_letter(body, receipt_handle, exc.reason_code, sqs_message_id)
        except Exception as exc:
            reason = f"INTAKE_ERROR:{type(exc).__name__}"
            if receive_count < self._config.max_receive_count:
                self._receipts.retry(
                    handler_id=self._config.consumer_id,
                    message_id=envelope.message_id,
                    payload_hash=envelope.payload_hash,
                    reason_code=reason,
                    now=self._clock(),
                )
                return self._return(receipt_handle, sqs_message_id, reason)
            self._receipts.fail(
                handler_id=self._config.consumer_id,
                message_id=envelope.message_id,
                payload_hash=envelope.payload_hash,
                reason_code=reason,
                now=self._clock(),
            )
            return self._dead_letter(body, receipt_handle, reason, sqs_message_id)

        self._receipts.complete(
            handler_id=self._config.consumer_id,
            message_id=envelope.message_id,
            payload_hash=envelope.payload_hash,
            now=self._clock(),
        )
        self._delete(receipt_handle)
        return RequestIntakeOutcome(sqs_message_id, RequestIntakeDisposition.ACCEPTED)

    def _transport_mismatch(
        self, envelope: TransportEnvelope, request: Mapping[str, Any]
    ) -> str | None:
        metadata = request["metadata"]
        event_type = str(metadata["messageType"])
        if event_type != self._config.lane.event_type:
            return "UNEXPECTED_MESSAGE_TYPE"
        aggregate_id = request["runId"]
        expected = (
            envelope.event_type == event_type
            and envelope.contract_version == metadata["contractVersion"]
            and envelope.owner_domain == _OWNER_DOMAINS[self._config.lane.value]
            and str(envelope.aggregate_id)
            == (str(request["botId"]) if self._config.lane is RequestLane.BASIC else aggregate_id)
            and str(envelope.message_id) == metadata["messageId"]
            and envelope.producer_idempotency_key == metadata["idempotencyKey"]
            and envelope.aggregate_sequence == request["aggregateSequence"]
        )
        return None if expected else "TRANSPORT_ENVELOPE_MISMATCH"

    def _return(
        self, receipt_handle: str, message_id: str, reason_code: str
    ) -> RequestIntakeOutcome:
        self._client.change_message_visibility(
            QueueUrl=self._config.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=0,
        )
        return RequestIntakeOutcome(
            message_id, RequestIntakeDisposition.RETURNED, reason_code
        )

    def _delete(self, receipt_handle: str) -> None:
        self._client.delete_message(
            QueueUrl=self._config.queue_url, ReceiptHandle=receipt_handle
        )

    def _dead_letter(
        self, body: str, receipt_handle: str, reason_code: str, message_id: str
    ) -> RequestIntakeOutcome:
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
                "DeadLetteredAt": {
                    "DataType": "String",
                    "StringValue": self._clock().isoformat(),
                },
            },
        )
        self._delete(receipt_handle)
        return RequestIntakeOutcome(
            message_id, RequestIntakeDisposition.DEAD_LETTERED, reason_code
        )
