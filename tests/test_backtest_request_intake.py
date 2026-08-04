from __future__ import annotations

import copy
import hashlib
import json
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from backtest_engine.backtest_request_intake import (
    BacktestRequestIntake,
    InMemoryRequestReceiptStore,
    RequestIntakeConfig,
    RequestIntakeDisposition,
    RequestLane,
    RequestProcessingError,
)


NOW = datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
BOT_ID = uuid.UUID("97000000-0000-4000-8000-000000000002")
DATASET_ID = uuid.UUID("97000000-0000-4000-8000-000000000003")
ROOM_ID = uuid.UUID("97000000-0000-4000-8000-000000000004")
PARTICIPATION_ID = uuid.UUID("97000000-0000-4000-8000-000000000005")
SNAPSHOT = "sha256:" + "1" * 64
PLAN = "sha256:" + "2" * 64
ROOM_PLAN = "sha256:" + "3" * 64


def _sha(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _java_name_uuid(value: str) -> str:
    digest = hashlib.md5(value.encode(), usedforsecurity=False).digest()
    return str(uuid.UUID(bytes=digest, version=3))


def custom_request(
    *, period_start: str = "2024-01-01", client_key: str = "request-42"
) -> dict[str, Any]:
    key = _sha(f"CUSTOM\n97000000-0000-4000-8000-000000000001\n{client_key}")
    request_hash = _sha(
        "\n".join(
            (
                str(BOT_ID),
                str(DATASET_ID),
                period_start,
                "2024-12-31",
                SNAPSHOT,
                PLAN,
                "accounting-v1",
            )
        )
    )
    event_type = "CUSTOM_BACKTEST_REQUESTED"
    return {
        "metadata": {
            "contractVersion": "backtest-request.v1",
            "messageType": event_type,
            "messageId": _java_name_uuid(f"{event_type}:{key}"),
            "occurredAt": "2026-08-04T12:00:00Z",
            "correlationId": str(BOT_ID),
            "idempotencyKey": key,
        },
        "requestReason": "USER_PERIOD",
        "requestHash": request_hash,
        "botId": str(BOT_ID),
        "expectedSnapshotHash": SNAPSHOT,
        "compiledPlanChecksum": PLAN,
        "datasetManifestId": str(DATASET_ID),
        "periodStart": period_start,
        "periodEnd": "2024-12-31",
        "assumptionsVersion": "accounting-v1",
    }


def competition_request() -> dict[str, Any]:
    key = _sha(f"COMPETITION\n{ROOM_ID}\n{PARTICIPATION_ID}\n{ROOM_PLAN}")
    request_hash = _sha(
        "\n".join(
            (
                str(ROOM_ID),
                str(PARTICIPATION_ID),
                str(BOT_ID),
                "competition-plan.v1",
                ROOM_PLAN,
                SNAPSHOT,
                PLAN,
                "accounting-v1",
            )
        )
    )
    event_type = "COMPETITION_BACKTEST_REQUESTED"
    return {
        "metadata": {
            "contractVersion": "backtest-request.v1",
            "messageType": event_type,
            "messageId": _java_name_uuid(f"{event_type}:{key}"),
            "occurredAt": "2026-08-04T12:00:00Z",
            "correlationId": str(PARTICIPATION_ID),
            "idempotencyKey": key,
        },
        "requestReason": "COMPETITION_EVALUATION",
        "requestHash": request_hash,
        "roomId": str(ROOM_ID),
        "participationId": str(PARTICIPATION_ID),
        "botId": str(BOT_ID),
        "planVersion": "competition-plan.v1",
        "planHash": ROOM_PLAN,
        "expectedSnapshotHash": SNAPSHOT,
        "compiledPlanChecksum": PLAN,
        "assumptionsVersion": "accounting-v1",
    }


def delivery(document: Mapping[str, Any], *, sequence: int = 1) -> dict[str, Any]:
    body = json.dumps(document, sort_keys=True, separators=(",", ":"))
    metadata = document["metadata"]
    event_type = str(metadata["messageType"])
    aggregate_id = (
        document["botId"]
        if event_type == "CUSTOM_BACKTEST_REQUESTED"
        else document["participationId"]
    )
    attributes = {
        "eventType": event_type,
        "contractVersion": "backtest-request.v1",
        "ownerDomain": "backtest-request",
        "aggregateId": str(aggregate_id),
        "aggregateSequence": str(sequence),
        "messageId": str(metadata["messageId"]),
        "idempotencyKey": str(metadata["idempotencyKey"]),
        "outboxIdempotencyKey": str(metadata["idempotencyKey"]),
        "payloadHash": hashlib.sha256(body.encode()).hexdigest(),
    }
    return {
        "MessageId": f"sqs-{metadata['messageId']}-{sequence}",
        "ReceiptHandle": f"receipt-{metadata['messageId']}-{sequence}",
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": "1"},
        "MessageAttributes": {
            key: {"DataType": "String", "StringValue": value}
            for key, value in attributes.items()
        },
    }


class RecordingSqs:
    def __init__(self) -> None:
        self.inbox: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.returned: list[str] = []
        self.dead_letters: list[dict[str, Any]] = []

    def receive_message(self, **kwargs: Any) -> Mapping[str, Any]:
        if not self.inbox:
            return {}
        return {"Messages": [self.inbox.pop(0)]}

    def delete_message(self, **kwargs: Any) -> None:
        self.deleted.append(str(kwargs["ReceiptHandle"]))

    def change_message_visibility(self, **kwargs: Any) -> None:
        self.returned.append(str(kwargs["ReceiptHandle"]))

    def send_message(self, **kwargs: Any) -> None:
        self.dead_letters.append(dict(kwargs))


def config(lane: RequestLane) -> RequestIntakeConfig:
    return RequestIntakeConfig(
        lane=lane,
        queue_url=f"https://sqs.test/{lane.value.lower()}",
        dead_letter_queue_url=f"https://sqs.test/{lane.value.lower()}-dlq",
        consumer_id=f"backtest-{lane.value.lower()}-request-v1",
        max_receive_count=3,
        visibility_timeout=timedelta(seconds=30),
        wait_time=timedelta(0),
    )


def intake(
    lane: RequestLane,
    handler: Any,
    *,
    client: RecordingSqs | None = None,
    store: InMemoryRequestReceiptStore | None = None,
) -> tuple[BacktestRequestIntake, RecordingSqs, InMemoryRequestReceiptStore]:
    sqs = client or RecordingSqs()
    receipts = store or InMemoryRequestReceiptStore()
    return (
        BacktestRequestIntake(
            client=sqs,
            config=config(lane),
            handler=handler,
            receipts=receipts,
            clock=lambda: NOW,
        ),
        sqs,
        receipts,
    )


@pytest.mark.parametrize(
    ("lane", "factory"),
    ((RequestLane.CUSTOM, custom_request), (RequestLane.COMPETITION, competition_request)),
)
def test_accepts_exact_backend_payload_and_routes_only_to_its_lane(
    lane: RequestLane, factory: Any
) -> None:
    handled: list[tuple[RequestLane, Mapping[str, Any]]] = []
    consumer, sqs, receipts = intake(
        lane, lambda request, observed_lane: handled.append((observed_lane, request))
    )
    message = delivery(factory())

    outcome = consumer.handle(message)

    assert outcome.disposition is RequestIntakeDisposition.ACCEPTED
    assert [item[0] for item in handled] == [lane]
    assert handled[0][1]["metadata"]["messageType"] == f"{lane.value}_BACKTEST_REQUESTED"
    assert sqs.deleted == [message["ReceiptHandle"]]
    assert sqs.dead_letters == []
    assert receipts.completed_message_ids == {uuid.UUID(factory()["metadata"]["messageId"])}


def test_wrong_lane_is_dead_lettered_without_invoking_handler() -> None:
    handled: list[Any] = []
    consumer, sqs, _ = intake(RequestLane.CUSTOM, lambda *args: handled.append(args))
    message = delivery(competition_request())

    outcome = consumer.handle(message)

    assert outcome.disposition is RequestIntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "UNEXPECTED_MESSAGE_TYPE"
    assert handled == []
    assert sqs.dead_letters[0]["QueueUrl"].endswith("custom-dlq")


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        (
            lambda message: message["MessageAttributes"]["payloadHash"].update(
                StringValue="0" * 64
            ),
            "PAYLOAD_HASH_MISMATCH",
        ),
        (
            lambda message: message["MessageAttributes"]["aggregateId"].update(
                StringValue=str(ROOM_ID)
            ),
            "TRANSPORT_ENVELOPE_MISMATCH",
        ),
        (lambda message: message["MessageAttributes"].pop("aggregateSequence"), "TRANSPORT_ENVELOPE_INVALID"),
    ),
)
def test_transport_tampering_fails_closed(mutation: Any, reason: str) -> None:
    consumer, sqs, _ = intake(RequestLane.CUSTOM, lambda *_: pytest.fail("must not run"))
    message = delivery(custom_request())
    mutation(message)

    outcome = consumer.handle(message)

    assert outcome.disposition is RequestIntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == reason
    assert sqs.deleted == [message["ReceiptHandle"]]


@pytest.mark.parametrize(
    "field", ("requestHash", "periodEnd", "compiledPlanChecksum", "datasetManifestId")
)
def test_custom_semantic_tampering_fails_closed(field: str) -> None:
    request = custom_request()
    request[field] = (
        "2023-01-01"
        if field == "periodEnd"
        else "not-a-uuid"
        if field == "datasetManifestId"
        else "sha256:" + "9" * 64
    )
    consumer, _, _ = intake(RequestLane.CUSTOM, lambda *_: pytest.fail("must not run"))

    outcome = consumer.handle(delivery(request))

    assert outcome.disposition is RequestIntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"


def test_competition_identity_and_request_hash_are_recomputed() -> None:
    request = competition_request()
    request["planHash"] = "sha256:" + "9" * 64
    consumer, _, _ = intake(RequestLane.COMPETITION, lambda *_: pytest.fail("must not run"))

    outcome = consumer.handle(delivery(request))

    assert outcome.disposition is RequestIntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"


def test_same_message_and_hash_is_a_completed_duplicate_with_no_second_effect() -> None:
    handled: list[Any] = []
    store = InMemoryRequestReceiptStore()
    consumer, sqs, _ = intake(
        RequestLane.CUSTOM, lambda *args: handled.append(args), store=store
    )
    first = delivery(custom_request())
    duplicate = delivery(custom_request())
    duplicate["ReceiptHandle"] = "receipt-redelivery"

    one = consumer.handle(first)
    two = consumer.handle(duplicate)

    assert one.disposition is RequestIntakeDisposition.ACCEPTED
    assert two.disposition is RequestIntakeDisposition.DUPLICATE
    assert len(handled) == 1
    assert sqs.deleted == [first["ReceiptHandle"], "receipt-redelivery"]


def test_same_message_id_with_a_different_payload_hash_is_permanent_conflict() -> None:
    store = InMemoryRequestReceiptStore()
    consumer, sqs, _ = intake(RequestLane.CUSTOM, lambda *_: None, store=store)
    consumer.handle(delivery(custom_request()))
    conflicting = delivery(custom_request(period_start="2023-01-01"), sequence=2)
    original_id = custom_request()["metadata"]["messageId"]
    conflicting_doc = json.loads(conflicting["Body"])
    conflicting_doc["metadata"]["messageId"] = original_id
    conflicting["Body"] = json.dumps(conflicting_doc, sort_keys=True, separators=(",", ":"))
    conflicting["MessageAttributes"]["messageId"]["StringValue"] = original_id
    conflicting["MessageAttributes"]["payloadHash"]["StringValue"] = hashlib.sha256(
        conflicting["Body"].encode()
    ).hexdigest()

    outcome = consumer.handle(conflicting)

    assert outcome.disposition is RequestIntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "IDEMPOTENCY_CONFLICT"
    assert len(sqs.dead_letters) == 1


def test_older_aggregate_sequence_is_acknowledged_stale_without_effect() -> None:
    handled: list[str] = []
    store = InMemoryRequestReceiptStore()
    consumer, sqs, _ = intake(
        RequestLane.CUSTOM,
        lambda request, _: handled.append(request["requestHash"]),
        store=store,
    )
    newer = delivery(custom_request(period_start="2024-01-01"), sequence=2)
    older = delivery(
        custom_request(period_start="2023-01-01", client_key="request-41"), sequence=1
    )

    assert consumer.handle(newer).disposition is RequestIntakeDisposition.ACCEPTED
    outcome = consumer.handle(older)

    assert outcome.disposition is RequestIntakeDisposition.STALE
    assert len(handled) == 1
    assert sqs.deleted == [newer["ReceiptHandle"], older["ReceiptHandle"]]


def test_retryable_handler_failure_releases_claim_and_returns_to_same_queue() -> None:
    attempts = 0

    def handler(*_: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RequestProcessingError("DATASET_NOT_READY", retryable=True)

    store = InMemoryRequestReceiptStore()
    consumer, sqs, _ = intake(RequestLane.COMPETITION, handler, store=store)
    message = delivery(competition_request())

    first = consumer.handle(message)
    redelivery = copy.deepcopy(message)
    redelivery["ReceiptHandle"] = "receipt-redelivery"
    redelivery["Attributes"]["ApproximateReceiveCount"] = "2"
    second = consumer.handle(redelivery)

    assert first.disposition is RequestIntakeDisposition.RETURNED
    assert second.disposition is RequestIntakeDisposition.ACCEPTED
    assert sqs.returned == [message["ReceiptHandle"]]
    assert sqs.deleted == ["receipt-redelivery"]


def test_permanent_handler_failure_and_exhausted_retry_use_the_lane_dlq() -> None:
    permanent, permanent_sqs, _ = intake(
        RequestLane.CUSTOM,
        lambda *_: (_ for _ in ()).throw(RequestProcessingError("BOT_MISMATCH", retryable=False)),
    )
    permanent_outcome = permanent.handle(delivery(custom_request()))

    retrying, retry_sqs, _ = intake(
        RequestLane.COMPETITION,
        lambda *_: (_ for _ in ()).throw(RequestProcessingError("PLAN_NOT_READY", retryable=True)),
    )
    exhausted = delivery(competition_request())
    exhausted["Attributes"]["ApproximateReceiveCount"] = "3"
    retry_outcome = retrying.handle(exhausted)

    assert permanent_outcome.reason_code == "BOT_MISMATCH"
    assert permanent_sqs.dead_letters[0]["QueueUrl"].endswith("custom-dlq")
    assert retry_outcome.reason_code == "PLAN_NOT_READY"
    assert retry_sqs.dead_letters[0]["QueueUrl"].endswith("competition-dlq")
