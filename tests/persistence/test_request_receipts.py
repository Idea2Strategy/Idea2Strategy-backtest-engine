from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Engine, text

from backtest_engine.backtest_request_intake import (
    PostgresRequestReceiptStore,
    RequestClaimDisposition,
    TransportEnvelope,
)


pytestmark = pytest.mark.docker

NOW = datetime(2026, 8, 4, 12, tzinfo=timezone.utc)
HANDLER = "backtest-custom-request-v1-test"
AGGREGATE_ID = uuid.UUID("97000000-0000-4000-8000-000000009901")
MESSAGE_NEW = uuid.UUID("97000000-0000-4000-8000-000000009902")
MESSAGE_OLD = uuid.UUID("97000000-0000-4000-8000-000000009903")


def _envelope(message_id: uuid.UUID, sequence: int, suffix: str) -> TransportEnvelope:
    return TransportEnvelope(
        event_type="CUSTOM_BACKTEST_REQUESTED",
        contract_version="backtest-request.v1",
        owner_domain="backtest-request",
        aggregate_id=AGGREGATE_ID,
        aggregate_sequence=sequence,
        message_id=message_id,
        producer_idempotency_key="sha256:" + suffix * 64,
        outbox_idempotency_key="sha256:" + suffix * 64,
        payload_hash=suffix * 64,
    )


def _insert(engine: Engine, envelope: TransportEnvelope) -> None:
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO operations.outbox_messages
                    (id, owner_domain, aggregate_id, aggregate_sequence, event_type,
                     event_schema_version, payload_document, payload_hash,
                     producer_idempotency_key, idempotency_key, created_at)
                VALUES
                    (:id, 'backtest-request', :aggregate_id, :sequence,
                     'CUSTOM_BACKTEST_REQUESTED', 'backtest-request.v1',
                     CAST(:payload AS jsonb), :payload_hash, :producer_key,
                     :delivery_key, :created_at)
                """
            ),
            {
                "id": envelope.message_id,
                "aggregate_id": envelope.aggregate_id,
                "sequence": envelope.aggregate_sequence,
                "payload": '{"fixture":true}',
                "payload_hash": envelope.payload_hash,
                "producer_key": envelope.producer_idempotency_key,
                "delivery_key": envelope.outbox_idempotency_key,
                "created_at": NOW + timedelta(seconds=envelope.aggregate_sequence),
            },
        )


def test_postgres_receipt_rejects_reversed_sequence_and_conflicting_transport(
    runtime_engine: Engine, admin_engine: Engine
) -> None:
    newer = _envelope(MESSAGE_NEW, 2, "a")
    older = _envelope(MESSAGE_OLD, 1, "b")
    _insert(admin_engine, newer)
    _insert(admin_engine, older)
    store = PostgresRequestReceiptStore(runtime_engine)
    try:
        claimed = store.claim(
            handler_id=HANDLER,
            envelope=newer,
            claimed_by="worker-1",
            now=NOW,
            claim_expires_at=NOW + timedelta(seconds=30),
        )
        assert claimed.disposition is RequestClaimDisposition.CLAIMED
        store.complete(
            handler_id=HANDLER,
            message_id=newer.message_id,
            payload_hash=newer.payload_hash,
            now=NOW + timedelta(seconds=1),
        )
        duplicate = store.claim(
            handler_id=HANDLER,
            envelope=newer,
            claimed_by="worker-2",
            now=NOW + timedelta(seconds=2),
            claim_expires_at=NOW + timedelta(seconds=32),
        )
        assert duplicate.disposition is RequestClaimDisposition.DUPLICATE

        stale = store.claim(
            handler_id=HANDLER,
            envelope=older,
            claimed_by="worker-2",
            now=NOW + timedelta(seconds=2),
            claim_expires_at=NOW + timedelta(seconds=32),
        )
        assert stale.disposition is RequestClaimDisposition.STALE

        forged = replace(newer, payload_hash="f" * 64)
        conflict = store.claim(
            handler_id=HANDLER,
            envelope=forged,
            claimed_by="worker-3",
            now=NOW + timedelta(seconds=3),
            claim_expires_at=NOW + timedelta(seconds=33),
        )
        assert conflict.disposition is RequestClaimDisposition.CONFLICT

        wrong_event = replace(newer, event_type="COMPETITION_BACKTEST_REQUESTED")
        event_conflict = store.claim(
            handler_id=HANDLER,
            envelope=wrong_event,
            claimed_by="worker-4",
            now=NOW + timedelta(seconds=4),
            claim_expires_at=NOW + timedelta(seconds=34),
        )
        assert event_conflict.disposition is RequestClaimDisposition.CONFLICT
    finally:
        with admin_engine.begin() as connection:
            connection.execute(
                text("DELETE FROM operations.outbox_consumer_receipts WHERE consumer_handler_id = :handler"),
                {"handler": HANDLER},
            )
            connection.execute(
                text("DELETE FROM operations.outbox_messages WHERE id IN (:newer, :older)"),
                {"newer": MESSAGE_NEW, "older": MESSAGE_OLD},
            )
