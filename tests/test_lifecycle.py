"""Lifecycle behaviour below the HTTP layer.

`test_backtest_api.py` covers the same rules through the HTTP surface; this module
covers the parts that have no HTTP representation: the deterministic run-id derivation,
the dispatch/no-dispatch decision, the dead-letter policy's distinction between poison
and transient failure, and the outbound `backtest.v1` event.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from backtest_engine.contracts import ContractValidationError
from backtest_engine.execution_policy import (
    D17_EXECUTION_POLICY_FIXTURE,
    ExecutionPolicyCatalog,
    et_quarter_start,
)
from backtest_engine.lifecycle import (
    RUN_ID_NAMESPACE,
    BacktestLifecycleService,
    BacktestRunNotFound,
    IdempotencyConflict,
    InMemoryBacktestJobQueue,
    InMemoryDeadLetterQueue,
    InMemoryRunGateway,
    InvalidStatusTransition,
    NotRunOwner,
    RequestNotSatisfiable,
    SqsBacktestJobQueue,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
    run_id_for,
)
from backtest_engine.persistence.rows import RunLane, RunStatus


FIXTURES = Path(__file__).parent / "fixtures/contracts/strategy-bot/v1"

BOT_ID = UUID("00000000-0000-4000-8000-000000000201")
OWNER_ID = UUID("66666666-6666-4666-8666-666666666666")
OTHER_OWNER_ID = UUID("55555555-5555-4555-8555-555555555555")
MANIFEST_ID = UUID("00000000-0000-4000-8000-000000000203")

B_IDEMPOTENCY_KEY = "sha256:c6dd5229151352a530ff8312f050258107370cf26ea943c68473bf81936f6c1e"
EXPECTED_RUN_ID = UUID("00000000-0000-4000-8000-000000000214")
DERIVED_RUN_ID = UUID("f876f259-4158-5a9a-8973-db21764024dc")
SNAPSHOT_HASH = "sha256:" + "1" * 64
RESULT_HASH = "sha256:" + "a" * 64
DATASET_HASH = "d9f6310297b7eb858570086d7292a709261eecc7bf92fc9a03745c46f514161c"

POLICY_2026Q3 = replace(
    D17_EXECUTION_POLICY_FIXTURE,
    version="backtest-policy:1.0.0",
    release_quarter="2026-Q3",
    period_start=et_quarter_start(2026, 3),
    period_end=et_quarter_start(2026, 4),
)


def _load(name: str) -> dict[str, Any]:
    """Load the frozen behaviour vector used by this suite.

    Cross-repository contract parity is verified in ``test_contracts.py``.  A
    lifecycle unit test must not silently select a different request merely
    because it happens to run below a superproject checkout; doing so changes
    the policy fixture without changing this suite's policy catalog.
    """
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def official_request() -> dict[str, Any]:
    return _load("official-backtest-request.valid.json")


@pytest.fixture
def compiled_plan() -> dict[str, Any]:
    return _load("basic-compiled-plan.valid.json")


@pytest.fixture
def service(compiled_plan: dict[str, Any]) -> BacktestLifecycleService:
    return BacktestLifecycleService(
        gateway=InMemoryRunGateway(),
        queue=InMemoryBacktestJobQueue(),
        owners=StaticOwnerDirectory({BOT_ID: OWNER_ID}),
        plans=StaticCompiledPlanSource({compiled_plan["planChecksum"]: compiled_plan}),
        manifests=StaticDatasetManifestSource(
            {MANIFEST_ID: {"dataset_hash": DATASET_HASH, "schema_id": "market-bars-v2"}}
        ),
        policies=ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE, POLICY_2026Q3]),
        dead_letters=InMemoryDeadLetterQueue(),
        max_delivery_attempts=3,
    )


def _event(service: BacktestLifecycleService, status: str, **detail: Any) -> dict[str, Any]:
    run = service.get(EXPECTED_RUN_ID, owner_account_id=OWNER_ID)
    return service.result_event_for(
        run,
        status=status,
        correlation_id="00000000-0000-4000-8000-000000000202",
        message_id=str(uuid4()),
        expected_snapshot_hash=SNAPSHOT_HASH,
        execution_policy_version=POLICY_2026Q3.version,
        **detail,
    )


# ===========================================================================
# Identity
# ===========================================================================


def test_run_id_namespace_is_pinned() -> None:
    """Changing this constant would re-address every run that already exists."""
    assert str(RUN_ID_NAMESPACE) == "a8eac5b9-0335-5d8c-b32a-1d969dec25ac"


def test_run_id_is_a_pinned_function_of_bs_idempotency_key() -> None:
    assert run_id_for(B_IDEMPOTENCY_KEY) == DERIVED_RUN_ID
    # A second pinned literal: a constant-returning implementation fails here.
    assert str(run_id_for("sha256:" + "0" * 64)) == "2b97cf3a-1700-5b1a-bbab-5e02f181c281"


def test_run_id_derivation_rejects_an_empty_key() -> None:
    with pytest.raises(ValueError, match="idempotency_key"):
        run_id_for("")


# ===========================================================================
# Acceptance
# ===========================================================================


def test_accept_creates_a_queued_run_and_publishes_one_job(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    accepted = service.accept(official_request)

    assert accepted.created is True
    assert accepted.dispatched is True
    assert accepted.run.backtest_run_id == EXPECTED_RUN_ID
    assert accepted.run.status is RunStatus.QUEUED
    assert accepted.run.run.owner_account_id == OWNER_ID
    assert len(service.queue.messages) == 1  # type: ignore[attr-defined]


def test_official_acceptance_records_the_basic_lane_envelope_identity(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    accepted = service.accept(official_request)
    row = accepted.run.run

    assert row.lane is RunLane.BASIC
    assert row.message_id == UUID(official_request["metadata"]["messageId"])
    assert row.canonical_payload_hash is not None
    assert len(row.canonical_payload_hash) == 64
    assert row.aggregate_sequence == 1
    assert row.execution_policy_version == POLICY_2026Q3.version
    assert row.idempotency_scope == official_request["botId"]


def test_official_acceptance_uses_the_run_identity_registered_by_the_producer(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    registered_run_id = UUID("00000000-0000-4000-8000-000000000299")
    official_request["runId"] = str(registered_run_id)
    official_request["lane"] = "BASIC"
    official_request["aggregateSequence"] = 1
    official_request["executionPolicyVersion"] = POLICY_2026Q3.version

    accepted = service.accept(official_request)

    assert accepted.run.backtest_run_id == registered_run_id
    assert service.queue.messages[0]["backtestRunId"] == str(registered_run_id)  # type: ignore[attr-defined]


def test_the_published_job_carries_the_identity_the_worker_needs(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    accepted = service.accept(official_request)
    message = service.queue.messages[0]  # type: ignore[attr-defined]

    assert message["backtestRunId"] == str(EXPECTED_RUN_ID)
    assert message["botId"] == str(BOT_ID)
    assert message["ownerAccountId"] == str(OWNER_ID)
    assert message["idempotencyKey"] == B_IDEMPOTENCY_KEY
    assert message["inputBundleFingerprint"] == accepted.run.run.configuration_hash
    assert message["inputBundleFingerprint"].startswith("sha256:")
    assert message["featureMaterializations"] == official_request["featureMaterializations"]


def test_feature_materialization_pins_are_part_of_the_fingerprint_and_stored_bundle(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    first = service.accept(official_request)
    gateway = service.gateway
    assert isinstance(gateway, InMemoryRunGateway)

    stored = gateway.features_of(first.run.backtest_run_id)
    assert [
        {
            "featureMaterializationId": str(item.feature_materialization_id),
            "lockedResultHash": item.locked_result_hash,
        }
        for item in stored
    ] == official_request["featureMaterializations"]

    changed = json.loads(json.dumps(official_request))
    changed["featureMaterializations"][0]["lockedResultHash"] = "sha256:" + "8" * 64
    comparison_service = replace(
        service,
        gateway=InMemoryRunGateway(),
        queue=InMemoryBacktestJobQueue(),
    )

    second = comparison_service.accept(changed)

    assert second.run.run.configuration_hash != first.run.run.configuration_hash


def test_redelivery_does_not_dispatch_a_second_job(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    first = service.accept(official_request)
    second = service.accept(official_request)

    assert first.created is True
    assert second.created is False
    assert second.dispatched is False
    assert first.run.run == second.run.run
    assert len(service.queue.messages) == 1  # type: ignore[attr-defined]


def test_an_unresolvable_owner_names_the_bot_and_creates_nothing(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.owners = StaticOwnerDirectory({})

    with pytest.raises(RequestNotSatisfiable) as caught:
        service.accept(official_request)

    assert caught.value.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert caught.value.missing == (f"owner:bot={BOT_ID}",)
    assert service.queue.messages == []  # type: ignore[attr-defined]


def test_an_unpublished_execution_policy_is_not_substituted(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    """Spec: an unavailable policy must never be silently replaced by another."""
    service.policies = ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE])

    with pytest.raises(RequestNotSatisfiable) as caught:
        service.accept(official_request)

    assert any("executionPolicy" in item for item in caught.value.missing)


def test_every_unresolvable_input_is_reported_together(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.owners = StaticOwnerDirectory({})
    service.plans = StaticCompiledPlanSource({})
    service.manifests = StaticDatasetManifestSource({})

    with pytest.raises(RequestNotSatisfiable) as caught:
        service.accept(official_request)

    assert len(caught.value.missing) == 3


def test_accept_verifies_the_plan_checksum_against_the_request(
    service: BacktestLifecycleService,
    official_request: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> None:
    other = json.loads(json.dumps(compiled_plan))
    other["compilerVersion"] = "basic-compiler:9.9.9"

    with pytest.raises(ContractValidationError, match="planChecksum|compiledPlanChecksum"):
        service.accept(official_request, compiled_plan=other)


# ===========================================================================
# Owner scoping
# ===========================================================================


def test_a_foreign_owner_cannot_read_the_run(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    with pytest.raises(NotRunOwner):
        service.get(EXPECTED_RUN_ID, owner_account_id=OTHER_OWNER_ID)


def test_an_unknown_run_is_not_found(service: BacktestLifecycleService) -> None:
    with pytest.raises(BacktestRunNotFound):
        service.get(uuid4(), owner_account_id=OWNER_ID)


# ===========================================================================
# Result ingestion
# ===========================================================================


def test_the_outbound_event_uses_the_canonical_completed_token(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    event = _event(
        service,
        "COMPLETED",
        completedAt="2026-07-31T12:30:00Z",
        attempt=1,
        resultManifestId="99999999-9999-4999-8999-999999999999",
        resultHash=RESULT_HASH,
    )

    assert event["status"] == "COMPLETED"
    assert event["metadata"]["messageType"] == "BACKTEST_COMPLETED"
    assert event["metadata"]["contractVersion"] == "backtest.v1"
    assert event["precisionRulesVersion"] == "precision:1.0.0"
    assert event["inputBundleFingerprint"].startswith("sha256:")


def test_ingesting_a_running_then_completed_event_advances_the_run(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    service.ingest_result(_event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1))
    outcome = service.ingest_result(
        _event(
            service,
            "COMPLETED",
            completedAt="2026-07-31T12:30:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
        )
    )

    assert outcome.applied is True
    assert outcome.run.status is RunStatus.COMPLETED
    assert outcome.run.run.result_hash == RESULT_HASH
    assert outcome.run.run.completed_at == datetime(2026, 7, 31, 12, 30, tzinfo=timezone.utc)


def test_replaying_an_identical_event_is_applied_once(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)
    event = _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)

    first = service.ingest_result(event)
    second = service.ingest_result(event, delivery_attempt=2)

    assert first.applied is True
    assert second.applied is False
    assert first.run.run == second.run.run


def test_the_same_key_with_different_content_is_a_conflict(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)
    event = _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    service.ingest_result(event)

    forged = json.loads(json.dumps(event))
    forged["startedAt"] = "2026-07-31T18:00:00Z"

    with pytest.raises(IdempotencyConflict):
        service.ingest_result(forged)


def test_a_terminal_run_cannot_be_reopened(service: BacktestLifecycleService, official_request: dict[str, Any]) -> None:
    service.accept(official_request)
    service.ingest_result(_event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1))
    service.ingest_result(
        _event(
            service,
            "COMPLETED",
            completedAt="2026-07-31T12:30:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
        )
    )

    with pytest.raises(InvalidStatusTransition):
        service.ingest_result(_event(service, "RUNNING", startedAt="2026-08-01T09:00:00Z", attempt=2))


def test_unavailable_records_the_reason_code(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    outcome = service.ingest_result(
        _event(
            service,
            "UNAVAILABLE",
            decidedAt="2026-07-31T12:06:00Z",
            reasonCode="REQUIRED_DATA_MISSING",
            missingRequirements=["resolution:1m"],
        )
    )

    assert outcome.run.status is RunStatus.UNAVAILABLE
    assert outcome.run.run.failure_code == "REQUIRED_DATA_MISSING"


def test_cancelled_result_records_the_cancellation_instead_of_a_failure(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    outcome = service.ingest_result(
        _event(
            service,
            "CANCELLED",
            cancelledAt="2026-07-31T12:06:00Z",
            attempt=1,
            reasonCode="USER_CANCELLED",
        )
    )

    assert outcome.run.status is RunStatus.CANCELLED
    assert outcome.run.run.cancelled_at == datetime(2026, 7, 31, 12, 6, tzinfo=timezone.utc)
    assert outcome.run.run.cancellation_reason_code == "USER_CANCELLED"


# ===========================================================================
# Dead-letter policy
# ===========================================================================


def test_a_poison_event_is_dead_lettered_on_its_first_delivery(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    """Retrying a contract violation can never succeed, so it must not be retried."""
    service.accept(official_request)
    poison = _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    poison["metadata"]["idempotencyKey"] = "sha256:" + "0" * 64

    with pytest.raises(ContractValidationError):
        service.ingest_result(poison, delivery_attempt=1)

    dead = service.dead_letters.messages  # type: ignore[union-attr]
    assert len(dead) == 1
    assert dead[0].failure_kind == "CONTRACT_VIOLATION"
    assert dead[0].delivery_attempt == 1


def test_a_valid_event_is_never_dead_lettered(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    service.accept(official_request)

    service.ingest_result(
        _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1),
        delivery_attempt=5,
    )

    assert service.dead_letters.messages == ()  # type: ignore[union-attr]


def test_a_service_without_a_dead_letter_sink_still_raises(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    """Dropping the sink must not turn a rejection into a silent success."""
    service.accept(official_request)
    poison = _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    poison["metadata"]["idempotencyKey"] = "sha256:" + "0" * 64
    service.dead_letters = None

    with pytest.raises(ContractValidationError):
        service.ingest_result(poison)


def test_an_event_whose_run_id_was_edited_fails_its_own_digest(
    service: BacktestLifecycleService, official_request: dict[str, Any]
) -> None:
    """The run id is inside the idempotency material, so it cannot be re-pointed."""
    service.accept(official_request)
    event = _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    event["backtestRunId"] = "11111111-1111-4111-8111-111111111111"

    with pytest.raises(ContractValidationError, match="idempotencyKey"):
        service.ingest_result(event)


# ===========================================================================
# Queue adapter
# ===========================================================================


class _RecordingSqs:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    def send_message(self, **kwargs: Any) -> dict[str, Any]:
        self.sent.append(kwargs)
        return {"MessageId": "stub"}


def test_the_sqs_adapter_publishes_canonical_json_with_routing_attributes() -> None:
    client = _RecordingSqs()
    queue = SqsBacktestJobQueue(client, "https://sqs.example/queue")

    queue.publish({"backtestRunId": str(EXPECTED_RUN_ID), "idempotencyKey": B_IDEMPOTENCY_KEY})

    sent = client.sent[0]
    assert sent["QueueUrl"] == "https://sqs.example/queue"
    assert json.loads(sent["MessageBody"]) == {
        "backtestRunId": str(EXPECTED_RUN_ID),
        "idempotencyKey": B_IDEMPOTENCY_KEY,
    }
    assert sent["MessageAttributes"]["BacktestRunId"]["StringValue"] == str(EXPECTED_RUN_ID)
    assert sent["MessageAttributes"]["IdempotencyKey"]["StringValue"] == B_IDEMPOTENCY_KEY


def test_the_sqs_adapter_refuses_an_empty_queue_url() -> None:
    with pytest.raises(ValueError, match="queue_url"):
        SqsBacktestJobQueue(_RecordingSqs(), "")
