"""Contract tests.

Every digest asserted here is a hardcoded literal. The ``strategy-bot.v1``
literals are B's own published test vectors, copied out of
``backend/modules/backend-messaging/src/main/resources/contracts/strategy-bot/v1/``;
they were not produced by this implementation.
"""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path
from typing import Any

import pytest

from backtest_engine.contracts import (
    BACKTEST_CONTRACT_VERSION,
    BACKTEST_RESULT_MESSAGE_TYPES,
    INPUT_BUNDLE_FIELDS,
    SCHEMA_ROOT,
    STRATEGY_BOT_CONTRACT_VERSION,
    SUPPORTED_PLAN_OPERATIONS,
    ContractValidationError,
    UnsupportedContractVersion,
    UnsupportedPlanElement,
    build_backtest_result_event,
    compiled_plan_checksum_material,
    compute_compiled_plan_checksum,
    compute_input_bundle_fingerprint,
    compute_message_idempotency_key,
    input_bundle_material,
    message_idempotency_material,
    official_backtest_operation_key,
    validate_backtest_result_event,
    validate_basic_compiled_plan,
    validate_dataset_manifest,
    validate_official_backtest_request,
)
from backtest_engine.money import PRECISION_RULES_VERSION


FIXTURE_ROOT = Path(__file__).parent / "fixtures/contracts"
STRATEGY_BOT_FIXTURES = FIXTURE_ROOT / "strategy-bot/v1"

# B's published test vectors. Its README says consumers may either reproduce
# the calculation independently or treat these values as test vectors; this
# suite does both.
B_PLAN_CHECKSUM = (
    "sha256:88d61198d46dce161c2a929702a7fd1cee5c9b044c470d2590b96f3825fcacb3"
)
B_REQUEST_IDEMPOTENCY_KEY = (
    "sha256:c6dd5229151352a530ff8312f050258107370cf26ea943c68473bf81936f6c1e"
)
B_SNAPSHOT_HASH = "sha256:" + "1" * 64
B_BOT_ID = "00000000-0000-4000-8000-000000000201"
B_DATASET_MANIFEST_ID = "00000000-0000-4000-8000-000000000203"

OFFICIAL_REQUEST_REQUIRED_RUNTIME_FIELDS = (
    "runId",
    "lane",
    "aggregateSequence",
    "expectedDatasetHash",
    "periodStart",
    "periodEnd",
    "executionPolicyVersion",
    "featureMaterializations",
    "requestHash",
)

RUN_ID = "77777777-7777-4777-8777-777777777777"
OWNER_ACCOUNT_ID = "66666666-6666-4666-8666-666666666666"
INPUT_BUNDLE_FINGERPRINT = "sha256:" + "e" * 64
RESULT_HASH = "sha256:" + "a" * 64


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def strategy_bot_fixture(name: str) -> dict[str, Any]:
    """Load one of B's documents, preferring B's own copy over the vendored one.

    The producer is authoritative. The vendored copies exist so this repository can
    run without a superproject checkout; when the real thing is reachable it wins, so
    a stale vendored copy can never quietly become the thing under test.
    """
    upstream = _locate_backend_contracts()
    if upstream is not None and (upstream / name).is_file():
        return _load(upstream / name)
    return _load(STRATEGY_BOT_FIXTURES / name)


@pytest.fixture
def official_request() -> dict[str, Any]:
    return strategy_bot_fixture("official-backtest-request.valid.json")


@pytest.fixture
def compiled_plan() -> dict[str, Any]:
    return strategy_bot_fixture("basic-compiled-plan.valid.json")


def _result_event(status: str, **detail: Any) -> dict[str, Any]:
    return build_backtest_result_event(
        status=status,
        backtest_run_id=RUN_ID,
        bot_id=B_BOT_ID,
        owner_account_id=OWNER_ACCOUNT_ID,
        expected_snapshot_hash=B_SNAPSHOT_HASH,
        input_bundle_fingerprint=INPUT_BUNDLE_FINGERPRINT,
        execution_policy_version="official-backtest-policy-v1",
        message_id="90000000-0000-4000-8000-000000000001",
        occurred_at="2024-01-03T01:05:01Z",
        correlation_id="55555555-5555-4555-8555-555555555555",
        **detail,
    )


def _locate_backend_contracts() -> Path | None:
    override = os.environ.get("IDEA2STRATEGY_BACKEND_CONTRACTS")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_dir() else None
    suffix = Path(
        "backend/modules/backend-messaging/src/main/resources/contracts/"
        "strategy-bot/v1"
    )
    for ancestor in Path(__file__).resolve().parents:
        for candidate in (ancestor / suffix, ancestor / "Idea2Strategy" / suffix):
            if candidate.is_dir():
                return candidate
    return None


def _locate_pipeline_fixture() -> Path | None:
    """The data-pipeline repository's single copy of its own fixture bundle."""
    override = os.environ.get("IDEA2STRATEGY_PIPELINE_FIXTURES")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_file() else None
    suffix = Path("tests/fixtures/contracts/com06-d-fixtures.v1.json")
    for ancestor in Path(__file__).resolve().parents:
        for repo in ("data-pipeline", "d-pipeline"):
            candidate = ancestor / repo / suffix
            if candidate.is_file():
                return candidate
    return None


# ===========================================================================
# strategy-bot.v1 - consumed verbatim from B
# ===========================================================================


def test_plan_checksum_material_matches_the_backend_assembly_rule(
    compiled_plan: dict[str, Any],
) -> None:
    """Newline-separated named fields; plan step arguments sorted by key."""
    assert compiled_plan_checksum_material(compiled_plan) == "\n".join(
        [
            "contractVersion=strategy-bot.v1",
            "schemaVersion=basic-compiled-plan.v1",
            "snapshotSchemaVersion=basic-launch-snapshot.v1",
            "semanticHash=sha256:" + "2" * 64,
            "snapshotHash=sha256:" + "1" * 64,
            "elementCatalogVersion=basic-elements:2026-07-31",
            "instrumentCatalogVersion=us-supported-universe:2026-07-31",
            "compilerVersion=basic-compiler:1.0.0",
            "requiredFeatureSetHash=sha256:" + "3" * 64,
            "mode=BASIC",
            "initialCashAmount=100000.00000000",
            "currency=USD",
            "requiredFeature=requirementId=rsi-14-pt1m"
            "|featureId=00000000-0000-4000-8000-000000000401"
            "|featureVersion=1.0.0"
            "|instruments=00000000-0000-4000-8000-000000000301"
            "|resolution=PT1M"
            "|requiredObservations=14",
            "partition=partition-1|budgetCapBps=10000",
            "flow=flow-1|officialInstrumentIds=00000000-0000-4000-8000-000000000301",
            "step=1|LOAD_FEATURE|feature=RSI_14|resolution=1m",
            "step=2|COMPARE|operator=LT|threshold=30",
            "step=3|EMIT_ORDER_CANDIDATE|allocation=EQUAL|orderType=MARKET|side=BUY",
        ]
    )


def test_plan_checksum_reproduces_the_published_test_vector(
    compiled_plan: dict[str, Any],
) -> None:
    assert compute_compiled_plan_checksum(compiled_plan) == B_PLAN_CHECKSUM
    assert compiled_plan["planChecksum"] == B_PLAN_CHECKSUM
    validate_basic_compiled_plan(compiled_plan)


def test_plan_step_arguments_are_sorted_by_key_not_by_document_order(
    compiled_plan: dict[str, Any],
) -> None:
    reordered = copy.deepcopy(compiled_plan)
    reordered["steps"][2]["arguments"] = {
        "side": "BUY",
        "orderType": "MARKET",
        "allocation": "EQUAL",
    }

    assert compute_compiled_plan_checksum(reordered) == B_PLAN_CHECKSUM
    validate_basic_compiled_plan(reordered)


def test_tampered_plan_step_is_caught_by_its_own_checksum(
    compiled_plan: dict[str, Any],
) -> None:
    tampered = copy.deepcopy(compiled_plan)
    tampered["steps"][1]["arguments"]["threshold"] = "70"

    with pytest.raises(ContractValidationError, match="planChecksum"):
        validate_basic_compiled_plan(tampered)


def test_official_request_idempotency_key_reproduces_the_published_vector(
    official_request: dict[str, Any],
) -> None:
    operation_key = official_backtest_operation_key(official_request)

    assert operation_key == (
        "OFFICIAL_BACKTEST|00000000-0000-4000-8000-000000000203|accounting:1.0.0"
    )
    assert message_idempotency_material(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type="OFFICIAL_BACKTEST_REQUESTED",
        aggregate_id=B_BOT_ID,
        snapshot_hash=B_SNAPSHOT_HASH,
        operation_key=operation_key,
    ) == "\n".join(
        [
            "contractVersion=strategy-bot.v1",
            "messageType=OFFICIAL_BACKTEST_REQUESTED",
            "aggregateId=00000000-0000-4000-8000-000000000201",
            "snapshotHash=sha256:" + "1" * 64,
            "operationKey=OFFICIAL_BACKTEST|"
            "00000000-0000-4000-8000-000000000203|accounting:1.0.0",
        ]
    )
    assert (
        compute_message_idempotency_key(
            contract_version=STRATEGY_BOT_CONTRACT_VERSION,
            message_type="OFFICIAL_BACKTEST_REQUESTED",
            aggregate_id=B_BOT_ID,
            snapshot_hash=B_SNAPSHOT_HASH,
            operation_key=operation_key,
        )
        == B_REQUEST_IDEMPOTENCY_KEY
    )
    assert official_request["metadata"]["idempotencyKey"] == B_REQUEST_IDEMPOTENCY_KEY


def test_consumer_accepts_bs_official_backtest_request_verbatim(
    official_request: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> None:
    accepted = validate_official_backtest_request(
        official_request, compiled_plan=compiled_plan
    )

    assert accepted["metadata"]["messageType"] == "OFFICIAL_BACKTEST_REQUESTED"
    assert accepted["compiledPlanChecksum"] == B_PLAN_CHECKSUM
    assert accepted["expectedSnapshotHash"] == B_SNAPSHOT_HASH
    assert accepted["datasetManifestId"] == B_DATASET_MANIFEST_ID


def test_official_request_identity_covers_every_server_selected_dataset(
    official_request: dict[str, Any],
) -> None:
    second_id = "00000000-0000-4000-8000-000000000204"
    official_request["datasets"] = [
        {
            "datasetManifestId": official_request["datasetManifestId"],
            "purposeCode": "MARKET_BARS",
            "expectedDatasetHash": official_request["expectedDatasetHash"],
        },
        {
            "datasetManifestId": second_id,
            "purposeCode": "MARKET_BARS",
            "expectedDatasetHash": "sha256:" + "6" * 64,
        },
    ]
    operation_key = official_backtest_operation_key(official_request)
    official_request["metadata"]["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type="OFFICIAL_BACKTEST_REQUESTED",
        aggregate_id=B_BOT_ID,
        snapshot_hash=B_SNAPSHOT_HASH,
        operation_key=operation_key,
    )

    accepted = validate_official_backtest_request(official_request)

    assert operation_key == (
        "OFFICIAL_BACKTEST|00000000-0000-4000-8000-000000000203,"
        f"{second_id}|accounting:1.0.0"
    )
    assert len(accepted["datasets"]) == 2
    assert accepted["requestReason"] == "STRATEGY_RELEASE"


@pytest.mark.parametrize("field", OFFICIAL_REQUEST_REQUIRED_RUNTIME_FIELDS)
def test_vendored_official_request_requires_every_provider_runtime_field(
    field: str,
) -> None:
    request = _load(
        STRATEGY_BOT_FIXTURES / "official-backtest-request.valid.json"
    )
    assert field in request, f"vendored provider fixture is missing {field}"

    request.pop(field)

    with pytest.raises(ContractValidationError, match=field):
        validate_official_backtest_request(request)


def test_runnable_reproducibility_request_keeps_the_provider_runtime_shape() -> None:
    from d_reproducibility_testkit import official_backtest_request

    request = official_backtest_request()

    assert validate_official_backtest_request(request) == request
    assert set(OFFICIAL_REQUEST_REQUIRED_RUNTIME_FIELDS) <= request.keys()


def test_request_whose_dataset_manifest_was_swapped_fails_its_idempotency_key(
    official_request: dict[str, Any],
) -> None:
    swapped = copy.deepcopy(official_request)
    swapped["datasetManifestId"] = "00000000-0000-4000-8000-000000000999"

    with pytest.raises(ContractValidationError, match="idempotencyKey"):
        validate_official_backtest_request(swapped)


def test_request_is_rejected_when_it_names_a_different_compiled_plan(
    official_request: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> None:
    other_plan = copy.deepcopy(compiled_plan)
    other_plan["compilerVersion"] = "basic-compiler:2.0.0"
    other_plan["planChecksum"] = compute_compiled_plan_checksum(other_plan)

    with pytest.raises(ContractValidationError, match="compiledPlanChecksum"):
        validate_official_backtest_request(official_request, compiled_plan=other_plan)


def test_request_is_rejected_when_the_plan_pins_a_different_snapshot(
    official_request: dict[str, Any],
    compiled_plan: dict[str, Any],
) -> None:
    other_plan = copy.deepcopy(compiled_plan)
    other_plan["executionSnapshot"]["immutableStrategyVersion"]["snapshotHash"] = (
        "sha256:" + "4" * 64
    )
    other_plan["planChecksum"] = compute_compiled_plan_checksum(other_plan)
    request = copy.deepcopy(official_request)
    request["compiledPlanChecksum"] = other_plan["planChecksum"]

    with pytest.raises(ContractValidationError, match="expectedSnapshotHash"):
        validate_official_backtest_request(request, compiled_plan=other_plan)


def test_bare_hex_hashes_are_not_accepted_where_b_publishes_prefixed_ones(
    official_request: dict[str, Any],
) -> None:
    unprefixed = copy.deepcopy(official_request)
    unprefixed["expectedSnapshotHash"] = "1" * 64

    with pytest.raises(ContractValidationError, match="expectedSnapshotHash"):
        validate_official_backtest_request(unprefixed)


def test_unsupported_request_version_is_rejected_without_substitution() -> None:
    document = strategy_bot_fixture("official-backtest-request.unsupported-version.json")

    with pytest.raises(UnsupportedContractVersion, match="strategy-bot.v999"):
        validate_official_backtest_request(document)


def test_unsupported_plan_version_is_rejected_without_substitution() -> None:
    document = strategy_bot_fixture("basic-compiled-plan.unsupported-version.json")

    with pytest.raises(UnsupportedContractVersion, match="strategy-bot.v999"):
        validate_basic_compiled_plan(document)


# ===========================================================================
# backtest.v1 - published by D
# ===========================================================================


def test_published_result_event_covers_exactly_the_canonical_run_status_enum() -> None:
    assert BACKTEST_RESULT_MESSAGE_TYPES == {
        "QUEUED": "BACKTEST_QUEUED",
        "RUNNING": "BACKTEST_RUNNING",
        "COMPLETED": "BACKTEST_COMPLETED",
        "FAILED": "BACKTEST_FAILED",
        "CANCELLED": "BACKTEST_CANCELLED",
        "UNAVAILABLE": "BACKTEST_UNAVAILABLE",
    }


def test_cancelled_result_is_a_distinct_terminal_event() -> None:
    event = _result_event(
        "CANCELLED",
        cancelledAt="2024-01-03T01:07:00Z",
        attempt=2,
        reasonCode="USER_CANCELLED",
    )

    assert event["metadata"]["messageType"] == "BACKTEST_CANCELLED"
    assert event["status"] == "CANCELLED"
    assert event["reasonCode"] == "USER_CANCELLED"
    validate_backtest_result_event(event)


@pytest.mark.parametrize(
    ("status", "detail", "expected_message_type", "expected_idempotency_key"),
    [
        (
            "QUEUED",
            {"queuedAt": "2024-01-03T01:05:01Z"},
            "BACKTEST_QUEUED",
            "sha256:a6a73e0b18ddac4931f912610e872fb5f0e428ad8219756ab820a7d56047858b",
        ),
        (
            "RUNNING",
            {"startedAt": "2024-01-03T01:06:00Z", "attempt": 1},
            "BACKTEST_RUNNING",
            "sha256:a6f8b05e20eea16008410f21944ece769298a049d489b3a9d71606633a32b515",
        ),
        (
            "COMPLETED",
            {
                "completedAt": "2024-01-03T01:10:00Z",
                "attempt": 1,
                "resultManifestId": "99999999-9999-4999-8999-999999999999",
                "resultHash": RESULT_HASH,
            },
            "BACKTEST_COMPLETED",
            "sha256:b9ef3969253ee8495d2e8241d891a75927e62c1d9814530f83ed07540b2e2f78",
        ),
        (
            "FAILED",
            {
                "failedAt": "2024-01-03T01:07:00Z",
                "attempt": 2,
                "failureCode": "WORKER_TIMEOUT",
                "retryable": True,
            },
            "BACKTEST_FAILED",
            "sha256:ddae478cec5865b979364f69f4140e8725e8ca462c53989be7b12bf4e60563e3",
        ),
        (
            "CANCELLED",
            {
                "cancelledAt": "2024-01-03T01:07:00Z",
                "attempt": 2,
                "reasonCode": "USER_CANCELLED",
            },
            "BACKTEST_CANCELLED",
            "sha256:acadace5c053c96f500e56c2dd43669bbdd64394f26b75e6d8243fed8ac5c9dc",
        ),
        (
            "UNAVAILABLE",
            {
                "decidedAt": "2024-01-03T01:05:02Z",
                "reasonCode": "REQUIRED_DATA_MISSING",
                "missingRequirements": ["resolution:1m"],
            },
            "BACKTEST_UNAVAILABLE",
            "sha256:737c01254c7a4b1ae69592ded71633fd941ea13376910bef602c70bc578f60cb",
        ),
    ],
)
def test_published_result_event_pins_every_run_status(
    status: str,
    detail: dict[str, Any],
    expected_message_type: str,
    expected_idempotency_key: str,
) -> None:
    event = _result_event(status, **detail)

    assert event["metadata"]["contractVersion"] == BACKTEST_CONTRACT_VERSION
    assert event["metadata"]["messageType"] == expected_message_type
    assert event["metadata"]["idempotencyKey"] == expected_idempotency_key
    assert event["status"] == status
    assert event["precisionRulesVersion"] == PRECISION_RULES_VERSION
    validate_backtest_result_event(event)


def test_result_event_idempotency_key_is_bound_to_the_outcome() -> None:
    timeout = _result_event(
        "FAILED",
        failedAt="2024-01-03T01:07:00Z",
        attempt=2,
        failureCode="WORKER_TIMEOUT",
        retryable=True,
    )
    unavailable_input = _result_event(
        "FAILED",
        failedAt="2024-01-03T01:07:00Z",
        attempt=2,
        failureCode="INPUT_UNAVAILABLE",
        retryable=False,
    )

    assert timeout["metadata"]["idempotencyKey"] == (
        "sha256:ddae478cec5865b979364f69f4140e8725e8ca462c53989be7b12bf4e60563e3"
    )
    assert unavailable_input["metadata"]["idempotencyKey"] == (
        "sha256:e1da992b5fba30a50d0494d1a38a6bb40c716ece10cb910f3239a169ebdf86d0"
    )


def test_result_event_requires_the_result_hash_a_completed_run_produced() -> None:
    with pytest.raises(ContractValidationError, match="resultHash"):
        _result_event(
            "COMPLETED",
            completedAt="2024-01-03T01:10:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
        )


def test_result_event_rejects_the_pre_rebuild_complete_token() -> None:
    with pytest.raises(ContractValidationError, match="COMPLETE"):
        _result_event("COMPLETE", completedAt="2024-01-03T01:10:00Z")


def test_result_event_rejects_a_forged_idempotency_key() -> None:
    event = _result_event("QUEUED", queuedAt="2024-01-03T01:05:01Z")
    event["metadata"]["idempotencyKey"] = "sha256:" + "0" * 64

    with pytest.raises(ContractValidationError, match="idempotencyKey"):
        validate_backtest_result_event(event)


def test_result_event_rejects_a_message_type_that_contradicts_the_status() -> None:
    event = _result_event("QUEUED", queuedAt="2024-01-03T01:05:01Z")
    event["metadata"]["messageType"] = "BACKTEST_RUNNING"

    with pytest.raises(ContractValidationError, match="BACKTEST_QUEUED"):
        validate_backtest_result_event(event)


def test_result_event_rejects_an_unsupported_contract_version() -> None:
    event = _result_event("QUEUED", queuedAt="2024-01-03T01:05:01Z")
    event["metadata"]["contractVersion"] = "backtest.v999"

    with pytest.raises(UnsupportedContractVersion, match="backtest.v999"):
        validate_backtest_result_event(event)


def test_result_event_follows_bs_conventions() -> None:
    event = _result_event("QUEUED", queuedAt="2024-01-03T01:05:01Z")

    assert set(event["metadata"]) == {
        "contractVersion",
        "messageType",
        "messageId",
        "occurredAt",
        "correlationId",
        "idempotencyKey",
    }
    assert event["metadata"]["idempotencyKey"].startswith("sha256:")
    assert all("_" not in name for name in event)


# ===========================================================================
# Input bundle fingerprint - one implementation, pinned to literals
# ===========================================================================


PINNED_BUNDLE = {
    "botId": B_BOT_ID,
    "ownerAccountId": OWNER_ACCOUNT_ID,
    "expectedSnapshotHash": B_SNAPSHOT_HASH,
    "compiledPlanChecksum": B_PLAN_CHECKSUM,
    "datasetManifestId": B_DATASET_MANIFEST_ID,
    "datasetHash": "sha256:" + "7" * 64,
    "featureMaterializationVersion": "features:2026-07-31",
    "executionPolicyVersion": "official-backtest-policy-v1",
    "precisionRulesVersion": "precision:1.0.0",
}

# Derived by hand from the documented rule (newline-joined `name=value` in the
# documented field order, SHA-256, `sha256:` prefix), not by calling the module
# under test.
PINNED_INPUT_BUNDLE_FINGERPRINT = (
    "sha256:e7499aa4fb420847c985291edd26cf16c92cf9f57a7d83bfa70b6a1f2162a73e"
)
PINNED_FINGERPRINT_OTHER_DATASET = (
    "sha256:6c7c6e9975c66ed370854f642c758dc0e05a676ff789a815cbbc3df822f6270c"
)


def test_input_bundle_material_follows_bs_assembly_rule() -> None:
    assert input_bundle_material(PINNED_BUNDLE) == "\n".join(
        [
            "botId=00000000-0000-4000-8000-000000000201",
            "ownerAccountId=66666666-6666-4666-8666-666666666666",
            "expectedSnapshotHash=sha256:" + "1" * 64,
            "compiledPlanChecksum=" + B_PLAN_CHECKSUM,
            "datasetManifestId=00000000-0000-4000-8000-000000000203",
            "datasetHash=sha256:" + "7" * 64,
            "featureMaterializationVersion=features:2026-07-31",
            "executionPolicyVersion=official-backtest-policy-v1",
            "precisionRulesVersion=precision:1.0.0",
        ]
    )


def test_input_bundle_fingerprint_is_pinned_to_a_literal() -> None:
    assert compute_input_bundle_fingerprint(PINNED_BUNDLE) == (
        PINNED_INPUT_BUNDLE_FINGERPRINT
    )


def test_input_bundle_fingerprint_is_pinned_for_a_different_dataset() -> None:
    """A second pinned literal: a constant-returning implementation cannot pass both."""
    other = dict(PINNED_BUNDLE, datasetHash="sha256:" + "8" * 64)

    assert compute_input_bundle_fingerprint(other) == PINNED_FINGERPRINT_OTHER_DATASET


@pytest.mark.parametrize("field", sorted(PINNED_BUNDLE))
def test_every_bundle_field_changes_the_fingerprint(field: str) -> None:
    """No field in the canonical set is decorative."""
    changed = dict(PINNED_BUNDLE, **{field: PINNED_BUNDLE[field] + "-changed"})

    assert (
        compute_input_bundle_fingerprint(changed) != PINNED_INPUT_BUNDLE_FINGERPRINT
    )


def test_input_bundle_fingerprint_ignores_fields_v1_does_not_understand() -> None:
    extended = dict(PINNED_BUNDLE, futureContractField="version-2")

    assert compute_input_bundle_fingerprint(extended) == (
        PINNED_INPUT_BUNDLE_FINGERPRINT
    )


@pytest.mark.parametrize("field", sorted(PINNED_BUNDLE))
def test_a_missing_bundle_field_is_an_error_not_a_blank_line(field: str) -> None:
    incomplete = {key: value for key, value in PINNED_BUNDLE.items() if key != field}

    with pytest.raises(ContractValidationError, match=field):
        compute_input_bundle_fingerprint(incomplete)


def test_input_bundle_fingerprint_does_not_key_on_strategy_version_id() -> None:
    """Spec 2.2: a run is `bot_id` + `owner_account_id`, not `strategy_version_id`."""
    assert "strategyVersionId" not in INPUT_BUNDLE_FIELDS
    assert "strategy_version_id" not in INPUT_BUNDLE_FIELDS
    assert {"botId", "ownerAccountId"} <= set(INPUT_BUNDLE_FIELDS)

    with_a_version_id = dict(PINNED_BUNDLE, strategyVersionId="whatever")
    assert compute_input_bundle_fingerprint(with_a_version_id) == (
        PINNED_INPUT_BUNDLE_FINGERPRINT
    )


# ===========================================================================
# Schema hygiene - one executable schema per consumed or produced wire contract
# ===========================================================================


#: Every schema family this repository is allowed to carry, and why.
SCHEMA_FAMILIES_THIS_REPO_MAY_HOLD = {
    # Published by D. This repository is the producer, so the single copy lives here.
    "backtest",
    # Root-approved request contract consumed from backend #199, whose producer
    # publishes executable fixtures but no JSON Schema.
    "backtest-request",
    # Shared primitives referenced by D's own schemas.
    "common",
    # Consumed from B, which publishes fixtures but no JSON Schema.
    "strategy-bot",
    # Consumed from data-pipeline, which likewise publishes a fixture but no schema.
    "market-data",
}


def test_no_com06_schema_or_fixture_survives() -> None:
    """`com06.backtest-request`/`-result` described a format no producer publishes."""
    assert not (SCHEMA_ROOT / "com06").exists()
    assert not list(SCHEMA_ROOT.rglob("*com06*"))
    # B's official release and the root-approved Custom/Competition envelope
    # survive; D's old flat snake_case COM06 shape does not.
    assert {path.name for path in SCHEMA_ROOT.rglob("*backtest-request*.schema.json")} == {
        "backtest-request.schema.json",
        "official-backtest-request.schema.json",
    }
    assert not list(SCHEMA_ROOT.rglob("*backtest-result.schema.json")) or [
        path.parent.parent.name for path in SCHEMA_ROOT.rglob("*backtest-result.schema.json")
    ] == ["backtest"]


def test_the_drifted_producer_fixture_copy_is_gone() -> None:
    """Spec section 1: two copies of the producer's fixture had already diverged.

    The fix is not to re-sync them, it is to stop keeping a second copy. The producer's
    single copy is read directly by the drift checks that need it.
    """
    assert not list(FIXTURE_ROOT.glob("com06*"))
    assert not list(FIXTURE_ROOT.glob("*d-fixtures*"))


def test_schema_root_holds_no_family_this_repo_has_no_claim_to() -> None:
    families = {path.name for path in SCHEMA_ROOT.iterdir() if path.is_dir()}

    assert families == SCHEMA_FAMILIES_THIS_REPO_MAY_HOLD


def test_exactly_one_schema_file_exists_per_contract() -> None:
    """No contract may be described twice, which is how the two copies drifted."""
    by_id: dict[str, list[str]] = {}
    for path in sorted(SCHEMA_ROOT.rglob("*.schema.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        by_id.setdefault(document["$id"], []).append(path.as_posix())

    duplicated = {schema_id: paths for schema_id, paths in by_id.items() if len(paths) > 1}
    assert duplicated == {}
    # Nine, not eight: the compiled plan has two live shapes. Version 1 carries one
    # plan-wide steps list, version 2 one per trade container (root #202), and both are
    # on the wire because every plan published before version 2 exists in the old shape.
    # The ninth is the approved backtest-request.v1 Custom/Competition consumer schema.
    assert len(by_id) == 9


def test_every_schema_declares_draft_2020_12_and_a_matching_id() -> None:
    """Validation is versioned JSON Schema, not hand-written Python `if` statements."""
    schemas = sorted(SCHEMA_ROOT.rglob("*.schema.json"))
    assert len(schemas) == 9

    for path in schemas:
        document = json.loads(path.read_text(encoding="utf-8"))
        assert document["$schema"] == "https://json-schema.org/draft/2020-12/schema", path
        relative = path.relative_to(SCHEMA_ROOT).as_posix()
        assert document["$id"] == f"https://contracts.idea2strategy.io/{relative}", path


def test_the_consumed_manifest_schema_is_validated_against_the_producers_fixture() -> None:
    """The consumer-side validator must accept the producer's own fixture.

    This is the exact regression spec section 1 records: the producer's fixture failed
    the consumer's validator and nothing noticed. Reading the producer's single copy
    directly is what makes that impossible to repeat.
    """
    producer = _locate_pipeline_fixture()
    if producer is None:
        pytest.skip(
            "the data-pipeline checkout is not reachable; set IDEA2STRATEGY_PIPELINE_FIXTURES"
        )
    manifest = json.loads(producer.read_text(encoding="utf-8"))["dataset_manifest"]

    validate_dataset_manifest(manifest)


def test_a_tampered_producer_manifest_is_rejected() -> None:
    producer = _locate_pipeline_fixture()
    if producer is None:
        pytest.skip("the data-pipeline checkout is not reachable")
    manifest = json.loads(producer.read_text(encoding="utf-8"))["dataset_manifest"]
    manifest["objects"][0]["row_count"] = 999

    with pytest.raises(ContractValidationError, match="dataset_hash"):
        validate_dataset_manifest(manifest)


def test_an_unimplementable_plan_element_is_distinguishable_from_a_malformed_plan(
    compiled_plan: dict[str, Any],
) -> None:
    """C-runtime parity needs `UNSUPPORTED_ELEMENT`, not an anonymous shape error."""
    unsupported = copy.deepcopy(compiled_plan)
    unsupported["steps"][1]["operation"] = "CALL_EXTERNAL_MODEL"

    with pytest.raises(UnsupportedPlanElement) as caught:
        validate_basic_compiled_plan(unsupported)

    assert caught.value.operations == ("CALL_EXTERNAL_MODEL",)
    # Still a ContractValidationError, so an existing broad handler keeps working.
    assert isinstance(caught.value, ContractValidationError)


def test_a_malformed_plan_is_not_reported_as_an_unsupported_element(
    compiled_plan: dict[str, Any],
) -> None:
    malformed = copy.deepcopy(compiled_plan)
    del malformed["steps"][0]["arguments"]

    with pytest.raises(ContractValidationError) as caught:
        validate_basic_compiled_plan(malformed)

    assert not isinstance(caught.value, UnsupportedPlanElement)


def test_the_supported_operation_set_matches_the_schemas_enum() -> None:
    """Widening one without the other would let an unrunnable plan validate."""
    schema = json.loads(
        (SCHEMA_ROOT / "strategy-bot/v1/basic-compiled-plan.schema.json").read_text(
            encoding="utf-8"
        )
    )

    assert set(schema["$defs"]["planStep"]["properties"]["operation"]["enum"]) == (
        SUPPORTED_PLAN_OPERATIONS
    )
    assert SUPPORTED_PLAN_OPERATIONS == {
        "LOAD_FEATURE", "COMPARE", "PRICE_COMPARE", "PRICE_CHANGE_PERCENT",
        "VOLUME_COMPARE", "STREAK", "SMA_CROSS", "RSI_CROSS", "MACD_CROSS",
        "BOLLINGER_REVERSAL", "POSITION_RETURN", "HOLDING_PERIOD", "PEAK_RETURN",
        "DRAWDOWN_FROM_PEAK", "SCHEDULE", "EMIT_ORDER_CANDIDATE",
    }
