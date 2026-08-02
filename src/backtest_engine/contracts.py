"""Contract boundary for the backtest engine.

Three separate contracts meet here and must not be confused:

``strategy-bot.v1`` (owned by B, backend)
    ``OFFICIAL_BACKTEST_REQUESTED`` and ``basic-compiled-plan`` are **consumed
    verbatim**. D does not redefine them. Their real shape is a ``metadata``
    envelope, camelCase fields and ``sha256:``-prefixed lowercase digests. The
    authoritative fixtures live in
    ``backend/modules/backend-messaging/src/main/resources/contracts/strategy-bot/v1/``
    and the canonical checksum material is assembled by
    ``StrategyBotContractFixtures``: newline-separated ``name=value`` lines with
    plan step arguments sorted by key. This module reproduces that material and
    verifies the supplied digests rather than trusting them.

``backtest.v1`` (owned by D, this repo)
    The result event D publishes, following the same conventions, covering
    every ``backtest.run_status`` value in ``db/schema.dbml``: ``QUEUED``,
    ``RUNNING``, ``COMPLETED``, ``FAILED``, ``UNAVAILABLE``.

``market-data.v1`` ``dataset-manifest`` (owned by D, data-pipeline repo)
    Consumed. The producer publishes a fixture but no schema, so the validator
    lives here; the *fixture* does not, and the drift check reads the
    producer's single copy directly.

The ``com06.backtest-request`` and ``com06.backtest-result`` contracts that
used to live here are **deleted**. They described a flat, snake_case shape that
no producer has ever published - B publishes ``strategy-bot.v1`` and D
publishes ``backtest.v1`` - so validating against them proved nothing. Worse,
this repository also kept its own copy of the producer's fixture bundle, and
the two copies drifted until the producer's own fixture failed this
repository's validator. Both copies are gone; a contract now has exactly one
fixture, in its producer's repository.

All shape validation is delegated to versioned JSON Schema (Draft 2020-12)
under ``schemas/``. Only the checks a schema genuinely cannot express - digest
recomputation and contract-version negotiation - remain in Python.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from functools import cache, lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import best_match
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from .money import PRECISION_RULES_VERSION


__all__ = [
    "BACKTEST_CONTRACT_VERSION",
    "BACKTEST_RESULT_EVENT_TYPE",
    "BACKTEST_RESULT_MESSAGE_TYPES",
    "BACKTEST_RESULT_ORIGIN_FIELDS",
    "BACKTEST_RESULT_SOURCE",
    "INPUT_BUNDLE_FIELDS",
    "LIVE_PERFORMANCE_ELIGIBLE",
    "OFFICIAL_BACKTEST_MESSAGE_TYPE",
    "SCHEMA_ROOT",
    "SCHEMA_VERSION",
    "STRATEGY_BOT_CONTRACT_VERSION",
    "SUPPORTED_PLAN_OPERATIONS",
    "ContractValidationError",
    "UnsupportedContractVersion",
    "UnsupportedPlanElement",
    "backtest_result_operation_key",
    "build_backtest_result_event",
    "canonical_dataset_hash",
    "compiled_plan_checksum_material",
    "compute_compiled_plan_checksum",
    "compute_input_bundle_fingerprint",
    "compute_message_idempotency_key",
    "cross_check_request_against_plan",
    "input_bundle_material",
    "message_idempotency_material",
    "official_backtest_operation_key",
    "validate_backtest_result_event",
    "validate_basic_compiled_plan",
    "validate_dataset_manifest",
    "validate_official_backtest_request",
]


SCHEMA_ROOT = Path(__file__).parent / "schemas"
SCHEMA_BASE_URI = "https://contracts.idea2strategy.io/"
SCHEMA_VERSION = 1

STRATEGY_BOT_CONTRACT_VERSION = "strategy-bot.v1"
BACKTEST_CONTRACT_VERSION = "backtest.v1"

OFFICIAL_BACKTEST_REQUEST_SCHEMA = (
    f"{SCHEMA_BASE_URI}strategy-bot/v1/official-backtest-request.schema.json"
)
BASIC_COMPILED_PLAN_SCHEMA = (
    f"{SCHEMA_BASE_URI}strategy-bot/v1/basic-compiled-plan.schema.json"
)
BACKTEST_RESULT_EVENT_SCHEMA = (
    f"{SCHEMA_BASE_URI}backtest/v1/backtest-result.schema.json"
)
DATASET_MANIFEST_SCHEMA = (
    f"{SCHEMA_BASE_URI}market-data/v1/dataset-manifest.schema.json"
)

OFFICIAL_BACKTEST_MESSAGE_TYPE = "OFFICIAL_BACKTEST_REQUESTED"

#: Plan step operations this build implements. Kept in step with the `operation` enum
#: in `strategy-bot/v1/basic-compiled-plan.schema.json`; `test_contracts.py` asserts
#: the two agree, so widening one without the other is a test failure.
SUPPORTED_PLAN_OPERATIONS: frozenset[str] = frozenset(
    {"LOAD_FEATURE", "COMPARE", "EMIT_ORDER_CANDIDATE"}
)

#: Card D93. Every ``backtest.v1`` result event states its own origin, as a
#: constant the producer cannot vary and the schema pins with ``const``.
#:
#: The three values are not decoration. E owns live room ranking, winner
#: determination and official performance, and its ``room-performance.v1``
#: fixture ``live-performance-input.backtest-rejected.json`` refuses an input
#: whose ``source`` is ``BACKTEST`` and whose ``eventType`` is
#: ``BACKTEST_RESULT``, with ``expectedDecision =
#: BACKTEST_SOURCE_NOT_ALLOWED``. Carrying exactly those two values means D's
#: output is, field for field, the input E has already declared it rejects: a
#: consumer that forwarded a backtest result into live scoring would be handing
#: E the document E refuses, rather than one E cannot tell apart.
#:
#: ``livePerformanceEligible`` states the same fact positively, so a reader that
#: does not know the ``source`` vocabulary still cannot conclude "eligible" from
#: an unrecognised token.
#:
#: These are *not* addressing dimensions: they are constants, so they do not
#: enter ``metadata.idempotencyKey`` and adding them re-addressed nothing.
BACKTEST_RESULT_SOURCE = "BACKTEST"
BACKTEST_RESULT_EVENT_TYPE = "BACKTEST_RESULT"
LIVE_PERFORMANCE_ELIGIBLE = False

#: The fields above, by name. A caller may not supply any of them.
BACKTEST_RESULT_ORIGIN_FIELDS: frozenset[str] = frozenset(
    {"source", "eventType", "livePerformanceEligible"}
)

BACKTEST_RESULT_MESSAGE_TYPES = {
    "QUEUED": "BACKTEST_QUEUED",
    "RUNNING": "BACKTEST_RUNNING",
    "COMPLETED": "BACKTEST_COMPLETED",
    "FAILED": "BACKTEST_FAILED",
    "UNAVAILABLE": "BACKTEST_UNAVAILABLE",
}

#: The reproducibility boundary of one run, in canonical order.
#:
#: Two runs with the same fingerprint must produce the same result, so every
#: field that can change a result appears here and nothing else does. Note what
#: is absent: ``strategy_version_id``. Spec 2.2 identifies a run by
#: ``bot_id`` + ``owner_account_id``, and the strategy identity that actually
#: pins behaviour is ``expectedSnapshotHash``, not a mutable version row.
#:
#: This digest is what ``backtest.input_bundles.bundle_hash`` stores and what
#: the ``backtest.v1`` result event publishes as ``inputBundleFingerprint``.
INPUT_BUNDLE_FIELDS = (
    "botId",
    "ownerAccountId",
    "expectedSnapshotHash",
    "compiledPlanChecksum",
    "datasetManifestId",
    "datasetHash",
    "featureMaterializationVersion",
    "executionPolicyVersion",
    "precisionRulesVersion",
)

#: The object metadata `market-data.v1` dataset_hash covers. Producer-owned order.
DATASET_HASH_FIELDS = (
    "content_hash",
    "object_kind",
    "partition_granularity",
    "partition_start",
    "partition_end",
    "period_start",
    "period_end",
    "shard_key",
    "part_number",
    "row_count",
    "schema_version",
)


class ContractValidationError(ValueError):
    """Raised when a contract document cannot be consumed safely."""


class UnsupportedContractVersion(ContractValidationError):
    """Raised for a document whose contract version this build does not implement.

    Producers of an unsupported version are rejected outright; substituting a
    supported version would silently change the meaning of the message.
    """


class UnsupportedPlanElement(ContractValidationError):
    """Raised for a well-formed plan step naming an operation this build cannot run.

    Distinct from a generic shape error on purpose. A plan that is *malformed* is a
    contract violation by the producer; a plan that is well-formed but names an
    element this runtime does not implement is a capability gap, and the C-runtime
    parity requirement is that it surfaces as ``UNSUPPORTED_ELEMENT`` rather than as
    an anonymous validation failure. Callers that need that distinction - the Basic
    runtime's element vocabulary layer - can catch this subclass without re-parsing
    the message text.
    """

    def __init__(self, message: str, *, operations: Sequence[str]) -> None:
        super().__init__(message)
        self.operations = tuple(operations)


# ---------------------------------------------------------------------------
# Schema loading and validation
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _registry() -> Registry:
    resources: list[tuple[str, Resource]] = []
    for path in sorted(SCHEMA_ROOT.rglob("*.schema.json")):
        contents = json.loads(path.read_text(encoding="utf-8"))
        schema_id = contents.get("$id")
        if not schema_id:
            raise RuntimeError(f"JSON Schema without $id: {path}")
        resources.append(
            (
                schema_id,
                Resource.from_contents(contents, default_specification=DRAFT202012),
            )
        )
    if not resources:
        raise RuntimeError(f"no JSON Schema files found under {SCHEMA_ROOT}")
    return Registry().with_resources(resources)


@cache
def _validator(schema_id: str) -> Draft202012Validator:
    registry = _registry()
    schema = registry.contents(schema_id)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, registry=registry)


def _validate(
    document: Mapping[str, Any], schema_id: str, label: str
) -> dict[str, Any]:
    if not isinstance(document, Mapping):
        raise ContractValidationError(f"{label} must be a JSON object")
    instance = dict(document)
    error = best_match(_validator(schema_id).iter_errors(instance))
    if error is not None:
        location = ".".join(str(part) for part in error.absolute_path) or "(root)"
        raise ContractValidationError(f"{label}.{location}: {error.message}")
    return instance


def _require_contract_version(actual: object, expected: str, label: str) -> None:
    if actual != expected:
        raise UnsupportedContractVersion(
            f"{label} declares contract version {actual!r}; this build implements "
            f"only {expected!r} and will not substitute one for the other"
        )


# ---------------------------------------------------------------------------
# Canonical digests - each algorithm exists exactly once in this repository
# ---------------------------------------------------------------------------


def _sha256_prefixed(material: str) -> str:
    return "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def canonical_dataset_hash(objects: Sequence[Mapping[str, Any]]) -> str:
    """Order-independent digest of a manifest's object metadata.

    This is the only implementation in this repository. Importers must not
    reimplement it: a test that recomputes the production formula proves
    nothing about the production formula.
    """
    rows = [{key: item.get(key) for key in DATASET_HASH_FIELDS} for item in objects]
    rows.sort(key=_canonical_json)
    return hashlib.sha256(_canonical_json(rows).encode("utf-8")).hexdigest()


def input_bundle_material(bundle: Mapping[str, Any]) -> str:
    """Canonical checksum material for a run's reproducibility boundary.

    Same assembly rule B uses for its own digests: newline-separated
    ``name=value`` lines in a fixed, documented field order. A field the caller
    left out is an error rather than an empty line, because a silently absent
    input is exactly the failure this fingerprint exists to catch.
    """
    lines: list[str] = []
    for field in INPUT_BUNDLE_FIELDS:
        value = bundle.get(field)
        if not isinstance(value, str) or not value:
            raise ContractValidationError(
                f"input_bundle.{field} must be a non-empty string to fingerprint "
                "the input bundle"
            )
        lines.append(f"{field}={value}")
    return "\n".join(lines)


def compute_input_bundle_fingerprint(bundle: Mapping[str, Any]) -> str:
    """``sha256:``-prefixed digest of a run's reproducibility boundary.

    This is the only implementation in this repository. Callers must not
    reimplement it: a test that recomputes the production formula proves
    nothing about the production formula.
    """
    return _sha256_prefixed(input_bundle_material(bundle))


# ---------------------------------------------------------------------------
# strategy-bot.v1 - consumed verbatim from B
# ---------------------------------------------------------------------------


def compiled_plan_checksum_material(plan: Mapping[str, Any]) -> str:
    """Reproduce ``StrategyBotContractFixtures.CompiledPlanDraft.checksumMaterial``."""
    snapshot = plan["executionSnapshot"]
    version = snapshot["immutableStrategyVersion"]
    lines = [
        f"contractVersion={plan['contractVersion']}",
        f"schemaVersion={plan['schemaVersion']}",
        f"snapshotSchemaVersion={version['snapshotSchemaVersion']}",
        f"semanticHash={version['semanticHash']}",
        f"snapshotHash={version['snapshotHash']}",
        f"elementCatalogVersion={plan['elementCatalogVersion']}",
        f"instrumentCatalogVersion={plan['instrumentCatalogVersion']}",
        f"compilerVersion={plan['compilerVersion']}",
        f"requiredFeatureSetHash={plan['requiredFeatureSetHash']}",
        f"mode={snapshot['mode']}",
        f"initialCashAmount={snapshot['initialCashAmount']}",
        f"currency={snapshot['currency']}",
    ]
    # `instruments` is sorted, matching `RequiredFeature`'s canonical constructor:
    # B normalises the list before hashing, so document order must not change the
    # digest.
    for feature in plan.get("requiredFeatures", ()):
        instruments = ",".join(sorted(feature["instruments"]))
        lines.append(
            "requiredFeature="
            f"requirementId={feature['requirementId']}"
            f"|featureId={feature['featureId']}"
            f"|featureVersion={feature['featureVersion']}"
            f"|instruments={instruments}"
            f"|resolution={feature['resolution']}"
            f"|requiredObservations={feature['requiredObservations']}"
        )
    for partition in snapshot["partitions"]:
        lines.append(
            f"partition={partition['key']}|budgetCapBps={partition['budgetCapBps']}"
        )
        for flow in partition["flows"]:
            instruments = ",".join(flow["officialInstrumentIds"])
            lines.append(f"flow={flow['key']}|officialInstrumentIds={instruments}")
    for step in plan["steps"]:
        arguments = step["arguments"]
        rendered = "".join(f"|{key}={arguments[key]}" for key in sorted(arguments))
        lines.append(f"step={step['sequence']}|{step['operation']}{rendered}")
    return "\n".join(lines)


def compute_compiled_plan_checksum(plan: Mapping[str, Any]) -> str:
    """``sha256:``-prefixed checksum of a compiled plan's canonical material.

    The single implementation of this algorithm in this repository; the Basic runtime
    imports this symbol rather than reproducing the rule.
    """
    return _sha256_prefixed(compiled_plan_checksum_material(plan))


def _reject_unsupported_operations(document: Mapping[str, Any]) -> None:
    """Separate "cannot run this element" from "this message is malformed"."""
    if not isinstance(document, Mapping):
        return
    steps = document.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return
    unsupported = [
        step["operation"]
        for step in steps
        if isinstance(step, Mapping)
        and isinstance(step.get("operation"), str)
        and step["operation"] not in SUPPORTED_PLAN_OPERATIONS
    ]
    if unsupported:
        raise UnsupportedPlanElement(
            "basic_compiled_plan names plan operations this build does not implement: "
            f"{sorted(set(unsupported))}; supported operations are "
            f"{sorted(SUPPORTED_PLAN_OPERATIONS)}",
            operations=unsupported,
        )


def message_idempotency_material(
    *,
    contract_version: str,
    message_type: str,
    aggregate_id: str,
    snapshot_hash: str,
    operation_key: str,
) -> str:
    """Reproduce the idempotency material ``StrategyBotContractFixtures`` assembles."""
    return "\n".join(
        [
            f"contractVersion={contract_version}",
            f"messageType={message_type}",
            f"aggregateId={aggregate_id}",
            f"snapshotHash={snapshot_hash}",
            f"operationKey={operation_key}",
        ]
    )


def compute_message_idempotency_key(
    *,
    contract_version: str,
    message_type: str,
    aggregate_id: str,
    snapshot_hash: str,
    operation_key: str,
) -> str:
    return _sha256_prefixed(
        message_idempotency_material(
            contract_version=contract_version,
            message_type=message_type,
            aggregate_id=aggregate_id,
            snapshot_hash=snapshot_hash,
            operation_key=operation_key,
        )
    )


def official_backtest_operation_key(request: Mapping[str, Any]) -> str:
    return (
        f"OFFICIAL_BACKTEST|{request['datasetManifestId']}|"
        f"{request['assumptionsVersion']}"
    )


def validate_basic_compiled_plan(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate B's compiled plan and verify its ``planChecksum``.

    A step naming an operation outside :data:`SUPPORTED_PLAN_OPERATIONS` raises
    :class:`UnsupportedPlanElement` rather than a generic shape error, so a caller
    can delegate wire-shape checking here and still report ``UNSUPPORTED_ELEMENT``.
    """
    _require_contract_version(
        document.get("contractVersion") if isinstance(document, Mapping) else None,
        STRATEGY_BOT_CONTRACT_VERSION,
        "basic_compiled_plan",
    )
    _reject_unsupported_operations(document)
    plan = _validate(document, BASIC_COMPILED_PLAN_SCHEMA, "basic_compiled_plan")
    declared = plan["planChecksum"]
    computed = compute_compiled_plan_checksum(plan)
    if declared != computed:
        raise ContractValidationError(
            "basic_compiled_plan.planChecksum does not match the canonical plan "
            f"material: declared {declared}, computed {computed}"
        )
    return plan


def validate_official_backtest_request(
    document: Mapping[str, Any],
    *,
    compiled_plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Validate B's ``OFFICIAL_BACKTEST_REQUESTED`` and verify its digests.

    When ``compiled_plan`` is supplied, ``compiledPlanChecksum`` and
    ``expectedSnapshotHash`` are cross-checked against the plan rather than
    accepted as opaque strings.
    """
    metadata = document.get("metadata") if isinstance(document, Mapping) else None
    _require_contract_version(
        metadata.get("contractVersion") if isinstance(metadata, Mapping) else None,
        STRATEGY_BOT_CONTRACT_VERSION,
        "official_backtest_request",
    )
    request = _validate(
        document, OFFICIAL_BACKTEST_REQUEST_SCHEMA, "official_backtest_request"
    )

    declared_key = request["metadata"]["idempotencyKey"]
    computed_key = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type=OFFICIAL_BACKTEST_MESSAGE_TYPE,
        aggregate_id=request["botId"],
        snapshot_hash=request["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(request),
    )
    if declared_key != computed_key:
        raise ContractValidationError(
            "official_backtest_request.metadata.idempotencyKey does not match the "
            f"canonical material: declared {declared_key}, computed {computed_key}"
        )

    if compiled_plan is not None:
        cross_check_request_against_plan(request, validate_basic_compiled_plan(compiled_plan))
    return request


def cross_check_request_against_plan(
    request: Mapping[str, Any], plan: Mapping[str, Any]
) -> None:
    """Verify that a request's digests really identify ``plan``.

    Separated from :func:`validate_official_backtest_request` because the plan is
    not always in the caller's hand at validation time. On the production intake
    path the message names a ``compiledPlanChecksum`` and the plan is fetched
    afterwards, so the cross-check has to be callable at the point the plan
    becomes available -- otherwise a request whose ``expectedSnapshotHash``
    points at one strategy while its checksum resolves another would be accepted
    with an internally consistent idempotency key and run the wrong strategy
    under the right release's identity.

    ``plan`` must already have passed :func:`validate_basic_compiled_plan`; this
    function checks the *relationship* between the two documents, not the plan.
    """
    if request["compiledPlanChecksum"] != plan["planChecksum"]:
        raise ContractValidationError(
            "official_backtest_request.compiledPlanChecksum does not match the "
            f"supplied plan: {request['compiledPlanChecksum']} != "
            f"{plan['planChecksum']}"
        )
    plan_snapshot_hash = plan["executionSnapshot"]["immutableStrategyVersion"][
        "snapshotHash"
    ]
    if request["expectedSnapshotHash"] != plan_snapshot_hash:
        raise ContractValidationError(
            "official_backtest_request.expectedSnapshotHash does not match the "
            f"supplied plan: {request['expectedSnapshotHash']} != "
            f"{plan_snapshot_hash}"
        )


# ---------------------------------------------------------------------------
# backtest.v1 - published by D
# ---------------------------------------------------------------------------


def backtest_result_operation_key(status: str, detail: Mapping[str, Any]) -> str:
    """Content-bound operation key.

    A redelivery of the same outcome keys identically; a different outcome for
    the same run keys differently, so at-least-once delivery stays safe without
    hiding a genuine state change.
    """

    def field(name: str) -> Any:
        try:
            return detail[name]
        except KeyError as exc:
            raise ContractValidationError(
                f"backtest_result_event.{name} is required for status {status}"
            ) from exc

    if status == "QUEUED":
        return "BACKTEST_RESULT|QUEUED"
    if status == "RUNNING":
        return f"BACKTEST_RESULT|RUNNING|attempt={field('attempt')}"
    if status == "COMPLETED":
        return (
            f"BACKTEST_RESULT|COMPLETED|attempt={field('attempt')}"
            f"|resultHash={field('resultHash')}"
        )
    if status == "FAILED":
        return (
            f"BACKTEST_RESULT|FAILED|attempt={field('attempt')}"
            f"|failureCode={field('failureCode')}"
        )
    if status == "UNAVAILABLE":
        requirements = ",".join(sorted(field("missingRequirements")))
        return (
            f"BACKTEST_RESULT|UNAVAILABLE|reasonCode={field('reasonCode')}"
            f"|missingRequirements={requirements}"
        )
    raise ContractValidationError(
        f"backtest_result_event.status is unsupported: {status!r}"
    )


def build_backtest_result_event(
    *,
    status: str,
    backtest_run_id: str,
    bot_id: str,
    owner_account_id: str,
    expected_snapshot_hash: str,
    input_bundle_fingerprint: str,
    execution_policy_version: str,
    message_id: str,
    occurred_at: str,
    correlation_id: str,
    precision_rules_version: str = PRECISION_RULES_VERSION,
    **detail: Any,
) -> dict[str, Any]:
    """Build and validate a ``backtest.v1`` result event for one run status."""
    try:
        message_type = BACKTEST_RESULT_MESSAGE_TYPES[status]
    except KeyError as exc:
        raise ContractValidationError(
            f"backtest_result_event.status is unsupported: {status!r}"
        ) from exc

    supplied_origin = sorted(BACKTEST_RESULT_ORIGIN_FIELDS & set(detail))
    if supplied_origin:
        # Refused rather than overwritten: a caller that passed `source` believed
        # it was choosing one, and silently substituting the right answer would
        # leave that belief intact for the next caller.
        raise ContractValidationError(
            "backtest_result_event.source, .eventType and .livePerformanceEligible "
            "are fixed by the contract and must not be supplied by the caller; "
            f"got {supplied_origin}"
        )

    document: dict[str, Any] = {
        "metadata": {
            "contractVersion": BACKTEST_CONTRACT_VERSION,
            "messageType": message_type,
            "messageId": message_id,
            "occurredAt": occurred_at,
            "correlationId": correlation_id,
            "idempotencyKey": compute_message_idempotency_key(
                contract_version=BACKTEST_CONTRACT_VERSION,
                message_type=message_type,
                aggregate_id=backtest_run_id,
                snapshot_hash=expected_snapshot_hash,
                operation_key=backtest_result_operation_key(status, detail),
            ),
        },
        "backtestRunId": backtest_run_id,
        "botId": bot_id,
        "ownerAccountId": owner_account_id,
        "expectedSnapshotHash": expected_snapshot_hash,
        "inputBundleFingerprint": input_bundle_fingerprint,
        "executionPolicyVersion": execution_policy_version,
        "precisionRulesVersion": precision_rules_version,
        "status": status,
        **detail,
        # Last, so no per-status detail can shadow them even if the guard above
        # were ever weakened.
        "source": BACKTEST_RESULT_SOURCE,
        "eventType": BACKTEST_RESULT_EVENT_TYPE,
        "livePerformanceEligible": LIVE_PERFORMANCE_ELIGIBLE,
    }
    validate_backtest_result_event(document)
    return document


def validate_backtest_result_event(document: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a ``backtest.v1`` result event and verify its idempotency key."""
    metadata = document.get("metadata") if isinstance(document, Mapping) else None
    _require_contract_version(
        metadata.get("contractVersion") if isinstance(metadata, Mapping) else None,
        BACKTEST_CONTRACT_VERSION,
        "backtest_result_event",
    )
    event = _validate(document, BACKTEST_RESULT_EVENT_SCHEMA, "backtest_result_event")

    declared_key = event["metadata"]["idempotencyKey"]
    computed_key = compute_message_idempotency_key(
        contract_version=BACKTEST_CONTRACT_VERSION,
        message_type=event["metadata"]["messageType"],
        aggregate_id=event["backtestRunId"],
        snapshot_hash=event["expectedSnapshotHash"],
        operation_key=backtest_result_operation_key(event["status"], event),
    )
    if declared_key != computed_key:
        raise ContractValidationError(
            "backtest_result_event.metadata.idempotencyKey does not match the "
            f"canonical material: declared {declared_key}, computed {computed_key}"
        )
    return event


# ---------------------------------------------------------------------------
# market-data.v1 - consumed from the data-pipeline repository
# ---------------------------------------------------------------------------


def validate_dataset_manifest(document: Mapping[str, Any]) -> None:
    """Validate a producer dataset manifest and recompute its ``dataset_hash``.

    The declared hash is verified rather than trusted: a manifest whose object
    metadata was edited after the hash was computed is exactly the silent
    corruption a reproducible backtest must refuse to consume.
    """
    manifest = _validate(document, DATASET_MANIFEST_SCHEMA, "dataset_manifest")
    declared = manifest["dataset_hash"]
    computed = canonical_dataset_hash(manifest["objects"])
    if declared != computed:
        raise ContractValidationError(
            "dataset_manifest.dataset_hash does not match canonical object "
            f"metadata: declared {declared}, computed {computed}"
        )
