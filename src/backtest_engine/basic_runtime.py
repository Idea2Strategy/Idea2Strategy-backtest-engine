"""B's compiled-plan loader, its evaluation, and the replay wiring (D21/D22/D24).

What this module consumes
-------------------------
B publishes ``strategy-bot.v1`` ``basic-compiled-plan`` documents: a camelCase
object with an ``executionSnapshot`` envelope, ``sha256:``-prefixed digests and
a flat ``steps[]`` array. The authoritative fixture is
``backend/modules/backend-messaging/src/main/resources/contracts/strategy-bot/v1/basic-compiled-plan.valid.json``
and is vendored byte-for-byte under ``tests/fixtures/``. D consumes that shape
unmodified; the older flat snake_case ``com06.*`` plan shape is gone.

``planChecksum`` is verified on every load, against the single canonical
implementation in :mod:`backtest_engine.contracts`. A mismatch fails closed:
no plan object is ever returned for a document whose checksum did not verify.
When the caller also carries B's copy of the checksum (the
``compiledPlanChecksum`` field of ``OFFICIAL_BACKTEST_REQUESTED``) it is checked
against the plan's own value as well, so a request that was paired with a
different plan is rejected rather than silently executed.

Layering
--------
B's wire contract - contract version, Draft 2020-12 schema and ``planChecksum``
- is validated once, by
:func:`backtest_engine.contracts.validate_basic_compiled_plan`. This module does
not re-derive any of it and does not contain a second checksum implementation.
What it adds on top is everything the wire schema cannot express:

* the two cross-entry ``requiredFeatures`` uniqueness rules;
* the *build capability* layer - which compiler, element catalog, instrument
  universe, operation, argument and feature version this build implements;
* plan structure - contiguous sequences, one terminal step last, an operand
  before every comparison, unique flow keys and instruments;
* the three C-parity version gates.

Failure ordering
----------------
``load`` rejects in a fixed order, so the reported failure is always the
earliest true cause:

1. ``PLAN_CONTRACT_INVALID`` - not B's contract version, not B's shape, or the
   plan's own ``planChecksum`` does not verify.
2. ``PLAN_INTEGRITY_MISMATCH`` - the requester's copy of the checksum disagrees.
3. build capability - ``COMPILER_VERSION_MISMATCH``,
   ``ELEMENT_CATALOG_VERSION_UNSUPPORTED``,
   ``INSTRUMENT_CATALOG_VERSION_UNSUPPORTED``, then per-step
   ``UNSUPPORTED_ELEMENT`` / ``UNSUPPORTED_ELEMENT_ARGUMENT`` /
   ``UNSUPPORTED_FEATURE``.
4. ``PLAN_STRUCTURE_INVALID`` - the steps or the partition/flow graph do not
   form an executable plan.
5. C parity - ``PLAN_SCHEMA_VERSION_MISMATCH``, ``FEATURE_VERSION_MISMATCH``,
   ``RUNTIME_SCHEMA_VERSION_MISMATCH``, mirroring ``ExecutionPlanLoadFailure``
   in the Java trading runtime (card D92).

Look-ahead safety
-----------------
Three independent gates, none of them optional:

* :class:`~backtest_engine.event_clock.MarketEventClock` releases an event only
  once its ``available_at`` has passed, so publication lag is real.
* :meth:`~backtest_engine.elements.core.InstrumentSeries.completed_through`
  drops the bar that contains the evaluation instant.
* :class:`ExecutionGate` combines the official session calendar with the pinned
  data-availability assessment, so neither a closed market nor a coverage gap
  can produce a fill.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from fractions import Fraction
from types import MappingProxyType
from typing import Any

from backtest_engine.contracts import (
    ContractValidationError,
    UnsupportedPlanElement,
    validate_basic_compiled_plan,
)
from backtest_engine.data_availability import (
    AvailabilityAssessment,
    AvailabilityStatus,
    DataRequirement,
    SkipStage,
)
from backtest_engine.elements import (
    CONDITION_ERROR_REASON,
    INPUT_MISSING_REASON,
    ElementCatalog,
    ElementCompatibilityError,
    ElementEvaluation,
    ElementEvaluationError,
    ElementInputMissing,
    InstrumentInput,
    InstrumentSeries,
    OrderCandidate,
    PinnedFeatureSeries,
    PlanLoadFailure,
    PlanStep,
    SeriesBar,
    bar_resolution,
    element_catalog,
    emit_order_candidate,
    feature_definition,
    resolution_period,
    supported_feature_versions,
)
from backtest_engine.event_clock import (
    MarketDataEvent,
    MarketEventClock,
    MarketSessionStatus,
    OfficialSessionSchedule,
)
from backtest_engine.money import quantize_money


__all__ = [
    "BAR_CLOSED_EVENT_TYPE",
    "COMPILER_VERSION",
    "CONTRACT_VERSION",
    "INSTRUMENT_CATALOG_VERSIONS",
    "MULTI_CONTAINER_PLAN_SCHEMA_VERSION",
    "PLAN_SCHEMA_VERSION",
    "RUNTIME_SCHEMA_VERSION",
    "SNAPSHOT_SCHEMA_VERSION",
    "BasicCompiledPlan",
    "BasicDecisionStatus",
    "BasicExecutionResult",
    "BasicInstrumentDecision",
    "BasicPlanCompatibilityError",
    "BasicPlanFlow",
    "BasicPlanReplay",
    "BasicPlanRuntime",
    "BasicRuntimeCompatibility",
    "BasicStepTrace",
    "ExecutionGate",
    "PlanEvaluation",
    "PlanLoadFailure",
    "ReplaySkipReason",
    "ReplayUnavailableError",
    "RequiredFeature",
    "bar_closed_event",
    "derive_data_requirements",
]


CONTRACT_VERSION = "strategy-bot.v1"
PLAN_SCHEMA_VERSION = "basic-compiled-plan.v1"

MULTI_CONTAINER_PLAN_SCHEMA_VERSION = "basic-compiled-plan.v2"
"""The plan shape a strategy with more than one trade container arrives on.

A Basic strategy is one container per side, and the blocks inside a container are an
AND chain. Version 1 carried a single plan-wide ``steps`` list and a single side, so a
strategy with a buy container and a sell container had no shape to be published in and
was refused at release (root #202). Version 2 moves ``side``, ``allocation`` and
``steps`` onto each flow. Version 1 is still read exactly as before, because every plan
published before this exists in that shape.
"""

SNAPSHOT_SCHEMA_VERSION = "basic-launch-snapshot.v1"
COMPILER_VERSION = "basic-compiler:1.0.0"

RUNTIME_SCHEMA_VERSION = "strategy-bot-runtime.v1"
"""The runtime-state schema version this build implements.

A resumed runtime state snapshot declares the version it was written with; this
build refuses any other version rather than guessing how the fields it does not
recognise should be interpreted.
"""

INSTRUMENT_CATALOG_VERSIONS: tuple[str, ...] = ("us-supported-universe:2026-07-31",)
"""Instrument universes this build can resolve.

The universe fixes which official instrument ids exist and what they refer to.
An unlisted universe is refused at load time: substituting the nearest known one
would silently re-point every ``officialInstrumentId`` in the plan.
"""

BAR_CLOSED_EVENT_TYPE = "BAR_CLOSED"

_EVALUATION_ID_NAMESPACE = uuid.UUID("6f5f4d8c-9a5b-4a3e-9b2f-1d0c8e7a6b54")
"""Fixed namespace, so an evaluation id is a pure function of plan and instant."""

_MISSING_INPUT_STEP_ID = "$input"


class BasicDecisionStatus(str, Enum):
    """``BasicStrategyExecutor`` decision statuses, reproduced one-for-one."""

    CANDIDATE = "CANDIDATE"
    CONDITION_NOT_MET = "CONDITION_NOT_MET"
    CONDITION_ERROR = "CONDITION_ERROR"
    INPUT_MISSING = "INPUT_MISSING"


class ReplaySkipReason(str, Enum):
    """Why a replay instant produced no decision at all."""

    DATA_GAP_EVALUATION_SKIPPED = "DATA_GAP_EVALUATION_SKIPPED"


class BasicPlanCompatibilityError(ValueError):
    """A compiled plan this build cannot execute with B's intended meaning."""

    def __init__(self, failure: PlanLoadFailure, detail: str) -> None:
        super().__init__(f"{failure.value}: {detail}")
        self.failure = failure
        self.detail = detail


class ReplayUnavailableError(RuntimeError):
    """The pinned dataset cannot support the replay at all.

    ``contract_fields`` is the payload a ``BACKTEST_UNAVAILABLE`` result event
    carries, so the reason reaches B unchanged.
    """

    def __init__(self, contract_fields: Mapping[str, Any]) -> None:
        fields = dict(contract_fields)
        super().__init__(f"replay cannot start: {fields.get('reason_code', 'UNKNOWN')}")
        self.contract_fields = fields


def _reject(failure: PlanLoadFailure, detail: str) -> BasicPlanCompatibilityError:
    return BasicPlanCompatibilityError(failure, detail)


def _invalid(detail: str) -> BasicPlanCompatibilityError:
    return _reject(PlanLoadFailure.PLAN_CONTRACT_INVALID, detail)


def _structure(detail: str) -> BasicPlanCompatibilityError:
    return _reject(PlanLoadFailure.PLAN_STRUCTURE_INVALID, detail)


# ---------------------------------------------------------------------------
# The loaded plan
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RequiredFeature:
    """One entry of B's ``requiredFeatures``, plus this build's derivations.

    The first six fields are B's document, verbatim and unrenamed. The rest are
    derived here, and each derivation is named so it can be audited:

    ``feature_key``
        ``featureId`` (a canonical UUID) resolved through the element catalog's
        ``canonical_feature_ids`` table. B addresses a feature by UUID in
        ``requiredFeatures`` and by name in a ``LOAD_FEATURE`` argument; this is
        the pinned join, never an inference from the name.
    ``definition_version``
        This build's ``rsi:1.0.0``. B's ``featureVersion`` carries only the
        ``1.0.0`` half; the loader checks that half matches and refuses the plan
        otherwise, so the two runtimes cannot disagree about the arithmetic.
    ``bar_resolution``
        ``PT1M`` -> ``1m`` through :data:`ISO8601_RESOLUTIONS`, a lookup table,
        not a duration parser.
    ``data_kind``
        ``ADJUSTED_BAR``. **B's document does not carry this.** It comes from
        this build's feature definition, which declares that RSI is defined on
        corporate-action-adjusted bars and that raw bars are not substitutable.

    Warm-up: ``required_observations`` vs ``definition_bars``
    ---------------------------------------------------------
    They are different numbers and neither is wrong.

    B declares ``requiredObservations = 14``. In the Java trading runtime
    (``StartupWarmupCoordinator.selectRequiredObservations``) that value is a
    **floor**: warm-up fails when ``observations.size() < requiredObservations``
    and the last ``requiredObservations`` are then taken. It is the minimum
    history B guarantees a warm-up source will be asked for.

    This build's ``rsi:1.0.0`` independently needs 15 completed bars: 14 price
    *changes* require 15 closes (see :mod:`backtest_engine.elements.features`).
    ``definition_bars`` is that number.

    :attr:`warmup_bars` is therefore ``max`` of the two, not either one alone.
    Taking B's 14 would leave the feature one bar short of computable; taking
    only the definition's 15 would silently ignore a future plan that asks for
    *more* history than the definition consumes. For B's published plan the two
    rules give 14 and 15, so the answer is 15 - the off-by-one is real and is
    resolved in favour of the arithmetic that must actually run.
    """

    # -- B's document, verbatim ------------------------------------------
    requirement_id: str
    feature_id: str
    feature_version: str
    instruments: tuple[str, ...]
    resolution: str
    required_observations: int
    # -- derived by this build -------------------------------------------
    feature_key: str
    definition_version: str
    bar_resolution: str
    data_kind: str
    definition_bars: int

    @property
    def warmup_bars(self) -> int:
        """Completed bars this build must hold before the first evaluation."""
        return max(self.required_observations, self.definition_bars)

    @property
    def warmup_span(self) -> timedelta:
        """Wall-clock history the feature needs before it has any value."""
        return resolution_period(self.bar_resolution) * self.warmup_bars

    @property
    def series_key(self) -> tuple[str, str]:
        return self.data_kind, self.bar_resolution


@dataclass(frozen=True, slots=True)
class BasicPlanFlow:
    """One ``executionSnapshot.partitions[].flows[]`` entry, flattened."""

    partition_key: str
    flow_id: str
    budget_cap_bps: int
    side: str
    instrument_ids: tuple[str, ...]
    #: This container's own AND chain. A version 1 plan has one chain for the whole
    #: plan and every flow carries the same tuple; a version 2 plan gives each
    #: container its own, which is what lets a buy container and a sell container
    #: coexist (root #202).
    condition_steps: tuple[PlanStep, ...] = ()
    terminal_step: PlanStep | None = None
    allocation: str = ""


@dataclass(frozen=True, slots=True)
class BasicCompiledPlan:
    """A verified, executable view of B's compiled plan."""

    contract_version: str
    schema_version: str
    compiler_version: str
    element_catalog_version: str
    instrument_catalog_version: str
    required_feature_set_hash: str
    plan_checksum: str
    snapshot_schema_version: str
    snapshot_hash: str
    semantic_hash: str
    mode: str
    currency: str
    initial_cash: Decimal
    condition_steps: tuple[PlanStep, ...]
    terminal_step: PlanStep
    side: str
    order_type: str
    allocation: str
    required_features: tuple[RequiredFeature, ...]
    reference_series: tuple[str, str]
    flows: tuple[BasicPlanFlow, ...]
    catalog: ElementCatalog

    @property
    def instrument_ids(self) -> tuple[str, ...]:
        """Every official instrument the plan touches, in UUID order."""
        return tuple(
            sorted(
                {
                    instrument_id
                    for flow in self.flows
                    for instrument_id in flow.instrument_ids
                },
                key=uuid.UUID,
            )
        )

    def flow(self, flow_id: str) -> BasicPlanFlow:
        for candidate in self.flows:
            if candidate.flow_id == flow_id:
                return candidate
        raise KeyError(flow_id)

    def evaluation_id(self, occurred_at: datetime) -> str:
        """Deterministic id for this plan's evaluation at ``occurred_at``."""
        instant = occurred_at.astimezone(timezone.utc).isoformat()
        return str(
            uuid.uuid5(_EVALUATION_ID_NAMESPACE, f"{self.plan_checksum}|{instant}")
        )


@dataclass(frozen=True, slots=True)
class BasicRuntimeCompatibility:
    """What this build implements, as C declares the same three facts."""

    #: Every compiled-plan shape this build reads. Plural because two are live: a
    #: single-container strategy still arrives on version 1, and a strategy with a buy
    #: container and a sell container arrives on version 2 (root #202). A build that
    #: read only the newest would refuse every bot released before it.
    plan_schema_versions: tuple[str, ...]
    runtime_schema_version: str
    supported_feature_versions: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "supported_feature_versions",
            MappingProxyType(dict(self.supported_feature_versions)),
        )

    @classmethod
    def implemented(cls) -> BasicRuntimeCompatibility:
        """The real capability of this build, read from the element registry."""
        return cls(
            plan_schema_versions=(
                PLAN_SCHEMA_VERSION,
                MULTI_CONTAINER_PLAN_SCHEMA_VERSION,
            ),
            runtime_schema_version=RUNTIME_SCHEMA_VERSION,
            supported_feature_versions=supported_feature_versions(),
        )


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BasicStepTrace:
    """One step's recorded outcome: the evidence of *why* a decision happened."""

    step_id: str
    passed: bool
    reason_code: str
    evidence: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(self, "evidence", MappingProxyType(dict(self.evidence)))


@dataclass(frozen=True, slots=True)
class BasicInstrumentDecision:
    """What the plan decided for one instrument at one instant."""

    flow_id: str
    instrument_id: str
    side: str
    status: BasicDecisionStatus
    trace: tuple[BasicStepTrace, ...]
    first_failure_step_id: str | None = None
    first_failure_reason: str | None = None
    buy_allocation: Fraction | None = None
    reference_price: Decimal | None = None


@dataclass(frozen=True, slots=True)
class BasicExecutionResult:
    """Every decision of one plan evaluation, in flow then UUID order.

    ``evaluations`` keeps the per-instrument scratchpad each decision was made
    from, keyed by ``instrument_id``. It is how a caller reads the raw feature
    values behind a decision (``result.evaluations[id].values``) without
    re-running the plan - the persistence layer needs them to write evaluation
    detail rows. An instrument whose input was missing entirely never got a
    scratchpad and is absent from the mapping.
    """

    decisions: tuple[BasicInstrumentDecision, ...]
    as_of: datetime
    evaluations: Mapping[str, ElementEvaluation] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evaluations", MappingProxyType(dict(self.evaluations))
        )


# ---------------------------------------------------------------------------
# Semantic rules the wire schema cannot express
# ---------------------------------------------------------------------------


def _require_unique_required_features(document: Mapping[str, Any]) -> None:
    """Enforce B's two ``requiredFeatures`` uniqueness rules.

    The JSON Schema in :mod:`backtest_engine.contracts` validates every field of
    every entry, but neither rule below is expressible in JSON Schema: both are
    cross-entry. B's Java record enforces both, and B's README tells consumers to
    "reject duplicates or ambiguous values rather than infer from a feature name
    or default", so they are enforced here rather than trusted.
    """
    entries = document["requiredFeatures"]
    requirement_ids: set[str] = set()
    identities: set[tuple[str, str, str, tuple[str, ...]]] = set()

    for index, entry in enumerate(entries):
        requirement_id = entry["requirementId"]
        if requirement_id in requirement_ids:
            raise _invalid(
                f"plan.requiredFeatures requirementId {requirement_id!r} is not unique"
            )
        requirement_ids.add(requirement_id)

        identity = (
            entry["featureId"],
            entry["featureVersion"],
            entry["resolution"],
            tuple(sorted(entry["instruments"])),
        )
        if identity in identities:
            raise _invalid(
                f"plan.requiredFeatures[{index}] duplicates an earlier required "
                "feature: the same featureId, featureVersion, resolution and "
                "instruments appear twice"
            )
        identities.add(identity)


def _plan_steps(document: Mapping[str, Any]) -> tuple[PlanStep, ...]:
    return tuple(
        PlanStep(
            sequence=step["sequence"],
            operation=step["operation"],
            arguments=dict(step["arguments"]),
        )
        for step in document["steps"]
    )


# ---------------------------------------------------------------------------
# The runtime
# ---------------------------------------------------------------------------


class BasicPlanRuntime:
    """Loads B's compiled plans and evaluates them with C-compatible rules."""

    def __init__(self, compatibility: BasicRuntimeCompatibility | None = None) -> None:
        self.compatibility = compatibility or BasicRuntimeCompatibility.implemented()
        self.on_step: Callable[[str, PlanStep], None] | None = None
        """Optional observer, called as ``on_step(instrument_id, step)``.

        It runs inside the step's error boundary, so an observer that raises is
        reported exactly like an evaluator that raises: instrumentation can
        never turn a failed evaluation into a silent success.
        """

    # -- loading ---------------------------------------------------------

    def load(
        self,
        document: Mapping[str, Any],
        *,
        compiled_plan_checksum: str | None = None,
        runtime_schema_version: str | None = None,
    ) -> BasicCompiledPlan:
        """Verify and load one ``basic-compiled-plan.v1`` document.

        ``compiled_plan_checksum`` is the requester's copy of the checksum (the
        ``compiledPlanChecksum`` field of ``OFFICIAL_BACKTEST_REQUESTED``).
        ``runtime_schema_version`` is the version declared by a runtime state
        snapshot being resumed. Both default to "not supplied", never to
        "assume it matches".
        """
        root = self._validate_contract(document)
        _require_unique_required_features(root)
        self._verify_request_checksum(root, compiled_plan_checksum)

        catalog = self._build_capability(root)
        snapshot = root["executionSnapshot"]
        per_container = root["schemaVersion"] == MULTI_CONTAINER_PLAN_SCHEMA_VERSION

        # A version 2 plan states one chain per container, so every container is checked
        # in its own right: the operand rule, the single terminal and the contiguous
        # sequence are properties of a chain, not of the document. The first container
        # also fills the plan-level fields, which stay for the fields that genuinely
        # describe the whole plan (the reference series a warm-up is built from).
        containers = (
            [flow["steps"] for partition in snapshot["partitions"] for flow in partition["flows"]]
            if per_container
            else [root["steps"]]
        )
        chains: list[tuple[tuple[PlanStep, ...], PlanStep]] = []
        for raw_steps in containers:
            steps = _plan_steps({"steps": raw_steps})
            for step in steps:
                try:
                    catalog.validate_step(step)
                except ElementCompatibilityError as failure:
                    raise _reject(failure.failure, failure.detail) from failure
            chains.append(self._require_structure(steps, catalog))

        condition_steps, terminal_step = chains[0]
        required_features = self._required_features(root, catalog)
        reference_series = self._require_declared_features(
            tuple(step for chain, _ in chains for step in chain),
            required_features,
            catalog,
        )
        self._require_compatibility(root, required_features, runtime_schema_version)

        version = snapshot["immutableStrategyVersion"]
        side = terminal_step.argument("side")
        return BasicCompiledPlan(
            contract_version=root["contractVersion"],
            schema_version=root["schemaVersion"],
            compiler_version=root["compilerVersion"],
            element_catalog_version=root["elementCatalogVersion"],
            instrument_catalog_version=root["instrumentCatalogVersion"],
            required_feature_set_hash=root["requiredFeatureSetHash"],
            plan_checksum=root["planChecksum"],
            snapshot_schema_version=version["snapshotSchemaVersion"],
            snapshot_hash=version["snapshotHash"],
            semantic_hash=version["semanticHash"],
            mode=snapshot["mode"],
            currency=snapshot["currency"],
            initial_cash=quantize_money(
                Decimal(snapshot["initialCashAmount"]), "initialCashAmount"
            ),
            condition_steps=condition_steps,
            terminal_step=terminal_step,
            side=side,
            order_type=terminal_step.argument("orderType"),
            allocation=terminal_step.argument("allocation"),
            required_features=required_features,
            reference_series=reference_series,
            flows=self._load_flows(snapshot, chains, per_container),
            catalog=catalog,
        )

    @staticmethod
    def _validate_contract(document: Mapping[str, Any]) -> dict[str, Any]:
        """Delegate B's wire contract to the versioned JSON-Schema validator.

        :func:`~backtest_engine.contracts.validate_basic_compiled_plan` checks the
        contract version, the Draft 2020-12 schema and the ``planChecksum``, in
        that order. Only the operation-vocabulary failure is re-labelled: a plan
        naming an element this build cannot run must surface as
        ``UNSUPPORTED_ELEMENT`` for parity with the Java runtime's
        ``ExecutionPlanLoadFailure``, not as an anonymous shape error.
        """
        try:
            return validate_basic_compiled_plan(document)
        except UnsupportedPlanElement as failure:
            raise _reject(PlanLoadFailure.UNSUPPORTED_ELEMENT, str(failure)) from failure
        except ContractValidationError as failure:
            raise _invalid(str(failure)) from failure

    @staticmethod
    def _verify_request_checksum(
        document: Mapping[str, Any], compiled_plan_checksum: str | None
    ) -> None:
        """Cross-check the requester's copy of the checksum against the plan's.

        The plan's own ``planChecksum`` was already verified by the contract
        layer; this catches a request that was paired with a *different* plan.
        """
        declared = document["planChecksum"]
        if compiled_plan_checksum is not None and compiled_plan_checksum != declared:
            raise _reject(
                PlanLoadFailure.PLAN_INTEGRITY_MISMATCH,
                "the request's compiledPlanChecksum does not identify this plan: "
                f"request {compiled_plan_checksum}, plan {declared}",
            )

    @staticmethod
    def _build_capability(document: Mapping[str, Any]) -> ElementCatalog:
        compiler_version = document["compilerVersion"]
        if compiler_version != COMPILER_VERSION:
            raise _reject(
                PlanLoadFailure.COMPILER_VERSION_MISMATCH,
                f"the plan was compiled by {compiler_version}; this build consumes "
                f"only {COMPILER_VERSION}",
            )
        try:
            catalog = element_catalog(document["elementCatalogVersion"])
        except ElementCompatibilityError as failure:
            raise _reject(failure.failure, failure.detail) from failure

        instrument_catalog_version = document["instrumentCatalogVersion"]
        if instrument_catalog_version not in INSTRUMENT_CATALOG_VERSIONS:
            raise _reject(
                PlanLoadFailure.INSTRUMENT_CATALOG_VERSION_UNSUPPORTED,
                f"instrumentCatalogVersion {instrument_catalog_version!r} is not "
                "implemented by this build; implemented: "
                + ", ".join(INSTRUMENT_CATALOG_VERSIONS),
            )
        return catalog

    @staticmethod
    def _require_structure(
        steps: tuple[PlanStep, ...], catalog: ElementCatalog
    ) -> tuple[tuple[PlanStep, ...], PlanStep]:
        for position, step in enumerate(steps):
            if step.sequence != position + 1:
                raise _structure(
                    "plan steps must carry contiguous sequence numbers starting at "
                    f"1: position {position} declares sequence {step.sequence}"
                )

        terminals = [step for step in steps if catalog.spec(step.operation).terminal]
        if len(terminals) != 1 or terminals[0] is not steps[-1]:
            raise _structure(
                "a compiled plan must end with EMIT_ORDER_CANDIDATE and contain "
                f"exactly one; got {len(terminals)} in "
                + " -> ".join(step.operation for step in steps)
            )

        condition_steps = steps[:-1]
        if not condition_steps:
            raise _structure(
                "a compiled plan must contain at least one condition step before "
                "EMIT_ORDER_CANDIDATE: an unconditional plan would emit an order "
                "for every instrument at every instant"
            )

        produced = False
        for step in condition_steps:
            spec = catalog.spec(step.operation)
            if spec.consumes_value and not produced:
                raise _structure(
                    f"{step.operation} at sequence {step.sequence} has no operand: "
                    "no preceding step produced a value"
                )
            produced = produced or spec.produces_value
        return condition_steps, steps[-1]

    @staticmethod
    def _required_features(
        document: Mapping[str, Any], catalog: ElementCatalog
    ) -> tuple[RequiredFeature, ...]:
        """Resolve B's ``requiredFeatures`` block against this build's registry.

        B's block is the authority on *what history the plan needs*. It is read
        verbatim and never reconstructed from the ``LOAD_FEATURE`` steps.
        """
        features: list[RequiredFeature] = []
        for entry in document["requiredFeatures"]:
            try:
                feature_key = catalog.require_canonical_feature(entry["featureId"])
            except ElementCompatibilityError as failure:
                raise _reject(failure.failure, failure.detail) from failure
            definition = feature_definition(feature_key)
            if definition is None:  # pragma: no cover - the catalog table pins these
                raise _reject(
                    PlanLoadFailure.UNSUPPORTED_FEATURE,
                    f"feature {feature_key!r} is in element catalog "
                    f"{catalog.version} but is not implemented by this build",
                )
            if entry["featureVersion"] != definition.semantic_version:
                raise _reject(
                    PlanLoadFailure.FEATURE_VERSION_MISMATCH,
                    f"requiredFeature {entry['requirementId']!r} pins "
                    f"{feature_key} version {entry['featureVersion']}; this build "
                    f"implements {definition.semantic_version} "
                    f"({definition.definition_version})",
                )
            try:
                token = bar_resolution(entry["resolution"])
            except ElementCompatibilityError as failure:
                raise _reject(failure.failure, failure.detail) from failure

            features.append(
                RequiredFeature(
                    requirement_id=entry["requirementId"],
                    feature_id=entry["featureId"],
                    feature_version=entry["featureVersion"],
                    instruments=tuple(entry["instruments"]),
                    resolution=entry["resolution"],
                    required_observations=entry["requiredObservations"],
                    feature_key=feature_key,
                    definition_version=definition.definition_version,
                    bar_resolution=token,
                    data_kind=definition.data_kind,
                    definition_bars=definition.required_bars,
                )
            )
        return tuple(features)

    @staticmethod
    def _require_declared_features(
        condition_steps: tuple[PlanStep, ...],
        features: tuple[RequiredFeature, ...],
        catalog: ElementCatalog,
    ) -> tuple[str, str]:
        """Every ``LOAD_FEATURE`` step must be covered by a declared requirement.

        Without this a plan could read a feature whose history B never declared,
        so the data layer would be asked for the wrong warm-up window and the
        step would fail at replay time instead of at load time. Returns the
        ``(data_kind, resolution)`` of the first ``LOAD_FEATURE`` step, which is
        the series an order candidate is priced against.
        """
        reference: tuple[str, str] | None = None
        for step in condition_steps:
            spec = catalog.spec(step.operation)
            for name in spec.feature_arguments:
                feature_key = step.arguments[name]
                token = step.argument("resolution")
                match = next(
                    (
                        item
                        for item in features
                        if item.feature_key == feature_key
                        and item.bar_resolution == token
                    ),
                    None,
                )
                if match is None:
                    raise _structure(
                        f"{step.operation} at sequence {step.sequence} reads "
                        f"{feature_key} at {token}, which plan.requiredFeatures "
                        "does not declare"
                    )
                if reference is None:
                    reference = match.series_key
        if reference is None:
            raise _structure(
                "a compiled plan must read at least one feature: a plan with no "
                "LOAD_FEATURE step has no market input to decide on"
            )
        return reference

    def _require_compatibility(
        self,
        document: Mapping[str, Any],
        required_features: tuple[RequiredFeature, ...],
        runtime_schema_version: str | None,
    ) -> None:
        plan_schema_version = document["schemaVersion"]
        readable_plan_schemas = self.compatibility.plan_schema_versions
        if plan_schema_version not in readable_plan_schemas:
            raise _reject(
                PlanLoadFailure.PLAN_SCHEMA_VERSION_MISMATCH,
                "the compiled plan schema version is not one this runtime "
                f"implements: {plan_schema_version} not in "
                f"{list(readable_plan_schemas)}",
            )

        supported = self.compatibility.supported_feature_versions
        for feature in required_features:
            if supported.get(feature.feature_key) != feature.definition_version:
                implemented = (
                    ", ".join(
                        f"{name}@{version}"
                        for name, version in sorted(supported.items())
                    )
                    or "none"
                )
                raise _reject(
                    PlanLoadFailure.FEATURE_VERSION_MISMATCH,
                    f"the plan requires {feature.feature_key}@"
                    f"{feature.definition_version}; this runtime implements "
                    f"{implemented}",
                )

        declared_runtime = (
            RUNTIME_SCHEMA_VERSION
            if runtime_schema_version is None
            else runtime_schema_version
        )
        expected_runtime = self.compatibility.runtime_schema_version
        if declared_runtime != expected_runtime:
            raise _reject(
                PlanLoadFailure.RUNTIME_SCHEMA_VERSION_MISMATCH,
                "the runtime state schema version is not the one this runtime "
                f"implements: {declared_runtime} != {expected_runtime}",
            )

    @staticmethod
    def _load_flows(
        snapshot: Mapping[str, Any],
        chains: Sequence[tuple[tuple[PlanStep, ...], PlanStep]],
        per_container: bool,
    ) -> tuple[BasicPlanFlow, ...]:
        """Flatten the snapshot's flows, giving each the chain that belongs to it.

        Under version 2 the chains arrive in the same order the flows are walked, so the
        n-th chain is the n-th container's. Under version 1 there is one chain and every
        flow shares it, which is exactly what a single-container strategy means.
        """
        flows: list[BasicPlanFlow] = []
        seen_flow_ids: set[str] = set()
        position = 0
        for partition in snapshot["partitions"]:
            partition_key = partition["key"]
            for flow in partition["flows"]:
                flow_id = flow["key"]
                if flow_id in seen_flow_ids:
                    raise _structure(
                        f"flow key {flow_id!r} appears more than once: a flow key "
                        "identifies one flow across the whole plan"
                    )
                seen_flow_ids.add(flow_id)
                instrument_ids = tuple(
                    str(uuid.UUID(value)) for value in flow["officialInstrumentIds"]
                )
                if len(set(instrument_ids)) != len(instrument_ids):
                    raise _structure(
                        f"flow {flow_id!r} officialInstrumentIds must be unique: a "
                        "repeated instrument would be allocated twice"
                    )
                condition_steps, terminal_step = chains[position if per_container else 0]
                position += 1
                flows.append(
                    BasicPlanFlow(
                        partition_key=partition_key,
                        flow_id=flow_id,
                        budget_cap_bps=partition["budgetCapBps"],
                        side=terminal_step.argument("side"),
                        instrument_ids=instrument_ids,
                        condition_steps=condition_steps,
                        terminal_step=terminal_step,
                        allocation=terminal_step.argument("allocation"),
                    )
                )
        return tuple(flows)

    # -- execution -------------------------------------------------------

    def evaluation_for(
        self,
        instrument_id: str,
        instrument_input: InstrumentInput,
        as_of: datetime,
    ) -> ElementEvaluation:
        """A fresh per-instrument scratchpad, one per instrument per evaluation.

        It is an instance method so a host can supply a richer evaluation
        without reimplementing the loop, and so nothing can be shared between
        two instruments by accident.
        """
        return ElementEvaluation(
            instrument_id=instrument_id, as_of=as_of, inputs=instrument_input
        )

    def execute(
        self,
        plan: BasicCompiledPlan,
        instrument_inputs: Mapping[str, InstrumentInput],
        *,
        as_of: datetime,
    ) -> BasicExecutionResult:
        """Evaluate ``plan`` for every instrument of every flow at ``as_of``.

        Instruments run in UUID order within a flow, each to completion before
        the next starts, so the decision sequence is a pure function of the plan
        and its inputs.
        """
        decisions: list[BasicInstrumentDecision] = []
        evaluations: dict[str, ElementEvaluation] = {}
        for flow in plan.flows:
            flow_decisions = []
            for instrument_id in sorted(flow.instrument_ids, key=uuid.UUID):
                supplied = instrument_inputs.get(instrument_id)
                if supplied is not None and supplied.instrument_id != instrument_id:
                    raise _structure(
                        "instrument inputs must be keyed by instrument_id: the entry "
                        f"keyed by {instrument_id} carries data for "
                        f"{supplied.instrument_id}"
                    )
                flow_decisions.append(
                    self._evaluate_instrument(
                        plan, flow, instrument_id, supplied, as_of, evaluations
                    )
                )
            # Each container allocates within its own side: a buy container spreads its
            # budget across the instruments it chose, and a sell container is separate.
            decisions.extend(_allocate_equally(flow_decisions, flow.side))
        return BasicExecutionResult(tuple(decisions), as_of, evaluations)

    def _evaluate_instrument(
        self,
        plan: BasicCompiledPlan,
        flow: BasicPlanFlow,
        instrument_id: str,
        instrument_input: InstrumentInput | None,
        as_of: datetime,
        evaluations: dict[str, ElementEvaluation],
    ) -> BasicInstrumentDecision:
        if instrument_input is None:
            return BasicInstrumentDecision(
                flow_id=flow.flow_id,
                instrument_id=instrument_id,
                side=flow.side,
                status=BasicDecisionStatus.INPUT_MISSING,
                trace=(),
                first_failure_step_id=_MISSING_INPUT_STEP_ID,
                first_failure_reason=INPUT_MISSING_REASON,
            )

        evaluation = self.evaluation_for(instrument_id, instrument_input, as_of)
        evaluations[instrument_id] = evaluation
        trace: list[BasicStepTrace] = []

        # The container's own chain, not the plan's: under version 2 a buy container and a
        # sell container hold different blocks, and each instrument is judged by the chain
        # of the container it belongs to.
        for step in flow.condition_steps:
            step_id = f"step-{step.sequence}:{step.operation}"
            try:
                if self.on_step is not None:
                    self.on_step(instrument_id, step)
                outcome = plan.catalog.evaluate(step, evaluation)
            except ElementInputMissing as missing:
                trace.append(
                    BasicStepTrace(
                        step_id,
                        False,
                        INPUT_MISSING_REASON,
                        {**dict(missing.evidence), "inputReason": missing.input_reason},
                    )
                )
                return _failed(
                    flow,
                    instrument_id,
                    BasicDecisionStatus.INPUT_MISSING,
                    trace,
                    step_id,
                    INPUT_MISSING_REASON,
                )
            except Exception as failure:
                trace.append(
                    BasicStepTrace(
                        step_id,
                        False,
                        CONDITION_ERROR_REASON,
                        {
                            "errorType": type(failure).__name__,
                            "errorDetail": str(failure),
                        },
                    )
                )
                return _failed(
                    flow,
                    instrument_id,
                    BasicDecisionStatus.CONDITION_ERROR,
                    trace,
                    step_id,
                    CONDITION_ERROR_REASON,
                )

            trace.append(
                BasicStepTrace(
                    step_id, outcome.is_passed, outcome.reason_code, outcome.evidence
                )
            )
            if not outcome.is_passed:
                return _failed(
                    flow,
                    instrument_id,
                    BasicDecisionStatus.CONDITION_NOT_MET,
                    trace,
                    step_id,
                    outcome.reason_code,
                )

        return BasicInstrumentDecision(
            flow_id=flow.flow_id,
            instrument_id=instrument_id,
            side=flow.side,
            status=BasicDecisionStatus.CANDIDATE,
            trace=tuple(trace),
            reference_price=_reference_price(plan, evaluation),
        )

    # -- emission --------------------------------------------------------

    def order_candidates(
        self,
        plan: BasicCompiledPlan,
        result: BasicExecutionResult,
        *,
        evaluation_id: str,
        session_date_et: date,
        session_closes_at: datetime,
        eligible_at: datetime | None = None,
    ) -> tuple[OrderCandidate, ...]:
        """Run the plan's terminal ``EMIT_ORDER_CANDIDATE`` for every survivor.

        A decision that did not reach ``CANDIDATE`` emits nothing at all - not
        an order with an empty or zero size.
        """
        decided_at = result.as_of
        candidates: list[OrderCandidate] = []
        for decision in result.decisions:
            if decision.status is not BasicDecisionStatus.CANDIDATE:
                continue
            flow = plan.flow(decision.flow_id)
            if decision.reference_price is None:  # pragma: no cover - invariant
                raise ElementEvaluationError(
                    f"candidate {decision.instrument_id} carries no reference price"
                )
            candidates.append(
                emit_order_candidate(
                    plan.terminal_step,
                    evaluation_id=evaluation_id,
                    instrument_id=decision.instrument_id,
                    partition_key=flow.partition_key,
                    flow_id=flow.flow_id,
                    budget_cap_bps=flow.budget_cap_bps,
                    allocation=decision.buy_allocation,
                    reference_price=decision.reference_price,
                    decided_at=decided_at,
                    eligible_at=decided_at if eligible_at is None else eligible_at,
                    session_date_et=session_date_et,
                    session_closes_at=session_closes_at,
                )
            )
        return tuple(candidates)


def _failed(
    flow: BasicPlanFlow,
    instrument_id: str,
    status: BasicDecisionStatus,
    trace: list[BasicStepTrace],
    step_id: str,
    reason_code: str,
) -> BasicInstrumentDecision:
    return BasicInstrumentDecision(
        flow_id=flow.flow_id,
        instrument_id=instrument_id,
        side=flow.side,
        status=status,
        trace=tuple(trace),
        first_failure_step_id=step_id,
        first_failure_reason=reason_code,
    )


def _allocate_equally(
    decisions: list[BasicInstrumentDecision], side: str
) -> tuple[BasicInstrumentDecision, ...]:
    """Give every surviving BUY candidate the exact share ``1/n``.

    ``Fraction``, not ``Decimal``: 1/3 has no finite decimal form, and three
    candidates must together claim exactly the whole budget. A SELL flow is
    sized from the held position and is never allocated here.
    """
    if side != "BUY":
        return tuple(decisions)
    candidate_count = sum(
        decision.status is BasicDecisionStatus.CANDIDATE for decision in decisions
    )
    if candidate_count == 0:
        return tuple(decisions)
    share = Fraction(1, candidate_count)
    return tuple(
        replace(decision, buy_allocation=share)
        if decision.status is BasicDecisionStatus.CANDIDATE
        else decision
        for decision in decisions
    )


def _reference_price(plan: BasicCompiledPlan, evaluation: ElementEvaluation) -> Decimal:
    data_kind, resolution = plan.reference_series
    series = evaluation.inputs.series_for(data_kind, resolution)
    completed = series.completed_through(evaluation.as_of) if series else ()
    if not completed:  # pragma: no cover - a candidate loaded a feature from it
        raise ElementEvaluationError(
            f"no completed {data_kind}/{resolution} bar to price "
            f"{evaluation.instrument_id} at {evaluation.as_of.isoformat()}"
        )
    return quantize_money(completed[-1].close, "reference_price")


# ---------------------------------------------------------------------------
# Data requirements
# ---------------------------------------------------------------------------


def derive_data_requirements(
    plan: BasicCompiledPlan,
    *,
    evaluation_from: datetime,
    evaluation_through: datetime,
) -> tuple[DataRequirement, ...]:
    """One requirement per ``(instrument, data_kind, resolution)`` the plan reads.

    The warm-up window is derived from the feature definitions, never
    configured: a feature needing 15 bars of 1m data needs its series from 15
    minutes before the first evaluation instant. Requirements are deduplicated
    across flows and partitions and returned in ``requirement_id`` order, so the
    same plan always makes the same request of the data layer.
    """
    by_id: dict[str, DataRequirement] = {}
    for instrument_id in plan.instrument_ids:
        for feature in plan.required_features:
            requirement_id = (
                f"{instrument_id}|{feature.data_kind}|{feature.bar_resolution}"
            )
            warmup_from = evaluation_from - feature.warmup_span
            existing = by_id.get(requirement_id)
            if existing is not None and existing.warmup_from <= warmup_from:
                # Two features on the same series: the longer warm-up wins.
                continue
            by_id[requirement_id] = DataRequirement(
                requirement_id=requirement_id,
                instrument_id=instrument_id,
                data_kind=feature.data_kind,
                resolution=feature.bar_resolution,
                warmup_from=warmup_from,
                evaluation_from=evaluation_from,
                evaluation_through=evaluation_through,
            )
    return tuple(by_id[key] for key in sorted(by_id))


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def bar_closed_event(
    *,
    event_id: str,
    instrument_id: str,
    data_kind: str,
    resolution: str,
    starts_at: datetime,
    close: Decimal,
    volume: Decimal,
    source_sequence: int,
    available_at: datetime | None = None,
) -> MarketDataEvent:
    """A completed bar, as the replay clock sees it.

    ``occurred_at`` is the bar's *end*, not its start: a bar becomes a fact only
    when its period closes. ``available_at`` defaults to that same instant and
    may only be later, never earlier - a feed that offers a bar before it closed
    is offering the future.
    """
    ends_at = starts_at + resolution_period(resolution)
    bar = SeriesBar(
        instrument_id=instrument_id,
        resolution=resolution,
        starts_at=starts_at,
        ends_at=ends_at,
        close=close,
        volume=volume,
    )
    published_at = bar.ends_at if available_at is None else available_at
    if published_at < bar.ends_at:
        raise ValueError(
            f"available_at {published_at.isoformat()} precedes the close of the bar "
            f"ending {bar.ends_at.isoformat()}: a bar cannot be available before it "
            "closes"
        )
    return MarketDataEvent(
        event_id=event_id,
        instrument_id=bar.instrument_id,
        occurred_at=bar.ends_at,
        available_at=published_at,
        source_sequence=source_sequence,
        event_type=BAR_CLOSED_EVENT_TYPE,
        payload={"dataKind": data_kind, "resolution": resolution, "bar": bar},
    )


def _instrument_inputs(
    events: Iterable[MarketDataEvent],
    as_of: datetime,
    *,
    feature_series: Sequence[PinnedFeatureSeries] = (),
    require_pinned_features: bool = False,
) -> dict[str, InstrumentInput]:
    """Group the bars available at ``as_of`` into one input per instrument."""
    grouped: dict[tuple[str, str, str], list[SeriesBar]] = {}
    for event in events:
        if event.event_type != BAR_CLOSED_EVENT_TYPE or event.available_at > as_of:
            continue
        payload = event.payload
        key = (event.instrument_id, payload["dataKind"], payload["resolution"])
        grouped.setdefault(key, []).append(payload["bar"])

    by_instrument: dict[str, list[InstrumentSeries]] = {}
    for (instrument_id, data_kind, resolution), bars in grouped.items():
        by_instrument.setdefault(instrument_id, []).append(
            InstrumentSeries(
                instrument_id=instrument_id,
                data_kind=data_kind,
                resolution=resolution,
                bars=tuple(sorted(bars, key=lambda bar: bar.starts_at)),
            )
        )
    features_by_instrument: dict[str, list[PinnedFeatureSeries]] = {}
    for feature in feature_series:
        features_by_instrument.setdefault(feature.instrument_id, []).append(feature)
    instrument_ids = set(by_instrument) | set(features_by_instrument)
    return {
        instrument_id: InstrumentInput(
            instrument_id=instrument_id,
            series=tuple(by_instrument.get(instrument_id, ())),
            feature_series=tuple(features_by_instrument.get(instrument_id, ())),
            require_pinned_features=require_pinned_features,
        )
        for instrument_id in instrument_ids
    }


# ---------------------------------------------------------------------------
# The execution gate and the replay
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExecutionGate:
    """Stateless "may this happen now?" query over calendar plus availability.

    The execution model needs the answer at instants the replay never evaluates
    (an order resting between bars, an expiry check). Advancing the replay clock
    to ask would corrupt the replay, so the gate reads the pinned *schedule* -
    the same data the clock reads - and never the clock's cursor.
    """

    schedule: OfficialSessionSchedule
    assessment: AvailabilityAssessment

    def session_status_at(self, instant: datetime) -> MarketSessionStatus:
        _, status = self.schedule.status_at(instant)
        return status

    def session_closes_at(self, instant: datetime) -> datetime | None:
        """The official close of ``instant``'s session, or ``None`` if there is none."""
        session, _ = self.schedule.status_at(instant)
        return None if session is None else session.closes_at

    def is_stage_allowed(self, stage: SkipStage, instant: datetime) -> bool:
        """Whether the pinned data coverage permits ``stage`` at ``instant``."""
        return self.assessment.is_stage_allowed(stage, instant)

    def is_evaluation_allowed(self, instant: datetime) -> bool:
        return self.is_stage_allowed(SkipStage.EVALUATION, instant)

    def is_order_trigger_allowed(self, instant: datetime) -> bool:
        return self.is_stage_allowed(SkipStage.ORDER_TRIGGER, instant)

    def is_fill_allowed(self, instant: datetime) -> bool:
        """Both gates must agree: the market is open *and* the data is there."""
        return self.session_status_at(
            instant
        ) is MarketSessionStatus.REGULAR_OPEN and self.is_stage_allowed(
            SkipStage.FILL, instant
        )


@dataclass(frozen=True, slots=True)
class PlanEvaluation:
    """Everything one replay instant produced."""

    evaluation_id: str
    occurred_at: datetime
    session_status: MarketSessionStatus
    decisions: tuple[BasicInstrumentDecision, ...]
    candidates: tuple[OrderCandidate, ...]
    skip_reason: ReplaySkipReason | None = None


class BasicPlanReplay:
    """Drives a loaded plan across a pinned event stream.

    The evaluation instants are the *bar close* instants of the stream, not the
    instants at which bars were published. Publication lag therefore changes
    what an evaluation can see without changing when the evaluations happen,
    which is the property a reproducible backtest needs: the same session
    replayed from a later snapshot of the same data must line up instant for
    instant.

    Because a lagged bar reveals its close instant only when it arrives, the
    loop may discover an instant after passing it. The evaluation still sees
    exactly what was available *at that instant* - the clock's release times are
    what filter the input - and the returned sequence is sorted by instant.
    """

    def __init__(
        self,
        *,
        runtime: BasicPlanRuntime,
        plan: BasicCompiledPlan,
        clock: MarketEventClock,
        assessment: AvailabilityAssessment,
        feature_series: Sequence[PinnedFeatureSeries] = (),
        require_pinned_features: bool = False,
    ) -> None:
        if assessment.status is AvailabilityStatus.UNAVAILABLE:
            raise ReplayUnavailableError(_unavailable_fields(assessment))
        self.runtime = runtime
        self.plan = plan
        self.clock = clock
        self.assessment = assessment
        self.feature_series = tuple(feature_series)
        self.require_pinned_features = require_pinned_features
        self.gate = ExecutionGate(clock.schedule, assessment)
        self._evaluations: tuple[PlanEvaluation, ...] | None = None

    def run(self) -> tuple[PlanEvaluation, ...]:
        """Replay the whole stream. Idempotent: the clock is advanced once."""
        if self._evaluations is None:
            self._evaluations = self._replay()
        return self._evaluations

    def _replay(self) -> tuple[PlanEvaluation, ...]:
        evaluations: list[PlanEvaluation] = []
        evaluated: set[datetime] = set()
        while (snapshot := self.clock.advance_to_next_event()) is not None:
            instants = sorted(
                {event.occurred_at for event in snapshot.released_events} - evaluated
            )
            for instant in instants:
                evaluated.add(instant)
                evaluations.append(self._evaluate_at(instant, snapshot.visible_events))
        return tuple(sorted(evaluations, key=lambda item: item.occurred_at))

    def _evaluate_at(
        self, instant: datetime, visible_events: tuple[MarketDataEvent, ...]
    ) -> PlanEvaluation:
        evaluation_id = self.plan.evaluation_id(instant)
        session, status = self.clock.schedule.status_at(instant)

        if not self.gate.is_evaluation_allowed(instant):
            return PlanEvaluation(
                evaluation_id=evaluation_id,
                occurred_at=instant,
                session_status=status,
                decisions=(),
                candidates=(),
                skip_reason=ReplaySkipReason.DATA_GAP_EVALUATION_SKIPPED,
            )

        result = self.runtime.execute(
            self.plan,
            _instrument_inputs(
                visible_events,
                instant,
                feature_series=self.feature_series,
                require_pinned_features=self.require_pinned_features,
            ),
            as_of=instant,
        )
        candidates: tuple[OrderCandidate, ...] = ()
        if session is not None:
            candidates = self.runtime.order_candidates(
                self.plan,
                result,
                evaluation_id=evaluation_id,
                session_date_et=session.trading_date_et,
                session_closes_at=session.closes_at,
            )
        return PlanEvaluation(
            evaluation_id=evaluation_id,
            occurred_at=instant,
            session_status=status,
            decisions=result.decisions,
            candidates=candidates,
        )


def _unavailable_fields(assessment: AvailabilityAssessment) -> dict[str, Any]:
    """The ``BACKTEST_UNAVAILABLE`` payload, naming the actual cause.

    When every missing requirement failed for the same reason, that reason is
    reported verbatim: "warm-up coverage missing" and "series unverified" call
    for different operator responses. Mixed causes collapse to the contract's
    umbrella code.
    """
    reasons = {item.reason_code for item in assessment.missing_requirements}
    reason_code = reasons.pop() if len(reasons) == 1 else "REQUIRED_DATA_UNAVAILABLE"
    return {
        "reason_code": reason_code,
        "missing_requirements": sorted(
            item.contract_value for item in assessment.missing_requirements
        ),
    }
