"""B's compiled-plan loader, C-compatible execution, and the replay wiring.

The plan under test is B's published ``strategy-bot.v1`` fixture, consumed
unmodified. Rejection cases mutate a copy and **re-seal** it with the contract
layer's canonical checksum, so each case proves the loader's own check rather
than tripping the checksum first.
"""

from __future__ import annotations

import copy
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from backtest_engine.basic_runtime import (
    COMPILER_VERSION,
    INSTRUMENT_CATALOG_VERSIONS,
    PLAN_SCHEMA_VERSION,
    RUNTIME_SCHEMA_VERSION,
    BasicDecisionStatus,
    BasicPlanCompatibilityError,
    BasicPlanReplay,
    BasicPlanRuntime,
    BasicRuntimeCompatibility,
    PlanLoadFailure,
    ReplaySkipReason,
    ReplayUnavailableError,
    _ReplayExecutionState,
    bar_closed_event,
    derive_data_requirements,
)
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.contracts import compute_compiled_plan_checksum
from backtest_engine.data_availability import (
    AvailabilityStatus,
    DataAvailabilityAssessor,
    DataObservation,
    SkipStage,
    TimeInterval,
)
from backtest_engine.elements import (
    ElementEvaluation,
    InstrumentInput,
    InstrumentSeries,
    SeriesBar,
    element_catalog,
)
from backtest_engine.event_clock import MarketEventClock, MarketSessionStatus


FIXTURES = Path(__file__).parent / "fixtures/contracts/strategy-bot/v1"
VALID_PLAN = FIXTURES / "basic-compiled-plan.valid.json"
UNSUPPORTED_PLAN = FIXTURES / "basic-compiled-plan.unsupported-version.json"

FIRST = "00000000-0000-4000-8000-000000000301"
SECOND = "00000000-0000-4000-8000-000000000302"

SESSION_DATE = date(2025, 11, 28)
OPEN = datetime.fromisoformat("2025-11-28T14:30:00+00:00")
MINUTE = timedelta(minutes=1)

# sum(gain)=1, sum(loss)=7 over the first 15 bars -> RSI_14 = 12.50000000 < 30.
OVERSOLD_CLOSES = [
    "100", "101", "100", "99", "98", "97", "96", "95", "94",
    "94", "94", "94", "94", "94", "94", "94", "94", "94", "94", "94",
]


def test_production_execution_gate_enforces_rearm_wait_and_maximum() -> None:
    state = _ReplayExecutionState()

    def candidate(session: date = SESSION_DATE) -> Any:
        return SimpleNamespace(
            execution_mode="대기 후 재진입",
            max_executions=2,
            wait_mode="조건 재충족",
            wait_interval=1,
            session_date_et=session,
        )

    assert state.accepts(candidate()) is True
    assert state.accepts(candidate()) is False
    state.observe_non_candidate(BasicDecisionStatus.CONDITION_NOT_MET)
    assert state.accepts(candidate()) is True
    state.observe_non_candidate(BasicDecisionStatus.CONDITION_NOT_MET)
    assert state.accepts(candidate()) is False


def _document(path: Path = VALID_PLAN) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resealed(mutate: Any) -> dict[str, Any]:
    """B's plan with one mutation applied and the plan checksum recomputed."""
    document = copy.deepcopy(_document())
    mutate(document)
    document["planChecksum"] = compute_compiled_plan_checksum(document)
    return document


def _runtime(
    compatibility: BasicRuntimeCompatibility | None = None,
) -> BasicPlanRuntime:
    return BasicPlanRuntime(
        compatibility or BasicRuntimeCompatibility.implemented()
    )


def _utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Loading B's real format
# ---------------------------------------------------------------------------


def test_loads_bs_published_plan_unmodified() -> None:
    plan = _runtime().load(_document())

    assert plan.contract_version == "strategy-bot.v1"
    assert plan.schema_version == PLAN_SCHEMA_VERSION == "basic-compiled-plan.v1"
    assert plan.compiler_version == COMPILER_VERSION == "basic-compiler:1.0.0"
    assert plan.element_catalog_version == "basic-elements:2026-07-31"
    assert plan.instrument_catalog_version == "us-supported-universe:2026-07-31"
    assert plan.instrument_catalog_version in INSTRUMENT_CATALOG_VERSIONS
    assert plan.plan_checksum == (
        "sha256:88d61198d46dce161c2a929702a7fd1cee5c9b044c470d2590b96f3825fcacb3"
    )
    assert plan.snapshot_hash == "sha256:" + "1" * 64
    assert plan.semantic_hash == "sha256:" + "2" * 64
    assert plan.required_feature_set_hash == "sha256:" + "3" * 64
    assert plan.mode == "BASIC"
    assert plan.currency == "USD"
    assert plan.initial_cash == Decimal("100000.00000000")

    assert [step.operation for step in plan.condition_steps] == [
        "LOAD_FEATURE",
        "COMPARE",
    ]
    assert plan.terminal_step.operation == "EMIT_ORDER_CANDIDATE"
    assert (plan.side, plan.order_type, plan.allocation) == ("BUY", "MARKET", "EQUAL")

    assert len(plan.flows) == 1
    flow = plan.flows[0]
    assert (flow.partition_key, flow.flow_id) == ("partition-1", "flow-1")
    assert flow.instrument_ids == (FIRST,)
    assert flow.budget_cap_bps == 10000
    assert flow.side == "BUY"

    # B's requiredFeatures block, verbatim and unrenamed.
    assert [
        (
            item.requirement_id,
            item.feature_id,
            item.feature_version,
            item.instruments,
            item.resolution,
            item.required_observations,
        )
        for item in plan.required_features
    ] == [
        (
            "rsi-14-pt1m",
            "00000000-0000-4000-8000-000000000401",
            "1.0.0",
            (FIRST,),
            "PT1M",
            14,
        )
    ]

    # This build's derivations from it, each with a single stated source.
    feature = plan.required_features[0]
    assert feature.feature_key == "RSI_14"           # featureId UUID via the catalog
    assert feature.definition_version == "rsi:1.0.0"  # featureVersion 1.0.0 + our slug
    assert feature.bar_resolution == "1m"             # PT1M via ISO8601_RESOLUTIONS
    assert feature.data_kind == "ADJUSTED_BAR"        # from our feature definition only
    # B declares a floor of 14 observations; rsi:1.0.0 needs 15 closes for 14
    # changes. The warm-up must satisfy both, so it is the larger.
    assert (feature.required_observations, feature.definition_bars) == (14, 15)
    assert feature.warmup_bars == 15
    assert plan.reference_series == ("ADJUSTED_BAR", "1m")


@pytest.mark.parametrize(
    ("resolution", "period"),
    [
        ("30m", timedelta(minutes=30)),
        ("1h", timedelta(hours=1)),
        ("4h", timedelta(hours=4)),
        ("1d", timedelta(days=1)),
    ],
)
def test_loads_and_executes_the_full_catalog_without_synthetic_features(
    resolution: str, period: timedelta
) -> None:
    def full_catalog(document: dict[str, Any]) -> None:
        document["elementCatalogVersion"] = "basic-elements:2026-08-08"
        document["requiredFeatures"] = []
        document["steps"] = [
            {"sequence": 1, "operation": "PRICE_COMPARE", "arguments": {
                "resolution": resolution, "operator": "GT", "reference": "PREVIOUS_CLOSE",
            }},
            {"sequence": 2, "operation": "EMIT_ORDER_CANDIDATE", "arguments": {
                "allocation": "EQUAL", "orderType": "MARKET", "timeInForce": "DAY",
                "side": "BUY", "orderPercent": "50", "executionMode": "1회만",
                "waitMode": "조건 재충족", "waitInterval": "1", "maxExecutions": "1",
            }},
        ]

    plan = _runtime().load(_resealed(full_catalog))
    bars = tuple(
        SeriesBar(
            instrument_id=FIRST, resolution=resolution,
            starts_at=OPEN + period * index, ends_at=OPEN + period * (index + 1),
            close=Decimal(value), volume=Decimal("1000"),
        )
        for index, value in enumerate(("100", "101"))
    )
    instrument_input = InstrumentInput(
        instrument_id=FIRST,
        series=(InstrumentSeries(
            instrument_id=FIRST, data_kind="ADJUSTED_BAR", resolution=resolution, bars=bars
        ),),
        values={
            f"bar.closed.{resolution}": "true",
            f"closes.{resolution}": "100,101",
            f"volumes.{resolution}": "1000,1000",
        },
    )
    result = _runtime().execute(plan, {FIRST: instrument_input}, as_of=bars[-1].ends_at)

    assert plan.reference_series == ("ADJUSTED_BAR", resolution)
    assert plan.required_features == ()
    assert result.decisions[0].status is BasicDecisionStatus.CANDIDATE
    assert result.decisions[0].reference_price == Decimal("101.00000000")


def test_v2_catalog_position_cap_survives_plan_loading_and_candidate_emission() -> None:
    def v2_catalog(document: dict[str, Any]) -> None:
        document["elementCatalogVersion"] = "basic-elements:2026-08-25"
        document["requiredFeatures"] = []
        document["steps"] = [
            {"sequence": 1, "operation": "PRICE_COMPARE", "arguments": {
                "resolution": "30m", "operator": "GT", "reference": "PREVIOUS_CLOSE",
            }},
            {"sequence": 2, "operation": "EMIT_ORDER_CANDIDATE", "arguments": {
                "allocation": "EQUAL", "orderType": "MARKET", "timeInForce": "DAY",
                "side": "BUY", "orderPercent": "25", "maxPositionPercent": "40",
                "executionMode": "1회만", "waitMode": "조건 재충족",
                "waitInterval": "1", "maxExecutions": "1",
            }},
        ]

    plan = _runtime().load(_resealed(v2_catalog))
    evaluation_at = OPEN + timedelta(hours=1)
    bars = tuple(
        SeriesBar(
            instrument_id=FIRST,
            resolution="30m",
            starts_at=OPEN + timedelta(minutes=30 * index),
            ends_at=OPEN + timedelta(minutes=30 * (index + 1)),
            close=Decimal(value),
            volume=Decimal("1000"),
        )
        for index, value in enumerate(("100", "101"))
    )
    result = _runtime().execute(
        plan,
        {FIRST: InstrumentInput(
            instrument_id=FIRST,
            series=(InstrumentSeries(
                instrument_id=FIRST,
                data_kind="ADJUSTED_BAR",
                resolution="30m",
                bars=bars,
            ),),
            values={
                "bar.closed.30m": "true",
                "closes.30m": "100,101",
                "volumes.30m": "1000,1000",
            },
        )},
        as_of=evaluation_at,
    )
    candidate = _runtime().order_candidates(
        plan,
        result,
        evaluation_id="evaluation-v2",
        session_date_et=SESSION_DATE,
        session_closes_at=evaluation_at + timedelta(hours=1),
    )[0]

    assert candidate.order_percent == Decimal("25")
    assert candidate.max_position_percent == Decimal("40")


@pytest.mark.parametrize(
    ("resolution", "wire_resolution", "feature_id"),
    [
        ("30m", "PT30M", "4b1c6801-0259-5176-a857-0e5ea923d898"),
        ("1h", "PT1H", "2e18c093-5d4e-5d9a-bd22-b7e5679f1a3e"),
        ("4h", "PT4H", "1b2785bd-20f0-50a2-ae96-6a1f7bad74b9"),
        ("1d", "PT24H", "eddfb2d4-8586-5260-8fc9-9c8125990270"),
    ],
)
def test_rsi_cross_requires_the_exact_selected_resolution_feature_definition(
    resolution: str, wire_resolution: str, feature_id: str
) -> None:
    def rsi_cross(document: dict[str, Any]) -> None:
        catalog = element_catalog("basic-elements:2026-08-08")
        terminal = catalog.spec("EMIT_ORDER_CANDIDATE")
        terminal_arguments = {
            name: values[0] for name, values in terminal.enumerations.items()
        }
        terminal_arguments.update(
            {"orderPercent": "50", "waitInterval": "1", "maxExecutions": "1"}
        )
        document["elementCatalogVersion"] = catalog.version
        document["requiredFeatures"] = [
            {
                "requirementId": f"rsi-14-{resolution}",
                "featureId": feature_id,
                "featureVersion": "1.0.0",
                "instruments": [FIRST],
                "resolution": wire_resolution,
                "requiredObservations": 14,
            }
        ]
        document["steps"] = [
            {
                "sequence": 1,
                "operation": "RSI_CROSS",
                "arguments": {
                    "resolution": resolution,
                    "direction": "UP",
                    "period": "14",
                    "threshold": "50",
                },
            },
            {"sequence": 2, "operation": "EMIT_ORDER_CANDIDATE", "arguments": terminal_arguments},
        ]

    plan = _runtime().load(_resealed(rsi_cross))

    assert plan.required_features[0].feature_id == feature_id
    assert plan.required_features[0].bar_resolution == resolution
    assert plan.reference_series == ("ADJUSTED_BAR", resolution)


def test_rsi_cross_rejects_a_feature_uuid_from_another_resolution() -> None:
    def mismatched(document: dict[str, Any]) -> None:
        catalog = element_catalog("basic-elements:2026-08-08")
        terminal = catalog.spec("EMIT_ORDER_CANDIDATE")
        terminal_arguments = {
            name: values[0] for name, values in terminal.enumerations.items()
        }
        terminal_arguments.update(
            {"orderPercent": "50", "waitInterval": "1", "maxExecutions": "1"}
        )
        document["elementCatalogVersion"] = catalog.version
        document["requiredFeatures"] = [{
            "requirementId": "rsi-14-30m",
            "featureId": "2e18c093-5d4e-5d9a-bd22-b7e5679f1a3e",
            "featureVersion": "1.0.0",
            "instruments": [FIRST],
            "resolution": "PT30M",
            "requiredObservations": 14,
        }]
        document["steps"] = [{
            "sequence": 1,
            "operation": "RSI_CROSS",
            "arguments": {"resolution": "30m", "direction": "UP", "period": "14", "threshold": "50"},
        }, {"sequence": 2, "operation": "EMIT_ORDER_CANDIDATE", "arguments": terminal_arguments}]

    with pytest.raises(BasicPlanCompatibilityError, match="pins 1h"):
        _runtime().load(_resealed(mismatched))


def test_the_vendored_fixture_is_byte_identical_to_bs_authoritative_copy() -> None:
    """The vendored copy must not have drifted from B's own.

    Compared after normalising line endings, not on the raw bytes. The superproject
    declares `* text=auto eol=lf`, but a Windows working tree can still hold the
    file with CRLF while git's content is unchanged, and this assertion then failed
    on a checkout artefact rather than on drift -- which is the one thing it must
    not do, because the noise trains a reader to ignore it. Any difference in the
    document's actual content still fails.
    """
    authoritative = (
        Path(__file__).resolve().parents[3]
        / "Idea2Strategy/backend/modules/backend-messaging/src/main/resources"
        / "contracts/strategy-bot/v1/basic-compiled-plan.valid.json"
    )
    if not authoritative.is_file():
        pytest.skip("the backend submodule is not checked out beside this worktree")

    def normalised(path: Path) -> bytes:
        return path.read_bytes().replace(b"\r\n", b"\n")

    assert normalised(VALID_PLAN) == normalised(authoritative)


def test_rejects_the_unsupported_version_fixture_without_substitution() -> None:
    with pytest.raises(BasicPlanCompatibilityError) as failure:
        _runtime().load(_document(UNSUPPORTED_PLAN))

    assert failure.value.failure is PlanLoadFailure.PLAN_CONTRACT_INVALID
    assert "strategy-bot.v999" in str(failure.value)


def test_verifies_both_the_plan_checksum_and_the_requests_copy_of_it() -> None:
    runtime = _runtime()
    document = _document()

    tampered = copy.deepcopy(document)
    tampered["steps"][1]["arguments"]["threshold"] = "70"
    with pytest.raises(BasicPlanCompatibilityError) as contract_failure:
        runtime.load(tampered)
    assert contract_failure.value.failure is PlanLoadFailure.PLAN_CONTRACT_INVALID
    assert "planChecksum" in str(contract_failure.value)

    with pytest.raises(BasicPlanCompatibilityError) as integrity_failure:
        runtime.load(document, compiled_plan_checksum="sha256:" + "0" * 64)
    assert integrity_failure.value.failure is PlanLoadFailure.PLAN_INTEGRITY_MISMATCH

    # The matching request checksum is accepted.
    assert (
        runtime.load(document, compiled_plan_checksum=document["planChecksum"]).plan_checksum
        == document["planChecksum"]
    )


# ---------------------------------------------------------------------------
# The three C compatibility gates
# ---------------------------------------------------------------------------


def test_plan_schema_version_mismatch_mirrors_c() -> None:
    runtime = _runtime(
        BasicRuntimeCompatibility(
            plan_schema_versions=("basic-compiled-plan.v99",),
            runtime_schema_version=RUNTIME_SCHEMA_VERSION,
            supported_feature_versions={"RSI_14": "rsi:1.0.0"},
        )
    )

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        runtime.load(_document())

    assert failure.value.failure is PlanLoadFailure.PLAN_SCHEMA_VERSION_MISMATCH
    assert "basic-compiled-plan.v1 not in ['basic-compiled-plan.v99']" in str(
        failure.value
    )


def test_feature_version_mismatch_mirrors_c() -> None:
    runtime = _runtime(
        BasicRuntimeCompatibility(
            plan_schema_versions=(PLAN_SCHEMA_VERSION,),
            runtime_schema_version=RUNTIME_SCHEMA_VERSION,
            supported_feature_versions={"RSI_14": "rsi:2.0.0"},
        )
    )

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        runtime.load(_document())

    assert failure.value.failure is PlanLoadFailure.FEATURE_VERSION_MISMATCH
    assert "RSI_14@rsi:1.0.0" in str(failure.value)


def test_a_feature_the_runtime_does_not_implement_at_all_is_a_version_mismatch() -> None:
    runtime = _runtime(
        BasicRuntimeCompatibility(
            plan_schema_versions=(PLAN_SCHEMA_VERSION,),
            runtime_schema_version=RUNTIME_SCHEMA_VERSION,
            supported_feature_versions={},
        )
    )

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        runtime.load(_document())

    assert failure.value.failure is PlanLoadFailure.FEATURE_VERSION_MISMATCH


def test_runtime_schema_version_mismatch_mirrors_c() -> None:
    runtime = _runtime(
        BasicRuntimeCompatibility(
            plan_schema_versions=(PLAN_SCHEMA_VERSION,),
            runtime_schema_version="strategy-bot-runtime.v2",
            supported_feature_versions={"RSI_14": "rsi:1.0.0"},
        )
    )

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        runtime.load(_document())

    assert failure.value.failure is PlanLoadFailure.RUNTIME_SCHEMA_VERSION_MISMATCH
    assert "strategy-bot-runtime.v1 != strategy-bot-runtime.v2" in str(failure.value)


def test_a_runtime_state_snapshot_supplies_its_own_declared_version() -> None:
    runtime = _runtime()

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        runtime.load(
            _document(), runtime_schema_version="strategy-bot-runtime.v0"
        )

    assert failure.value.failure is PlanLoadFailure.RUNTIME_SCHEMA_VERSION_MISMATCH


# ---------------------------------------------------------------------------
# Structural rejection, with the checksum re-sealed so the loader is the judge
# ---------------------------------------------------------------------------


def _set_steps(steps: list[dict[str, Any]]) -> Any:
    def mutate(document: dict[str, Any]) -> None:
        document["steps"] = steps

    return mutate


LOAD_STEP = {"sequence": 1, "operation": "LOAD_FEATURE", "arguments": {"feature": "RSI_14", "resolution": "1m"}}
COMPARE_STEP = {"sequence": 2, "operation": "COMPARE", "arguments": {"operator": "LT", "threshold": "30"}}
EMIT_STEP = {
    "sequence": 3,
    "operation": "EMIT_ORDER_CANDIDATE",
    "arguments": {"allocation": "EQUAL", "orderType": "MARKET", "side": "BUY"},
}


@pytest.mark.parametrize(
    ("mutate", "failure", "detail"),
    [
        (
            lambda document: document.update(compilerVersion="basic-compiler:2.0.0"),
            PlanLoadFailure.COMPILER_VERSION_MISMATCH,
            "basic-compiler:2.0.0",
        ),
        (
            lambda document: document.update(elementCatalogVersion="basic-elements:2099-01-01"),
            PlanLoadFailure.ELEMENT_CATALOG_VERSION_UNSUPPORTED,
            "basic-elements:2099-01-01",
        ),
        (
            lambda document: document.update(instrumentCatalogVersion="eu-universe:2026-07-31"),
            PlanLoadFailure.INSTRUMENT_CATALOG_VERSION_UNSUPPORTED,
            "eu-universe:2026-07-31",
        ),
        (
            _set_steps([LOAD_STEP, COMPARE_STEP]),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "must end with EMIT_ORDER_CANDIDATE",
        ),
        (
            _set_steps([EMIT_STEP | {"sequence": 1}, LOAD_STEP | {"sequence": 2}, COMPARE_STEP | {"sequence": 3}]),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "must end with EMIT_ORDER_CANDIDATE",
        ),
        (
            _set_steps([EMIT_STEP | {"sequence": 1}]),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "at least one condition step",
        ),
        (
            _set_steps([COMPARE_STEP | {"sequence": 1}, EMIT_STEP | {"sequence": 2}]),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "no preceding step produced a value",
        ),
        (
            _set_steps([LOAD_STEP, COMPARE_STEP, EMIT_STEP | {"sequence": 9}]),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "sequence",
        ),
        (
            _set_steps(
                [
                    {"sequence": 1, "operation": "PRO_SCRIPT", "arguments": {"body": "x"}},
                    EMIT_STEP | {"sequence": 2},
                ]
            ),
            PlanLoadFailure.UNSUPPORTED_ELEMENT,
            "PRO_SCRIPT",
        ),
        (
            _set_steps(
                [
                    LOAD_STEP,
                    {"sequence": 2, "operation": "COMPARE", "arguments": {"operator": "APPROX", "threshold": "30"}},
                    EMIT_STEP,
                ]
            ),
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "APPROX",
        ),
        (
            _set_steps(
                [
                    {"sequence": 1, "operation": "LOAD_FEATURE", "arguments": {"feature": "MACD", "resolution": "1m"}},
                    COMPARE_STEP,
                    EMIT_STEP,
                ]
            ),
            PlanLoadFailure.UNSUPPORTED_FEATURE,
            "MACD",
        ),
        (
            lambda document: document["executionSnapshot"]["partitions"].append(
                {
                    "key": "partition-2",
                    "budgetCapBps": 5000,
                    "flows": [{"key": "flow-1", "officialInstrumentIds": [SECOND]}],
                }
            ),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "flow key",
        ),
        (
            lambda document: document["executionSnapshot"]["partitions"][0]["flows"][0].update(
                officialInstrumentIds=[FIRST, FIRST]
            ),
            PlanLoadFailure.PLAN_STRUCTURE_INVALID,
            "unique",
        ),
    ],
)
def test_rejects_incompatible_plans_without_substitution(
    mutate: Any, failure: PlanLoadFailure, detail: str
) -> None:
    with pytest.raises(BasicPlanCompatibilityError) as raised:
        _runtime().load(_resealed(mutate))

    assert raised.value.failure is failure
    assert detail in str(raised.value)


def test_a_sell_plan_keeps_sell_meaning() -> None:
    plan = _runtime().load(
        _resealed(
            lambda document: document["steps"][2]["arguments"].update(side="SELL")
        )
    )

    assert plan.side == "SELL"
    assert plan.flows[0].side == "SELL"


# ---------------------------------------------------------------------------
# B's requiredFeatures block
# ---------------------------------------------------------------------------


def test_the_checksum_covers_the_required_features_block() -> None:
    # Changing only a requiredFeatures field must change the checksum, or the
    # block would be unprotected and a warm-up window could be tampered with.
    without = copy.deepcopy(_document())
    without["requiredFeatures"][0]["requiredObservations"] = 20

    assert compute_compiled_plan_checksum(without) != _document()["planChecksum"]

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        _runtime().load(without)
    assert failure.value.failure is PlanLoadFailure.PLAN_CONTRACT_INVALID
    assert "planChecksum" in str(failure.value)


def _set_feature(**changes: Any) -> Any:
    def mutate(document: dict[str, Any]) -> None:
        document["requiredFeatures"][0].update(changes)

    return mutate


@pytest.mark.parametrize(
    ("mutate", "failure", "detail"),
    [
        (
            _set_feature(featureId="00000000-0000-4000-8000-000000000499"),
            PlanLoadFailure.UNSUPPORTED_FEATURE,
            "00000000-0000-4000-8000-000000000499",
        ),
        (
            _set_feature(featureVersion="2.0.0"),
            PlanLoadFailure.FEATURE_VERSION_MISMATCH,
            "2.0.0",
        ),
        (
            _set_feature(featureVersion="1.0"),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "featureVersion",
        ),
        (
            # A resolution this build does not implement at all.
            _set_feature(resolution="PT2H"),
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "PT2H",
        ),
        (
            # PT60M and PT1H are the same duration, but only PT1H is normalized.
            # Accepting the synonym would give one plan two valid checksums.
            _set_feature(resolution="PT60M"),
            PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT,
            "PT60M",
        ),
        (
            _set_feature(requiredObservations=0),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "requiredObservations",
        ),
        (
            # Same UUID value, upper-case hex. B normalises and rejects it, so
            # the two forms can never hash to two different plan checksums.
            _set_feature(featureId="00000000-0000-4000-8000-0000000004AB"),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "featureId",
        ),
        (
            _set_feature(instruments=[FIRST, FIRST]),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "instruments",
        ),
        (
            lambda document: document["requiredFeatures"].append(
                copy.deepcopy(document["requiredFeatures"][0])
            ),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "not unique",
        ),
        (
            lambda document: document["requiredFeatures"].append(
                copy.deepcopy(document["requiredFeatures"][0]) | {"requirementId": "other"}
            ),
            PlanLoadFailure.PLAN_CONTRACT_INVALID,
            "duplicates an earlier required feature",
        ),
            (
                # The step still reads RSI_14 at 1m, but the plan now declares the
                # history for 5m. The canonical feature UUID itself pins 1m.
                _set_feature(resolution="PT5M"),
                PlanLoadFailure.FEATURE_VERSION_MISMATCH,
                "pins 1m",
            ),
    ],
)
def test_rejects_a_required_features_block_it_cannot_honour(
    mutate: Any, failure: PlanLoadFailure, detail: str
) -> None:
    with pytest.raises(BasicPlanCompatibilityError) as raised:
        _runtime().load(_resealed(mutate))

    assert raised.value.failure is failure
    assert detail in str(raised.value)


@pytest.mark.parametrize("value", [None, [], "PT1M", {}])
def test_a_plan_without_a_usable_required_features_block_is_refused(
    value: Any,
) -> None:
    """Not re-sealed on purpose.

    The checksum material reads ``requiredFeatures``, so a document missing it
    cannot be sealed at all. The shape gate therefore has to run *before* the
    checksum, and this proves it does rather than crashing on a KeyError.
    """
    document = copy.deepcopy(_document())
    if value is None:
        document.pop("requiredFeatures")
    else:
        document["requiredFeatures"] = value

    with pytest.raises(BasicPlanCompatibilityError) as failure:
        _runtime().load(document)

    assert failure.value.failure is PlanLoadFailure.PLAN_CONTRACT_INVALID
    assert ("planChecksum" if value == [] else "requiredFeatures") in str(failure.value)


def test_a_declared_floor_above_the_definition_window_widens_the_warm_up() -> None:
    # rsi:1.0.0 consumes 15 bars; B here declares a floor of 40 observations.
    # The warm-up must satisfy both, so it is 40 - proving warmup_bars is not a
    # constant 15 that happens to match B's published plan.
    plan = _runtime().load(_resealed(_set_feature(requiredObservations=40)))
    feature = plan.required_features[0]

    assert (feature.required_observations, feature.definition_bars) == (40, 15)
    assert feature.warmup_bars == 40

    requirements = derive_data_requirements(
        plan,
        evaluation_from=_utc("2025-11-28T14:45:00Z"),
        evaluation_through=_utc("2025-11-28T14:50:00Z"),
    )

    assert requirements[0].warmup_from == _utc("2025-11-28T14:05:00Z")


# ---------------------------------------------------------------------------
# Execution: parity with BasicStrategyExecutor
# ---------------------------------------------------------------------------


def _series(closes: list[str], instrument_id: str = FIRST) -> InstrumentSeries:
    return InstrumentSeries(
        instrument_id=instrument_id,
        data_kind="ADJUSTED_BAR",
        resolution="1m",
        bars=tuple(
            SeriesBar(
                instrument_id=instrument_id,
                resolution="1m",
                starts_at=OPEN + MINUTE * index,
                ends_at=OPEN + MINUTE * (index + 1),
                close=Decimal(value),
                volume=Decimal(1000),
            )
            for index, value in enumerate(closes)
        ),
    )


def _input(closes: list[str], instrument_id: str = FIRST) -> InstrumentInput:
    return InstrumentInput(
        instrument_id=instrument_id, series=(_series(closes, instrument_id),)
    )


def _two_instrument_plan() -> Any:
    return _resealed(
        lambda document: document["executionSnapshot"]["partitions"][0]["flows"][
            0
        ].update(officialInstrumentIds=[SECOND, FIRST])
    )


def test_iterates_instruments_in_uuid_order_and_short_circuits_at_first_failure() -> None:
    runtime = _runtime()
    plan = runtime.load(_two_instrument_plan())
    calls: list[str] = []
    runtime.on_step = lambda instrument_id, step: calls.append(
        f"{step.operation}:{instrument_id[-3:]}"
    )

    result = runtime.execute(
        plan,
        {
            SECOND: _input(["100"] * 15, SECOND),  # flat -> RSI 50, fails LT 30
            FIRST: _input(OVERSOLD_CLOSES[:15]),  # RSI 12.5, passes
        },
        as_of=OPEN + MINUTE * 15,
    )

    assert [decision.instrument_id for decision in result.decisions] == [FIRST, SECOND]
    accepted, rejected = result.decisions
    assert accepted.status is BasicDecisionStatus.CANDIDATE
    assert accepted.buy_allocation == Fraction(1, 1)
    assert [trace.step_id for trace in accepted.trace] == [
        "step-1:LOAD_FEATURE",
        "step-2:COMPARE",
    ]
    assert accepted.trace[0].evidence["value"] == "12.50000000"

    assert rejected.status is BasicDecisionStatus.CONDITION_NOT_MET
    assert rejected.first_failure_step_id == "step-2:COMPARE"
    assert rejected.first_failure_reason == "COMPARE_FALSE"
    assert rejected.buy_allocation is None

    # Every instrument runs to completion before the next one starts, and the
    # failing instrument does not evaluate anything after its first failure.
    assert calls == [
        "LOAD_FEATURE:301",
        "COMPARE:301",
        "LOAD_FEATURE:302",
        "COMPARE:302",
    ]


def test_assigns_the_exact_fraction_one_over_n_to_every_buy_candidate() -> None:
    runtime = _runtime()
    plan = runtime.load(_two_instrument_plan())

    result = runtime.execute(
        plan,
        {
            FIRST: _input(OVERSOLD_CLOSES[:15]),
            SECOND: _input(OVERSOLD_CLOSES[:15], SECOND),
        },
        as_of=OPEN + MINUTE * 15,
    )

    assert [decision.buy_allocation for decision in result.decisions] == [
        Fraction(1, 2),
        Fraction(1, 2),
    ]


def test_sell_flows_are_not_allocated() -> None:
    runtime = _runtime()
    document = copy.deepcopy(_document())
    document["steps"][2]["arguments"]["side"] = "SELL"
    document["executionSnapshot"]["partitions"][0]["flows"][0][
        "officialInstrumentIds"
    ] = [SECOND, FIRST]
    document["planChecksum"] = compute_compiled_plan_checksum(document)
    plan = runtime.load(document)

    result = runtime.execute(
        plan,
        {
            FIRST: _input(OVERSOLD_CLOSES[:15]),
            SECOND: _input(OVERSOLD_CLOSES[:15], SECOND),
        },
        as_of=OPEN + MINUTE * 15,
    )

    assert all(item.status is BasicDecisionStatus.CANDIDATE for item in result.decisions)
    assert all(item.side == "SELL" for item in result.decisions)
    assert all(item.buy_allocation is None for item in result.decisions)


def test_missing_instrument_input_uses_cs_exact_reason_literal() -> None:
    runtime = _runtime()
    plan = runtime.load(_two_instrument_plan())

    result = runtime.execute(
        plan, {FIRST: _input(OVERSOLD_CLOSES[:15])}, as_of=OPEN + MINUTE * 15
    )

    missing = result.decisions[1]
    assert missing.instrument_id == SECOND
    assert missing.status is BasicDecisionStatus.INPUT_MISSING
    assert missing.first_failure_step_id == "$input"
    assert missing.first_failure_reason == "INSTRUMENT_INPUT_MISSING"
    assert missing.trace == ()


def test_an_unusable_series_is_input_missing_not_a_false_condition() -> None:
    runtime = _runtime()
    plan = runtime.load(_document())

    result = runtime.execute(
        plan, {FIRST: _input(OVERSOLD_CLOSES[:14])}, as_of=OPEN + MINUTE * 14
    )

    decision = result.decisions[0]
    assert decision.status is BasicDecisionStatus.INPUT_MISSING
    assert decision.first_failure_step_id == "step-1:LOAD_FEATURE"
    assert decision.first_failure_reason == "INSTRUMENT_INPUT_MISSING"
    assert decision.trace[-1].evidence["inputReason"] == "FEATURE_WARMUP_INCOMPLETE"
    assert decision.trace[-1].evidence["availableBars"] == "14"


def test_an_evaluator_crash_is_isolated_as_a_condition_error() -> None:
    runtime = _runtime()
    plan = runtime.load(_document())

    def explode(instrument_id: str, step: Any) -> None:
        del instrument_id, step
        raise RuntimeError("feature service unavailable")

    runtime.on_step = explode

    result = runtime.execute(
        plan, {FIRST: _input(OVERSOLD_CLOSES[:15])}, as_of=OPEN + MINUTE * 15
    )

    decision = result.decisions[0]
    assert decision.status is BasicDecisionStatus.CONDITION_ERROR
    assert decision.first_failure_reason == "CONDITION_EVALUATION_ERROR"
    assert decision.trace[-1].evidence["errorType"] == "RuntimeError"


def test_execute_rejects_an_input_belonging_to_another_instrument() -> None:
    runtime = _runtime()
    plan = runtime.load(_document())

    with pytest.raises(BasicPlanCompatibilityError, match="keyed by"):
        runtime.execute(
            plan, {FIRST: _input(OVERSOLD_CLOSES[:15], SECOND)}, as_of=OPEN + MINUTE * 15
        )


def test_element_evaluation_state_is_not_shared_between_instruments() -> None:
    runtime = _runtime()
    plan = runtime.load(_two_instrument_plan())
    seen: list[int] = []

    original = runtime.evaluation_for

    def spy(instrument_id: str, instrument_input: Any, as_of: datetime) -> ElementEvaluation:
        evaluation = original(instrument_id, instrument_input, as_of)
        seen.append(id(evaluation))
        return evaluation

    runtime.evaluation_for = spy  # type: ignore[method-assign]
    runtime.execute(
        plan,
        {
            FIRST: _input(OVERSOLD_CLOSES[:15]),
            SECOND: _input(OVERSOLD_CLOSES[:15], SECOND),
        },
        as_of=OPEN + MINUTE * 15,
    )

    assert len(seen) == 2
    assert len(set(seen)) == 2


# ---------------------------------------------------------------------------
# Data requirements derived from the plan
# ---------------------------------------------------------------------------


def test_derives_one_requirement_per_instrument_series_with_the_features_warmup() -> None:
    plan = _runtime().load(_two_instrument_plan())

    requirements = derive_data_requirements(
        plan,
        evaluation_from=_utc("2025-11-28T14:45:00Z"),
        evaluation_through=_utc("2025-11-28T14:50:00Z"),
    )

    assert [item.requirement_id for item in requirements] == [
        f"{FIRST}|ADJUSTED_BAR|1m",
        f"{SECOND}|ADJUSTED_BAR|1m",
    ]
    first = requirements[0]
    assert first.instrument_id == FIRST
    assert first.data_kind == "ADJUSTED_BAR"
    assert first.resolution == "1m"
    # 15 bars of 1m warm-up before the first evaluation instant.
    assert first.warmup_from == _utc("2025-11-28T14:30:00Z")
    assert first.evaluation_from == _utc("2025-11-28T14:45:00Z")
    assert first.evaluation_through == _utc("2025-11-28T14:50:00Z")


def test_requirements_are_deduplicated_across_flows_and_partitions() -> None:
    plan = _runtime().load(
        _resealed(
            lambda document: document["executionSnapshot"]["partitions"].append(
                {
                    "key": "partition-2",
                    "budgetCapBps": 5000,
                    "flows": [{"key": "flow-2", "officialInstrumentIds": [FIRST]}],
                }
            )
        )
    )

    requirements = derive_data_requirements(
        plan,
        evaluation_from=_utc("2025-11-28T14:45:00Z"),
        evaluation_through=_utc("2025-11-28T14:50:00Z"),
    )

    assert len(plan.flows) == 2
    assert [item.requirement_id for item in requirements] == [
        f"{FIRST}|ADJUSTED_BAR|1m"
    ]


# ---------------------------------------------------------------------------
# Replay: clock and availability gate actually applied
# ---------------------------------------------------------------------------


def _events(count: int = 20, lagged: set[int] | None = None) -> list[Any]:
    lagged = lagged or set()
    events = []
    for index in range(count):
        starts_at = OPEN + MINUTE * index
        events.append(
            bar_closed_event(
                event_id=f"bar-{index:02d}",
                instrument_id=FIRST,
                data_kind="ADJUSTED_BAR",
                resolution="1m",
                starts_at=starts_at,
                close=Decimal(OVERSOLD_CLOSES[index]),
                volume=Decimal(1000),
                source_sequence=index + 1,
                available_at=starts_at + MINUTE * (3 if index in lagged else 1),
            )
        )
    return events


def _clock(events: list[Any]) -> MarketEventClock:
    return MarketEventClock(
        XNYS_CALENDAR.session_schedule(SESSION_DATE, SESSION_DATE), events
    )


def _assessment(intervals: tuple[TimeInterval, ...] | None = None) -> Any:
    plan = _runtime().load(_document())
    requirements = derive_data_requirements(
        plan,
        evaluation_from=_utc("2025-11-28T14:45:00Z"),
        evaluation_through=_utc("2025-11-28T14:50:00Z"),
    )
    observation = DataObservation(
        requirement_id=requirements[0].requirement_id,
        instrument_id=FIRST,
        data_kind="ADJUSTED_BAR",
        resolution="1m",
        available_intervals=intervals
        or (TimeInterval(_utc("2025-11-28T14:30:00Z"), _utc("2025-11-28T14:50:00Z")),),
        verified=True,
        listed_at=_utc("2020-01-02T14:30:00Z"),
    )
    return DataAvailabilityAssessor().assess(requirements, [observation])


def _replay(
    events: list[Any] | None = None,
    intervals: tuple[TimeInterval, ...] | None = None,
) -> BasicPlanReplay:
    runtime = _runtime()
    plan = runtime.load(_document())
    return BasicPlanReplay(
        runtime=runtime,
        plan=plan,
        clock=_clock(events if events is not None else _events()),
        assessment=_assessment(intervals),
    )


def test_replay_evaluates_only_after_the_clock_releases_enough_completed_bars() -> None:
    evaluations = _replay().run()

    assert len(evaluations) == 20
    assert [item.occurred_at for item in evaluations[:2]] == [
        _utc("2025-11-28T14:31:00Z"),
        _utc("2025-11-28T14:32:00Z"),
    ]
    statuses = [
        item.decisions[0].status if item.decisions else None for item in evaluations
    ]
    assert statuses[:14] == [BasicDecisionStatus.INPUT_MISSING] * 14
    assert statuses[14] is BasicDecisionStatus.CANDIDATE

    first_candidate = evaluations[14]
    assert first_candidate.occurred_at == _utc("2025-11-28T14:45:00Z")
    assert first_candidate.session_status is MarketSessionStatus.REGULAR_OPEN
    assert first_candidate.decisions[0].trace[0].evidence["value"] == "12.50000000"
    # The bar that starts at 14:45 has not closed; it must not be in the window.
    assert (
        first_candidate.decisions[0].trace[0].evidence["windowThrough"]
        == "2025-11-28T14:45:00+00:00"
    )


def test_replay_emits_order_candidates_execution_model_can_consume() -> None:
    evaluations = _replay().run()

    candidates = evaluations[14].candidates
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.instrument_id == FIRST
    assert candidate.partition_key == "partition-1"
    assert candidate.flow_id == "flow-1"
    assert candidate.side == "BUY"
    assert candidate.order_type == "MARKET"
    assert candidate.allocation == Fraction(1, 1)
    assert candidate.reference_price == Decimal("94.00000000")
    assert candidate.decided_at == _utc("2025-11-28T14:45:00Z")
    assert candidate.eligible_at == _utc("2025-11-28T14:45:00Z")
    assert candidate.session_date_et == SESSION_DATE
    assert candidate.session_closes_at == _utc("2025-11-28T18:00:00Z")
    assert candidate.budget_cap_bps == 10000
    assert candidate.evaluation_id == evaluations[14].evaluation_id

    # Non-candidate evaluations emit nothing at all, not an empty-success order.
    assert evaluations[0].candidates == ()


def test_publication_lag_keeps_a_bar_out_of_the_window_until_it_is_available() -> None:
    # Bar 14 (14:44-14:45) is published two minutes late, so the evaluation at
    # 14:45 has only 14 bars and the RSI window is not yet complete.
    lagged = _replay(events=_events(lagged={14})).run()
    on_time = _replay().run()

    lagged_by_instant = {item.occurred_at: item for item in lagged}
    at_1445 = lagged_by_instant[_utc("2025-11-28T14:45:00Z")]
    assert at_1445.decisions[0].status is BasicDecisionStatus.INPUT_MISSING
    assert at_1445.decisions[0].trace[-1].evidence["availableBars"] == "14"

    at_1447 = lagged_by_instant[_utc("2025-11-28T14:47:00Z")]
    assert at_1447.decisions[0].status is BasicDecisionStatus.CANDIDATE

    # Without the lag the same instant already decides.
    assert {item.occurred_at for item in on_time} == {item.occurred_at for item in lagged}


def test_a_coverage_gap_skips_exactly_the_gap_and_nothing_else() -> None:
    intervals = (
        TimeInterval(_utc("2025-11-28T14:30:00Z"), _utc("2025-11-28T14:46:00Z")),
        TimeInterval(_utc("2025-11-28T14:47:00Z"), _utc("2025-11-28T14:50:00Z")),
    )
    replay = _replay(intervals=intervals)

    assert replay.assessment.status is AvailabilityStatus.DEGRADED

    evaluations = replay.run()
    skipped = [item for item in evaluations if item.skip_reason is not None]

    assert [item.occurred_at for item in skipped] == [_utc("2025-11-28T14:46:00Z")]
    assert skipped[0].skip_reason is ReplaySkipReason.DATA_GAP_EVALUATION_SKIPPED
    assert skipped[0].decisions == ()
    assert skipped[0].candidates == ()
    # The instants either side of the gap still decide.
    decided = {item.occurred_at for item in evaluations if item.candidates}
    assert _utc("2025-11-28T14:45:00Z") in decided
    assert _utc("2025-11-28T14:47:00Z") in decided


def test_replay_refuses_to_start_when_the_dataset_is_unavailable() -> None:
    runtime = _runtime()
    plan = runtime.load(_document())
    unavailable = _assessment(
        intervals=(
            TimeInterval(_utc("2025-11-28T14:35:00Z"), _utc("2025-11-28T14:50:00Z")),
        )
    )

    assert unavailable.status is AvailabilityStatus.UNAVAILABLE

    with pytest.raises(ReplayUnavailableError) as failure:
        BasicPlanReplay(
            runtime=runtime,
            plan=plan,
            clock=_clock(_events()),
            assessment=unavailable,
        )

    assert failure.value.contract_fields["reason_code"] == "WARMUP_COVERAGE_MISSING"
    assert failure.value.contract_fields["missing_requirements"] == [
        f"{FIRST}|ADJUSTED_BAR|1m:WARMUP_COVERAGE_MISSING"
    ]


def test_events_outside_the_official_session_never_reach_the_plan() -> None:
    # The event clock refuses to build at all, so no replay can read them.
    with pytest.raises(Exception, match="outside an official regular session"):
        _clock(
            [
                bar_closed_event(
                    event_id="pre-market",
                    instrument_id=FIRST,
                    data_kind="ADJUSTED_BAR",
                    resolution="1m",
                    starts_at=_utc("2025-11-28T14:00:00Z"),
                    close=Decimal("100"),
                    volume=Decimal(1),
                    source_sequence=1,
                )
            ]
        )


def test_a_bar_available_before_it_closes_is_rejected_as_look_ahead() -> None:
    with pytest.raises(ValueError, match="available_at"):
        bar_closed_event(
            event_id="early",
            instrument_id=FIRST,
            data_kind="ADJUSTED_BAR",
            resolution="1m",
            starts_at=OPEN,
            close=Decimal("100"),
            volume=Decimal(1),
            source_sequence=1,
            available_at=OPEN + timedelta(seconds=30),
        )


# ---------------------------------------------------------------------------
# The gate execution_model consumes
# ---------------------------------------------------------------------------


def test_the_execution_gate_combines_the_session_and_the_availability_skip() -> None:
    intervals = (
        TimeInterval(_utc("2025-11-28T14:30:00Z"), _utc("2025-11-28T14:46:00Z")),
        TimeInterval(_utc("2025-11-28T14:47:00Z"), _utc("2025-11-28T14:50:00Z")),
    )
    gate = _replay(intervals=intervals).gate

    assert gate.is_fill_allowed(_utc("2025-11-28T14:45:00Z")) is True
    assert gate.is_fill_allowed(_utc("2025-11-28T14:46:30Z")) is False
    assert gate.is_fill_allowed(_utc("2025-11-28T14:47:00Z")) is True
    # Outside the regular session, no fill is possible whatever the data says.
    assert gate.is_fill_allowed(_utc("2025-11-28T18:00:00Z")) is False
    assert gate.is_fill_allowed(_utc("2025-11-28T13:00:00Z")) is False

    assert gate.session_status_at(_utc("2025-11-28T18:00:00Z")) is (
        MarketSessionStatus.POST_MARKET
    )
    assert gate.session_closes_at(_utc("2025-11-28T14:45:00Z")) == _utc(
        "2025-11-28T18:00:00Z"
    )
    assert gate.session_closes_at(_utc("2025-11-27T14:45:00Z")) is None
    assert gate.is_stage_allowed(
        SkipStage.ORDER_TRIGGER, _utc("2025-11-28T14:46:30Z")
    ) is False


def test_the_gate_is_stateless_and_does_not_move_the_replay_clock() -> None:
    replay = _replay()
    gate = replay.gate

    for _ in range(3):
        assert gate.is_fill_allowed(_utc("2025-11-28T14:40:00Z")) is True
        assert gate.is_fill_allowed(_utc("2025-11-28T14:35:00Z")) is True

    assert len(replay.run()) == 20


# ---------------------------------------------------------------------------
# Root #202: one container per trade side
# ---------------------------------------------------------------------------


def _two_container_document() -> dict[str, Any]:
    """B's plan reshaped as version 2: a buy container and a sell container.

    Built from the pinned version 1 fixture rather than hand-written, so the only
    difference under test is where the steps live. The buy container keeps the
    fixture's chain; the sell container reads the same feature and sells above 70,
    which is the ordinary shape a user writes.
    """
    document = copy.deepcopy(_document())
    steps = copy.deepcopy(document.pop("steps"))
    document["schemaVersion"] = "basic-compiled-plan.v2"

    sell_steps = copy.deepcopy(steps)
    for step in sell_steps:
        if step["operation"] == "COMPARE":
            step["arguments"] = {"operator": "GT", "threshold": "70"}
        if step["operation"] == "EMIT_ORDER_CANDIDATE":
            step["arguments"] = dict(step["arguments"], side="SELL")

    partition = document["executionSnapshot"]["partitions"][0]
    buy_flow = partition["flows"][0]
    buy_flow["steps"] = steps
    sell_flow = copy.deepcopy(buy_flow)
    sell_flow["key"] = buy_flow["key"] + "-sell"
    sell_flow["steps"] = sell_steps
    partition["flows"] = [buy_flow, sell_flow]

    document["planChecksum"] = compute_compiled_plan_checksum(document)
    return document


def test_loads_one_container_per_side_from_a_version_two_plan() -> None:
    """The ordinary Basic strategy: version 1 had no shape for it at all."""
    plan = _runtime().load(_two_container_document())

    assert plan.schema_version == "basic-compiled-plan.v2"
    assert [flow.side for flow in plan.flows] == ["BUY", "SELL"]
    # Each container keeps its own chain, which is what makes the two sides differ.
    assert all(flow.condition_steps for flow in plan.flows)
    buy, sell = plan.flows
    assert buy.condition_steps != sell.condition_steps
    assert sell.condition_steps[-1].arguments["operator"] == "GT"


def test_a_version_two_plan_missing_per_flow_steps_is_refused() -> None:
    """The version says the steps are per container; there is no fallback."""
    document = copy.deepcopy(_document())
    document["schemaVersion"] = "basic-compiled-plan.v2"
    document["planChecksum"] = compute_compiled_plan_checksum(document)

    with pytest.raises(BasicPlanCompatibilityError):
        _runtime().load(document)


def test_an_unknown_plan_schema_version_is_refused_rather_than_guessed() -> None:
    """A plan checked against the wrong schema fails its checksum, which reads
    like tampering. Naming the version explicitly keeps the error honest."""
    document = copy.deepcopy(_document())
    document["schemaVersion"] = "basic-compiled-plan.v9"
    document["planChecksum"] = compute_compiled_plan_checksum(document)

    with pytest.raises(BasicPlanCompatibilityError, match="schemaVersion"):
        _runtime().load(document)
