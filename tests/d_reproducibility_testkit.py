"""D30 fixture data for the reproducibility end-to-end test.

Spec section 4 says the orchestration this module used to carry belongs in
``src/``. It now does -- :mod:`backtest_engine.orchestrator` assembles and
replays a run, and :mod:`backtest_engine.wiring` binds it to the execution
model, the object store and PostgreSQL -- so what is left here is what the spec
asks for: fixture data and the small builders that render it.

Nothing in this module computes a result, and nothing in it may be used as an
oracle. The builders below produce *inputs*: a pinned bar series, the
``market-data.v1`` manifest that describes it, B's ``strategy-bot.v1`` request
that names it, and the pinned execution policy the run is measured under. Every
expected digest is a literal in the test that asserts it.

Identifier discipline
---------------------
Every cross-domain id is one the canonical reference seed
(``db/migration-contributions/fixtures/backtest_reference_seed.sql.fixture``)
actually inserts, so a run built from this fixture satisfies every foreign key
in ``backtest.runs`` against a real upstream row. The one exception is the
plan's instrument, which comes from B's published compiled-plan fixture and has
no ``backtest`` foreign key.
"""

from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from backtest_engine.contracts import (
    OFFICIAL_BACKTEST_MESSAGE_TYPE,
    STRATEGY_BOT_CONTRACT_VERSION,
    canonical_dataset_hash,
    compute_message_idempotency_key,
    official_backtest_operation_key,
    validate_dataset_manifest,
)
from backtest_engine.execution_model import (
    EXECUTION_MICROSTRUCTURE_RULES_VERSION,
    ExecutionMicrostructurePolicy,
    InstrumentFractionalPolicy,
    RiskLimits,
)
from backtest_engine.execution_policy import ExecutionPolicy, et_quarter_start
from backtest_engine.money import PRECISION_RULES_VERSION


ET = ZoneInfo("America/New_York")

FIXTURES = Path(__file__).parent / "fixtures/contracts/strategy-bot/v1"

# -- reference-seed identifiers -------------------------------------------------
# `backtest.runs` has foreign keys to all four of these.
ACCOUNT_ID = UUID("00000000-0000-4000-8000-0000000000a1")
BOT_ID = UUID("00000000-0000-4000-8000-0000000000b1")
FEE_POLICY_ID = UUID("00000000-0000-4000-8000-0000000000f1")
BUFFER_POLICY_ID = UUID("00000000-0000-4000-8000-0000000000f2")
#: `market_data.dataset_manifests`, referenced by `backtest.input_datasets`.
DATASET_MANIFEST_ID = UUID("00000000-0000-4000-8000-0000000000d1")

#: The seeded `trading.buying_power_buffer_policy_versions.buffer_bps`. The
#: microstructure policy below repeats it rather than choosing its own, so the
#: run's arithmetic and the row it cites cannot disagree.
BUFFER_BPS = 50

#: The single official instrument of B's published compiled plan.
INSTRUMENT_ID = "00000000-0000-4000-8000-000000000301"
PROVIDER_SYMBOL = "AAPL"

#: Correlation id of B's published request fixture.
CORRELATION_ID = "00000000-0000-4000-8000-000000000202"
MESSAGE_ID = "00000000-0000-4000-8000-000000000213"
RUN_ID = "76a6a20c-0651-5748-8187-6bf0ae155194"
FEATURE_MATERIALIZATION_ID = "00000000-0000-4000-8000-000000000204"
FEATURE_RESULT_HASH = "sha256:" + "6" * 64

DATASET_ID = "00000000-0000-4000-8000-0000000000d2"
STORAGE_OBJECT_ID = "00000000-0000-4000-8000-0000000000d3"

MARKET_DATA_SCHEMA_VERSION = "market-bars-v2"
OBJECT_KEY = "market-data/adjusted-bars-2024-01-02.parquet"

# -- the pinned session ---------------------------------------------------------
SESSION_DATE = date(2024, 1, 2)
#: 09:30 ET on the first XNYS session of 2024-Q1.
FIRST_BAR_START = datetime(2024, 1, 2, 14, 30, tzinfo=UTC)
BAR = timedelta(minutes=1)
BAR_COUNT = 20

#: RSI_14 reads the last 15 closes, so the fifteenth bar is the first instant at
#: which the plan's ``COMPARE LT 30`` can decide anything at all. Fourteen
#: consecutive one-point falls put RSI at exactly 0, and the jump to 130 pulls
#: every later window back above the threshold: the series is shaped so the plan
#: emits exactly one candidate, at 14:45:00Z.
CLOSES: tuple[str, ...] = (
    "114", "113", "112", "111", "110", "109", "108", "107", "106", "105",
    "104", "103", "102", "101", "100",
    "130", "131", "132", "133", "134",
)
#: One tenth of this is the per-bar fill capacity (D23 volume participation).
BAR_VOLUME = 20_000

#: The instant the single candidate is decided, and the instant it fills.
DECISION_INSTANT = FIRST_BAR_START + BAR * 15
FILL_INSTANT = FIRST_BAR_START + BAR * 16

#: Wall-clock completion instant. Pinned because `result_hash` covers
#: ``calculated_at``; the replay clock is 2024 market time and is untouched by it.
COMPLETED_AT = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)


_SCHEMA = pa.schema(
    [
        pa.field("instrument_id", pa.string(), nullable=False),
        pa.field("provider_symbol", pa.string(), nullable=False),
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("session_date_et", pa.date32(), nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.int64(), nullable=False),
    ],
    metadata={b"schema_version": MARKET_DATA_SCHEMA_VERSION.encode()},
)


# ==========================================================================
# Execution policy and the D23 policies the model needs alongside it
# ==========================================================================

#: The pinned policy for this fixture. It differs from
#: `D17_EXECUTION_POLICY_FIXTURE` in exactly one respect: the fee and buffer
#: policy ids are the seeded `trading.*` rows, because `backtest.runs` has a
#: foreign key to both and the D17 ids are not in any database.
E2E_EXECUTION_POLICY = ExecutionPolicy(
    version="official-backtest-policy-e2e-2024q1",
    release_quarter="2024-Q1",
    period_start=et_quarter_start(2024, 1),
    period_end=et_quarter_start(2024, 2),
    fee_rate=Decimal("0.002"),
    slippage_rate_bps=5,
    timezone="America/New_York",
    session_calendar="XNYS",
    timestamp_unit="us",
    price_arrow_type="double",
    volume_arrow_type="int64",
    market_data_schema_version=MARKET_DATA_SCHEMA_VERSION,
    calculation_model_version="backtest-calculation-v1",
    market_rules_version="market:1.0.0",
    accounting_rules_version="accounting:1.0.0",
    fee_policy_id=str(FEE_POLICY_ID),
    buying_power_buffer_policy_id=str(BUFFER_POLICY_ID),
    good_till_cancelled_horizon=timedelta(days=90),
    max_order_horizon=timedelta(days=90),
    precision_rules_version=PRECISION_RULES_VERSION,
)

E2E_MICROSTRUCTURE = ExecutionMicrostructurePolicy(
    version=EXECUTION_MICROSTRUCTURE_RULES_VERSION,
    max_volume_participation_bps=1000,
    buying_power_buffer_policy_id=str(BUFFER_POLICY_ID),
    buying_power_buffer_bps=BUFFER_BPS,
)

#: No instrument in this fixture is fractional-eligible, stated rather than
#: assumed: `InstrumentFractionalPolicy` has no default set.
E2E_FRACTIONAL_POLICY = InstrumentFractionalPolicy(
    policy_version="fractional:e2e:1.0.0", fractional_instrument_ids=frozenset()
)

#: Deliberately far above anything this fixture can reach, so a risk rejection
#: in a test is a real finding and not the fixture's own ceiling.
E2E_RISK_LIMITS = RiskLimits(
    max_strategy_notional=Decimal("1000000.00000000"),
    max_gross_exposure=Decimal("1000000.00000000"),
    max_instrument_exposure=Decimal("1000000.00000000"),
)


# ==========================================================================
# Market data
# ==========================================================================


@dataclass(frozen=True, slots=True)
class MarketDataFixture:
    """One pinned Parquet object and the manifest that describes it."""

    root: Path
    path: Path
    parquet_bytes: bytes
    manifest: dict[str, Any]

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(self.parquet_bytes).hexdigest()


def bar_rows(closes: Sequence[str] = CLOSES) -> list[dict[str, Any]]:
    """One row per one-minute bar, OHLC walked from the previous close."""
    rows: list[dict[str, Any]] = []
    previous = Decimal(closes[0])
    for index, close_text in enumerate(closes):
        close = Decimal(close_text)
        starts_at = FIRST_BAR_START + BAR * index
        rows.append(
            {
                "instrument_id": INSTRUMENT_ID,
                "provider_symbol": PROVIDER_SYMBOL,
                "bar_start_at": starts_at,
                "session_date_et": starts_at.astimezone(ET).date(),
                "open": float(previous),
                "high": float(max(previous, close)),
                "low": float(min(previous, close)),
                "close": float(close),
                "volume": BAR_VOLUME,
            }
        )
        previous = close
    return rows


def market_bars_parquet(closes: Sequence[str] = CLOSES) -> bytes:
    """Deterministic UNCOMPRESSED Parquet bytes for the pinned bar series."""
    table = pa.Table.from_pylist(bar_rows(closes), schema=_SCHEMA)
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="none",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
        row_group_size=len(closes),
    )
    return bytes(sink.getvalue().to_pybytes())


def dataset_manifest(content_hash: str, *, row_count: int, coverage_end: datetime) -> dict[str, Any]:
    """The ``market-data.v1`` manifest a producer would publish for that object.

    ``period_start``/``period_end`` at manifest level are the *dataset's* window,
    which `ParquetMarketDataReader` requires to equal the execution policy's
    pinned quarter. The object's own period is the coverage actually delivered,
    and that is what decides the run's evaluation window.
    """
    metadata: dict[str, Any] = {
        "storage_object_id": STORAGE_OBJECT_ID,
        "object_key": OBJECT_KEY,
        "content_hash": content_hash,
        "object_kind": "PARQUET",
        "partition_granularity": "DAY",
        "partition_start": SESSION_DATE.isoformat(),
        "partition_end": (SESSION_DATE + timedelta(days=1)).isoformat(),
        "period_start": _iso(FIRST_BAR_START),
        "period_end": _iso(coverage_end),
        "shard_key": "s00-of-01",
        "part_number": 1,
        "row_count": row_count,
        "schema_version": MARKET_DATA_SCHEMA_VERSION,
    }
    manifest: dict[str, Any] = {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": str(DATASET_MANIFEST_ID),
        "dataset_id": DATASET_ID,
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": canonical_dataset_hash([metadata]),
        "schema_id": MARKET_DATA_SCHEMA_VERSION,
        "period_start": _iso(E2E_EXECUTION_POLICY.period_start),
        "period_end": _iso(E2E_EXECUTION_POLICY.period_end),
        "available_at": "2024-01-03T01:00:00Z",
        "objects": [metadata],
    }
    validate_dataset_manifest(manifest)
    return manifest


def write_market_data(root: Path, closes: Sequence[str] = CLOSES) -> MarketDataFixture:
    """Materialise the pinned object under ``root`` and describe it."""
    parquet_bytes = market_bars_parquet(closes)
    path = root / OBJECT_KEY
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(parquet_bytes)
    manifest = dataset_manifest(
        hashlib.sha256(parquet_bytes).hexdigest(),
        row_count=len(closes),
        coverage_end=FIRST_BAR_START + BAR * len(closes),
    )
    return MarketDataFixture(root=root, path=path, parquet_bytes=parquet_bytes, manifest=manifest)


# ==========================================================================
# B's contracts
# ==========================================================================


def compiled_plan() -> dict[str, Any]:
    """B's published ``basic-compiled-plan``, consumed unmodified."""
    return json.loads((FIXTURES / "basic-compiled-plan.valid.json").read_text(encoding="utf-8"))


def official_backtest_request(
    *,
    plan: Mapping[str, Any] | None = None,
    occurred_at: str = "2024-01-03T01:01:00Z",
    expected_dataset_hash: str | None = None,
) -> dict[str, Any]:
    """B's ``OFFICIAL_BACKTEST_REQUESTED``, re-addressed to the seeded bot.

    The identifiers and execution window address the seeded 2024-Q1 stack rather
    than B's published 2026-Q3 example. The ``idempotencyKey`` is B's own
    canonical material for *this* message -- the same function B's
    ``StrategyBotContractFixtures`` uses -- because an unchanged key copied from
    a different message would be a forgery, not a fixture.
    """
    document = plan if plan is not None else compiled_plan()
    if expected_dataset_hash is None:
        parquet_bytes = market_bars_parquet()
        manifest = dataset_manifest(
            hashlib.sha256(parquet_bytes).hexdigest(),
            row_count=BAR_COUNT,
            coverage_end=FIRST_BAR_START + BAR * BAR_COUNT,
        )
        expected_dataset_hash = f"sha256:{manifest['dataset_hash']}"
    request: dict[str, Any] = {
        "metadata": {
            "contractVersion": STRATEGY_BOT_CONTRACT_VERSION,
            "messageType": OFFICIAL_BACKTEST_MESSAGE_TYPE,
            "messageId": MESSAGE_ID,
            "occurredAt": occurred_at,
            "correlationId": CORRELATION_ID,
            "idempotencyKey": "",
        },
        "runId": RUN_ID,
        "lane": "BASIC",
        "aggregateSequence": 1,
        "botId": str(BOT_ID),
        "expectedSnapshotHash": document["executionSnapshot"]["immutableStrategyVersion"][
            "snapshotHash"
        ],
        "compiledPlanChecksum": document["planChecksum"],
        "datasetManifestId": str(DATASET_MANIFEST_ID),
        "expectedDatasetHash": expected_dataset_hash,
        "periodStart": E2E_EXECUTION_POLICY.period_start.astimezone(ET).date().isoformat(),
        "periodEnd": E2E_EXECUTION_POLICY.period_end.astimezone(ET).date().isoformat(),
        "assumptionsVersion": "accounting:1.0.0",
        "executionPolicyVersion": E2E_EXECUTION_POLICY.version,
        "requestReason": "STRATEGY_RELEASE",
        "featureMaterializations": [
            {
                "featureMaterializationId": FEATURE_MATERIALIZATION_ID,
                "lockedResultHash": FEATURE_RESULT_HASH,
            }
        ],
    }
    request["metadata"]["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type=OFFICIAL_BACKTEST_MESSAGE_TYPE,
        aggregate_id=request["botId"],
        snapshot_hash=request["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(request),
    )
    request["requestHash"] = "sha256:" + hashlib.sha256(
        json.dumps(request, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return request


def policy_with(**overrides: Any) -> ExecutionPolicy:
    """A variant of the pinned policy, for tests that move one input."""
    return replace(E2E_EXECUTION_POLICY, **overrides)


def plan_with_budget_cap(budget_cap_bps: int) -> dict[str, Any]:
    """B's plan with one partition budget cap changed and the checksum re-sealed."""
    from backtest_engine.contracts import compute_compiled_plan_checksum

    document = copy.deepcopy(compiled_plan())
    document["executionSnapshot"]["partitions"][0]["budgetCapBps"] = budget_cap_bps
    document["planChecksum"] = compute_compiled_plan_checksum(document)
    return document


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
