"""Independent D30 fixture kit spanning deterministic pipeline and backtest boundaries."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from backtest_engine.attempt_coordinator import (
    AttemptCoordinator,
    AttemptPolicy,
    ResourceSample,
    RunState,
)
from backtest_engine.contracts import (
    compute_input_bundle_fingerprint,
    validate_backtest_request,
    validate_dataset_manifest,
)
from backtest_engine.detail_object_manifest import (
    DetailObjectBuilder,
    DetailObjectBundle,
    PerformancePoint,
    ReplayLedgerDetail,
)
from backtest_engine.execution_model import (
    BacktestExecutionModel,
    MinuteBar,
    OrderRequest,
    OrderSide,
    OrderType,
    QuantityMode,
    RiskLimits,
    TimeInForce,
)
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE
from backtest_engine.market_data import ParquetMarketDataReader
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultSnapshot,
    ResultSnapshotBuilder,
    RunSnapshot,
    fill_result_record,
    order_result_record,
)


INSTRUMENT_ID = "11111111-1111-4111-8111-111111111111"
DATASET_ID = "22222222-2222-4222-8222-222222222222"
MANIFEST_ID = "33333333-3333-4333-8333-333333333333"
STORAGE_OBJECT_ID = "44444444-4444-4444-8444-444444444444"
BACKTEST_RUN_ID = "55555555-5555-4555-8555-555555555555"
STRATEGY_VERSION_ID = "66666666-6666-4666-8666-666666666666"
ORDER_ID = "77777777-7777-4777-8777-777777777777"
PERFORMANCE_POINT_ID = "88888888-8888-4888-8888-888888888888"
BACKTEST_MEMORY_BYTES = 512 * 1024 * 1024
ET = ZoneInfo("America/New_York")

FIXED_ALPACA_RESPONSE: tuple[Mapping[str, object], ...] = (
    {
        "t": "2024-01-02T14:30:00Z",
        "o": 100.0,
        "h": 102.0,
        "l": 99.0,
        "c": 101.0,
        "v": 1000,
    },
    {
        "t": "2024-01-02T15:00:00Z",
        "o": 101.0,
        "h": 103.0,
        "l": 100.0,
        "c": 102.0,
        "v": 1200,
    },
)


class PipelineResourceLimitExceeded(RuntimeError):
    """Raised before a fixture object is published beyond its injected budget."""


@dataclass(frozen=True, slots=True)
class PipelineComputePolicy:
    max_input_rows: int
    max_output_bytes: int

    def __post_init__(self) -> None:
        if self.max_input_rows <= 0 or self.max_output_bytes <= 0:
            raise ValueError("pipeline compute limits must be positive")


@dataclass(frozen=True, slots=True)
class PipelineInput:
    object_path: Path
    parquet_bytes: bytes
    manifest: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class BacktestComputeSample:
    cpu_time: timedelta
    memory_bytes: int


@dataclass(frozen=True, slots=True)
class OfficialBacktestOutcome:
    request: Mapping[str, Any]
    result: ResultSnapshot
    details: DetailObjectBundle
    attempt_completed: bool


def _utc(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("Alpaca timestamp must be UTC with a Z suffix")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("Alpaca timestamp must be UTC")
    return parsed


def _schema() -> pa.Schema:
    return pa.schema(
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
        metadata={b"schema_version": b"market-bars-v2"},
    )


def _row(payload: Mapping[str, object]) -> dict[str, object]:
    timestamp = _utc(payload.get("t"))
    return {
        "instrument_id": INSTRUMENT_ID,
        "provider_symbol": "AAPL",
        "bar_start_at": timestamp,
        "session_date_et": timestamp.astimezone(ET).date(),
        "open": payload.get("o"),
        "high": payload.get("h"),
        "low": payload.get("l"),
        "close": payload.get("c"),
        "volume": payload.get("v"),
    }


def _dataset_hash(objects: Sequence[Mapping[str, Any]]) -> str:
    fields = (
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
    rows = [{field: item.get(field) for field in fields} for item in objects]
    rows.sort(
        key=lambda item: json.dumps(
            item, sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )
    )
    payload = json.dumps(
        rows, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def materialize_fixed_alpaca_response(
    response: Sequence[Mapping[str, object]],
    target: Path,
    policy: PipelineComputePolicy,
) -> PipelineInput:
    if len(response) > policy.max_input_rows:
        raise PipelineResourceLimitExceeded("pipeline input row limit exceeded")
    rows = sorted(
        (_row(item) for item in response),
        key=lambda item: item["bar_start_at"],
    )
    table = pa.Table.from_pylist(rows, schema=_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        version="2.6",
        use_dictionary=False,
        write_statistics=True,
    )
    parquet_bytes = sink.getvalue().to_pybytes()
    if len(parquet_bytes) > policy.max_output_bytes:
        raise PipelineResourceLimitExceeded("pipeline output byte limit exceeded")

    content_hash = hashlib.sha256(parquet_bytes).hexdigest()
    object_metadata: dict[str, Any] = {
        "storage_object_id": STORAGE_OBJECT_ID,
        "object_key": target.name,
        "content_hash": content_hash,
        "object_kind": "PARQUET",
        "partition_granularity": "DAY",
        "partition_start": "2024-01-02",
        "partition_end": "2024-01-03",
        "period_start": "2024-01-02T14:30:00Z",
        "period_end": "2024-01-02T15:30:00Z",
        "shard_key": "s00-of-01",
        "part_number": 1,
        "row_count": table.num_rows,
        "schema_version": "market-bars-v2",
    }
    manifest: dict[str, Any] = {
        "contract_id": "com06.dataset-manifest",
        "schema_version": 1,
        "manifest_id": MANIFEST_ID,
        "dataset_id": DATASET_ID,
        "revision": 1,
        "status": "AVAILABLE",
        "dataset_hash": _dataset_hash([object_metadata]),
        "schema_id": "market-bars-v2",
        "period_start": "2024-01-02T14:30:00Z",
        "period_end": "2024-01-02T15:30:00Z",
        "available_at": "2024-01-03T01:00:00Z",
        "objects": [object_metadata],
    }
    validate_dataset_manifest(manifest)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(parquet_bytes)
    return PipelineInput(target, parquet_bytes, manifest)


def _request(manifest: Mapping[str, Any]) -> dict[str, Any]:
    request: dict[str, Any] = {
        "contract_id": "com06.backtest-request",
        "schema_version": 1,
        "message_id": "99999999-9999-4999-8999-999999999999",
        "occurred_at": "2024-01-03T01:01:00Z",
        "correlation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "idempotency_key": "d30-fixed-official-backtest",
        "event_type": "BACKTEST_REQUESTED",
        "backtest_run_id": BACKTEST_RUN_ID,
        "strategy_version_id": STRATEGY_VERSION_ID,
        "strategy_snapshot_hash": "a" * 64,
        "compiled_plan_hash": "b" * 64,
        "dataset_manifest_id": manifest["manifest_id"],
        "dataset_hash": manifest["dataset_hash"],
        "feature_materialization_version": "feature-materialization-v1",
        "execution_policy_version": D17_EXECUTION_POLICY_FIXTURE.version,
        "requested_at": "2024-01-03T01:01:00Z",
    }
    request["input_bundle_fingerprint"] = compute_input_bundle_fingerprint(request)
    validate_backtest_request(request)
    return request


def _minute_bar(row: Mapping[str, object]) -> MinuteBar:
    starts_at = row["bar_start_at"]
    assert isinstance(starts_at, datetime)
    return MinuteBar(
        instrument_id=str(row["instrument_id"]),
        starts_at=starts_at,
        ends_at=starts_at + timedelta(minutes=1),
        open=Decimal(str(row["open"])),
        high=Decimal(str(row["high"])),
        low=Decimal(str(row["low"])),
        close=Decimal(str(row["close"])),
        volume=Decimal(str(row["volume"])),
    )


def run_official_backtest(
    pipeline_input: PipelineInput,
    *,
    compute_sample: BacktestComputeSample | None = None,
) -> OfficialBacktestOutcome:
    request = _request(pipeline_input.manifest)
    table = ParquetMarketDataReader(pipeline_input.object_path.parent).read(
        pipeline_input.manifest,
        D17_EXECUTION_POLICY_FIXTURE,
    )

    attempt_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    coordinator = AttemptCoordinator(
        BACKTEST_RUN_ID,
        AttemptPolicy(
            max_attempts=2,
            lease_duration=timedelta(minutes=1),
            attempt_timeout=timedelta(minutes=10),
            max_cpu_time=timedelta(minutes=5),
            max_memory_bytes=BACKTEST_MEMORY_BYTES,
        ),
        attempt_time,
    )
    lease = coordinator.acquire("d30-worker", attempt_time)
    sample = compute_sample or BacktestComputeSample(
        cpu_time=timedelta(seconds=1), memory_bytes=64 * 1024 * 1024
    )
    coordinator.heartbeat(
        lease,
        attempt_time + timedelta(seconds=1),
        ResourceSample(sample.cpu_time, sample.memory_bytes),
    )

    run = RunSnapshot(
        backtest_run_id=BACKTEST_RUN_ID,
        strategy_version_id=STRATEGY_VERSION_ID,
        input_bundle_fingerprint=str(request["input_bundle_fingerprint"]),
        calculation_model_version=(
            D17_EXECUTION_POLICY_FIXTURE.calculation_model_version
        ),
        cost_model_version="official-cost-v1",
        execution_model_version="official-execution-v1",
        initial_cash=Decimal("10000"),
    )
    model = BacktestExecutionModel(
        D17_EXECUTION_POLICY_FIXTURE,
        run.initial_cash,
        RiskLimits(Decimal("10000"), Decimal("10000"), Decimal("10000")),
    )
    rows = table.to_pylist()
    accepted = model.submit(
        OrderRequest(
            order_id=ORDER_ID,
            instrument_id=INSTRUMENT_ID,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10"),
            quantity_mode=QuantityMode.WHOLE_SHARES,
            time_in_force=TimeInForce.DAY,
            submitted_at=rows[0]["bar_start_at"],
            eligible_at=rows[1]["bar_start_at"],
            day_expires_at=D17_EXECUTION_POLICY_FIXTURE.period_end,
            reference_price=Decimal(str(rows[0]["close"])),
        )
    )
    records = [
        order_result_record(run, accepted, accepted.submitted_at, model.cash, ())
    ]
    fills = model.process_bars([_minute_bar(row) for row in rows])
    position = model.position(INSTRUMENT_ID)
    positions = (
        PositionAfter(position.instrument_id, position.quantity, position.cost_basis),
    )
    records.append(
        fill_result_record(
            run,
            fills[0],
            model.order(ORDER_ID),
            model.cash,
            positions,
        )
    )
    completed_at = datetime(2024, 1, 2, 15, 5, tzinfo=timezone.utc)
    result = ResultSnapshotBuilder().build(run, records, completed_at)
    last_close = Decimal(str(rows[-1]["close"]))
    equity = model.cash + position.quantity * last_close
    details = DetailObjectBuilder().build(
        result,
        [
            ReplayLedgerDetail(run.snapshot_id, transaction)
            for transaction in model.ledger_transactions
        ],
        [
            PerformancePoint(
                PERFORMANCE_POINT_ID,
                run.snapshot_id,
                datetime(2024, 1, 2, 15, 1, tzinfo=timezone.utc),
                "equity",
                equity,
                None,
            )
        ],
        completed_at,
    )
    coordinator.complete(
        lease,
        attempt_time + timedelta(seconds=2),
        result.manifest.result_manifest_id,
        details.manifest.detail_manifest_id,
    )
    return OfficialBacktestOutcome(
        request=request,
        result=result,
        details=details,
        attempt_completed=coordinator.state is RunState.COMPLETE,
    )
