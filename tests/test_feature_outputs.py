from __future__ import annotations

import copy
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from backtest_engine.attempt_coordinator import AttemptPolicy
from backtest_engine.basic_runtime import BasicDecisionStatus, BasicPlanRuntime
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.contracts import compute_compiled_plan_checksum
from backtest_engine.elements import (
    ElementInputMissing,
    InstrumentInput,
    InstrumentSeries,
    PinnedFeatureSeries,
    PinnedFeatureValue,
    SeriesBar,
)
from backtest_engine.execution_policy import ExecutionPolicyCatalog
from backtest_engine.feature_outputs import (
    FEATURE_SERIES_SCHEMA,
    FeatureOutputBindingError,
    resolve_feature_materialization_pins,
)
from backtest_engine.lifecycle import StaticCompiledPlanSource, StaticDatasetManifestSource
from backtest_engine.production import S3VersionedFeatureObjectReader
from backtest_engine.wiring import (
    FeatureMaterializationPin,
    JobEnvelope,
    JobNotSatisfiable,
    OrchestratorJobHandler,
)
from backtest_engine.worker import JobContext
from d_reproducibility_testkit import (
    BAR,
    CLOSES,
    DATASET_MANIFEST_ID,
    E2E_EXECUTION_POLICY,
    E2E_FRACTIONAL_POLICY,
    E2E_MICROSTRUCTURE,
    E2E_RISK_LIMITS,
    FIRST_BAR_START,
    INSTRUMENT_ID,
    compiled_plan,
    dataset_manifest,
    market_bars_parquet,
)


OFFICIAL_RSI_ID = "0f1b0000-0000-4000-8000-000000000001"
MATERIALIZATION_ID = uuid.UUID("10000000-0000-4000-8000-000000000001")
DEFINITION_HASH = "1" * 64
INPUT_HASH = "2" * 64
BUCKET = "feature-data"
KEY = "features/rsi.parquet"
VERSION = "version-1"
EVALUATION_FROM = FIRST_BAR_START + BAR * 15
EVALUATION_THROUGH = FIRST_BAR_START + BAR * len(CLOSES)
PERIOD_START = FIRST_BAR_START
PERIOD_END = EVALUATION_THROUGH


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _canonical_hash(rows: list[dict[str, str]]) -> str:
    payload = {
        "definition_hash": DEFINITION_HASH,
        "input_dataset_set_hash": INPUT_HASH,
        "instrument_id": INSTRUMENT_ID,
        "period_end": _utc_text(PERIOD_END),
        "period_start": _utc_text(PERIOD_START),
        "result_schema_version": 1,
        "rows": rows,
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _rows() -> list[dict[str, str]]:
    return [
        {
            "at": _utc_text(FIRST_BAR_START + BAR * index),
            "value": value,
        }
        for index, value in enumerate(
            (
                "0.00000000",
                "68.18181818",
                "70.45454545",
                "72.72727273",
                "75.00000000",
                "77.27272727",
            ),
            start=14,
        )
    ]


def _parquet(rows: list[dict[str, str]] | None = None, *, schema: pa.Schema = FEATURE_SERIES_SCHEMA) -> bytes:
    rows = _rows() if rows is None else rows
    table = pa.Table.from_arrays(
        [
            pa.array(
                [datetime.fromisoformat(item["at"].replace("Z", "+00:00")) for item in rows],
                type=schema.field(0).type,
            ),
            pa.array(
                [
                    Decimal(item["value"]) if pa.types.is_decimal(schema.field(1).type) else float(item["value"])
                    for item in rows
                ],
                type=schema.field(1).type,
            ),
        ],
        schema=schema,
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd", use_dictionary=False, write_statistics=False)
    return sink.getvalue().to_pybytes()


def _plan():
    return BasicPlanRuntime().load(_plan_document())


def _plan_document() -> dict[str, Any]:
    document = copy.deepcopy(compiled_plan())
    document["requiredFeatures"][0]["featureId"] = OFFICIAL_RSI_ID
    document["planChecksum"] = compute_compiled_plan_checksum(document)
    return document


def _record(body: bytes | None = None, **changes: Any) -> dict[str, Any]:
    body = _parquet() if body is None else body
    result_hash = _canonical_hash(_rows())
    record = {
        "id": MATERIALIZATION_ID,
        "status": "SUCCEEDED",
        "result_hash": result_hash,
        "feature_definition_id": uuid.UUID(OFFICIAL_RSI_ID),
        "feature_code": "RSI_14",
        "calculator_version": "1.0.0",
        "resolution": "1m",
        "definition_hash": DEFINITION_HASH,
        "instrument_id": uuid.UUID(INSTRUMENT_ID),
        "input_dataset_set_hash": INPUT_HASH,
        "period_start": PERIOD_START,
        "period_end": PERIOD_END,
        "output_dataset_manifest_id": uuid.UUID("30000000-0000-4000-8000-000000000001"),
        "output_dataset_status": "AVAILABLE",
        "output_dataset_layer": "DERIVED",
        "output_dataset_instrument_id": uuid.UUID(INSTRUMENT_ID),
        "output_dataset_schema": "feature-series.parquet.v1",
        "output_dataset_resolution": "1m",
        "objects": (
            {
                "object_kind": "FEATURE_SERIES",
                "status": "AVAILABLE",
                "storage_provider": "S3_COMPATIBLE",
                "bucket_name": BUCKET,
                "object_key": KEY,
                "provider_version_id": VERSION,
                "content_hash": hashlib.sha256(body).hexdigest(),
                "byte_size": len(body),
                "file_format": "PARQUET",
                "schema_version": "feature-series.parquet.v1",
                "row_count": len(_rows()),
            },
        ),
    }
    record.update(changes)
    return record


class Source:
    def __init__(self, records: dict[uuid.UUID, dict[str, Any]]) -> None:
        self.records = records

    def by_id(self, materialization_id: uuid.UUID) -> dict[str, Any] | None:
        return self.records.get(materialization_id)


class Reader:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls: list[tuple[str, str, str, str]] = []

    def read_version(
        self, provider: str, bucket: str, key: str, version_id: str
    ) -> bytes:
        self.calls.append((provider, bucket, key, version_id))
        return self.body


def _resolve(
    *,
    pins: tuple[FeatureMaterializationPin, ...] | None = None,
    records: dict[uuid.UUID, dict[str, Any]] | None = None,
    body: bytes | None = None,
):
    body = _parquet() if body is None else body
    record = _record(body)
    records = {MATERIALIZATION_ID: record} if records is None else records
    pinned_record = records.get(MATERIALIZATION_ID, record)
    pins = pins or (
        FeatureMaterializationPin(
            materialization_id=MATERIALIZATION_ID,
            locked_result_hash="sha256:" + str(pinned_record["result_hash"]),
        ),
    )
    reader = Reader(body)
    resolved = resolve_feature_materialization_pins(
        plan=_plan(),
        pins=pins,
        source=Source(records),
        reader=reader,
        evaluation_from=EVALUATION_FROM,
        evaluation_through=EVALUATION_THROUGH,
    )
    return resolved, reader


def test_official_rsi_catalog_id_loads_as_the_production_feature() -> None:
    plan = _plan()

    assert plan.required_features[0].feature_id == OFFICIAL_RSI_ID
    assert plan.required_features[0].feature_key == "RSI_14"
    assert plan.required_features[0].definition_version == "rsi:1.0.0"


def test_provider_calculator_semantic_version_matches_compiled_feature_version() -> None:
    body = _parquet()
    record = _record(body, calculator_version="1.0.0")

    resolved, _reader = _resolve(records={MATERIALIZATION_ID: record}, body=body)

    assert resolved[0].feature_id == "RSI_14"


def test_exact_pin_is_read_from_the_named_object_version_and_decoded() -> None:
    resolved, reader = _resolve()

    assert reader.calls == [("S3_COMPATIBLE", BUCKET, KEY, VERSION)]
    assert len(resolved) == 1
    series = resolved[0]
    assert series.feature_id == "RSI_14"
    assert series.instrument_id == INSTRUMENT_ID
    assert series.resolution == "1m"
    assert series.value_at(EVALUATION_FROM) == Decimal("0.00000000")
    assert series.value_at(EVALUATION_FROM + BAR) == Decimal("68.18181818")


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda record: record.update(status="FAILED"), "SUCCEEDED"),
        (lambda record: record.update(calculator_version="rsi:2.0.0"), "semantic version"),
        (lambda record: record.update(resolution="15m"), "resolution"),
        (lambda record: record.update(period_start=PERIOD_START + BAR), "warm-up"),
        (lambda record: record.update(period_end=PERIOD_END - BAR), "evaluation window"),
        (lambda record: record.update(output_dataset_status="BUILDING"), "AVAILABLE"),
        (lambda record: record.update(output_dataset_layer="RAW"), "DERIVED"),
        (
            lambda record: record.update(output_dataset_instrument_id=uuid.uuid4()),
            "dataset instrument",
        ),
        (lambda record: record.update(output_dataset_schema="other"), "schema"),
        (lambda record: record.update(output_dataset_resolution="15m"), "resolution"),
        (lambda record: record.update(objects=()), "exactly one"),
    ],
)
def test_metadata_mismatch_fails_closed(mutator, message: str) -> None:
    body = _parquet()
    record = _record(body)
    mutator(record)

    with pytest.raises(FeatureOutputBindingError, match=message):
        _resolve(records={MATERIALIZATION_ID: record}, body=body)


def test_missing_pin_fails_the_required_feature_instrument_join() -> None:
    with pytest.raises(FeatureOutputBindingError, match="missing"):
        resolve_feature_materialization_pins(
            plan=_plan(),
            pins=(),
            source=Source({}),
            reader=Reader(b""),
            evaluation_from=EVALUATION_FROM,
            evaluation_through=EVALUATION_THROUGH,
        )


def test_duplicate_tuple_fails_the_required_feature_instrument_join() -> None:
    body = _parquet()
    second_id = uuid.UUID("10000000-0000-4000-8000-000000000002")
    first = _record(body)
    second = _record(body, id=second_id)
    pins = (
        FeatureMaterializationPin(MATERIALIZATION_ID, "sha256:" + str(first["result_hash"])),
        FeatureMaterializationPin(second_id, "sha256:" + str(second["result_hash"])),
    )

    with pytest.raises(FeatureOutputBindingError, match="duplicate"):
        _resolve(pins=pins, records={MATERIALIZATION_ID: first, second_id: second}, body=body)


def test_extra_tuple_fails_the_required_feature_instrument_join() -> None:
    body = _parquet()
    second_id = uuid.UUID("10000000-0000-4000-8000-000000000002")
    second_instrument = uuid.UUID("20000000-0000-4000-8000-000000000002")
    first = _record(body)
    second = _record(body, id=second_id, instrument_id=second_instrument)
    pins = (
        FeatureMaterializationPin(MATERIALIZATION_ID, "sha256:" + str(first["result_hash"])),
        FeatureMaterializationPin(second_id, "sha256:" + str(second["result_hash"])),
    )

    with pytest.raises(FeatureOutputBindingError, match="extra"):
        _resolve(pins=pins, records={MATERIALIZATION_ID: first, second_id: second}, body=body)


def test_locked_result_hash_is_checked_before_object_download() -> None:
    body = _parquet()
    record = _record(body)
    pins = (FeatureMaterializationPin(MATERIALIZATION_ID, "sha256:" + "f" * 64),)

    with pytest.raises(FeatureOutputBindingError, match="result hash"):
        _resolve(pins=pins, records={MATERIALIZATION_ID: record}, body=body)


def test_resolved_materialization_id_must_match_the_exact_pin() -> None:
    body = _parquet()
    record = _record(body, id=uuid.uuid4())

    with pytest.raises(FeatureOutputBindingError, match="materialization id"):
        _resolve(records={MATERIALIZATION_ID: record}, body=body)


def test_provider_neutral_decoder_delegates_local_version_identity_to_the_reader() -> None:
    body = _parquet()
    record = _record(body)
    record["objects"] = ({**record["objects"][0], "storage_provider": "LOCAL"},)

    resolved, reader = _resolve(records={MATERIALIZATION_ID: record}, body=body)

    assert resolved[0].feature_id == "RSI_14"
    assert reader.calls == [("LOCAL", BUCKET, KEY, VERSION)]


def test_exact_versioned_object_bytes_must_match_the_catalog_hash_and_size() -> None:
    expected = _parquet()
    changed = expected + b"changed"

    with pytest.raises(FeatureOutputBindingError, match="content hash"):
        _resolve(body=changed, records={MATERIALIZATION_ID: _record(expected)})


def test_decoded_rows_must_reproduce_the_materialization_result_hash() -> None:
    rows = _rows()
    rows[-1] = {**rows[-1], "value": "99.00000000"}
    changed = _parquet(rows)
    record = _record(changed)
    record["result_hash"] = _canonical_hash(_rows())

    with pytest.raises(FeatureOutputBindingError, match="decoded result hash"):
        _resolve(records={MATERIALIZATION_ID: record}, body=changed)


def test_out_of_order_or_duplicate_rows_fail_closed() -> None:
    rows = _rows()
    rows[1] = {**rows[1], "at": rows[0]["at"]}
    body = _parquet(rows)
    record = _record(body)

    with pytest.raises(FeatureOutputBindingError, match="strictly increasing"):
        _resolve(records={MATERIALIZATION_ID: record}, body=body)


def test_wrong_parquet_schema_fails_closed() -> None:
    wrong_schema = pa.schema(
        [
            pa.field("bar_start_at", pa.timestamp("ms", tz="UTC"), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )
    body = _parquet(schema=wrong_schema)
    record = _record(body)

    with pytest.raises(FeatureOutputBindingError, match="Parquet schema"):
        _resolve(records={MATERIALIZATION_ID: record}, body=body)


def test_missing_exact_visible_feature_instant_is_a_data_gap() -> None:
    rows = _rows()
    del rows[1]
    body = _parquet(rows)
    record = _record(body, result_hash=_canonical_hash(rows), row_count=len(rows))
    record["objects"] = ({**record["objects"][0], "row_count": len(rows)},)
    resolved, _reader = _resolve(records={MATERIALIZATION_ID: record}, body=body)

    assert resolved[0].value_at(EVALUATION_FROM) == Decimal("0.00000000")
    with pytest.raises(ElementInputMissing, match="data gap"):
        resolved[0].value_at(EVALUATION_FROM + BAR)


def _market_input(feature_values: tuple[PinnedFeatureValue, ...]) -> InstrumentInput:
    bars = tuple(
        SeriesBar(
            instrument_id=INSTRUMENT_ID,
            resolution="1m",
            starts_at=FIRST_BAR_START + BAR * index,
            ends_at=FIRST_BAR_START + BAR * (index + 1),
            close=Decimal(100 + index),
            volume=Decimal(1000),
        )
        for index in range(15)
    )
    return InstrumentInput(
        instrument_id=INSTRUMENT_ID,
        series=(
            InstrumentSeries(
                instrument_id=INSTRUMENT_ID,
                data_kind="ADJUSTED_BAR",
                resolution="1m",
                bars=bars,
            ),
        ),
        feature_series=(
            PinnedFeatureSeries(
                feature_id="RSI_14",
                instrument_id=INSTRUMENT_ID,
                resolution="1m",
                values=feature_values,
            ),
        ),
        require_pinned_features=True,
    )


def test_load_feature_uses_the_pinned_value_instead_of_recomputing_from_market_bars() -> None:
    # Ascending market closes would recompute RSI_14 as 100.  The pinned value
    # is 0, so the fixture's LT 30 comparison can pass only if LOAD_FEATURE
    # consumed the verified object series.
    inputs = _market_input(
        (
            PinnedFeatureValue(
                bar_start_at=EVALUATION_FROM - BAR,
                value=Decimal("0.00000000"),
            ),
        )
    )

    result = BasicPlanRuntime().execute(_plan(), {INSTRUMENT_ID: inputs}, as_of=EVALUATION_FROM)

    assert result.decisions[0].status is BasicDecisionStatus.CANDIDATE
    assert result.decisions[0].trace[0].evidence["value"] == "0.00000000"
    assert result.decisions[0].trace[0].evidence["source"] == "PINNED_FEATURE_OUTPUT"


def test_load_feature_does_not_fall_back_to_recomputation_when_the_pin_has_a_gap() -> None:
    inputs = _market_input(
        (
            PinnedFeatureValue(
                bar_start_at=EVALUATION_FROM - BAR * 2,
                value=Decimal("0.00000000"),
            ),
        )
    )

    result = BasicPlanRuntime().execute(_plan(), {INSTRUMENT_ID: inputs}, as_of=EVALUATION_FROM)

    assert result.decisions[0].status is BasicDecisionStatus.INPUT_MISSING
    assert result.decisions[0].trace[0].evidence["inputReason"] == "FEATURE_SERIES_DATA_GAP"


def _handler(source: Source, reader: Reader) -> OrchestratorJobHandler:
    plan = _plan_document()
    market_bytes = market_bars_parquet()
    manifest = dataset_manifest(
        hashlib.sha256(market_bytes).hexdigest(),
        row_count=len(CLOSES),
        coverage_end=EVALUATION_THROUGH,
    )
    return OrchestratorJobHandler(
        persistence=None,  # type: ignore[arg-type]
        policies=ExecutionPolicyCatalog([E2E_EXECUTION_POLICY]),
        plans=StaticCompiledPlanSource({plan["planChecksum"]: plan}),
        manifests=StaticDatasetManifestSource({DATASET_MANIFEST_ID: manifest}),
        feature_materializations=source,
        feature_object_reader=reader,
        reader=None,  # type: ignore[arg-type]
        calendar=XNYS_CALENDAR,
        object_store=None,  # type: ignore[arg-type]
        storage_write_port=None,
        sink=None,  # type: ignore[arg-type]
        attempt_policy=AttemptPolicy(
            max_attempts=2,
            lease_duration=timedelta(minutes=5),
            attempt_timeout=timedelta(minutes=30),
            max_cpu_time=timedelta(minutes=5),
            max_memory_bytes=512 * 1024 * 1024,
        ),
        monitor=None,  # type: ignore[arg-type]
        microstructure=E2E_MICROSTRUCTURE,
        fractional_policy=E2E_FRACTIONAL_POLICY,
        risk_limits=E2E_RISK_LIMITS,
        runtime=BasicPlanRuntime(),
        wall_clock=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        correlation_id="10000000-0000-4000-8000-000000000099",
    )


def _envelope(*, pins: list[dict[str, str]]) -> JobEnvelope:
    plan = _plan_document()
    market_bytes = market_bars_parquet()
    manifest = dataset_manifest(
        hashlib.sha256(market_bytes).hexdigest(),
        row_count=len(CLOSES),
        coverage_end=EVALUATION_THROUGH,
    )
    return JobEnvelope.parse(
        {
            "backtestRunId": "40000000-0000-4000-8000-000000000001",
            "botId": "40000000-0000-4000-8000-000000000002",
            "ownerAccountId": "40000000-0000-4000-8000-000000000003",
            "idempotencyKey": "FEATURE_OUTPUT_CONSUMER_TEST",
            "inputBundleFingerprint": "sha256:" + "a" * 64,
            "executionPolicyVersion": E2E_EXECUTION_POLICY.version,
            "compiledPlanChecksum": plan["planChecksum"],
            "datasetManifestId": str(DATASET_MANIFEST_ID),
            "expectedDatasetHash": "sha256:" + manifest["dataset_hash"],
            "expectedSnapshotHash": plan["executionSnapshot"]["immutableStrategyVersion"]["snapshotHash"],
            "datasets": [
                {
                    "datasetManifestId": str(DATASET_MANIFEST_ID),
                    "purposeCode": "MARKET_BARS",
                    "expectedDatasetHash": "sha256:" + manifest["dataset_hash"],
                }
            ],
            "featureMaterializations": pins,
            "featureMaterializationVersion": "feature-series.parquet.v1",
        }
    )


def _context() -> JobContext:
    return JobContext(
        worker_execution_key="FEATURE_OUTPUT_CONSUMER_TEST",
        attempt_number=1,
        receive_count=1,
        message_id="message-1",
        worker_id="worker-1",
    )


def test_job_binding_attaches_only_fully_verified_feature_series() -> None:
    body = _parquet()
    record = _record(body)
    handler = _handler(Source({MATERIALIZATION_ID: record}), Reader(body))

    binding = handler.bind(
        _envelope(
            pins=[
                {
                    "featureMaterializationId": str(MATERIALIZATION_ID),
                    "lockedResultHash": "sha256:" + str(record["result_hash"]),
                }
            ]
        ),
        _context(),
    )

    assert binding.feature_series[0].feature_id == "RSI_14"
    assert binding.feature_series[0].value_at(EVALUATION_FROM) == Decimal("0.00000000")


def test_production_feature_binding_refuses_a_job_with_no_required_pin() -> None:
    handler = _handler(Source({}), Reader(b""))

    with pytest.raises(JobNotSatisfiable, match="missing") as failure:
        handler.bind(_envelope(pins=[]), _context())

    assert failure.value.reason_code == "REQUIRED_INPUT_UNAVAILABLE"


@pytest.mark.docker
def test_localstack_reader_returns_the_pinned_version_after_the_key_changes(s3: Any, bucket: str) -> None:
    s3.put_bucket_versioning(
        Bucket=bucket,
        VersioningConfiguration={"Status": "Enabled"},
    )
    key = f"feature-output-consumer/{uuid.uuid4()}.parquet"
    original = s3.put_object(Bucket=bucket, Key=key, Body=b"original-version")
    replacement = s3.put_object(Bucket=bucket, Key=key, Body=b"replacement-version")
    assert original["VersionId"] != replacement["VersionId"]

    reader = S3VersionedFeatureObjectReader(s3)

    assert (
        reader.read_version("S3_COMPATIBLE", bucket, key, original["VersionId"])
        == b"original-version"
    )
