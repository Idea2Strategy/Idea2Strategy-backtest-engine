from __future__ import annotations

import base64
import hashlib
import hmac
import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from backtest_engine.api import RESULT_INGEST_SCOPE
from backtest_engine.backtest_request_intake import RequestLane
from backtest_engine.production import (
    ConfigurationError,
    PostgresCompiledPlanSource,
    PostgresFeatureMaterializationSource,
    PostgresOwnerDirectory,
    PostgresQueuedRunSource,
    PostgresSessionAuthenticator,
    S3ParquetMarketDataReader,
    S3VersionedFeatureObjectReader,
    SqsExecutionJobQueue,
    load_execution_policy_catalog,
)


ACCOUNT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
BOT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


class _Rows:
    def __init__(self, row: dict[str, object] | None) -> None:
        self._row = row

    def mappings(self) -> _Rows:
        return self

    def first(self) -> dict[str, object] | None:
        return self._row


class _Connection:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.row = row
        self.params: dict[str, object] | None = None

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _statement: object, params: dict[str, object]) -> _Rows:
        self.params = params
        return _Rows(self.row)


class _Engine:
    def __init__(self, row: dict[str, object] | None) -> None:
        self.connection = _Connection(row)

    def connect(self) -> _Connection:
        return self.connection


def test_owner_directory_reads_only_an_active_owned_bot() -> None:
    engine = _Engine({"owner_account_id": ACCOUNT_ID})
    directory = PostgresOwnerDirectory(engine)  # type: ignore[arg-type]

    assert directory.owner_of(BOT_ID) == ACCOUNT_ID
    assert engine.connection.params == {"bot_id": BOT_ID}


def test_compiled_plan_source_returns_the_immutable_launch_contract_document() -> None:
    plan = {"schemaVersion": 1, "planChecksum": "sha256:" + "a" * 64}
    source = PostgresCompiledPlanSource(_Engine({"plan_document": plan}))  # type: ignore[arg-type]

    assert source.by_checksum(plan["planChecksum"]) == plan


def test_feature_materialization_source_returns_definition_manifest_and_object_evidence() -> None:
    materialization_id = UUID("10000000-0000-4000-8000-000000000001")
    objects = [{"object_key": "features/rsi.parquet", "provider_version_id": "v1"}]
    source = PostgresFeatureMaterializationSource(
        _Engine(
            {
                "id": materialization_id,
                "status": "SUCCEEDED",
                "feature_definition_id": UUID("0f1b0000-0000-4000-8000-000000000001"),
                "objects": objects,
            }
        )  # type: ignore[arg-type]
    )

    resolved = source.by_id(materialization_id)

    assert resolved is not None
    assert resolved["objects"] == tuple(objects)
    assert source._engine.connection.params == {"materialization_id": materialization_id}  # type: ignore[attr-defined]


def test_feature_object_reader_requests_the_exact_s3_version_and_closes_the_body() -> None:
    class Body:
        closed = False

        def read(self) -> bytes:
            return b"versioned-feature-bytes"

        def close(self) -> None:
            self.closed = True

    class S3:
        kwargs: dict[str, str] | None = None
        body = Body()

        def get_object(self, **kwargs: str) -> dict[str, object]:
            self.kwargs = kwargs
            return {"Body": self.body}

    s3 = S3()
    reader = S3VersionedFeatureObjectReader(s3)

    body = reader.read_version(
        "S3_COMPATIBLE", "feature-bucket", "features/rsi.parquet", "version-7"
    )

    assert body == b"versioned-feature-bytes"
    assert s3.kwargs == {
        "Bucket": "feature-bucket",
        "Key": "features/rsi.parquet",
        "VersionId": "version-7",
    }
    assert s3.body.closed


@pytest.mark.docker
def test_feature_materialization_projection_executes_against_postgresql_16(
    runtime_engine,
) -> None:
    source = PostgresFeatureMaterializationSource(runtime_engine)

    resolved = source.by_id(UUID("00000000-0000-4000-8000-0000000000e1"))

    assert resolved is not None
    assert resolved["status"] == "SUCCEEDED"
    assert resolved["feature_definition_id"] == UUID(
        "00000000-0000-4000-8000-000000000095"
    )
    assert resolved["output_dataset_status"] == "AVAILABLE"
    assert resolved["objects"] == ()


def test_session_authenticator_accepts_valid_customer_and_separate_worker_token() -> None:
    key = b"k" * 32
    raw = "opaque-session"
    digest = base64.urlsafe_b64encode(hmac.new(key, raw.encode(), hashlib.sha256).digest()).rstrip(b"=").decode()
    row = {"account_id": ACCOUNT_ID, "token_digest": digest}
    authenticator = PostgresSessionAuthenticator(
        _Engine(row),  # type: ignore[arg-type]
        hmac_key=key,
        result_token="worker-only",
        result_principal_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    )

    customer = authenticator.authenticate(raw)
    worker = authenticator.authenticate("worker-only")

    assert customer is not None and customer.account_id == ACCOUNT_ID
    assert customer.scopes == frozenset()
    assert worker is not None and worker.has(RESULT_INGEST_SCOPE)
    assert authenticator.authenticate("wrong") is None


def test_policy_catalog_is_loaded_from_a_versioned_json_document(tmp_path: Path) -> None:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "policies": [
                    {
                        "version": "policy-v1",
                        "releaseQuarter": "2024-Q1",
                        "periodStart": "2024-01-01T05:00:00Z",
                        "periodEnd": "2024-04-01T04:00:00Z",
                        "feeRate": "0.002",
                        "slippageRateBps": 5,
                        "timezone": "America/New_York",
                        "sessionCalendar": "XNYS",
                        "timestampUnit": "us",
                        "priceArrowType": "double",
                        "volumeArrowType": "int64",
                        "marketDataSchemaVersion": "market-bars-v2",
                        "calculationModelVersion": "backtest-calculation-v1",
                        "marketRulesVersion": "market:1.0.0",
                        "accountingRulesVersion": "accounting:1.0.0",
                        "precisionRulesVersion": "precision:1.0.0",
                        "feePolicyId": "00000000-0000-4000-8000-000000000001",
                        "buyingPowerBufferPolicyId": "00000000-0000-4000-8000-000000000001",
                        "goodTillCancelledHorizonSeconds": 7776000,
                        "maxOrderHorizonSeconds": 7776000,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    policy = load_execution_policy_catalog(path).get("policy-v1")

    assert policy.period_start == datetime(2024, 1, 1, 5, tzinfo=UTC)
    assert policy.slippage_rate_bps == 5


def test_policy_catalog_refuses_unversioned_configuration(tmp_path: Path) -> None:
    path = tmp_path / "policy.json"
    path.write_text('{"policies": []}', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="schemaVersion"):
        load_execution_policy_catalog(path)


def test_request_dispatch_reads_the_provider_created_run_and_publishes_a_small_job() -> None:
    run_id = UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
    message_id = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
    source = PostgresQueuedRunSource(
        _Engine(
            {
                "id": run_id,
                "lane": "CUSTOM",
                "message_id": message_id,
                "bot_id": BOT_ID,
                "owner_account_id": ACCOUNT_ID,
                "input_bundle_fingerprint": "a" * 64,
                "compiled_plan_checksum": "sha256:" + "b" * 64,
                "strategy_snapshot_hash": "sha256:" + "c" * 64,
                "dataset_manifest_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                "dataset_hash": "sha256:" + "d" * 64,
                "feature_materialization_version": "features-v1",
                "aggregate_sequence": 1,
                "evaluation_start": date(2024, 1, 1),
                "evaluation_end": date(2024, 12, 31),
                "execution_policy_version": "policy-v1",
            }
        )  # type: ignore[arg-type]
    )

    run = source.by_id(run_id)

    assert run is not None
    assert run.lane is RequestLane.CUSTOM
    assert run.owner_account_id == ACCOUNT_ID

    class _Sqs:
        sent: dict[str, object] | None = None

        def send_message(self, **kwargs: object) -> None:
            self.sent = kwargs

    sqs = _Sqs()
    queue = SqsExecutionJobQueue(
        sqs,  # type: ignore[arg-type]
        {RequestLane.CUSTOM: "https://sqs/jobs-custom"},
    )
    queue.publish(
        RequestLane.CUSTOM,
        {"backtestRunId": str(run_id), "idempotencyKey": "sha256:" + "b" * 64},
    )

    assert sqs.sent is not None
    assert sqs.sent["QueueUrl"] == "https://sqs/jobs-custom"
    assert json.loads(str(sqs.sent["MessageBody"]))["backtestRunId"] == str(run_id)


def test_s3_reader_materializes_to_a_private_cache_before_delegate_read(tmp_path: Path) -> None:
    body = b"immutable parquet bytes"

    class _Body:
        def read(self, amount: int = -1) -> bytes:
            if getattr(self, "done", False):
                return b""
            self.done = True
            return body if amount else b""

        def close(self) -> None:
            pass

    class _S3:
        def get_object(self, **kwargs: str) -> dict[str, object]:
            assert kwargs == {"Bucket": "market", "Key": "year/part.parquet"}
            return {"Body": _Body()}

    reader = S3ParquetMarketDataReader(bucket="market", cache_root=tmp_path, client=_S3())
    manifest = {
        "objects": [
            {
                "object_key": "year/part.parquet",
                "content_hash": hashlib.sha256(body).hexdigest(),
            }
        ]
    }

    reader.materialize(manifest)

    assert (tmp_path / "year" / "part.parquet").read_bytes() == body


def test_s3_reader_removes_a_download_whose_checksum_does_not_match(tmp_path: Path) -> None:
    class _Body:
        def read(self, _amount: int = -1) -> bytes:
            if getattr(self, "done", False):
                return b""
            self.done = True
            return b"wrong"

        def close(self) -> None:
            pass

    client = SimpleNamespace(get_object=lambda **_kwargs: {"Body": _Body()})
    reader = S3ParquetMarketDataReader(bucket="market", cache_root=tmp_path, client=client)

    with pytest.raises(ConfigurationError, match="checksum"):
        reader.materialize({"objects": [{"object_key": "part.parquet", "content_hash": "a" * 64}]})
    assert not (tmp_path / "part.parquet").exists()
