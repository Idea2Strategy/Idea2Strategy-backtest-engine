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

import backtest_engine.production as production
from backtest_engine.api import RESULT_INGEST_SCOPE
from backtest_engine.backtest_request_intake import RequestLane
from backtest_engine.production import (
    ConfigurationError,
    HttpResultSink,
    JwtAuthenticator,
    MonotonicUtcClock,
    PostgresCompiledPlanSource,
    PostgresDatasetManifestSource,
    PostgresFeatureMaterializationSource,
    PostgresOwnerDirectory,
    PostgresQueuedRunSource,
    S3ParquetMarketDataReader,
    S3VersionedFeatureObjectReader,
    SqsExecutionJobQueue,
    api_authenticator,
    load_execution_policy_catalog,
    orchestrator_job_handler,
    service_endpoint,
)
from backtest_engine.wiring import JobNotSatisfiable


ACCOUNT_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
BOT_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def test_worker_correlation_id_is_rejected_before_other_worker_dependencies_are_built() -> None:
    with pytest.raises(ConfigurationError, match="BACKTEST_WORKER_CORRELATION_ID must be a UUID"):
        orchestrator_job_handler({"BACKTEST_WORKER_CORRELATION_ID": "i-07a6870a8c4c199dc"})


def test_worker_correlation_id_is_normalized_to_the_result_event_uuid_format() -> None:
    assert production._required_uuid(  # type: ignore[attr-defined]
        {"BACKTEST_WORKER_CORRELATION_ID": "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"},
        "BACKTEST_WORKER_CORRELATION_ID",
    ) == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def test_worker_wall_clock_advances_from_monotonic_elapsed_time() -> None:
    ticks = iter((100.0, 102.5, 102.0, 104.0))
    anchor = datetime(2026, 8, 25, 13, 0, tzinfo=UTC)
    clock = MonotonicUtcClock(lambda: anchor, lambda: next(ticks))

    assert clock() == anchor.replace(second=2, microsecond=500000)
    assert clock() == anchor.replace(second=2, microsecond=500000)
    assert clock() == anchor.replace(second=4)


def test_worker_wall_clock_resynchronizes_forward_without_regressing() -> None:
    anchor = datetime(2026, 8, 25, 13, 0, tzinfo=UTC)
    wall_instants = iter(
        (
            anchor,
            anchor.replace(hour=14),
            anchor.replace(hour=12),
            anchor.replace(hour=14, second=5),
        )
    )
    ticks = iter((100.0, 101.0, 102.0, 103.0))
    clock = MonotonicUtcClock(lambda: next(wall_instants), lambda: next(ticks))

    assert clock() == anchor.replace(hour=14)
    assert clock() == anchor.replace(hour=14)
    assert clock() == anchor.replace(hour=14, second=5)


class _HttpResultResponse:
    status = 200

    def __enter__(self) -> _HttpResultResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def test_result_sink_retries_a_transient_timeout_with_the_exact_same_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[object] = []
    sleeps: list[float] = []

    def urlopen(request: object, *, timeout: float) -> _HttpResultResponse:
        assert timeout == 2.0
        requests.append(request)
        if len(requests) == 1:
            raise TimeoutError("temporary result endpoint timeout")
        return _HttpResultResponse()

    monkeypatch.setattr(production, "urlopen", urlopen)
    sink = HttpResultSink(
        "https://api.example.com",
        "worker-token",
        timeout_seconds=2.0,
        max_transport_attempts=3,
        retry_backoff_seconds=0.25,
        sleeper=sleeps.append,
    )

    sink.publish(
        {"backtestRunId": str(BOT_ID), "status": "FAILED"},
        delivery_attempt=1,
    )

    assert len(requests) == 2
    assert requests[0].data == requests[1].data  # type: ignore[attr-defined]
    assert requests[0].headers == requests[1].headers  # type: ignore[attr-defined]
    assert sleeps == [0.25]


def test_result_sink_bounds_transient_transport_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def urlopen(_request: object, *, timeout: float) -> _HttpResultResponse:
        nonlocal calls
        assert timeout == 1.0
        calls += 1
        raise TimeoutError("still unavailable")

    monkeypatch.setattr(production, "urlopen", urlopen)
    sink = HttpResultSink(
        "https://api.example.com",
        "worker-token",
        timeout_seconds=1.0,
        max_transport_attempts=3,
        retry_backoff_seconds=0.1,
        sleeper=sleeps.append,
    )

    with pytest.raises(RuntimeError, match="after 3 transport attempts"):
        sink.publish(
            {"backtestRunId": str(BOT_ID), "status": "FAILED"},
            delivery_attempt=1,
        )

    assert calls == 3
    assert sleeps == [0.1, 0.2]


def test_result_sink_does_not_retry_an_http_response_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0
    sleeps: list[float] = []

    def urlopen(_request: object, *, timeout: float) -> _HttpResultResponse:
        nonlocal calls
        assert timeout == 1.0
        calls += 1
        raise production.HTTPError(
            "https://api.example.com",
            503,
            "Service Unavailable",
            {},
            None,
        )

    monkeypatch.setattr(production, "urlopen", urlopen)
    sink = HttpResultSink(
        "https://api.example.com",
        "worker-token",
        timeout_seconds=1.0,
        max_transport_attempts=3,
        retry_backoff_seconds=0.1,
        sleeper=sleeps.append,
    )

    with pytest.raises(RuntimeError, match="HTTP Error 503"):
        sink.publish(
            {"backtestRunId": str(BOT_ID), "status": "FAILED"},
            delivery_attempt=1,
        )

    assert calls == 1
    assert sleeps == []


def test_service_specific_aws_endpoint_overrides_the_legacy_shared_endpoint() -> None:
    environment = {
        "AWS_ENDPOINT_URL": "http://legacy:4566",
        "AWS_ENDPOINT_URL_S3": "http://minio:9000",
        "AWS_ENDPOINT_URL_SQS": "http://localstack:4566",
    }

    assert service_endpoint(environment, "S3") == "http://minio:9000"
    assert service_endpoint(environment, "SQS") == "http://localstack:4566"
    assert service_endpoint({"AWS_ENDPOINT_URL": "http://legacy:4566"}, "S3") == "http://legacy:4566"


def test_api_authenticator_requires_the_customer_jwt_signing_key() -> None:
    environment = {
        "CUSTOMER_JWT_SIGNING_KEY_BASE64": base64.b64encode(b"j" * 32).decode(),
        "BACKTEST_RESULT_INGEST_TOKEN": "worker-only",
        "BACKTEST_RESULT_PRINCIPAL_ID": str(ACCOUNT_ID),
    }

    assert isinstance(api_authenticator(environment), JwtAuthenticator)
    with pytest.raises(ConfigurationError, match="CUSTOMER_JWT_SIGNING_KEY_BASE64"):
        api_authenticator({
            "BACKTEST_SESSION_HMAC_KEY_BASE64": environment["CUSTOMER_JWT_SIGNING_KEY_BASE64"],
            "BACKTEST_RESULT_INGEST_TOKEN": "worker-only",
            "BACKTEST_RESULT_PRINCIPAL_ID": str(ACCOUNT_ID),
        })


class _Rows:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows

    def mappings(self) -> _Rows:
        return self

    def first(self) -> dict[str, object] | None:
        return self._rows[0] if self._rows else None

    def all(self) -> list[dict[str, object]]:
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _Connection:
    def __init__(self, rows: list[list[dict[str, object]]]) -> None:
        self.rows = rows
        self.params: dict[str, object] | None = None

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _statement: object, params: dict[str, object]) -> _Rows:
        self.params = params
        rows = self.rows[0] if len(self.rows) == 1 else self.rows.pop(0)
        if "token_digest" in params:
            rows = [row for row in rows if row.get("token_digest") == params["token_digest"]]
        return _Rows(rows)


class _Engine:
    def __init__(
        self,
        row: dict[str, object] | None,
        *additional_rows: list[dict[str, object]],
    ) -> None:
        self.connection = _Connection(
            [([] if row is None else [row]), *additional_rows]
        )

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


def _dataset_manifest_source(
    manifest_id: UUID,
    object_keys: list[str],
    *,
    object_overrides: dict[str, object] | None = None,
    is_composite: bool = False,
) -> PostgresDatasetManifestSource:
    manifest = {
        "id": manifest_id,
        "revision_number": 1,
        "status": "AVAILABLE",
        "dataset_hash": "sha256:" + "a" * 64,
        "schema_version": "market-bars/1",
        "period_start": datetime(2024, 1, 1, tzinfo=UTC),
        "period_end": datetime(2024, 2, 1, tzinfo=UTC),
        "available_at": datetime(2026, 8, 9, tzinfo=UTC),
        "provider_code": "ALPACA",
        "feed_code": "ALPACA_SIP_ALL_30M",
        "data_layer": "ADJUSTED",
        "feed_resolution": "30m",
        "is_composite": is_composite,
    }
    objects = [
        {
            "storage_object_id": UUID(f"00000000-0000-4000-8000-{index:012d}"),
            "object_key": object_key,
            "content_hash": "sha256:" + f"{index:x}" * 64,
            "object_kind": "MARKET_BARS",
            "partition_granularity": "YEAR",
            "partition_start": date(2024, 1, 1),
            "partition_end": date(2025, 1, 1),
            "period_start": datetime(2024, 1, 1, tzinfo=UTC),
            "period_end": datetime(2024, 2, 1, tzinfo=UTC),
            "shard_key": f"{index:02d}-of-{len(object_keys):02d}",
            "part_number": 1,
            "row_count": 10,
            "schema_version": "market-bars/1",
            "storage_provider": "S3",
            "bucket_name": "market",
            "provider_version_id": f"version-{index}",
            "file_format": "PARQUET",
            "media_type": "application/vnd.apache.parquet",
            "status": "AVAILABLE",
        }
        for index, object_key in enumerate(object_keys, start=1)
    ]
    for item in objects:
        item.update(object_overrides or {})
    return PostgresDatasetManifestSource(_Engine(manifest, objects))  # type: ignore[arg-type]


def test_dataset_manifest_source_accepts_the_deployed_legacy_loader_binding() -> None:
    manifest_id = UUID("7f7113c9-3b02-4098-97ec-0baa07e2b3b0")
    prefix = (
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        "resolution=30m/revision=00000001/year=2024"
    )
    object_keys = [
        f"{prefix}/shard={shard:02d}-of-08/manifest_id={manifest_id}/part-00001.parquet"
        for shard in range(8)
    ]

    resolved = _dataset_manifest_source(manifest_id, object_keys).by_id(manifest_id)

    assert resolved is not None
    assert resolved["manifest_id"] == str(manifest_id)
    assert resolved["dataset_id"] == str(manifest_id)
    assert [item["object_key"] for item in resolved["objects"]] == object_keys
    assert [item["provider_version_id"] for item in resolved["objects"]] == [
        f"version-{index}" for index in range(1, 9)
    ]


def test_dataset_manifest_source_rejects_an_object_without_an_immutable_s3_version() -> None:
    manifest_id = UUID("7f7113c9-3b02-4098-97ec-0baa07e2b3b0")
    key = (
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        "resolution=30m/revision=00000001/year=2024/shard=00-of-01/"
        f"manifest_id={manifest_id}/part-00001.parquet"
    )
    source = _dataset_manifest_source(
        manifest_id,
        [key],
        object_overrides={"provider_version_id": None},
    )

    with pytest.raises(JobNotSatisfiable, match="immutable S3 version evidence"):
        source.by_id(manifest_id)


def test_dataset_manifest_source_preserves_the_canonical_logical_dataset_binding() -> None:
    manifest_id = UUID("7f7113c9-3b02-4098-97ec-0baa07e2b3b0")
    dataset_id = UUID("11111111-1111-4111-8111-111111111111")
    object_keys = [
        f"market-data/provider=ALPACA/feed=SIP/dataset={dataset_id}/revision=1/part-{part:05d}.parquet"
        for part in range(1, 3)
    ]

    resolved = _dataset_manifest_source(manifest_id, object_keys).by_id(manifest_id)

    assert resolved is not None
    assert resolved["dataset_id"] == str(dataset_id)


def test_dataset_manifest_source_accepts_explicit_composite_legacy_lineage() -> None:
    composite_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
    source_ids = (
        UUID("11111111-1111-4111-8111-111111111111"),
        UUID("22222222-2222-4222-8222-222222222222"),
    )
    object_keys = [
        "historical/provider=alpaca/feed=sip/adjustment=all/session=regular/"
        f"resolution=30m/revision=00000001/year={2016 + index}/shard=00-of-01/"
        f"manifest_id={source_id}/part-00001.parquet"
        for index, source_id in enumerate(source_ids)
    ]

    resolved = _dataset_manifest_source(
        composite_id,
        object_keys,
        is_composite=True,
    ).by_id(composite_id)

    assert resolved is not None
    assert resolved["dataset_id"] == str(composite_id)
    assert resolved["composite"] is True


@pytest.mark.parametrize(
    "object_keys",
    [
        [
            "historical/dataset=11111111-1111-4111-8111-111111111111/part-00001.parquet",
            "historical/manifest_id=7f7113c9-3b02-4098-97ec-0baa07e2b3b0/part-00002.parquet",
        ],
        [
            "historical/manifest_id=22222222-2222-4222-8222-222222222222/part-00001.parquet",
        ],
        ["historical/dataset=not-a-uuid/part-00001.parquet"],
        ["historical/revision=00000001/part-00001.parquet"],
    ],
    ids=["mixed-conventions", "wrong-legacy-manifest", "invalid-uuid", "missing-binding"],
)
def test_dataset_manifest_source_classifies_invalid_catalog_bindings_as_terminal(
    object_keys: list[str],
) -> None:
    manifest_id = UUID("7f7113c9-3b02-4098-97ec-0baa07e2b3b0")

    with pytest.raises(JobNotSatisfiable) as failure:
        _dataset_manifest_source(manifest_id, object_keys).by_id(manifest_id)

    assert failure.value.reason_code == "REQUIRED_INPUT_UNAVAILABLE"


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


def test_feature_object_reader_accepts_the_legacy_s3_provider_name() -> None:
    class Body:
        def read(self) -> bytes:
            return b"versioned-feature-bytes"

        def close(self) -> None:
            pass

    class S3:
        def get_object(self, **_kwargs: str) -> dict[str, object]:
            return {"Body": Body()}

    reader = S3VersionedFeatureObjectReader(S3())

    assert reader.read_version("S3", "feature-bucket", "features/rsi.parquet", "version-7") == (
        b"versioned-feature-bytes"
    )


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
    assert resolved["output_dataset_feed_id"] == UUID(
        "00000000-0000-4000-8000-000000000092"
    )
    assert resolved["output_feed_code"] == "FIXTURE_BARS"
    assert resolved["output_feed_data_kind"] == "BAR"
    assert resolved["output_feed_resolution"] == "PT1M"
    assert resolved["output_feed_timezone"] == "America/New_York"
    assert resolved["output_feed_version"] == "1.0.0"
    assert resolved["output_feed_retired_at"] is None
    assert resolved["output_provider_id"] == UUID(
        "00000000-0000-4000-8000-000000000091"
    )
    assert resolved["output_provider_code"] == "FIXTURE"
    assert resolved["output_provider_display_name"] == "Fixture Provider"
    assert resolved["output_provider_rights_version"] == "1.0.0"
    assert resolved["output_provider_status"] == "ACTIVE"
    assert resolved["objects"] == ()


def test_jwt_authenticator_accepts_valid_customer_and_separate_worker_token() -> None:
    key = b"k" * 32
    raw = _customer_access_jwt(key, ACCOUNT_ID)
    authenticator = JwtAuthenticator(
        hmac_key=key,
        result_token="worker-only",
        result_principal_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
        issuer="https://ideatostrategy.com",
        audience="idea2strategy-api",
    )

    customer = authenticator.authenticate(raw)
    worker = authenticator.authenticate("worker-only")

    assert customer is not None and customer.account_id == ACCOUNT_ID
    assert customer.scopes == frozenset()
    assert worker is not None and worker.has(RESULT_INGEST_SCOPE)
    assert authenticator.authenticate("wrong") is None


def _customer_access_jwt(key: bytes, account_id: UUID) -> str:
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")  # noqa: E731
    header = encode(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    now = int(datetime.now(UTC).timestamp())
    payload = encode(json.dumps({
        "iss": "https://ideatostrategy.com",
        "aud": "idea2strategy-api",
        "sub": str(account_id),
        "lid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "ae": 1,
        "cv": 1,
        "typ": "access",
        "iat": now,
        "exp": now + 300,
    }, separators=(",", ":")).encode())
    signature = encode(hmac.new(key, f"{header}.{payload}".encode("ascii"), hashlib.sha256).digest())
    return f"{header}.{payload}.{signature}"


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
                "input_bundle_id": UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                "input_bundle_fingerprint": "a" * 64,
                "input_contract_version": "backtest-request.v1",
                "compiled_plan_checksum": "sha256:" + "b" * 64,
                "strategy_snapshot_hash": "sha256:" + "c" * 64,
                "aggregate_sequence": 1,
                "evaluation_start": date(2024, 1, 1),
                "evaluation_end": date(2024, 12, 31),
                "execution_policy_version": "policy-v1",
            },
            [
                {
                    "dataset_manifest_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    "purpose_code": "MARKET_BARS",
                    "locked_dataset_hash": "sha256:" + "d" * 64,
                }
            ],
            [
                {
                    "feature_materialization_id": UUID(
                        "99999999-9999-4999-8999-999999999999"
                    ),
                    "locked_result_hash": "sha256:" + "e" * 64,
                }
            ],
        )  # type: ignore[arg-type]
    )

    run = source.by_id(run_id)

    assert run is not None
    assert run.lane is RequestLane.CUSTOM
    assert run.owner_account_id == ACCOUNT_ID
    assert run.input_bundle_id == UUID("ffffffff-ffff-4fff-8fff-ffffffffffff")
    assert run.input_contract_version == "backtest-request.v1"
    assert run.datasets[0].purpose_code == "MARKET_BARS"
    assert len(run.feature_materializations) == 1

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
            assert kwargs == {
                "Bucket": "market",
                "Key": "year/part.parquet",
                "VersionId": "immutable-version-1",
            }
            return {"Body": _Body()}

    reader = S3ParquetMarketDataReader(bucket="market", cache_root=tmp_path, client=_S3())
    manifest = {
        "objects": [
            {
                "object_key": "year/part.parquet",
                "content_hash": hashlib.sha256(body).hexdigest(),
                "storage_provider": "S3",
                "bucket_name": "market",
                "provider_version_id": "immutable-version-1",
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
        reader.materialize(
            {
                "objects": [
                    {
                        "object_key": "part.parquet",
                        "content_hash": "a" * 64,
                        "storage_provider": "S3",
                        "bucket_name": "market",
                        "provider_version_id": "immutable-version-1",
                    }
                ]
            }
        )
    assert not (tmp_path / "part.parquet").exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("storage_provider", "LOCAL"),
        ("bucket_name", "other-market"),
        ("provider_version_id", ""),
    ],
)
def test_s3_reader_rejects_mutable_or_substituted_object_evidence(
    tmp_path: Path, field: str, value: str
) -> None:
    metadata = {
        "object_key": "part.parquet",
        "content_hash": "a" * 64,
        "storage_provider": "S3",
        "bucket_name": "market",
        "provider_version_id": "immutable-version-1",
    }
    metadata[field] = value
    client = SimpleNamespace(get_object=lambda **_kwargs: pytest.fail("must fail before S3"))
    reader = S3ParquetMarketDataReader(bucket="market", cache_root=tmp_path, client=client)

    with pytest.raises(ConfigurationError):
        reader.materialize({"objects": [metadata]})
