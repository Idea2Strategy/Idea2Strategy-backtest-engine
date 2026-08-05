"""Fail-closed production adapters for the API and worker entry points.

Every public factory in this module is a valid ``package.module:factory`` target.
Factories read only explicit environment settings and never substitute an in-memory
implementation.  Tests may instantiate the adapter classes directly.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, BinaryIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import Engine, text

from backtest_engine.api import RESULT_INGEST_SCOPE, Principal
from backtest_engine.attempt_coordinator import AttemptPolicy, ProcessResourceMonitor
from backtest_engine.backtest_request_intake import PostgresRequestReceiptStore, RequestLane
from backtest_engine.basic_runtime import BasicPlanRuntime
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.execution_model import (
    ExecutionMicrostructurePolicy,
    InstrumentFractionalPolicy,
    RiskLimits,
)
from backtest_engine.execution_policy import ExecutionPolicy, ExecutionPolicyCatalog
from backtest_engine.lifecycle import DeadLetteredMessage
from backtest_engine.market_data import ParquetMarketDataReader
from backtest_engine.money import PRECISION_RULES_VERSION
from backtest_engine.object_store import S3ObjectStore
from backtest_engine.persistence import BacktestPersistence, create_backtest_engine
from backtest_engine.request_dispatch import (
    BacktestRequestJobPublisher,
    QueuedRunProjection,
)
from backtest_engine.wiring import (
    OrchestratorJobHandler,
    PersistenceExecutionKeyStore,
    PersistenceStorageObjectWritePort,
)


class ConfigurationError(RuntimeError):
    """A production process cannot safely start with the supplied configuration."""


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"missing required environment setting: {name}")
    return value


def service_endpoint(environ: Mapping[str, str], service: str) -> str | None:
    """Resolve an emulator endpoint without coupling S3 and SQS together.

    ``AWS_ENDPOINT_URL`` remains the backwards-compatible fallback used by a
    single-service emulator. Local development uses MinIO for S3 and LocalStack
    for SQS, so the service-specific variables must win when present.
    """

    return environ.get(f"AWS_ENDPOINT_URL_{service.upper()}") or environ.get(
        "AWS_ENDPOINT_URL"
    )


def _engine(environ: Mapping[str, str] = os.environ) -> Engine:
    return create_backtest_engine(_required(environ, "BACKTEST_DATABASE_URL"))


def _utc(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ConfigurationError(f"{label} must be a UTC timestamp ending in Z")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() != timedelta(0):
        raise ConfigurationError(f"{label} must use UTC")
    return parsed.astimezone(UTC)


def _utc_text(value: object) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ConfigurationError(f"database timestamp is not timezone-aware: {value!r}")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_document(path: Path, label: str) -> Mapping[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"{label} is not readable JSON: {path}") from exc
    if not isinstance(document, Mapping):
        raise ConfigurationError(f"{label} must contain a JSON object")
    return document


class PostgresOwnerDirectory:
    """Read bot ownership without importing or writing the backend-owned schema."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def owner_of(self, bot_id: uuid.UUID) -> uuid.UUID | None:
        statement = text(
            """
            SELECT owner_account_id
              FROM bot.bots
             WHERE id = :bot_id
               AND owner_account_id IS NOT NULL
               AND owner_anonymized_at IS NULL
               AND deleted_at IS NULL
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"bot_id": bot_id}).mappings().first()
        return None if row is None else uuid.UUID(str(row["owner_account_id"]))


class PostgresCompiledPlanSource:
    """Resolve only the immutable plan published with a bot launch."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def by_checksum(self, checksum: str) -> Mapping[str, Any] | None:
        statement = text(
            """
            SELECT plan_document
              FROM bot.launch_contract_plans
             WHERE plan_checksum = :checksum
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"checksum": checksum}).mappings().first()
        if row is None:
            return None
        document = row["plan_document"]
        if isinstance(document, str):
            document = json.loads(document)
        if not isinstance(document, Mapping) or document.get("planChecksum") != checksum:
            raise ConfigurationError("bot.launch_contract_plans document does not match its plan_checksum")
        return dict(document)


def _dataset_id(object_keys: list[str]) -> str:
    values = {
        segment.removeprefix("dataset=")
        for key in object_keys
        for segment in key.split("/")
        if segment.startswith("dataset=")
    }
    if len(values) != 1:
        raise ConfigurationError("dataset manifest object keys must bind one logical dataset= UUID")
    value = values.pop()
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ConfigurationError(f"dataset object key contains invalid dataset id: {value}") from exc


class PostgresDatasetManifestSource:
    """Rebuild the consumer contract from canonical catalog and storage rows."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def by_id(self, manifest_id: uuid.UUID) -> Mapping[str, Any] | None:
        manifest_sql = text(
            """
            SELECT id, revision_number, status, dataset_hash, schema_version,
                   period_start, period_end, available_at
              FROM market_data.dataset_manifests
             WHERE id = :manifest_id
            """
        )
        objects_sql = text(
            """
            SELECT o.id AS storage_object_id, o.object_key, o.content_hash,
                   d.object_kind, d.partition_granularity, d.partition_start,
                   d.partition_end, d.period_start, d.period_end, d.shard_key,
                   d.part_number, d.row_count, o.schema_version, o.status
              FROM market_data.dataset_objects d
              JOIN storage.objects o ON o.id = d.object_id
             WHERE d.dataset_manifest_id = :manifest_id
             ORDER BY d.partition_start, d.shard_key, d.part_number, d.id
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(manifest_sql, {"manifest_id": manifest_id}).mappings().first()
            if row is None:
                return None
            object_rows = list(connection.execute(objects_sql, {"manifest_id": manifest_id}).mappings())
        if not object_rows:
            raise ConfigurationError(f"dataset manifest {manifest_id} has no objects")
        if any(item["status"] != "AVAILABLE" for item in object_rows):
            raise ConfigurationError(f"dataset manifest {manifest_id} references a non-AVAILABLE object")
        objects = [
            {
                "storage_object_id": str(item["storage_object_id"]),
                "object_key": str(item["object_key"]),
                "content_hash": str(item["content_hash"]),
                "object_kind": str(item["object_kind"]),
                "partition_granularity": str(item["partition_granularity"]),
                "partition_start": item["partition_start"].isoformat(),
                "partition_end": item["partition_end"].isoformat(),
                "period_start": _utc_text(item["period_start"]),
                "period_end": _utc_text(item["period_end"]),
                "shard_key": str(item["shard_key"]),
                "part_number": int(item["part_number"]),
                "row_count": int(item["row_count"]),
                "schema_version": str(item["schema_version"]),
            }
            for item in object_rows
        ]
        available_at = row["available_at"]
        if available_at is None:
            raise ConfigurationError(f"dataset manifest {manifest_id} has no available_at evidence")
        return {
            "contract_id": "com06.dataset-manifest",
            "schema_version": 1,
            "manifest_id": str(row["id"]),
            "dataset_id": _dataset_id([item["object_key"] for item in objects]),
            "revision": int(row["revision_number"]),
            "status": str(row["status"]),
            "dataset_hash": str(row["dataset_hash"]),
            "schema_id": str(row["schema_version"]),
            "period_start": _utc_text(row["period_start"]),
            "period_end": _utc_text(row["period_end"]),
            "available_at": _utc_text(available_at),
            "objects": objects,
        }


class PostgresFeatureMaterializationSource:
    """Resolve a locked feature result and its immutable output manifest evidence."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def by_id(self, materialization_id: uuid.UUID) -> Mapping[str, Any] | None:
        statement = text(
            """
            SELECT f.id, f.status::text, f.result_hash, f.feature_definition_id,
                   fd.feature_code, fd.calculator_version, fd.resolution,
                   fd.definition_hash, f.instrument_id, f.input_dataset_set_hash,
                   f.period_start, f.period_end, f.output_dataset_manifest_id,
                   d.status::text AS output_dataset_status,
                   d.data_layer AS output_dataset_layer,
                   d.instrument_id AS output_dataset_instrument_id,
                   d.schema_version AS output_dataset_schema,
                   d.resolution AS output_dataset_resolution,
                   COALESCE(
                     jsonb_agg(
                       jsonb_build_object(
                         'object_kind', rel.object_kind,
                         'status', obj.status::text,
                         'storage_provider', obj.storage_provider,
                         'bucket_name', obj.bucket_name,
                         'object_key', obj.object_key,
                         'provider_version_id', obj.provider_version_id,
                         'content_hash', obj.content_hash,
                         'byte_size', obj.byte_size,
                         'file_format', obj.file_format,
                         'schema_version', obj.schema_version,
                         'row_count', rel.row_count
                       ) ORDER BY rel.part_number
                     ) FILTER (WHERE rel.id IS NOT NULL),
                     '[]'::jsonb
                   ) AS objects
              FROM market_data.feature_materializations f
              JOIN market_data.feature_definitions fd
                ON fd.id = f.feature_definition_id
              LEFT JOIN market_data.dataset_manifests d
                ON d.id = f.output_dataset_manifest_id
              LEFT JOIN market_data.dataset_objects rel
                ON rel.dataset_manifest_id = d.id
              LEFT JOIN storage.objects obj
                ON obj.id = rel.object_id
             WHERE f.id = :materialization_id
             GROUP BY f.id, fd.id, d.id
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(
                statement, {"materialization_id": materialization_id}
            ).mappings().first()
        if row is None:
            return None
        resolved = dict(row)
        objects = resolved.get("objects")
        if isinstance(objects, str):
            try:
                objects = json.loads(objects)
            except json.JSONDecodeError as exc:
                raise ConfigurationError(
                    f"feature materialization {materialization_id} has invalid object evidence"
                ) from exc
        resolved["objects"] = tuple(objects or ())
        return resolved


class S3VersionedFeatureObjectReader:
    """Read the exact immutable S3 version named by ``storage.objects``."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def read_version(
        self, provider: str, bucket: str, key: str, version_id: str
    ) -> bytes:
        if provider != "S3_COMPATIBLE":
            raise ConfigurationError(
                f"S3 feature reader cannot read storage provider {provider!r}"
            )
        response = self._client.get_object(
            Bucket=bucket,
            Key=key,
            VersionId=version_id,
        )
        body = response["Body"]
        try:
            payload = body.read()
        finally:
            body.close()
        if not isinstance(payload, bytes):
            raise ConfigurationError("S3 feature object body did not return bytes")
        return payload


class PostgresQueuedRunSource:
    """Read the provider-created run that authorizes request-to-job conversion."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def by_id(self, run_id: uuid.UUID) -> QueuedRunProjection | None:
        statement = text(
            """
            SELECT r.id, r.lane::text, r.message_id, r.bot_id, r.owner_account_id,
                   p.input_bundle_fingerprint, p.compiled_plan_checksum,
                   p.strategy_snapshot_hash, p.dataset_manifest_id, p.dataset_hash,
                   p.feature_materialization_version, r.aggregate_sequence,
                   r.evaluation_start, r.evaluation_end, r.execution_policy_version
              FROM backtest.runs r
              JOIN backtest.run_input_pins p ON p.run_id = r.id
             WHERE r.id = :run_id
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"run_id": run_id}).mappings().first()
        if row is None:
            return None
        try:
            return QueuedRunProjection(
                run_id=uuid.UUID(str(row["id"])),
                lane=RequestLane(str(row["lane"])),
                message_id=uuid.UUID(str(row["message_id"])),
                bot_id=uuid.UUID(str(row["bot_id"])),
                owner_account_id=uuid.UUID(str(row["owner_account_id"])),
                input_bundle_fingerprint=str(row["input_bundle_fingerprint"]),
                compiled_plan_checksum=str(row["compiled_plan_checksum"]),
                strategy_snapshot_hash=str(row["strategy_snapshot_hash"]),
                dataset_manifest_id=uuid.UUID(str(row["dataset_manifest_id"])),
                dataset_hash=str(row["dataset_hash"]),
                feature_materialization_version=str(row["feature_materialization_version"]),
                aggregate_sequence=int(row["aggregate_sequence"]),
                evaluation_start=row["evaluation_start"],
                evaluation_end=row["evaluation_end"],
                execution_policy_version=str(row["execution_policy_version"]),
            )
        except (TypeError, ValueError) as exc:
            raise ConfigurationError(f"backtest run {run_id} cannot form an execution job") from exc


class SqsExecutionJobQueue:
    """Publish the small internal job body to the already bounded lane queues."""

    def __init__(self, client: Any, queue_urls: Mapping[RequestLane, str]) -> None:
        self._client = client
        self._queue_urls = dict(queue_urls)

    def publish(self, lane: RequestLane, job: dict[str, Any]) -> None:
        body = json.dumps(job, sort_keys=True, separators=(",", ":"))
        self._client.send_message(
            QueueUrl=self._queue_urls[lane],
            MessageBody=body,
            MessageAttributes={
                "BacktestLane": {"DataType": "String", "StringValue": lane.value},
                "BacktestRunId": {
                    "DataType": "String",
                    "StringValue": str(job["backtestRunId"]),
                },
            },
        )


class PostgresSessionAuthenticator:
    """Validate backend opaque sessions and a distinct worker-only token."""

    def __init__(
        self,
        engine: Engine,
        *,
        hmac_key: bytes,
        result_token: str,
        result_principal_id: uuid.UUID,
    ) -> None:
        if len(hmac_key) < 32:
            raise ConfigurationError("BACKTEST_SESSION_HMAC_KEY must contain at least 32 bytes")
        if not result_token:
            raise ConfigurationError("BACKTEST_RESULT_INGEST_TOKEN must not be empty")
        self._engine = engine
        self._hmac_key = hmac_key
        self._result_token = result_token
        self._result_principal_id = result_principal_id

    def authenticate(self, token: str) -> Principal | None:
        if hmac.compare_digest(token, self._result_token):
            return Principal(
                account_id=self._result_principal_id,
                scopes=frozenset({RESULT_INGEST_SCOPE}),
            )
        digest = (
            base64.urlsafe_b64encode(hmac.new(self._hmac_key, token.encode("utf-8"), hashlib.sha256).digest())
            .rstrip(b"=")
            .decode("ascii")
        )
        statement = text(
            """
            SELECT s.account_id, s.token_digest
              FROM identity.sessions s
              JOIN identity.accounts a ON a.id = s.account_id
              JOIN identity.login_identities li
                ON li.id = s.authenticated_by_login_identity_id
              JOIN identity.account_security_states sec ON sec.account_id = s.account_id
              LEFT JOIN identity.password_credentials pc
                ON pc.login_identity_id = li.id
             WHERE s.token_digest = :token_digest
               AND s.revoked_at IS NULL
               AND s.expires_at > CURRENT_TIMESTAMP
               AND a.lifecycle_status = 'ACTIVE'
               AND li.status = 'ACTIVE'
               AND s.auth_epoch_at_issue = sec.auth_epoch
               AND s.credential_version_at_issue IS NOT DISTINCT FROM pc.credential_version
               AND NOT EXISTS (
                   SELECT 1 FROM identity.account_sanctions sanction
                    WHERE sanction.account_id = s.account_id
                      AND sanction.status = 'ACTIVE'
                      AND sanction.effective_at <= CURRENT_TIMESTAMP
                      AND (sanction.expires_at IS NULL OR sanction.expires_at > CURRENT_TIMESTAMP)
               )
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"token_digest": digest}).mappings().first()
        if row is None or not hmac.compare_digest(str(row["token_digest"]), digest):
            return None
        return Principal(account_id=uuid.UUID(str(row["account_id"])))


class SqsDeadLetterSink:
    def __init__(self, client: Any, queue_url: str) -> None:
        self._client = client
        self._queue_url = queue_url

    def dead_letter(self, message: DeadLetteredMessage) -> None:
        body = {
            "payload": dict(message.payload),
            "reason": message.reason,
            "failureKind": message.failure_kind,
            "deliveryAttempt": message.delivery_attempt,
        }
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=json.dumps(body, sort_keys=True, separators=(",", ":")),
        )


class HttpResultSink:
    def __init__(self, base_url: str, token: str, *, timeout_seconds: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds

    def publish(self, event: Mapping[str, Any], *, delivery_attempt: int) -> None:
        run_id = str(event["backtestRunId"])
        request = Request(
            f"{self._base_url}/api/v1/backtests/{run_id}/results",
            data=json.dumps(dict(event), separators=(",", ":")).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
                "X-Delivery-Attempt": str(delivery_attempt),
            },
        )
        try:
            with urlopen(request, timeout=self._timeout) as response:
                status = response.status
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"backtest result ingestion failed: {exc}") from exc
        if status != 200:
            raise RuntimeError(f"backtest result ingestion returned HTTP {status}")


class S3ParquetMarketDataReader:
    """Materialize immutable input objects into a bounded local cache."""

    def __init__(
        self,
        *,
        bucket: str,
        cache_root: Path,
        client: Any,
        batch_size: int = 65_536,
    ) -> None:
        if not bucket:
            raise ConfigurationError("BACKTEST_MARKET_DATA_BUCKET must not be empty")
        self._bucket = bucket
        self._root = cache_root.expanduser().resolve()
        self._client = client
        self._reader = ParquetMarketDataReader(self._root, batch_size=batch_size)

    def _target(self, key: str) -> Path:
        target = (self._root / key).resolve()
        try:
            target.relative_to(self._root)
        except ValueError as exc:
            raise ConfigurationError("market-data object key escapes cache root") from exc
        return target

    @staticmethod
    def _copy_and_hash(source: BinaryIO, target: Path) -> str:
        digest = hashlib.sha256()
        with target.open("wb") as output:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
                output.write(chunk)
        return digest.hexdigest()

    def materialize(self, manifest: Mapping[str, Any]) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        for metadata in manifest.get("objects", []):
            key = str(metadata.get("object_key", ""))
            expected = str(metadata.get("content_hash", ""))
            target = self._target(key)
            if target.is_file():
                with target.open("rb") as existing:
                    actual = hashlib.file_digest(existing, "sha256").hexdigest()
                if hmac.compare_digest(actual, expected):
                    continue
                target.unlink()
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"]
            try:
                actual = self._copy_and_hash(body, temporary)
            finally:
                body.close()
            if not hmac.compare_digest(actual, expected):
                temporary.unlink(missing_ok=True)
                raise ConfigurationError(f"downloaded market-data object checksum does not match: {key}")
            temporary.replace(target)

    def iter_batches(self, manifest: Mapping[str, Any], policy: ExecutionPolicy) -> Any:
        self.materialize(manifest)
        return self._reader.iter_batches(manifest, policy)

    def read(self, manifest: Mapping[str, Any], policy: ExecutionPolicy) -> Any:
        self.materialize(manifest)
        return self._reader.read(manifest, policy)


def load_execution_policy_catalog(path: Path) -> ExecutionPolicyCatalog:
    document = _read_document(path, "execution policy catalog")
    if document.get("schemaVersion") != 1:
        raise ConfigurationError("execution policy catalog schemaVersion must be 1")
    items = document.get("policies")
    if not isinstance(items, list) or not items:
        raise ConfigurationError("execution policy catalog must publish at least one policy")
    policies = []
    try:
        for item in items:
            policies.append(
                ExecutionPolicy(
                    version=item["version"],
                    release_quarter=item["releaseQuarter"],
                    period_start=_utc(item["periodStart"], "periodStart"),
                    period_end=_utc(item["periodEnd"], "periodEnd"),
                    fee_rate=Decimal(item["feeRate"]),
                    slippage_rate_bps=item["slippageRateBps"],
                    timezone=item["timezone"],
                    session_calendar=item["sessionCalendar"],
                    timestamp_unit=item["timestampUnit"],
                    price_arrow_type=item["priceArrowType"],
                    volume_arrow_type=item["volumeArrowType"],
                    market_data_schema_version=item["marketDataSchemaVersion"],
                    calculation_model_version=item["calculationModelVersion"],
                    market_rules_version=item["marketRulesVersion"],
                    accounting_rules_version=item["accountingRulesVersion"],
                    precision_rules_version=item.get("precisionRulesVersion", PRECISION_RULES_VERSION),
                    fee_policy_id=item["feePolicyId"],
                    buying_power_buffer_policy_id=item["buyingPowerBufferPolicyId"],
                    good_till_cancelled_horizon=timedelta(seconds=item["goodTillCancelledHorizonSeconds"]),
                    max_order_horizon=timedelta(seconds=item["maxOrderHorizonSeconds"]),
                )
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"execution policy catalog is invalid: {exc}") from exc
    return ExecutionPolicyCatalog(policies)


@dataclass(frozen=True, slots=True)
class RuntimePolicy:
    """Validated values loaded from the checksum-pinned worker policy file."""

    attempt: AttemptPolicy
    microstructure: ExecutionMicrostructurePolicy
    fractional: InstrumentFractionalPolicy
    risk_limits: RiskLimits


def load_runtime_policy(path: Path) -> RuntimePolicy:
    document = _read_document(path, "runtime policy")
    if document.get("schemaVersion") != 1:
        raise ConfigurationError("runtime policy schemaVersion must be 1")
    try:
        attempt = document["attempt"]
        micro = document["microstructure"]
        fractional = document["fractional"]
        risks = document["riskLimits"]
        attempt_policy = AttemptPolicy(
            max_attempts=int(attempt["maxAttempts"]),
            lease_duration=timedelta(seconds=int(attempt["leaseDurationSeconds"])),
            attempt_timeout=timedelta(seconds=int(attempt["attemptTimeoutSeconds"])),
            max_cpu_time=timedelta(seconds=int(attempt["maxCpuTimeSeconds"])),
            max_memory_bytes=int(attempt["maxMemoryBytes"]),
        )
        attempt_values = {
            "maxAttempts": attempt_policy.max_attempts,
            "leaseDurationSeconds": int(attempt_policy.lease_duration.total_seconds()),
            "attemptTimeoutSeconds": int(attempt_policy.attempt_timeout.total_seconds()),
            "maxCpuTimeSeconds": int(attempt_policy.max_cpu_time.total_seconds()),
            "maxMemoryBytes": attempt_policy.max_memory_bytes,
        }
        for name, value in attempt_values.items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        microstructure = ExecutionMicrostructurePolicy(
            version=micro["version"],
            max_volume_participation_bps=int(micro["maxVolumeParticipationBps"]),
            buying_power_buffer_policy_id=micro["buyingPowerBufferPolicyId"],
            buying_power_buffer_bps=int(micro["buyingPowerBufferBps"]),
        )
        fractional_policy = InstrumentFractionalPolicy(
            policy_version=fractional["policyVersion"],
            fractional_instrument_ids=frozenset(fractional["instrumentIds"]),
        )
        risk_limits = RiskLimits(
            max_strategy_notional=Decimal(risks["maxStrategyNotional"]),
            max_gross_exposure=Decimal(risks["maxGrossExposure"]),
            max_instrument_exposure=Decimal(risks["maxInstrumentExposure"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigurationError(f"runtime policy is invalid: {exc}") from exc
    return RuntimePolicy(attempt_policy, microstructure, fractional_policy, risk_limits)


def api_authenticator(environ: Mapping[str, str] = os.environ) -> PostgresSessionAuthenticator:
    encoded = _required(environ, "BACKTEST_SESSION_HMAC_KEY_BASE64")
    try:
        hmac_key = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ConfigurationError("BACKTEST_SESSION_HMAC_KEY_BASE64 is not valid base64") from exc
    return PostgresSessionAuthenticator(
        _engine(environ),
        hmac_key=hmac_key,
        result_token=_required(environ, "BACKTEST_RESULT_INGEST_TOKEN"),
        result_principal_id=uuid.UUID(_required(environ, "BACKTEST_RESULT_PRINCIPAL_ID")),
    )


def s3_object_store(environ: Mapping[str, str] = os.environ) -> S3ObjectStore:
    return S3ObjectStore(
        _required(environ, "BACKTEST_RESULTS_BUCKET"),
        prefix=environ.get("BACKTEST_RESULTS_PREFIX", "backtest-results"),
        endpoint_url=service_endpoint(environ, "S3"),
    )


def postgres_owner_directory(
    environ: Mapping[str, str] = os.environ,
) -> PostgresOwnerDirectory:
    return PostgresOwnerDirectory(_engine(environ))


def postgres_compiled_plan_source(
    environ: Mapping[str, str] = os.environ,
) -> PostgresCompiledPlanSource:
    return PostgresCompiledPlanSource(_engine(environ))


def postgres_dataset_manifest_source(
    environ: Mapping[str, str] = os.environ,
) -> PostgresDatasetManifestSource:
    return PostgresDatasetManifestSource(_engine(environ))


def execution_policy_catalog(
    environ: Mapping[str, str] = os.environ,
) -> ExecutionPolicyCatalog:
    return load_execution_policy_catalog(Path(_required(environ, "BACKTEST_EXECUTION_POLICY_FILE")))


def sqs_dead_letter_sink(environ: Mapping[str, str] = os.environ) -> SqsDeadLetterSink:
    import boto3

    client = boto3.client(
        "sqs",
        endpoint_url=service_endpoint(environ, "SQS"),
        region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
    )
    return SqsDeadLetterSink(client, _required(environ, "BACKTEST_API_DLQ_URL"))


def postgres_execution_key_store(
    environ: Mapping[str, str] = os.environ,
) -> PersistenceExecutionKeyStore:
    return PersistenceExecutionKeyStore(BacktestPersistence(_engine(environ)))


def postgres_request_receipt_store(
    environ: Mapping[str, str] = os.environ,
) -> PostgresRequestReceiptStore:
    """Durable Custom/Competition message receipt and sequence CAS."""

    return PostgresRequestReceiptStore(_engine(environ))


def backtest_request_handler(
    environ: Mapping[str, str] = os.environ,
) -> BacktestRequestJobPublisher:
    """Build the durable provider-envelope to execution-job adapter."""

    import boto3

    client = boto3.client(
        "sqs",
        endpoint_url=service_endpoint(environ, "SQS"),
        region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
    )
    return BacktestRequestJobPublisher(
        PostgresQueuedRunSource(_engine(environ)),
        SqsExecutionJobQueue(
            client,
            {
                RequestLane.BASIC: _required(environ, "BACKTEST_BASIC_QUEUE_URL"),
                RequestLane.CUSTOM: _required(environ, "BACKTEST_CUSTOM_QUEUE_URL"),
                RequestLane.COMPETITION: _required(
                    environ, "BACKTEST_COMPETITION_QUEUE_URL"
                ),
            },
        ),
    )


def _market_reader(environ: Mapping[str, str]) -> S3ParquetMarketDataReader:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=service_endpoint(environ, "S3"),
        region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
    )
    return S3ParquetMarketDataReader(
        bucket=_required(environ, "BACKTEST_MARKET_DATA_BUCKET"),
        cache_root=Path(_required(environ, "BACKTEST_MARKET_DATA_CACHE")),
        client=client,
        batch_size=int(environ.get("BACKTEST_MARKET_DATA_BATCH_SIZE", "65536")),
    )


def _feature_object_reader(environ: Mapping[str, str]) -> S3VersionedFeatureObjectReader:
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=service_endpoint(environ, "S3"),
        region_name=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION"),
    )
    return S3VersionedFeatureObjectReader(client)


def orchestrator_job_handler(
    environ: Mapping[str, str] = os.environ,
) -> OrchestratorJobHandler:
    policy = load_runtime_policy(Path(_required(environ, "BACKTEST_RUNTIME_POLICY_FILE")))

    engine = _engine(environ)
    persistence = BacktestPersistence(engine)
    return OrchestratorJobHandler(
        persistence=persistence,
        policies=execution_policy_catalog(environ),
        plans=PostgresCompiledPlanSource(engine),
        manifests=PostgresDatasetManifestSource(engine),
        feature_materializations=PostgresFeatureMaterializationSource(engine),
        feature_object_reader=_feature_object_reader(environ),
        reader=_market_reader(environ),
        calendar=XNYS_CALENDAR,
        object_store=s3_object_store(environ),
        storage_write_port=PersistenceStorageObjectWritePort(persistence),
        sink=HttpResultSink(
            _required(environ, "BACKTEST_API_BASE_URL"),
            _required(environ, "BACKTEST_RESULT_INGEST_TOKEN"),
            timeout_seconds=float(environ.get("BACKTEST_RESULT_TIMEOUT_SECONDS", "10")),
        ),
        attempt_policy=policy.attempt,
        monitor=ProcessResourceMonitor(),
        microstructure=policy.microstructure,
        fractional_policy=policy.fractional,
        risk_limits=policy.risk_limits,
        runtime=BasicPlanRuntime(),
        wall_clock=lambda: datetime.now(UTC),
        correlation_id=_required(environ, "BACKTEST_WORKER_CORRELATION_ID"),
    )
