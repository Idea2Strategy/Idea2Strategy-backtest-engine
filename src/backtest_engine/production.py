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
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic, sleep
from typing import Any, BinaryIO
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from sqlalchemy import Engine, text

from backtest_engine.api import RESULT_INGEST_SCOPE, Principal
from backtest_engine.attempt_coordinator import AttemptPolicy, ProcessResourceMonitor
from backtest_engine.backtest_request_intake import PostgresRequestReceiptStore, RequestLane
from backtest_engine.basic_runtime import BasicPlanRuntime
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.elements import bar_resolution
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
    PinnedDataset,
    PinnedFeatureMaterialization,
    QueuedRunProjection,
)
from backtest_engine.wiring import (
    JobNotSatisfiable,
    OrchestratorJobHandler,
    PersistenceExecutionKeyStore,
    PersistenceStorageObjectWritePort,
)


class ConfigurationError(RuntimeError):
    """A production process cannot safely start with the supplied configuration."""


class MonotonicUtcClock:
    """UTC instants that cannot regress when the host wall clock is corrected."""

    def __init__(
        self,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        source = wall_clock or (lambda: datetime.now(UTC))
        self._monotonic = monotonic_clock or monotonic
        self._anchor = source()
        self._started = self._monotonic()
        self._elapsed = 0.0

    def __call__(self) -> datetime:
        self._elapsed = max(self._monotonic() - self._started, self._elapsed)
        return self._anchor + timedelta(seconds=self._elapsed)


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name, "").strip()
    if not value:
        raise ConfigurationError(f"missing required environment setting: {name}")
    return value


def _required_uuid(environ: Mapping[str, str], name: str) -> str:
    value = _required(environ, name)
    try:
        return str(uuid.UUID(value))
    except ValueError as exc:
        raise ConfigurationError(f"{name} must be a UUID") from exc


def service_endpoint(environ: Mapping[str, str], service: str) -> str | None:
    """Resolve an emulator endpoint without coupling S3 and SQS together.

    ``AWS_ENDPOINT_URL`` remains the backwards-compatible fallback used by a
    single-service emulator. Local development uses MinIO for S3 and LocalStack
    for SQS, so the service-specific variables must win when present.
    """

    return environ.get(f"AWS_ENDPOINT_URL_{service.upper()}") or environ.get("AWS_ENDPOINT_URL")


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


def _unavailable_manifest(message: str) -> JobNotSatisfiable:
    return JobNotSatisfiable(message, reason_code="REQUIRED_INPUT_UNAVAILABLE")


def _dataset_id(
    object_keys: list[str], manifest_id: uuid.UUID, *, is_composite: bool = False
) -> str:
    bindings: list[tuple[str, str]] = []
    for key in object_keys:
        key_bindings = [
            (name, segment.removeprefix(f"{name}="))
            for segment in key.split("/")
            for name in ("dataset", "manifest_id")
            if segment.startswith(f"{name}=")
        ]
        if len(key_bindings) != 1:
            raise _unavailable_manifest(
                "dataset manifest object keys must each bind exactly one dataset= or manifest_id= UUID"
            )
        bindings.extend(key_bindings)

    conventions = {name for name, _value in bindings}
    values = {value for _name, value in bindings}
    if is_composite:
        if conventions != {"manifest_id"}:
            raise _unavailable_manifest(
                "composite legacy manifests require source manifest_id bindings"
            )
        try:
            for value in values:
                uuid.UUID(value)
        except ValueError as exc:
            raise _unavailable_manifest(
                "composite dataset object key contains an invalid source manifest UUID"
            ) from exc
        return str(manifest_id)
    if len(conventions) != 1 or len(values) != 1:
        raise _unavailable_manifest(
            "dataset manifest object keys must use one binding convention and one UUID"
        )
    convention = conventions.pop()
    value = values.pop()
    try:
        resolved = uuid.UUID(value)
    except ValueError as exc:
        raise _unavailable_manifest(
            f"dataset manifest object key contains an invalid {convention} UUID: {value}"
        ) from exc
    if convention == "manifest_id" and resolved != manifest_id:
        raise _unavailable_manifest(
            f"legacy dataset object key binds manifest {resolved}, expected {manifest_id}"
        )
    return str(resolved)


class PostgresDatasetManifestSource:
    """Rebuild the consumer contract from canonical catalog and storage rows."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def by_id(self, manifest_id: uuid.UUID) -> Mapping[str, Any] | None:
        manifest_sql = text(
            """
            SELECT manifest.id, manifest.instrument_id, manifest.revision_number, manifest.status,
                   manifest.dataset_hash, manifest.schema_version,
                   manifest.period_start, manifest.period_end, manifest.available_at,
                   provider.code AS provider_code, feed.code AS feed_code,
                   manifest.data_layer, feed.resolution AS feed_resolution,
                   EXISTS (
                       SELECT 1 FROM market_data.dataset_lineage lineage
                        WHERE lineage.derived_manifest_id = manifest.id
                          AND lineage.relation_type = 'COMPOSED_FROM'
                   ) AS is_composite
              FROM market_data.dataset_manifests manifest
              JOIN market_data.feeds feed ON feed.id = manifest.feed_id
              JOIN market_data.providers provider ON provider.id = feed.provider_id
             WHERE manifest.id = :manifest_id
            """
        )
        objects_sql = text(
            """
            SELECT o.id AS storage_object_id, o.object_key, o.content_hash,
                   d.object_kind, d.partition_granularity, d.partition_start,
                   d.partition_end, d.period_start, d.period_end, d.shard_key,
                   d.part_number, d.row_count, o.schema_version, o.status,
                   o.storage_provider, o.bucket_name, o.provider_version_id,
                   o.file_format, o.media_type
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
        if row["status"] != "AVAILABLE":
            raise _unavailable_manifest(f"dataset manifest {manifest_id} is not AVAILABLE")
        if not object_rows:
            raise _unavailable_manifest(f"dataset manifest {manifest_id} has no objects")
        if any(item["status"] != "AVAILABLE" for item in object_rows):
            raise _unavailable_manifest(
                f"dataset manifest {manifest_id} references a non-AVAILABLE object"
            )
        if any(
            item["storage_provider"] != "S3"
            or not item["bucket_name"]
            or not item["provider_version_id"]
            for item in object_rows
        ):
            raise _unavailable_manifest(
                f"dataset manifest {manifest_id} lacks immutable S3 version evidence"
            )
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
                "storage_provider": str(item["storage_provider"]),
                "bucket_name": str(item["bucket_name"]),
                "provider_version_id": str(item["provider_version_id"]),
                "file_format": str(item["file_format"]),
                "media_type": str(item["media_type"]),
            }
            for item in object_rows
        ]
        available_at = row["available_at"]
        if available_at is None:
            raise _unavailable_manifest(f"dataset manifest {manifest_id} has no available_at evidence")
        raw_resolution = str(row["feed_resolution"])
        try:
            resolution = bar_resolution(raw_resolution)
        except ValueError:
            resolution = raw_resolution
        if resolution not in {"30m", "1h", "4h", "1d"}:
            raise _unavailable_manifest(
                f"dataset manifest {manifest_id} has unsupported production resolution {raw_resolution}"
            )
        is_composite = bool(row.get("is_composite", False))
        return {
            "contract_id": "com06.dataset-manifest",
            "schema_version": 1,
            "manifest_id": str(row["id"]),
            "instrument_id": str(row.get("instrument_id")) if row.get("instrument_id") else None,
            "dataset_id": _dataset_id(
                [item["object_key"] for item in objects],
                manifest_id,
                is_composite=is_composite,
            ),
            "composite": is_composite,
            "revision": int(row["revision_number"]),
            "status": str(row["status"]),
            "dataset_hash": str(row["dataset_hash"]),
            "schema_id": str(row["schema_version"]),
            "provider_code": str(row["provider_code"]),
            "feed_code": str(row["feed_code"]),
            "data_layer": str(row["data_layer"]),
            "resolution": resolution,
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
                   d.feed_id AS output_dataset_feed_id,
                   output_feed.code AS output_feed_code,
                   output_feed.data_kind AS output_feed_data_kind,
                   output_feed.resolution AS output_feed_resolution,
                   output_feed.timezone_name AS output_feed_timezone,
                   output_feed.feed_version AS output_feed_version,
                   output_feed.retired_at AS output_feed_retired_at,
                   output_provider.id AS output_provider_id,
                   output_provider.code AS output_provider_code,
                   output_provider.display_name AS output_provider_display_name,
                   output_provider.rights_version AS output_provider_rights_version,
                   output_provider.status AS output_provider_status,
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
              LEFT JOIN market_data.feeds output_feed
                ON output_feed.id = d.feed_id
              LEFT JOIN market_data.providers output_provider
                ON output_provider.id = output_feed.provider_id
              LEFT JOIN market_data.dataset_objects rel
                ON rel.dataset_manifest_id = d.id
              LEFT JOIN storage.objects obj
                ON obj.id = rel.object_id
             WHERE f.id = :materialization_id
              GROUP BY f.id, fd.id, d.id, output_feed.id, output_provider.id
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(statement, {"materialization_id": materialization_id}).mappings().first()
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

    def read_version(self, provider: str, bucket: str, key: str, version_id: str) -> bytes:
        if provider != "S3_COMPATIBLE":
            raise ConfigurationError(f"S3 feature reader cannot read storage provider {provider!r}")
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
        run_statement = text(
            """
            SELECT r.id, r.lane::text, r.message_id, r.bot_id, r.owner_account_id,
                   p.input_bundle_id, p.input_bundle_fingerprint,
                   p.input_contract_version, p.compiled_plan_checksum,
                   p.strategy_snapshot_hash, r.aggregate_sequence,
                   r.evaluation_start, r.evaluation_end, r.execution_policy_version
              FROM backtest.runs r
              JOIN backtest.run_input_pins p ON p.run_id = r.id
             WHERE r.id = :run_id
            """
        )
        dataset_statement = text(
            """
            SELECT dataset_manifest_id, purpose_code, locked_dataset_hash
              FROM backtest.input_datasets
             WHERE input_bundle_id = :input_bundle_id
             ORDER BY purpose_code, dataset_manifest_id
            """
        )
        feature_statement = text(
            """
            SELECT feature_materialization_id, locked_result_hash
              FROM backtest.input_feature_materializations
             WHERE input_bundle_id = :input_bundle_id
             ORDER BY feature_materialization_id
            """
        )
        with self._engine.connect() as connection:
            row = connection.execute(run_statement, {"run_id": run_id}).mappings().first()
            if row is None:
                return None
            bundle_id = uuid.UUID(str(row["input_bundle_id"]))
            dataset_rows = connection.execute(dataset_statement, {"input_bundle_id": bundle_id}).mappings().all()
            feature_rows = connection.execute(feature_statement, {"input_bundle_id": bundle_id}).mappings().all()
        try:
            datasets = tuple(
                PinnedDataset(
                    dataset_manifest_id=uuid.UUID(str(item["dataset_manifest_id"])),
                    purpose_code=str(item["purpose_code"]),
                    locked_dataset_hash=str(item["locked_dataset_hash"]),
                )
                for item in dataset_rows
            )
            if not datasets:
                raise ValueError("the canonical input bundle has no datasets")
            return QueuedRunProjection(
                run_id=uuid.UUID(str(row["id"])),
                lane=RequestLane(str(row["lane"])),
                message_id=uuid.UUID(str(row["message_id"])),
                bot_id=uuid.UUID(str(row["bot_id"])),
                owner_account_id=uuid.UUID(str(row["owner_account_id"])),
                input_bundle_id=uuid.UUID(str(row["input_bundle_id"])),
                input_bundle_fingerprint=str(row["input_bundle_fingerprint"]),
                input_contract_version=str(row["input_contract_version"]),
                compiled_plan_checksum=str(row["compiled_plan_checksum"]),
                strategy_snapshot_hash=str(row["strategy_snapshot_hash"]),
                datasets=datasets,
                feature_materializations=tuple(
                    PinnedFeatureMaterialization(
                        feature_materialization_id=uuid.UUID(str(item["feature_materialization_id"])),
                        locked_result_hash=str(item["locked_result_hash"]),
                    )
                    for item in feature_rows
                ),
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


class JwtAuthenticator:
    """Validate customer access JWTs locally and a distinct worker-only token."""

    def __init__(
        self,
        *,
        hmac_key: bytes,
        result_token: str,
        result_principal_id: uuid.UUID,
        issuer: str,
        audience: str,
    ) -> None:
        if len(hmac_key) < 32:
            raise ConfigurationError("CUSTOMER_JWT_SIGNING_KEY_BASE64 must contain at least 32 bytes")
        if not result_token:
            raise ConfigurationError("BACKTEST_RESULT_INGEST_TOKEN must not be empty")
        self._hmac_key = hmac_key
        self._result_token = result_token
        self._result_principal_id = result_principal_id
        self._issuer = issuer
        self._audience = audience

    def authenticate(self, token: str) -> Principal | None:
        if hmac.compare_digest(token, self._result_token):
            return Principal(
                account_id=self._result_principal_id,
                scopes=frozenset({RESULT_INGEST_SCOPE}),
            )
        principal_id = self._verify_customer_access(token)
        if principal_id is None:
            return None
        return Principal(account_id=principal_id)

    def _verify_customer_access(self, token: str) -> uuid.UUID | None:
        try:
            parts = token.split(".")
            if len(parts) != 3 or any(not part for part in parts):
                return None
            header = json.loads(_decode_jwt_part(parts[0]))
            claims = json.loads(_decode_jwt_part(parts[1]))
            if not isinstance(header, dict) or not isinstance(claims, dict):
                return None
            if header != {"alg": "HS256", "typ": "JWT"}:
                return None
            expected = hmac.new(
                self._hmac_key,
                f"{parts[0]}.{parts[1]}".encode("ascii"),
                hashlib.sha256,
            ).digest()
            if not hmac.compare_digest(expected, _decode_jwt_part(parts[2])):
                return None
            issued_at = claims.get("iat")
            expires_at = claims.get("exp")
            now = int(datetime.now(UTC).timestamp())
            if (
                claims.get("iss") != self._issuer
                or claims.get("aud") != self._audience
                or claims.get("typ") != "access"
                or not isinstance(issued_at, int)
                or isinstance(issued_at, bool)
                or not isinstance(expires_at, int)
                or isinstance(expires_at, bool)
                or issued_at > now + 30
                or expires_at <= now
            ):
                return None
            auth_epoch = claims.get("ae")
            credential_version = claims.get("cv")
            if (
                not isinstance(auth_epoch, int)
                or isinstance(auth_epoch, bool)
                or auth_epoch < 1
                or (
                    credential_version is not None
                    and (
                        not isinstance(credential_version, int)
                        or isinstance(credential_version, bool)
                        or credential_version < 1
                    )
                )
            ):
                return None
            uuid.UUID(str(claims["lid"]))
            return uuid.UUID(str(claims["sub"]))
        except (KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError):
            return None


def _decode_jwt_part(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)


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
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        timeout_seconds: float = 10.0,
        max_transport_attempts: int = 3,
        retry_backoff_seconds: float = 0.25,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if not 1 <= max_transport_attempts <= 5:
            raise ValueError("max_transport_attempts must be between 1 and 5")
        if not 0 <= retry_backoff_seconds <= 5:
            raise ValueError("retry_backoff_seconds must be between 0 and 5")
        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds
        self._max_transport_attempts = max_transport_attempts
        self._retry_backoff = retry_backoff_seconds
        self._sleep = sleeper

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
        for transport_attempt in range(1, self._max_transport_attempts + 1):
            try:
                with urlopen(request, timeout=self._timeout) as response:
                    status = response.status
            except HTTPError as exc:
                # The endpoint answered. Retrying a deterministic HTTP response can
                # only duplicate load and must not disguise a rejected event as a
                # transient network outage.
                raise RuntimeError(f"backtest result ingestion failed: {exc}") from exc
            except (URLError, TimeoutError) as exc:
                if transport_attempt == self._max_transport_attempts:
                    raise RuntimeError(
                        "backtest result ingestion failed after "
                        f"{transport_attempt} transport attempts: {exc}"
                    ) from exc
                self._sleep(self._retry_backoff * (2 ** (transport_attempt - 1)))
                continue
            if status != 200:
                raise RuntimeError(f"backtest result ingestion returned HTTP {status}")
            return


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
            storage_provider = str(metadata.get("storage_provider", ""))
            bucket = str(metadata.get("bucket_name", ""))
            version_id = str(metadata.get("provider_version_id", ""))
            if storage_provider != "S3":
                raise ConfigurationError(f"market-data object is not stored in S3: {key}")
            if bucket != self._bucket:
                raise ConfigurationError(f"market-data object bucket does not match configuration: {key}")
            if not version_id:
                raise ConfigurationError(f"market-data object has no immutable provider version: {key}")
            target = self._target(key)
            if target.is_file():
                with target.open("rb") as existing:
                    actual = hashlib.file_digest(existing, "sha256").hexdigest()
                if hmac.compare_digest(actual, expected):
                    continue
                target.unlink()
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.part")
            response = self._client.get_object(Bucket=bucket, Key=key, VersionId=version_id)
            body = response["Body"]
            try:
                actual = self._copy_and_hash(body, temporary)
            finally:
                body.close()
            if not hmac.compare_digest(actual, expected):
                temporary.unlink(missing_ok=True)
                raise ConfigurationError(f"downloaded market-data object checksum does not match: {key}")
            temporary.replace(target)

    def iter_batches(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
        *,
        instrument_ids: frozenset[str] | None = None,
    ) -> Any:
        self.materialize(manifest)
        return self._reader.iter_batches(
            manifest,
            policy,
            instrument_ids=instrument_ids,
        )

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


def api_authenticator(environ: Mapping[str, str] = os.environ) -> JwtAuthenticator:
    encoded = _required(environ, "CUSTOMER_JWT_SIGNING_KEY_BASE64")
    try:
        hmac_key = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise ConfigurationError("CUSTOMER_JWT_SIGNING_KEY_BASE64 is not valid base64") from exc
    return JwtAuthenticator(
        hmac_key=hmac_key,
        result_token=_required(environ, "BACKTEST_RESULT_INGEST_TOKEN"),
        result_principal_id=uuid.UUID(_required(environ, "BACKTEST_RESULT_PRINCIPAL_ID")),
        issuer=environ.get("CUSTOMER_JWT_ISSUER", "https://ideatostrategy.com"),
        audience=environ.get("CUSTOMER_JWT_ACCESS_AUDIENCE", "idea2strategy-api"),
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
                RequestLane.COMPETITION: _required(environ, "BACKTEST_COMPETITION_QUEUE_URL"),
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
    correlation_id = _required_uuid(environ, "BACKTEST_WORKER_CORRELATION_ID")
    policy = load_runtime_policy(Path(_required(environ, "BACKTEST_RUNTIME_POLICY_FILE")))

    engine = _engine(environ)
    persistence = BacktestPersistence(engine)
    wall_clock = MonotonicUtcClock()
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
        wall_clock=wall_clock,
        correlation_id=correlation_id,
    )
