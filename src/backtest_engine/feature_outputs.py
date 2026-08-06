"""Fail-closed binding of pinned historical feature materializations.

This module implements the consumer side of the isolated, unapproved
``feature-series.parquet.v1`` proposal.  It deliberately owns no fallback
calculator: once a production job opts into this binder, every required
feature/instrument tuple must resolve to one immutable, version-addressed
object whose bytes and decoded result reproduce the database pins.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from itertools import pairwise
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from backtest_engine.basic_runtime import BasicCompiledPlan, RequiredFeature
from backtest_engine.elements import PinnedFeatureSeries, PinnedFeatureValue


__all__ = [
    "FEATURE_SERIES_SCHEMA",
    "FEATURE_SERIES_SCHEMA_VERSION",
    "FeatureMaterializationSource",
    "FeatureObjectReader",
    "FeatureOutputBindingError",
    "resolve_feature_materialization_pins",
]


FEATURE_SERIES_SCHEMA_VERSION = "feature-series.parquet.v1"
PROJECT_UUID_NAMESPACE = uuid.UUID("05a27d5a-75d8-4d57-bc9a-31cedf90d791")
INTERNAL_PROVIDER_ID = "b9146ed9-dbb0-5323-93e3-8518f3851236"
INTERNAL_PROVIDER_CODE = "IDEA2STRATEGY_INTERNAL"
INTERNAL_PROVIDER_DISPLAY_NAME = "Idea2Strategy Derived Data"
INTERNAL_PROVIDER_RIGHTS_VERSION = "internal-derived-v1"
OFFICIAL_RSI_DEFINITION_HASH = "sha256:1a7c3e5b9d2f4068a1c3e5b7d9f20416283a5c7e9b1d3f50627496a8c0e2b4d6"
OFFICIAL_RSI_FEED_ID = "063f8f27-5c6a-5348-b2bb-abc3c634149c"
OFFICIAL_RSI_FEED_CODE = "FEATURE_RSI_14_1M_RSI_1_0_0"
OFFICIAL_RSI_FEED_VERSION = "rsi-1.0.0+feature-series.parquet.v1"
FEATURE_SERIES_SCHEMA = pa.schema(
    [
        pa.field("bar_start_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("value", pa.decimal128(38, 8), nullable=False),
    ]
)


class FeatureOutputBindingError(ValueError):
    """A pinned feature output cannot safely become a runtime input."""


class FeatureMaterializationSource(Protocol):
    def by_id(self, materialization_id: Any) -> Mapping[str, Any] | None: ...


class FeatureObjectReader(Protocol):
    def read_version(
        self, provider: str, bucket: str, key: str, version_id: str
    ) -> bytes: ...


def _utc(value: Any, label: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise FeatureOutputBindingError(f"{label} is not a timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise FeatureOutputBindingError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _hash(value: Any, label: str) -> str:
    digest = str(value).removeprefix("sha256:")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise FeatureOutputBindingError(f"{label} must be a lowercase SHA-256 digest")
    return digest


def _uuid_text(value: Any, label: str) -> str:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError) as exc:
        raise FeatureOutputBindingError(f"{label} must be a UUID") from exc


def _expected_feature_feed_id(record: Mapping[str, Any]) -> str:
    definition_hash = str(record.get("definition_hash") or "")
    digest = _hash(definition_hash, "definition hash")
    if definition_hash != f"sha256:{digest}":
        raise FeatureOutputBindingError("definition hash must use the canonical sha256: prefix")
    if definition_hash != OFFICIAL_RSI_DEFINITION_HASH:
        raise FeatureOutputBindingError("feature definition hash does not match the official RSI adapter")
    calculator_version = str(record.get("calculator_version") or "")
    resolution = str(record.get("resolution") or "")
    if not calculator_version or not resolution:
        raise FeatureOutputBindingError(
            "feature feed identity requires calculator version and resolution"
        )
    identity = "|".join(
        (
            "feature-output-feed",
            definition_hash,
            calculator_version,
            resolution,
            FEATURE_SERIES_SCHEMA_VERSION,
        )
    )
    expected = str(uuid.uuid5(PROJECT_UUID_NAMESPACE, identity))
    if expected != OFFICIAL_RSI_FEED_ID:
        raise FeatureOutputBindingError("feature feed identity inputs do not match the official RSI adapter")
    return OFFICIAL_RSI_FEED_ID


def _require_output_provenance(record: Mapping[str, Any], requirement: RequiredFeature) -> None:
    if _uuid_text(record.get("output_provider_id"), "output provider id") != INTERNAL_PROVIDER_ID:
        raise FeatureOutputBindingError("feature output provider identity does not match the internal provider")
    if str(record.get("output_provider_code")) != INTERNAL_PROVIDER_CODE:
        raise FeatureOutputBindingError("feature output provider code does not match")
    if str(record.get("output_provider_display_name")) != INTERNAL_PROVIDER_DISPLAY_NAME:
        raise FeatureOutputBindingError("feature output provider display name does not match")
    if str(record.get("output_provider_rights_version")) != INTERNAL_PROVIDER_RIGHTS_VERSION:
        raise FeatureOutputBindingError("feature output provider rights version does not match")
    if str(record.get("output_provider_status")) != "ACTIVE":
        raise FeatureOutputBindingError("feature output provider must be ACTIVE")
    if str(record.get("output_feed_code")) != OFFICIAL_RSI_FEED_CODE:
        raise FeatureOutputBindingError("feature output feed code does not match the official RSI adapter")
    if str(record.get("output_feed_data_kind")) != "FEATURE_SERIES":
        raise FeatureOutputBindingError("feature output feed must use FEATURE_SERIES data kind")
    if str(record.get("output_feed_resolution")) != requirement.bar_resolution:
        raise FeatureOutputBindingError("feature output feed resolution does not match the plan")
    if str(record.get("output_feed_timezone")) != "UTC":
        raise FeatureOutputBindingError("feature output feed timezone must be UTC")
    if str(record.get("output_feed_version")) != OFFICIAL_RSI_FEED_VERSION:
        raise FeatureOutputBindingError("feature output feed version does not match the official RSI adapter")
    if record.get("output_feed_retired_at") is not None:
        raise FeatureOutputBindingError("feature output feed is retired")


def _canonical_result_hash(record: Mapping[str, Any], values: Sequence[PinnedFeatureValue]) -> str:
    rows = [{"at": _utc_text(item.bar_start_at), "value": format(item.value, "f")} for item in values]
    payload = {
        "definition_hash": _hash(record.get("definition_hash"), "definition hash"),
        "input_dataset_set_hash": _hash(record.get("input_dataset_set_hash"), "input dataset set hash"),
        "instrument_id": _uuid_text(record.get("instrument_id"), "instrument id"),
        "period_end": _utc_text(_utc(record.get("period_end"), "period_end")),
        "period_start": _utc_text(_utc(record.get("period_start"), "period_start")),
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


def _require_metadata(
    record: Mapping[str, Any],
    requirement: RequiredFeature,
    *,
    materialization_id: Any,
    instrument_id: str,
    locked_result_hash: str,
    evaluation_from: datetime,
    evaluation_through: datetime,
) -> Mapping[str, Any]:
    if _uuid_text(record.get("id"), "resolved materialization id") != _uuid_text(
        materialization_id, "pinned materialization id"
    ):
        raise FeatureOutputBindingError("resolved materialization id does not match the pin")
    if str(record.get("status")) != "SUCCEEDED":
        raise FeatureOutputBindingError(f"feature materialization {materialization_id} must be SUCCEEDED")
    actual_result = _hash(record.get("result_hash"), "materialization result hash")
    if actual_result != _hash(locked_result_hash, "locked result hash"):
        raise FeatureOutputBindingError(f"feature materialization {materialization_id} result hash changed")
    if _uuid_text(record.get("feature_definition_id"), "feature definition id") != requirement.feature_id:
        raise FeatureOutputBindingError("feature definition does not match the compiled plan")
    if str(record.get("feature_code")) != requirement.feature_key:
        raise FeatureOutputBindingError("feature code does not match the compiled plan")
    calculator_version = str(record.get("calculator_version"))
    if calculator_version != requirement.definition_version:
        raise FeatureOutputBindingError("feature semantic version does not match the compiled plan")
    if str(record.get("resolution")) != requirement.bar_resolution:
        raise FeatureOutputBindingError("feature resolution does not match the compiled plan")
    if _uuid_text(record.get("instrument_id"), "instrument id") != instrument_id:
        raise FeatureOutputBindingError("feature instrument does not match the compiled plan")

    period_start = _utc(record.get("period_start"), "period_start")
    period_end = _utc(record.get("period_end"), "period_end")
    if period_start > evaluation_from - requirement.warmup_span:
        raise FeatureOutputBindingError("feature period does not cover the required warm-up")
    if period_end < evaluation_through:
        raise FeatureOutputBindingError("feature period does not cover the full evaluation window")

    if record.get("output_dataset_manifest_id") is None:
        raise FeatureOutputBindingError("feature output dataset manifest is missing")
    if _uuid_text(record.get("output_dataset_feed_id"), "output dataset feed id") != (
        _expected_feature_feed_id(record)
    ):
        raise FeatureOutputBindingError(
            "feature output dataset feed identity does not match the definition"
        )
    _require_output_provenance(record, requirement)
    if _uuid_text(
        record.get("output_dataset_instrument_id"), "output dataset instrument id"
    ) != instrument_id:
        raise FeatureOutputBindingError("feature output dataset instrument does not match the plan")
    if str(record.get("output_dataset_status")) != "AVAILABLE":
        raise FeatureOutputBindingError("feature output dataset must be AVAILABLE")
    if str(record.get("output_dataset_layer")) != "DERIVED":
        raise FeatureOutputBindingError("feature output dataset must use the DERIVED layer")
    if str(record.get("output_dataset_schema")) != FEATURE_SERIES_SCHEMA_VERSION:
        raise FeatureOutputBindingError("feature output dataset schema is not feature-series.parquet.v1")
    if str(record.get("output_dataset_resolution")) != requirement.bar_resolution:
        raise FeatureOutputBindingError("feature output dataset resolution does not match the plan")

    objects = tuple(record.get("objects") or ())
    if len(objects) != 1:
        raise FeatureOutputBindingError("a feature output manifest must contain exactly one object")
    return objects[0]


def _decode_object(
    record: Mapping[str, Any],
    object_record: Mapping[str, Any],
    reader: FeatureObjectReader,
) -> tuple[PinnedFeatureValue, ...]:
    if str(object_record.get("object_kind")) != "FEATURE_SERIES":
        raise FeatureOutputBindingError("feature dataset object kind must be FEATURE_SERIES")
    if str(object_record.get("status")) != "AVAILABLE":
        raise FeatureOutputBindingError("feature storage object must be AVAILABLE")
    provider = str(object_record.get("storage_provider") or "")
    if not provider:
        raise FeatureOutputBindingError("feature storage object requires a provider")
    if str(object_record.get("file_format")) != "PARQUET":
        raise FeatureOutputBindingError("feature storage object must be PARQUET")
    if str(object_record.get("schema_version")) != FEATURE_SERIES_SCHEMA_VERSION:
        raise FeatureOutputBindingError("feature storage object schema is not feature-series.parquet.v1")

    bucket = str(object_record.get("bucket_name") or "")
    key = str(object_record.get("object_key") or "")
    version_id = str(object_record.get("provider_version_id") or "")
    if not bucket or not key or not version_id:
        raise FeatureOutputBindingError("feature object requires bucket, key, and provider version")
    try:
        body = reader.read_version(provider, bucket, key, version_id)
    except Exception as exc:
        raise FeatureOutputBindingError("versioned feature object is unavailable") from exc
    if not isinstance(body, bytes):
        raise FeatureOutputBindingError("versioned feature object reader must return bytes")
    expected_content_hash = _hash(object_record.get("content_hash"), "object content hash")
    if hashlib.sha256(body).hexdigest() != expected_content_hash:
        raise FeatureOutputBindingError("versioned feature object content hash does not match")
    try:
        raw_size = object_record.get("byte_size")
        if raw_size is None:
            raise TypeError("missing")
        expected_size = int(raw_size)
    except (TypeError, ValueError) as exc:
        raise FeatureOutputBindingError("feature object byte size is invalid") from exc
    if len(body) != expected_size:
        raise FeatureOutputBindingError("versioned feature object byte size does not match")

    try:
        table = pq.read_table(pa.BufferReader(body))
    except Exception as exc:
        raise FeatureOutputBindingError("feature object is not readable Parquet") from exc
    if table.schema != FEATURE_SERIES_SCHEMA:
        raise FeatureOutputBindingError(f"feature Parquet schema mismatch: {table.schema} != {FEATURE_SERIES_SCHEMA}")
    if table.num_rows <= 0:
        raise FeatureOutputBindingError("feature Parquet must contain at least one row")
    if table.column("bar_start_at").null_count or table.column("value").null_count:
        raise FeatureOutputBindingError("feature Parquet columns must not contain nulls")
    try:
        raw_rows = object_record.get("row_count")
        if raw_rows is None:
            raise TypeError("missing")
        expected_rows = int(raw_rows)
    except (TypeError, ValueError) as exc:
        raise FeatureOutputBindingError("feature object row count is invalid") from exc
    if table.num_rows != expected_rows:
        raise FeatureOutputBindingError("feature object row count does not match")

    moments = table.column("bar_start_at").to_pylist()
    decimals = table.column("value").to_pylist()
    values = tuple(
        PinnedFeatureValue(bar_start_at=moment, value=value) for moment, value in zip(moments, decimals, strict=True)
    )
    if any(current.bar_start_at <= previous.bar_start_at for previous, current in pairwise(values)):
        raise FeatureOutputBindingError("feature values must be strictly increasing and unique by bar_start_at")
    if _canonical_result_hash(record, values) != _hash(record.get("result_hash"), "result hash"):
        raise FeatureOutputBindingError("decoded result hash does not match the materialization")
    return values


def resolve_feature_materialization_pins(
    *,
    plan: BasicCompiledPlan,
    pins: Sequence[Any],
    source: FeatureMaterializationSource,
    reader: FeatureObjectReader,
    evaluation_from: datetime,
    evaluation_through: datetime,
) -> tuple[PinnedFeatureSeries, ...]:
    """Resolve the exact ``requiredFeatures x instruments`` join.

    Missing, duplicate and extra tuples are all distinct refusals.  The caller
    receives only decoded series that passed metadata, versioned-byte, schema,
    ordering, content-hash and result-hash validation.
    """

    required: dict[tuple[str, str], RequiredFeature] = {}
    for feature in plan.required_features:
        for instrument_id in feature.instruments:
            key = (feature.feature_id, instrument_id)
            if key in required:  # The plan loader already rejects this; retain a local guard.
                raise FeatureOutputBindingError("compiled plan contains a duplicate feature tuple")
            required[key] = feature

    resolved: dict[tuple[str, str], PinnedFeatureSeries] = {}
    for pin in pins:
        materialization_id = getattr(pin, "materialization_id", None)
        locked_result_hash = getattr(pin, "locked_result_hash", None)
        record = source.by_id(materialization_id)
        if record is None:
            raise FeatureOutputBindingError(f"feature materialization {materialization_id} is missing")
        key = (
            _uuid_text(record.get("feature_definition_id"), "feature definition id"),
            _uuid_text(record.get("instrument_id"), "instrument id"),
        )
        requirement = required.get(key)
        if requirement is None:
            raise FeatureOutputBindingError(f"feature materialization tuple {key} is extra")
        if key in resolved:
            raise FeatureOutputBindingError(f"feature materialization tuple {key} is duplicate")
        object_record = _require_metadata(
            record,
            requirement,
            materialization_id=materialization_id,
            instrument_id=key[1],
            locked_result_hash=str(locked_result_hash),
            evaluation_from=_utc(evaluation_from, "evaluation_from"),
            evaluation_through=_utc(evaluation_through, "evaluation_through"),
        )
        resolved[key] = PinnedFeatureSeries(
            feature_id=requirement.feature_key,
            instrument_id=key[1],
            resolution=requirement.bar_resolution,
            values=_decode_object(record, object_record, reader),
        )

    missing = sorted(set(required) - set(resolved))
    if missing:
        raise FeatureOutputBindingError(f"required feature materialization tuples are missing: {missing}")
    return tuple(resolved[key] for key in sorted(resolved))
