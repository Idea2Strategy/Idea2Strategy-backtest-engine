"""Immutable manifest-backed Parquet input for reproducible backtests.

Contract validation of the manifest -- including the ``dataset_hash``
recomputation -- belongs to :mod:`backtest_engine.contracts`, which owns the
single canonical implementation. This module adds only what a *consumer* needs
on top of a valid manifest:

* the manifest must be ``AVAILABLE``. The contract schema also admits
  ``STAGED``, ``QUARANTINED``, ``SUPERSEDED`` and ``DELETED``, which are
  legitimate producer states but are not readable inputs to an official run;
* every object's bytes must re-hash to its declared ``content_hash``;
* the Parquet schema, row count, ordering and ET session dates must match the
  pinned execution policy.

``ParquetMarketDataReader(..., manifest_validator=...)`` is the seam where a
stricter producer-side validator can be injected during integration.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import ContractValidationError, validate_dataset_manifest
from .execution_policy import ExecutionPolicy


__all__ = [
    "READABLE_MANIFEST_STATUS",
    "MarketDataValidationError",
    "ParquetMarketDataReader",
]


class MarketDataValidationError(ValueError):
    """Raised when pinned market data cannot be consumed without substitution."""


#: The only ``storage.objects`` / manifest state an official run may read.
READABLE_MANIFEST_STATUS = "AVAILABLE"


def _utc_timestamp(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise MarketDataValidationError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MarketDataValidationError(f"{label} must be ISO-8601") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise MarketDataValidationError(f"{label} must be UTC")
    return parsed


class ParquetMarketDataReader:
    """Read only objects whose bytes and metadata match an AVAILABLE manifest."""

    REQUIRED_FIELDS = {
        "instrument_id": pa.string(),
        "provider_symbol": pa.string(),
        "bar_start_at": pa.timestamp("us", tz="UTC"),
        "session_date_et": pa.date32(),
        "open": pa.float64(),
        "high": pa.float64(),
        "low": pa.float64(),
        "close": pa.float64(),
        "volume": pa.int64(),
    }

    def __init__(
        self,
        object_root: Path,
        *,
        manifest_validator: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> None:
        self.object_root = object_root.expanduser().resolve()
        self._manifest_validator = manifest_validator

    def _object_path(self, object_key: object) -> Path:
        if not isinstance(object_key, str) or not object_key:
            raise MarketDataValidationError("object_key must be a non-empty string")
        candidate = (self.object_root / object_key).resolve()
        try:
            candidate.relative_to(self.object_root)
        except ValueError as exc:
            raise MarketDataValidationError("object_key escapes object root") from exc
        if not candidate.is_file():
            raise MarketDataValidationError(f"object missing: {object_key}")
        return candidate

    @staticmethod
    def _verify_schema(table: pa.Table, policy: ExecutionPolicy) -> None:
        metadata = table.schema.metadata or {}
        actual_version = metadata.get(b"schema_version", b"").decode("ascii", "replace")
        if actual_version != policy.market_data_schema_version:
            raise MarketDataValidationError("Parquet schema_version does not match policy")
        for name, expected_type in ParquetMarketDataReader.REQUIRED_FIELDS.items():
            field_index = table.schema.get_field_index(name)
            if field_index < 0:
                raise MarketDataValidationError(f"Parquet schema missing field: {name}")
            field = table.schema.field(field_index)
            if field.type != expected_type or field.nullable:
                raise MarketDataValidationError(
                    f"Parquet field {name} must be non-nullable {expected_type}"
                )

    @staticmethod
    def _verify_rows(table: pa.Table, policy: ExecutionPolicy) -> None:
        timestamp_values = table["bar_start_at"].cast(pa.int64()).to_pylist()
        session_dates = table["session_date_et"].to_pylist()
        if timestamp_values != sorted(timestamp_values):
            raise MarketDataValidationError("bar_start_at must be ordered")
        period_start_micros = int(policy.period_start.timestamp() * 1_000_000)
        period_end_micros = int(policy.period_end.timestamp() * 1_000_000)
        zone = ZoneInfo(policy.timezone)
        for timestamp_micros, session_date in zip(
            timestamp_values, session_dates, strict=True
        ):
            if not period_start_micros <= timestamp_micros < period_end_micros:
                raise MarketDataValidationError("bar_start_at is outside the pinned period")
            timestamp = datetime.fromtimestamp(
                timestamp_micros / 1_000_000,
                tz=policy.period_start.tzinfo,
            )
            if timestamp.astimezone(zone).date() != session_date:
                raise MarketDataValidationError(
                    "session_date_et does not match bar_start_at in policy timezone"
                )

    def _read_object(
        self,
        metadata: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> pa.Table:
        if metadata.get("object_kind") != "PARQUET":
            raise MarketDataValidationError("object_kind must be PARQUET")
        if metadata.get("schema_version") != policy.market_data_schema_version:
            raise MarketDataValidationError("manifest schema_version does not match policy")
        path = self._object_path(metadata.get("object_key"))
        actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual_hash != metadata.get("content_hash"):
            raise MarketDataValidationError("object content_hash does not match manifest")
        try:
            table = pq.read_table(path)
        except Exception as exc:
            raise MarketDataValidationError("object is not readable Parquet") from exc
        if table.num_rows != metadata.get("row_count"):
            raise MarketDataValidationError("Parquet row_count does not match manifest")
        self._verify_schema(table, policy)
        return table

    def read(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> pa.Table:
        try:
            validate_dataset_manifest(manifest)
        except ContractValidationError as exc:
            raise MarketDataValidationError(str(exc)) from exc
        if self._manifest_validator is not None:
            try:
                self._manifest_validator(manifest)
            except MarketDataValidationError:
                raise
            except Exception as exc:
                raise MarketDataValidationError(str(exc)) from exc

        status = manifest.get("status")
        if status != READABLE_MANIFEST_STATUS:
            raise MarketDataValidationError(
                f"dataset_manifest.status must be {READABLE_MANIFEST_STATUS} to be "
                f"consumed by an official run, got {status!r}"
            )
        if manifest.get("schema_id") != policy.market_data_schema_version:
            raise MarketDataValidationError("manifest schema_id does not match policy")
        if (
            _utc_timestamp(manifest.get("period_start"), "manifest.period_start")
            != policy.period_start
        ):
            raise MarketDataValidationError("manifest period_start does not match policy")
        if (
            _utc_timestamp(manifest.get("period_end"), "manifest.period_end")
            != policy.period_end
        ):
            raise MarketDataValidationError("manifest period_end does not match policy")

        objects = manifest["objects"]
        tables = [self._read_object(item, policy) for item in objects]
        table = pa.concat_tables(tables)
        self._verify_rows(table, policy)
        return table
