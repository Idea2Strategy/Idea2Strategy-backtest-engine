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
from collections.abc import Callable, Iterator, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .contracts import ContractValidationError, validate_dataset_manifest
from .execution_policy import ExecutionPolicy
from .legacy_market_data import (
    LEGACY_MARKET_SCHEMA_ID,
    is_legacy_market_loader_manifest,
    legacy_period_matches,
    validate_legacy_market_loader_manifest,
)
from .object_store.paths import long_path


__all__ = [
    "READABLE_MANIFEST_STATUS",
    "MarketDataValidationError",
    "ParquetMarketDataReader",
]


class MarketDataValidationError(ValueError):
    """Raised when pinned market data cannot be consumed without substitution."""


#: The only ``storage.objects`` / manifest state an official run may read.
READABLE_MANIFEST_STATUS = "AVAILABLE"

DEFAULT_BATCH_SIZE = 65_536
_HASH_CHUNK_SIZE = 1024 * 1024
_OrderState = tuple[tuple[str, int], tuple[int, str], bool, bool]


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
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        self.object_root = object_root.expanduser().resolve()
        self._manifest_validator = manifest_validator
        self._batch_size = batch_size

    def _object_path(self, object_key: object) -> Path:
        if not isinstance(object_key, str) or not object_key:
            raise MarketDataValidationError("object_key must be a non-empty string")
        candidate = (self.object_root / object_key).resolve()
        try:
            candidate.relative_to(self.object_root)
        except ValueError as exc:
            raise MarketDataValidationError("object_key escapes object root") from exc
        filesystem_path = Path(long_path(candidate))
        if not filesystem_path.is_file():
            raise MarketDataValidationError(f"object missing: {object_key}")
        return filesystem_path

    @staticmethod
    def _verify_schema(schema: pa.Schema, policy: ExecutionPolicy) -> None:
        metadata = schema.metadata or {}
        actual_version = metadata.get(b"schema_version", b"").decode("ascii", "replace")
        if actual_version != policy.market_data_schema_version:
            raise MarketDataValidationError("Parquet schema_version does not match policy")
        for name, expected_type in ParquetMarketDataReader.REQUIRED_FIELDS.items():
            field_index = schema.get_field_index(name)
            if field_index < 0:
                raise MarketDataValidationError(f"Parquet schema missing field: {name}")
            field = schema.field(field_index)
            if field.type != expected_type or field.nullable:
                raise MarketDataValidationError(
                    f"Parquet field {name} must be non-nullable {expected_type}"
                )

    @staticmethod
    def _verify_batch_rows(
        batch: pa.RecordBatch,
        policy: ExecutionPolicy,
        order_state: _OrderState | None,
    ) -> _OrderState | None:
        instrument_values = batch.column(
            batch.schema.get_field_index("instrument_id")
        ).to_pylist()
        timestamp_values = batch.column(
            batch.schema.get_field_index("bar_start_at")
        ).cast(pa.int64()).to_pylist()
        session_dates = batch.column(
            batch.schema.get_field_index("session_date_et")
        ).to_pylist()
        period_start_micros = int(policy.period_start.timestamp() * 1_000_000)
        period_end_micros = int(policy.period_end.timestamp() * 1_000_000)
        zone = ZoneInfo(policy.timezone)
        if order_state is None:
            previous_instrument_key = None
            previous_time_key = None
            instrument_major = True
            time_major = True
        else:
            (
                previous_instrument_key,
                previous_time_key,
                instrument_major,
                time_major,
            ) = order_state
        for instrument_id, timestamp_micros, session_date in zip(
            instrument_values, timestamp_values, session_dates, strict=True
        ):
            instrument_key = (str(instrument_id), timestamp_micros)
            time_key = (timestamp_micros, str(instrument_id))
            if previous_instrument_key is not None and instrument_key <= previous_instrument_key:
                instrument_major = False
            if previous_time_key is not None and time_key <= previous_time_key:
                time_major = False
            if not instrument_major and not time_major:
                raise MarketDataValidationError(
                    "rows must be uniquely ordered by instrument_id, bar_start_at "
                    "or by bar_start_at, instrument_id"
                )
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
            previous_instrument_key = instrument_key
            previous_time_key = time_key
        if previous_instrument_key is None or previous_time_key is None:
            return order_state
        return (
            previous_instrument_key,
            previous_time_key,
            instrument_major,
            time_major,
        )

    @staticmethod
    def _content_hash(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            while chunk := source.read(_HASH_CHUNK_SIZE):
                digest.update(chunk)
        return digest.hexdigest()

    def _parquet_file(
        self,
        metadata: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> pq.ParquetFile:
        expected_kind = (
            "MARKET_BARS"
            if policy.market_data_schema_version == LEGACY_MARKET_SCHEMA_ID
            else "PARQUET"
        )
        if metadata.get("object_kind") != expected_kind:
            raise MarketDataValidationError(f"object_kind must be {expected_kind}")
        if metadata.get("schema_version") != policy.market_data_schema_version:
            raise MarketDataValidationError("manifest schema_version does not match policy")
        path = self._object_path(metadata.get("object_key"))
        actual_hash = self._content_hash(path)
        if actual_hash != metadata.get("content_hash"):
            raise MarketDataValidationError("object content_hash does not match manifest")
        try:
            parquet = pq.ParquetFile(path)
        except Exception as exc:
            raise MarketDataValidationError("object is not readable Parquet") from exc
        if parquet.metadata.num_rows != metadata.get("row_count"):
            raise MarketDataValidationError("Parquet row_count does not match manifest")
        self._verify_schema(parquet.schema_arrow, policy)
        return parquet

    def _validate_manifest(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> None:
        legacy = is_legacy_market_loader_manifest(manifest)
        try:
            if legacy:
                validate_legacy_market_loader_manifest(manifest)
            else:
                validate_dataset_manifest(manifest)
        except (ContractValidationError, ValueError) as exc:
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
        if legacy:
            if not legacy_period_matches(
                manifest,
                policy.period_start,
                policy.period_end,
                policy.timezone,
            ):
                raise MarketDataValidationError("legacy manifest period does not match policy")
        else:
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

    def iter_batches(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> Iterator[pa.RecordBatch]:
        """Yield verified bounded batches without loading an object as one byte string.

        Hash verification is deliberately a streaming first pass. Parquet decoding is
        then bounded by ``batch_size``. Producer objects are canonically ordered
        by either ``instrument_id, bar_start_at`` or ``bar_start_at, instrument_id``;
        the event clock later forms the global time order without forcing this reader
        to materialize the dataset.
        """
        self._validate_manifest(manifest, policy)
        schema: pa.Schema | None = None
        parquets: list[pq.ParquetFile] = []
        for metadata in manifest["objects"]:
            parquet = self._parquet_file(metadata, policy)
            object_schema = parquet.schema_arrow
            if schema is None:
                schema = object_schema
            # Producers may attach shard-local provenance metadata. Each object
            # already proves the required schema_version above; the logical
            # Arrow fields, not unrelated file metadata, must match across the stream.
            elif not schema.equals(object_schema, check_metadata=False):
                raise MarketDataValidationError("Parquet object schemas do not match")
            parquets.append(parquet)

        if schema is None:  # pragma: no cover - both manifest contracts require objects
            return

        for parquet in parquets:
            order_state: _OrderState | None = None
            try:
                for batch in parquet.iter_batches(batch_size=self._batch_size):
                    order_state = self._verify_batch_rows(batch, policy, order_state)
                    yield batch
            except MarketDataValidationError:
                raise
            except Exception as exc:
                raise MarketDataValidationError("object is not readable Parquet") from exc

    def read(
        self,
        manifest: Mapping[str, Any],
        policy: ExecutionPolicy,
    ) -> pa.Table:
        """Compatibility materialization for callers not yet migrated to batches."""
        batches = list(self.iter_batches(manifest, policy))
        if not batches:
            raise MarketDataValidationError("the pinned dataset contains no rows")
        try:
            return pa.Table.from_batches(batches)
        except Exception as exc:
            raise MarketDataValidationError("Parquet object schemas do not match") from exc
