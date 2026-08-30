"""Fail-closed adapter for immutable ``market-loader/1.0.0`` manifests.

The Development catalog predates the canonical ``market-data.v1`` object
shape. Its rows are still reproducible, but their dataset hash covers the
loader's logical publication material rather than the current canonical object
metadata. This module recognizes only that exact legacy shape and recomputes
the original producer hash. It is deliberately separate from the canonical
validator so accepting the old Development fixture cannot widen the current
contract.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from itertools import pairwise
from typing import Any, cast
from zoneinfo import ZoneInfo


__all__ = [
    "LEGACY_MARKET_SCHEMA_ID",
    "LegacyMarketDataError",
    "is_legacy_market_loader_manifest",
    "legacy_dataset_hash",
    "legacy_period_matches",
    "legacy_period_overlaps_policy",
    "legacy_period_within_policy",
    "validate_legacy_market_loader_manifest",
]


LEGACY_MARKET_SCHEMA_ID = "market-bars/1"
LEGACY_PROCESSING_VERSION = "market-loader/1.0.0"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_KEY = re.compile(
    r"^historical/provider=alpaca/feed=sip/adjustment=(?P<adjustment>all)/"
    r"session=regular/resolution=(?P<resolution>30m|1h|4h|1d)/"
    r"revision=(?P<revision>[0-9]{8})/year=(?P<year>[0-9]{4})/"
    r"shard=(?P<shard>[0-9]{2})-of-(?P<shard_count>[0-9]{2})/"
    r"manifest_id=(?P<manifest_id>[0-9a-f-]{36})/part-(?P<part>[0-9]{5})\.parquet$"
)


class LegacyMarketDataError(ValueError):
    """The legacy catalog evidence cannot identify one immutable publication."""


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LegacyMarketDataError(f"{label} must be an object")
    return value


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LegacyMarketDataError(f"{label} must be a non-empty string")
    return value


def _uuid(value: object, label: str) -> str:
    text = _text(value, label)
    try:
        parsed = uuid.UUID(text)
    except ValueError as exc:
        raise LegacyMarketDataError(f"{label} must be a UUID") from exc
    if str(parsed) != text:
        raise LegacyMarketDataError(f"{label} must use canonical lowercase UUID text")
    return text


def _period_date(value: object, label: str) -> date:
    text = _text(value, label)
    if not text.endswith("T00:00:00Z"):
        raise LegacyMarketDataError(f"{label} must be a legacy UTC date boundary")
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise LegacyMarketDataError(f"{label} must contain an ISO date") from exc


def is_legacy_market_loader_manifest(manifest: Mapping[str, Any]) -> bool:
    """Return whether the document explicitly selects the isolated legacy path."""
    return manifest.get("schema_id") == LEGACY_MARKET_SCHEMA_ID


def _legacy_objects(manifest: Mapping[str, Any]) -> list[dict[str, object]]:
    manifest_id = _uuid(manifest.get("manifest_id"), "manifest_id")
    composite = manifest.get("composite") is True
    revision = manifest.get("revision")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
        raise LegacyMarketDataError("revision must be a positive integer")
    resolution = _text(manifest.get("resolution"), "resolution")
    period_start = _period_date(manifest.get("period_start"), "period_start")
    period_end = _period_date(manifest.get("period_end"), "period_end")
    if period_start >= period_end:
        raise LegacyMarketDataError("legacy manifest period must be increasing")
    raw_objects = manifest.get("objects")
    if not isinstance(raw_objects, Sequence) or isinstance(raw_objects, (str, bytes)):
        raise LegacyMarketDataError("objects must be a non-empty array")
    if not raw_objects:
        raise LegacyMarketDataError("objects must be a non-empty array")

    hashed: list[dict[str, object]] = []
    seen: set[tuple[int, int, int]] = set()
    shard_counts: set[int] = set()
    covered_partitions: set[tuple[date, date]] = set()
    for index, raw in enumerate(raw_objects):
        item = _mapping(raw, f"objects[{index}]")
        _uuid(item.get("storage_object_id"), f"objects[{index}].storage_object_id")
        key = _text(item.get("object_key"), f"objects[{index}].object_key")
        match = _KEY.fullmatch(key)
        if match is None:
            raise LegacyMarketDataError(f"objects[{index}].object_key is not a legacy loader key")
        source_manifest_id = _uuid(match["manifest_id"], f"objects[{index}].source_manifest_id")
        if not composite and source_manifest_id != manifest_id:
            raise LegacyMarketDataError(f"objects[{index}].object_key binds another manifest")
        if int(match["revision"]) != revision:
            raise LegacyMarketDataError(f"objects[{index}].object_key revision does not match")
        if match["resolution"] != resolution:
            raise LegacyMarketDataError(f"objects[{index}].object_key resolution does not match")
        if composite:
            try:
                object_start = date.fromisoformat(
                    _text(item.get("partition_start"), f"objects[{index}].partition_start")
                )
                object_end = date.fromisoformat(
                    _text(item.get("partition_end"), f"objects[{index}].partition_end")
                )
            except ValueError as exc:
                raise LegacyMarketDataError(
                    f"objects[{index}] has an invalid partition boundary"
                ) from exc
        else:
            object_start, object_end = period_start, period_end
        if int(match["year"]) != object_start.year:
            raise LegacyMarketDataError(f"objects[{index}].object_key year does not match")

        shard = int(match["shard"])
        shard_count = int(match["shard_count"])
        part = int(match["part"])
        if shard_count < 1 or shard >= shard_count or part < 1:
            raise LegacyMarketDataError(f"objects[{index}].object_key shard/part is invalid")
        identity = (object_start.year, shard, part)
        if identity in seen:
            raise LegacyMarketDataError("legacy manifest contains a duplicate shard/part")
        seen.add(identity)
        shard_counts.add(shard_count)
        covered_partitions.add((object_start, object_end))
        if item.get("shard_key") != f"s{shard:02d}-of-{shard_count}":
            raise LegacyMarketDataError(f"objects[{index}].shard_key does not match object_key")
        if item.get("part_number") != part:
            raise LegacyMarketDataError(f"objects[{index}].part_number does not match object_key")

        content_hash = _text(item.get("content_hash"), f"objects[{index}].content_hash")
        if _SHA256.fullmatch(content_hash) is None:
            raise LegacyMarketDataError(f"objects[{index}].content_hash must be lowercase SHA-256")
        row_count = item.get("row_count")
        if not isinstance(row_count, int) or isinstance(row_count, bool) or row_count < 0:
            raise LegacyMarketDataError(f"objects[{index}].row_count must be non-negative")
        if item.get("object_kind") != "MARKET_BARS":
            raise LegacyMarketDataError(f"objects[{index}].object_kind must be MARKET_BARS")
        if item.get("partition_granularity") != "YEAR":
            raise LegacyMarketDataError(f"objects[{index}].partition_granularity must be YEAR")
        if item.get("schema_version") != LEGACY_MARKET_SCHEMA_ID:
            raise LegacyMarketDataError(f"objects[{index}].schema_version does not match")
        if item.get("partition_start") != object_start.isoformat():
            raise LegacyMarketDataError(f"objects[{index}].partition_start does not match")
        if item.get("partition_end") != object_end.isoformat():
            raise LegacyMarketDataError(f"objects[{index}].partition_end does not match")
        if _period_date(item.get("period_start"), f"objects[{index}].period_start") != object_start:
            raise LegacyMarketDataError(f"objects[{index}].period_start does not match")
        if _period_date(item.get("period_end"), f"objects[{index}].period_end") != object_end:
            raise LegacyMarketDataError(f"objects[{index}].period_end does not match")
        if not period_start <= object_start < object_end <= period_end:
            raise LegacyMarketDataError(f"objects[{index}] partition is outside the manifest period")

        storage_fields = (
            item.get("storage_provider"),
            item.get("bucket_name"),
            item.get("provider_version_id"),
        )
        if any(value is not None for value in storage_fields):
            if item.get("storage_provider") != "S3":
                raise LegacyMarketDataError(f"objects[{index}].storage_provider must be S3")
            _text(item.get("bucket_name"), f"objects[{index}].bucket_name")
            _text(item.get("provider_version_id"), f"objects[{index}].provider_version_id")

        hashed_item: dict[str, object] = {
                "content_sha256": content_hash,
                "row_count": row_count,
                "period_start": object_start.isoformat(),
                "period_end": object_end.isoformat(),
                "shard": shard,
                "part": part,
            }
        if composite:
            hashed_item["source_manifest_id"] = source_manifest_id
        hashed.append(hashed_item)

    if len(shard_counts) != 1:
        raise LegacyMarketDataError("legacy manifest objects disagree on shard count")
    shard_count = shard_counts.pop()
    expected = {
        (partition_start.year, shard, 1)
        for partition_start, _partition_end in covered_partitions
        for shard in range(shard_count)
    }
    if seen != expected:
        raise LegacyMarketDataError(
            "legacy manifest must contain exactly one part for every shard and partition"
        )
    if composite:
        ordered_partitions = sorted(covered_partitions)
        if (
            not ordered_partitions
            or ordered_partitions[0][0] != period_start
            or ordered_partitions[-1][1] != period_end
            or any(left[1] != right[0] for left, right in pairwise(ordered_partitions))
        ):
            raise LegacyMarketDataError("composite legacy partitions must cover the manifest contiguously")
    return sorted(
        hashed,
        key=lambda item: (
            cast(str, item["period_start"]),
            cast(int, item["shard"]),
            cast(int, item["part"]),
        ),
    )


def legacy_dataset_hash(manifest: Mapping[str, Any]) -> str:
    """Recompute the exact ``market-loader/1.0.0`` publication digest."""
    payload = {
        "provider": "ALPACA",
        "feed": _text(manifest.get("feed_code"), "feed_code"),
        "adjustment": "all",
        "session": "XNYS_REGULAR",
        "resolution": _text(manifest.get("resolution"), "resolution"),
        "period_start": _period_date(manifest.get("period_start"), "period_start").isoformat(),
        "period_end": _period_date(manifest.get("period_end"), "period_end").isoformat(),
        "revision": manifest.get("revision"),
        "schema_version": manifest.get("schema_id"),
        "processing_version": LEGACY_PROCESSING_VERSION,
        "objects": _legacy_objects(manifest),
    }
    if manifest.get("composite") is True:
        payload["publication_kind"] = "COMPOSITE_FROM_IMMUTABLE_MANIFESTS"
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def validate_legacy_market_loader_manifest(manifest: Mapping[str, Any]) -> None:
    """Validate the isolated legacy shape and its original producer digest."""
    if manifest.get("contract_id") != "com06.dataset-manifest":
        raise LegacyMarketDataError("contract_id must be com06.dataset-manifest")
    if manifest.get("schema_version") != 1:
        raise LegacyMarketDataError("schema_version must be 1")
    manifest_id = _uuid(manifest.get("manifest_id"), "manifest_id")
    if _uuid(manifest.get("dataset_id"), "dataset_id") != manifest_id:
        raise LegacyMarketDataError("legacy dataset_id must equal manifest_id")
    if manifest.get("schema_id") != LEGACY_MARKET_SCHEMA_ID:
        raise LegacyMarketDataError(f"schema_id must be {LEGACY_MARKET_SCHEMA_ID}")
    if manifest.get("provider_code") != "ALPACA":
        raise LegacyMarketDataError("provider_code must be ALPACA")
    resolution = _text(manifest.get("resolution"), "resolution")
    if manifest.get("feed_code") != f"ALPACA_SIP_ALL_{resolution.upper()}":
        raise LegacyMarketDataError("feed_code does not match the adjusted SIP resolution")
    if manifest.get("data_layer") != "ADJUSTED":
        raise LegacyMarketDataError("data_layer must be ADJUSTED")
    if manifest.get("status") != "AVAILABLE":
        raise LegacyMarketDataError("legacy manifest must be AVAILABLE")
    available_at = _text(manifest.get("available_at"), "available_at")
    try:
        datetime.fromisoformat(available_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LegacyMarketDataError("available_at must be ISO-8601") from exc
    declared = _text(manifest.get("dataset_hash"), "dataset_hash")
    if _SHA256.fullmatch(declared) is None:
        raise LegacyMarketDataError("dataset_hash must be lowercase SHA-256")
    computed = legacy_dataset_hash(manifest)
    if declared != computed:
        raise LegacyMarketDataError(
            "legacy dataset_hash does not match market-loader/1.0.0 publication material: "
            f"declared {declared}, computed {computed}"
        )


def legacy_period_matches(
    manifest: Mapping[str, Any],
    period_start: datetime,
    period_end: datetime,
    timezone_name: str,
) -> bool:
    """Compare loader date labels with policy-local midnight boundaries."""
    zone = ZoneInfo(timezone_name)
    local_start = period_start.astimezone(zone)
    local_end = period_end.astimezone(zone)
    midnight = (0, 0, 0, 0)
    return (
        (local_start.hour, local_start.minute, local_start.second, local_start.microsecond) == midnight
        and (local_end.hour, local_end.minute, local_end.second, local_end.microsecond) == midnight
        and _period_date(manifest.get("period_start"), "period_start") == local_start.date()
        and _period_date(manifest.get("period_end"), "period_end") == local_end.date()
    )


def legacy_period_within_policy(
    manifest: Mapping[str, Any],
    period_start: datetime,
    period_end: datetime,
    timezone_name: str,
) -> bool:
    """Accept one loader date-labelled segment contained by policy-local dates."""
    zone = ZoneInfo(timezone_name)
    local_start = period_start.astimezone(zone)
    local_end = period_end.astimezone(zone)
    midnight = (0, 0, 0, 0)
    manifest_start = _period_date(manifest.get("period_start"), "period_start")
    manifest_end = _period_date(manifest.get("period_end"), "period_end")
    return (
        (local_start.hour, local_start.minute, local_start.second, local_start.microsecond) == midnight
        and (local_end.hour, local_end.minute, local_end.second, local_end.microsecond) == midnight
        and local_start.date() <= manifest_start < manifest_end <= local_end.date()
    )


def legacy_period_overlaps_policy(
    manifest: Mapping[str, Any],
    period_start: datetime,
    period_end: datetime,
    timezone_name: str,
) -> bool:
    """Accept a loader partition that contributes rows to a policy-local window.

    Immutable Parquet partitions are commonly calendar-year sized while a locked
    evaluation policy can end mid-year. The replay layer applies the explicit
    evaluation interval after binding, so requiring every partition boundary to
    equal or sit inside the policy would reject correct, immutable source data.
    """
    zone = ZoneInfo(timezone_name)
    local_start = period_start.astimezone(zone)
    local_end = period_end.astimezone(zone)
    midnight = (0, 0, 0, 0)
    manifest_start = _period_date(manifest.get("period_start"), "period_start")
    manifest_end = _period_date(manifest.get("period_end"), "period_end")
    return (
        (local_start.hour, local_start.minute, local_start.second, local_start.microsecond)
        == midnight
        and (local_end.hour, local_end.minute, local_end.second, local_end.microsecond)
        == midnight
        and manifest_start < local_end.date()
        and manifest_end > local_start.date()
    )
