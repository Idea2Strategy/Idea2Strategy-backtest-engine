"""Convert validated backend request envelopes into internal execution jobs."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from .backtest_request_intake import RequestLane, RequestProcessingError


@dataclass(frozen=True, slots=True, order=True)
class PinnedDataset:
    dataset_manifest_id: uuid.UUID
    purpose_code: str
    locked_dataset_hash: str


@dataclass(frozen=True, slots=True, order=True)
class PinnedFeatureMaterialization:
    feature_materialization_id: uuid.UUID
    locked_result_hash: str


@dataclass(frozen=True, slots=True)
class QueuedRunProjection:
    """Provider-created run facts required to authorize one queue conversion."""

    run_id: uuid.UUID
    lane: RequestLane
    message_id: uuid.UUID
    bot_id: uuid.UUID
    owner_account_id: uuid.UUID
    input_bundle_id: uuid.UUID
    input_bundle_fingerprint: str
    input_contract_version: str
    compiled_plan_checksum: str
    strategy_snapshot_hash: str
    datasets: tuple[PinnedDataset, ...]
    feature_materializations: tuple[PinnedFeatureMaterialization, ...]
    aggregate_sequence: int
    evaluation_start: date
    evaluation_end: date
    execution_policy_version: str


class QueuedRunSource(Protocol):
    def by_id(self, run_id: uuid.UUID) -> QueuedRunProjection | None: ...


class ExecutionJobQueue(Protocol):
    def publish(self, lane: RequestLane, job: dict[str, Any]) -> None: ...


def _prefixed(value: str) -> str:
    prefixed = value if value.startswith("sha256:") else f"sha256:{value}"
    digest = prefixed.removeprefix("sha256:")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise RequestProcessingError("RUN_CONFIGURATION_HASH_INVALID", retryable=False)
    return prefixed


def _dataset_payload(pin: PinnedDataset) -> dict[str, str]:
    return {
        "datasetManifestId": str(pin.dataset_manifest_id),
        "purposeCode": pin.purpose_code,
        "expectedDatasetHash": pin.locked_dataset_hash,
    }


def _feature_payload(pin: PinnedFeatureMaterialization) -> dict[str, str]:
    return {
        "featureMaterializationId": str(pin.feature_materialization_id),
        "lockedResultHash": pin.locked_result_hash,
    }


def _request_period(
    request: Mapping[str, Any], lane: RequestLane
) -> tuple[date, date, tuple[PinnedDataset, ...], tuple[PinnedFeatureMaterialization, ...]]:
    try:
        if lane is RequestLane.BASIC:
            raw_datasets = request.get("datasets") or (
                {
                    "datasetManifestId": request["datasetManifestId"],
                    "purposeCode": "MARKET_BARS",
                    "expectedDatasetHash": request["expectedDatasetHash"],
                },
            )
            return (
                date.fromisoformat(str(request["periodStart"])),
                date.fromisoformat(str(request["periodEnd"])),
                tuple(
                    sorted(
                    PinnedDataset(
                            uuid.UUID(str(item["datasetManifestId"])),
                            str(item["purposeCode"]),
                            str(item["expectedDatasetHash"]),
                        )
                        for item in raw_datasets
                    ),
                ),
                tuple(
                    sorted(
                        PinnedFeatureMaterialization(
                            uuid.UUID(str(item["featureMaterializationId"])),
                            str(item["lockedResultHash"]),
                        )
                        for item in request["featureMaterializations"]
                    )
                ),
            )
        if lane is RequestLane.CUSTOM:
            raw_datasets = request.get("datasets") or (
                {
                    "datasetManifestId": request["datasetManifestId"],
                    "purposeCode": "MARKET_BARS",
                    "expectedDatasetHash": request["expectedDatasetHash"],
                },
            )
            return (
                date.fromisoformat(str(request["periodStart"])),
                date.fromisoformat(str(request["periodEnd"])),
                tuple(
                    sorted(
                        PinnedDataset(
                            uuid.UUID(str(item["datasetManifestId"])),
                            str(item["purposeCode"]),
                            str(item["expectedDatasetHash"]),
                        )
                        for item in raw_datasets
                    )
                ),
                tuple(
                    sorted(
                        PinnedFeatureMaterialization(
                            uuid.UUID(str(item["featureMaterializationId"])),
                            str(item["lockedResultHash"]),
                        )
                        for item in request["featureMaterializations"]
                    )
                ),
            )
        periods = request["periods"]
        if not isinstance(periods, list) or len(periods) != 1:
            raise RequestProcessingError("COMPETITION_PERIOD_COUNT_INVALID", retryable=False)
        period = periods[0]
        datasets = period["datasets"]
        dataset_pins = tuple(
            sorted(
                PinnedDataset(
                    uuid.UUID(str(item["datasetManifestId"])),
                    str(item["purposeCode"]),
                    str(item["expectedDatasetHash"]),
                )
                for item in datasets
            )
        )
        feature_pins = tuple(
            sorted(
                PinnedFeatureMaterialization(
                    uuid.UUID(str(item["featureMaterializationId"])),
                    str(item["lockedResultHash"]),
                )
                for item in period["featureMaterializations"]
            )
        )
        market_bars = [item for item in dataset_pins if item.purpose_code == "MARKET_BARS"]
        if len(market_bars) != 1:
            raise RequestProcessingError("MARKET_BARS_DATASET_INVALID", retryable=False)
        return (
            date.fromisoformat(str(period["evaluationStart"])),
            date.fromisoformat(str(period["evaluationEnd"])),
            dataset_pins,
            feature_pins,
        )
    except RequestProcessingError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise RequestProcessingError("REQUEST_JOB_INPUT_INVALID", retryable=False) from exc


class BacktestRequestJobPublisher:
    """Authorize conversion against the durable run, then publish one small job."""

    def __init__(self, source: QueuedRunSource, queue: ExecutionJobQueue) -> None:
        self._source = source
        self._queue = queue

    def __call__(self, request: Mapping[str, Any], lane: RequestLane) -> None:
        try:
            run_id = uuid.UUID(str(request["runId"]))
            message_id = uuid.UUID(str(request["metadata"]["messageId"]))
            bot_id = uuid.UUID(str(request["botId"]))
            aggregate_sequence = int(request["aggregateSequence"])
            policy_version = str(request["executionPolicyVersion"])
            owner_id = (
                uuid.UUID(str(request["requestingAccountId"]))
                if lane is RequestLane.CUSTOM
                else None
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RequestProcessingError("REQUEST_JOB_INPUT_INVALID", retryable=False) from exc

        run = self._source.by_id(run_id)
        if run is None:
            raise RequestProcessingError("RUN_NOT_FOUND", retryable=False)
        stored_datasets = tuple(sorted(run.datasets))
        stored_features = tuple(sorted(run.feature_materializations))
        market_bars = tuple(item for item in stored_datasets if item.purpose_code == "MARKET_BARS")
        if not market_bars or (lane is RequestLane.COMPETITION and len(market_bars) != 1):
            raise RequestProcessingError("MARKET_BARS_DATASET_INVALID", retryable=False)
        representative_id = (
            market_bars[0].dataset_manifest_id
            if lane is RequestLane.COMPETITION
            else uuid.UUID(str(request["datasetManifestId"]))
        )
        representatives = tuple(
            item for item in market_bars if item.dataset_manifest_id == representative_id
        )
        if len(representatives) != 1:
            raise RequestProcessingError("MARKET_BARS_DATASET_INVALID", retryable=False)
        primary = representatives[0]
        if lane is RequestLane.BASIC:
            start, end, requested_datasets, requested_features = _request_period(
                request, lane
            )
            period_identity: dict[str, str] = {}
        else:
            start, end, requested_datasets, requested_features = _request_period(request, lane)
            if lane is RequestLane.COMPETITION:
                period = request["periods"][0]
                period_identity = {
                    "evaluationPeriodId": str(period["evaluationPeriodId"]),
                    "inputSetHash": str(period["inputSetHash"]),
                }
            else:
                # CUSTOM messages name the selected market dataset. Feature pins are
                # resolved and committed by the Backend provider, not copied into the
                # request envelope, so the stored bundle is authoritative for them.
                requested_features = stored_features
                period_identity = {}
        identity_matches = all(
            (
                run.lane is lane,
                run.message_id == message_id,
                run.bot_id == bot_id,
                owner_id is None or run.owner_account_id == owner_id,
                run.aggregate_sequence == aggregate_sequence,
                run.evaluation_start == start,
                run.evaluation_end == end,
                run.execution_policy_version == policy_version,
                run.input_contract_version == str(request["metadata"]["contractVersion"]),
                run.compiled_plan_checksum == str(request["compiledPlanChecksum"]),
                run.strategy_snapshot_hash == str(request["expectedSnapshotHash"]),
                stored_datasets == tuple(sorted(requested_datasets)),
                stored_features == tuple(sorted(requested_features)),
            )
        )
        if not identity_matches:
            raise RequestProcessingError("RUN_IDENTITY_MISMATCH", retryable=False)

        job = {
            "backtestRunId": str(run.run_id),
            "inputBundleId": str(run.input_bundle_id),
            "botId": str(run.bot_id),
            "ownerAccountId": str(run.owner_account_id),
            "idempotencyKey": str(request["metadata"]["idempotencyKey"]),
            "inputBundleFingerprint": _prefixed(run.input_bundle_fingerprint),
            "executionPolicyVersion": run.execution_policy_version,
            "compiledPlanChecksum": str(request["compiledPlanChecksum"]),
            "datasetManifestId": str(primary.dataset_manifest_id),
            "expectedDatasetHash": primary.locked_dataset_hash,
            "expectedSnapshotHash": str(request["expectedSnapshotHash"]),
            "evaluationStart": run.evaluation_start.isoformat(),
            "evaluationEnd": run.evaluation_end.isoformat(),
            "datasets": [_dataset_payload(item) for item in stored_datasets],
            "featureMaterializations": [_feature_payload(item) for item in stored_features],
            **period_identity,
        }
        self._queue.publish(lane, job)
