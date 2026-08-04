"""Convert validated backend request envelopes into internal execution jobs."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from .backtest_request_intake import RequestLane, RequestProcessingError


@dataclass(frozen=True, slots=True)
class QueuedRunProjection:
    """Provider-created run facts required to authorize one queue conversion."""

    run_id: uuid.UUID
    lane: RequestLane
    message_id: uuid.UUID
    bot_id: uuid.UUID
    owner_account_id: uuid.UUID
    input_bundle_fingerprint: str
    compiled_plan_checksum: str
    strategy_snapshot_hash: str
    dataset_manifest_id: uuid.UUID
    dataset_hash: str
    feature_materialization_version: str
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


def _period(
    request: Mapping[str, Any], lane: RequestLane
) -> tuple[date, date, uuid.UUID, str]:
    try:
        if lane is RequestLane.BASIC:
            raise RequestProcessingError("BASIC_PERIOD_COMES_FROM_RUN", retryable=False)
        if lane is RequestLane.CUSTOM:
            return (
                date.fromisoformat(str(request["periodStart"])),
                date.fromisoformat(str(request["periodEnd"])),
                uuid.UUID(str(request["datasetManifestId"])),
                str(request["expectedDatasetHash"]),
            )
        periods = request["periods"]
        if not isinstance(periods, list) or len(periods) != 1:
            raise RequestProcessingError("COMPETITION_PERIOD_COUNT_INVALID", retryable=False)
        period = periods[0]
        datasets = period["datasets"]
        market_bars = [item for item in datasets if item.get("purposeCode") == "MARKET_BARS"]
        if len(market_bars) != 1:
            raise RequestProcessingError("MARKET_BARS_DATASET_INVALID", retryable=False)
        return (
            date.fromisoformat(str(period["evaluationStart"])),
            date.fromisoformat(str(period["evaluationEnd"])),
            uuid.UUID(str(market_bars[0]["datasetManifestId"])),
            str(market_bars[0]["expectedDatasetHash"]),
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
        if lane is RequestLane.BASIC:
            start, end = run.evaluation_start, run.evaluation_end
            dataset_id = uuid.UUID(str(request["datasetManifestId"]))
            expected_dataset_hash = run.dataset_hash
            datasets = ({
                "datasetManifestId": str(dataset_id),
                "purposeCode": "MARKET_BARS",
                "expectedDatasetHash": expected_dataset_hash,
            },)
            features: tuple[Mapping[str, Any], ...] = ()
            period_identity: dict[str, str] = {}
        else:
            start, end, dataset_id, expected_dataset_hash = _period(request, lane)
            if lane is RequestLane.CUSTOM:
                datasets = ({
                    "datasetManifestId": str(dataset_id),
                    "purposeCode": "MARKET_BARS",
                    "expectedDatasetHash": expected_dataset_hash,
                },)
                features = ()
                period_identity = {}
            else:
                period = request["periods"][0]
                datasets = tuple(period["datasets"])
                features = tuple(period["featureMaterializations"])
                period_identity = {
                    "evaluationPeriodId": str(period["evaluationPeriodId"]),
                    "inputSetHash": str(period["inputSetHash"]),
                }
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
                run.compiled_plan_checksum == str(request["compiledPlanChecksum"]),
                run.strategy_snapshot_hash == str(request["expectedSnapshotHash"]),
                run.dataset_manifest_id == dataset_id,
                _prefixed(run.dataset_hash) == _prefixed(expected_dataset_hash),
            )
        )
        if not identity_matches:
            raise RequestProcessingError("RUN_IDENTITY_MISMATCH", retryable=False)

        job = {
            "backtestRunId": str(run.run_id),
            "botId": str(run.bot_id),
            "ownerAccountId": str(run.owner_account_id),
            "idempotencyKey": str(request["metadata"]["idempotencyKey"]),
            "inputBundleFingerprint": _prefixed(run.input_bundle_fingerprint),
            "executionPolicyVersion": run.execution_policy_version,
            "compiledPlanChecksum": str(request["compiledPlanChecksum"]),
            "datasetManifestId": str(dataset_id),
            "expectedDatasetHash": expected_dataset_hash,
            "expectedSnapshotHash": str(request["expectedSnapshotHash"]),
            "datasets": [dict(item) for item in datasets],
            "featureMaterializations": [dict(item) for item in features],
            "featureMaterializationVersion": run.feature_materialization_version,
            **period_identity,
        }
        self._queue.publish(lane, job)
