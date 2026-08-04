from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date
from typing import Any

import pytest

from backtest_engine.backtest_request_intake import RequestLane, RequestProcessingError
from backtest_engine.request_dispatch import (
    BacktestRequestJobPublisher,
    QueuedRunProjection,
)
from backtest_engine.wiring import (
    FeatureMaterializationPin,
    JobNotSatisfiable,
    verify_feature_materialization_pins,
)
from test_backtest_request_intake import (
    ACCOUNT_ID,
    BOT_ID,
    DATASET_ID,
    basic_request,
    competition_request,
    custom_request,
)


class Source:
    def __init__(self, run: QueuedRunProjection | None) -> None:
        self.run = run

    def by_id(self, run_id: uuid.UUID) -> QueuedRunProjection | None:
        assert self.run is None or self.run.run_id == run_id
        return self.run


class Queue:
    def __init__(self) -> None:
        self.jobs: list[tuple[RequestLane, dict[str, Any]]] = []

    def publish(self, lane: RequestLane, job: dict[str, Any]) -> None:
        self.jobs.append((lane, job))


def projection(request: dict[str, Any], lane: RequestLane) -> QueuedRunProjection:
    period = (
        ("2024-01-01", "2024-12-31")
        if lane is RequestLane.BASIC
        else (request["periodStart"], request["periodEnd"])
        if lane is RequestLane.CUSTOM
        else (
            request["periods"][0]["evaluationStart"],
            request["periods"][0]["evaluationEnd"],
        )
    )
    return QueuedRunProjection(
        run_id=uuid.UUID(request["runId"]),
        lane=lane,
        message_id=uuid.UUID(request["metadata"]["messageId"]),
        bot_id=uuid.UUID(request["botId"]),
        owner_account_id=ACCOUNT_ID,
        input_bundle_hash="7" * 64,
        compiled_plan_checksum=request["compiledPlanChecksum"],
        strategy_snapshot_hash=request["expectedSnapshotHash"],
        dataset_manifest_id=uuid.UUID(
            request["datasetManifestId"]
            if lane is not RequestLane.COMPETITION
            else request["periods"][0]["datasets"][0]["datasetManifestId"]
        ),
        dataset_hash=(
            "sha256:" + "4" * 64
            if lane is RequestLane.BASIC
            else request["expectedDatasetHash"]
            if lane is RequestLane.CUSTOM
            else request["periods"][0]["datasets"][0]["expectedDatasetHash"]
        ),
        feature_materialization_version="features-v1",
        aggregate_sequence=1,
        evaluation_start=date.fromisoformat(period[0]),
        evaluation_end=date.fromisoformat(period[1]),
        execution_policy_version=request["executionPolicyVersion"],
    )


@pytest.mark.parametrize(
    ("lane", "factory"),
    ((RequestLane.CUSTOM, custom_request), (RequestLane.COMPETITION, competition_request)),
)
def test_publishes_existing_provider_created_run_as_an_execution_job(
    lane: RequestLane, factory: Any
) -> None:
    request = factory()
    queue = Queue()
    publisher = BacktestRequestJobPublisher(Source(projection(request, lane)), queue)

    publisher(request, lane)

    assert queue.jobs == [
        (
            lane,
            {
                "backtestRunId": request["runId"],
                "botId": str(BOT_ID),
                "ownerAccountId": str(ACCOUNT_ID),
                "idempotencyKey": request["metadata"]["idempotencyKey"],
                "inputBundleFingerprint": "sha256:" + "7" * 64,
                "executionPolicyVersion": request["executionPolicyVersion"],
                "compiledPlanChecksum": request["compiledPlanChecksum"],
                "datasetManifestId": str(DATASET_ID),
                "expectedDatasetHash": (
                    request["expectedDatasetHash"]
                    if lane is RequestLane.CUSTOM
                    else request["periods"][0]["datasets"][0]["expectedDatasetHash"]
                ),
                "expectedSnapshotHash": request["expectedSnapshotHash"],
                "datasets": [
                    {
                        "datasetManifestId": str(DATASET_ID),
                        "purposeCode": "MARKET_BARS",
                        "expectedDatasetHash": (
                            request["expectedDatasetHash"]
                            if lane is RequestLane.CUSTOM
                            else request["periods"][0]["datasets"][0]["expectedDatasetHash"]
                        ),
                    }
                ],
                "featureMaterializations": [],
                "featureMaterializationVersion": "features-v1",
                **(
                    {
                        "evaluationPeriodId": request["periods"][0]["evaluationPeriodId"],
                        "inputSetHash": request["periods"][0]["inputSetHash"],
                    }
                    if lane is RequestLane.COMPETITION
                    else {}
                ),
            },
        )
    ]


def test_refuses_a_payload_that_does_not_match_the_precreated_run() -> None:
    request = custom_request()
    run = replace(projection(request, RequestLane.CUSTOM), owner_account_id=uuid.uuid4())
    publisher = BacktestRequestJobPublisher(Source(run), Queue())

    with pytest.raises(RequestProcessingError, match="RUN_IDENTITY_MISMATCH") as error:
        publisher(request, RequestLane.CUSTOM)

    assert error.value.retryable is False


def test_competition_requires_exactly_one_market_bars_dataset_for_the_period() -> None:
    request = competition_request()
    request["periods"][0]["datasets"][0]["purposeCode"] = "FUNDAMENTALS"
    publisher = BacktestRequestJobPublisher(
        Source(projection(request, RequestLane.COMPETITION)), Queue()
    )

    with pytest.raises(RequestProcessingError, match="MARKET_BARS_DATASET_INVALID"):
        publisher(request, RequestLane.COMPETITION)


def test_competition_preserves_every_dataset_and_feature_pin() -> None:
    request = competition_request()
    request["periods"][0]["datasets"].append(
        {
            "datasetManifestId": str(uuid.uuid4()),
            "purposeCode": "CORPORATE_ACTIONS",
            "expectedDatasetHash": "sha256:" + "8" * 64,
        }
    )
    request["periods"][0]["featureMaterializations"].append(
        {
            "featureMaterializationId": str(uuid.uuid4()),
            "lockedResultHash": "sha256:" + "9" * 64,
        }
    )
    queue = Queue()
    publisher = BacktestRequestJobPublisher(
        Source(projection(request, RequestLane.COMPETITION)), queue
    )

    publisher(request, RequestLane.COMPETITION)

    job = queue.jobs[0][1]
    assert job["evaluationPeriodId"] == request["periods"][0]["evaluationPeriodId"]
    assert job["inputSetHash"] == request["periods"][0]["inputSetHash"]
    assert job["datasets"] == request["periods"][0]["datasets"]
    assert job["featureMaterializations"] == request["periods"][0]["featureMaterializations"]


def test_basic_request_is_dispatched_through_the_same_pinned_two_stage_boundary() -> None:
    request = basic_request()
    queue = Queue()
    run = projection(request, RequestLane.BASIC)
    publisher = BacktestRequestJobPublisher(Source(run), queue)

    publisher(request, RequestLane.BASIC)

    assert queue.jobs == [
        (
            RequestLane.BASIC,
            {
                "backtestRunId": request["runId"],
                "botId": request["botId"],
                "ownerAccountId": str(ACCOUNT_ID),
                "idempotencyKey": request["metadata"]["idempotencyKey"],
                "inputBundleFingerprint": "sha256:" + "7" * 64,
                "executionPolicyVersion": request["executionPolicyVersion"],
                "compiledPlanChecksum": request["compiledPlanChecksum"],
                "datasetManifestId": request["datasetManifestId"],
                "expectedDatasetHash": "sha256:" + "4" * 64,
                "expectedSnapshotHash": request["expectedSnapshotHash"],
                "datasets": [
                    {
                        "datasetManifestId": request["datasetManifestId"],
                        "purposeCode": "MARKET_BARS",
                        "expectedDatasetHash": "sha256:" + "4" * 64,
                    }
                ],
                "featureMaterializations": [],
                "featureMaterializationVersion": "features-v1",
            },
        )
    ]


def test_changed_feature_output_is_rejected_before_execution() -> None:
    feature_id = uuid.uuid4()

    class Features:
        def by_id(self, materialization_id: uuid.UUID) -> dict[str, Any]:
            assert materialization_id == feature_id
            return {
                "status": "SUCCEEDED",
                "result_hash": "sha256:" + "8" * 64,
                "output_dataset_manifest_id": uuid.uuid4(),
                "output_dataset_status": "AVAILABLE",
                "output_dataset_hash": "sha256:" + "7" * 64,
            }

    with pytest.raises(JobNotSatisfiable) as error:
        verify_feature_materialization_pins(
            (
                FeatureMaterializationPin(
                    materialization_id=feature_id,
                    locked_result_hash="sha256:" + "9" * 64,
                ),
            ),
            Features(),
        )

    assert error.value.reason_code == "REQUIRED_INPUT_UNAVAILABLE"


def test_verified_feature_output_still_fails_closed_without_consumption_semantics() -> None:
    feature_id = uuid.uuid4()
    locked_hash = "sha256:" + "9" * 64

    class Features:
        def by_id(self, materialization_id: uuid.UUID) -> dict[str, Any]:
            assert materialization_id == feature_id
            return {
                "status": "SUCCEEDED",
                "result_hash": locked_hash,
                "output_dataset_manifest_id": uuid.uuid4(),
                "output_dataset_status": "AVAILABLE",
                "output_dataset_hash": "sha256:" + "7" * 64,
            }

    with pytest.raises(JobNotSatisfiable) as error:
        verify_feature_materialization_pins(
            (
                FeatureMaterializationPin(
                    materialization_id=feature_id,
                    locked_result_hash=locked_hash,
                ),
            ),
            Features(),
        )

    assert error.value.reason_code == "FEATURE_OUTPUT_CONSUMPTION_UNSUPPORTED"
