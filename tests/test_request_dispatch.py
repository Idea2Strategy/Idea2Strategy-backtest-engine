from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date
from typing import Any

import pytest

from backtest_engine.backtest_request_intake import RequestLane, RequestProcessingError
from backtest_engine.request_dispatch import (
    BacktestRequestJobPublisher,
    PinnedDataset,
    PinnedFeatureMaterialization,
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
    custom_request_with_two_market_datasets,
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
        (request["periodStart"], request["periodEnd"])
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
        input_bundle_id=uuid.UUID("96000000-0000-4000-8000-000000000001"),
        input_bundle_fingerprint="7" * 64,
        input_contract_version=str(request["metadata"]["contractVersion"]),
        compiled_plan_checksum=request["compiledPlanChecksum"],
        strategy_snapshot_hash=request["expectedSnapshotHash"],
        datasets=tuple(
            PinnedDataset(
                dataset_manifest_id=uuid.UUID(str(item["datasetManifestId"])),
                purpose_code=str(item["purposeCode"]),
                locked_dataset_hash=str(item["expectedDatasetHash"]),
            )
            for item in (
                request["periods"][0]["datasets"]
                if lane is RequestLane.COMPETITION
                else request.get("datasets", (
                    {
                        "datasetManifestId": request["datasetManifestId"],
                        "purposeCode": "MARKET_BARS",
                        "expectedDatasetHash": request["expectedDatasetHash"],
                    },
                ))
            )
        ),
        feature_materializations=tuple(
            PinnedFeatureMaterialization(
                feature_materialization_id=uuid.UUID(str(item["featureMaterializationId"])),
                locked_result_hash=str(item["lockedResultHash"]),
            )
            for item in (
                request["periods"][0]["featureMaterializations"]
                if lane is RequestLane.COMPETITION
                else request["featureMaterializations"]
                if lane in {RequestLane.BASIC, RequestLane.CUSTOM}
                else ()
            )
        ),
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
                    "inputBundleId": "96000000-0000-4000-8000-000000000001",
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
    assert sorted(job["datasets"], key=lambda item: item["datasetManifestId"]) == sorted(
        request["periods"][0]["datasets"], key=lambda item: item["datasetManifestId"]
    )
    assert sorted(
        job["featureMaterializations"], key=lambda item: item["featureMaterializationId"]
    ) == sorted(
        request["periods"][0]["featureMaterializations"],
        key=lambda item: item["featureMaterializationId"],
    )


def test_competition_refuses_feature_pins_that_differ_from_the_provider_bundle() -> None:
    request = competition_request()
    run = projection(request, RequestLane.COMPETITION)
    request["periods"][0]["featureMaterializations"].append(
        {
            "featureMaterializationId": str(uuid.uuid4()),
            "lockedResultHash": "sha256:" + "9" * 64,
        }
    )
    publisher = BacktestRequestJobPublisher(Source(run), Queue())

    with pytest.raises(RequestProcessingError, match="RUN_IDENTITY_MISMATCH"):
        publisher(request, RequestLane.COMPETITION)


def test_basic_request_refuses_feature_pins_that_differ_from_the_provider_bundle() -> None:
    request = basic_request()
    feature = PinnedFeatureMaterialization(uuid.uuid4(), "sha256:" + "9" * 64)
    run = replace(
        projection(request, RequestLane.BASIC),
        feature_materializations=(feature,),
    )
    publisher = BacktestRequestJobPublisher(Source(run), Queue())

    with pytest.raises(RequestProcessingError, match="RUN_IDENTITY_MISMATCH"):
        publisher(request, RequestLane.BASIC)


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
                    "inputBundleId": "96000000-0000-4000-8000-000000000001",
                    "botId": request["botId"],
                "ownerAccountId": str(ACCOUNT_ID),
                "idempotencyKey": request["metadata"]["idempotencyKey"],
                "inputBundleFingerprint": "sha256:" + "7" * 64,
                "executionPolicyVersion": request["executionPolicyVersion"],
                "compiledPlanChecksum": request["compiledPlanChecksum"],
                "datasetManifestId": request["datasetManifestId"],
                "expectedDatasetHash": request["expectedDatasetHash"],
                "expectedSnapshotHash": request["expectedSnapshotHash"],
                "datasets": [
                    {
                        "datasetManifestId": request["datasetManifestId"],
                        "purposeCode": "MARKET_BARS",
                        "expectedDatasetHash": request["expectedDatasetHash"],
                    }
                ],
                "featureMaterializations": request["featureMaterializations"],
            },
        )
    ]


def test_basic_request_preserves_every_server_selected_market_dataset() -> None:
    request = basic_request()
    second_id = uuid.UUID("95000000-0000-4000-8000-000000000002")
    request["datasets"] = [
        {
            "datasetManifestId": request["datasetManifestId"],
            "purposeCode": "MARKET_BARS",
            "expectedDatasetHash": request["expectedDatasetHash"],
        },
        {
            "datasetManifestId": str(second_id),
            "purposeCode": "MARKET_BARS",
            "expectedDatasetHash": "sha256:" + "8" * 64,
        },
    ]
    run = replace(
        projection(request, RequestLane.BASIC),
        datasets=(
            PinnedDataset(
                uuid.UUID(request["datasetManifestId"]),
                "MARKET_BARS",
                request["expectedDatasetHash"],
            ),
            PinnedDataset(second_id, "MARKET_BARS", "sha256:" + "8" * 64),
        ),
    )
    queue = Queue()

    BacktestRequestJobPublisher(Source(run), queue)(request, RequestLane.BASIC)

    assert queue.jobs[0][1]["datasetManifestId"] == request["datasetManifestId"]
    assert queue.jobs[0][1]["datasets"] == request["datasets"]


def test_custom_request_preserves_every_server_selected_market_dataset() -> None:
    request = custom_request_with_two_market_datasets()
    queue = Queue()

    BacktestRequestJobPublisher(Source(projection(request, RequestLane.CUSTOM)), queue)(
        request, RequestLane.CUSTOM
    )

    assert queue.jobs[0][1]["datasetManifestId"] == request["datasetManifestId"]
    assert queue.jobs[0][1]["datasets"] == request["datasets"]


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
