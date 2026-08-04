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
from test_backtest_request_intake import (
    ACCOUNT_ID,
    BOT_ID,
    DATASET_ID,
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
        (request["periodStart"], request["periodEnd"])
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
        bot_id=BOT_ID,
        owner_account_id=ACCOUNT_ID,
        configuration_hash="7" * 64,
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
