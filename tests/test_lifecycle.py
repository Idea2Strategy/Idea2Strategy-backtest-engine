from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    IdempotencyConflict,
    InMemoryBacktestJobQueue,
    InMemoryBacktestRunStore,
    InvalidStatusTransition,
    SqsBacktestJobQueue,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json"


@pytest.fixture
def fixtures() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


@pytest.fixture
def lifecycle() -> tuple[BacktestLifecycleService, InMemoryBacktestJobQueue]:
    queue = InMemoryBacktestJobQueue()
    return BacktestLifecycleService(InMemoryBacktestRunStore(), queue), queue


def test_duplicate_request_is_stored_and_enqueued_once(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, queue = lifecycle
    request = fixtures["backtest_request"]

    first = service.accept(request)
    second = service.accept(copy.deepcopy(request))

    assert first == second
    assert first.status == "QUEUED"
    assert queue.messages == [request]


def test_reused_request_idempotency_key_must_have_identical_payload(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, _ = lifecycle
    request = fixtures["backtest_request"]
    service.accept(request)
    changed = copy.deepcopy(request)
    changed["compiled_plan_hash"] = "d" * 64

    with pytest.raises(IdempotencyConflict, match="idempotency_key"):
        service.accept(changed)


def test_concurrent_duplicate_requests_enqueue_once(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, queue = lifecycle
    request = fixtures["backtest_request"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        runs = list(executor.map(lambda _: service.accept(request), range(16)))

    assert len({run.backtest_run_id for run in runs}) == 1
    assert queue.messages == [request]


def test_queue_failure_can_be_retried_with_the_same_request(
    fixtures: dict[str, object],
) -> None:
    class FailOnceQueue:
        def __init__(self) -> None:
            self.attempts = 0

        def publish(self, request: object) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("queue unavailable")

    queue = FailOnceQueue()
    service = BacktestLifecycleService(InMemoryBacktestRunStore(), queue)
    request = fixtures["backtest_request"]

    with pytest.raises(RuntimeError, match="queue unavailable"):
        service.accept(request)

    assert service.accept(request).status == "QUEUED"
    assert queue.attempts == 2


def test_sqs_queue_preserves_contract_payload_and_identifiers(
    fixtures: dict[str, object],
) -> None:
    class RecordingSqsClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def send_message(self, **kwargs: object) -> dict[str, str]:
            self.calls.append(kwargs)
            return {"MessageId": "message-1"}

    request = fixtures["backtest_request"]
    client = RecordingSqsClient()

    SqsBacktestJobQueue(client, "https://sqs.example/backtests").publish(request)

    assert len(client.calls) == 1
    call = client.calls[0]
    assert json.loads(call["MessageBody"]) == request
    assert call["MessageAttributes"]["BacktestRunId"]["StringValue"] == request[
        "backtest_run_id"
    ]
    assert call["MessageAttributes"]["IdempotencyKey"]["StringValue"] == request[
        "idempotency_key"
    ]


def test_fixture_lifecycle_reaches_complete_in_order(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, _ = lifecycle
    request = fixtures["backtest_request"]
    queued, running, complete = fixtures["backtest_results"][:3]
    service.accept(request)

    assert service.apply_result(queued).status == "QUEUED"
    assert service.apply_result(running).status == "RUNNING"
    finished = service.apply_result(complete)

    assert finished.status == "COMPLETE"
    assert finished.status_result == complete


def test_same_result_event_is_idempotent(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, _ = lifecycle
    request = fixtures["backtest_request"]
    running = fixtures["backtest_results"][1]
    service.accept(request)

    first = service.apply_result(running)
    second = service.apply_result(copy.deepcopy(running))

    assert first == second


def test_complete_cannot_skip_running(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
) -> None:
    service, _ = lifecycle
    request = fixtures["backtest_request"]
    complete = fixtures["backtest_results"][2]
    service.accept(request)

    with pytest.raises(InvalidStatusTransition, match="QUEUED.*COMPLETE"):
        service.apply_result(complete)


@pytest.mark.parametrize("result_index", [3, 4])
def test_failed_and_unavailable_are_terminal_from_queued(
    fixtures: dict[str, object],
    lifecycle: tuple[BacktestLifecycleService, InMemoryBacktestJobQueue],
    result_index: int,
) -> None:
    service, _ = lifecycle
    request = fixtures["backtest_request"]
    terminal = fixtures["backtest_results"][result_index]
    running = fixtures["backtest_results"][1]
    service.accept(request)

    assert service.apply_result(terminal).status == terminal["status"]
    with pytest.raises(InvalidStatusTransition, match="cannot transition"):
        service.apply_result(running)
