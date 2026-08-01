from __future__ import annotations

import copy
import json
from pathlib import Path

from fastapi.testclient import TestClient

from backtest_engine.api import create_app
from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    InMemoryBacktestJobQueue,
    InMemoryBacktestRunStore,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures/contracts/com06-d-fixtures.v1.json"


def _client() -> tuple[
    TestClient,
    InMemoryBacktestJobQueue,
    BacktestLifecycleService,
]:
    queue = InMemoryBacktestJobQueue()
    service = BacktestLifecycleService(InMemoryBacktestRunStore(), queue)
    return TestClient(create_app(service)), queue, service


def test_post_is_idempotent_and_get_returns_current_status() -> None:
    request = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["backtest_request"]
    client, queue, _ = _client()

    first = client.post("/backtests", json=request)
    second = client.post("/backtests", json=request)
    fetched = client.get(f"/backtests/{request['backtest_run_id']}")

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json() == second.json() == fetched.json()
    assert first.json()["status"] == "QUEUED"
    assert len(queue.messages) == 1


def test_post_rejects_invalid_contract() -> None:
    request = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["backtest_request"]
    request["schema_version"] = 2
    client, _, _ = _client()

    response = client.post("/backtests", json=request)

    assert response.status_code == 422
    assert "schema_version" in response.json()["detail"]


def test_post_rejects_reused_key_with_changed_payload() -> None:
    request = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["backtest_request"]
    changed = copy.deepcopy(request)
    changed["compiled_plan_hash"] = "d" * 64
    client, _, _ = _client()
    client.post("/backtests", json=request)

    response = client.post("/backtests", json=changed)

    assert response.status_code == 409
    assert "idempotency_key" in response.json()["detail"]


def test_get_unknown_run_returns_not_found() -> None:
    client, _, _ = _client()

    response = client.get("/backtests/77777777-7777-4777-8777-777777777777")

    assert response.status_code == 404


def test_get_reflects_worker_lifecycle_result() -> None:
    fixtures = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    request = fixtures["backtest_request"]
    running = fixtures["backtest_results"][1]
    client, _, service = _client()
    client.post("/backtests", json=request)
    service.apply_result(running)

    response = client.get(f"/backtests/{request['backtest_run_id']}")

    assert response.status_code == 200
    assert response.json()["status"] == "RUNNING"
    assert response.json()["status_result"] == running
