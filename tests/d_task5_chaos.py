"""Deterministic Task 5 chaos controls and sanitized evidence fragments.

This module is integration-owned.  Production code has no fault flags, sleeps, or
checkpoint files; tests place gates on existing dependency boundaries instead.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from backtest_engine.contracts import (
    compute_message_idempotency_key,
    official_backtest_operation_key,
)


TASK5_SEED = "task5-chaos-v1"
TASK5_NAMESPACE = uuid.UUID("9f3bb614-8f86-4ef7-916c-a8f14f5df87a")


def wait_until(
    condition: Callable[[], Any],
    *,
    description: str,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.05,
) -> Any:
    """Poll fresh state until the exact condition holds, with a finite deadline."""

    deadline = time.monotonic() + timeout_seconds
    waiter = threading.Event()
    while True:
        result = condition()
        if result:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"timed out waiting for {description}")
        waiter.wait(min(poll_seconds, remaining))


def task5_run_id(scenario: str) -> str:
    return str(uuid.uuid5(TASK5_NAMESPACE, scenario))


def task5_request(base: Mapping[str, Any], scenario: str) -> dict[str, Any]:
    """Give a real request a deterministic Task 5-only run and execution key."""

    request = json.loads(json.dumps(base))
    request["runId"] = task5_run_id(f"run:{scenario}")
    request["assumptionsVersion"] = f"accounting:task5:{scenario}"
    metadata = request["metadata"]
    metadata["messageId"] = task5_run_id(f"message:{scenario}")
    metadata["correlationId"] = request["runId"]
    metadata["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=metadata["contractVersion"],
        message_type=metadata["messageType"],
        aggregate_id=request["botId"],
        snapshot_hash=request["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(request),
    )
    request.pop("requestHash", None)
    request["requestHash"] = (
        "sha256:"
        + hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    )
    return request


def canonical_digest(value: Mapping[str, Any] | Sequence[Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def evidence_result(
    *,
    scenario: str,
    terminal_state: str,
    duration_seconds: float,
    run_id: str,
    attempt_lineage: Sequence[str],
    observations: Mapping[str, Any],
    trade_kind_counts: Mapping[str, int] | None = None,
    failure_reason: str | None = None,
    resource_peak: Mapping[str, float] | None = None,
    result_hash: str | None = None,
) -> dict[str, Any]:
    """Create one Task 2-shaped result from independently observed facts."""

    return {
        "scenario": scenario,
        "seed": TASK5_SEED,
        "input_fingerprint": f"sha256:{canonical_digest(observations)}",
        "terminal_state": terminal_state,
        "duration_seconds": max(0.0, float(duration_seconds)),
        "run_id": run_id,
        "attempt_lineage": list(attempt_lineage),
        "result_hash": result_hash or canonical_digest(observations),
        "trade_kind_counts": dict(trade_kind_counts or {}),
        "failure_reason": failure_reason,
        "resource_peak": {key: float(value) for key, value in (resource_peak or {}).items()},
    }


def record_evidence(result: Mapping[str, Any]) -> None:
    """Write a safe fragment only during the explicit local Task 5 evidence run."""

    directory_value = os.environ.get("BACKTEST_TASK5_EVIDENCE_DIR", "").strip()
    if not directory_value:
        return
    scenario = result.get("scenario")
    if not isinstance(scenario, str) or not scenario.startswith("task5-"):
        raise ValueError("Task 5 evidence scenario is invalid")
    directory = Path(directory_value)
    directory.mkdir(parents=True, exist_ok=True)
    name = hashlib.sha256(scenario.encode("utf-8")).hexdigest()[:20] + ".json"
    target = directory / name
    temporary = target.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(dict(result), indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    temporary.replace(target)
