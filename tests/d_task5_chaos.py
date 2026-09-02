"""Deterministic Task 5 chaos controls and sanitized evidence fragments.

This module is integration-owned.  Production code has no fault flags, sleeps, or
checkpoint files; tests place gates on existing dependency boundaries instead.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class ObservedResourcePeaks:
    """Peak values that a boundary observer actually sampled."""

    values: Mapping[str, float]
    observation_count: int


class ResourcePeakObserver:
    """Thread-safe recorder for production-boundary resource/concurrency samples."""

    def __init__(self) -> None:
        self._values: dict[str, float] = {}
        self._count = 0
        self._lock = threading.Lock()

    def observe(self, resource: str, value: int | float) -> None:
        measured = float(value)
        if not resource.strip() or not math.isfinite(measured) or measured < 0:
            raise ValueError("resource observations must be named, finite, and non-negative")
        with self._lock:
            self._count += 1
            self._values[resource] = max(measured, self._values.get(resource, measured))

    def snapshot(self) -> ObservedResourcePeaks:
        with self._lock:
            if self._count < 1:
                raise ValueError("no resource peak was observed")
            return ObservedResourcePeaks(dict(self._values), self._count)


def evidence_result(
    *,
    scenario: str,
    terminal_state: str,
    duration_seconds: float,
    run_id: str,
    attempt_lineage: Sequence[str],
    input_fingerprint: str,
    result_hash: str,
    observations: Mapping[str, Any] | None = None,
    trade_kind_counts: Mapping[str, int] | None = None,
    failure_reason: str | None = None,
    resource_peak: ObservedResourcePeaks | None = None,
) -> dict[str, Any]:
    """Create one Task 2-shaped result from independently observed facts."""

    if observations:
        raise ValueError(
            "output observations cannot establish an independent canonical input identity"
        )
    if not isinstance(input_fingerprint, str) or not input_fingerprint.startswith("sha256:"):
        raise ValueError("input_fingerprint must be a canonical sha256 identity")
    input_digest = input_fingerprint.removeprefix("sha256:")
    result_digest = str(result_hash).removeprefix("sha256:")
    if len(input_digest) != 64 or any(char not in "0123456789abcdef" for char in input_digest):
        raise ValueError("input_fingerprint must be a canonical sha256 identity")
    if len(result_digest) != 64 or any(char not in "0123456789abcdef" for char in result_digest):
        raise ValueError("result_hash must be a canonical sha256 identity")
    if input_digest == result_digest:
        raise ValueError("input and terminal/result identities must be independent")
    duration = float(duration_seconds)
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError("duration_seconds must be a positive observed interval")
    if resource_peak is not None and not isinstance(resource_peak, ObservedResourcePeaks):
        raise TypeError("resource_peak must come from an observed production boundary")

    return {
        "scenario": scenario,
        "seed": TASK5_SEED,
        "input_fingerprint": input_fingerprint,
        "terminal_state": terminal_state,
        "duration_seconds": duration,
        "run_id": run_id,
        "attempt_lineage": list(attempt_lineage),
        "result_hash": result_hash,
        "trade_kind_counts": dict(trade_kind_counts or {}),
        "failure_reason": failure_reason,
        "resource_peak": (
            {}
            if resource_peak is None
            else {key: float(value) for key, value in resource_peak.values.items()}
        ),
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
