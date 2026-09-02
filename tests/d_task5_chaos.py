"""Deterministic Task 5 chaos controls and sanitized evidence fragments.

This module is integration-owned.  Production code has no fault flags, sleeps, or
checkpoint files; tests place gates on existing dependency boundaries instead.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from backtest_engine.attempt_coordinator import (
    AttemptPolicy,
    ProcessResourceMonitor,
    ResourceSample,
)
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


class BoundedProcessMonitorFactory:
    """Create per-attempt production monitors over small isolated workloads.

    The workload constants bound how much work the child may do; none of them is
    copied into evidence.  Evidence receives only samples read by the production
    ``ProcessResourceMonitor`` and returned to the attempt coordinator.
    """

    _CPU_EVIDENCE_SECONDS = 0.08
    _MEMORY_EVIDENCE_DELTA = 16 * 1024 * 1024

    def __init__(
        self,
        mode: str,
        observer: ResourcePeakObserver,
        root: Path,
    ) -> None:
        if mode not in {"cpu", "memory"}:
            raise ValueError("bounded process monitor mode must be cpu or memory")
        self._mode = mode
        self._observer = observer
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)
        self._monitors: list[_BoundedProcessMonitor] = []
        baseline = self._start_monitor("baseline")
        try:
            self._baseline_memory_bytes = baseline.baseline.memory_bytes
        finally:
            baseline.close()

    @property
    def policy(self) -> AttemptPolicy:
        return AttemptPolicy(
            max_attempts=3,
            lease_duration=timedelta(minutes=5),
            attempt_timeout=timedelta(minutes=30),
            max_cpu_time=(
                timedelta(seconds=0.05)
                if self._mode == "cpu"
                else timedelta(seconds=30)
            ),
            max_memory_bytes=(
                self._baseline_memory_bytes + 8 * 1024 * 1024
                if self._mode == "memory"
                else self._baseline_memory_bytes + 128 * 1024 * 1024
            ),
        )

    def __call__(self) -> _BoundedProcessMonitor:
        return self.new_monitor()

    def new_monitor(self) -> _BoundedProcessMonitor:
        monitor = self._start_monitor(f"attempt-{len(self._monitors) + 1}")
        self._monitors.append(monitor)
        return monitor

    def close(self) -> None:
        for monitor in self._monitors:
            monitor.close()

    def _start_monitor(self, label: str) -> _BoundedProcessMonitor:
        stem = f"{label}-{uuid.uuid4().hex}"
        marker = self._root / f"{stem}.ready"
        command = self._root / f"{stem}.command"
        exit_marker = self._root / f"{stem}.exit"
        # On Windows ``venv/Scripts/python.exe`` is a launcher process; psutil
        # would then sample the idle launcher rather than the workload child.
        interpreter = str(getattr(sys, "_base_executable", sys.executable))
        process = subprocess.Popen(
            [
                interpreter,
                str(Path(__file__).with_name("d_task5_resource_probe.py")),
                str(marker),
                str(command),
                str(exit_marker),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        def child_ready() -> bool:
            if process.poll() is not None:
                raise RuntimeError(
                    f"bounded resource child exited before ready: {process.returncode}"
                )
            return marker.is_file()

        wait_until(
            child_ready,
            description=f"bounded {self._mode} resource child to become ready",
            timeout_seconds=10,
        )
        return _BoundedProcessMonitor(
            mode=self._mode,
            process=process,
            marker=marker,
            command=command,
            exit_marker=exit_marker,
            observer=self._observer,
        )


class _BoundedProcessMonitor:
    def __init__(
        self,
        *,
        mode: str,
        process: subprocess.Popen[bytes],
        marker: Path,
        command: Path,
        exit_marker: Path,
        observer: ResourcePeakObserver,
    ) -> None:
        self._mode = mode
        self._process = process
        self._marker = marker
        self._command = command
        self._exit_marker = exit_marker
        self._observer = observer
        self._delegate = ProcessResourceMonitor(process.pid)
        self.baseline = self._delegate.sample()
        self._started = False

    def sample(self) -> ResourceSample:
        if not self._started:
            self._command.write_text(self._mode, encoding="utf-8")
            self._started = True

        def budget_crossed() -> ResourceSample | None:
            if self._process.poll() is not None:
                raise RuntimeError(
                    f"bounded resource child exited during observation: {self._process.returncode}"
                )
            measured = self._delegate.sample()
            crossed = (
                measured.cpu_time.total_seconds()
                >= BoundedProcessMonitorFactory._CPU_EVIDENCE_SECONDS
                if self._mode == "cpu"
                else measured.memory_bytes
                >= self.baseline.memory_bytes
                + BoundedProcessMonitorFactory._MEMORY_EVIDENCE_DELTA
            )
            return measured if crossed else None

        measured = wait_until(
            budget_crossed,
            description=f"real child {self._mode} usage to cross the test policy",
            timeout_seconds=10,
            poll_seconds=0.01,
        )
        self._observer.observe("cpu_seconds", measured.cpu_time.total_seconds())
        self._observer.observe("memory_bytes", measured.memory_bytes)
        self.close()
        return measured

    def close(self) -> None:
        if self._process.poll() is None:
            self._exit_marker.write_text("exit", encoding="utf-8")
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        for path in (self._marker, self._command, self._exit_marker):
            path.unlink(missing_ok=True)


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
