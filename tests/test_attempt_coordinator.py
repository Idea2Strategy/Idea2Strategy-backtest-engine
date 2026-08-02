from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from threading import Barrier

import pytest

from backtest_engine.attempt_coordinator import (
    AttemptCoordinator,
    AttemptFailure,
    AttemptPolicy,
    AttemptState,
    FailureKind,
    InvalidAttemptInput,
    LeaseUnavailable,
    ProcessResourceMonitor,
    ResourceSample,
    RunState,
    RunTerminal,
    StaleLease,
)


RUN_ID = "00000000-0000-4000-8000-000000002801"
RESULT_MANIFEST_ID = "00000000-0000-4000-8000-000000002802"
DETAIL_MANIFEST_ID = "00000000-0000-4000-8000-000000002803"
T0 = datetime(2025, 11, 28, 14, 30, tzinfo=timezone.utc)


def _policy(*, max_attempts: int = 3) -> AttemptPolicy:
    return AttemptPolicy(
        max_attempts=max_attempts,
        lease_duration=timedelta(seconds=30),
        attempt_timeout=timedelta(minutes=10),
        max_cpu_time=timedelta(minutes=5),
        max_memory_bytes=512 * 1024 * 1024,
    )


def _coordinator(*, max_attempts: int = 3) -> AttemptCoordinator:
    return AttemptCoordinator(RUN_ID, _policy(max_attempts=max_attempts), T0)


def _sample(*, cpu_seconds: int = 1, memory_bytes: int = 1024) -> ResourceSample:
    return ResourceSample(
        cpu_time=timedelta(seconds=cpu_seconds),
        memory_bytes=memory_bytes,
    )


def test_only_one_worker_holds_the_active_lease() -> None:
    coordinator = _coordinator()

    lease = coordinator.acquire("worker-a", T0)

    assert lease.run_id == RUN_ID
    assert lease.attempt_number == 1
    assert coordinator.state is RunState.RUNNING
    with pytest.raises(LeaseUnavailable, match="worker-a"):
        coordinator.acquire("worker-b", T0 + timedelta(seconds=1))


def test_simultaneous_workers_receive_exactly_one_lease() -> None:
    coordinator = _coordinator()
    barrier = Barrier(8)

    def acquire(worker_number: int) -> bool:
        barrier.wait()
        try:
            coordinator.acquire(f"worker-{worker_number}", T0)
        except LeaseUnavailable:
            return False
        return True

    with ThreadPoolExecutor(max_workers=8) as executor:
        acquired = list(executor.map(acquire, range(8)))

    assert acquired.count(True) == 1
    assert acquired.count(False) == 7
    assert len(coordinator.attempts) == 1


def test_heartbeat_renews_lease_and_rejects_non_monotonic_cpu_usage() -> None:
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)

    renewed = coordinator.heartbeat(
        lease,
        T0 + timedelta(seconds=20),
        _sample(cpu_seconds=10),
    )

    assert renewed.lease_token == lease.lease_token
    assert renewed.expires_at == T0 + timedelta(seconds=50)
    with pytest.raises(InvalidAttemptInput, match="cpu_time"):
        coordinator.heartbeat(
            renewed,
            T0 + timedelta(seconds=21),
            _sample(cpu_seconds=9),
        )


def test_expired_lease_is_retried_on_same_run_and_stale_worker_is_fenced() -> None:
    coordinator = _coordinator()
    first = coordinator.acquire("worker-a", T0)

    second = coordinator.acquire("worker-b", T0 + timedelta(seconds=30))

    assert second.run_id == first.run_id
    assert second.attempt_number == 2
    assert second.lease_token != first.lease_token
    assert coordinator.attempts[0].state is AttemptState.LEASE_EXPIRED
    assert coordinator.attempts[0].failure_kind is FailureKind.LEASE_EXPIRED
    with pytest.raises(StaleLease):
        coordinator.complete(
            first,
            T0 + timedelta(seconds=31),
            RESULT_MANIFEST_ID,
            DETAIL_MANIFEST_ID,
        )


def test_late_heartbeat_cannot_resurrect_an_expired_lease() -> None:
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)

    with pytest.raises(AttemptFailure, match="lease expired") as failure:
        coordinator.heartbeat(lease, T0 + timedelta(seconds=30), _sample())

    assert failure.value.kind is FailureKind.LEASE_EXPIRED
    assert coordinator.state is RunState.WAITING
    assert coordinator.attempts[0].state is AttemptState.LEASE_EXPIRED


@pytest.mark.parametrize(
    ("sample", "kind", "message"),
    [
        (_sample(cpu_seconds=301), FailureKind.CPU_LIMIT, "CPU"),
        (
            _sample(memory_bytes=512 * 1024 * 1024 + 1),
            FailureKind.MEMORY_LIMIT,
            "memory",
        ),
    ],
)
def test_resource_limit_stops_attempt_and_schedules_retry(
    sample: ResourceSample,
    kind: FailureKind,
    message: str,
) -> None:
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)

    with pytest.raises(AttemptFailure, match=message) as failure:
        coordinator.heartbeat(lease, T0 + timedelta(seconds=10), sample)

    assert failure.value.kind is kind
    assert coordinator.state is RunState.WAITING
    assert coordinator.attempts[0].failure_kind is kind
    assert coordinator.result_manifest_id is None
    assert coordinator.detail_manifest_id is None


def test_timeout_is_measured_from_attempt_start_and_exhaustion_fails_run() -> None:
    coordinator = AttemptCoordinator(
        RUN_ID,
        replace(
            _policy(max_attempts=1),
            lease_duration=timedelta(minutes=20),
        ),
        T0,
    )
    lease = coordinator.acquire("worker-a", T0)

    with pytest.raises(AttemptFailure, match="timeout") as failure:
        coordinator.heartbeat(
            lease,
            T0 + timedelta(minutes=10),
            _sample(),
        )

    assert failure.value.kind is FailureKind.TIMEOUT
    assert coordinator.state is RunState.FAILED
    assert coordinator.attempts[0].state is AttemptState.TIMED_OUT
    with pytest.raises(RunTerminal, match="FAILED"):
        coordinator.acquire("worker-b", T0 + timedelta(minutes=11))


def test_retryable_and_permanent_failures_have_distinct_retry_behavior() -> None:
    coordinator = _coordinator()
    first = coordinator.acquire("worker-a", T0)

    coordinator.fail(
        first,
        T0 + timedelta(seconds=5),
        reason_code="OBJECT_STORE_TEMPORARY",
        retryable=True,
    )
    second = coordinator.acquire("worker-b", T0 + timedelta(seconds=6))
    coordinator.fail(
        second,
        T0 + timedelta(seconds=7),
        reason_code="INPUT_CORRUPT",
        retryable=False,
    )

    assert coordinator.state is RunState.FAILED
    assert [attempt.attempt_number for attempt in coordinator.attempts] == [1, 2]
    assert coordinator.attempts[0].state is AttemptState.RETRYABLE_FAILED
    assert coordinator.attempts[1].state is AttemptState.PERMANENT_FAILED
    assert coordinator.attempts[1].reason_code == "INPUT_CORRUPT"


def test_success_atomically_publishes_manifests_and_fences_late_messages() -> None:
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)

    coordinator.complete(
        lease,
        T0 + timedelta(seconds=5),
        RESULT_MANIFEST_ID,
        DETAIL_MANIFEST_ID,
    )

    assert coordinator.state is RunState.COMPLETED
    assert coordinator.result_manifest_id == RESULT_MANIFEST_ID
    assert coordinator.detail_manifest_id == DETAIL_MANIFEST_ID
    assert coordinator.attempts[0].state is AttemptState.SUCCEEDED
    with pytest.raises(RunTerminal, match="COMPLETE"):
        coordinator.fail(
            lease,
            T0 + timedelta(seconds=6),
            reason_code="LATE_FAILURE",
            retryable=True,
        )


def test_system_cancellation_never_retries_even_with_attempts_remaining() -> None:
    coordinator = _coordinator(max_attempts=3)
    lease = coordinator.acquire("worker-a", T0)

    coordinator.cancel_by_system(
        T0 + timedelta(seconds=5), reason_code="DEPLOYMENT_SHUTDOWN"
    )

    assert coordinator.state is RunState.FAILED
    assert coordinator.attempts[0].state is AttemptState.SYSTEM_CANCELLED
    assert coordinator.attempts[0].failure_kind is FailureKind.SYSTEM_CANCELLED
    assert coordinator.attempts[0].reason_code == "DEPLOYMENT_SHUTDOWN"
    # Two attempts were still budgeted. A retryable failure would have left the
    # run WAITING and let worker-b back in; cancellation must not.
    assert len(coordinator.attempts) == 1
    with pytest.raises(RunTerminal, match="FAILED"):
        coordinator.acquire("worker-b", T0 + timedelta(seconds=6))
    with pytest.raises(RunTerminal, match="FAILED"):
        coordinator.heartbeat(lease, T0 + timedelta(seconds=6), _sample())


@pytest.mark.parametrize(
    "policy",
    [
        replace(_policy(), max_attempts=0),
        replace(_policy(), lease_duration=timedelta(0)),
        replace(_policy(), attempt_timeout=timedelta(0)),
        replace(_policy(), max_cpu_time=timedelta(0)),
        replace(_policy(), max_memory_bytes=0),
    ],
)
def test_policy_rejects_non_positive_values(policy: AttemptPolicy) -> None:
    with pytest.raises(InvalidAttemptInput):
        AttemptCoordinator(RUN_ID, policy, T0)


# --------------------------------------------------------------------------
# D29 -- cooperative cancellation
# --------------------------------------------------------------------------


def test_cancellation_request_is_observed_by_the_next_heartbeat() -> None:
    """A long replay must be stoppable without killing the process."""
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)

    coordinator.request_cancellation(
        T0 + timedelta(seconds=3),
        reason_code="USER_CANCELLED",
        requested_by="00000000-0000-4000-8000-0000000028aa",
    )

    request = coordinator.cancellation
    assert request is not None
    assert request.reason_code == "USER_CANCELLED"
    assert request.requested_by == "00000000-0000-4000-8000-0000000028aa"
    # The attempt is still live until the worker reaches its next checkpoint.
    assert coordinator.state is RunState.RUNNING

    with pytest.raises(AttemptFailure, match="cancelled") as failure:
        coordinator.heartbeat(lease, T0 + timedelta(seconds=4), _sample())

    assert failure.value.kind is FailureKind.CANCELLED
    assert coordinator.state is RunState.CANCELLED
    assert coordinator.attempts[0].state is AttemptState.CANCELLED
    assert coordinator.attempts[0].reason_code == "USER_CANCELLED"


def test_cancellation_is_terminal_and_never_retried() -> None:
    coordinator = _coordinator(max_attempts=3)
    lease = coordinator.acquire("worker-a", T0)
    coordinator.request_cancellation(
        T0 + timedelta(seconds=1), reason_code="USER_CANCELLED", requested_by="owner"
    )
    with pytest.raises(AttemptFailure):
        coordinator.heartbeat(lease, T0 + timedelta(seconds=2), _sample())

    with pytest.raises(RunTerminal, match="CANCELLED"):
        coordinator.acquire("worker-b", T0 + timedelta(seconds=3))
    assert coordinator.result_manifest_id is None


def test_cancelling_a_queued_run_is_immediate_and_needs_no_attempt() -> None:
    coordinator = _coordinator()

    coordinator.request_cancellation(
        T0 + timedelta(seconds=1), reason_code="USER_CANCELLED", requested_by="owner"
    )

    assert coordinator.state is RunState.CANCELLED
    assert coordinator.attempts == ()
    with pytest.raises(RunTerminal, match="CANCELLED"):
        coordinator.acquire("worker-a", T0 + timedelta(seconds=2))


def test_completed_run_cannot_be_cancelled_afterwards() -> None:
    coordinator = _coordinator()
    lease = coordinator.acquire("worker-a", T0)
    coordinator.complete(
        lease, T0 + timedelta(seconds=1), RESULT_MANIFEST_ID, DETAIL_MANIFEST_ID
    )

    with pytest.raises(RunTerminal, match="COMPLETED"):
        coordinator.request_cancellation(
            T0 + timedelta(seconds=2),
            reason_code="USER_CANCELLED",
            requested_by="owner",
        )
    assert coordinator.result_manifest_id == RESULT_MANIFEST_ID


# --------------------------------------------------------------------------
# D29 -- real resource measurement (not a simulated counter)
# --------------------------------------------------------------------------


def test_monitor_reports_the_real_process_cpu_and_memory_growth() -> None:
    """The sample must move with actual work, not with a call counter."""
    monitor = ProcessResourceMonitor()
    baseline = monitor.sample()

    ballast = bytearray(96 * 1024 * 1024)
    ballast[:: 4096] = b"\x01" * len(ballast[:: 4096])
    total = 0
    deadline = time.monotonic() + 0.4
    while time.monotonic() < deadline:
        total += sum(range(2048))

    after = monitor.sample()

    assert total > 0
    assert after.memory_bytes - baseline.memory_bytes > 48 * 1024 * 1024
    assert after.cpu_time > baseline.cpu_time
    assert after.cpu_time >= timedelta(seconds=0.2)
    del ballast


def test_monitor_cpu_time_is_measured_from_its_own_baseline() -> None:
    """Two monitors created at different times must not share a CPU origin."""
    first = ProcessResourceMonitor()
    deadline = time.monotonic() + 0.3
    while time.monotonic() < deadline:
        sum(range(2048))
    second = ProcessResourceMonitor()

    assert first.sample().cpu_time > second.sample().cpu_time


def test_monitor_names_the_enforcement_mechanism_actually_in_force() -> None:
    monitor = ProcessResourceMonitor()

    applied = monitor.apply_hard_limits(_policy())

    assert "psutil-sampling" in applied
    if sys.platform == "win32":
        # Windows has no POSIX rlimit; the sampler is the only enforcement.
        assert applied == ("psutil-sampling",)
    else:  # pragma: no cover - exercised on the Linux CI runner
        assert "rlimit-cpu" in applied
        assert "rlimit-as" in applied


def test_monitor_sample_drives_the_coordinator_limit_to_a_real_failure() -> None:
    """End to end: a real RSS reading trips a 1-byte budget."""
    coordinator = AttemptCoordinator(
        RUN_ID, replace(_policy(), max_memory_bytes=1), T0
    )
    lease = coordinator.acquire("worker-a", T0)
    monitor = ProcessResourceMonitor()

    with pytest.raises(AttemptFailure, match="memory") as failure:
        coordinator.heartbeat(lease, T0 + timedelta(seconds=1), monitor.sample())

    assert failure.value.kind is FailureKind.MEMORY_LIMIT
    assert coordinator.attempts[0].memory_bytes > 1


def test_rejects_naive_time_invalid_usage_and_blank_identity() -> None:
    with pytest.raises(InvalidAttemptInput, match="timezone-aware"):
        AttemptCoordinator(RUN_ID, _policy(), datetime(2025, 11, 28, 14, 30))
    with pytest.raises(InvalidAttemptInput, match="run_id"):
        AttemptCoordinator(" ", _policy(), T0)
    with pytest.raises(InvalidAttemptInput, match="worker_id"):
        _coordinator().acquire(" ", T0)
    with pytest.raises(InvalidAttemptInput, match="cpu_time"):
        ResourceSample(cpu_time=timedelta(seconds=-1), memory_bytes=1)
    with pytest.raises(InvalidAttemptInput, match="memory_bytes"):
        ResourceSample(cpu_time=timedelta(0), memory_bytes=-1)
