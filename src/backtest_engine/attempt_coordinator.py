"""Thread-safe attempt, lease, retry, and resource-limit coordination."""

from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from uuid import uuid4


class InvalidAttemptInput(ValueError):
    """Raised when an attempt policy, identity, time, or sample is invalid."""


class LeaseUnavailable(RuntimeError):
    """Raised when another worker still owns the active lease."""


class StaleLease(RuntimeError):
    """Raised when an old or foreign fencing token attempts a mutation."""


class RunTerminal(RuntimeError):
    """Raised when a worker attempts to mutate a completed or failed run."""


class FailureKind(StrEnum):
    LEASE_EXPIRED = "LEASE_EXPIRED"
    TIMEOUT = "TIMEOUT"
    CPU_LIMIT = "CPU_LIMIT"
    MEMORY_LIMIT = "MEMORY_LIMIT"
    RETRYABLE = "RETRYABLE"
    PERMANENT = "PERMANENT"
    SYSTEM_CANCELLED = "SYSTEM_CANCELLED"


class AttemptFailure(RuntimeError):
    """Signals that policy stopped the current worker attempt."""

    def __init__(self, kind: FailureKind, message: str) -> None:
        super().__init__(message)
        self.kind = kind


class RunState(StrEnum):
    WAITING = "WAITING"
    RUNNING = "RUNNING"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class AttemptState(StrEnum):
    ACTIVE = "ACTIVE"
    SUCCEEDED = "SUCCEEDED"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    PERMANENT_FAILED = "PERMANENT_FAILED"
    TIMED_OUT = "TIMED_OUT"
    CPU_LIMIT_EXCEEDED = "CPU_LIMIT_EXCEEDED"
    MEMORY_LIMIT_EXCEEDED = "MEMORY_LIMIT_EXCEEDED"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    SYSTEM_CANCELLED = "SYSTEM_CANCELLED"


@dataclass(frozen=True, slots=True)
class AttemptPolicy:
    """Operator-supplied limits; product defaults are intentionally not invented."""

    max_attempts: int
    lease_duration: timedelta
    attempt_timeout: timedelta
    max_cpu_time: timedelta
    max_memory_bytes: int


@dataclass(frozen=True, slots=True)
class ResourceSample:
    cpu_time: timedelta
    memory_bytes: int

    def __post_init__(self) -> None:
        if self.cpu_time < timedelta(0):
            raise InvalidAttemptInput("cpu_time must not be negative")
        if self.memory_bytes < 0:
            raise InvalidAttemptInput("memory_bytes must not be negative")


@dataclass(frozen=True, slots=True)
class AttemptLease:
    run_id: str
    attempt_id: str
    attempt_number: int
    worker_id: str
    lease_token: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    run_id: str
    attempt_id: str
    attempt_number: int
    worker_id: str
    lease_token: str
    state: AttemptState
    started_at: datetime
    lease_expires_at: datetime
    last_heartbeat_at: datetime
    cpu_time: timedelta
    memory_bytes: int
    ended_at: datetime | None = None
    failure_kind: FailureKind | None = None
    reason_code: str | None = None


_FAILURE_STATES = {
    FailureKind.LEASE_EXPIRED: AttemptState.LEASE_EXPIRED,
    FailureKind.TIMEOUT: AttemptState.TIMED_OUT,
    FailureKind.CPU_LIMIT: AttemptState.CPU_LIMIT_EXCEEDED,
    FailureKind.MEMORY_LIMIT: AttemptState.MEMORY_LIMIT_EXCEEDED,
    FailureKind.RETRYABLE: AttemptState.RETRYABLE_FAILED,
    FailureKind.PERMANENT: AttemptState.PERMANENT_FAILED,
    FailureKind.SYSTEM_CANCELLED: AttemptState.SYSTEM_CANCELLED,
}


class AttemptCoordinator:
    """In-memory domain aggregate mirroring a durable compare-and-set boundary.

    Each acquisition receives a new fencing token. A persistence adapter can use the
    token and attempt number as its conditional-update generation.
    """

    def __init__(
        self,
        run_id: str,
        policy: AttemptPolicy,
        queued_at: datetime,
    ) -> None:
        _require_text(run_id, "run_id")
        _require_aware(queued_at)
        _validate_policy(policy)
        self._run_id = run_id
        self._policy = policy
        self._queued_at = queued_at
        self._state = RunState.WAITING
        self._attempts: list[AttemptRecord] = []
        self._result_manifest_id: str | None = None
        self._detail_manifest_id: str | None = None
        self._lock = threading.RLock()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def state(self) -> RunState:
        with self._lock:
            return self._state

    @property
    def attempts(self) -> tuple[AttemptRecord, ...]:
        with self._lock:
            return tuple(self._attempts)

    @property
    def result_manifest_id(self) -> str | None:
        with self._lock:
            return self._result_manifest_id

    @property
    def detail_manifest_id(self) -> str | None:
        with self._lock:
            return self._detail_manifest_id

    def acquire(self, worker_id: str, now: datetime) -> AttemptLease:
        _require_text(worker_id, "worker_id")
        _require_aware(now)
        with self._lock:
            self._require_nonterminal()
            self._require_not_before(now, self._queued_at)
            active = self._active_attempt()
            if active is not None and now < active.lease_expires_at:
                raise LeaseUnavailable(
                    f"active lease is held by {active.worker_id} until "
                    f"{active.lease_expires_at.isoformat()}"
                )
            if active is not None:
                self._stop_active(
                    now,
                    FailureKind.LEASE_EXPIRED,
                    "LEASE_EXPIRED",
                    retryable=True,
                )
                if self._state is RunState.FAILED:
                    raise RunTerminal("backtest run is terminal: FAILED")

            number = len(self._attempts) + 1
            if number > self._policy.max_attempts:
                self._state = RunState.FAILED
                raise RunTerminal("backtest run is terminal: FAILED")
            attempt_id = str(uuid4())
            lease_token = str(uuid4())
            expires_at = now + self._policy.lease_duration
            record = AttemptRecord(
                run_id=self._run_id,
                attempt_id=attempt_id,
                attempt_number=number,
                worker_id=worker_id,
                lease_token=lease_token,
                state=AttemptState.ACTIVE,
                started_at=now,
                lease_expires_at=expires_at,
                last_heartbeat_at=now,
                cpu_time=timedelta(0),
                memory_bytes=0,
            )
            self._attempts.append(record)
            self._state = RunState.RUNNING
            return _lease(record)

    def heartbeat(
        self,
        lease: AttemptLease,
        now: datetime,
        sample: ResourceSample,
    ) -> AttemptLease:
        _require_aware(now)
        with self._lock:
            active = self._require_live_lease(lease, now)
            if sample.cpu_time < active.cpu_time:
                raise InvalidAttemptInput("cpu_time must be monotonic")

            elapsed = now - active.started_at
            failure: tuple[FailureKind, str] | None = None
            if elapsed >= self._policy.attempt_timeout:
                failure = (FailureKind.TIMEOUT, "attempt timeout exceeded")
            elif sample.cpu_time > self._policy.max_cpu_time:
                failure = (FailureKind.CPU_LIMIT, "attempt CPU limit exceeded")
            elif sample.memory_bytes > self._policy.max_memory_bytes:
                failure = (FailureKind.MEMORY_LIMIT, "attempt memory limit exceeded")

            if failure is not None:
                kind, message = failure
                self._stop_active(now, kind, kind.value, retryable=True, sample=sample)
                raise AttemptFailure(kind, message)

            updated = replace(
                active,
                lease_expires_at=now + self._policy.lease_duration,
                last_heartbeat_at=now,
                cpu_time=sample.cpu_time,
                memory_bytes=sample.memory_bytes,
            )
            self._attempts[-1] = updated
            return _lease(updated)

    def fail(
        self,
        lease: AttemptLease,
        now: datetime,
        *,
        reason_code: str,
        retryable: bool,
    ) -> None:
        _require_aware(now)
        _require_text(reason_code, "reason_code")
        with self._lock:
            self._require_live_lease(lease, now)
            kind = FailureKind.RETRYABLE if retryable else FailureKind.PERMANENT
            self._stop_active(now, kind, reason_code, retryable=retryable)

    def complete(
        self,
        lease: AttemptLease,
        now: datetime,
        result_manifest_id: str,
        detail_manifest_id: str,
    ) -> None:
        _require_aware(now)
        _require_text(result_manifest_id, "result_manifest_id")
        _require_text(detail_manifest_id, "detail_manifest_id")
        with self._lock:
            active = self._require_live_lease(lease, now)
            self._attempts[-1] = replace(
                active,
                state=AttemptState.SUCCEEDED,
                ended_at=now,
            )
            self._result_manifest_id = result_manifest_id
            self._detail_manifest_id = detail_manifest_id
            self._state = RunState.COMPLETE

    def cancel_by_system(self, now: datetime, *, reason_code: str) -> None:
        """Fail closed for an operator/runtime stop; user cancellation is absent."""

        _require_aware(now)
        _require_text(reason_code, "reason_code")
        with self._lock:
            self._require_nonterminal()
            active = self._active_attempt()
            if active is not None:
                self._require_not_before(now, active.last_heartbeat_at)
                self._stop_active(
                    now,
                    FailureKind.SYSTEM_CANCELLED,
                    reason_code,
                    retryable=False,
                )
            else:
                self._require_not_before(now, self._queued_at)
                self._state = RunState.FAILED

    def _active_attempt(self) -> AttemptRecord | None:
        if self._attempts and self._attempts[-1].state is AttemptState.ACTIVE:
            return self._attempts[-1]
        return None

    def _require_nonterminal(self) -> None:
        if self._state in (RunState.COMPLETE, RunState.FAILED):
            raise RunTerminal(f"backtest run is terminal: {self._state.value}")

    def _require_lease(self, lease: AttemptLease) -> AttemptRecord:
        self._require_nonterminal()
        active = self._active_attempt()
        if (
            active is None
            or lease.run_id != self._run_id
            or lease.attempt_id != active.attempt_id
            or lease.attempt_number != active.attempt_number
            or lease.worker_id != active.worker_id
            or lease.lease_token != active.lease_token
        ):
            raise StaleLease("lease fencing token is stale or foreign")
        return active

    def _require_live_lease(
        self, lease: AttemptLease, now: datetime
    ) -> AttemptRecord:
        active = self._require_lease(lease)
        self._require_not_before(now, active.last_heartbeat_at)
        if now >= active.lease_expires_at:
            self._stop_active(
                now,
                FailureKind.LEASE_EXPIRED,
                "LEASE_EXPIRED",
                retryable=True,
            )
            raise AttemptFailure(FailureKind.LEASE_EXPIRED, "attempt lease expired")
        return active

    def _stop_active(
        self,
        now: datetime,
        kind: FailureKind,
        reason_code: str,
        *,
        retryable: bool,
        sample: ResourceSample | None = None,
    ) -> None:
        active = self._active_attempt()
        if active is None:
            raise StaleLease("no active attempt")
        self._require_not_before(now, active.last_heartbeat_at)
        updated = replace(
            active,
            state=_FAILURE_STATES[kind],
            ended_at=now,
            failure_kind=kind,
            reason_code=reason_code,
            last_heartbeat_at=now if sample is not None else active.last_heartbeat_at,
            cpu_time=sample.cpu_time if sample is not None else active.cpu_time,
            memory_bytes=sample.memory_bytes if sample is not None else active.memory_bytes,
        )
        self._attempts[-1] = updated
        can_retry = retryable and len(self._attempts) < self._policy.max_attempts
        self._state = RunState.WAITING if can_retry else RunState.FAILED

    @staticmethod
    def _require_not_before(now: datetime, lower_bound: datetime) -> None:
        if now < lower_bound:
            raise InvalidAttemptInput("event time cannot move backwards")


def _lease(record: AttemptRecord) -> AttemptLease:
    return AttemptLease(
        run_id=record.run_id,
        attempt_id=record.attempt_id,
        attempt_number=record.attempt_number,
        worker_id=record.worker_id,
        lease_token=record.lease_token,
        expires_at=record.lease_expires_at,
    )


def _require_text(value: str, field: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InvalidAttemptInput(f"{field} must not be blank")


def _require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise InvalidAttemptInput("event time must be timezone-aware")


def _validate_policy(policy: AttemptPolicy) -> None:
    if policy.max_attempts <= 0:
        raise InvalidAttemptInput("max_attempts must be positive")
    if policy.lease_duration <= timedelta(0):
        raise InvalidAttemptInput("lease_duration must be positive")
    if policy.attempt_timeout <= timedelta(0):
        raise InvalidAttemptInput("attempt_timeout must be positive")
    if policy.max_cpu_time <= timedelta(0):
        raise InvalidAttemptInput("max_cpu_time must be positive")
    if policy.max_memory_bytes <= 0:
        raise InvalidAttemptInput("max_memory_bytes must be positive")
