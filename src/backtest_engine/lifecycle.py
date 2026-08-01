"""Idempotent backtest acceptance, queue dispatch, and lifecycle transitions."""

from __future__ import annotations

import copy
import json
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Protocol

from .contracts import validate_backtest_request, validate_backtest_result


class IdempotencyConflict(ValueError):
    """Raised when an idempotency key or run ID is reused for other input."""


class BacktestRunNotFound(LookupError):
    """Raised when a lifecycle result or query names an unknown run."""


class InvalidStatusTransition(ValueError):
    """Raised when a result attempts to skip or reverse lifecycle state."""


class BacktestJobQueue(Protocol):
    def publish(self, request: Mapping[str, Any]) -> None: ...


class BacktestRunStore(Protocol):
    def accept(self, request: Mapping[str, Any]) -> tuple[BacktestRun, bool]: ...

    def claim_dispatch(self, run_id: str) -> bool: ...

    def release_dispatch(self, run_id: str) -> None: ...

    def get(self, run_id: str) -> BacktestRun: ...

    def apply_result(self, result: Mapping[str, Any]) -> BacktestRun: ...


class SqsClient(Protocol):
    def send_message(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class BacktestRun:
    backtest_run_id: str
    idempotency_key: str
    status: str
    request: dict[str, Any]
    status_result: dict[str, Any] | None = None
    version: int = 1
    dispatch_pending: bool = True


def _canonical(document: Mapping[str, Any]) -> str:
    return json.dumps(
        document,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )


class InMemoryBacktestJobQueue:
    """Thread-safe local queue fake implementing the production queue boundary."""

    def __init__(self) -> None:
        self._messages: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    @property
    def messages(self) -> list[dict[str, Any]]:
        with self._lock:
            return copy.deepcopy(self._messages)

    def publish(self, request: Mapping[str, Any]) -> None:
        with self._lock:
            self._messages.append(copy.deepcopy(dict(request)))


class SqsBacktestJobQueue:
    """SQS Standard publisher; consumers must remain idempotent at least once."""

    def __init__(self, client: SqsClient, queue_url: str) -> None:
        if not queue_url:
            raise ValueError("queue_url must not be empty")
        self._client = client
        self._queue_url = queue_url

    def publish(self, request: Mapping[str, Any]) -> None:
        self._client.send_message(
            QueueUrl=self._queue_url,
            MessageBody=_canonical(request),
            MessageAttributes={
                "BacktestRunId": {
                    "DataType": "String",
                    "StringValue": str(request["backtest_run_id"]),
                },
                "IdempotencyKey": {
                    "DataType": "String",
                    "StringValue": str(request["idempotency_key"]),
                },
            },
        )


class InMemoryBacktestRunStore:
    """Atomic local store fake for the future durable persistence adapter."""

    _TRANSITIONS = {
        "QUEUED": frozenset({"RUNNING", "FAILED", "UNAVAILABLE"}),
        "RUNNING": frozenset({"COMPLETE", "FAILED", "UNAVAILABLE"}),
        "COMPLETE": frozenset(),
        "FAILED": frozenset(),
        "UNAVAILABLE": frozenset(),
    }

    def __init__(self) -> None:
        self._by_run_id: dict[str, BacktestRun] = {}
        self._run_id_by_request_key: dict[str, str] = {}
        self._result_payloads: dict[str, str] = {}
        self._lock = threading.RLock()

    def accept(self, request: Mapping[str, Any]) -> tuple[BacktestRun, bool]:
        payload = copy.deepcopy(dict(request))
        run_id = str(payload["backtest_run_id"])
        request_key = str(payload["idempotency_key"])
        canonical_payload = _canonical(payload)
        with self._lock:
            existing_run_id = self._run_id_by_request_key.get(request_key)
            if existing_run_id is not None:
                existing = self._by_run_id[existing_run_id]
                if _canonical(existing.request) != canonical_payload:
                    raise IdempotencyConflict(
                        "idempotency_key was already used for another request"
                    )
                return copy.deepcopy(existing), False

            existing = self._by_run_id.get(run_id)
            if existing is not None:
                raise IdempotencyConflict(
                    "backtest_run_id was already used with another idempotency_key"
                )

            run = BacktestRun(
                backtest_run_id=run_id,
                idempotency_key=request_key,
                status="QUEUED",
                request=payload,
            )
            self._by_run_id[run_id] = run
            self._run_id_by_request_key[request_key] = run_id
            return copy.deepcopy(run), True

    def claim_dispatch(self, run_id: str) -> bool:
        with self._lock:
            run = self._require(run_id)
            if not run.dispatch_pending:
                return False
            self._by_run_id[run_id] = replace(run, dispatch_pending=False)
            return True

    def release_dispatch(self, run_id: str) -> None:
        with self._lock:
            run = self._require(run_id)
            if not run.dispatch_pending:
                self._by_run_id[run_id] = replace(run, dispatch_pending=True)

    def get(self, run_id: str) -> BacktestRun:
        with self._lock:
            return copy.deepcopy(self._require(run_id))

    def apply_result(self, result: Mapping[str, Any]) -> BacktestRun:
        payload = copy.deepcopy(dict(result))
        run_id = str(payload["backtest_run_id"])
        result_key = str(payload["idempotency_key"])
        canonical_payload = _canonical(payload)
        with self._lock:
            run = self._require(run_id)
            existing_payload = self._result_payloads.get(result_key)
            if existing_payload is not None:
                if existing_payload != canonical_payload:
                    raise IdempotencyConflict(
                        "result idempotency_key was already used for another payload"
                    )
                return copy.deepcopy(run)

            target = str(payload["status"])
            records_initial_queued_result = (
                run.status == "QUEUED"
                and target == "QUEUED"
                and run.status_result is None
            )
            if not records_initial_queued_result and target not in self._TRANSITIONS[
                run.status
            ]:
                raise InvalidStatusTransition(
                    f"{run.status} cannot transition to {target}"
                )

            updated = replace(
                run,
                status=target,
                status_result=payload,
                version=run.version + 1,
            )
            self._by_run_id[run_id] = updated
            self._result_payloads[result_key] = canonical_payload
            return copy.deepcopy(updated)

    def _require(self, run_id: str) -> BacktestRun:
        try:
            return self._by_run_id[run_id]
        except KeyError as exc:
            raise BacktestRunNotFound(f"backtest run not found: {run_id}") from exc


class BacktestLifecycleService:
    def __init__(
        self,
        store: BacktestRunStore,
        queue: BacktestJobQueue,
    ) -> None:
        self._store = store
        self._queue = queue

    def accept(self, request: Mapping[str, Any]) -> BacktestRun:
        validate_backtest_request(request)
        run, _ = self._store.accept(request)
        if self._store.claim_dispatch(run.backtest_run_id):
            try:
                self._queue.publish(request)
            except Exception:
                self._store.release_dispatch(run.backtest_run_id)
                raise
        return self._store.get(run.backtest_run_id)

    def get(self, run_id: str) -> BacktestRun:
        return self._store.get(run_id)

    def apply_result(self, result: Mapping[str, Any]) -> BacktestRun:
        validate_backtest_result(result)
        return self._store.apply_result(result)
