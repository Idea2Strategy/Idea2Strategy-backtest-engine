"""Three server gaps the D31 UI work found against the live server.

Each one is a field the `backtest.v1` result contract *requires* a worker to send
and that the server then threw away, so the fact reached PostgreSQL nowhere and
reached the UI nowhere:

1. `missingRequirements` on an UNAVAILABLE event.
   `schemas/backtest/v1/backtest-result.schema.json` lists it in the UNAVAILABLE
   branch's `required` with `minItems: 1`, next to `reasonCode`. The server
   persisted `reasonCode` as `failure_code` and dropped the list, so
   "REQUIRED_DATA_MISSING" arrived without the one thing an operator needs:
   *which* requirement was missing.
2. `resultManifestId` on COMPLETED and `retryable` on FAILED.
   Without `resultManifestId` a completed run cannot be linked to the manifest
   that holds its result at all; without `retryable` a failed run cannot say
   whether it is worth re-queuing.
3. `GET /api/v1/backtests` reported `attemptCount: 0` for every run while
   `GET /api/v1/backtests/{run_id}` reported the true count, because
   `list_runs` built `BacktestRun(run=row)` with the default `attempts=()`
   while `get` went through `_load`.

The assertions below are about state read back, not about return values, and
every expectation is a literal.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from backtest_engine.execution_policy import (
    D17_EXECUTION_POLICY_FIXTURE,
    ExecutionPolicyCatalog,
    et_quarter_start,
)
from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    InMemoryBacktestJobQueue,
    InMemoryRunGateway,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
)
from backtest_engine.persistence.rows import RunAttemptRow, RunStatus, WorkStatus
from test_lifecycle import (
    BOT_ID,
    DATASET_HASH,
    EXPECTED_RUN_ID,
    MANIFEST_ID,
    OWNER_ID,
    RESULT_HASH,
    SNAPSHOT_HASH,
    _load,
)


POLICY_2026Q3 = replace(
    D17_EXECUTION_POLICY_FIXTURE,
    version="official-backtest-policy-v2",
    release_quarter="2026-Q3",
    period_start=et_quarter_start(2026, 3),
    period_end=et_quarter_start(2026, 4),
)

RESULT_MANIFEST_ID = "99999999-9999-4999-8999-999999999999"
STARTED_AT = datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc)


@pytest.fixture
def service() -> BacktestLifecycleService:
    plan = _load("basic-compiled-plan.valid.json")
    return BacktestLifecycleService(
        gateway=InMemoryRunGateway(),
        queue=InMemoryBacktestJobQueue(),
        owners=StaticOwnerDirectory({BOT_ID: OWNER_ID}),
        plans=StaticCompiledPlanSource({plan["planChecksum"]: plan}),
        manifests=StaticDatasetManifestSource(
            {MANIFEST_ID: {"dataset_hash": DATASET_HASH, "schema_id": "market-bars-v2"}}
        ),
        policies=ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE, POLICY_2026Q3]),
    )


@pytest.fixture
def accepted(service: BacktestLifecycleService) -> BacktestLifecycleService:
    service.accept(_load("official-backtest-request.valid.json"))
    return service


def _event(service: BacktestLifecycleService, status: str, **detail: Any) -> dict[str, Any]:
    run = service.get(EXPECTED_RUN_ID, owner_account_id=OWNER_ID)
    return service.result_event_for(
        run,
        status=status,
        correlation_id="00000000-0000-4000-8000-000000000202",
        message_id=str(uuid4()),
        expected_snapshot_hash=SNAPSHOT_HASH,
        execution_policy_version="official-backtest-policy-v2",
        **detail,
    )


def _run_running(service: BacktestLifecycleService) -> None:
    service.ingest_result(
        _event(service, "RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    )


def _stored(service: BacktestLifecycleService) -> Any:
    """The run as a *reader* sees it, never the value ingestion returned."""
    return service.get(EXPECTED_RUN_ID, owner_account_id=OWNER_ID).run


# ===========================================================================
# Gap 1 - `missingRequirements` on UNAVAILABLE
# ===========================================================================


def test_an_unavailable_event_persists_the_requirements_it_names(
    accepted: BacktestLifecycleService,
) -> None:
    accepted.ingest_result(
        _event(
            accepted,
            "UNAVAILABLE",
            decidedAt="2026-07-31T12:05:02Z",
            reasonCode="REQUIRED_DATA_MISSING",
            missingRequirements=[
                "ADJUSTED_BAR/1m/00000000-0000-4000-8000-000000000301",
                "ADJUSTED_BAR/1m/00000000-0000-4000-8000-000000000302",
            ],
        )
    )

    stored = _stored(accepted)
    assert stored.status is RunStatus.UNAVAILABLE
    assert stored.failure_code == "REQUIRED_DATA_MISSING"
    assert stored.missing_requirements == (
        "ADJUSTED_BAR/1m/00000000-0000-4000-8000-000000000301",
        "ADJUSTED_BAR/1m/00000000-0000-4000-8000-000000000302",
    )


def test_the_stored_requirement_list_keeps_the_order_the_worker_sent(
    accepted: BacktestLifecycleService,
) -> None:
    """Not a set: the worker sorts them, and re-sorting here would hide a change."""
    accepted.ingest_result(
        _event(
            accepted,
            "UNAVAILABLE",
            decidedAt="2026-07-31T12:05:02Z",
            reasonCode="WARMUP_COVERAGE_MISSING",
            missingRequirements=["zeta", "alpha", "mu"],
        )
    )

    assert _stored(accepted).missing_requirements == ("zeta", "alpha", "mu")


def test_a_terminal_run_that_is_not_unavailable_stores_no_requirement_list(
    accepted: BacktestLifecycleService,
) -> None:
    """Absence must be `None`, never an empty list that reads as "nothing missing"."""
    _run_running(accepted)
    accepted.ingest_result(
        _event(
            accepted,
            "COMPLETED",
            completedAt="2026-07-31T12:10:00Z",
            attempt=1,
            resultManifestId=RESULT_MANIFEST_ID,
            resultHash=RESULT_HASH,
        )
    )

    assert _stored(accepted).missing_requirements is None


# ===========================================================================
# Gap 2 - `resultManifestId` on COMPLETED, `retryable` on FAILED
# ===========================================================================


def test_a_completed_event_persists_the_manifest_the_ui_has_to_link_to(
    accepted: BacktestLifecycleService,
) -> None:
    _run_running(accepted)
    accepted.ingest_result(
        _event(
            accepted,
            "COMPLETED",
            completedAt="2026-07-31T12:10:00Z",
            attempt=1,
            resultManifestId=RESULT_MANIFEST_ID,
            resultHash=RESULT_HASH,
        )
    )

    stored = _stored(accepted)
    assert stored.status is RunStatus.COMPLETED
    assert stored.result_hash == RESULT_HASH
    assert stored.result_manifest_id == UUID(RESULT_MANIFEST_ID)
    # A completed run is not a failed one: `retryable` must stay unset rather than
    # default to False, which would read as "we decided it cannot be retried".
    assert stored.retryable is None


@pytest.mark.parametrize("retryable", [True, False])
def test_a_failed_event_persists_whether_the_failure_is_worth_retrying(
    accepted: BacktestLifecycleService, retryable: bool
) -> None:
    _run_running(accepted)
    accepted.ingest_result(
        _event(
            accepted,
            "FAILED",
            failedAt="2026-07-31T12:07:00Z",
            attempt=2,
            failureCode="WORKER_TIMEOUT",
            retryable=retryable,
        )
    )

    stored = _stored(accepted)
    assert stored.status is RunStatus.FAILED
    assert stored.failure_code == "WORKER_TIMEOUT"
    assert stored.retryable is retryable
    assert stored.result_manifest_id is None


# ===========================================================================
# Gap 3 - the list endpoint's attempt count
# ===========================================================================


def _attempt(number: int) -> RunAttemptRow:
    return RunAttemptRow(
        id=uuid4(),
        run_id=EXPECTED_RUN_ID,
        attempt_number=number,
        worker_execution_key=f"BACKTEST_RUN:{EXPECTED_RUN_ID}:attempt-{number}",
        status=WorkStatus.SUCCEEDED,
        started_at=STARTED_AT + timedelta(minutes=number),
        completed_at=STARTED_AT + timedelta(minutes=number, seconds=30),
    )


def test_list_runs_reports_the_same_attempt_count_as_get(
    accepted: BacktestLifecycleService,
) -> None:
    """`GET /backtests` said 0 while `GET /backtests/{id}` said 2, for one run."""
    gateway = accepted.gateway
    assert isinstance(gateway, InMemoryRunGateway)
    gateway.record_attempt(_attempt(1))
    gateway.record_attempt(_attempt(2))

    listed = accepted.list_runs(OWNER_ID)
    fetched = accepted.get(EXPECTED_RUN_ID, owner_account_id=OWNER_ID)

    assert len(listed) == 1
    assert len(fetched.attempts) == 2
    assert len(listed[0].attempts) == 2
    assert [item.attempt_number for item in listed[0].attempts] == [1, 2]


def test_a_listed_run_with_no_attempts_still_reports_zero(
    accepted: BacktestLifecycleService,
) -> None:
    """The fix must not turn "no attempts yet" into an error or a phantom attempt."""
    listed = accepted.list_runs(OWNER_ID)

    assert len(listed) == 1
    assert listed[0].attempts == ()


def test_listing_many_runs_does_not_query_attempts_once_per_run(
    accepted: BacktestLifecycleService,
) -> None:
    """One page must cost one attempt query, not one per row.

    `list_runs` is the endpoint the dashboard polls. Populating attempts by
    calling the single-run reader in a loop would fix the wrong number and
    replace it with N+1 round trips, so the batch reader is the contract.
    """
    gateway = accepted.gateway
    assert isinstance(gateway, InMemoryRunGateway)
    gateway.record_attempt(_attempt(1))
    calls: list[tuple[Any, ...]] = []
    original = gateway.attempts_for_runs

    def counting(run_ids: Any) -> Any:
        calls.append(tuple(run_ids))
        return original(run_ids)

    gateway.attempts_for_runs = counting  # type: ignore[method-assign]

    listed = accepted.list_runs(OWNER_ID)

    assert len(listed[0].attempts) == 1
    assert len(calls) == 1
    assert calls[0] == (EXPECTED_RUN_ID,)


def test_an_empty_page_asks_for_no_attempts_at_all(
    service: BacktestLifecycleService,
) -> None:
    calls: list[Any] = []
    gateway = service.gateway
    assert isinstance(gateway, InMemoryRunGateway)
    original = gateway.attempts_for_runs

    def counting(run_ids: Any) -> Any:
        calls.append(tuple(run_ids))
        return original(run_ids)

    gateway.attempts_for_runs = counting  # type: ignore[method-assign]

    assert service.list_runs(OWNER_ID) == ()
    assert calls == []
