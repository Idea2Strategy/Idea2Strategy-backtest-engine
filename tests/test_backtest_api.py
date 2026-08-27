"""`/api/v1` behaviour (D28).

Replaces the deleted `test_api.py`, which asserted that a dictionary-backed stub
returned what the test had just put into it.

Coverage required by spec section 4: unauthenticated 401, wrong-owner 403, duplicate
request, retry, idempotency, at-least-once redelivery, DLQ, and 412 / lost-response
reconciliation.

The request and plan documents are frozen behaviour vectors. Cross-repository
parity with B's authoritative copies is covered separately by ``test_contracts``.
"""

from __future__ import annotations

import copy
import json
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from backtest_engine.api import RESULT_INGEST_SCOPE, Principal, StaticTokenAuthenticator, create_app
from backtest_engine.contracts import build_backtest_result_event
from backtest_engine.detail_object_manifest import (
    DetailObjectBuilder,
    DetailObjectBundle,
    DetailObjectKind,
    PerformancePoint,
)
from backtest_engine.execution_model import OrderStatus
from backtest_engine.execution_policy import (
    D17_EXECUTION_POLICY_FIXTURE,
    ExecutionPolicyCatalog,
    et_quarter_start,
)
from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    InMemoryBacktestJobQueue,
    InMemoryDeadLetterQueue,
    InMemoryRunGateway,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
    run_id_for,
)
from backtest_engine.monthly_judgment import MonthlyJudgmentBuilder, MonthlyJudgmentSummary
from backtest_engine.persistence.rows import RunAttemptRow, RunStatus, WorkStatus
from backtest_engine.result_query import (
    BacktestResultQueryService,
    InMemoryBacktestResultQueryStore,
    RunDatasetInput,
    RunInputs,
    RunProjection,
)
from backtest_engine.result_snapshot import (
    PositionAfter,
    ResultRecord,
    ResultRecordKind,
    ResultSnapshot,
    ResultSnapshotBuilder,
    RunSnapshot,
)


FIXTURES = Path(__file__).parent / "fixtures/contracts/strategy-bot/v1"

BOT_ID = UUID("00000000-0000-4000-8000-000000000201")
OWNER_ID = UUID("66666666-6666-4666-8666-666666666666")
OTHER_OWNER_ID = UUID("55555555-5555-4555-8555-555555555555")
MANIFEST_ID = UUID("00000000-0000-4000-8000-000000000203")

OWNER_TOKEN = "owner-token"
OTHER_TOKEN = "other-owner-token"
WORKER_TOKEN = "worker-token"

# B's published idempotency key and provider-registered run id. Both are pinned so
# the consumer cannot silently re-address an already-created run.
B_IDEMPOTENCY_KEY = "sha256:c6dd5229151352a530ff8312f050258107370cf26ea943c68473bf81936f6c1e"
EXPECTED_RUN_ID = "00000000-0000-4000-8000-000000000214"
DERIVED_RUN_ID = "f876f259-4158-5a9a-8973-db21764024dc"

SNAPSHOT_HASH = "sha256:" + "1" * 64
RESULT_HASH = "sha256:" + "a" * 64
DATASET_HASH = "d9f6310297b7eb858570086d7292a709261eecc7bf92fc9a03745c46f514161c"

# -- the D29 result read model ---------------------------------------------
#
# `result_query` is a separate projection from the `backtest.runs` write model, so it
# carries its own identities. The two ET months below are deliberately the two halves
# of a single ET Monday week: 2026-07-31 is a Friday, so the week starting Monday
# 2026-07-27 runs to Sunday 2026-08-02 and one Parquet part holds both months' rows.
# Both months fall inside the accepted run's 2026-07-01..2026-10-01 evaluation window.
QUERY_FINGERPRINT = "d" * 64
INSTRUMENT_ID = "00000000-0000-4000-8000-000000002908"
JULY_RECORD_ID = "00000000-0000-4000-8000-000000002910"
JULY_ORDER_ID = "00000000-0000-4000-8000-000000002909"
JULY_FILL_ID = "00000000-0000-4000-8000-000000002913"
AUGUST_RECORD_ID = "00000000-0000-4000-8000-000000002920"
AUGUST_ORDER_ID = "00000000-0000-4000-8000-000000002921"
AUGUST_FILL_ID = "00000000-0000-4000-8000-000000002922"


def _load(name: str) -> dict[str, Any]:
    """Load the frozen vector for deterministic API behaviour tests."""
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.fixture
def official_request() -> dict[str, Any]:
    return _load("official-backtest-request.valid.json")


@pytest.fixture
def compiled_plan() -> dict[str, Any]:
    return _load("basic-compiled-plan.valid.json")


@pytest.fixture
def manifest() -> dict[str, Any]:
    """Only the fields the acceptance path reads from a producer manifest."""
    return {"dataset_hash": DATASET_HASH, "schema_id": "market-bars-v2"}


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _query_inputs() -> RunInputs:
    return RunInputs(
        compiled_plan_checksum="sha256:" + "b" * 64,
        strategy_snapshot_hash=SNAPSHOT_HASH,
        input_bundle_fingerprint=QUERY_FINGERPRINT,
        input_contract_version="strategy-bot.v1",
        datasets=(RunDatasetInput(str(MANIFEST_ID), "MARKET_BARS", DATASET_HASH),),
        feature_materializations=(),
        execution_policy_version="official-backtest-policy-v2",
        precision_rules_version="precision:1.0.0",
    )


def _run_snapshot() -> RunSnapshot:
    return RunSnapshot(
        backtest_run_id=EXPECTED_RUN_ID,
        strategy_version_id=str(BOT_ID),
        input_bundle_fingerprint=QUERY_FINGERPRINT,
        calculation_model_version="calculation-v9",
        cost_model_version="cost-v3",
        execution_model_version="execution-v5",
        initial_cash=Decimal("100000"),
    )


def _projection(status: str, *, result_manifest_id: str | None = None) -> RunProjection:
    extra: dict[str, Any] = {"started_at": _instant("2026-07-31T12:05:00Z")}
    if status == "COMPLETED":
        extra["finished_at"] = _instant("2026-08-02T04:10:00Z")
        extra["result_manifest_id"] = result_manifest_id
    return RunProjection(
        run_id=EXPECTED_RUN_ID,
        bot_id=str(BOT_ID),
        owner_account_id=str(OWNER_ID),
        status=status,
        queued_at=_instant("2026-07-31T12:00:00Z"),
        inputs=_query_inputs(),
        version=2 if status != "COMPLETED" else 3,
        **extra,
    )


def _fill(
    *,
    record_id: str,
    order_id: str,
    fill_id: str,
    occurred_at: str,
    cash_after: str,
    quantity_after: str,
    cost_basis_after: str,
) -> ResultRecord:
    return ResultRecord(
        run_snapshot_id=_run_snapshot().snapshot_id,
        record_id=record_id,
        kind=ResultRecordKind.FILL,
        occurred_at=_instant(occurred_at),
        order_id=order_id,
        instrument_id=INSTRUMENT_ID,
        order_status=OrderStatus.FILLED,
        cash_after=Decimal(cash_after),
        positions_after=(
            PositionAfter(INSTRUMENT_ID, Decimal(quantity_after), Decimal(cost_basis_after)),
        ),
        fill_id=fill_id,
        quantity=Decimal("1"),
        base_price=Decimal("100"),
        price=Decimal("100.05"),
        gross_amount=Decimal("100.05"),
        slippage_amount=Decimal("0.05"),
        fee=Decimal("2.20"),
        cost_basis=Decimal("100.05"),
        realized_pnl=Decimal("0"),
    )


def _completed_projection() -> tuple[
    RunProjection, ResultSnapshot, DetailObjectBundle, tuple[MonthlyJudgmentSummary, ...]
]:
    """One COMPLETED run whose single ET-week detail part spans two ET months."""
    snapshot = _run_snapshot()
    built_at = _instant("2026-08-02T04:10:00Z")
    records = [
        # ET Friday 2026-07-31 23:30 — July.
        _fill(
            record_id=JULY_RECORD_ID,
            order_id=JULY_ORDER_ID,
            fill_id=JULY_FILL_ID,
            occurred_at="2026-08-01T03:30:00Z",
            cash_after="9897.80",
            quantity_after="1",
            cost_basis_after="100.05",
        ),
        # ET Saturday 2026-08-01 10:30 — August, same ET week.
        _fill(
            record_id=AUGUST_RECORD_ID,
            order_id=AUGUST_ORDER_ID,
            fill_id=AUGUST_FILL_ID,
            occurred_at="2026-08-01T14:30:00Z",
            cash_after="9795.55",
            quantity_after="2",
            cost_basis_after="200.15",
        ),
    ]
    result = ResultSnapshotBuilder().build(snapshot, records, built_at)
    details = DetailObjectBuilder().build(
        result,
        [],
        [
            PerformancePoint(
                point_id="00000000-0000-4000-8000-000000002930",
                run_snapshot_id=snapshot.snapshot_id,
                occurred_at=_instant("2026-07-31T20:00:00Z"),
                metric_id="equity",
                value=Decimal("100000.00000000"),
            ),
            PerformancePoint(
                point_id="00000000-0000-4000-8000-000000002931",
                run_snapshot_id=snapshot.snapshot_id,
                occurred_at=_instant("2026-08-01T20:00:00Z"),
                metric_id="equity",
                value=Decimal("100250.50000000"),
            ),
        ],
        built_at,
    )
    monthly = MonthlyJudgmentBuilder().build(
        snapshot.snapshot_id, result.manifest.result_manifest_id, [], result.records
    )
    run = _projection("COMPLETED", result_manifest_id=result.manifest.result_manifest_id)
    return run, result, details, monthly


class Harness:
    def __init__(
        self,
        service: BacktestLifecycleService,
        client: TestClient,
        results: InMemoryBacktestResultQueryStore,
    ) -> None:
        self.service = service
        self.client = client
        self.results = results
        self.gateway: InMemoryRunGateway = service.gateway  # type: ignore[assignment]
        self.queue: InMemoryBacktestJobQueue = service.queue  # type: ignore[assignment]
        self.dlq: InMemoryDeadLetterQueue = service.dead_letters  # type: ignore[assignment]

    def owner(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {OWNER_TOKEN}"}

    def other(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {OTHER_TOKEN}"}

    def worker(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {WORKER_TOKEN}"}

    def publish_completed(self) -> None:
        """Arrange the read model as a finished worker leaves it."""
        self.results.publish_completed(*_completed_projection())

    def project(self, status: str) -> None:
        self.results.upsert_run(_projection(status))

    def complete_run(self, request: dict[str, Any]) -> None:
        """Walk the *write* model's run to COMPLETED, as an ingested result does."""
        _accept(self, request)
        run_id = UUID(EXPECTED_RUN_ID)
        self.gateway.transition(
            run_id, RunStatus.RUNNING, started_at=datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc)
        )
        self.gateway.transition(
            run_id,
            RunStatus.COMPLETED,
            completed_at=datetime(2026, 7, 31, 12, 9, tzinfo=timezone.utc),
            result_hash="c" * 64,
            failure_code=None,
        )


#: B's published request is dated 2026-07-31, so the policy the catalog must select is
#: the one published for that ET release quarter. The D17 fixture pins 2024-Q1, so the
#: harness publishes the 2026-Q3 policy the message actually calls for rather than
#: loosening the catalog's "no substitution" rule.
POLICY_2026Q3 = replace(
    D17_EXECUTION_POLICY_FIXTURE,
    version="backtest-policy:1.0.0",
    release_quarter="2026-Q3",
    period_start=et_quarter_start(2026, 3),
    period_end=et_quarter_start(2026, 4),
)


@pytest.fixture
def harness(compiled_plan: dict[str, Any], manifest: dict[str, Any]) -> Iterator[Harness]:
    service = BacktestLifecycleService(
        gateway=InMemoryRunGateway(),
        queue=InMemoryBacktestJobQueue(),
        owners=StaticOwnerDirectory({BOT_ID: OWNER_ID}),
        plans=StaticCompiledPlanSource({compiled_plan["planChecksum"]: compiled_plan}),
        manifests=StaticDatasetManifestSource({MANIFEST_ID: manifest}),
        policies=ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE, POLICY_2026Q3]),
        dead_letters=InMemoryDeadLetterQueue(),
        max_delivery_attempts=3,
    )
    authenticator = StaticTokenAuthenticator(
        {
            OWNER_TOKEN: Principal(account_id=OWNER_ID),
            OTHER_TOKEN: Principal(account_id=OTHER_OWNER_ID),
            WORKER_TOKEN: Principal(account_id=OWNER_ID, scopes=frozenset({RESULT_INGEST_SCOPE})),
        }
    )
    results = InMemoryBacktestResultQueryStore()
    app = create_app(
        service,
        authenticator,
        BacktestResultQueryService(results),
        allow_test_provider_creation=True,
    )
    with TestClient(app) as client:
        yield Harness(service, client, results)


def _accept(harness: Harness, request: dict[str, Any]) -> Any:
    return harness.client.post(
        "/api/v1/backtests", json={"request": request}, headers=harness.owner()
    )


def test_production_api_refuses_to_create_provider_owned_runs(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    authenticator = StaticTokenAuthenticator(
        {OWNER_TOKEN: Principal(account_id=OWNER_ID)}
    )
    app = create_app(harness.service, authenticator)

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/backtests",
            json={"request": official_request},
            headers=harness.owner(),
        )

    assert response.status_code == 405
    assert "Backend provider transaction" in response.json()["detail"]


def _result(status: str, run_id: str = EXPECTED_RUN_ID, **detail: Any) -> dict[str, Any]:
    return build_backtest_result_event(
        status=status,
        backtest_run_id=run_id,
        bot_id=str(BOT_ID),
        owner_account_id=str(OWNER_ID),
        expected_snapshot_hash=SNAPSHOT_HASH,
        input_bundle_fingerprint="sha256:" + "e" * 64,
        execution_policy_version="official-backtest-policy-v1",
        message_id=str(uuid4()),
        occurred_at="2026-07-31T12:05:00Z",
        correlation_id="00000000-0000-4000-8000-000000000202",
        **detail,
    )


# ===========================================================================
# Authentication and authorisation
# ===========================================================================


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("GET", "/api/v1/backtests"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/attempts"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/performance"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/performance-series"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-summaries"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/detail-manifests"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07"),
        # No `et_month` at all: the credential is checked before the query string, so
        # an anonymous caller cannot probe the parameter contract.
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/inputs"),
        ("POST", "/api/v1/backtests"),
        ("POST", f"/api/v1/backtests/{EXPECTED_RUN_ID}/results"),
    ],
)
def test_every_endpoint_rejects_an_unauthenticated_caller(
    harness: Harness, method: str, path: str
) -> None:
    response = harness.client.request(method, path, json={})

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize(
    "header",
    [
        {"Authorization": "Bearer "},
        {"Authorization": "Basic abcdef"},
        {"Authorization": OWNER_TOKEN},
        {"Authorization": "Bearer not-a-known-token"},
    ],
)
def test_a_malformed_or_unknown_credential_is_401_not_403(
    harness: Harness, header: dict[str, str]
) -> None:
    response = harness.client.get("/api/v1/backtests", headers=header)

    assert response.status_code == 401


def test_wrong_owner_gets_403_and_the_run_is_not_disclosed(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.other()
    )

    assert response.status_code == 403
    assert "another account" in response.json()["detail"]
    # 403 is deliberate: a foreign run must not be reported as merely missing, or a
    # genuine authorisation bug is indistinguishable from a typo in the run id.
    unknown = harness.client.get(
        "/api/v1/backtests/11111111-1111-4111-8111-111111111111", headers=harness.other()
    )
    assert unknown.status_code == 404


def test_listing_is_scoped_to_the_authenticated_owner(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    mine = harness.client.get("/api/v1/backtests", headers=harness.owner()).json()
    theirs = harness.client.get("/api/v1/backtests", headers=harness.other()).json()

    assert [item["backtestRunId"] for item in mine["items"]] == [EXPECTED_RUN_ID]
    assert theirs["items"] == []


def test_owner_can_cancel_a_queued_run_immediately(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/cancellation",
        json={"reasonCode": "USER_CANCELLED"},
        headers=harness.owner(),
    )

    assert response.status_code == 202
    run = response.json()["run"]
    assert run["status"] == "CANCELLED"
    assert run["cancellationReasonCode"] == "USER_CANCELLED"
    assert run["cancellationRequestedAt"] is not None
    assert run["cancelledAt"] is not None


def test_running_cancellation_is_cooperative_and_owner_scoped(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)
    harness.gateway.transition(
        UUID(EXPECTED_RUN_ID),
        RunStatus.RUNNING,
        started_at=datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc),
    )

    forbidden = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/cancellation", headers=harness.other()
    )
    accepted = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/cancellation", headers=harness.owner()
    )

    assert forbidden.status_code == 403
    run = accepted.json()["run"]
    assert run["status"] == "RUNNING"
    assert run["cancellationReasonCode"] == "USER_CANCELLED"
    assert run["cancellationRequestedAt"] is not None
    assert run["cancelledAt"] is None


def test_owner_delete_is_idempotent_and_deleted_runs_disappear_from_customer_queries(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    first = harness.client.delete(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    )
    second = harness.client.delete(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert first.json()["run"]["status"] == "CANCELLED"
    assert first.json()["deletionRequested"] is True
    assert first.json()["deleted"] is True
    assert harness.client.get("/api/v1/backtests", headers=harness.owner()).json()["items"] == []
    assert harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    ).status_code == 404
    assert harness.gateway.get(UUID(EXPECTED_RUN_ID)).deleted_at is not None


def test_foreign_owner_cannot_delete_a_backtest(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    response = harness.client.delete(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.other()
    )

    assert response.status_code == 403
    assert harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    ).status_code == 200


def test_result_ingestion_requires_its_own_scope(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """Owning a run does not entitle the owner to forge its result."""
    _accept(harness, official_request)
    event = _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.owner()
    )

    assert response.status_code == 403
    assert RESULT_INGEST_SCOPE in response.json()["detail"]


# ===========================================================================
# Acceptance: duplicates, idempotency, dispatch
# ===========================================================================


def test_accepting_bs_request_creates_the_run_at_the_derived_id(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    response = _accept(harness, official_request)

    assert response.status_code == 202
    body = response.json()
    assert body["created"] is True
    assert body["dispatched"] is True
    assert body["run"]["backtestRunId"] == EXPECTED_RUN_ID
    assert body["run"]["status"] == "QUEUED"
    assert body["run"]["idempotencyKey"] == B_IDEMPOTENCY_KEY
    assert response.headers["Location"] == f"/api/v1/backtests/{EXPECTED_RUN_ID}"


def test_the_run_id_is_derived_from_bs_idempotency_key_not_invented(
    official_request: dict[str, Any],
) -> None:
    """Pinned literal: a random id would make redelivery create a second run."""
    assert str(run_id_for(B_IDEMPOTENCY_KEY)) == DERIVED_RUN_ID
    assert official_request["metadata"]["idempotencyKey"] == B_IDEMPOTENCY_KEY


def test_the_accepted_run_pins_the_canonical_policy_values(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    run = _accept(harness, official_request).json()["run"]

    assert run["status"] == "QUEUED"
    assert run["marketRulesVersion"] == "market:1.0.0"
    assert run["accountingRulesVersion"] == "accounting:1.0.0"
    assert run["precisionRulesVersion"] == "precision:1.0.0"
    assert run["slippageRateBps"] == 5
    assert run["initialCashAmount"] == "100000.00000000"
    assert run["evaluationStart"] == "2026-07-01"
    assert run["evaluationEnd"] == "2026-10-01"


def test_a_duplicate_request_returns_the_same_run_and_enqueues_once(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """At-least-once delivery of B's message must not produce two runs or two jobs."""
    first = _accept(harness, official_request)
    second = _accept(harness, official_request)

    assert first.status_code == second.status_code == 202
    assert first.json()["created"] is True
    assert second.json()["created"] is False
    assert second.json()["dispatched"] is False
    assert first.json()["run"] == second.json()["run"]
    assert len(harness.queue.messages) == 1


def test_a_reused_idempotency_key_with_different_content_is_409(
    harness: Harness, official_request: dict[str, Any], compiled_plan: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    # Same metadata (so the same key and the same run id) but a different bot.
    forged = copy.deepcopy(official_request)
    forged["botId"] = "00000000-0000-4000-8000-000000000209"
    harness.service.owners = StaticOwnerDirectory(
        {BOT_ID: OWNER_ID, UUID(forged["botId"]): OWNER_ID}
    )

    response = harness.client.post(
        "/api/v1/backtests", json={"request": forged}, headers=harness.owner()
    )

    # B's idempotency key covers botId, so a forged bot fails digest verification
    # before it can even reach the store. Either rejection is correct; a 2xx is not.
    assert response.status_code in (409, 422)
    assert len(harness.queue.messages) == 1


def test_a_request_with_a_tampered_digest_is_rejected(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    tampered = copy.deepcopy(official_request)
    tampered["expectedSnapshotHash"] = "sha256:" + "9" * 64

    response = _accept(harness, tampered)

    assert response.status_code == 422
    assert "idempotencyKey" in json.dumps(response.json())
    assert harness.queue.messages == []


def test_a_request_whose_inputs_cannot_be_resolved_names_what_is_missing(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """No hidden defaults: an unresolvable input is reported, not invented."""
    harness.service.manifests = StaticDatasetManifestSource({})

    response = _accept(harness, official_request)

    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["reasonCode"] == "REQUIRED_INPUT_UNAVAILABLE"
    assert detail["missingRequirements"] == [f"datasetManifest:{MANIFEST_ID}"]
    assert harness.queue.messages == []


def test_a_bot_this_account_does_not_own_is_403(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    harness.service.owners = StaticOwnerDirectory({BOT_ID: OTHER_OWNER_ID})

    response = _accept(harness, official_request)

    assert response.status_code == 403


# ===========================================================================
# Result ingestion: retry, redelivery, idempotency, 412, DLQ
# ===========================================================================


def _accept_and_start(harness: Harness, official_request: dict[str, Any]) -> str:
    _accept(harness, official_request)
    return EXPECTED_RUN_ID


def test_a_running_result_advances_the_run(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)
    event = _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
    )

    assert response.status_code == 200
    assert response.json()["applied"] is True
    assert response.json()["run"]["status"] == "RUNNING"


def test_at_least_once_redelivery_of_the_same_event_is_applied_once(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """The lost-response case: the worker never saw the first 200 and sends it again."""
    _accept_and_start(harness, official_request)
    event = _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)

    first = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
    )
    second = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=event,
        headers={**harness.worker(), "X-Delivery-Attempt": "2"},
    )

    assert first.status_code == second.status_code == 200
    assert first.json()["applied"] is True
    assert second.json()["applied"] is False
    assert first.json()["run"] == second.json()["run"]
    assert harness.dlq.messages == ()


def test_a_different_event_under_the_same_idempotency_key_is_409(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)
    event = _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
    )

    forged = copy.deepcopy(event)
    forged["startedAt"] = "2026-07-31T13:00:00Z"

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=forged, headers=harness.worker()
    )

    assert response.status_code == 409


def test_a_stale_if_match_is_412_and_returns_the_current_state(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """Lost-response reconciliation.

    The worker read the run while it was QUEUED, wrote RUNNING, lost the response, and
    now retries a *different* write with the stale token. It must be told the run
    moved on, and told what it moved to, rather than silently overwriting.
    """
    _accept_and_start(harness, official_request)
    stale_etag = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    ).headers["ETag"]
    assert stale_etag == '"QUEUED.0"'

    harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1),
        headers=harness.worker(),
    )
    harness.gateway.record_attempt(
        RunAttemptRow(
            id=uuid4(),
            run_id=UUID(EXPECTED_RUN_ID),
            attempt_number=1,
            worker_execution_key="worker-1",
            status=WorkStatus.RUNNING,
            started_at=datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc),
        )
    )

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result(
            "COMPLETED",
            completedAt="2026-07-31T12:30:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
        ),
        headers={**harness.worker(), "If-Match": stale_etag},
    )

    assert response.status_code == 412
    assert response.json()["current"]["status"] == "RUNNING"
    assert response.headers["ETag"] == '"RUNNING.1"'
    # The stale write did not land.
    assert (
        harness.client.get(
            f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
        ).json()["status"]
        == "RUNNING"
    )


def test_a_fresh_if_match_is_accepted(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)
    harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1),
        headers=harness.worker(),
    )
    current = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    ).headers["ETag"]

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result(
            "COMPLETED",
            completedAt="2026-07-31T12:30:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
        ),
        headers={**harness.worker(), "If-Match": current},
    )

    assert response.status_code == 200
    assert response.json()["run"]["status"] == "COMPLETED"
    assert response.json()["run"]["resultHash"] == RESULT_HASH


def test_a_result_that_would_reverse_a_terminal_run_is_409(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)
    for event in (
        _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1),
        _result(
            "COMPLETED",
            completedAt="2026-07-31T12:30:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
        ),
    ):
        harness.client.post(
            f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
        )

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result("RUNNING", startedAt="2026-07-31T14:00:00Z", attempt=2),
        headers=harness.worker(),
    )

    assert response.status_code == 409
    assert (
        harness.client.get(
            f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
        ).json()["status"]
        == "COMPLETED"
    )


def test_a_structurally_invalid_event_is_dead_lettered_not_retried(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """Poison message: no number of retries can make it valid, so it goes to the DLQ."""
    _accept_and_start(harness, official_request)
    poison = _result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1)
    poison["metadata"]["idempotencyKey"] = "sha256:" + "0" * 64

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=poison, headers=harness.worker()
    )

    assert response.status_code == 422
    assert len(harness.dlq.messages) == 1
    dead = harness.dlq.messages[0]
    assert dead.failure_kind == "CONTRACT_VIOLATION"
    assert dead.delivery_attempt == 1
    assert "idempotencyKey" in dead.reason
    assert (
        harness.client.get(
            f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
        ).json()["status"]
        == "QUEUED"
    )


def test_the_pre_rebuild_complete_token_is_rejected_at_the_http_boundary(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """Spec 2.2: the canonical terminal status is `COMPLETED`, never `COMPLETE`."""
    _accept_and_start(harness, official_request)
    event = _result(
        "COMPLETED",
        completedAt="2026-07-31T12:30:00Z",
        attempt=1,
        resultManifestId="99999999-9999-4999-8999-999999999999",
        resultHash=RESULT_HASH,
    )
    event["status"] = "COMPLETE"

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
    )

    assert response.status_code == 422


def test_an_event_addressed_to_a_different_run_is_rejected(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)
    event = _result(
        "RUNNING",
        run_id="11111111-1111-4111-8111-111111111111",
        startedAt="2026-07-31T12:05:00Z",
        attempt=1,
    )

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results", json=event, headers=harness.worker()
    )

    assert response.status_code == 422


def test_a_result_for_an_unknown_run_is_404(harness: Harness) -> None:
    unknown = "11111111-1111-4111-8111-111111111111"
    event = _result("RUNNING", run_id=unknown, startedAt="2026-07-31T12:05:00Z", attempt=1)

    response = harness.client.post(
        f"/api/v1/backtests/{unknown}/results", json=event, headers=harness.worker()
    )

    assert response.status_code == 404


def test_a_malformed_delivery_attempt_header_is_rejected(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept_and_start(harness, official_request)

    response = harness.client.post(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/results",
        json=_result("RUNNING", startedAt="2026-07-31T12:05:00Z", attempt=1),
        headers={**harness.worker(), "X-Delivery-Attempt": "zero"},
    )

    assert response.status_code == 400


# ===========================================================================
# Query endpoints
# ===========================================================================


def test_attempt_history_is_exposed(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)
    harness.gateway.record_attempt(
        RunAttemptRow(
            id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            run_id=UUID(EXPECTED_RUN_ID),
            attempt_number=1,
            worker_execution_key="worker-execution-1",
            status=WorkStatus.SUCCEEDED,
            started_at=datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc),
        )
    )

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/attempts", headers=harness.owner()
    )

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "attemptId": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "backtestRunId": EXPECTED_RUN_ID,
            "attemptNumber": 1,
            "workerExecutionKey": "worker-execution-1",
            "status": "SUCCEEDED",
            "startedAt": "2026-07-31T12:05:00Z",
            "completedAt": None,
            "failureCode": None,
        }
    ]


@pytest.mark.parametrize(
    "suffix",
    ["/performance", "/performance-series", "/monthly-summaries", "/detail-manifests"],
)
def test_evidence_routes_are_409_until_the_run_completes(
    harness: Harness, official_request: dict[str, Any], suffix: str
) -> None:
    """One status code for "the run is yours, it just has no result yet".

    `/monthly-trades` already answered 409 through the result read model while
    `/performance` answered 404 and the two list routes answered `200 {"items": []}`.
    Three answers to one question is three branches in the UI, and the empty list is
    indistinguishable from a finished run that traded nothing.
    """

    _accept(harness, official_request)

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}{suffix}", headers=harness.owner()
    )

    assert response.status_code == 409, response.text
    detail = response.json()["detail"]
    assert detail["reasonCode"] == "BACKTEST_RESULT_NOT_READY"
    assert detail["status"] == "QUEUED"


def test_a_completed_run_that_produced_no_detail_objects_is_an_empty_list_not_409(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """409 is about the run's status, never about the answer being empty."""

    harness.complete_run(official_request)

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/detail-manifests", headers=harness.owner()
    )

    assert response.status_code == 200
    assert response.json()["items"] == []


def test_unknown_run_is_404_for_every_query(harness: Harness) -> None:
    unknown = "11111111-1111-4111-8111-111111111111"

    for suffix in (
        "",
        "/attempts",
        "/performance",
        "/performance-series",
        "/monthly-summaries",
        "/detail-manifests",
        "/monthly-trades?et_month=2026-07",
        "/inputs",
    ):
        response = harness.client.get(
            f"/api/v1/backtests/{unknown}{suffix}", headers=harness.owner()
        )
        assert response.status_code == 404, suffix


@pytest.mark.parametrize(("limit", "offset"), [(0, 0), (201, 0), (50, -1)])
def test_pagination_bounds_are_enforced(harness: Harness, limit: int, offset: int) -> None:
    response = harness.client.get(
        f"/api/v1/backtests?limit={limit}&offset={offset}", headers=harness.owner()
    )

    assert response.status_code == 422


def test_the_listing_reports_the_same_attempt_count_as_the_run_detail(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    """`attemptCount` is one number and both query shapes must report it.

    `lifecycle.list_runs` builds each aggregate without its attempts, so the listing
    used to answer 0 for every run while `GET /{run_id}` answered the truth.
    """
    _accept(harness, official_request)
    harness.gateway.record_attempt(
        RunAttemptRow(
            id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
            run_id=UUID(EXPECTED_RUN_ID),
            attempt_number=1,
            worker_execution_key="worker-execution-1",
            status=WorkStatus.RUNNING,
            started_at=datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc),
        )
    )

    listed = harness.client.get("/api/v1/backtests", headers=harness.owner()).json()
    detail = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}", headers=harness.owner()
    ).json()

    assert listed["items"][0]["attemptCount"] == 1
    assert detail["attemptCount"] == 1


# ===========================================================================
# Monthly trade detail (D29 "월별 거래 상세")
# ===========================================================================


def test_monthly_trade_detail_is_served_for_the_owning_account(harness: Harness) -> None:
    """The whole row, pinned. This is the payload D31 binds the 거래 상세 screen to."""
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
        headers=harness.owner(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "backtestRunId": EXPECTED_RUN_ID,
        "etMonth": "2026-07",
        "items": [
            {
                "recordId": JULY_RECORD_ID,
                "occurredAt": "2026-08-01T03:30:00Z",
                "kind": "FILL",
                "orderId": JULY_ORDER_ID,
                "instrumentId": INSTRUMENT_ID,
                "orderStatus": "FILLED",
                "cashAfter": "9897.80000000",
                "positionsAfter": [
                    {
                        "instrumentId": INSTRUMENT_ID,
                        "quantity": "1.00000000",
                        "costBasis": "100.05000000",
                    }
                ],
                "reasonCode": None,
                "fillId": JULY_FILL_ID,
                "quantity": "1.00000000",
                "basePrice": "100.00000000",
                "price": "100.05000000",
                "grossAmount": "100.05000000",
                "slippageAmount": "0.05000000",
                "fee": "2.20000000",
                "costBasis": "100.05000000",
                "realizedPnl": "0.00000000",
            }
        ],
    }


def test_performance_series_is_served_from_official_equity_detail_rows(harness: Harness) -> None:
    """Catches generated curves and payloads disconnected from immutable Parquet evidence."""
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/performance-series",
        headers=harness.owner(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["backtestRunId"] == EXPECTED_RUN_ID
    assert body["points"] == [
        {"occurredAt": "2026-07-31T20:00:00Z", "equity": "100000.00000000"},
        {"occurredAt": "2026-08-01T20:00:00Z", "equity": "100250.50000000"},
    ]
    assert body["resultHash"]
    assert body["sourceSetHash"]


def test_one_et_week_part_is_split_across_the_two_et_months_it_spans(
    harness: Harness,
) -> None:
    """Detail Parquet is partitioned by ET Monday week; the API answer is ET month.

    Both fills live in the single week part starting 2026-07-27. A route that handed
    the week partition back as the month would answer both records for both months.
    """
    harness.publish_completed()

    july = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
        headers=harness.owner(),
    ).json()
    august = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-08",
        headers=harness.owner(),
    ).json()

    assert [item["recordId"] for item in july["items"]] == [JULY_RECORD_ID]
    assert [item["recordId"] for item in august["items"]] == [AUGUST_RECORD_ID]
    assert august["items"][0]["cashAfter"] == "9795.55000000"
    assert august["items"][0]["positionsAfter"] == [
        {
            "instrumentId": INSTRUMENT_ID,
            "quantity": "2.00000000",
            "costBasis": "200.15000000",
        }
    ]


def test_an_et_month_the_run_never_traded_in_is_an_empty_result_not_an_error(
    harness: Harness,
) -> None:
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-09",
        headers=harness.owner(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "backtestRunId": EXPECTED_RUN_ID,
        "etMonth": "2026-09",
        "items": [],
    }


def test_a_month_whose_evidence_went_missing_is_500_not_an_empty_month(
    harness: Harness,
) -> None:
    """Fail closed. "The objects are gone" and "you traded nothing" are not the same.

    Reporting the first as an empty list would let a lost Parquet part look like a
    quiet month on the 거래 상세 screen.
    """
    harness.publish_completed()
    entry = harness.results.get_owned(str(OWNER_ID), EXPECTED_RUN_ID)
    object.__setattr__(
        entry,
        "details",
        replace(
            entry.details,
            objects=tuple(
                item
                for item in entry.details.objects
                if item.descriptor.record_type is not DetailObjectKind.TRADE_DETAIL
            ),
        ),
    )

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
        headers=harness.owner(),
    )

    assert response.status_code == 500
    assert "do not match" in response.json()["detail"]


def test_monthly_trade_detail_for_a_foreign_owner_is_404_not_403(harness: Harness) -> None:
    """The read model answers not-found for a foreign run, and the route keeps that.

    Trade detail is the run's evidence, not its metadata: a 403 here would confirm
    that this run id exists and that somebody else finished it.
    """
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
        headers=harness.other(),
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "backtest not found"


@pytest.mark.parametrize("status", ["QUEUED", "RUNNING"])
def test_monthly_trade_detail_of_an_unfinished_run_is_409_not_a_partial_answer(
    harness: Harness, status: str
) -> None:
    harness.project(status)

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
        headers=harness.owner(),
    )

    assert response.status_code == 409
    assert response.json()["detail"] == {
        "message": f"backtest result is not available for status {status}",
        "reasonCode": "BACKTEST_RESULT_NOT_READY",
    }


@pytest.mark.parametrize(
    "et_month",
    ["2026-7", "2026-13", "2026-00", "0000-01", "202607", "2026-07-01", "July", ""],
)
def test_a_malformed_et_month_is_422_with_a_typed_reason(
    harness: Harness, et_month: str
) -> None:
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month={et_month}",
        headers=harness.owner(),
    )

    assert response.status_code == 422
    assert response.json()["detail"]["reasonCode"] == "ET_MONTH_MALFORMED"
    assert response.json()["detail"]["parameter"] == "et_month"


def test_et_month_is_required_rather_than_defaulted_to_a_month(harness: Harness) -> None:
    """No hidden default: the route never picks a month on the caller's behalf."""
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades", headers=harness.owner()
    )

    assert response.status_code == 422


def test_the_result_read_model_endpoints_are_503_when_no_read_model_is_configured(
    compiled_plan: dict[str, Any], manifest: dict[str, Any]
) -> None:
    """A deployment that supplied no read model says so, rather than answering 404.

    404 would be indistinguishable from "that run does not exist" and would leave the
    UI unable to tell a misconfigured deployment from an empty account.
    """
    service = BacktestLifecycleService(
        gateway=InMemoryRunGateway(),
        queue=InMemoryBacktestJobQueue(),
        owners=StaticOwnerDirectory({BOT_ID: OWNER_ID}),
        plans=StaticCompiledPlanSource({compiled_plan["planChecksum"]: compiled_plan}),
        manifests=StaticDatasetManifestSource({MANIFEST_ID: manifest}),
        policies=ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE, POLICY_2026Q3]),
    )
    authenticator = StaticTokenAuthenticator({OWNER_TOKEN: Principal(account_id=OWNER_ID)})
    headers = {"Authorization": f"Bearer {OWNER_TOKEN}"}

    with TestClient(create_app(service, authenticator)) as client:
        trades = client.get(
            f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-trades?et_month=2026-07",
            headers=headers,
        )
        inputs = client.get(f"/api/v1/backtests/{EXPECTED_RUN_ID}/inputs", headers=headers)

    assert trades.status_code == 503
    assert inputs.status_code == 503
    assert "BacktestResultQueryService" in trades.json()["detail"]


# ===========================================================================
# Inputs, models and the unavailable reason (D29 "입력 데이터·모델과 unavailable 이유")
# ===========================================================================


def test_inputs_and_models_expose_the_locked_reproducibility_identity(
    harness: Harness,
) -> None:
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/inputs", headers=harness.owner()
    )

    assert response.status_code == 200
    assert response.json() == {
        "backtestRunId": EXPECTED_RUN_ID,
        "botId": str(BOT_ID),
        "status": "COMPLETED",
        "strategySnapshotHash": SNAPSHOT_HASH,
        "compiledPlanChecksum": "sha256:" + "b" * 64,
        "datasetManifestId": str(MANIFEST_ID),
        "datasetHash": DATASET_HASH,
        "inputBundleFingerprint": QUERY_FINGERPRINT,
        "inputContractVersion": "strategy-bot.v1",
        "datasets": [
            {
                "datasetManifestId": str(MANIFEST_ID),
                "purposeCode": "MARKET_BARS",
                "lockedDatasetHash": DATASET_HASH,
            }
        ],
        "featureMaterializations": [],
        "executionPolicyVersion": "official-backtest-policy-v2",
        "precisionRulesVersion": "precision:1.0.0",
        "calculationModelVersion": "calculation-v9",
        "costModelVersion": "cost-v3",
        "executionModelVersion": "execution-v5",
        "reasonCode": None,
        "missingRequirements": [],
    }


def test_an_unavailable_run_names_what_was_missing(harness: Harness) -> None:
    """`UNAVAILABLE` is the one status whose whole content is the reason it has none.

    The model versions are null because no model ever ran, and that is reported as
    null rather than as a plausible-looking version string.
    """
    harness.results.upsert_run(
        RunProjection(
            run_id=EXPECTED_RUN_ID,
            bot_id=str(BOT_ID),
            owner_account_id=str(OWNER_ID),
            status="UNAVAILABLE",
            queued_at=_instant("2026-07-31T12:00:00Z"),
            inputs=_query_inputs(),
            finished_at=_instant("2026-07-31T12:01:00Z"),
            reason_code="REQUIRED_DATA_MISSING",
            missing_requirements=("resolution:1m", "symbol:XYZ"),
        )
    )

    body = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/inputs", headers=harness.owner()
    ).json()

    assert body["status"] == "UNAVAILABLE"
    assert body["reasonCode"] == "REQUIRED_DATA_MISSING"
    assert body["missingRequirements"] == ["resolution:1m", "symbol:XYZ"]
    assert body["calculationModelVersion"] is None
    assert body["costModelVersion"] is None
    assert body["executionModelVersion"] is None
    assert body["inputBundleFingerprint"] == QUERY_FINGERPRINT


def test_inputs_for_a_foreign_owner_are_404_not_403(harness: Harness) -> None:
    harness.publish_completed()

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/inputs", headers=harness.other()
    )

    assert response.status_code == 404


def test_create_app_refuses_to_build_without_its_collaborators() -> None:
    """No silent in-memory fallback; the pre-rebuild app defaulted to a dictionary."""
    with pytest.raises(ValueError, match="Authenticator"):
        create_app(object(), None)  # type: ignore[arg-type]
