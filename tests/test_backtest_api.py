"""`/api/v1` behaviour (D28).

Replaces the deleted `test_api.py`, which asserted that a dictionary-backed stub
returned what the test had just put into it.

Coverage required by spec section 4: unauthenticated 401, wrong-owner 403, duplicate
request, retry, idempotency, at-least-once redelivery, DLQ, and 412 / lost-response
reconciliation.

The request and plan documents are loaded from B's own published copies under
`backend/modules/backend-messaging/.../strategy-bot/v1/` whenever the superproject is
reachable, so a stale vendored copy can never become the thing under test.
"""

from __future__ import annotations

import copy
import json
import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from backtest_engine.api import RESULT_INGEST_SCOPE, Principal, StaticTokenAuthenticator, create_app
from backtest_engine.contracts import build_backtest_result_event
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
from backtest_engine.persistence.rows import RunAttemptRow, WorkStatus


FIXTURES = Path(__file__).parent / "fixtures/contracts/strategy-bot/v1"

BOT_ID = UUID("00000000-0000-4000-8000-000000000201")
OWNER_ID = UUID("66666666-6666-4666-8666-666666666666")
OTHER_OWNER_ID = UUID("55555555-5555-4555-8555-555555555555")
MANIFEST_ID = UUID("00000000-0000-4000-8000-000000000203")

OWNER_TOKEN = "owner-token"
OTHER_TOKEN = "other-owner-token"
WORKER_TOKEN = "worker-token"

# B's `metadata.idempotencyKey` from the published fixture, and the run id `uuid5`
# derives from it. Pinned so a change to the derivation is a visible test failure and
# not a silently re-addressed run.
B_IDEMPOTENCY_KEY = "sha256:c6dd5229151352a530ff8312f050258107370cf26ea943c68473bf81936f6c1e"
EXPECTED_RUN_ID = "f876f259-4158-5a9a-8973-db21764024dc"

SNAPSHOT_HASH = "sha256:" + "1" * 64
RESULT_HASH = "sha256:" + "a" * 64
DATASET_HASH = "d9f6310297b7eb858570086d7292a709261eecc7bf92fc9a03745c46f514161c"


def _locate_backend_contracts() -> Path | None:
    override = os.environ.get("IDEA2STRATEGY_BACKEND_CONTRACTS")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_dir() else None
    suffix = Path(
        "backend/modules/backend-messaging/src/main/resources/contracts/strategy-bot/v1"
    )
    for ancestor in Path(__file__).resolve().parents:
        for candidate in (ancestor / suffix, ancestor / "Idea2Strategy" / suffix):
            if candidate.is_dir():
                return candidate
    return None


def _load(name: str) -> dict[str, Any]:
    """B's own copy is authoritative; the vendored copy is the offline fallback."""
    upstream = _locate_backend_contracts()
    if upstream is not None and (upstream / name).is_file():
        return json.loads((upstream / name).read_text(encoding="utf-8"))
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


class Harness:
    def __init__(self, service: BacktestLifecycleService, client: TestClient) -> None:
        self.service = service
        self.client = client
        self.gateway: InMemoryRunGateway = service.gateway  # type: ignore[assignment]
        self.queue: InMemoryBacktestJobQueue = service.queue  # type: ignore[assignment]
        self.dlq: InMemoryDeadLetterQueue = service.dead_letters  # type: ignore[assignment]

    def owner(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {OWNER_TOKEN}"}

    def other(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {OTHER_TOKEN}"}

    def worker(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {WORKER_TOKEN}"}


#: B's published request is dated 2026-07-31, so the policy the catalog must select is
#: the one published for that ET release quarter. The D17 fixture pins 2024-Q1, so the
#: harness publishes the 2026-Q3 policy the message actually calls for rather than
#: loosening the catalog's "no substitution" rule.
POLICY_2026Q3 = replace(
    D17_EXECUTION_POLICY_FIXTURE,
    version="official-backtest-policy-v2",
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
    with TestClient(create_app(service, authenticator)) as client:
        yield Harness(service, client)


def _accept(harness: Harness, request: dict[str, Any]) -> Any:
    return harness.client.post(
        "/api/v1/backtests", json={"request": request}, headers=harness.owner()
    )


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
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/monthly-summaries"),
        ("GET", f"/api/v1/backtests/{EXPECTED_RUN_ID}/detail-manifests"),
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
    assert str(run_id_for(B_IDEMPOTENCY_KEY)) == EXPECTED_RUN_ID
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


def test_performance_is_404_until_the_run_produces_one(
    harness: Harness, official_request: dict[str, Any]
) -> None:
    _accept(harness, official_request)

    response = harness.client.get(
        f"/api/v1/backtests/{EXPECTED_RUN_ID}/performance", headers=harness.owner()
    )

    assert response.status_code == 404


def test_unknown_run_is_404_for_every_query(harness: Harness) -> None:
    unknown = "11111111-1111-4111-8111-111111111111"

    for suffix in ("", "/attempts", "/performance", "/monthly-summaries", "/detail-manifests"):
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


def test_create_app_refuses_to_build_without_its_collaborators() -> None:
    """No silent in-memory fallback; the pre-rebuild app defaulted to a dictionary."""
    with pytest.raises(ValueError, match="Authenticator"):
        create_app(object(), None)  # type: ignore[arg-type]
