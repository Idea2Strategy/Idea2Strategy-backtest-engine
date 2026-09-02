"""D30/D31: one official backtest, all the way through, twice.

What the deleted version of this file did
-----------------------------------------
It called a 405-line testkit that assembled the domain modules in-process,
finished in 0.04 seconds, and touched no database, no object store, no queue and
no HTTP. It asserted ``first == second``, which a constant-returning
implementation satisfies.

What this one does
------------------
Every leg is real, and each is *observed* rather than assumed:

``HTTP``
    B's ``OFFICIAL_BACKTEST_REQUESTED`` is posted to the FastAPI app through
    ``fastapi.testclient.TestClient``. The worker posts its ``backtest.v1``
    events back to ``POST /api/v1/backtests/{id}/results``, and the results are
    read out again through all eight owner-scoped query endpoints - including
    ``monthly-trades``, which closes the loop: the app is built with a real
    ``BacktestResultQueryService`` over the same PostgreSQL and the same S3
    bucket the worker published into, so the traversal asserts that the trade
    row in the Parquet object is readable back over HTTP rather than only that
    it was written. A server-side ASGI recorder captures the requests the app
    actually received, so the ``X-Delivery-Attempt`` assertions are about what
    arrived, not what was sent.

``SQS``
    A real LocalStack queue. The test asserts the queue depth goes 0 -> 1 on
    acceptance and 1 -> 0 once the real ``BacktestWorker`` long-polls it, and the
    dead-letter queue receives the poison job.

``PostgreSQL``
    The session-scoped Testcontainers PostgreSQL 16 from ``conftest``, migrated
    with the canonical central Flyway bundle. Rows are read back with plain SQL
    on a separate unguarded engine -- never through the repository that wrote
    them.

``object store``
    A real LocalStack S3 bucket behind ``S3ObjectStore``. Every published object
    is fetched back out of the bucket with a raw boto3 ``get_object``, re-hashed,
    and parsed as Parquet.

Reproducibility
---------------
``test_the_same_official_request_reproduces_every_digest`` runs the whole thing
twice and pins ``run_snapshot_id``, ``result_hash``, ``input_bundle_fingerprint``
and the detail objects' content hashes as **literals**.
``test_moving_one_input_bar_moves_every_digest`` changes a single close price and
asserts each of those literals moves, so a constant-returning implementation
fails one of the two tests whichever value it returns.

The literals are anchored to arithmetic that is verified independently of them:
``tests/test_wiring.py`` pins the sized quantity (992 shares), the slipped fill
price (100.05), the fee (198.4992) and the ending cash (551.9008) by hand from
the fixture's inputs, and this file asserts the same numbers appear in the row
PostgreSQL and the object store hold.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import threading
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from sqlalchemy import Engine, text

from backtest_engine.attempt_coordinator import ResourceSample
from backtest_engine.lifecycle import StaticDatasetManifestSource
from backtest_engine.persistence import (
    BacktestPersistence,
    StaleAttemptClaim,
    WorkStatus,
)
from backtest_engine.recovery import QueueDispatchPolicy, StaleRunRecovery
from backtest_engine.wiring import DurableResultPublisher, PersistenceExecutionKeyStore
from backtest_engine.worker import (
    BacktestWorker,
    ExecutionRecordStatus,
    MessageDisposition,
    WorkerConfig,
    worker_execution_key_for,
)

# `build_stack` builds the app *with* a real `BacktestResultQueryService` (see
# `d_integration_stack`), which is what lets the traversal below read the published
# run back over HTTP rather than only assert that the worker wrote it.
from d_integration_stack import (
    ScriptedMonitor,
    Stack,
    build_stack,
    fetch_object,
    sql_all,
    sql_one,
    truncate_backtest,
)
from d_reproducibility_testkit import (
    ACCOUNT_ID,
    BOT_ID,
    CLOSES,
    DATASET_MANIFEST_ID,
    E2E_EXECUTION_POLICY,
    INSTRUMENT_ID,
)
from d_task5_chaos import (
    evidence_result,
    record_evidence,
    task5_request,
    task5_run_id,
    wait_until,
)
from persistence.support import make_run


pytestmark = pytest.mark.docker


# ---------------------------------------------------------------------------
# Pinned digests
# ---------------------------------------------------------------------------

#: `uuid5` of B's own `metadata.idempotencyKey`. Two deliveries of the same
#: request address this run without a database round trip.
EXPECTED_RUN_ID = "76a6a20c-0651-5748-8187-6bf0ae155194"

#: `backtest.runs.configuration_hash`, published as `inputBundleFingerprint`.
EXPECTED_INPUT_BUNDLE_FINGERPRINT = "sha256:8b73c1ad86cf42c2360989ecb14b225a95e191eccd9c8a7e3f6ead8ef84add25"

#: `RunSnapshot.snapshot_id`: the pinned run inputs.
EXPECTED_RUN_SNAPSHOT_ID = "75fd83d0c9cd6356a9c0ed1db9833881f19a0a136042bf96d82617085ba64348"

#: `backtest.performance_summaries.result_hash`.
EXPECTED_RESULT_HASH = "d32135e1775553edc037d17fea01afc1e04f30844653741b77b235cf3677470b"

#: `storage.objects.content_hash` of the TRADE_DETAIL Parquet part.
EXPECTED_TRADE_DETAIL_CONTENT_HASH = "054779480a7c8f4cd4a1d16cebd67455fe141529ac242f57cec81039031d9188"


# ---------------------------------------------------------------------------
# The stack under test
# ---------------------------------------------------------------------------


@pytest.fixture
def stack(
    persistence: BacktestPersistence,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> Stack:
    return build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "market-data",
    )


def run_once(stack: Stack) -> str:
    """HTTP -> SQS -> worker -> PostgreSQL -> S3, once. Returns the run id."""
    accepted = stack.accept()
    assert accepted.status_code == 202, accepted.text
    run_id = accepted.json()["run"]["backtestRunId"]
    assert stack.visible(stack.main_queue) == 1, "the job never reached SQS"

    handled = stack.worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED], handled
    return str(run_id)


def _accept_task5(stack: Stack, scenario: str) -> tuple[dict[str, Any], str]:
    request = task5_request(stack.request, scenario)
    accepted = stack.accept(request=request)
    assert accepted.status_code == 202, accepted.text
    assert accepted.json()["created"] is True
    run_id = str(accepted.json()["run"]["backtestRunId"])
    assert run_id == request["runId"]
    return request, run_id


def _start_live_sentinel(
    persistence: BacktestPersistence,
    scenario: str,
) -> tuple[str, PersistenceExecutionKeyStore, Any]:
    run_id = task5_run_id(f"live-sentinel:{scenario}")
    with persistence.unit_of_work() as uow:
        run, created = uow.runs.accept(
            make_run(
                id=uuid.UUID(run_id),
                idempotency_key=f"TASK5:LIVE-SENTINEL:{scenario.upper()}",
            )
        )
    assert created
    key = worker_execution_key_for(str(run.id), run.idempotency_key)
    store = PersistenceExecutionKeyStore(persistence)
    claim = store.claim(
        key,
        run_id=str(run.id),
        owner=f"task5-live-{scenario}",
        now=datetime.now(timezone.utc),
        lease_duration=timedelta(minutes=5),
    )
    assert claim.acquired
    return key, store, claim


def _assert_recovery_invariants(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    *,
    sentinel_run_id: str,
) -> Any:
    report = StaleRunRecovery(
        persistence,
        max_attempts=3,
        queue_policy=QueueDispatchPolicy.from_environment({}),
    ).recover_once()
    with admin_engine.connect() as connection:
        live = (
            connection.execute(
                text(
                    "SELECT r.status AS run_status,a.status AS attempt_status,"
                    "a.claim_expires_at > clock_timestamp() AS lease_live "
                    "FROM backtest.runs r JOIN backtest.run_attempts a ON a.run_id=r.id "
                    "WHERE r.id=:id ORDER BY a.attempt_number DESC LIMIT 1"
                ),
                {"id": uuid.UUID(sentinel_run_id)},
            )
            .mappings()
            .one()
        )
        invalid = connection.scalar(
            text(
                "SELECT count(*) FROM backtest.runs r WHERE "
                "(r.status='RUNNING' AND NOT EXISTS ("
                " SELECT 1 FROM backtest.run_attempts a WHERE a.run_id=r.id "
                " AND a.status='RUNNING' AND a.claim_expires_at > clock_timestamp())) "
                "OR (r.status='QUEUED' AND EXISTS ("
                " SELECT 1 FROM backtest.run_attempts a WHERE a.run_id=r.id "
                " AND a.status='RUNNING'))"
            )
        )
    assert dict(live) == {
        "run_status": "RUNNING",
        "attempt_status": "RUNNING",
        "lease_live": True,
    }
    assert invalid == 0
    return report


def _finish_live_sentinel(
    persistence: BacktestPersistence,
    sentinel_run_id: str,
    key: str,
    store: PersistenceExecutionKeyStore,
    claim: Any,
) -> None:
    with persistence.unit_of_work() as uow:
        uow.runs.request_cancellation(
            uuid.UUID(sentinel_run_id),
            reason_code="USER_CANCELLED",
        )
    store.finish(
        key,
        ExecutionRecordStatus.CANCELLED,
        now=datetime.now(timezone.utc),
        claim=claim,
        reason_code="USER_CANCELLED",
        run_id=sentinel_run_id,
    )


def _attempt_evidence(admin_engine: Engine, run_id: str) -> tuple[str, ...]:
    rows = sql_all(
        admin_engine,
        "SELECT attempt_number,status,COALESCE(terminal_reason_code,'NONE') AS reason "
        "FROM backtest.run_attempts WHERE run_id=:id ORDER BY attempt_number",
        id=run_id,
    )
    return tuple(f"{row['attempt_number']}:{row['status']}:{row['reason']}" for row in rows) or ("no-attempt",)


# ===========================================================================
# The end-to-end traversal
# ===========================================================================


def test_an_official_request_traverses_http_sqs_worker_postgres_and_the_object_store(
    stack: Stack, admin_engine: Engine, s3: Any
) -> None:
    """Each leg is asserted against the leg's own storage, not against a return value."""
    accepted = stack.accept()

    # -- HTTP in -----------------------------------------------------------
    assert accepted.status_code == 202, accepted.text
    body = accepted.json()
    run_id = body["run"]["backtestRunId"]
    assert run_id == EXPECTED_RUN_ID
    assert body["created"] is True
    assert body["dispatched"] is True

    # -- the run row exists in PostgreSQL before any worker ran ------------
    queued = sql_one(admin_engine, "SELECT status, configuration_hash FROM backtest.runs WHERE id = :id", id=run_id)
    assert queued["status"] == "QUEUED"
    assert queued["configuration_hash"] == EXPECTED_INPUT_BUNDLE_FINGERPRINT

    # -- SQS ----------------------------------------------------------------
    assert stack.visible(stack.main_queue) == 1
    handled = stack.worker.poll_once()
    assert [item.disposition for item in handled] == [MessageDisposition.DELETED], [
        item.reason_code for item in handled
    ]
    assert stack.visible(stack.main_queue) == 0
    assert stack.visible(stack.dead_letter_queue) == 0

    # -- PostgreSQL: the nine canonical tables the run touches --------------
    run_row = sql_one(
        admin_engine,
        "SELECT status, result_hash, completed_at, started_at, initial_cash_amount FROM backtest.runs WHERE id = :id",
        id=run_id,
    )
    assert run_row["status"] == "COMPLETED"
    assert run_row["result_hash"] == EXPECTED_RESULT_HASH
    assert run_row["started_at"] is not None
    assert run_row["initial_cash_amount"] == Decimal("100000.00000000")

    attempts = sql_all(
        admin_engine,
        "SELECT attempt_number, status, worker_execution_key FROM backtest.run_attempts "
        "WHERE run_id = :id ORDER BY attempt_number",
        id=run_id,
    )
    assert [item["status"] for item in attempts] == ["SUCCEEDED"]
    assert attempts[0]["worker_execution_key"].startswith(f"BACKTEST_RUN:{run_id}:")

    performance = sql_one(
        admin_engine,
        "SELECT metrics_document, result_hash, source_set_hash, input_hash "
        "FROM backtest.performance_summaries WHERE run_id = :id",
        id=run_id,
    )
    assert performance["result_hash"] == EXPECTED_RESULT_HASH
    metrics = performance["metrics_document"]
    assert set(metrics) >= {"totalReturnPct", "maxDrawdownPct", "sharpe", "winRatePct"}

    monthly = sql_all(
        admin_engine,
        "SELECT et_year_month, evaluation_count, active_branch_count, trade_event_count, "
        "data_gap_count, triggered_count, rejected_count, summary_hash "
        "FROM backtest.monthly_judgment_summaries WHERE run_id = :id ORDER BY et_year_month",
        id=run_id,
    )
    assert [item["et_year_month"] for item in monthly] == ["2024-01"]
    # RSI_14 needs 15 prior closes, so only bars 15..19 are executable
    # evaluation instants; exactly one of those five instants decided.
    assert monthly[0]["evaluation_count"] == len(CLOSES) - 15
    assert monthly[0]["triggered_count"] == 1
    assert monthly[0]["data_gap_count"] == 0
    assert monthly[0]["trade_event_count"] == 2  # the accepted order and its fill
    assert monthly[0]["rejected_count"] == 0

    bundle = sql_one(admin_engine, "SELECT bundle_hash FROM backtest.input_bundles WHERE run_id = :id", id=run_id)
    assert bundle["bundle_hash"] == EXPECTED_INPUT_BUNDLE_FINGERPRINT
    datasets = sql_all(
        admin_engine,
        "SELECT d.dataset_manifest_id, d.purpose_code FROM backtest.input_datasets d "
        "JOIN backtest.input_bundles b ON b.id = d.input_bundle_id WHERE b.run_id = :id",
        id=run_id,
    )
    assert [str(item["dataset_manifest_id"]) for item in datasets] == [str(DATASET_MANIFEST_ID)]

    manifests = sql_all(
        admin_engine,
        "SELECT record_type, week_start_date, part_number, row_count, object_id, schema_version "
        "FROM backtest.detail_manifests WHERE run_id = :id ORDER BY record_type, week_start_date",
        id=run_id,
    )
    assert manifests, "the run published no detail objects"
    assert {item["record_type"] for item in manifests} >= {
        "TRADE_DETAIL",
        "POSITION_SNAPSHOT",
        "REPLAY_LEDGER",
        "CALCULATION_SERIES",
    }
    # Spec 2.2: ET **Monday** week boundaries, never a month.
    assert all(item["week_start_date"].weekday() == 0 for item in manifests)
    assert all(item["part_number"] >= 1 for item in manifests)

    # -- the object store: read every registered object back out of S3 ------
    objects = sql_all(
        admin_engine,
        "SELECT id, object_key, content_hash, byte_size, status, compression_codec, file_format "
        "FROM storage.objects WHERE id = ANY(:ids)",
        ids=[item["object_id"] for item in manifests],
    )
    assert len(objects) == len(manifests)
    for stored in objects:
        assert stored["status"] == "AVAILABLE"
        assert stored["compression_codec"] == "UNCOMPRESSED"
        assert stored["file_format"] == "PARQUET"
        fetched = fetch_object(s3, stack.bucket, stored["object_key"])
        assert hashlib.sha256(fetched).hexdigest() == stored["content_hash"]
        assert len(fetched) == stored["byte_size"]
        # Not just bytes: readable Parquet with the canonical codec.
        table = pq.read_table(io.BytesIO(fetched))
        assert table.num_rows > 0

    # -- spec 2.5: the immutable result object is registered and stored too --
    result_objects = sql_all(
        admin_engine,
        "SELECT object_key, content_hash, status, file_format, row_count "
        "FROM storage.objects WHERE object_key LIKE :prefix",
        prefix=f"backtest-results/{run_id}/%.json",
    )
    assert len(result_objects) == 1
    result_object = result_objects[0]
    assert result_object["status"] == "AVAILABLE"
    assert result_object["file_format"] == "JSON"
    result_bytes = fetch_object(s3, stack.bucket, result_object["object_key"])
    assert hashlib.sha256(result_bytes).hexdigest() == result_object["content_hash"]
    snapshot = json.loads(result_bytes)
    assert snapshot["run_snapshot"]["backtest_run_id"] == run_id
    assert len(snapshot["records"]) == result_object["row_count"] == 2

    # -- the trade detail really carries the fill the engine computed -------
    trade_key = _trade_detail_key(objects, manifests)
    trades_rows = pq.read_table(io.BytesIO(fetch_object(s3, stack.bucket, trade_key))).to_pylist()
    fill = next(row for row in trades_rows if row["kind"] == "FILL")
    assert fill["instrument_id"] == INSTRUMENT_ID
    assert fill["quantity"] == "992.00000000"
    assert fill["price"] == "100.05000000"
    assert fill["fee"] == "198.49920000"
    assert fill["cash_after"] == "551.90080000"

    # -- HTTP out: the five owner-scoped query endpoints --------------------
    assert stack.client.get(f"/api/v1/backtests/{run_id}", headers=stack.owner()).json()["status"] == "COMPLETED"
    assert [
        item["backtestRunId"] for item in stack.client.get("/api/v1/backtests", headers=stack.owner()).json()["items"]
    ] == [run_id]
    api_attempts = stack.client.get(f"/api/v1/backtests/{run_id}/attempts", headers=stack.owner()).json()["items"]
    assert [item["status"] for item in api_attempts] == ["SUCCEEDED"]
    api_performance = stack.client.get(f"/api/v1/backtests/{run_id}/performance", headers=stack.owner()).json()
    assert api_performance["resultHash"] == EXPECTED_RESULT_HASH
    api_monthly = stack.client.get(f"/api/v1/backtests/{run_id}/monthly-summaries", headers=stack.owner()).json()[
        "items"
    ]
    assert [item["etYearMonth"] for item in api_monthly] == ["2024-01"]
    api_manifests = stack.client.get(f"/api/v1/backtests/{run_id}/detail-manifests", headers=stack.owner()).json()[
        "items"
    ]
    assert len(api_manifests) == len(manifests)

    # -- D29's whole point: the published trade detail, read back over HTTP --
    #
    # Everything above proves the worker wrote the evidence. This proves a client can
    # get it: the read model finds the run in `backtest.runs`, its ET-week parts in
    # `backtest.detail_manifests`, their bytes in `storage.objects` and S3, re-hashes
    # them, re-verifies the Parquet footers, and places each row in its own ET month.
    # The row asserted here is the *same* row `fill` above was read from with raw
    # boto3, to the last of its eight decimal places.
    trades = stack.client.get(
        f"/api/v1/backtests/{run_id}/monthly-trades?et_month=2024-01",
        headers=stack.owner(),
    )
    assert trades.status_code == 200, trades.text
    body = trades.json()
    assert body["etMonth"] == "2024-01"
    over_http = next(item for item in body["items"] if item["kind"] == "FILL")
    assert over_http["recordId"] == fill["record_id"]
    assert over_http["instrumentId"] == INSTRUMENT_ID
    assert over_http["quantity"] == "992.00000000"
    assert over_http["price"] == "100.05000000"
    assert over_http["fee"] == "198.49920000"
    assert over_http["cashAfter"] == "551.90080000"
    assert over_http["orderStatus"] == "FILLED"
    # `trade_event_count` for the month is 2 (the accepted order and its fill), and the
    # read model cross-checks the Parquet rows against exactly that summary.
    assert [item["kind"] for item in body["items"]] == ["ORDER", "FILL"]
    assert {item["recordId"] for item in body["items"]} == {str(row["record_id"]) for row in trades_rows}
    # A month the run never traded in is empty, not a 404 and not an error.
    assert (
        stack.client.get(
            f"/api/v1/backtests/{run_id}/monthly-trades?et_month=2024-02",
            headers=stack.owner(),
        ).json()["items"]
        == []
    )

    # -- and the pinned inputs the acceptance transaction recorded ----------
    api_inputs = stack.client.get(f"/api/v1/backtests/{run_id}/inputs", headers=stack.owner())
    assert api_inputs.status_code == 200, api_inputs.text
    reported = api_inputs.json()
    assert reported["status"] == "COMPLETED"
    assert reported["inputBundleFingerprint"] == EXPECTED_INPUT_BUNDLE_FINGERPRINT
    assert reported["compiledPlanChecksum"] == stack.request["compiledPlanChecksum"]
    assert reported["strategySnapshotHash"] == stack.request["expectedSnapshotHash"]
    assert reported["datasetManifestId"] == str(DATASET_MANIFEST_ID)
    assert reported["executionPolicyVersion"] == E2E_EXECUTION_POLICY.version
    assert reported["precisionRulesVersion"] == "precision:1.0.0"
    # Model versions come from the stored result object, not from the request.
    assert reported["costModelVersion"] == "backtest-cost:1.0.0"
    assert reported["executionModelVersion"] == "backtest-execution:1.0.0"

    # -- the worker's own events really went over HTTP ----------------------
    posted = stack.recorder.result_posts()
    assert [headers["x-delivery-attempt"] for headers in posted] == ["1", "1"]
    assert [response.status_code for response in stack.sink.responses] == [200, 200]


def _trade_detail_key(objects: list[Any], manifests: list[Any]) -> str:
    by_id = {str(stored["id"]): stored["object_key"] for stored in objects}
    for manifest in manifests:
        if manifest["record_type"] == "TRADE_DETAIL":
            return str(by_id[str(manifest["object_id"])])
    raise AssertionError("the run published no TRADE_DETAIL object")


# ===========================================================================
# Reproducibility
# ===========================================================================


def test_the_same_official_request_reproduces_every_digest(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> None:
    """The same request, run twice against a clean database, agrees digit for digit.

    A completed run is immutable, so the second execution needs an empty
    ``backtest`` schema -- which is precisely the reproducibility question: does
    the same pinned input produce the same official result from nothing? The
    object store is *not* cleared between the two, so the second run re-publishes
    content-addressed objects that already exist and must reconcile with them
    rather than conflict.
    """
    first_stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "first",
    )
    run_id = run_once(first_stack)
    first = _digests(admin_engine, run_id)
    first_objects = _object_hashes(admin_engine, run_id)

    truncate_backtest(admin_engine)

    second_stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "second",
    )
    assert run_once(second_stack) == run_id
    second = _digests(admin_engine, run_id)
    second_objects = _object_hashes(admin_engine, run_id)

    # Pinned literals first: `first == second` alone would pass for a constant.
    assert first["run_id"] == EXPECTED_RUN_ID
    assert first["input_bundle_fingerprint"] == EXPECTED_INPUT_BUNDLE_FINGERPRINT
    assert first["run_snapshot_id"] == EXPECTED_RUN_SNAPSHOT_ID
    assert first["result_hash"] == EXPECTED_RESULT_HASH
    assert first_objects["TRADE_DETAIL"] == EXPECTED_TRADE_DETAIL_CONTENT_HASH

    assert first == second
    assert first_objects == second_objects
    # The bytes really are in the bucket, not merely recorded as equal hashes.
    stored = _stored_parts(admin_engine, run_id)
    assert len(stored) >= 4
    for key, content_hash in stored:
        assert hashlib.sha256(fetch_object(s3, bucket, key)).hexdigest() == content_hash


def test_moving_one_input_bar_moves_every_digest(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> None:
    """Change one close price; every pinned digest must move.

    This is what makes the literals above evidence rather than decoration: a
    constant-returning implementation passes the equality test and fails this
    one. The mutated bar is the last one, which is *after* the only decision
    instant, so the plan still emits the same single candidate and the same fill
    -- only the valuation grid and therefore the result change.
    """
    mutated = (*CLOSES[:-1], "200")
    assert mutated != CLOSES

    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "mutated",
        closes=mutated,
    )
    run_id = run_once(stack)
    moved = _digests(admin_engine, run_id)
    moved_objects = _object_hashes(admin_engine, run_id)

    # The run is still the same run: identity comes from B's idempotency key.
    assert moved["run_id"] == EXPECTED_RUN_ID
    # Everything the dataset feeds must move.
    assert moved["input_bundle_fingerprint"] != EXPECTED_INPUT_BUNDLE_FINGERPRINT
    assert moved["run_snapshot_id"] != EXPECTED_RUN_SNAPSHOT_ID
    assert moved["result_hash"] != EXPECTED_RESULT_HASH
    assert moved_objects["TRADE_DETAIL"] != EXPECTED_TRADE_DETAIL_CONTENT_HASH


def _digests(engine: Engine, run_id: str) -> dict[str, str]:
    run = sql_one(
        engine,
        "SELECT r.configuration_hash, r.result_hash, p.source_set_hash, p.input_hash "
        "FROM backtest.runs r JOIN backtest.performance_summaries p ON p.run_id = r.id "
        "WHERE r.id = :id",
        id=run_id,
    )
    snapshot = sql_one(
        engine,
        "SELECT summary_document FROM backtest.monthly_judgment_summaries WHERE run_id = :id",
        id=run_id,
    )
    document = snapshot["summary_document"]
    if isinstance(document, str):  # pragma: no cover - driver dependent
        document = json.loads(document)
    return {
        "run_id": run_id,
        "input_bundle_fingerprint": run["configuration_hash"],
        "result_hash": run["result_hash"],
        "source_set_hash": run["source_set_hash"],
        "input_hash": run["input_hash"],
        "run_snapshot_id": document["run_snapshot_id"],
    }


def _object_hashes(engine: Engine, run_id: str) -> dict[str, str]:
    rows = sql_all(
        engine,
        "SELECT m.record_type, o.content_hash FROM backtest.detail_manifests m "
        "JOIN storage.objects o ON o.id = m.object_id WHERE m.run_id = :id "
        "ORDER BY m.record_type, m.week_start_date, m.part_number",
        id=run_id,
    )
    digest: dict[str, str] = {}
    for row in rows:
        # One entry per record type, chaining the parts so a lost or reordered
        # part changes the value.
        previous = digest.get(row["record_type"], "")
        digest[row["record_type"]] = (
            row["content_hash"]
            if not previous
            else hashlib.sha256(f"{previous}|{row['content_hash']}".encode()).hexdigest()
        )
    return digest


def _stored_parts(engine: Engine, run_id: str) -> list[tuple[str, str]]:
    """Every published part, as (object_key, content_hash), in canonical order."""
    rows = sql_all(
        engine,
        "SELECT o.object_key, o.content_hash FROM backtest.detail_manifests m "
        "JOIN storage.objects o ON o.id = m.object_id WHERE m.run_id = :id "
        "ORDER BY m.record_type, m.week_start_date, m.part_number",
        id=run_id,
    )
    return [(str(row["object_key"]), str(row["content_hash"])) for row in rows]


# ===========================================================================
# At-least-once delivery, redelivery counts and the dead-letter queue
# ===========================================================================


def test_a_retryable_failure_is_redelivered_and_the_api_sees_the_delivery_count(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> None:
    """SQS's ApproximateReceiveCount must reach the API as `X-Delivery-Attempt`.

    The first delivery trips the pinned CPU budget, which is retryable, so no
    terminal event is published -- `FAILED` is terminal in `backtest.run_status`
    and would lock the run out of its own retry. The message becomes visible
    again, the second delivery succeeds, and the `COMPLETED` event it posts
    carries the delivery count the queue reported.
    """
    over_budget = ResourceSample(timedelta(minutes=5, microseconds=1), 64 * 1024 * 1024)
    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "retry",
        monitor=ScriptedMonitor(over_budget),
    )
    accepted = stack.accept()
    assert accepted.status_code == 202, accepted.text
    run_id = accepted.json()["run"]["backtestRunId"]

    first = stack.worker.poll_once()
    second = stack.worker.poll_once()

    assert [item.disposition for item in first] == [MessageDisposition.RETURNED]
    assert [item.reason_code for item in first] == ["CPU_LIMIT"]
    assert [item.disposition for item in second] == [MessageDisposition.DELETED]

    posted = stack.recorder.result_posts()
    # RUNNING on delivery 1, COMPLETED on delivery 2. The retryable failure
    # published nothing, by design.
    assert [headers["x-delivery-attempt"] for headers in posted] == ["1", "2"]

    row = sql_one(admin_engine, "SELECT status FROM backtest.runs WHERE id = :id", id=run_id)
    assert row["status"] == "COMPLETED"
    attempts = sql_all(
        admin_engine,
        "SELECT attempt_number, status FROM backtest.run_attempts WHERE run_id = :id",
        id=run_id,
    )
    # A retry closes the failed lease and creates a fenced successor. Preserving
    # both rows is what makes the redelivery and its result auditable; mutating the
    # first attempt back to SUCCEEDED would erase the CPU_LIMIT history.
    assert attempts == [
        {"attempt_number": 1, "status": "FAILED"},
        {"attempt_number": 2, "status": "SUCCEEDED"},
    ]
    assert stack.visible(stack.dead_letter_queue) == 0


def test_a_dataset_too_short_for_the_warmup_fails_the_run_and_dead_letters_the_job(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> None:
    """A permanently unsatisfiable job must not loop, and must not vanish silently."""
    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "short",
        closes=CLOSES[:10],
    )
    accepted = stack.accept()
    assert accepted.status_code == 202, accepted.text
    run_id = accepted.json()["run"]["backtestRunId"]

    handled = stack.worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == ["REQUIRED_INPUT_UNAVAILABLE"]
    assert stack.visible(stack.dead_letter_queue) == 1

    row = sql_one(
        admin_engine,
        "SELECT status, failure_code FROM backtest.runs WHERE id = :id",
        id=run_id,
    )
    assert row["status"] == "UNAVAILABLE"
    assert row["failure_code"] == "REQUIRED_INPUT_UNAVAILABLE"
    assert (
        sql_all(admin_engine, "SELECT run_id FROM backtest.performance_summaries WHERE run_id = :id", id=run_id) == []
    )
    assert sql_all(admin_engine, "SELECT id FROM backtest.detail_manifests WHERE run_id = :id", id=run_id) == []


@pytest.mark.parametrize("drift", ["missing", "version-changed"])
def test_task5_missing_or_version_changed_pinned_input_is_finite_and_ui_readable(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
    drift: str,
) -> None:
    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / f"input-{drift}",
    )
    _request, run_id = _accept_task5(stack, f"input-{drift}")
    if drift == "missing":
        changed: dict[Any, Any] = {}
    else:
        manifest = dict(stack.market_data.manifest)
        manifest["dataset_hash"] = "f" * 64
        changed = {DATASET_MANIFEST_ID: manifest}
    stack.handler._manifests = StaticDatasetManifestSource(changed)  # type: ignore[attr-defined]
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, f"input-{drift}")
    sentinel_run_id = task5_run_id(f"live-sentinel:input-{drift}")

    handled = stack.worker.poll_once()

    expected_reason = (
        "REQUIRED_INPUT_UNAVAILABLE" if drift == "missing" else "INPUT_DATASET_UNREADABLE"
    )
    expected_status = "UNAVAILABLE" if drift == "missing" else "FAILED"
    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == [expected_reason]
    assert stack.visible(stack.dead_letter_queue) == 1
    api = stack.client.get(f"/api/v1/backtests/{run_id}", headers=stack.owner())
    assert api.status_code == 200, api.text
    assert api.json()["status"] == expected_status
    assert api.json()["failureCode"] == expected_reason
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    record_evidence(
        evidence_result(
            scenario=f"task5-input-{drift}",
            terminal_state=expected_status,
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            failure_reason=expected_reason,
            observations={
                "drift": drift,
                "dlq_depth": 1,
                "ui_failure_code": expected_reason,
                "live_lease_preserved": True,
            },
        )
    )


def test_task5_addressable_invalid_job_is_failed_once_dead_lettered_and_ui_readable(
    stack: Stack,
    persistence: BacktestPersistence,
    admin_engine: Engine,
) -> None:
    request, run_id = _accept_task5(stack, "invalid-input")
    original = stack.sqs.receive_message(
        QueueUrl=stack.main_queue,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=1,
    )["Messages"][0]
    stack.sqs.delete_message(
        QueueUrl=stack.main_queue,
        ReceiptHandle=original["ReceiptHandle"],
    )
    stack.sqs.send_message(
        QueueUrl=stack.main_queue,
        MessageBody=json.dumps(
            {
                "backtestRunId": run_id,
                "idempotencyKey": request["metadata"]["idempotencyKey"],
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, "invalid-input")
    sentinel_run_id = task5_run_id("live-sentinel:invalid-input")

    handled = stack.worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DEAD_LETTERED]
    assert [item.reason_code for item in handled] == ["MESSAGE_NOT_PARSEABLE"]
    assert stack.visible(stack.dead_letter_queue) == 1
    api = stack.client.get(f"/api/v1/backtests/{run_id}", headers=stack.owner()).json()
    assert api["status"] == "FAILED"
    assert api["failureCode"] == "MESSAGE_NOT_PARSEABLE"
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    record_evidence(
        evidence_result(
            scenario="task5-invalid-input",
            terminal_state="FAILED",
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            failure_reason="MESSAGE_NOT_PARSEABLE",
            observations={
                "dlq_depth": 1,
                "ui_failure_code": api["failureCode"],
                "attempt_count": 1,
                "live_lease_preserved": True,
            },
        )
    )


@pytest.mark.parametrize(
    ("failure", "reason", "sample"),
    [
        (
            "cpu-limit",
            "CPU_LIMIT",
            ResourceSample(timedelta(minutes=5, microseconds=1), 64 * 1024 * 1024),
        ),
        (
            "memory-limit",
            "MEMORY_LIMIT",
            ResourceSample(timedelta(seconds=1), 512 * 1024 * 1024 + 1),
        ),
        ("publication-failure", "RESULT_PUBLICATION_FAILED", None),
    ],
)
def test_task5_retryable_resource_and_publication_failures_exhaust_finitely(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    reason: str,
    sample: ResourceSample | None,
) -> None:
    monitor: Any = (lambda: ScriptedMonitor(sample)) if sample is not None else ScriptedMonitor()
    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / failure,
        monitor=monitor,
    )
    if failure == "publication-failure":

        def fail_publication(*_args: Any, **_kwargs: Any) -> None:
            raise OSError("injected Task 5 publication boundary failure")

        monkeypatch.setattr(DurableResultPublisher, "_write", fail_publication)
    _request, run_id = _accept_task5(stack, failure)
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, failure)
    sentinel_run_id = task5_run_id(f"live-sentinel:{failure}")

    outcomes = [stack.worker.poll_once()[0] for _ in range(3)]

    assert [item.disposition for item in outcomes] == [
        MessageDisposition.RETURNED,
        MessageDisposition.RETURNED,
        MessageDisposition.DEAD_LETTERED,
    ]
    assert [item.reason_code for item in outcomes[:2]] == [reason, reason]
    assert outcomes[2].reason_code == f"MAX_ATTEMPTS_EXHAUSTED:{reason}"
    assert stack.visible(stack.main_queue) == 0
    assert stack.visible(stack.dead_letter_queue) == 1
    api = stack.client.get(f"/api/v1/backtests/{run_id}", headers=stack.owner()).json()
    assert api["status"] == "FAILED"
    assert api["failureCode"] == "MAX_ATTEMPTS_EXHAUSTED"
    attempts = sql_all(
        admin_engine,
        "SELECT attempt_number,status,failure_code FROM backtest.run_attempts WHERE run_id=:id ORDER BY attempt_number",
        id=run_id,
    )
    assert attempts == [
        {"attempt_number": number, "status": "FAILED", "failure_code": reason} for number in range(1, 4)
    ]
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    resource_peak = {}
    if failure == "cpu-limit":
        resource_peak = {"cpu_seconds": 300.000001}
    elif failure == "memory-limit":
        resource_peak = {"memory_bytes": float(512 * 1024 * 1024 + 1)}
    record_evidence(
        evidence_result(
            scenario=f"task5-{failure}",
            terminal_state="FAILED",
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            failure_reason=f"MAX_ATTEMPTS_EXHAUSTED:{reason}",
            observations={
                "attempt_count": 3,
                "dlq_depth": 1,
                "ui_failure_code": api["failureCode"],
                "typed_attempt_reason": reason,
                "live_lease_preserved": True,
            },
            resource_peak=resource_peak,
        )
    )


@pytest.mark.parametrize("checkpoint", ["binding", "replay", "upload", "publication"])
def test_task5_process_kill_restart_preserves_lineage_and_one_terminal_publication(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    postgres_url: str,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
    checkpoint: str,
) -> None:
    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "parent-market",
    )
    request, run_id = _accept_task5(stack, f"kill-restart-{checkpoint}")
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, f"kill-restart-{checkpoint}")
    sentinel_run_id = task5_run_id(f"live-sentinel:kill-restart-{checkpoint}")
    marker = tmp_path / f"task5-{checkpoint}.checkpoint.json"
    error_marker = tmp_path / f"task5-{checkpoint}.error.txt"
    child_environment = os.environ.copy()
    child_environment.update(
        {
            "TASK5_DATABASE_URL": postgres_url,
            "TASK5_SQS_ENDPOINT": str(sqs.meta.endpoint_url),
            "TASK5_S3_ENDPOINT": str(s3.meta.endpoint_url),
            "TASK5_MAIN_QUEUE": stack.main_queue,
            "TASK5_DLQ": stack.dead_letter_queue,
            "TASK5_BUCKET": bucket,
            "TASK5_MARKET_ROOT": str(tmp_path / "child-market"),
            "TASK5_REQUEST_JSON": json.dumps(request, sort_keys=True, separators=(",", ":")),
            "TASK5_CHECKPOINT": checkpoint,
            "TASK5_RUN_ID": run_id,
            "TASK5_CHECKPOINT_MARKER": str(marker),
            "TASK5_ERROR_MARKER": str(error_marker),
        }
    )
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).with_name("d_task5_chaos_worker.py"))],
        cwd=Path(__file__).parents[1],
        env=child_environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_until(
            lambda: marker.exists() or process.poll() is not None,
            description=f"child worker to reach {checkpoint}",
            timeout_seconds=45,
        )
        assert marker.exists(), (
            f"child exited before {checkpoint}: "
            f"{error_marker.read_text(encoding='utf-8').strip() if error_marker.exists() else process.returncode}"
        )
        assert not error_marker.exists()
        first_claim = sql_one(
            admin_engine,
            "SELECT id,claim_token,status FROM backtest.run_attempts WHERE run_id=:id ORDER BY attempt_number LIMIT 1",
            id=run_id,
        )
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=15)

    wait_until(
        lambda: stack.visible(stack.main_queue) == 1,
        description=f"{checkpoint} delivery visibility after process kill",
        timeout_seconds=30,
    )
    if checkpoint != "publication":
        wait_until(
            lambda: bool(
                sql_one(
                    admin_engine,
                    "SELECT claim_expires_at <= clock_timestamp() AS expired FROM backtest.run_attempts WHERE id=:id",
                    id=first_claim["id"],
                )["expired"]
            ),
            description=f"{checkpoint} lease expiry",
            timeout_seconds=30,
        )
        recovery = _assert_recovery_invariants(
            persistence,
            admin_engine,
            sentinel_run_id=sentinel_run_id,
        )
        assert recovery.requeued == 1
        recovered = sql_one(
            admin_engine,
            "SELECT status FROM backtest.runs WHERE id=:id",
            id=run_id,
        )
        assert recovered["status"] == "QUEUED"
    else:
        assert first_claim["status"] == "SUCCEEDED"
        recovery = _assert_recovery_invariants(
            persistence,
            admin_engine,
            sentinel_run_id=sentinel_run_id,
        )
        assert recovery.requeued == recovery.failed == recovery.cancelled == 0

    restarted = stack.worker.poll_once()
    assert [item.disposition for item in restarted] == [MessageDisposition.DELETED]
    if checkpoint == "publication":
        assert [item.reason_code for item in restarted] == ["DUPLICATE_ALREADY_SUCCEEDED"]
    else:
        assert [item.reason_code for item in restarted] == [None]

    terminal = sql_one(
        admin_engine,
        "SELECT status,result_hash FROM backtest.runs WHERE id=:id",
        id=run_id,
    )
    assert terminal["status"] == "COMPLETED"
    assert terminal["result_hash"]
    attempts = sql_all(
        admin_engine,
        "SELECT id,attempt_number,status,terminal_reason_code,previous_attempt_id "
        "FROM backtest.run_attempts WHERE run_id=:id ORDER BY attempt_number",
        id=run_id,
    )
    if checkpoint == "publication":
        assert len(attempts) == 1
        assert attempts[0]["status"] == "SUCCEEDED"
    else:
        assert len(attempts) == 2
        assert [item["status"] for item in attempts] == ["FAILED", "SUCCEEDED"]
        assert attempts[0]["terminal_reason_code"] == "LEASE_EXPIRED"
        assert attempts[1]["previous_attempt_id"] == attempts[0]["id"]
        with persistence.unit_of_work() as uow, pytest.raises(StaleAttemptClaim):
            uow.attempts.close_fenced(
                uuid.UUID(str(first_claim["id"])),
                uuid.UUID(str(first_claim["claim_token"])),
                status=WorkStatus.SUCCEEDED,
                terminal_reason_code="LATE_COMPLETION_MUST_BE_FENCED",
            )
    assert (
        len(
            sql_all(
                admin_engine,
                "SELECT run_id FROM backtest.performance_summaries WHERE run_id=:id",
                id=run_id,
            )
        )
        == 1
    )
    assert (
        len(
            sql_all(
                admin_engine,
                "SELECT id FROM backtest.detail_manifests WHERE run_id=:id",
                id=run_id,
            )
        )
        > 0
    )
    assert stack.visible(stack.main_queue) == 0
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    record_evidence(
        evidence_result(
            scenario=f"task5-kill-restart-{checkpoint}",
            terminal_state="COMPLETED",
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            observations={
                "checkpoint": checkpoint,
                "attempt_count": len(attempts),
                "late_completion_fenced": checkpoint != "publication",
                "terminal_publications": 1,
                "live_lease_preserved": True,
            },
            result_hash=str(terminal["result_hash"]),
        )
    )


def test_cancellation_at_completion_cannot_publish_success_or_result_rows(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation after replay still wins the atomic publication boundary.

    Moving the cancellation check after result inserts, or treating a cancelled
    fenced close as successful publication, must make this test expose COMPLETED
    state, result rows, a failed worker call, or a redeliverable queue message.
    """

    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "cancel-at-completion",
    )

    _request, run_id = _accept_task5(stack, "cancellation-completion")
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, "cancellation-completion")
    sentinel_run_id = task5_run_id("live-sentinel:cancellation-completion")

    publication_reached = threading.Event()
    continue_publication = threading.Event()
    original_write = DurableResultPublisher._write

    def gated_write(self: DurableResultPublisher, *args: Any, **kwargs: Any) -> None:
        publication_reached.set()
        assert continue_publication.wait(timeout=30), "publication gate was never released"
        original_write(self, *args, **kwargs)

    monkeypatch.setattr(DurableResultPublisher, "_write", gated_write)
    handled: list[Any] = []
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            handled.extend(stack.worker.poll_once())
        except BaseException as exc:  # surfaced below with its exact exception type
            errors.append(exc)

    worker_thread = threading.Thread(target=execute, name="task5-cancel-completion")
    worker_thread.start()
    assert publication_reached.wait(timeout=30), "worker never reached publication"

    cancellation = stack.client.post(
        f"/api/v1/backtests/{run_id}/cancellation",
        json={"reasonCode": "USER_CANCELLED"},
        headers=stack.owner(),
    )
    assert cancellation.status_code == 202, cancellation.text
    assert cancellation.json()["run"]["status"] == "RUNNING"
    continue_publication.set()
    worker_thread.join(timeout=30)

    assert not worker_thread.is_alive(), "worker did not finish after publication was released"
    assert errors == []
    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert [item.reason_code for item in handled] == ["USER_CANCELLED"]
    row = sql_one(
        admin_engine,
        "SELECT status, result_hash, failure_code, cancellation_reason_code FROM backtest.runs WHERE id=:id",
        id=run_id,
    )
    assert row == {
        "status": "CANCELLED",
        "result_hash": None,
        "failure_code": None,
        "cancellation_reason_code": "USER_CANCELLED",
    }
    attempts = sql_all(
        admin_engine,
        "SELECT attempt_number,status,terminal_reason_code FROM backtest.run_attempts "
        "WHERE run_id=:id ORDER BY attempt_number",
        id=run_id,
    )
    assert attempts == [
        {
            "attempt_number": 1,
            "status": "CANCELLED",
            "terminal_reason_code": "CANCELLED_BY_REQUEST",
        }
    ]
    assert (
        sql_all(
            admin_engine,
            "SELECT run_id FROM backtest.performance_summaries WHERE run_id=:id",
            id=run_id,
        )
        == []
    )
    assert (
        sql_all(
            admin_engine,
            "SELECT id FROM backtest.detail_manifests WHERE run_id=:id",
            id=run_id,
        )
        == []
    )
    assert stack.visible(stack.main_queue) == 0
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    record_evidence(
        evidence_result(
            scenario="task5-cancellation-race-completion",
            terminal_state="CANCELLED",
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            failure_reason="USER_CANCELLED",
            observations={
                "result_rows": 0,
                "detail_rows": 0,
                "queue_visible": 0,
                "live_lease_preserved": True,
            },
        )
    )


def test_cancellation_raced_with_heartbeat_is_observed_at_the_next_replay_checkpoint(
    persistence: BacktestPersistence,
    admin_engine: Engine,
    sqs: Any,
    s3: Any,
    queues: tuple[str, str],
    bucket: str,
    tmp_path: Path,
) -> None:
    """A durable heartbeat carries cancellation into the next replay checkpoint."""

    checkpoint_reached = threading.Event()
    continue_checkpoint = threading.Event()

    class GatedCheckpointMonitor:
        def sample(self) -> ResourceSample:
            checkpoint_reached.set()
            assert continue_checkpoint.wait(timeout=30), "checkpoint gate was never released"
            return ResourceSample(timedelta(seconds=1), 64 * 1024 * 1024)

    stack = build_stack(
        persistence=persistence,
        sqs_client=sqs,
        s3_client=s3,
        queues=queues,
        bucket=bucket,
        root=tmp_path / "cancel-heartbeat-checkpoint",
        monitor=GatedCheckpointMonitor(),
    )
    _request, run_id = _accept_task5(stack, "cancellation-heartbeat-checkpoint")
    stack.worker = BacktestWorker(
        client=sqs,
        config=WorkerConfig(
            queue_url=stack.main_queue,
            dead_letter_queue_url=stack.dead_letter_queue,
            worker_id="task5-cancellation-checkpoint-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=3),
            wait_time=timedelta(0),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=1),
        ),
        handler=stack.handler,
        store=PersistenceExecutionKeyStore(persistence),
    )
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(
        persistence, "cancellation-heartbeat-checkpoint"
    )
    sentinel_run_id = task5_run_id("live-sentinel:cancellation-heartbeat-checkpoint")
    handled: list[Any] = []
    errors: list[BaseException] = []

    def execute() -> None:
        try:
            handled.extend(stack.worker.poll_once())
        except BaseException as exc:
            errors.append(exc)

    worker_thread = threading.Thread(target=execute, name="task5-cancel-checkpoint")
    worker_thread.start()
    assert checkpoint_reached.wait(timeout=30), "worker never reached replay checkpoint"
    cancellation = stack.client.post(
        f"/api/v1/backtests/{run_id}/cancellation",
        json={"reasonCode": "USER_CANCELLED"},
        headers=stack.owner(),
    )
    assert cancellation.status_code == 202, cancellation.text

    def cancellation_crossed_heartbeat() -> bool:
        with admin_engine.connect() as connection:
            return bool(
                connection.scalar(
                    text(
                        "SELECT a.last_heartbeat_at >= r.cancellation_requested_at "
                        "FROM backtest.runs r JOIN backtest.run_attempts a ON a.run_id=r.id "
                        "WHERE r.id=:id AND a.status='RUNNING'"
                    ),
                    {"id": uuid.UUID(run_id)},
                )
            )

    wait_until(
        cancellation_crossed_heartbeat,
        description="worker heartbeat to observe the cancellation request",
        timeout_seconds=30,
    )
    continue_checkpoint.set()
    worker_thread.join(timeout=30)

    assert not worker_thread.is_alive()
    assert errors == []
    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert [item.reason_code for item in handled] == ["USER_CANCELLED"]
    terminal = sql_one(
        admin_engine,
        "SELECT status,result_hash,cancellation_reason_code FROM backtest.runs WHERE id=:id",
        id=run_id,
    )
    assert terminal == {
        "status": "CANCELLED",
        "result_hash": None,
        "cancellation_reason_code": "USER_CANCELLED",
    }
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    observations = {
        "heartbeat_observed_cancellation": True,
        "checkpoint_terminal": "CANCELLED",
        "success_result_excluded": True,
        "live_lease_preserved": True,
    }
    for scenario in (
        "task5-cancellation-race-heartbeat",
        "task5-cancellation-race-checkpoint",
    ):
        record_evidence(
            evidence_result(
                scenario=scenario,
                terminal_state="CANCELLED",
                duration_seconds=0,
                run_id=run_id,
                attempt_lineage=_attempt_evidence(admin_engine, run_id),
                failure_reason="USER_CANCELLED",
                observations=observations,
            )
        )


def test_a_redelivered_completed_job_is_acknowledged_without_running_twice(
    stack: Stack,
    persistence: BacktestPersistence,
    admin_engine: Engine,
) -> None:
    """At-least-once delivery must not produce a second official result."""
    request, run_id = _accept_task5(stack, "duplicate-delivery")
    first = stack.worker.poll_once()
    assert [item.disposition for item in first] == [MessageDisposition.DELETED]
    sentinel_key, sentinel_store, sentinel_claim = _start_live_sentinel(persistence, "duplicate-delivery")
    sentinel_run_id = task5_run_id("live-sentinel:duplicate-delivery")
    # A duplicate of the very same message, as the queue would redeliver it.
    stack.sqs.send_message(
        QueueUrl=stack.main_queue,
        MessageBody=json.dumps(
            {
                "backtestRunId": run_id,
                "botId": str(BOT_ID),
                "ownerAccountId": str(ACCOUNT_ID),
                "idempotencyKey": request["metadata"]["idempotencyKey"],
                "inputBundleFingerprint": EXPECTED_INPUT_BUNDLE_FINGERPRINT,
                "executionPolicyVersion": E2E_EXECUTION_POLICY.version,
                "compiledPlanChecksum": stack.request["compiledPlanChecksum"],
                "datasetManifestId": str(DATASET_MANIFEST_ID),
                "expectedSnapshotHash": stack.request["expectedSnapshotHash"],
            },
            separators=(",", ":"),
            sort_keys=True,
        ),
    )

    handled = stack.worker.poll_once()

    assert [item.disposition for item in handled] == [MessageDisposition.DELETED]
    assert [item.reason_code for item in handled] == ["DUPLICATE_ALREADY_SUCCEEDED"]
    assert sql_all(
        admin_engine,
        "SELECT attempt_number FROM backtest.run_attempts WHERE run_id = :id",
        id=run_id,
    ) == [{"attempt_number": 1}]
    assert (
        len(
            sql_all(
                admin_engine,
                "SELECT run_id FROM backtest.performance_summaries WHERE run_id=:id",
                id=run_id,
            )
        )
        == 1
    )
    _assert_recovery_invariants(
        persistence,
        admin_engine,
        sentinel_run_id=sentinel_run_id,
    )
    _finish_live_sentinel(
        persistence,
        sentinel_run_id,
        sentinel_key,
        sentinel_store,
        sentinel_claim,
    )
    record_evidence(
        evidence_result(
            scenario="task5-duplicate-delivery-idempotency",
            terminal_state="COMPLETED",
            duration_seconds=0,
            run_id=run_id,
            attempt_lineage=_attempt_evidence(admin_engine, run_id),
            observations={
                "duplicate_reason": handled[0].reason_code,
                "attempt_count": 1,
                "terminal_publications": 1,
                "live_lease_preserved": True,
            },
            result_hash=sql_one(
                admin_engine,
                "SELECT result_hash FROM backtest.runs WHERE id=:id",
                id=run_id,
            )["result_hash"],
        )
    )


def test_a_second_acceptance_of_the_same_request_neither_re_creates_nor_re_queues(
    stack: Stack,
) -> None:
    """B may redeliver its own request; that must not enqueue a second job."""
    first = stack.accept()
    second = stack.accept()

    assert first.json()["created"] is True and first.json()["dispatched"] is True
    assert second.status_code == 202
    assert second.json()["created"] is False
    assert second.json()["dispatched"] is False
    assert stack.visible(stack.main_queue) == 1
