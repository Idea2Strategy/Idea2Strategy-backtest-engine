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
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest
from sqlalchemy import Engine

from backtest_engine.attempt_coordinator import ResourceSample
from backtest_engine.persistence import BacktestPersistence
from backtest_engine.worker import MessageDisposition

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


pytestmark = pytest.mark.docker


# ---------------------------------------------------------------------------
# Pinned digests
# ---------------------------------------------------------------------------

#: `uuid5` of B's own `metadata.idempotencyKey`. Two deliveries of the same
#: request address this run without a database round trip.
EXPECTED_RUN_ID = "76a6a20c-0651-5748-8187-6bf0ae155194"

#: `backtest.runs.configuration_hash`, published as `inputBundleFingerprint`.
EXPECTED_INPUT_BUNDLE_FINGERPRINT = (
    "sha256:8b73c1ad86cf42c2360989ecb14b225a95e191eccd9c8a7e3f6ead8ef84add25"
)

#: `RunSnapshot.snapshot_id`: the pinned run inputs.
EXPECTED_RUN_SNAPSHOT_ID = "75fd83d0c9cd6356a9c0ed1db9833881f19a0a136042bf96d82617085ba64348"

#: `backtest.performance_summaries.result_hash`.
EXPECTED_RESULT_HASH = "ce53f523451e506c2ec8264043b9221729eefe8c2040088883a30dc922d56c08"

#: `storage.objects.content_hash` of the TRADE_DETAIL Parquet part.
EXPECTED_TRADE_DETAIL_CONTENT_HASH = (
    "28977f43a1a3cb811538affd6a2a98903641060089b24e2de48d84d925be5629"
)


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
    queued = sql_one(
        admin_engine, "SELECT status, configuration_hash FROM backtest.runs WHERE id = :id", id=run_id
    )
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
        "SELECT status, result_hash, completed_at, started_at, initial_cash_amount "
        "FROM backtest.runs WHERE id = :id",
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
    # 20 one-minute bars -> 20 evaluation instants; exactly one of them decided.
    assert monthly[0]["evaluation_count"] == len(CLOSES)
    assert monthly[0]["triggered_count"] == 1
    assert monthly[0]["data_gap_count"] == 0
    assert monthly[0]["trade_event_count"] == 2  # the accepted order and its fill
    assert monthly[0]["rejected_count"] == 0

    bundle = sql_one(
        admin_engine,
        "SELECT bundle_hash FROM backtest.input_bundles WHERE run_id = :id", id=run_id
    )
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
    assert stack.client.get(
        f"/api/v1/backtests/{run_id}", headers=stack.owner()
    ).json()["status"] == "COMPLETED"
    assert [
        item["backtestRunId"]
        for item in stack.client.get("/api/v1/backtests", headers=stack.owner()).json()["items"]
    ] == [run_id]
    api_attempts = stack.client.get(
        f"/api/v1/backtests/{run_id}/attempts", headers=stack.owner()
    ).json()["items"]
    assert [item["status"] for item in api_attempts] == ["SUCCEEDED"]
    api_performance = stack.client.get(
        f"/api/v1/backtests/{run_id}/performance", headers=stack.owner()
    ).json()
    assert api_performance["resultHash"] == EXPECTED_RESULT_HASH
    api_monthly = stack.client.get(
        f"/api/v1/backtests/{run_id}/monthly-summaries", headers=stack.owner()
    ).json()["items"]
    assert [item["etYearMonth"] for item in api_monthly] == ["2024-01"]
    api_manifests = stack.client.get(
        f"/api/v1/backtests/{run_id}/detail-manifests", headers=stack.owner()
    ).json()["items"]
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
    assert {item["recordId"] for item in body["items"]} == {
        str(row["record_id"]) for row in trades_rows
    }
    # A month the run never traded in is empty, not a 404 and not an error.
    assert stack.client.get(
        f"/api/v1/backtests/{run_id}/monthly-trades?et_month=2024-02",
        headers=stack.owner(),
    ).json()["items"] == []

    # -- and the pinned inputs the acceptance transaction recorded ----------
    api_inputs = stack.client.get(
        f"/api/v1/backtests/{run_id}/inputs", headers=stack.owner()
    )
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
    assert [item.reason_code for item in handled] == ["REQUIRED_DATA_UNAVAILABLE"]
    assert stack.visible(stack.dead_letter_queue) == 1

    row = sql_one(
        admin_engine,
        "SELECT status, failure_code FROM backtest.runs WHERE id = :id",
        id=run_id,
    )
    assert row["status"] == "FAILED"
    assert row["failure_code"] == "REQUIRED_DATA_UNAVAILABLE"
    assert sql_all(
        admin_engine, "SELECT run_id FROM backtest.performance_summaries WHERE run_id = :id", id=run_id
    ) == []
    assert sql_all(
        admin_engine, "SELECT id FROM backtest.detail_manifests WHERE run_id = :id", id=run_id
    ) == []


def test_a_redelivered_completed_job_is_acknowledged_without_running_twice(
    stack: Stack, admin_engine: Engine
) -> None:
    """At-least-once delivery must not produce a second official result."""
    run_id = run_once(stack)
    # A duplicate of the very same message, as the queue would redeliver it.
    stack.sqs.send_message(
        QueueUrl=stack.main_queue,
        MessageBody=json.dumps(
            {
                "backtestRunId": run_id,
                "botId": str(BOT_ID),
                "ownerAccountId": str(ACCOUNT_ID),
                "idempotencyKey": stack.request["metadata"]["idempotencyKey"],
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
