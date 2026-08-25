"""D91 against real infrastructure: exactly one official run per strategy release.

`test_d91_release_intake.py` drives the decision table through a recording fake
queue. This module drives the same `OfficialBacktestIntake` over a **real**
LocalStack SQS queue and a **real** Testcontainers PostgreSQL 16, because the two
properties D91 is actually about cannot be observed anywhere else:

*A duplicate delivery must not create a second run.*
    SQS Standard is at-least-once, and a redelivery is produced here by letting the
    message's visibility timeout expire and receiving it again -- the queue's own
    redelivery, with the queue's own `ApproximateReceiveCount`. A test that called
    `handle()` twice with a hand-written receive count would be asserting that the
    test can count, not that the queue's duplicate is absorbed.

*Exactly one row.*
    `backtest.runs.idempotency_key` is UNIQUE in the canonical schema, and the run
    id is `uuid5` of B's own `metadata.idempotencyKey`. Both halves only exist in
    PostgreSQL, so the row count is read back with SQL on the admin engine rather
    than from the gateway that wrote it.

B's bot id is seeded by `db/migration-contributions/fixtures/backtest_reference_seed
.sql.fixture`, so B's published message is consumed **verbatim** -- no field is
re-addressed to make it fit this repository's fixtures.
"""

from __future__ import annotations

import copy
import json
import time
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine

from backtest_engine.contracts import (
    OFFICIAL_BACKTEST_MESSAGE_TYPE,
    STRATEGY_BOT_CONTRACT_VERSION,
    compute_compiled_plan_checksum,
    compute_message_idempotency_key,
    official_backtest_operation_key,
    validate_backtest_result_event,
)
from backtest_engine.execution_policy import ExecutionPolicyCatalog, et_quarter_start
from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    PersistenceRunGateway,
    SqsBacktestJobQueue,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
    run_id_for,
)
from backtest_engine.persistence import BacktestPersistence
from backtest_engine.release_intake import (
    IntakeConfig,
    IntakeDisposition,
    OfficialBacktestIntake,
)
from d_integration_stack import sql_all, sql_one, sql_scalar
from d_reproducibility_testkit import ACCOUNT_ID, policy_with
from test_d91_release_intake import (
    B_BOT_ID,
    B_DATASET_MANIFEST_ID,
    B_EXECUTION_POLICY_VERSION,
    B_IDEMPOTENCY_KEY,
    B_RELEASE_QUARTER,
    B_RUN_ID,
    b_dataset_manifest,
    b_plan,
    b_request,
    release_request_for,
)


pytestmark = pytest.mark.docker


#: Short enough that a real redelivery is observable inside a test, long enough that
#: the first receive is not redelivered while it is still being handled.
VISIBILITY = timedelta(seconds=2)

B_RELEASE_POLICY = policy_with(
    version=B_EXECUTION_POLICY_VERSION,
    release_quarter=B_RELEASE_QUARTER,
    period_start=et_quarter_start(2026, 3),
    period_end=et_quarter_start(2026, 4),
)


class Harness:
    """The real intake, bound to a real queue pair and a real database."""

    def __init__(
        self,
        *,
        persistence: BacktestPersistence,
        sqs: Any,
        queues: tuple[str, str],
        plans: dict[str, Any] | None = None,
    ) -> None:
        plan = b_plan()
        self.sqs = sqs
        self.release_queue, self.dead_letter_queue = queues
        # A third queue: the release queue carries B's contract in, the job queue
        # carries D's own job out. One queue with two contracts is the shape D91
        # exists to avoid.
        self.job_queue = sqs.create_queue(
            QueueName=f"d91-jobs-{uuid4().hex[:12]}",
            Attributes={"VisibilityTimeout": "30", "ReceiveMessageWaitTimeSeconds": "0"},
        )["QueueUrl"]
        self.lifecycle = BacktestLifecycleService(
            gateway=PersistenceRunGateway(persistence),
            queue=SqsBacktestJobQueue(sqs, self.job_queue),
            owners=StaticOwnerDirectory({B_BOT_ID: ACCOUNT_ID}),
            plans=StaticCompiledPlanSource(
                plans if plans is not None else {plan["planChecksum"]: plan}
            ),
            manifests=StaticDatasetManifestSource(
                {B_DATASET_MANIFEST_ID: b_dataset_manifest()}
            ),
            policies=ExecutionPolicyCatalog([B_RELEASE_POLICY]),
        )
        self.intake = OfficialBacktestIntake(
            client=sqs,
            config=IntakeConfig(
                queue_url=self.release_queue,
                dead_letter_queue_url=self.dead_letter_queue,
                consumer_id="d91-e2e-intake",
                max_receive_count=3,
                visibility_timeout=VISIBILITY,
                wait_time=timedelta(seconds=0),
                max_messages=10,
            ),
            lifecycle=self.lifecycle,
        )

    def publish(self, document: Any) -> str:
        body = document if isinstance(document, str) else json.dumps(document, sort_keys=True)
        sent: str = self.sqs.send_message(QueueUrl=self.release_queue, MessageBody=body)[
            "MessageId"
        ]
        return sent

    def drain(self, *, expected: int, deadline_seconds: float = 20.0) -> list[Any]:
        """Poll until `expected` deliveries have been dispositioned.

        Long-polling with a deadline rather than a fixed sleep: LocalStack's SQS is
        eventually consistent about visibility, so "receive until we have seen what
        we published" is the only stable stopping rule.
        """
        outcomes: list[Any] = []
        stop = time.monotonic() + deadline_seconds
        while len(outcomes) < expected and time.monotonic() < stop:
            outcomes.extend(self.intake.poll_once())
        assert len(outcomes) == expected, [
            (item.disposition, item.reason_code) for item in outcomes
        ]
        return outcomes

    def jobs(self) -> list[dict[str, Any]]:
        """Every job message currently on D's own queue."""
        received: list[dict[str, Any]] = []
        stop = time.monotonic() + 8.0
        while time.monotonic() < stop:
            batch = self.sqs.receive_message(
                QueueUrl=self.job_queue, MaxNumberOfMessages=10, WaitTimeSeconds=1
            ).get("Messages", [])
            if not batch:
                break
            for message in batch:
                received.append(json.loads(message["Body"]))
                self.sqs.delete_message(
                    QueueUrl=self.job_queue, ReceiptHandle=message["ReceiptHandle"]
                )
        return received

    def dead_letters(self) -> list[dict[str, Any]]:
        messages = self.sqs.receive_message(
            QueueUrl=self.dead_letter_queue,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=2,
            MessageAttributeNames=["All"],
        ).get("Messages", [])
        return [dict(item) for item in messages]


@pytest.fixture
def harness(
    persistence: BacktestPersistence, sqs: Any, queues: tuple[str, str]
) -> Harness:
    return Harness(persistence=persistence, sqs=sqs, queues=queues)


def _runs(engine: Engine) -> list[Any]:
    return sql_all(
        engine,
        "SELECT id, bot_id, owner_account_id, status, idempotency_key, "
        "slippage_rate_bps, evaluation_start, evaluation_end, configuration_hash "
        "FROM backtest.runs ORDER BY idempotency_key",
    )


# ===========================================================================
# B's published message, verbatim, creates exactly one row
# ===========================================================================


def test_bs_published_release_creates_exactly_one_run_row(
    harness: Harness, admin_engine: Engine
) -> None:
    published = b_request()
    # Verbatim: the bytes put on the queue are B's fixture, unmodified. `b_request`
    # reads B's own copy when the superproject is checked out and the vendored copy
    # otherwise, and `tests/test_contracts.py` pins those two to the same digest.
    assert published == b_request()
    assert _runs(admin_engine) == [], "nothing may exist before the message is published"

    harness.publish(published)
    outcome = harness.drain(expected=1)[0]

    assert outcome.disposition is IntakeDisposition.ACCEPTED_CREATED
    assert outcome.backtest_run_id == B_RUN_ID
    stored_features = sql_all(
        admin_engine,
        "SELECT f.feature_materialization_id, f.locked_result_hash "
        "FROM backtest.input_feature_materializations f "
        "JOIN backtest.input_bundles b ON b.id = f.input_bundle_id "
        "WHERE b.run_id = :run_id ORDER BY f.feature_materialization_id",
        run_id=B_RUN_ID,
    )
    assert stored_features == [
        {
            "feature_materialization_id": UUID(
                published["featureMaterializations"][0]["featureMaterializationId"]
            ),
            "locked_result_hash": published["featureMaterializations"][0]["lockedResultHash"],
        }
    ]

    rows = _runs(admin_engine)
    assert len(rows) == 1
    stored = rows[0]
    assert stored["id"] == B_RUN_ID
    assert stored["bot_id"] == B_BOT_ID
    assert stored["owner_account_id"] == ACCOUNT_ID
    assert stored["status"] == "QUEUED"
    assert stored["idempotency_key"] == B_IDEMPOTENCY_KEY
    # The backend's SQL hardcodes 5; D reads the published policy, which also says 5
    # here, so the next assertion is the one that distinguishes them.
    assert stored["slippage_rate_bps"] == B_RELEASE_POLICY.slippage_rate_bps
    assert stored["evaluation_start"].isoformat() == "2026-07-01"
    assert stored["evaluation_end"].isoformat() == "2026-10-01"


def test_the_slippage_comes_from_the_policy_not_from_the_backends_literal_5(
    persistence: BacktestPersistence, sqs: Any, queues: tuple[str, str], admin_engine: Engine
) -> None:
    """A build that hardcoded 5, as `ImmutableStrategyReleaseJooqCommandAdapter` does, fails here."""
    harness = Harness(persistence=persistence, sqs=sqs, queues=queues)
    harness.lifecycle.policies = ExecutionPolicyCatalog(
        [
            policy_with(
                version="official-backtest-policy-2026q3-slip17",
                release_quarter=B_RELEASE_QUARTER,
                period_start=et_quarter_start(2026, 3),
                period_end=et_quarter_start(2026, 4),
                slippage_rate_bps=17,
            )
        ]
    )

    request = b_request()
    request["executionPolicyVersion"] = "official-backtest-policy-2026q3-slip17"
    harness.publish(request)
    harness.drain(expected=1)

    assert _runs(admin_engine)[0]["slippage_rate_bps"] == 17


# ===========================================================================
# At-least-once: a real redelivery through the queue
# ===========================================================================


def test_a_real_queue_redelivery_creates_no_second_run(
    harness: Harness, admin_engine: Engine
) -> None:
    """The redelivery is SQS's, not the test's.

    The first delivery is received and handled, but the intake's `delete_message`
    is suppressed for that one call, so the message stays on the queue and SQS
    makes it visible again once the visibility timeout lapses. The second receive
    therefore carries the queue's own `ApproximateReceiveCount = 2`.
    """
    deleted: list[str] = []
    real_delete = harness.sqs.delete_message

    def swallow_first_delete(**kwargs: Any) -> Any:
        if not deleted:
            deleted.append(str(kwargs["ReceiptHandle"]))
            return None
        return real_delete(**kwargs)

    harness.intake._client = _DelegatingSqs(harness.sqs, delete_message=swallow_first_delete)

    harness.publish(b_request())
    first = harness.drain(expected=1)[0]
    assert first.disposition is IntakeDisposition.ACCEPTED_CREATED
    assert len(deleted) == 1, "the first delete must have been suppressed"

    # Wait for SQS itself to make the undeleted message visible again.
    time.sleep(VISIBILITY.total_seconds() + 1)
    second = harness.drain(expected=1)[0]

    assert second.disposition is IntakeDisposition.ACCEPTED_DUPLICATE
    assert second.reason_code == "DUPLICATE_RELEASE_DELIVERY"
    assert second.backtest_run_id == B_RUN_ID
    # One row, and the unique index on idempotency_key is what guarantees it.
    assert sql_scalar(admin_engine, "SELECT count(*) FROM backtest.runs") == 1
    assert (
        sql_scalar(
            admin_engine,
            "SELECT count(*) FROM backtest.runs WHERE idempotency_key = :key",
            key=B_IDEMPOTENCY_KEY,
        )
        == 1
    )
    # And exactly one job: a duplicate release must not schedule a second execution.
    assert [job["backtestRunId"] for job in harness.jobs()] == [str(B_RUN_ID)]


def test_the_same_release_published_twice_as_two_messages_is_still_one_run(
    harness: Harness, admin_engine: Engine
) -> None:
    """Redrive and replay produce new `MessageId`s; identity is B's own key."""
    first_id = harness.publish(b_request())
    second_id = harness.publish(b_request())
    assert first_id != second_id

    outcomes = harness.drain(expected=2)

    assert sorted(item.disposition.value for item in outcomes) == [
        "ACCEPTED_CREATED",
        "ACCEPTED_DUPLICATE",
    ]
    assert sql_scalar(admin_engine, "SELECT count(*) FROM backtest.runs") == 1
    assert [job["backtestRunId"] for job in harness.jobs()] == [str(B_RUN_ID)]


# ===========================================================================
# Two different releases, interleaved on one queue
# ===========================================================================


def test_interleaved_deliveries_of_two_releases_do_not_cross_contaminate(
    persistence: BacktestPersistence, sqs: Any, queues: tuple[str, str], admin_engine: Engine
) -> None:
    """Two releases of the same bot, their deliveries interleaved A B A B.

    Each must land on its own run with its own configuration hash. A consumer that
    kept per-consumer state keyed by anything other than the message's own
    idempotency key would collapse them or mix their fields.
    """
    other_plan = copy.deepcopy(b_plan())
    other_plan["executionSnapshot"]["immutableStrategyVersion"]["snapshotHash"] = (
        "sha256:" + "5" * 64
    )
    other_plan["planChecksum"] = compute_compiled_plan_checksum(other_plan)
    other_request = release_request_for(other_plan)

    harness = Harness(
        persistence=persistence,
        sqs=sqs,
        queues=queues,
        plans={
            b_plan()["planChecksum"]: b_plan(),
            other_plan["planChecksum"]: other_plan,
        },
    )

    for _ in range(2):
        harness.publish(b_request())
        harness.publish(other_request)

    outcomes = harness.drain(expected=4)

    created = [
        item for item in outcomes if item.disposition is IntakeDisposition.ACCEPTED_CREATED
    ]
    duplicates = [
        item for item in outcomes if item.disposition is IntakeDisposition.ACCEPTED_DUPLICATE
    ]
    assert len(created) == 2
    assert len(duplicates) == 2

    other_run_id = run_id_for(other_request["metadata"]["idempotencyKey"])
    rows = _runs(admin_engine)
    assert len(rows) == 2
    assert {row["id"] for row in rows} == {B_RUN_ID, other_run_id}
    assert {row["idempotency_key"] for row in rows} == {
        B_IDEMPOTENCY_KEY,
        other_request["metadata"]["idempotencyKey"],
    }
    # Different releases, different pinned inputs: the two rows must not agree.
    assert len({row["configuration_hash"] for row in rows}) == 2
    # Both runs are addressed to the same bot, which is the case a bot-keyed
    # consumer would get wrong.
    assert {row["bot_id"] for row in rows} == {B_BOT_ID}
    assert sorted(job["backtestRunId"] for job in harness.jobs()) == sorted(
        [str(B_RUN_ID), str(other_run_id)]
    )


# ===========================================================================
# Rejections: none of them may leave a row behind
# ===========================================================================


def test_a_mismatched_expected_snapshot_hash_is_dead_lettered_and_writes_nothing(
    harness: Harness, admin_engine: Engine
) -> None:
    tampered = copy.deepcopy(b_request())
    tampered["expectedSnapshotHash"] = "sha256:" + "9" * 64
    tampered["metadata"]["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type=OFFICIAL_BACKTEST_MESSAGE_TYPE,
        aggregate_id=tampered["botId"],
        snapshot_hash=tampered["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(tampered),
    )

    harness.publish(tampered)
    outcome = harness.drain(expected=1)[0]

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"
    assert sql_scalar(admin_engine, "SELECT count(*) FROM backtest.runs") == 0

    letters = harness.dead_letters()
    assert len(letters) == 1
    attributes = letters[0]["MessageAttributes"]
    assert attributes["DeadLetterReason"]["StringValue"] == "CONTRACT_VIOLATION"
    assert attributes["ConsumerId"]["StringValue"] == "d91-e2e-intake"
    # The body is preserved byte for byte so the message can be re-driven.
    assert json.loads(letters[0]["Body"]) == tampered


def test_a_mismatched_compiled_plan_checksum_is_rejected_and_writes_nothing(
    harness: Harness, admin_engine: Engine
) -> None:
    tampered = copy.deepcopy(b_request())
    tampered["compiledPlanChecksum"] = "sha256:" + "9" * 64

    harness.publish(tampered)
    outcome = harness.drain(expected=1)[0]

    assert outcome.disposition is not IntakeDisposition.ACCEPTED_CREATED
    assert sql_scalar(admin_engine, "SELECT count(*) FROM backtest.runs") == 0


def test_the_unsupported_version_fixture_is_rejected_without_substitution(
    harness: Harness, admin_engine: Engine
) -> None:
    """B publishes this fixture so consumers can prove they refuse it."""
    from test_contracts import strategy_bot_fixture

    document = strategy_bot_fixture("official-backtest-request.unsupported-version.json")
    assert document["metadata"]["contractVersion"] == "strategy-bot.v999"

    harness.publish(document)
    outcome = harness.drain(expected=1)[0]

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "UNSUPPORTED_CONTRACT_VERSION"
    assert sql_scalar(admin_engine, "SELECT count(*) FROM backtest.runs") == 0
    assert harness.dead_letters()[0]["MessageAttributes"]["DeadLetterReason"][
        "StringValue"
    ] == "UNSUPPORTED_CONTRACT_VERSION"


# ===========================================================================
# The completion result B has to be able to consume
# ===========================================================================


def test_the_release_reaches_a_terminal_canonical_status_with_an_event_b_can_consume(
    harness: Harness, admin_engine: Engine
) -> None:
    """The run B asked for ends COMPLETED, and the event says so in B's vocabulary.

    `COMPLETED` -- not the pre-rebuild `COMPLETE` -- is the canonical
    `backtest.run_status` label, and the event is re-validated against D's published
    `backtest.v1` schema here rather than trusted because D built it.
    """
    harness.publish(b_request())
    harness.drain(expected=1)

    run = harness.lifecycle.get(B_RUN_ID, owner_account_id=ACCOUNT_ID)
    started = harness.lifecycle.result_event_for(
        run,
        status="RUNNING",
        correlation_id=b_request()["metadata"]["correlationId"],
        expected_snapshot_hash=b_request()["expectedSnapshotHash"],
        execution_policy_version=B_RELEASE_POLICY.version,
        startedAt="2026-07-31T12:05:00Z",
        attempt=1,
    )
    harness.lifecycle.ingest_result(started)

    run = harness.lifecycle.get(B_RUN_ID, owner_account_id=ACCOUNT_ID)
    completed = harness.lifecycle.result_event_for(
        run,
        status="COMPLETED",
        correlation_id=b_request()["metadata"]["correlationId"],
        expected_snapshot_hash=b_request()["expectedSnapshotHash"],
        execution_policy_version=B_RELEASE_POLICY.version,
        completedAt="2026-07-31T12:40:00Z",
        attempt=1,
        resultManifestId="99999999-9999-4999-8999-999999999999",
        resultHash="sha256:" + "a" * 64,
    )
    harness.lifecycle.ingest_result(completed)

    # -- what PostgreSQL holds -------------------------------------------------
    stored = sql_one(
        admin_engine,
        "SELECT status, result_hash, result_manifest_id, retryable, missing_requirements "
        "FROM backtest.runs WHERE id = :id",
        id=B_RUN_ID,
    )
    assert stored["status"] == "COMPLETED"
    assert stored["result_hash"] == "sha256:" + "a" * 64
    assert stored["result_manifest_id"] == UUID("99999999-9999-4999-8999-999999999999")
    assert stored["retryable"] is None
    assert stored["missing_requirements"] is None

    # -- what B receives -------------------------------------------------------
    validate_backtest_result_event(completed)
    assert completed["metadata"]["contractVersion"] == "backtest.v1"
    assert completed["metadata"]["messageType"] == "BACKTEST_COMPLETED"
    # The three joins B needs to tie the result back to the release it published.
    assert completed["backtestRunId"] == str(B_RUN_ID)
    assert completed["botId"] == str(B_BOT_ID)
    assert completed["expectedSnapshotHash"] == b_request()["expectedSnapshotHash"]
    assert completed["metadata"]["correlationId"] == b_request()["metadata"]["correlationId"]
    # D93: it is unmistakably a backtest, even for a consumer that only reads fields.
    assert completed["source"] == "BACKTEST"
    assert completed["livePerformanceEligible"] is False


def test_an_unavailable_outcome_records_which_requirements_were_missing(
    harness: Harness, admin_engine: Engine
) -> None:
    """The D31 gap, end to end: `reasonCode` alone does not say what was missing."""
    harness.publish(b_request())
    harness.drain(expected=1)

    run = harness.lifecycle.get(B_RUN_ID, owner_account_id=ACCOUNT_ID)
    event = harness.lifecycle.result_event_for(
        run,
        status="UNAVAILABLE",
        correlation_id=b_request()["metadata"]["correlationId"],
        expected_snapshot_hash=b_request()["expectedSnapshotHash"],
        execution_policy_version=B_RELEASE_POLICY.version,
        decidedAt="2026-07-31T12:06:00Z",
        reasonCode="WARMUP_COVERAGE_MISSING",
        missingRequirements=[
            "00000000-0000-4000-8000-000000000301|ADJUSTED_BAR|1m",
        ],
    )
    harness.lifecycle.ingest_result(event)

    stored = sql_one(
        admin_engine,
        "SELECT status, failure_code, missing_requirements FROM backtest.runs WHERE id = :id",
        id=B_RUN_ID,
    )
    assert stored["status"] == "UNAVAILABLE"
    assert stored["failure_code"] == "WARMUP_COVERAGE_MISSING"
    assert stored["missing_requirements"] == [
        "00000000-0000-4000-8000-000000000301|ADJUSTED_BAR|1m"
    ]


class _DelegatingSqs:
    """A boto3 SQS client with exactly one method replaced.

    Used to suppress a single `delete_message` so the *queue* produces the
    redelivery. Every other call goes to LocalStack unchanged.
    """

    def __init__(self, inner: Any, **overrides: Any) -> None:
        self._inner = inner
        self._overrides = overrides

    def __getattr__(self, name: str) -> Any:
        if name in self._overrides:
            return self._overrides[name]
        return getattr(self._inner, name)
