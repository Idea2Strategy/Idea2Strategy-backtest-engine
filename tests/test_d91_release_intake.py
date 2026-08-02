"""D91 -- the contract-driven intake that replaces the backend's direct INSERT.

Today `ImmutableStrategyReleaseJooqCommandAdapter.saveOfficialBacktestOnce`
writes `backtest.runs` itself, from the backend, with `slippage_rate_bps`
hardcoded to the literal `5`. `DatabaseAccessPolicy` says the backend may not
write `backtest`. The replacement is the outbox row that adapter *already*
writes: `operations.outbox_messages` carries B's own
`OFFICIAL_BACKTEST_REQUESTED` payload, a relay publishes it, and
:class:`~backtest_engine.release_intake.OfficialBacktestIntake` -- this module's
subject -- turns it into a run through D's own lifecycle.

These are the decision-table tests, driven message by message through a
recording fake queue so every branch is reachable in the fast suite.
`test_d91_official_release_e2e.py` drives the same intake over a real LocalStack
queue and a real PostgreSQL 16.

Every assertion here is about *observed state*: the runs the gateway holds, the
jobs the queue holds, the messages the dead-letter queue holds. No test asserts
only the value the intake returned.
"""

from __future__ import annotations

import copy
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from backtest_engine.contracts import (
    OFFICIAL_BACKTEST_MESSAGE_TYPE,
    STRATEGY_BOT_CONTRACT_VERSION,
    compute_compiled_plan_checksum,
    compute_message_idempotency_key,
    official_backtest_operation_key,
)
from backtest_engine.execution_policy import ExecutionPolicyCatalog, et_quarter_start
from backtest_engine.lifecycle import (
    RUN_ID_NAMESPACE,
    BacktestLifecycleService,
    InMemoryBacktestJobQueue,
    InMemoryRunGateway,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
    run_id_for,
)
from backtest_engine.release_intake import (
    IntakeConfig,
    IntakeDisposition,
    OfficialBacktestIntake,
)
from d_reproducibility_testkit import (
    ACCOUNT_ID,
    E2E_EXECUTION_POLICY,
    dataset_manifest,
    market_bars_parquet,
    policy_with,
)
from test_contracts import strategy_bot_fixture


# ---------------------------------------------------------------------------
# B's published release, unmodified
# ---------------------------------------------------------------------------

#: B's own fixture ids. Not re-addressed anywhere in this module: the whole point
#: of D91 is that B's published bytes are the thing that works.
B_BOT_ID = UUID("00000000-0000-4000-8000-000000000201")
B_DATASET_MANIFEST_ID = UUID("00000000-0000-4000-8000-000000000203")
B_IDEMPOTENCY_KEY = (
    "sha256:c6dd5229151352a530ff8312f050258107370cf26ea943c68473bf81936f6c1e"
)
B_PLAN_CHECKSUM = (
    "sha256:88d61198d46dce161c2a929702a7fd1cee5c9b044c470d2590b96f3825fcacb3"
)
B_SNAPSHOT_HASH = "sha256:" + "1" * 64

#: B's fixture is released at 2026-07-31T12:00:00Z, which is ET 2026-Q3.
B_RELEASE_QUARTER = "2026-Q3"

#: `uuid5(RUN_ID_NAMESPACE, B_IDEMPOTENCY_KEY)`, derived from the two literals
#: below rather than from a run this suite produced. `run_id_for` is a pure
#: function of them, so pinning the output pins the addressing scheme: changing
#: the namespace would re-address every run B has ever requested, and this
#: literal is what makes that impossible to do by accident.
B_RUN_ID = UUID("f876f259-4158-5a9a-8973-db21764024dc")
EXPECTED_RUN_ID_NAMESPACE = UUID("a8eac5b9-0335-5d8c-b32a-1d969dec25ac")


def b_request() -> dict[str, Any]:
    """B's published `OFFICIAL_BACKTEST_REQUESTED`, byte-source unmodified."""
    return strategy_bot_fixture("official-backtest-request.valid.json")


def b_plan() -> dict[str, Any]:
    return strategy_bot_fixture("basic-compiled-plan.valid.json")


B_RELEASE_POLICY = policy_with(
    version="official-backtest-policy-2026q3",
    release_quarter=B_RELEASE_QUARTER,
    period_start=et_quarter_start(2026, 3),
    period_end=et_quarter_start(2026, 4),
)


def b_dataset_manifest() -> dict[str, Any]:
    """A `market-data.v1` manifest for the id B's request names.

    The bar bytes are the pinned fixture's; only the manifest's declared window
    moves, because `ParquetMarketDataReader` requires it to equal the execution
    policy's quarter and B's release is 2026-Q3.
    """
    parquet = market_bars_parquet()
    manifest = dataset_manifest(
        hashlib.sha256(parquet).hexdigest(),
        row_count=20,
        coverage_end=datetime(2024, 1, 2, 14, 50, tzinfo=timezone.utc),
    )
    manifest["manifest_id"] = str(B_DATASET_MANIFEST_ID)
    manifest["period_start"] = "2026-07-01T04:00:00Z"
    manifest["period_end"] = "2026-10-01T04:00:00Z"
    return manifest


# ---------------------------------------------------------------------------
# A recording fake queue
# ---------------------------------------------------------------------------


class RecordingSqs:
    """The four SQS calls the intake makes, recorded rather than performed.

    Faithful to the semantics the intake depends on and to nothing else:
    `receive_message` serves what was scripted, `delete_message` removes it,
    `change_message_visibility` records the timeout, `send_message` appends to
    the dead-letter list. LocalStack covers the real thing in the e2e module.
    """

    def __init__(self) -> None:
        self.inbox: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self.visibility: list[tuple[str, int]] = []
        self.dead_letters: list[dict[str, Any]] = []

    def enqueue(self, body: Any, *, message_id: str, receive_count: int = 1) -> dict[str, Any]:
        message = {
            "MessageId": message_id,
            "ReceiptHandle": f"receipt-{message_id}-{receive_count}",
            "Body": body if isinstance(body, str) else json.dumps(body, sort_keys=True),
            "Attributes": {"ApproximateReceiveCount": str(receive_count)},
        }
        self.inbox.append(message)
        return message

    def receive_message(self, **kwargs: Any) -> Mapping[str, Any]:
        limit = int(kwargs.get("MaxNumberOfMessages", 1))
        served, self.inbox = self.inbox[:limit], self.inbox[limit:]
        return {"Messages": served}

    def delete_message(self, **kwargs: Any) -> Any:
        self.deleted.append(str(kwargs["ReceiptHandle"]))

    def change_message_visibility(self, **kwargs: Any) -> Any:
        self.visibility.append((str(kwargs["ReceiptHandle"]), int(kwargs["VisibilityTimeout"])))

    def send_message(self, **kwargs: Any) -> Any:
        self.dead_letters.append(dict(kwargs))

    def dead_letter_reasons(self) -> list[str]:
        return [
            item["MessageAttributes"]["DeadLetterReason"]["StringValue"]
            for item in self.dead_letters
        ]


INTAKE_CONFIG = IntakeConfig(
    queue_url="https://sqs.test/release-intake",
    dead_letter_queue_url="https://sqs.test/release-intake-dlq",
    consumer_id="d91-intake-1",
    max_receive_count=3,
    visibility_timeout=timedelta(seconds=30),
    wait_time=timedelta(seconds=0),
    max_messages=1,
)


class Harness:
    """One lifecycle, one intake, and the state both of them write into."""

    def __init__(
        self,
        *,
        plans: Mapping[str, Mapping[str, Any]] | None = None,
        manifests: Mapping[UUID, Mapping[str, Any]] | None = None,
        owners: Mapping[UUID, UUID] | None = None,
        policies: ExecutionPolicyCatalog | None = None,
    ) -> None:
        plan = b_plan()
        self.gateway = InMemoryRunGateway()
        self.jobs = InMemoryBacktestJobQueue()
        self.client = RecordingSqs()
        self.lifecycle = BacktestLifecycleService(
            gateway=self.gateway,
            queue=self.jobs,
            owners=StaticOwnerDirectory(
                dict(owners) if owners is not None else {B_BOT_ID: ACCOUNT_ID}
            ),
            plans=StaticCompiledPlanSource(
                dict(plans) if plans is not None else {plan["planChecksum"]: plan}
            ),
            manifests=StaticDatasetManifestSource(
                dict(manifests)
                if manifests is not None
                else {B_DATASET_MANIFEST_ID: b_dataset_manifest()}
            ),
            policies=policies if policies is not None else ExecutionPolicyCatalog([B_RELEASE_POLICY]),
        )
        self.intake = OfficialBacktestIntake(
            client=self.client, config=INTAKE_CONFIG, lifecycle=self.lifecycle
        )

    def deliver(self, body: Any, *, message_id: str = "m-1", receive_count: int = 1) -> Any:
        self.client.enqueue(body, message_id=message_id, receive_count=receive_count)
        outcomes = self.intake.poll_once()
        assert len(outcomes) == 1, outcomes
        return outcomes[0]

    @property
    def runs(self) -> list[Any]:
        """Every stored run, read back through the gateway's own query path."""
        return list(self.gateway.list_by_owner(ACCOUNT_ID, limit=100, offset=0))


@pytest.fixture
def harness() -> Harness:
    return Harness()


# ===========================================================================
# The normal path: exactly one run per release
# ===========================================================================


def test_the_run_id_namespace_and_derivation_are_pinned() -> None:
    assert RUN_ID_NAMESPACE == EXPECTED_RUN_ID_NAMESPACE
    assert run_id_for(B_IDEMPOTENCY_KEY) == B_RUN_ID
    # A different release addresses a different run; the derivation is not a constant.
    assert run_id_for(B_IDEMPOTENCY_KEY.replace("c6dd", "c6de")) != B_RUN_ID


def test_bs_published_release_message_creates_exactly_one_run(harness: Harness) -> None:
    outcome = harness.deliver(b_request())

    assert outcome.disposition is IntakeDisposition.ACCEPTED_CREATED
    assert outcome.backtest_run_id == B_RUN_ID
    assert len(harness.runs) == 1
    stored = harness.runs[0]
    assert stored.id == B_RUN_ID
    assert stored.bot_id == B_BOT_ID
    assert stored.owner_account_id == ACCOUNT_ID
    assert stored.idempotency_key == B_IDEMPOTENCY_KEY
    assert stored.status.value == "QUEUED"
    # Exactly one job, addressed to that run.
    assert [job["backtestRunId"] for job in harness.jobs.messages] == [str(B_RUN_ID)]
    assert harness.client.deleted == ["receipt-m-1-1"]
    assert harness.client.dead_letters == []


def test_the_run_takes_its_slippage_from_the_policy_not_from_a_literal(
    harness: Harness,
) -> None:
    """The backend's SQL hardcodes `slippage_rate_bps` to `5`; D reads the policy.

    Two policies, two different values, from the same message. A build that
    hardcoded 5 -- as the backend adapter does today -- passes the first
    assertion and fails the second.
    """
    harness.deliver(b_request())
    assert harness.runs[0].slippage_rate_bps == B_RELEASE_POLICY.slippage_rate_bps == 5

    other = Harness(
        policies=ExecutionPolicyCatalog([policy_with(
            version="official-backtest-policy-2026q3-slip7",
            release_quarter=B_RELEASE_QUARTER,
            period_start=et_quarter_start(2026, 3),
            period_end=et_quarter_start(2026, 4),
            slippage_rate_bps=7,
        )])
    )
    other.deliver(b_request())

    assert other.runs[0].slippage_rate_bps == 7


def test_the_run_pins_every_policy_column_the_canonical_row_declares(
    harness: Harness,
) -> None:
    harness.deliver(b_request())
    stored = harness.runs[0]

    assert stored.market_rules_version == "market:1.0.0"
    assert stored.accounting_rules_version == "accounting:1.0.0"
    assert stored.precision_rules_version == "precision:1.0.0"
    assert str(stored.fee_policy_id) == B_RELEASE_POLICY.fee_policy_id
    assert str(stored.buying_power_buffer_policy_id) == (
        B_RELEASE_POLICY.buying_power_buffer_policy_id
    )
    assert stored.evaluation_start.isoformat() == "2026-07-01"
    assert stored.evaluation_end.isoformat() == "2026-10-01"


# ===========================================================================
# At-least-once delivery
# ===========================================================================


def test_a_redelivery_of_the_same_message_creates_no_second_run(harness: Harness) -> None:
    """SQS Standard guarantees duplicates. One release, one run, forever."""
    first = harness.deliver(b_request(), message_id="m-1", receive_count=1)
    second = harness.deliver(b_request(), message_id="m-1", receive_count=2)

    assert first.disposition is IntakeDisposition.ACCEPTED_CREATED
    assert second.disposition is IntakeDisposition.ACCEPTED_DUPLICATE
    assert second.backtest_run_id == B_RUN_ID
    assert len(harness.runs) == 1
    # And no second job: a duplicate must not schedule a second execution either.
    assert len(harness.jobs.messages) == 1
    assert harness.client.deleted == ["receipt-m-1-1", "receipt-m-1-2"]


def test_a_different_message_id_carrying_the_same_release_is_still_one_run(
    harness: Harness,
) -> None:
    """Redrive and replay produce new `MessageId`s; identity is B's own key."""
    harness.deliver(b_request(), message_id="m-1")
    harness.deliver(b_request(), message_id="m-2")

    assert len(harness.runs) == 1
    assert len(harness.jobs.messages) == 1


def test_two_distinct_releases_do_not_share_a_run(harness: Harness) -> None:
    other_plan = copy.deepcopy(b_plan())
    other_plan["executionSnapshot"]["immutableStrategyVersion"]["snapshotHash"] = (
        "sha256:" + "5" * 64
    )
    other_plan["planChecksum"] = compute_compiled_plan_checksum(other_plan)
    other = release_request_for(other_plan)
    harness.lifecycle.plans = StaticCompiledPlanSource(
        {b_plan()["planChecksum"]: b_plan(), other_plan["planChecksum"]: other_plan}
    )

    harness.deliver(b_request(), message_id="m-1")
    harness.deliver(other, message_id="m-2")

    assert len(harness.runs) == 2
    assert {str(run.id) for run in harness.runs} == {
        str(B_RUN_ID),
        str(run_id_for(other["metadata"]["idempotencyKey"])),
    }
    assert len({run.configuration_hash for run in harness.runs}) == 2
    assert len(harness.jobs.messages) == 2


def release_request_for(plan: Mapping[str, Any]) -> dict[str, Any]:
    """B's message for a different release of the same bot, keyed B's own way."""
    request = copy.deepcopy(b_request())
    request["compiledPlanChecksum"] = plan["planChecksum"]
    request["expectedSnapshotHash"] = plan["executionSnapshot"]["immutableStrategyVersion"][
        "snapshotHash"
    ]
    request["metadata"]["messageId"] = "00000000-0000-4000-8000-000000000214"
    request["metadata"]["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type=OFFICIAL_BACKTEST_MESSAGE_TYPE,
        aggregate_id=request["botId"],
        snapshot_hash=request["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(request),
    )
    return request


# ===========================================================================
# Rejection paths -- none of them may create a run
# ===========================================================================


def test_the_unsupported_version_fixture_is_rejected_without_substitution(
    harness: Harness,
) -> None:
    """B publishes this fixture precisely so consumers prove they refuse it."""
    document = strategy_bot_fixture("official-backtest-request.unsupported-version.json")
    assert document["metadata"]["contractVersion"] == "strategy-bot.v999"

    outcome = harness.deliver(document)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "UNSUPPORTED_CONTRACT_VERSION"
    assert harness.runs == []
    assert harness.jobs.messages == []
    assert harness.client.dead_letter_reasons() == ["UNSUPPORTED_CONTRACT_VERSION"]


def test_a_mismatched_expected_snapshot_hash_is_rejected_not_silently_accepted(
    harness: Harness,
) -> None:
    """The request resolves a real plan but names a snapshot that plan does not pin.

    The digest is re-keyed, so the message is internally consistent: only the
    cross-check against the plan can catch it. Accepting it would run the wrong
    strategy under the right release's identity.
    """
    tampered = copy.deepcopy(b_request())
    tampered["expectedSnapshotHash"] = "sha256:" + "9" * 64
    tampered["metadata"]["idempotencyKey"] = compute_message_idempotency_key(
        contract_version=STRATEGY_BOT_CONTRACT_VERSION,
        message_type=OFFICIAL_BACKTEST_MESSAGE_TYPE,
        aggregate_id=tampered["botId"],
        snapshot_hash=tampered["expectedSnapshotHash"],
        operation_key=official_backtest_operation_key(tampered),
    )
    assert tampered["compiledPlanChecksum"] == B_PLAN_CHECKSUM

    outcome = harness.deliver(tampered)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"
    assert harness.runs == []
    assert harness.jobs.messages == []


def test_a_mismatched_compiled_plan_checksum_is_rejected(harness: Harness) -> None:
    """A checksum that identifies no published plan cannot become a run."""
    tampered = copy.deepcopy(b_request())
    tampered["compiledPlanChecksum"] = "sha256:" + "9" * 64

    outcome = harness.deliver(tampered)

    assert outcome.disposition is not IntakeDisposition.ACCEPTED_CREATED
    assert harness.runs == []
    assert harness.jobs.messages == []


def test_a_plan_whose_own_checksum_does_not_verify_is_rejected(harness: Harness) -> None:
    """A tampered plan reached through the plan source, not through the message."""
    forged = copy.deepcopy(b_plan())
    forged["steps"][1]["arguments"]["threshold"] = "70"
    harness.lifecycle.plans = StaticCompiledPlanSource({B_PLAN_CHECKSUM: forged})

    outcome = harness.deliver(b_request())

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"
    assert harness.runs == []


def test_a_forged_idempotency_key_is_rejected(harness: Harness) -> None:
    tampered = copy.deepcopy(b_request())
    tampered["metadata"]["idempotencyKey"] = "sha256:" + "0" * 64

    outcome = harness.deliver(tampered)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "CONTRACT_VIOLATION"
    assert harness.runs == []


def test_a_message_that_is_not_json_is_dead_lettered_not_retried(harness: Harness) -> None:
    outcome = harness.deliver("not json at all")

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "MESSAGE_NOT_PARSEABLE"
    assert harness.runs == []


def test_a_different_strategy_bot_message_type_is_not_treated_as_a_release(
    harness: Harness,
) -> None:
    """`bot-run-command` travels the same contract version; it is not a backtest."""
    command = strategy_bot_fixture("bot-run-command.valid.json")

    outcome = harness.deliver(command)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "UNEXPECTED_MESSAGE_TYPE"
    assert harness.runs == []


def test_a_release_for_an_unknown_bot_is_retried_then_dead_lettered() -> None:
    """An unresolved owner may be a replication lag; it is not poison on delivery 1."""
    harness = Harness(owners={})

    first = harness.deliver(b_request(), message_id="m-1", receive_count=1)
    second = harness.deliver(b_request(), message_id="m-1", receive_count=2)
    third = harness.deliver(b_request(), message_id="m-1", receive_count=3)

    assert [item.disposition for item in (first, second)] == [
        IntakeDisposition.RETURNED,
        IntakeDisposition.RETURNED,
    ]
    assert first.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert harness.client.visibility == [("receipt-m-1-1", 0), ("receipt-m-1-2", 0)]
    assert third.disposition is IntakeDisposition.DEAD_LETTERED
    assert third.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert harness.runs == []


def test_a_delivery_past_the_receive_budget_is_dead_lettered(harness: Harness) -> None:
    outcome = harness.deliver(b_request(), receive_count=4)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "MAX_RECEIVE_COUNT_EXCEEDED"
    assert harness.runs == []


def test_an_execution_policy_gap_does_not_invent_one() -> None:
    """No policy for B's release quarter means no run, not the nearest quarter."""
    harness = Harness(policies=ExecutionPolicyCatalog([E2E_EXECUTION_POLICY]))

    outcome = harness.deliver(b_request(), receive_count=3)

    assert outcome.disposition is IntakeDisposition.DEAD_LETTERED
    assert outcome.reason_code == "REQUIRED_INPUT_UNAVAILABLE"
    assert harness.runs == []


def test_a_dead_letter_carries_enough_context_to_triage(harness: Harness) -> None:
    harness.deliver("not json at all")

    sent = harness.client.dead_letters[0]
    attributes = sent["MessageAttributes"]
    assert sent["QueueUrl"] == INTAKE_CONFIG.dead_letter_queue_url
    assert sent["MessageBody"] == "not json at all"
    assert attributes["DeadLetterReason"]["StringValue"] == "MESSAGE_NOT_PARSEABLE"
    assert attributes["SourceQueueUrl"]["StringValue"] == INTAKE_CONFIG.queue_url
    assert attributes["ConsumerId"]["StringValue"] == "d91-intake-1"


# ===========================================================================
# The ownership violation this card must surface, quoted against the source
# ===========================================================================


def backend_adapter_source() -> str | None:
    override = os.environ.get("IDEA2STRATEGY_BACKEND_PERSISTENCE")
    suffix = Path(
        "backend/modules/backend-persistence/src/main/java/com/idea2strategy/backend/"
        "persistence/strategy/ImmutableStrategyReleaseJooqCommandAdapter.java"
    )
    candidates = [Path(override)] if override else []
    if not candidates:
        for ancestor in Path(__file__).resolve().parents:
            candidates.extend([ancestor / suffix, ancestor / "Idea2Strategy" / suffix])
    for candidate in candidates:
        if candidate.is_file():
            return candidate.read_text(encoding="utf-8")
    return None


def test_the_backend_still_writes_backtest_runs_directly() -> None:
    """A tripwire, not a fix. It fails the day the backend change lands.

    When it does, delete this test and this repository's D91 note along with it:
    the point of a tripwire is that it goes away.
    """
    source = backend_adapter_source()
    if source is None:
        pytest.skip(
            "the backend submodule is not checked out beside this worktree, so the "
            "upstream half of the D91 ownership violation cannot be re-read here. "
            "The D-side replacement is covered unconditionally by the tests above."
        )

    assert "insert into backtest.runs " in source
    assert "on conflict (idempotency_key) do nothing" in source
    # The literal `5` the backend substitutes for `slippage_rate_bps`.
    assert (
        "values (?, ?, ?, ?, 'QUEUED', ?::date, ?::date, ?, ?, ?, ?, ?, 5, ?, ?, "
    ) in source
    # And the outbox row that makes the replacement possible without new contracts.
    assert "insert into operations.outbox_messages " in source
