"""D93 -- a backtest result must never reach E's live room ranking or performance.

E owns the `competition` and `performance` schemas and the `room-performance.v1`
contract. The isolation this card requires is proved from **both** directions,
because either one alone is a half proof:

*D cannot write into E's world.*
    The persistence layer has no table object in `competition` or `performance`,
    `contribution.writable_schemas()` does not admit them, and
    `persistence.engine.check_statement` -- the guard `install_runtime_guards`
    installs on every engine `create_backtest_engine` builds -- refuses the
    statement before the driver sees it. The Docker test additionally records
    **every** statement a real end-to-end run executes and asserts the set of
    schemas it touched.

*E cannot mistake D's output for live input.*
    Every `backtest.v1` result event D publishes carries `source = "BACKTEST"`,
    `eventType = "BACKTEST_RESULT"` and `livePerformanceEligible = false`, and
    the schema pins all three as constants, so D cannot even construct an event
    that claims to be live. Those are exactly the field values in E's own
    `room-performance/v1/live-performance-input.backtest-rejected.json`, whose
    `expectedDecision` is `BACKTEST_SOURCE_NOT_ALLOWED`. D's output therefore
    matches, field for field, the input E has declared it rejects.

The second direction is the one a marker-free design gets wrong: an event whose
"it is a backtest" fact is implied by the topic it arrived on stops being a fact
the moment somebody bridges two topics.
"""

from __future__ import annotations

import ast
import copy
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

from backtest_engine.contracts import (
    BACKTEST_RESULT_EVENT_TYPE,
    BACKTEST_RESULT_SOURCE,
    LIVE_PERFORMANCE_ELIGIBLE,
    ContractValidationError,
    build_backtest_result_event,
    validate_backtest_result_event,
)
from backtest_engine.persistence import METADATA, SchemaWriteForbidden, check_statement, load_contribution
from backtest_engine.persistence.engine import READABLE_SCHEMAS


#: The schemas E owns. `db/schema.dbml` and `DatabaseAccessPolicy.SCHEMA_OWNERS`
#: both register them to the backend; D may not write either, ever.
E_OWNED_SCHEMAS = ("competition", "performance")

# E's discriminators, transcribed from its published `room-performance/v1`
# fixtures. They are literals here for the same reason B's checksums are
# literals in `test_contracts.py`: this repository must be verifiable from a bare
# clone, so the primary assertions run against the transcription and a separate,
# clearly-reported guard re-reads E's single copy when the superproject is
# reachable. Nothing is vendored: no copy of E's fixture lives in this repo.
#
#   live-performance-input.backtest-rejected.json -> refused
E_REJECTED_SOURCE = "BACKTEST"
E_REJECTED_EVENT_TYPE = "BACKTEST_RESULT"
E_BACKTEST_REJECTION_DECISION = "BACKTEST_SOURCE_NOT_ALLOWED"
#   live-performance-input.valid.json             -> accepted into live scoring
E_LIVE_SOURCE = "LIVE_TRADING"
E_LIVE_EVENT_TYPE = "FILL"

RUN_ID = "77777777-7777-4777-8777-777777777777"
BOT_ID = "00000000-0000-4000-8000-000000000201"
OWNER_ACCOUNT_ID = "66666666-6666-4666-8666-666666666666"
SNAPSHOT_HASH = "sha256:" + "1" * 64
INPUT_BUNDLE_FINGERPRINT = "sha256:" + "e" * 64
RESULT_HASH = "sha256:" + "a" * 64

EVERY_STATUS: tuple[tuple[str, dict[str, Any]], ...] = (
    ("QUEUED", {"queuedAt": "2024-01-03T01:05:01Z"}),
    ("RUNNING", {"startedAt": "2024-01-03T01:06:00Z", "attempt": 1}),
    (
        "COMPLETED",
        {
            "completedAt": "2024-01-03T01:10:00Z",
            "attempt": 1,
            "resultManifestId": "99999999-9999-4999-8999-999999999999",
            "resultHash": RESULT_HASH,
        },
    ),
    (
        "FAILED",
        {
            "failedAt": "2024-01-03T01:07:00Z",
            "attempt": 2,
            "failureCode": "WORKER_TIMEOUT",
            "retryable": True,
        },
    ),
    (
        "UNAVAILABLE",
        {
            "decidedAt": "2024-01-03T01:05:02Z",
            "reasonCode": "REQUIRED_DATA_MISSING",
            "missingRequirements": ["resolution:1m"],
        },
    ),
)


def result_event(status: str, **detail: Any) -> dict[str, Any]:
    return build_backtest_result_event(
        status=status,
        backtest_run_id=RUN_ID,
        bot_id=BOT_ID,
        owner_account_id=OWNER_ACCOUNT_ID,
        expected_snapshot_hash=SNAPSHOT_HASH,
        input_bundle_fingerprint=INPUT_BUNDLE_FINGERPRINT,
        execution_policy_version="official-backtest-policy-v1",
        message_id="90000000-0000-4000-8000-000000000001",
        occurred_at="2024-01-03T01:05:01Z",
        correlation_id="55555555-5555-4555-8555-555555555555",
        **detail,
    )


def locate_room_performance_contracts() -> Path | None:
    """E's single copy of its `room-performance.v1` fixtures, if reachable.

    Never vendored here. E is the producer of that contract and a second copy is
    exactly the drift the rebuild spec records in section 1.
    """
    override = os.environ.get("IDEA2STRATEGY_ROOM_PERFORMANCE_CONTRACTS")
    if override:
        candidate = Path(override)
        return candidate if candidate.is_dir() else None
    suffix = Path(
        "backend/modules/backend-messaging/src/main/resources/contracts/"
        "room-performance/v1"
    )
    for ancestor in Path(__file__).resolve().parents:
        for candidate in (ancestor / suffix, ancestor / "Idea2Strategy" / suffix):
            if candidate.is_dir():
                return candidate
    return None


# ===========================================================================
# Direction 1 -- D cannot write into E's schemas
# ===========================================================================


def test_the_declared_write_set_admits_neither_of_es_schemas() -> None:
    writable = load_contribution().writable_schemas()

    assert writable == frozenset({"backtest", "storage"})
    for schema in E_OWNED_SCHEMAS:
        assert schema not in writable


def test_the_declared_read_set_admits_neither_of_es_schemas() -> None:
    """D reads `market_data` and `strategy`; it has no reason to read E at all."""
    assert READABLE_SCHEMAS == frozenset({"backtest", "storage", "market_data", "strategy"})
    for schema in E_OWNED_SCHEMAS:
        assert schema not in READABLE_SCHEMAS


@pytest.mark.parametrize("schema", E_OWNED_SCHEMAS)
@pytest.mark.parametrize(
    "statement_template",
    [
        "INSERT INTO {schema}.room_results (id, score) VALUES (1, 2)",
        "UPDATE {schema}.room_results SET score = 1 WHERE id = 2",
        "DELETE FROM {schema}.room_results WHERE id = 2",
        'MERGE INTO "{schema}"."room_results" USING x ON (1 = 1)',
        "/* backtest publish */ INSERT INTO {schema}.room_results (id) VALUES (1)",
    ],
)
def test_the_runtime_guard_refuses_every_write_shape_into_es_schemas(
    schema: str, statement_template: str
) -> None:
    """The guard reads the final SQL text, so `text()` cannot slip past it."""
    writable = load_contribution().writable_schemas()

    with pytest.raises(SchemaWriteForbidden, match=schema):
        check_statement(statement_template.format(schema=schema), writable)


def test_the_persistence_layer_declares_no_table_in_es_schemas() -> None:
    """Not "we do not write it" but "there is nothing to write with"."""
    schemas = {table.schema for table in METADATA.sorted_tables}

    assert schemas == {"backtest", "storage"}
    for schema in E_OWNED_SCHEMAS:
        assert schema not in schemas


def test_no_literal_sql_in_the_package_writes_outside_the_declared_schemas() -> None:
    """Every SQL string the package carries is checked by production's own guard.

    `check_statement` is the function `install_runtime_guards` installs, so this
    reuses the real parser rather than a second, quietly different one. Scanning
    string *constants* through `ast` rather than raw lines keeps prose out of it:
    `backtest.performance_summaries` is D's own table and a docstring mentioning
    `room-performance.v1` is not SQL.
    """
    package = Path(__file__).resolve().parents[1] / "src" / "backtest_engine"
    writable = load_contribution().writable_schemas()
    sql_like = re.compile(r"^\s*(?:/\*|--|INSERT|UPDATE|DELETE|MERGE|SELECT|WITH)\b", re.I)

    checked = 0
    offenders: list[str] = []
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            if not sql_like.match(node.value):
                continue
            checked += 1
            try:
                check_statement(node.value, writable)
            except SchemaWriteForbidden as exc:
                offenders.append(f"{path.name}:{node.lineno}: {exc}")

    # The scan has to have found something, or it proves nothing.
    assert checked > 0, "no SQL string literals were scanned; the detector is broken"
    assert offenders == []


# ===========================================================================
# Direction 2 -- E cannot mistake D's output for live input
# ===========================================================================


@pytest.mark.parametrize(("status", "detail"), EVERY_STATUS, ids=[item[0] for item in EVERY_STATUS])
def test_every_published_result_event_declares_itself_a_backtest(
    status: str, detail: dict[str, Any]
) -> None:
    event = result_event(status, **detail)

    assert event["source"] == "BACKTEST"
    assert event["eventType"] == "BACKTEST_RESULT"
    assert event["livePerformanceEligible"] is False
    validate_backtest_result_event(event)


def test_the_markers_are_module_constants_not_per_call_arguments() -> None:
    assert BACKTEST_RESULT_SOURCE == "BACKTEST"
    assert BACKTEST_RESULT_EVENT_TYPE == "BACKTEST_RESULT"
    assert LIVE_PERFORMANCE_ELIGIBLE is False


@pytest.mark.parametrize(
    ("field", "forged"),
    [
        ("source", "LIVE_TRADING"),
        ("eventType", "FILL"),
        ("livePerformanceEligible", True),
    ],
)
def test_a_result_event_that_claims_to_be_live_is_refused(field: str, forged: Any) -> None:
    """D cannot construct, and will not accept, an event that hides its origin."""
    event = result_event("COMPLETED", **dict(EVERY_STATUS[2][1]))
    event[field] = forged

    with pytest.raises(ContractValidationError, match=field):
        validate_backtest_result_event(event)


@pytest.mark.parametrize("field", ["source", "eventType", "livePerformanceEligible"])
def test_a_result_event_that_drops_the_marker_is_refused(field: str) -> None:
    """Silence is not permission: an absent marker is a contract violation."""
    event = result_event("COMPLETED", **dict(EVERY_STATUS[2][1]))
    del event[field]

    with pytest.raises(ContractValidationError, match=field):
        validate_backtest_result_event(event)


def test_a_caller_cannot_override_the_marker_through_the_detail_kwargs() -> None:
    with pytest.raises(ContractValidationError, match="source"):
        result_event(
            "COMPLETED",
            completedAt="2024-01-03T01:10:00Z",
            attempt=1,
            resultManifestId="99999999-9999-4999-8999-999999999999",
            resultHash=RESULT_HASH,
            source="LIVE_TRADING",
        )


# ===========================================================================
# The two directions meet: D's output is what E declared it rejects
# ===========================================================================


@pytest.mark.parametrize(("status", "detail"), EVERY_STATUS, ids=[item[0] for item in EVERY_STATUS])
def test_ds_output_carries_exactly_the_source_e_refuses(
    status: str, detail: dict[str, Any]
) -> None:
    """Isolation verified from both ends, against E's transcribed discriminators.

    Unconditional: it does not need E's checkout, because the two values it
    compares against are the ones E published and this module transcribed.
    `test_es_own_fixture_still_declares_the_discriminators_this_module_transcribed`
    is the guard that the transcription is still true.
    """
    event = result_event(status, **detail)

    assert event["source"] == E_REJECTED_SOURCE
    assert event["eventType"] == E_REJECTED_EVENT_TYPE
    assert event["source"] != E_LIVE_SOURCE
    assert event["eventType"] != E_LIVE_EVENT_TYPE


def room_performance_fixture(name: str) -> dict[str, Any] | None:
    root = locate_room_performance_contracts()
    if root is None:
        return None
    return json.loads((root / name).read_text(encoding="utf-8"))


def test_es_own_fixture_still_declares_the_discriminators_this_module_transcribed() -> None:
    """Drift guard against E's single copy. Skips only when E is unreachable.

    A skip here means the *transcription* is unverified for this run; the
    isolation assertions above still ran. The skip reason says so explicitly so
    it cannot read as coverage.
    """
    rejected = room_performance_fixture("live-performance-input.backtest-rejected.json")
    accepted = room_performance_fixture("live-performance-input.valid.json")
    if rejected is None or accepted is None:
        pytest.skip(
            "NOT COVERED: E's room-performance/v1 fixtures are unreachable from a "
            "bare clone, so this run did not confirm that E still rejects "
            f"source={E_REJECTED_SOURCE!r}/eventType={E_REJECTED_EVENT_TYPE!r}. "
            "The D-side isolation assertions ran regardless. Set "
            "IDEA2STRATEGY_ROOM_PERFORMANCE_CONTRACTS or check out the "
            "superproject to close this gap."
        )

    assert rejected["contractVersion"] == "room-performance.v1"
    assert rejected["source"] == E_REJECTED_SOURCE
    assert rejected["eventType"] == E_REJECTED_EVENT_TYPE
    assert rejected["expectedDecision"] == E_BACKTEST_REJECTION_DECISION
    assert accepted["source"] == E_LIVE_SOURCE
    assert accepted["eventType"] == E_LIVE_EVENT_TYPE
    assert "expectedDecision" not in accepted
    differing = {
        key
        for key in set(accepted) | set(rejected)
        if accepted.get(key) != rejected.get(key)
    }
    # `eventId`, `sourceEventSequence`, `occurredAt` and `evidenceHash` differ
    # because they are two different events; the *decision* fields are these two.
    assert {"source", "eventType"} <= differing


def test_a_d_result_event_carries_no_room_or_ranking_field_at_all() -> None:
    """D does not even name E's aggregate, so nothing could be routed by it."""
    event = result_event("COMPLETED", **dict(EVERY_STATUS[2][1]))
    forbidden = {
        "roomId",
        "evaluationSegmentId",
        "anonymousBotId",
        "scheduleVersion",
        "rank",
        "ranking",
        "winner",
        "score",
    }

    assert forbidden.isdisjoint(event)
    assert forbidden.isdisjoint(event["metadata"])


def test_the_marker_does_not_move_the_idempotency_key() -> None:
    """The markers are constants, so they cannot be an addressing dimension.

    Pinned against the literal `tests/test_contracts.py` already asserts for the
    same COMPLETED event, so adding the markers provably did not re-address any
    event a consumer had already seen.
    """
    event = result_event("COMPLETED", **dict(EVERY_STATUS[2][1]))

    assert event["metadata"]["idempotencyKey"] == (
        "sha256:b9ef3969253ee8495d2e8241d891a75927e62c1d9814530f83ed07540b2e2f78"
    )


def test_removing_the_marker_from_a_stored_event_cannot_be_replayed_as_live() -> None:
    """A consumer that strips the marker breaks the event rather than laundering it."""
    event = result_event("COMPLETED", **dict(EVERY_STATUS[2][1]))
    laundered = copy.deepcopy(event)
    laundered["source"] = "LIVE_TRADING"
    laundered["eventType"] = "FILL"
    laundered["livePerformanceEligible"] = True

    with pytest.raises(ContractValidationError):
        validate_backtest_result_event(laundered)
