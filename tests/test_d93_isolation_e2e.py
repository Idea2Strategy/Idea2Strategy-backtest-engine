"""D93 against real infrastructure: what a whole official run actually touches.

The unit half of D93 (`test_d93_live_performance_isolation.py`) proves the rules.
This half proves they hold for a run that really happened: every statement the
runtime engine executed during a complete HTTP -> SQS -> worker -> PostgreSQL ->
S3 traversal is recorded and then examined.

Recording on the engine, not on the repositories, is the point. A repository-level
assertion would only cover the code paths the test knew to look at; the
`before_cursor_execute` hook sees the final SQL of *every* statement, including
anything a future adapter adds, and including `text()`.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, event, text

from backtest_engine.persistence import BacktestPersistence, SchemaWriteForbidden, check_statement
from d_integration_stack import Stack, build_stack, sql_all, sql_one
from test_d93_live_performance_isolation import (
    E_OWNED_SCHEMAS,
    E_REJECTED_EVENT_TYPE,
    E_REJECTED_SOURCE,
)


pytestmark = pytest.mark.docker


#: The two schemas a backtest run is allowed to write rows into.
D_WRITABLE_SCHEMAS = frozenset({"backtest", "storage"})

#: A schema-qualified reference in the *schema* position, quoted or not. The
#: negative lookbehind is what keeps `backtest.performance_summaries` -- D's own
#: table, whose *name* contains the word -- out of it.
#: `test_the_e_reference_detector_is_not_vacuous` pins both directions.
_E_REFERENCE = re.compile(
    r'(?<![.\w"])"?(' + "|".join(E_OWNED_SCHEMAS) + r')"?\s*\.\s*"?[a-z_]', re.I
)

_WRITE = re.compile(r"^\s*(?:INSERT|UPDATE|DELETE|MERGE)\b", re.I)
_WRITE_TARGET = re.compile(
    r"^\s*(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+(?:ONLY\s+)?"
    r'"?(?P<schema>[a-z_][a-z0-9_]*)"?\s*\.',
    re.I,
)
_CAPABILITY_WRITE_TARGET = re.compile(
    r"^\s*SELECT\s+(?:\*\s+FROM\s+)?"
    r'"?(?P<schema>backtest|storage)"?\s*\.\s*"?'
    r"(?:claim_run_attempt|heartbeat_run_attempt|close_run_attempt|"
    r"recover_expired_run_attempt|register_backtest_object|"
    r"transition_backtest_object|prepare_backtest_object_cleanup|"
    r"reissue_backtest_object_cleanup)\b",
    re.I,
)


class StatementRecorder:
    """Every statement the engine executed, in order."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def __call__(
        self,
        _conn: Any,
        _cursor: Any,
        statement: str,
        _parameters: Any,
        _context: Any,
        _executemany: bool,
    ) -> None:
        self.statements.append(statement)

    @property
    def writes(self) -> list[str]:
        return [
            item
            for item in self.statements
            if _WRITE.match(item.strip()) or _CAPABILITY_WRITE_TARGET.match(item.strip())
        ]

    def written_schemas(self) -> set[str]:
        found: set[str] = set()
        for statement in self.writes:
            match = _WRITE_TARGET.match(statement.strip()) or _CAPABILITY_WRITE_TARGET.match(
                statement.strip()
            )
            assert match is not None, f"unparsed write statement: {statement[:160]!r}"
            found.add(match.group("schema").lower())
        return found


@pytest.fixture
def recorder(runtime_engine: Engine) -> Iterator[StatementRecorder]:
    listener = StatementRecorder()
    event.listen(runtime_engine, "before_cursor_execute", listener)
    try:
        yield listener
    finally:
        event.remove(runtime_engine, "before_cursor_execute", listener)


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


def run_to_completion(stack: Stack) -> str:
    accepted = stack.accept()
    assert accepted.status_code == 202, accepted.text
    run_id: str = accepted.json()["run"]["backtestRunId"]
    handled = stack.worker.poll_once()
    assert [item.disposition.value for item in handled] == ["DELETED"], [
        item.reason_code for item in handled
    ]
    return run_id


# ===========================================================================
# Direction 1: what the run actually wrote
# ===========================================================================


def test_a_complete_official_run_writes_only_backtest_and_storage(
    stack: Stack, recorder: StatementRecorder, admin_engine: Engine
) -> None:
    run_id = run_to_completion(stack)

    # The run really completed, so the recording covers a full publish and not an
    # early failure that never reached the interesting tables.
    assert sql_one(
        admin_engine, "SELECT status FROM backtest.runs WHERE id = :id", id=run_id
    )["status"] == "COMPLETED"

    assert recorder.written_schemas() == D_WRITABLE_SCHEMAS
    # A recorder that saw nothing would satisfy the assertion above vacuously.
    assert len(recorder.writes) >= 8, recorder.writes


@pytest.mark.parametrize(
    ("statement", "is_an_e_reference"),
    [
        ("INSERT INTO competition.rooms (id) VALUES (1)", True),
        ('UPDATE "performance"."room_scores" SET x = 1', True),
        ("SELECT * FROM competition . room_participants", True),
        ("select 1 from PERFORMANCE.official_results", True),
        ("SELECT * FROM backtest.performance_summaries p WHERE p.run_id = 1", False),
        ('INSERT INTO "backtest"."performance_summaries" (run_id) VALUES (1)', False),
        ("SELECT r.id FROM backtest.runs r JOIN backtest.performance_summaries p ON p.run_id = r.id", False),
        ("SELECT status FROM backtest.runs WHERE id = 1", False),
    ],
)
def test_the_e_reference_detector_is_not_vacuous(statement: str, is_an_e_reference: bool) -> None:
    """A scan that matches nothing would pass the next test for the wrong reason."""
    assert bool(_E_REFERENCE.search(statement)) is is_an_e_reference


def test_a_complete_official_run_never_names_an_e_owned_schema_at_all(
    stack: Stack, recorder: StatementRecorder
) -> None:
    """Not even a read. D has no reason to look at a room or a live performance."""
    run_to_completion(stack)

    offenders = [
        statement for statement in recorder.statements if _E_REFERENCE.search(statement)
    ]

    assert offenders == []
    assert recorder.statements, "the recorder captured nothing"


def test_every_recorded_statement_passes_the_production_write_guard(
    stack: Stack, recorder: StatementRecorder
) -> None:
    """Re-run production's own check over the real statement stream.

    The guard already ran inline; running it again here with the schema set
    written out as a literal makes the assertion independent of whatever
    `contribution.properties` happens to say.
    """
    run_to_completion(stack)

    for statement in recorder.statements:
        check_statement(statement, D_WRITABLE_SCHEMAS)


def test_the_runtime_engine_refuses_a_write_into_es_schema_in_practice(
    runtime_engine: Engine,
) -> None:
    """The guard is installed on the real engine, not merely importable.

    `competition.rooms` exists in the migrated container, so this fails at the
    guard rather than at PostgreSQL: without the guard the statement would be a
    permission error at worst, and on a database with no role GRANTs -- which the
    canonical baseline is -- it would simply succeed.
    """
    with runtime_engine.connect() as connection, pytest.raises(SchemaWriteForbidden, match="competition"):
        connection.execute(
            text("INSERT INTO competition.rooms (id) VALUES ('00000000-0000-4000-8000-000000000999')")
        )


def test_the_container_really_has_es_tables_so_the_refusal_is_not_vacuous(
    admin_engine: Engine,
) -> None:
    """If `competition` did not exist, the test above would prove nothing."""
    tables = sql_all(
        admin_engine,
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema = ANY(:schemas) ORDER BY table_schema, table_name",
        schemas=list(E_OWNED_SCHEMAS),
    )

    present = {row["table_schema"] for row in tables}
    assert present == set(E_OWNED_SCHEMAS), tables


def test_a_completed_run_leaves_es_tables_untouched(
    stack: Stack, admin_engine: Engine
) -> None:
    """Counted before and after, on the unguarded engine E itself would use."""
    before = _e_row_counts(admin_engine)

    run_to_completion(stack)

    assert _e_row_counts(admin_engine) == before
    assert sum(before.values()) == 0


def _e_row_counts(engine: Engine) -> dict[str, int]:
    tables = sql_all(
        engine,
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_schema = ANY(:schemas) AND table_type = 'BASE TABLE' "
        "ORDER BY table_schema, table_name",
        schemas=list(E_OWNED_SCHEMAS),
    )
    counts: dict[str, int] = {}
    for row in tables:
        qualified = f"{row['table_schema']}.{row['table_name']}"
        counts[qualified] = sql_one(
            engine, f"SELECT count(*) AS n FROM {qualified}"
        )["n"]
    return counts


# ===========================================================================
# Direction 2: what the run actually published
# ===========================================================================


def test_every_event_a_real_run_published_is_marked_as_a_backtest(stack: Stack) -> None:
    """Unconditional. No branch, no skip: this is a gate-8 test.

    The cross-repository drift guard that re-reads E's own copy of the fixture is
    deliberately **not** here. It needs the superproject checked out, which CI's
    bare clone does not have, and a `pytest.skip` inside the Docker-marked suite is
    a silent pass that the gate-8 zero-skip assertion would then turn red for the
    wrong reason. That guard lives in the Docker-free
    `test_d93_live_performance_isolation.py`, where it costs nothing and where the
    repository already accepts cross-repo skips for B's contracts.
    """
    run_to_completion(stack)

    assert [event["status"] for event in stack.sink.events] == ["RUNNING", "COMPLETED"]
    for published in stack.sink.events:
        assert published["source"] == E_REJECTED_SOURCE
        assert published["eventType"] == E_REJECTED_EVENT_TYPE
        assert published["livePerformanceEligible"] is False


def test_the_marker_survives_the_http_ingestion_round_trip(stack: Stack) -> None:
    """The API stores the event it validated, so the marker is not a client-side coat."""
    run_id = run_to_completion(stack)
    completed = next(item for item in stack.sink.events if item["status"] == "COMPLETED")

    # Re-post the identical event: the endpoint recognises it, which it can only
    # do by having kept the same document -- markers included.
    replayed = stack.client.post(
        f"/api/v1/backtests/{run_id}/results",
        json=completed,
        headers={"Authorization": "Bearer d-int-worker-token", "X-Delivery-Attempt": "2"},
    )

    assert replayed.status_code == 200, replayed.text
    assert replayed.json()["applied"] is False

    stripped = {key: value for key, value in completed.items() if key != "source"}
    refused = stack.client.post(
        f"/api/v1/backtests/{run_id}/results",
        json=stripped,
        headers={"Authorization": "Bearer d-int-worker-token", "X-Delivery-Attempt": "2"},
    )
    assert refused.status_code == 422, refused.text
    assert "source" in refused.text
