"""The contributed `backtest.runs` columns against a real PostgreSQL 16.

`tests/test_run_outcome_detail.py` proves the lifecycle keeps the three fields.
This module proves the *database* does: the columns exist because
`db/migration-contributions/migrations/V20260805170000__backtest_run_outcome_detail.sql`
was applied to the container alongside the central bundle, they round-trip through
psycopg with their Python types intact, and the CHECK that mirrors the contract's
`minItems: 1` is enforced by PostgreSQL rather than only by the row dataclass.

Every readback here goes through the *admin* engine's raw SQL, never through the
repository that wrote the value, so a repository that returned its own argument
unchanged would not pass.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Engine, text

from backtest_engine.persistence import BacktestPersistence
from backtest_engine.persistence.rows import RunRow, RunStatus

from .support import make_run


pytestmark = pytest.mark.docker


RESULT_MANIFEST_ID = UUID("99999999-9999-4999-8999-999999999999")
RESULT_HASH = "sha256:" + "a" * 64
STARTED_AT = datetime(2026, 7, 31, 12, 5, tzinfo=timezone.utc)
DECIDED_AT = datetime(2026, 7, 31, 12, 6, tzinfo=timezone.utc)


def _queued(persistence: BacktestPersistence) -> RunRow:
    row = make_run(run_id=uuid4(), idempotency_key=f"OUTCOME_DETAIL:{uuid4()}")
    with persistence.unit_of_work() as uow:
        stored, created = uow.runs.accept(row)
    assert created is True
    return stored


def _column(engine: Engine, run_id: UUID, column: str) -> Any:
    with engine.connect() as connection:
        return connection.execute(
            text(f'SELECT "{column}" FROM backtest.runs WHERE id = :id'),
            {"id": run_id},
        ).scalar_one()


def test_the_container_really_has_the_contributed_columns(admin_engine: Engine) -> None:
    """If the contribution were not applied, every other test here would error."""
    with admin_engine.connect() as connection:
        found = {
            row[0]
            for row in connection.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'backtest' AND table_name = 'runs'"
                )
            )
        }

    assert {"result_manifest_id", "retryable", "missing_requirements"} <= found


def test_a_completed_run_stores_its_result_manifest_id(persistence: BacktestPersistence, admin_engine: Engine) -> None:
    queued = _queued(persistence)
    with persistence.unit_of_work() as uow:
        uow.runs.mark_running(queued.id, STARTED_AT)
        uow.runs.mark_completed(
            queued.id,
            DECIDED_AT,
            RESULT_HASH,
            result_manifest_id=RESULT_MANIFEST_ID,
        )

    assert _column(admin_engine, queued.id, "result_manifest_id") == RESULT_MANIFEST_ID
    assert _column(admin_engine, queued.id, "retryable") is None
    assert _column(admin_engine, queued.id, "missing_requirements") is None

    with persistence.read_only() as uow:
        reread = uow.runs.get(queued.id)
    assert reread.status is RunStatus.COMPLETED
    assert reread.result_manifest_id == RESULT_MANIFEST_ID


@pytest.mark.parametrize("retryable", [True, False])
def test_a_failed_run_stores_whether_it_is_retryable(
    persistence: BacktestPersistence, admin_engine: Engine, retryable: bool
) -> None:
    queued = _queued(persistence)
    with persistence.unit_of_work() as uow:
        uow.runs.mark_running(queued.id, STARTED_AT)
        uow.runs.mark_failed(queued.id, DECIDED_AT, "WORKER_TIMEOUT", retryable=retryable)

    assert _column(admin_engine, queued.id, "retryable") is retryable

    with persistence.read_only() as uow:
        assert uow.runs.get(queued.id).retryable is retryable


def test_an_unavailable_run_stores_the_requirement_list_in_order(
    persistence: BacktestPersistence, admin_engine: Engine
) -> None:
    queued = _queued(persistence)
    requirements = ["zeta/1m", "alpha/1m", "mu/1m"]
    with persistence.unit_of_work() as uow:
        uow.runs.mark_unavailable(
            queued.id,
            DECIDED_AT,
            "REQUIRED_DATA_MISSING",
            missing_requirements=requirements,
        )

    # jsonb preserves array order (unlike object key order), which is why the list is
    # stored as an array and not as an object.
    assert _column(admin_engine, queued.id, "missing_requirements") == requirements

    with persistence.read_only() as uow:
        reread = uow.runs.get(queued.id)
    assert reread.status is RunStatus.UNAVAILABLE
    assert reread.missing_requirements == ("zeta/1m", "alpha/1m", "mu/1m")


def test_postgresql_refuses_an_empty_requirement_list(admin_engine: Engine) -> None:
    """The CHECK is the storage half of the contract's `minItems: 1`.

    Written straight through the admin engine, bypassing `RunRow`, because the
    point is that the *database* refuses it: a future writer that does not go
    through the row dataclass must not be able to store `[]`.
    """
    row = make_run(run_id=uuid4(), idempotency_key=f"OUTCOME_DETAIL:{uuid4()}")
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO backtest.runs (id, bot_id, owner_account_id, configuration_hash, "
                "status, evaluation_start, evaluation_end, initial_cash_amount, "
                "market_rules_version, accounting_rules_version, precision_rules_version, "
                "fee_policy_id, slippage_rate_bps, buying_power_buffer_policy_id, "
                "idempotency_key, queued_at, lane, message_id, canonical_payload_hash, "
                "aggregate_sequence, execution_policy_version, idempotency_scope) "
                "VALUES (:id, :bot_id, :owner, :hash, 'QUEUED', "
                ":start, :end, :cash, :market, :accounting, :precision, :fee, :slip, :buffer, "
                ":key, :queued_at, :lane, :message_id, :payload_hash, 1, :policy, :scope)"
            ),
            {
                "id": row.id,
                "bot_id": row.bot_id,
                "owner": row.owner_account_id,
                "hash": row.configuration_hash,
                "start": row.evaluation_start,
                "end": row.evaluation_end,
                "cash": row.initial_cash_amount,
                "market": row.market_rules_version,
                "accounting": row.accounting_rules_version,
                "precision": row.precision_rules_version,
                "fee": row.fee_policy_id,
                "slip": row.slippage_rate_bps,
                "buffer": row.buying_power_buffer_policy_id,
                "key": row.idempotency_key,
                "queued_at": row.queued_at,
                "lane": row.lane,
                "message_id": row.message_id,
                "payload_hash": row.canonical_payload_hash,
                "policy": row.execution_policy_version,
                "scope": row.idempotency_scope,
            },
        )

    with (
        pytest.raises(Exception, match="runs_missing_requirements_is_a_non_empty_string_array"),
        admin_engine.begin() as connection,
    ):
        connection.execute(
            text("UPDATE backtest.runs SET missing_requirements = '[]'::jsonb WHERE id = :id"),
            {"id": row.id},
        )

    # A non-empty array of strings is accepted, so the constraint is not "always no".
    with admin_engine.begin() as connection:
        connection.execute(
            text("UPDATE backtest.runs SET missing_requirements = '[\"one\"]'::jsonb WHERE id = :id"),
            {"id": row.id},
        )
    assert _column(admin_engine, row.id, "missing_requirements") == ["one"]


def test_postgresql_refuses_a_requirement_list_of_non_strings(admin_engine: Engine) -> None:
    row = make_run(run_id=uuid4(), idempotency_key=f"OUTCOME_DETAIL:{uuid4()}")
    with admin_engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO backtest.runs (id, bot_id, owner_account_id, configuration_hash, "
                "status, evaluation_start, evaluation_end, initial_cash_amount, "
                "market_rules_version, accounting_rules_version, precision_rules_version, "
                "fee_policy_id, slippage_rate_bps, buying_power_buffer_policy_id, "
                "idempotency_key, queued_at, lane, message_id, canonical_payload_hash, "
                "aggregate_sequence, execution_policy_version, idempotency_scope) "
                "VALUES (:id, :bot_id, :owner, :hash, 'QUEUED', "
                ":start, :end, :cash, :market, :accounting, :precision, :fee, :slip, :buffer, "
                ":key, :queued_at, :lane, :message_id, :payload_hash, 1, :policy, :scope)"
            ),
            {
                "id": row.id,
                "bot_id": row.bot_id,
                "owner": row.owner_account_id,
                "hash": row.configuration_hash,
                "start": row.evaluation_start,
                "end": row.evaluation_end,
                "cash": row.initial_cash_amount,
                "market": row.market_rules_version,
                "accounting": row.accounting_rules_version,
                "precision": row.precision_rules_version,
                "fee": row.fee_policy_id,
                "slip": row.slippage_rate_bps,
                "buffer": row.buying_power_buffer_policy_id,
                "key": row.idempotency_key,
                "queued_at": row.queued_at,
                "lane": row.lane,
                "message_id": row.message_id,
                "payload_hash": row.canonical_payload_hash,
                "policy": row.execution_policy_version,
                "scope": row.idempotency_scope,
            },
        )

    with (
        pytest.raises(Exception, match="runs_missing_requirements_is_a_non_empty_string_array"),
        admin_engine.begin() as connection,
    ):
        connection.execute(
            text("UPDATE backtest.runs SET missing_requirements = '[1, 2]'::jsonb WHERE id = :id"),
            {"id": row.id},
        )


def test_the_batch_attempt_reader_groups_by_run(persistence: BacktestPersistence) -> None:
    """`list_for_runs` must return every run's own attempts, not a flat mixture."""
    from backtest_engine.persistence.rows import RunAttemptRow, WorkStatus

    first = _queued(persistence)
    second = _queued(persistence)
    with persistence.unit_of_work() as uow:
        for run_id, numbers in ((first.id, (1, 2)), (second.id, (1,))):
            for number in numbers:
                uow.attempts.claim(
                    RunAttemptRow(
                        id=uuid4(),
                        run_id=run_id,
                        attempt_number=number,
                        worker_execution_key=f"BACKTEST_RUN:{run_id}:{number}",
                        status=WorkStatus.RUNNING,
                        started_at=STARTED_AT,
                    )
                )

    with persistence.read_only() as uow:
        grouped = uow.attempts.list_for_runs([first.id, second.id])

    assert sorted(grouped) == sorted([first.id, second.id])
    assert [item.attempt_number for item in grouped[first.id]] == [1, 2]
    assert [item.attempt_number for item in grouped[second.id]] == [1]


def test_the_batch_attempt_reader_omits_runs_with_no_attempts(
    persistence: BacktestPersistence,
) -> None:
    queued = _queued(persistence)

    with persistence.read_only() as uow:
        assert uow.attempts.list_for_runs([queued.id]) == {}
        assert uow.attempts.list_for_runs([]) == {}
