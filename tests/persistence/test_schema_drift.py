"""The runtime refuses to run against a schema it does not recognise.

Because the runtime never repairs anything, the only safe response to drift is a loud
failure at startup. PostgreSQL has transactional DDL, so the drift is introduced inside
a transaction on the *unguarded* admin engine and rolled back afterwards; the shared
container is left exactly as it was.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Engine, MetaData, Table
from sqlalchemy.dialects.postgresql import UUID, VARCHAR

from backtest_engine.persistence import (
    BacktestPersistence,
    SchemaDriftError,
    describe_schema_drift,
    verify_schema,
)


pytestmark = pytest.mark.docker


def test_healthy_schema_passes(persistence: BacktestPersistence) -> None:
    persistence.verify_schema()


def test_healthy_schema_reports_no_drift(persistence: BacktestPersistence) -> None:
    with persistence.unit_of_work() as uow:
        assert describe_schema_drift(uow.connection) == []


def test_a_missing_column_fails_loudly(admin_engine: Engine) -> None:
    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql('ALTER TABLE "backtest"."runs" DROP COLUMN "result_hash"')
            with pytest.raises(SchemaDriftError, match="runs.result_hash: column is missing"):
                verify_schema(connection)
        finally:
            transaction.rollback()

    # The rollback really happened: the healthy check passes again.
    with admin_engine.connect() as connection:
        verify_schema(connection)


def test_a_missing_table_fails_loudly(admin_engine: Engine) -> None:
    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql('DROP TABLE "backtest"."failure_condition_counts"')
            with pytest.raises(SchemaDriftError, match="failure_condition_counts: table is missing"):
                verify_schema(connection)
        finally:
            transaction.rollback()


def test_a_widened_column_fails_loudly(admin_engine: Engine) -> None:
    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql(
                'ALTER TABLE "backtest"."runs" ALTER COLUMN "configuration_hash" TYPE varchar(200)'
            )
            with pytest.raises(SchemaDriftError, match=r"configuration_hash: type is varchar\(200\)"):
                verify_schema(connection)
        finally:
            transaction.rollback()


def test_a_relaxed_not_null_fails_loudly(admin_engine: Engine) -> None:
    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql('ALTER TABLE "backtest"."runs" ALTER COLUMN "result_hash" SET NOT NULL')
            with pytest.raises(SchemaDriftError, match="result_hash: nullable is False"):
                verify_schema(connection)
        finally:
            transaction.rollback()


def test_a_dropped_unique_constraint_fails_loudly(admin_engine: Engine) -> None:
    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql("DROP INDEX backtest.run_attempts_run_id_attempt_number_idx")
            with pytest.raises(SchemaDriftError, match="unique constraint"):
                verify_schema(connection)
        finally:
            transaction.rollback()


def test_a_changed_enum_label_fails_loudly(admin_engine: Engine) -> None:
    """`COMPLETE` is not `COMPLETED`; the guard must notice the difference."""

    with admin_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.exec_driver_sql("ALTER TYPE backtest.run_status RENAME VALUE 'COMPLETED' TO 'COMPLETE'")
            with pytest.raises(SchemaDriftError, match="enum backtest.run_status"):
                verify_schema(connection)
        finally:
            transaction.rollback()


def test_a_table_this_code_expects_but_the_database_lacks_is_reported(
    persistence: BacktestPersistence,
) -> None:
    """Drift detection is driven by the metadata, so an unmigrated addition is caught."""

    expected_later = MetaData()
    Table(
        "runs_v2",
        expected_later,
        Column("id", UUID(as_uuid=True), primary_key=True),
        Column("note", VARCHAR(10), nullable=False),
        schema="backtest",
    )

    with persistence.unit_of_work() as uow:
        problems = describe_schema_drift(uow.connection, expected_later)

    assert problems == ["backtest.runs_v2: table is missing"]
