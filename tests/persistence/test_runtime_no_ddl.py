"""COM07 `runtime-no-ddl`: an application connection cannot change the schema.

The unit half exercises the statement classifier directly. The Docker half proves the
guard is actually wired to the engine the production factory builds.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from backtest_engine.persistence import (
    METADATA,
    RUNTIME_ROW_ONLY_SCHEMAS,
    RUNTIME_SHARED_WRITE_TABLES,
    BacktestPersistence,
    RuntimeDdlForbidden,
    SchemaWriteForbidden,
    check_statement,
    load_contribution,
)


#: The set the production factory actually installs, not a copy of it. A hardcoded set
#: here would keep passing after the real one changed.
WRITABLE = load_contribution().writable_schemas()
SRC_ROOT = Path(__file__).resolve().parents[2] / "src"


def test_the_runtime_writable_set_is_backtest_plus_storage_rows_only() -> None:
    """Spec 2.4: `backtest` is D's to migrate; `storage` is D's to write rows in."""
    assert WRITABLE == {"backtest", "storage"}
    assert RUNTIME_ROW_ONLY_SCHEMAS == {"storage"}
    assert RUNTIME_SHARED_WRITE_TABLES == {"operations.outbox_consumer_receipts"}
    # `storage` must NOT appear in the COM07 migration declaration: D authors no
    # `storage` DDL, because the central policy registers that schema as SHARED.
    assert load_contribution().schemas == ("backtest",)


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE TABLE backtest.sneaky (id uuid)",
        "  create table backtest.sneaky (id uuid)",
        'ALTER TABLE "backtest"."runs" ADD COLUMN x int',
        "DROP TABLE backtest.runs",
        "TRUNCATE TABLE backtest.runs",
        "GRANT CREATE ON SCHEMA backtest TO idea2strategy_backtest",
        "REVOKE ALL ON backtest.runs FROM public",
        "COMMENT ON TABLE backtest.runs IS 'x'",
        "CREATE TYPE backtest.other AS ENUM ('A')",
        "CREATE UNIQUE INDEX ON backtest.runs (id)",
        "REFRESH MATERIALIZED VIEW backtest.something",
        "-- a comment\nDROP SCHEMA backtest CASCADE",
        "/* leading block */ ALTER SCHEMA backtest RENAME TO other",
    ],
)
def test_ddl_statements_are_rejected(statement: str) -> None:
    with pytest.raises(RuntimeDdlForbidden):
        check_statement(statement, WRITABLE)


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE identity.accounts SET lifecycle_status = 'CLOSED'",
        "DELETE FROM market_data.dataset_manifests WHERE id = :id",
        "UPDATE ONLY bot.bots SET name = 'x'",
        "UPDATE operations.outbox_messages SET delivery_status = 'PUBLISHED'",
    ],
)
def test_writes_outside_the_declared_schemas_are_rejected(statement: str) -> None:
    with pytest.raises(SchemaWriteForbidden):
        check_statement(statement, WRITABLE)


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT * FROM storage.objects WHERE id = :id",
        # Spec rule 2 and 2.5: D writes `storage.objects` rows. Spec 2.4 forbids it
        # from authoring `storage` DDL, which the DDL guard below still enforces.
        "INSERT INTO storage.objects (id) VALUES (:id)",
        'insert into "storage"."objects" (id) values (1)',
        "UPDATE storage.objects SET status = 'AVAILABLE' WHERE id = :id",
        "SELECT * FROM market_data.dataset_manifests",
        "INSERT INTO backtest.runs (id) VALUES (:id)",
        'UPDATE "backtest"."runs" SET status = :status',
        "DELETE FROM backtest.run_attempts WHERE id = :id",
        "SET TRANSACTION READ ONLY",
        "",
        "   ",
    ],
)
def test_permitted_statements_pass(statement: str) -> None:
    check_statement(statement, WRITABLE, RUNTIME_SHARED_WRITE_TABLES)


def test_only_the_contract_receipt_table_is_writable_in_operations() -> None:
    check_statement(
        "INSERT INTO operations.outbox_consumer_receipts (consumer_handler_id) VALUES (:handler)",
        WRITABLE,
        RUNTIME_SHARED_WRITE_TABLES,
    )


def test_no_production_module_calls_create_all() -> None:
    """`metadata.create_all()` must not appear anywhere under `src/`."""

    pattern = re.compile(r"create_all\s*\(")
    offenders = [
        str(path.relative_to(SRC_ROOT))
        for path in sorted(SRC_ROOT.rglob("*.py"))
        if pattern.search(path.read_text(encoding="utf-8"))
    ]

    assert offenders == []


@pytest.mark.docker
def test_runtime_engine_refuses_ddl(persistence: BacktestPersistence) -> None:
    with pytest.raises(RuntimeDdlForbidden), persistence.unit_of_work() as uow:
        uow.connection.execute(text("CREATE TABLE backtest.sneaky (id uuid PRIMARY KEY)"))


@pytest.mark.docker
def test_runtime_engine_refuses_metadata_create_all(runtime_engine: Engine) -> None:
    """Even the classic mistake is blocked at the connection, not by convention.

    `checkfirst=False` so SQLAlchemy actually emits the `CREATE TABLE`; with the default
    `checkfirst=True` it would find the tables already present and emit nothing, which
    would prove only that the schema exists.
    """

    with pytest.raises(RuntimeDdlForbidden):
        METADATA.create_all(runtime_engine, checkfirst=False)


@pytest.mark.docker
def test_runtime_engine_refuses_ddl_against_the_shared_storage_schema(
    persistence: BacktestPersistence,
) -> None:
    """Rows yes, schema changes no.

    Spec 2.4: `DatabaseAccessPolicy` registers `storage` as SHARED, so this repository
    writes object rows but must never alter the shared schema. The two permissions are
    separate and only one of them is granted.
    """
    with pytest.raises(RuntimeDdlForbidden), persistence.unit_of_work() as uow:
        uow.connection.execute(text("ALTER TABLE storage.objects ADD COLUMN sneaky text"))


@pytest.mark.docker
def test_runtime_engine_still_refuses_writes_to_schemas_d_does_not_own(
    persistence: BacktestPersistence,
) -> None:
    with pytest.raises(SchemaWriteForbidden), persistence.unit_of_work() as uow:
        uow.connection.execute(text("UPDATE identity.accounts SET lifecycle_status = 'CLOSED'"))


@pytest.mark.docker
def test_ddl_really_did_not_happen(persistence: BacktestPersistence, admin_engine: Engine) -> None:
    with pytest.raises(RuntimeDdlForbidden), persistence.unit_of_work() as uow:
        uow.connection.execute(text("CREATE TABLE backtest.sneaky (id uuid PRIMARY KEY)"))

    with admin_engine.connect() as connection:
        exists = connection.execute(text("SELECT to_regclass('backtest.sneaky') IS NOT NULL")).scalar_one()

    assert exists is False
