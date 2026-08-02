"""Engine construction, runtime guards, and the transaction boundary.

Two guards are installed on every engine this module builds:

* **No DDL.** COM07 acceptance `runtime-no-ddl`. Migration execution belongs to the
  central Flyway bundle; an application connection must never be able to create,
  alter, drop or truncate anything, no matter what code path asks it to.
* **Declared schemas only.** Writes are restricted to the schemas declared in
  `db/migration-contributions/contribution.properties` (`backtest`). The applied
  baseline contains no role `GRANT`s, so the database itself does not enforce
  ownership; this is the part of that boundary this repository can enforce for itself.

Both guards are `before_cursor_execute` listeners, so they see the final SQL text
regardless of whether it came from Core constructs or `text()`.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from sqlalchemy import Connection, Engine, create_engine, event, text
from sqlalchemy.engine import URL

from .contribution import MigrationContribution, load_contribution
from .errors import RuntimeDdlForbidden, SchemaWriteForbidden
from .repositories import BacktestUnitOfWork
from .schema_guard import verify_schema


__all__ = [
    "BacktestPersistence",
    "check_statement",
    "create_backtest_engine",
    "install_runtime_guards",
]


_COMMENT = re.compile(r"(?s)/\*.*?\*/|--[^\n]*")

_DDL_VERBS = re.compile(
    r"^(?:CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|COMMENT|REINDEX|CLUSTER|VACUUM|REFRESH"
    r"|IMPORT|SECURITY\s+LABEL)\b",
    re.I,
)

_WRITE_TARGET = re.compile(
    r"^(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+(?:ONLY\s+)?"
    r'"?(?P<schema>[a-z_][a-z0-9_]*)"?\s*\.\s*"?(?P<table>[a-z_][a-z0-9_]*)"?',
    re.I,
)

#: Schemas this repository reads. `identity`, `bot`, `competition` and the rest are not
#: read by the persistence layer at all, so they are not listed.
READABLE_SCHEMAS: frozenset[str] = frozenset({"backtest", "storage", "market_data", "strategy"})


def _strip(statement: str) -> str:
    return _COMMENT.sub(" ", statement).strip()


def check_statement(statement: str, writable_schemas: frozenset[str]) -> None:
    """Raise if `statement` is DDL, or writes a schema outside `writable_schemas`.

    Pure and side-effect free so it can be unit-tested without a database.
    """

    cleaned = _strip(statement)
    if not cleaned:
        return
    if _DDL_VERBS.match(cleaned):
        raise RuntimeDdlForbidden(
            "the backtest runtime must not execute DDL; migrations belong to the central "
            f"Flyway bundle. Rejected: {cleaned[:120]!r}"
        )
    target = _WRITE_TARGET.match(cleaned)
    if target is not None and target.group("schema").lower() not in writable_schemas:
        raise SchemaWriteForbidden(
            f"this repository may only write {sorted(writable_schemas)}; rejected write to "
            f"{target.group('schema')}.{target.group('table')}"
        )


def install_runtime_guards(engine: Engine, writable_schemas: Sequence[str]) -> None:
    """Refuse DDL and out-of-contract writes on every cursor this engine executes."""

    writable = frozenset(writable_schemas)
    if not writable:
        raise ValueError("writable_schemas must not be empty")

    @event.listens_for(engine, "before_cursor_execute")
    def _guard(  # type: ignore[no-untyped-def]
        conn, cursor, statement, parameters, context, executemany
    ) -> None:
        check_statement(statement, writable)


def create_backtest_engine(
    url: str | URL,
    *,
    writable_schemas: Sequence[str] | None = None,
    contribution: MigrationContribution | None = None,
    application_name: str = "idea2strategy-backtest-engine",
    pool_pre_ping: bool = True,
    echo: bool = False,
    **engine_kwargs: Any,
) -> Engine:
    """Build a guarded SQLAlchemy Core engine for the `backtest` schema.

    The writable schema set comes from this repository's contribution root, so it is
    literally the list the central Flyway gate reads. A deployment that ships only the
    wheel (no `db/` directory) must pass `writable_schemas` explicitly; there is no
    silent default, because a wrong default here is a database-ownership violation.
    """

    if writable_schemas is None:
        contribution = contribution or load_contribution()
        writable_schemas = sorted(contribution.writable_schemas())
    connect_args: dict[str, Any] = dict(engine_kwargs.pop("connect_args", {}))
    connect_args.setdefault("application_name", application_name)
    engine = create_engine(
        url,
        future=True,
        pool_pre_ping=pool_pre_ping,
        echo=echo,
        connect_args=connect_args,
        **engine_kwargs,
    )
    install_runtime_guards(engine, writable_schemas)
    return engine


class BacktestPersistence:
    """Owns the engine and hands out transactional units of work.

    The runtime never applies DDL. `verify_schema()` is the startup check that the
    expected schema is already present; it fails loudly rather than repairing anything.
    """

    def __init__(self, engine: Engine, *, contribution: MigrationContribution | None = None) -> None:
        self._engine = engine
        self._contribution = contribution

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def contribution(self) -> MigrationContribution:
        """This repository's COM07 contract, loaded on first use.

        Lazy so a wheel-only deployment without `db/migration-contributions` can still
        construct this object; asking for the contract there is a hard error.
        """

        if self._contribution is None:
            self._contribution = load_contribution()
        return self._contribution

    def verify_schema(self) -> None:
        """Raise `SchemaDriftError` unless the live schema matches this code."""

        with self._engine.connect() as connection:
            verify_schema(connection)

    @contextmanager
    def unit_of_work(self) -> Iterator[BacktestUnitOfWork]:
        """One transaction spanning every repository handed out inside the block.

        Commits on a clean exit, rolls back on any exception. A multi-table publish that
        fails half way through leaves no rows behind.
        """

        with self._engine.begin() as connection:
            yield BacktestUnitOfWork(connection)

    @contextmanager
    def read_only(self) -> Iterator[BacktestUnitOfWork]:
        """A read-only transaction; the database rejects writes made inside it."""

        with self._engine.connect() as connection:
            connection.execute(text("SET TRANSACTION READ ONLY"))
            try:
                yield BacktestUnitOfWork(connection)
            finally:
                connection.rollback()

    def dispose(self) -> None:
        self._engine.dispose()


def bind(connection: Connection) -> BacktestUnitOfWork:
    """Wrap an already-open connection, for callers that own the transaction."""

    return BacktestUnitOfWork(connection)
