"""The SQLAlchemy Core metadata must restate the canonical DDL exactly.

This is a Docker-free test: it parses the vendored copy of the applied central
baseline **plus this repository's own contributed migrations**, and compares the union,
column by column, with `backtest_engine.persistence.tables`. Anything the metadata
invents, omits, widens or narrows fails here long before a container is started.

The contributed migrations are parsed from the same directory the central Flyway
assembler reads (`db/migration-contributions/migrations`), in version order, so a table
this repository authors is held to exactly the same standard as one it inherited.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from sqlalchemy import DefaultClause, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.types import Numeric, TypeEngine

from backtest_engine.persistence.tables import (
    BACKTEST_SCHEMA,
    METADATA,
    STORAGE_SCHEMA,
    detail_manifests,
    failure_condition_counts,
    input_bundles,
    input_datasets,
    input_feature_materializations,
    monthly_judgment_summaries,
    performance_summaries,
    run_attempts,
    run_input_pins,
    runs,
    storage_objects,
)


CONTRIBUTION_ROOT = Path(__file__).resolve().parents[2] / "db" / "migration-contributions"

CENTRAL_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "central-migration"
CENTRAL_BASELINE = CENTRAL_MIGRATIONS / "V1__initial_schema.sql.fixture"

#: This repository's own contributed migrations. The canonical schema this metadata
#: must restate is *baseline plus contributions*, not the baseline alone: a table or
#: column this repository legally added is as canonical as one the baseline declared,
#: and comparing against the baseline alone would make every legal contribution look
#: like an invention. `tests/conftest.py` applies exactly these files to the
#: container after the bundle, so the two halves cannot drift apart.
#:
#: Two kinds of contribution are folded in, because this repository has one of each:
#: `V20260802094500__backtest_run_input_pins` adds a whole table (parsed by
#: `_parse_baseline`, like any `CREATE TABLE`), and
#: `V20260802143000__backtest_run_outcome_detail` adds columns to an applied table
#: (folded in by `_apply_contributions`).
CONTRIBUTED_MIGRATIONS = CONTRIBUTION_ROOT / "migrations"

EXPECTED_TABLES = {
    "backtest.runs",
    "backtest.run_attempts",
    "backtest.input_bundles",
    "backtest.input_datasets",
    "backtest.input_feature_materializations",
    "backtest.monthly_judgment_summaries",
    "backtest.failure_condition_counts",
    "backtest.performance_summaries",
    "backtest.detail_manifests",
    "backtest.run_input_pins",
    "storage.objects",
}

_TYPE_ALIASES = {
    "timestamp with time zone": "timestamptz",
    "timestamp without time zone": "timestamp",
    "character varying": "varchar",
    "character": "char",
    "integer": "int",
    "serial": "int",
}

_COLUMN_LINE = re.compile(
    r'^\s*"(?P<name>[a-z0-9_]+)"\s+'
    r"(?P<type>[a-z_]+(?:\.[a-z_]+)?(?:\s*\(\s*[0-9]+(?:\s*,\s*[0-9]+)?\s*\))?)"
    r"(?P<rest>.*?),?\s*$"
)


def _normalise_type(rendered: str) -> str:
    text = rendered.strip().lower()
    text = re.sub(r"\s*\(\s*", "(", text)
    text = re.sub(r"\s*\)\s*", ")", text)
    text = re.sub(r"\s*,\s*", ",", text)
    base, _, suffix = text.partition("(")
    base = base.strip()
    base = _TYPE_ALIASES.get(base, base)
    return f"{base}({suffix}" if suffix else base


def _render(type_: TypeEngine[object]) -> str:
    rendered: str = type_.compile(dialect=postgresql.dialect())  # type: ignore[no-untyped-call]
    return _normalise_type(rendered)


def _normalise_default(rendered: str | None) -> str | None:
    if rendered is None:
        return None
    text = rendered.strip().lower()
    while text.startswith("(") and text.endswith(")"):
        text = text[1:-1].strip()
    return text


class _DdlTable:
    def __init__(self, schema: str, name: str) -> None:
        self.schema = schema
        self.name = name
        self.columns: dict[str, dict[str, object]] = {}
        self.primary_key: tuple[str, ...] = ()
        self.uniques: set[tuple[str, ...]] = set()
        self.indexes: set[tuple[str, ...]] = set()

    @property
    def key(self) -> str:
        return f"{self.schema}.{self.name}"


def _parse_baseline(sql: str) -> dict[str, _DdlTable]:
    tables: dict[str, _DdlTable] = {}
    for match in re.finditer(
        r'CREATE TABLE "(?P<schema>[a-z_]+)"\."(?P<name>[a-z_]+)" \((?P<body>.*?)\n\);',
        sql,
        re.S,
    ):
        table = _DdlTable(match.group("schema"), match.group("name"))
        if table.key not in EXPECTED_TABLES:
            continue
        for raw in match.group("body").splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.upper().startswith("CONSTRAINT ") or line.upper().startswith("CHECK ("):
                continue
            if line.upper().startswith("PRIMARY KEY ("):
                table.primary_key = tuple(re.findall(r'"([a-z0-9_]+)"', line))
                continue
            column = _COLUMN_LINE.match(line)
            if column is None:
                raise AssertionError(f"unparsed DDL line in {table.key}: {line!r}")
            rest = column.group("rest").upper()
            default = re.search(r"DEFAULT\s+(.*?)$", column.group("rest"), re.I)
            table.columns[column.group("name")] = {
                "type": _normalise_type(column.group("type")),
                "nullable": "NOT NULL" not in rest and "PRIMARY KEY" not in rest,
                "default": _normalise_default(default.group(1)) if default else None,
            }
            if "PRIMARY KEY" in rest:
                table.primary_key = (column.group("name"),)
            if "UNIQUE" in rest:
                table.uniques.add((column.group("name"),))
        tables[table.key] = table

    for match in re.finditer(
        r'CREATE (?P<unique>UNIQUE )?INDEX ON "(?P<schema>[a-z_]+)"\."(?P<name>[a-z_]+)"'
        r" \((?P<cols>[^)]*)\);",
        sql,
    ):
        key = f"{match.group('schema')}.{match.group('name')}"
        if key not in tables:
            continue
        columns = tuple(re.findall(r'"([a-z0-9_]+)"', match.group("cols")))
        if match.group("unique"):
            tables[key].uniques.add(columns)
        else:
            tables[key].indexes.add(columns)
    return tables


def contributed_migration_files() -> list[Path]:
    """This repository's contributed migrations, in filename (= timestamp) order."""

    return sorted(CONTRIBUTED_MIGRATIONS.glob("V*.sql"))


def central_migration_files() -> list[Path]:
    """The complete vendored central Flyway history, in version order."""

    return sorted(CENTRAL_MIGRATIONS.glob("V*.sql.fixture"))


def ordered_migration_files() -> list[Path]:
    """Central and D-owned migrations in the exact Flyway version order."""

    files = [*central_migration_files(), *contributed_migration_files()]
    return sorted(files, key=lambda path: int(path.name.split("__", 1)[0].removeprefix("V")))


def canonical_sql() -> str:
    """The applied baseline followed by this repository's contributed migrations.

    Concatenated in the order the central assembler applies them, so a contributed
    `CREATE TABLE` is parsed exactly like a baseline one and a contributed table that
    the metadata does not restate fails the same assertions. Contributed `ADD COLUMN`
    clauses are *not* handled here -- `_parse_baseline` only reads `CREATE TABLE` --
    which is why `baseline()` folds them in afterwards with `_apply_contributions`.
    """

    sources = [path.read_text(encoding="utf-8") for path in ordered_migration_files()]
    return "\n\n".join(sources)


_ADD_COLUMN = re.compile(
    r'ALTER TABLE\s+"?(?P<schema>[a-z_]+)"?\."?(?P<name>[a-z_]+)"?\s+(?P<body>.*?);',
    re.S | re.I,
)

_ADDED_COLUMN_CLAUSE = re.compile(
    r'ADD COLUMN\s+"?(?P<name>[a-z0-9_]+)"?\s+'
    r"(?P<type>[a-z_]+(?:\.[a-z_]+)?(?:\s*\(\s*[0-9]+(?:\s*,\s*[0-9]+)?\s*\))?)"
    r"(?P<rest>[^,]*)",
    re.I,
)

_ALTER_COLUMN_NULLABILITY = re.compile(
    r'ALTER COLUMN\s+"?(?P<name>[a-z0-9_]+)"?\s+'
    r"(?P<operation>DROP|SET)\s+NOT NULL",
    re.I,
)

_CREATE_INDEX = re.compile(
    r"CREATE\s+(?P<unique>UNIQUE\s+)?INDEX(?:\s+[a-z0-9_]+)?\s+"
    r'ON\s+"?(?P<schema>[a-z_]+)"?\."?(?P<name>[a-z_]+)"?\s*'
    r"\((?P<cols>[^)]*)\)",
    re.I,
)


def _apply_contributions(tables: dict[str, _DdlTable], sql: str) -> None:
    """Fold this repository's `ADD COLUMN` clauses into the parsed baseline.

    Only `ADD COLUMN` is interpreted. An `ALTER TABLE` that does anything else --
    drops a column, retypes one, adds a constraint -- is left alone deliberately:
    silently ignoring a statement this parser does not model would let a real
    schema change pass unnoticed, so anything unmodelled must show up as a
    difference between the metadata and the live container in
    `tests/persistence/test_schema_drift.py` instead of being absorbed here.
    """

    for statement in _ADD_COLUMN.finditer(sql):
        key = f"{statement.group('schema')}.{statement.group('name')}"
        table = tables.get(key)
        if table is None:
            continue
        for clause in _ADDED_COLUMN_CLAUSE.finditer(statement.group("body")):
            rest = clause.group("rest").upper()
            default = re.search(r"DEFAULT\s+(.*?)$", clause.group("rest"), re.I)
            table.columns[clause.group("name")] = {
                "type": _normalise_type(clause.group("type")),
                "nullable": "NOT NULL" not in rest,
                "default": _normalise_default(default.group(1)) if default else None,
            }
            if "UNIQUE" in rest:
                table.uniques.add((clause.group("name"),))
        for clause in _ALTER_COLUMN_NULLABILITY.finditer(statement.group("body")):
            column = table.columns[clause.group("name")]
            column["nullable"] = clause.group("operation").upper() == "DROP"

    # Later central migrations replace the baseline's inline idempotency UNIQUE
    # constraint with lane-scoped uniqueness. The small parser models that exact
    # evolution so metadata checks compare with the applied schema, not V1 alone.
    if re.search(r"DROP\s+CONSTRAINT\s+IF\s+EXISTS\s+runs_idempotency_key_key", sql, re.I):
        tables["backtest.runs"].uniques.discard(("idempotency_key",))

    for index in _CREATE_INDEX.finditer(sql):
        key = f"{index.group('schema')}.{index.group('name')}"
        table = tables.get(key)
        if table is None:
            continue
        columns = tuple(
            token.strip().strip('"')
            for token in index.group("cols").split(",")
        )
        if index.group("unique"):
            table.uniques.add(columns)
        else:
            table.indexes.add(columns)


@pytest.fixture(scope="module")
def baseline() -> dict[str, _DdlTable]:
    """The canonical schema: applied baseline + contributed tables + contributed columns.

    Both contribution shapes are applied, in that order. `canonical_sql` brings in
    contributed `CREATE TABLE`s (so `backtest.run_input_pins` exists to be compared
    at all), and `_apply_contributions` then folds contributed `ADD COLUMN` clauses
    onto whichever table they target -- baseline or contributed alike, which is why
    the folding runs after the parse rather than against the baseline alone.
    """

    parsed = _parse_baseline(canonical_sql())
    missing = EXPECTED_TABLES - set(parsed)
    assert missing == set(), f"canonical DDL does not declare {sorted(missing)}"
    for path in ordered_migration_files()[1:]:
        _apply_contributions(parsed, path.read_text(encoding="utf-8"))
    return parsed


def test_the_contributed_migrations_really_add_the_columns_this_module_folds_in() -> None:
    """The folding step must not be a no-op that quietly accepts anything.

    If `_apply_contributions` stopped matching -- a changed quoting style, a
    renamed clause -- every contributed column would silently disappear from the
    expected set and the metadata comparison would start passing for a schema the
    container does not have.
    """
    parsed = _parse_baseline(CENTRAL_BASELINE.read_text(encoding="utf-8"))
    before = set(parsed["backtest.runs"].columns)
    for path in contributed_migration_files():
        _apply_contributions(parsed, path.read_text(encoding="utf-8"))

    assert set(parsed["backtest.runs"].columns) - before == {
        "result_manifest_id",
        "retryable",
        "missing_requirements",
    }
    assert parsed["backtest.runs"].columns["retryable"] == {
        "type": "boolean",
        "nullable": True,
        "default": None,
    }


ALL_TABLES = [
    runs,
    run_attempts,
    input_bundles,
    input_datasets,
    input_feature_materializations,
    monthly_judgment_summaries,
    failure_condition_counts,
    performance_summaries,
    detail_manifests,
    run_input_pins,
    storage_objects,
]


def test_both_contributed_migrations_are_present_in_timestamp_order() -> None:
    """Both contributions exist and apply in the order the central assembler uses.

    Named explicitly rather than counted: the two migrations were authored on
    separate branches, and the failure mode a merge introduces is that one of them
    quietly disappears while every other assertion in this module still passes,
    because each half is individually self-consistent.
    """

    assert [path.name for path in contributed_migration_files()] == [
        "V20260802094500__backtest_run_input_pins.sql",
        "V20260802143000__backtest_run_outcome_detail.sql",
    ]


def test_the_contributed_migration_is_the_only_new_backtest_table() -> None:
    """A contributed migration may add tables; it may never redefine an applied one."""

    contributed = _parse_baseline(
        "\n\n".join(path.read_text(encoding="utf-8") for path in contributed_migration_files())
    )
    applied = _parse_baseline(CENTRAL_BASELINE.read_text(encoding="utf-8"))

    # Only the input-pins contribution creates a table; the outcome-detail
    # contribution is `ALTER TABLE ... ADD COLUMN` only, which `_parse_baseline`
    # does not see and `_apply_contributions` folds in instead.
    assert set(contributed) == {"backtest.run_input_pins"}
    assert set(contributed) & set(applied) == set()


def test_metadata_declares_exactly_the_canonical_tables() -> None:
    declared = {f"{table.schema}.{table.name}" for table in METADATA.tables.values()}

    assert declared == EXPECTED_TABLES


def test_metadata_only_touches_the_backtest_and_storage_schemas() -> None:
    schemas = {table.schema for table in METADATA.tables.values()}

    assert schemas == {BACKTEST_SCHEMA, STORAGE_SCHEMA}


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_column_names_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    assert [column.name for column in table.columns] == list(expected.columns)


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_column_types_and_nullability_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    mismatches: list[str] = []
    for column in table.columns:
        want = expected.columns[column.name]
        rendered = _render(column.type)
        if rendered != want["type"]:
            mismatches.append(f"{column.name}: type {rendered!r} != canonical {want['type']!r}")
        if column.nullable != want["nullable"]:
            mismatches.append(f"{column.name}: nullable {column.nullable} != canonical {want['nullable']}")

    assert mismatches == []


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_server_defaults_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    mismatches: list[str] = []
    for column in table.columns:
        want = expected.columns[column.name]["default"]
        got = None
        default = column.server_default
        if isinstance(default, DefaultClause):
            got = _normalise_default(str(default.arg))
        if got != want:
            mismatches.append(f"{column.name}: default {got!r} != canonical {want!r}")

    assert mismatches == []


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_primary_keys_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    assert tuple(column.name for column in table.primary_key.columns) == expected.primary_key


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_unique_constraints_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    declared: set[tuple[str, ...]] = set()
    for constraint in table.constraints:
        columns = getattr(constraint, "columns", None)
        if columns is not None and constraint.__class__.__name__ == "UniqueConstraint":
            declared.add(tuple(column.name for column in columns))
    for index in table.indexes:
        if index.unique:
            declared.add(tuple(column.name for column in index.columns))

    assert declared == expected.uniques


@pytest.mark.parametrize("table", ALL_TABLES, ids=lambda table: f"{table.schema}.{table.name}")
def test_non_unique_indexes_match_the_canonical_ddl(table: Table, baseline: dict[str, _DdlTable]) -> None:
    expected = baseline[f"{table.schema}.{table.name}"]

    declared = {tuple(column.name for column in index.columns) for index in table.indexes if not index.unique}

    assert declared == expected.indexes


def test_money_columns_are_numeric_24_8() -> None:
    money = runs.c.initial_cash_amount

    assert _render(money.type) == "numeric(24,8)"
    assert isinstance(money.type, Numeric)
    assert money.type.asdecimal is True


def test_run_status_enum_uses_the_canonical_labels() -> None:
    status = runs.c.status.type
    assert isinstance(status, ENUM)

    assert status.name == "run_status"
    assert status.schema == "backtest"
    assert tuple(status.enums) == (
        "QUEUED", "RUNNING", "COMPLETED", "FAILED", "UNAVAILABLE", "CANCELLED"
    )
    assert "COMPLETE" not in status.enums


def test_enum_types_are_never_created_by_this_metadata() -> None:
    """The runtime must not emit `CREATE TYPE`; the central baseline owns the enums."""

    enum_columns = [
        runs.c.status,
        run_attempts.c.status,
        storage_objects.c.status,
    ]

    for column in enum_columns:
        assert isinstance(column.type, ENUM)
        assert column.type.create_type is False
