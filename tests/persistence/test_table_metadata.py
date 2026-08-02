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

CENTRAL_BASELINE = CONTRIBUTION_ROOT / "fixtures" / "central-migration" / "V1__initial_schema.sql.fixture"

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


def canonical_sql() -> str:
    """The applied baseline followed by this repository's contributed migrations.

    Concatenated in the order the central assembler applies them, so a contributed
    `CREATE TABLE` is parsed exactly like a baseline one and a contributed table that
    the metadata does not restate fails the same assertions.
    """

    sources = [CENTRAL_BASELINE.read_text(encoding="utf-8")]
    sources.extend(path.read_text(encoding="utf-8") for path in sorted(CONTRIBUTED_MIGRATIONS.glob("V*.sql")))
    return "\n\n".join(sources)


@pytest.fixture(scope="module")
def baseline() -> dict[str, _DdlTable]:
    parsed = _parse_baseline(canonical_sql())
    missing = EXPECTED_TABLES - set(parsed)
    assert missing == set(), f"canonical DDL does not declare {sorted(missing)}"
    return parsed


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


def test_the_contributed_migration_is_the_only_new_backtest_table() -> None:
    """A contributed migration may add tables; it may never redefine an applied one."""

    contributed = _parse_baseline(
        "\n\n".join(path.read_text(encoding="utf-8") for path in sorted(CONTRIBUTED_MIGRATIONS.glob("V*.sql")))
    )
    applied = _parse_baseline(CENTRAL_BASELINE.read_text(encoding="utf-8"))

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
    assert tuple(status.enums) == ("QUEUED", "RUNNING", "COMPLETED", "FAILED", "UNAVAILABLE")
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
