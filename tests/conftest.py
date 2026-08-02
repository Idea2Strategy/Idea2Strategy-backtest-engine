"""Shared test fixtures, including the Testcontainers PostgreSQL 16 harness.

Everything Docker-dependent is behind the `docker` marker and behind lazily-created
session fixtures, so the default `pytest` run (`-m 'not docker'`, see `pyproject.toml`)
never touches Docker.

The container is migrated with the **canonical** SQL: the vendored byte-for-byte copy
of the applied central Flyway bundle under
`db/migration-contributions/fixtures/central-migration/`, followed by this
repository's own contributed migrations from
`db/migration-contributions/migrations/`, in exactly the order the central assembler
applies them. No DDL is hand-written in the suite, so a persistence layer that
disagrees with the canonical schema fails here rather than in production.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import Connection, Engine, create_engine

from backtest_engine.persistence import BacktestPersistence, create_backtest_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTION_ROOT = REPO_ROOT / "db" / "migration-contributions"
VENDORED_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "central-migration"
CONTRIBUTED_MIGRATIONS = CONTRIBUTION_ROOT / "migrations"
VENDORED_DIGESTS = CONTRIBUTION_ROOT / "fixtures" / "central-migration.sha256"
REFERENCE_SEED = CONTRIBUTION_ROOT / "fixtures" / "backtest_reference_seed.sql.fixture"

POSTGRES_IMAGE = os.environ.get("BACKTEST_TEST_POSTGRES_IMAGE", "postgres:16-alpine")

_VERSION = re.compile(r"^V(?P<version>[0-9]+)__")

_BACKTEST_TABLES = (
    "backtest.run_input_pins",
    "backtest.failure_condition_counts",
    "backtest.monthly_judgment_summaries",
    "backtest.performance_summaries",
    "backtest.detail_manifests",
    "backtest.input_datasets",
    "backtest.input_feature_materializations",
    "backtest.input_bundles",
    "backtest.run_attempts",
    "backtest.runs",
)


def sha256_of(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def recorded_digests() -> dict[str, str]:
    """Parse `central-migration.sha256` into `{filename: digest}`."""

    digests: dict[str, str] = {}
    for line in VENDORED_DIGESTS.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        digest, _, name = stripped.partition("  ")
        digests[name.strip().lstrip("*")] = digest.strip()
    return digests


def migration_files() -> list[Path]:
    """The vendored bundle in Flyway version order."""

    files = [path for path in VENDORED_MIGRATIONS.glob("V*.sql.fixture") if path.is_file()]
    if not files:
        raise AssertionError(f"no vendored migrations under {VENDORED_MIGRATIONS}")

    def order(path: Path) -> int:
        match = _VERSION.match(path.name)
        if match is None:
            raise AssertionError(f"unversioned migration file: {path.name}")
        return int(match.group("version"))

    return sorted(files, key=order)


def docker_is_available() -> bool:
    try:
        import docker
    except ImportError:  # pragma: no cover - testcontainers depends on docker
        return False
    try:
        docker.from_env().ping()
    except Exception:  # pragma: no cover - depends on the developer's machine
        return False
    else:
        return True


@pytest.fixture(scope="session")
def postgres_url() -> Iterator[str]:
    """A PostgreSQL 16 container with the canonical schema and reference rows applied."""

    if not docker_is_available():
        pytest.skip("Docker is not available; the default run already deselects this suite")

    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(POSTGRES_IMAGE, driver="psycopg") as container:
        url = container.get_connection_url()
        _apply_canonical_migrations(url)
        _apply_reference_seed(url)
        yield url


def contributed_migration_files() -> list[Path]:
    """This repository's own contributed migrations, in Flyway version order.

    They are applied *after* the vendored central bundle, which is what the central
    assembler does with them. Without this the integration suite would prove the
    persistence layer against a schema the deployment will not have.
    """

    files = [path for path in CONTRIBUTED_MIGRATIONS.glob("V*.sql") if path.is_file()]
    return sorted(files, key=lambda path: path.name)


def _apply_canonical_migrations(url: str) -> None:
    """Apply the central bundle then this repository's contributions.

    The only DDL executed anywhere in the suite. It runs on a *separate, unguarded*
    engine on purpose: the runtime engine built by `create_backtest_engine` refuses
    DDL, which `test_runtime_no_ddl.py` asserts.
    """

    ordered = migration_files() + contributed_migration_files()
    _execute_scripts(url, [path.read_text(encoding="utf-8") for path in ordered])


def _apply_reference_seed(url: str) -> None:
    _execute_scripts(url, [REFERENCE_SEED.read_text(encoding="utf-8")])


def _execute_scripts(url: str, scripts: list[str]) -> None:
    """Run whole SQL scripts through the raw driver cursor.

    Not `exec_driver_sql`: SQLAlchemy hands psycopg an empty parameter tuple, which
    turns on client-side placeholder parsing, and the canonical baseline contains `%`
    inside Korean `COMMENT` strings. Passing no parameters at all avoids that entirely.
    """

    engine = create_engine(url, future=True)
    try:
        raw = engine.raw_connection()
        try:
            cursor = raw.cursor()
            for script in scripts:
                cursor.execute(script)
            raw.commit()
        finally:
            raw.close()
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def admin_engine(postgres_url: str) -> Iterator[Engine]:
    """An unguarded engine for arranging state and simulating drift."""

    engine = create_engine(postgres_url, future=True)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def runtime_engine(postgres_url: str) -> Iterator[Engine]:
    """The engine the production code would build, guards and all."""

    engine = create_backtest_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def persistence(runtime_engine: Engine) -> BacktestPersistence:
    return BacktestPersistence(runtime_engine)


@pytest.fixture(autouse=True)
def _empty_backtest_tables(request: pytest.FixtureRequest) -> Iterator[None]:
    """Leave `backtest.*` empty between Docker tests, without touching the seed rows."""

    engine: Engine | None = None
    if "postgres_url" in request.fixturenames:
        engine = request.getfixturevalue("admin_engine")
    yield
    if engine is not None:
        with engine.begin() as connection:
            _truncate(connection)


def _truncate(connection: Connection) -> None:
    connection.exec_driver_sql("TRUNCATE TABLE " + ", ".join(_BACKTEST_TABLES) + " RESTART IDENTITY CASCADE")
