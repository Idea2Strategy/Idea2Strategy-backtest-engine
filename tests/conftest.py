"""Shared test fixtures: the Testcontainers PostgreSQL 16 and LocalStack harnesses.

Everything Docker-dependent is behind the `docker` marker and behind lazily-created
session fixtures, so the default `pytest` run (`-m 'not docker'`, see `pyproject.toml`)
never touches Docker.

The container is migrated with the **canonical** SQL: the vendored byte-for-byte copy
of the applied central Flyway bundle under
`db/migration-contributions/fixtures/central-migration/`, followed by this
repository's active outcome contribution and the pending provider-owned pin fixture,
in exact Flyway version order. No DDL is hand-written in the suite, so a persistence layer that
disagrees with the canonical schema fails here rather than in production.

The LocalStack fixtures (`localstack`, `sqs`, `s3`, `queues`, `bucket`) live here
rather than in one test module because three integration modules -- the D30/D31
reproducibility traversal, D91 and D93 -- all need the same real queue and bucket,
and starting one emulator per module would triple the suite's wall time for no
additional coverage.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Connection, Engine, create_engine

from backtest_engine.persistence import BacktestPersistence, create_backtest_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTION_ROOT = REPO_ROOT / "db" / "migration-contributions"
VENDORED_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "central-migration"
CONTRIBUTED_MIGRATIONS = CONTRIBUTION_ROOT / "migrations"
PENDING_ROOT_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "pending-root"
SUPERSEDED_PIN_MIGRATION = "V20260802094500__backtest_run_input_pins.sql"
VENDORED_DIGESTS = CONTRIBUTION_ROOT / "fixtures" / "central-migration.sha256"
REFERENCE_SEED = CONTRIBUTION_ROOT / "fixtures" / "backtest_reference_seed.sql.fixture"

POSTGRES_IMAGE = os.environ.get("BACKTEST_TEST_POSTGRES_IMAGE", "postgres:16-alpine")
LOCALSTACK_IMAGE = os.environ.get("BACKTEST_TEST_LOCALSTACK_IMAGE", "localstack/localstack:4.7.0")

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
    """Active outcome contribution plus provider-owned pending target fixture.

    They are not part of the vendored central bundle: the bundle is a byte-for-byte
    copy of what has *already been applied* centrally, and a contribution has by
    definition not been. Applying them here after the bundle is what the central
    assembler does, so the container ends up with the schema this repository is
    actually asking for. Without this step a contributed column would be invisible
    to every integration test and the contribution would be untested SQL.

    The historical consumer-owned input-pin migration remains on disk for audit but
    is deliberately excluded: current code consumes the normalized provider bundle.
    """

    files = [
        path
        for path in CONTRIBUTED_MIGRATIONS.glob("V*.sql")
        if path.is_file() and path.name != SUPERSEDED_PIN_MIGRATION
    ]
    files.extend(PENDING_ROOT_MIGRATIONS.glob("V*.sql.fixture"))
    def order(path: Path) -> int:
        match = _VERSION.match(path.name)
        if match is None:
            raise AssertionError(f"unversioned migration file: {path.name}")
        return int(match.group("version"))

    return sorted(files, key=order)


def _apply_canonical_migrations(url: str) -> None:
    """Apply the vendored central bundle, then this repository's contributions.

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
                # Flyway commits each versioned migration before applying the next.
                # This is required when a later migration uses an enum value added
                # by its predecessor, which PostgreSQL rejects in one transaction.
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


# ---------------------------------------------------------------------------
# LocalStack: a real SQS queue pair and a real S3 bucket
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def localstack() -> Iterator[Any]:
    """One LocalStack for the session, with both services the D suite needs."""

    if not docker_is_available():  # pragma: no cover - environment dependent
        pytest.skip(
            "missing dependency: a reachable Docker daemon for the "
            f"{LOCALSTACK_IMAGE} SQS + S3 emulator. Without it the queue and "
            "object-store legs of the integration suite are NOT covered."
        )
    from testcontainers.community.localstack import LocalStackContainer

    # us-east-1: any other region makes CreateBucket require a LocationConstraint.
    container = LocalStackContainer(image=LOCALSTACK_IMAGE, region_name="us-east-1")
    with container.with_services("sqs", "s3") as running:
        yield running


@pytest.fixture(scope="session")
def sqs(localstack: Any) -> Any:
    return localstack.get_client("sqs")


@pytest.fixture(scope="session")
def s3(localstack: Any) -> Any:
    return localstack.get_client("s3")


@pytest.fixture
def queues(sqs: Any) -> Iterator[tuple[str, str]]:
    """A fresh (main, dead-letter) pair per test, so depth assertions are absolute."""

    suffix = uuid.uuid4().hex[:12]
    main = sqs.create_queue(
        QueueName=f"d-main-{suffix}",
        Attributes={"VisibilityTimeout": "30", "ReceiveMessageWaitTimeSeconds": "0"},
    )["QueueUrl"]
    dead = sqs.create_queue(QueueName=f"d-dlq-{suffix}")["QueueUrl"]
    yield main, dead
    for url in (main, dead):
        with contextlib.suppress(Exception):  # best-effort cleanup
            sqs.delete_queue(QueueUrl=url)


@pytest.fixture(scope="session")
def bucket(s3: Any) -> str:
    """One bucket for the whole session, because a deployment has one bucket.

    The scope is load-bearing, not a performance choice. A backtest object's
    `storage.objects` id is derived from its content, and `_OBJECT_IDENTITY_FIELDS`
    includes `bucket_name`, while `_empty_backtest_tables` deliberately leaves
    `storage.objects` alone: an object outlives the run that produced it. Two
    modules that replay the same pinned fixture therefore publish *the same object
    id*, and if they addressed different buckets the second one would be refused by
    `StorageObjectRepository.register` with "already registered for different
    bytes: ['bucket_name']".

    That refusal is correct behaviour -- one stored object has exactly one row --
    so the fix is to stop lying about the topology rather than to weaken the
    repository. With one bucket per session the second publication takes the
    reconciliation path `register` exists for and returns `inserted=False`, which
    is what a redelivery does in production.
    """

    name = f"d-int-{uuid.uuid4().hex[:12]}"
    s3.create_bucket(Bucket=name)
    return name


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
