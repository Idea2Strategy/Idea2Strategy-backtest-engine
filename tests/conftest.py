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
from typing import Any, Literal, cast

import pytest
from sqlalchemy import Connection, Engine, create_engine, event, text

from backtest_engine.persistence import BacktestPersistence, create_backtest_engine


REPO_ROOT = Path(__file__).resolve().parents[1]
CONTRIBUTION_ROOT = REPO_ROOT / "db" / "migration-contributions"
VENDORED_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "central-migration"
CONTRIBUTED_MIGRATIONS = CONTRIBUTION_ROOT / "migrations"
PENDING_ROOT_MIGRATIONS = CONTRIBUTION_ROOT / "fixtures" / "pending-root"
VENDORED_DIGESTS = CONTRIBUTION_ROOT / "fixtures" / "central-migration.sha256"
REFERENCE_SEED = CONTRIBUTION_ROOT / "fixtures" / "backtest_reference_seed.sql.fixture"

POSTGRES_IMAGE = os.environ.get("BACKTEST_TEST_POSTGRES_IMAGE", "postgres:16-alpine")
LOCALSTACK_IMAGE = os.environ.get("BACKTEST_TEST_LOCALSTACK_IMAGE", "localstack/localstack:4.7.0")

_VERSION = re.compile(r"^V(?P<version>[0-9]+)__")
_COPY_FROM_STDIN = re.compile(r"(?m)^COPY [^\r\n]+ FROM stdin;\r?\n")
_COPY_TERMINATOR = re.compile(r"(?m)^\\\.\r?(?:\n|$)")

type ScriptChunk = tuple[Literal["sql"], str] | tuple[Literal["copy"], tuple[str, str]]

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

    The historical consumer-owned input-pin migration is preserved outside the active
    directory under fixtures/superseded-proposals. Current code consumes the normalized
    provider bundle fixture instead.
    """

    files = [path for path in CONTRIBUTED_MIGRATIONS.glob("V*.sql") if path.is_file()]
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

    ordered = sorted(
        migration_files() + contributed_migration_files(),
        key=lambda path: int(_VERSION.match(path.name).group("version")),  # type: ignore[union-attr]
    )
    _execute_scripts(url, [path.read_text(encoding="utf-8") for path in ordered])
    _apply_backtest_runtime_role(url)


def _apply_backtest_runtime_role(url: str) -> None:
    """Install the cleanup-relevant slice of the generated production ACL.

    The central assembler generates ``R__database_runtime_grants.sql`` at build time,
    so there is intentionally no checked-in repeatable SQL file to vendor beside the
    versioned migrations.  These statements reproduce the canonical role name and the
    exact ``storage.objects`` table privileges from ``DatabaseAccessPolicy``.  The
    capability grant is conditional so a regression run against the pre-migration
    bundle fails in the owner test, rather than in fixture setup.
    """

    _execute_scripts(
        url,
        [
            """
            DO $$ BEGIN
              IF NOT EXISTS (
                SELECT 1 FROM pg_roles WHERE rolname = 'idea2strategy_backtest'
              ) THEN
                CREATE ROLE idea2strategy_backtest NOLOGIN;
              END IF;
            END $$;
            ALTER ROLE idea2strategy_backtest
              NOLOGIN NOCREATEDB NOCREATEROLE NOINHERIT;
            GRANT USAGE ON SCHEMA backtest TO idea2strategy_backtest;
            GRANT USAGE ON SCHEMA storage TO idea2strategy_backtest;
            GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA backtest
              TO idea2strategy_backtest;
            REVOKE INSERT, UPDATE ON TABLE backtest.run_attempts
              FROM idea2strategy_backtest;
            GRANT SELECT ON TABLE storage.objects TO idea2strategy_backtest;
            REVOKE INSERT, UPDATE, DELETE ON TABLE storage.objects
              FROM idea2strategy_backtest;
            DO $$ BEGIN
              IF to_regprocedure(
                'storage.prepare_backtest_object_cleanup(jsonb)'
              ) IS NOT NULL THEN
                GRANT EXECUTE ON FUNCTION
                  storage.prepare_backtest_object_cleanup(jsonb)
                  TO idea2strategy_backtest;
              END IF;
              IF to_regprocedure(
                'storage.reissue_backtest_object_cleanup(jsonb,text)'
              ) IS NOT NULL THEN
                GRANT EXECUTE ON FUNCTION
                  storage.reissue_backtest_object_cleanup(jsonb,text)
                  TO idea2strategy_backtest;
              END IF;
              IF to_regprocedure(
                'backtest.claim_run_attempt(uuid,text,text,bigint)'
              ) IS NOT NULL THEN
                GRANT EXECUTE ON FUNCTION
                  backtest.claim_run_attempt(uuid,text,text,bigint)
                  TO idea2strategy_backtest;
                GRANT EXECUTE ON FUNCTION
                  backtest.heartbeat_run_attempt(uuid,uuid,bigint)
                  TO idea2strategy_backtest;
                GRANT EXECUTE ON FUNCTION
                  backtest.close_run_attempt(uuid,uuid,text,text,text,boolean)
                  TO idea2strategy_backtest;
                GRANT EXECUTE ON FUNCTION
                  backtest.recover_expired_run_attempt(uuid,text,text)
                  TO idea2strategy_backtest;
                GRANT EXECUTE ON FUNCTION
                  storage.register_backtest_object(jsonb)
                  TO idea2strategy_backtest;
                GRANT EXECUTE ON FUNCTION
                  storage.transition_backtest_object(uuid,text,timestamp with time zone)
                  TO idea2strategy_backtest;
              END IF;
            END $$;
            """
        ],
    )


def _apply_reference_seed(url: str) -> None:
    _execute_scripts(url, [REFERENCE_SEED.read_text(encoding="utf-8")])


def _split_copy_from_stdin(script: str) -> Iterator[ScriptChunk]:
    """Split pg_dump COPY blocks from SQL that psycopg can execute normally."""

    offset = 0
    while copy_header := _COPY_FROM_STDIN.search(script, offset):
        if copy_header.start() > offset:
            yield "sql", script[offset : copy_header.start()]

        terminator = _COPY_TERMINATOR.search(script, copy_header.end())
        if terminator is None:
            raise ValueError("unterminated COPY FROM stdin block")

        statement = copy_header.group(0).rstrip("\r\n")
        payload = script[copy_header.end() : terminator.start()]
        yield "copy", (statement, payload)
        offset = terminator.end()

    if offset < len(script):
        yield "sql", script[offset:]


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
                for kind, chunk in _split_copy_from_stdin(script):
                    if kind == "sql":
                        cursor.execute(chunk)
                    else:
                        statement, payload = cast(tuple[str, str], chunk)
                        with cursor.copy(statement) as copy:
                            copy.write(payload)
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


@pytest.fixture(scope="session")
def backtest_role_engine(postgres_url: str) -> Iterator[Engine]:
    """The guarded engine with every physical connection set to the runtime role."""

    engine = create_backtest_engine(
        postgres_url,
        application_name="task5-production-backtest-role",
    )

    @event.listens_for(engine, "checkout")
    def _set_canonical_role(
        dbapi_connection: Any,
        _connection_record: Any,
        _connection_proxy: Any,
    ) -> None:
        # SQLAlchemy rolls an idle DBAPI connection back when it returns to the
        # pool.  SET ROLE is therefore repeated on every checkout: no later test
        # can silently inherit the container superuser after that rollback.
        with dbapi_connection.cursor() as cursor:
            cursor.execute("SET ROLE idea2strategy_backtest")

    try:
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT current_user")) == "idea2strategy_backtest"
        yield engine
    finally:
        engine.dispose()


@pytest.fixture
def backtest_role_persistence(backtest_role_engine: Engine) -> BacktestPersistence:
    return BacktestPersistence(backtest_role_engine)


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
