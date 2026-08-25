"""The runnable D stack, assembled from real infrastructure. Fixture code only.

Three integration modules drive the same stack -- the D30/D31 reproducibility
traversal, D91's exactly-once release intake and D93's isolation proof -- so it
is built once here rather than three times. Nothing in this module computes a
result or may be used as an oracle: it wires the production adapters to a real
LocalStack queue, a real LocalStack bucket and the Testcontainers PostgreSQL 16
from `conftest`, and hands the caller the seams it needs to observe.

The one thing this module deliberately does *not* provide is an expected value.
Every pinned digest is a literal in the test that asserts it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from sqlalchemy import Engine, text

from backtest_engine.api import RESULT_INGEST_SCOPE, Principal, StaticTokenAuthenticator, create_app
from backtest_engine.attempt_coordinator import AttemptPolicy, ResourceSample
from backtest_engine.basic_runtime import BasicPlanRuntime
from backtest_engine.calendar import XNYS_CALENDAR
from backtest_engine.execution_policy import ExecutionPolicyCatalog
from backtest_engine.lifecycle import (
    BacktestLifecycleService,
    InMemoryDeadLetterQueue,
    PersistenceRunGateway,
    SqsBacktestJobQueue,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
)
from backtest_engine.market_data import ParquetMarketDataReader
from backtest_engine.object_store import S3ObjectStore
from backtest_engine.persistence import BacktestPersistence
from backtest_engine.wiring import (
    OrchestratorJobHandler,
    PersistenceExecutionKeyStore,
    PersistenceStorageObjectWritePort,
    build_result_query_service,
)
from backtest_engine.worker import BacktestWorker, WorkerConfig
from d_reproducibility_testkit import (
    ACCOUNT_ID,
    BOT_ID,
    CLOSES,
    COMPLETED_AT,
    CORRELATION_ID,
    DATASET_MANIFEST_ID,
    E2E_EXECUTION_POLICY,
    E2E_FRACTIONAL_POLICY,
    E2E_MICROSTRUCTURE,
    E2E_RISK_LIMITS,
    MarketDataFixture,
    compiled_plan,
    official_backtest_request,
    write_market_data,
)


OWNER_TOKEN = "d-int-owner-token"
WORKER_TOKEN = "d-int-worker-token"

ATTEMPT_POLICY = AttemptPolicy(
    max_attempts=3,
    lease_duration=timedelta(minutes=5),
    attempt_timeout=timedelta(minutes=30),
    max_cpu_time=timedelta(minutes=5),
    max_memory_bytes=512 * 1024 * 1024,
)


# ---------------------------------------------------------------------------
# Server-side observation
# ---------------------------------------------------------------------------


class HeaderRecorder:
    """A pass-through ASGI app that records what the server actually received.

    Deliberately *not* a wrapper around the HTTP client: an assertion about what
    the client sent would be an assertion about the test's own code.
    """

    def __init__(self, app: Any) -> None:
        self._app = app
        self.requests: list[tuple[str, str, Mapping[str, str]]] = []

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] == "http":
            headers = {
                key.decode("latin-1").lower(): value.decode("latin-1")
                for key, value in scope["headers"]
            }
            self.requests.append((scope["method"], scope["path"], headers))
        await self._app(scope, receive, send)

    def result_posts(self) -> list[Mapping[str, str]]:
        return [
            headers
            for method, path, headers in self.requests
            if method == "POST" and path.endswith("/results")
        ]


class HttpResultSink:
    """The worker's `ResultSink`, bound to the real `/api/v1` endpoint."""

    def __init__(self, client: TestClient, token: str) -> None:
        self._client = client
        self._token = token
        self.responses: list[Any] = []
        self.events: list[dict[str, Any]] = []

    def publish(self, event: Mapping[str, Any], *, delivery_attempt: int) -> None:
        self.events.append(dict(event))
        response = self._client.post(
            f"/api/v1/backtests/{event['backtestRunId']}/results",
            json=dict(event),
            headers={
                "Authorization": f"Bearer {self._token}",
                "X-Delivery-Attempt": str(delivery_attempt),
            },
        )
        self.responses.append(response)
        if response.status_code != 200:
            raise AssertionError(
                f"result ingestion rejected a {event['status']} event: "
                f"{response.status_code} {response.text}"
            )


class ScriptedMonitor:
    """A resource monitor whose samples are a pinned script, not the real process."""

    def __init__(self, *samples: ResourceSample) -> None:
        self._samples = list(samples)
        self.calls = 0
        self._steady = ResourceSample(timedelta(seconds=1), 64 * 1024 * 1024)

    def sample(self) -> ResourceSample:
        self.calls += 1
        if self._samples:
            return self._samples.pop(0)
        return self._steady


# ---------------------------------------------------------------------------
# The stack under test
# ---------------------------------------------------------------------------


@dataclass
class Stack:
    client: TestClient
    recorder: HeaderRecorder
    sink: HttpResultSink
    worker: BacktestWorker
    handler: OrchestratorJobHandler
    lifecycle: BacktestLifecycleService
    store: S3ObjectStore
    dead_letters: InMemoryDeadLetterQueue
    request: dict[str, Any]
    market_data: MarketDataFixture
    bucket: str
    main_queue: str
    dead_letter_queue: str
    sqs: Any

    def owner(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {OWNER_TOKEN}"}

    def accept(self, request: Mapping[str, Any] | None = None, **body: Any) -> Any:
        payload: dict[str, Any] = {
            "request": dict(self.request if request is None else request),
            "compiledPlan": compiled_plan(),
        }
        payload.update(body)
        return self.client.post("/api/v1/backtests", json=payload, headers=self.owner())

    def visible(self, queue_url: str) -> int:
        attributes = self.sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["ApproximateNumberOfMessages"]
        )["Attributes"]
        return int(attributes["ApproximateNumberOfMessages"])


def build_stack(
    *,
    persistence: BacktestPersistence,
    sqs_client: Any,
    s3_client: Any,
    queues: tuple[str, str],
    bucket: str,
    root: Path,
    closes: tuple[str, ...] = CLOSES,
    monitor: Any | None = None,
    plans: Mapping[str, Mapping[str, Any]] | None = None,
    manifests: Mapping[Any, Mapping[str, Any]] | None = None,
    policies: ExecutionPolicyCatalog | None = None,
    owners: Mapping[Any, Any] | None = None,
    request: Mapping[str, Any] | None = None,
) -> Stack:
    """Wire the production adapters to the supplied real infrastructure.

    Every port is overridable, because D91 needs to accept B's *published*
    message -- whose bot, dataset manifest and release quarter differ from the
    runnable 2024-Q1 fixture -- without a second copy of this wiring.
    """
    main, dead = queues
    market_data = write_market_data(root, closes)
    plan = compiled_plan()
    accepted_request = (
        dict(request)
        if request is not None
        else official_backtest_request(
            plan=plan,
            expected_dataset_hash=f"sha256:{market_data.manifest['dataset_hash']}",
            # This traversal pins the independently hand-checked RSI arithmetic
            # from market bars. Verified materialized-feature consumption has its
            # own object-boundary suite in test_feature_outputs.py.
            include_feature_materializations=False,
        )
    )

    plan_source = StaticCompiledPlanSource(
        dict(plans) if plans is not None else {plan["planChecksum"]: plan}
    )
    manifest_source = StaticDatasetManifestSource(
        dict(manifests)
        if manifests is not None
        else {DATASET_MANIFEST_ID: market_data.manifest}
    )
    policy_catalog = policies if policies is not None else ExecutionPolicyCatalog([E2E_EXECUTION_POLICY])
    dead_letters = InMemoryDeadLetterQueue()

    lifecycle = BacktestLifecycleService(
        gateway=PersistenceRunGateway(persistence),
        queue=SqsBacktestJobQueue(sqs_client, main),
        owners=StaticOwnerDirectory(dict(owners) if owners is not None else {BOT_ID: ACCOUNT_ID}),
        plans=plan_source,
        manifests=manifest_source,
        policies=policy_catalog,
        dead_letters=dead_letters,
    )
    authenticator = StaticTokenAuthenticator(
        {
            OWNER_TOKEN: Principal(account_id=ACCOUNT_ID),
            WORKER_TOKEN: Principal(
                account_id=ACCOUNT_ID, scopes=frozenset({RESULT_INGEST_SCOPE})
            ),
        }
    )
    store = S3ObjectStore(bucket, client=s3_client, sleep=lambda _seconds: None)
    # The D29 join. The same `persistence` and the same bucket the worker publishes
    # into are what the read model reads back, so `GET /monthly-trades` and
    # `GET /inputs` answer from the rows and objects the run actually wrote.
    # `create_app(lifecycle, authenticator)` with no query service - which is what
    # this wiring did before D29 landed - leaves those two routes answering 503, and
    # no integration module can then assert that a published run is readable.
    recorder = HeaderRecorder(
        create_app(
            lifecycle,
            authenticator,
            build_result_query_service(persistence, store),
            allow_test_provider_creation=True,
        )
    )
    client = TestClient(recorder)
    sink = HttpResultSink(client, WORKER_TOKEN)

    handler = OrchestratorJobHandler(
        persistence=persistence,
        policies=policy_catalog,
        plans=plan_source,
        manifests=manifest_source,
        reader=ParquetMarketDataReader(market_data.root),
        calendar=XNYS_CALENDAR,
        object_store=store,
        storage_write_port=PersistenceStorageObjectWritePort(persistence),
        sink=sink,
        attempt_policy=ATTEMPT_POLICY,
        monitor=monitor or ScriptedMonitor(),
        microstructure=E2E_MICROSTRUCTURE,
        fractional_policy=E2E_FRACTIONAL_POLICY,
        risk_limits=E2E_RISK_LIMITS,
        runtime=BasicPlanRuntime(),
        # Pinned: `result_hash` covers `calculated_at`, so the wall clock is an
        # input of the run rather than an accident of when it ran. The replay
        # clock is 2024 market time and is untouched by this.
        wall_clock=lambda: COMPLETED_AT,
        correlation_id=CORRELATION_ID,
    )
    worker = BacktestWorker(
        client=sqs_client,
        config=WorkerConfig(
            queue_url=main,
            dead_letter_queue_url=dead,
            worker_id="d-int-worker",
            max_receive_count=3,
            visibility_timeout=timedelta(seconds=30),
            wait_time=timedelta(seconds=1),
            max_messages=1,
            heartbeat_interval=timedelta(seconds=10),
        ),
        handler=handler,
        store=PersistenceExecutionKeyStore(persistence),
    )
    return Stack(
        client=client,
        recorder=recorder,
        sink=sink,
        worker=worker,
        handler=handler,
        lifecycle=lifecycle,
        store=store,
        dead_letters=dead_letters,
        request=accepted_request,
        market_data=market_data,
        bucket=bucket,
        main_queue=main,
        dead_letter_queue=dead,
        sqs=sqs_client,
    )


# ---------------------------------------------------------------------------
# Reading the world back, never through the writer
# ---------------------------------------------------------------------------


def sql_one(engine: Engine, statement: str, **params: Any) -> Any:
    with engine.connect() as connection:
        return connection.execute(text(statement), params).mappings().one()


def sql_all(engine: Engine, statement: str, **params: Any) -> list[Any]:
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(statement), params).mappings()]


def sql_scalar(engine: Engine, statement: str, **params: Any) -> Any:
    with engine.connect() as connection:
        return connection.execute(text(statement), params).scalar_one()


def fetch_object(s3_client: Any, bucket_name: str, key: str) -> bytes:
    body: bytes = s3_client.get_object(Bucket=bucket_name, Key=key)["Body"].read()
    return body


def truncate_backtest(engine: Engine) -> None:
    """Empty `backtest.*`, leaving the reference seed and stored objects alone."""
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "TRUNCATE TABLE backtest.run_input_pins, "
            "backtest.failure_condition_counts, "
            "backtest.monthly_judgment_summaries, backtest.performance_summaries, "
            "backtest.detail_manifests, backtest.input_datasets, "
            "backtest.input_feature_materializations, backtest.input_bundles, "
            "backtest.run_attempts, backtest.runs RESTART IDENTITY CASCADE"
        )


def policy_unavailable(*_args: Any, **_kwargs: Any) -> None:  # pragma: no cover - guard
    raise AssertionError("this port must not be consulted in this test")
