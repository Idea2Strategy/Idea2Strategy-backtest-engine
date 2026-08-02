"""`backtest-api` must be a runnable process, not a `NotImplementedError`.

`pyproject.toml` declares a `backtest-api` console script. Until now it raised, while
`backtest-worker` had real wiring, so the half of D29 that *serves* results had no way
to be deployed at all - and `wiring.py` built no `BacktestResultQueryStore`, so even a
hand-assembled deployment answered 503 on the two result read routes.

These tests are Docker-free on purpose: `build_api_runtime` performs no I/O, so a
configuration mistake is caught before anything connects. The database and object
store are exercised in `tests/persistence/test_durable_result_query.py` and in the
end-to-end suite.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from backtest_engine.api import API_PREFIX, Principal, StaticTokenAuthenticator
from backtest_engine.execution_policy import D17_EXECUTION_POLICY_FIXTURE, ExecutionPolicyCatalog
from backtest_engine.lifecycle import (
    InMemoryDeadLetterQueue,
    StaticCompiledPlanSource,
    StaticDatasetManifestSource,
    StaticOwnerDirectory,
)
from backtest_engine.object_store import LocalObjectStore
from backtest_engine.result_query import (
    BacktestResultQueryService,
    DurableBacktestResultQueryStore,
)
from backtest_engine.wiring import (
    API_REQUIRED_ENV,
    WiringError,
    build_api_runtime,
    build_result_query_service,
)


TOKEN = "api-wiring-token"
ACCOUNT_ID = UUID("66666666-6666-4666-8666-666666666666")

#: Set by `_environment` so the object-store factory below has somewhere to point.
_OBJECT_STORE_ROOT: list[Path] = []


# ---------------------------------------------------------------------------
# Factories the environment names. `package.module:factory`, exactly as
# BACKTEST_JOB_HANDLER already works for the worker.
# ---------------------------------------------------------------------------


def authenticator() -> StaticTokenAuthenticator:
    return StaticTokenAuthenticator({TOKEN: Principal(account_id=ACCOUNT_ID)})


def object_store() -> LocalObjectStore:
    return LocalObjectStore(_OBJECT_STORE_ROOT[-1], bucket_name="api-wiring-bucket")


def owners() -> StaticOwnerDirectory:
    return StaticOwnerDirectory({})


def plans() -> StaticCompiledPlanSource:
    return StaticCompiledPlanSource({})


def manifests() -> StaticDatasetManifestSource:
    return StaticDatasetManifestSource({})


def policies() -> ExecutionPolicyCatalog:
    return ExecutionPolicyCatalog([D17_EXECUTION_POLICY_FIXTURE])


def dead_letters() -> InMemoryDeadLetterQueue:
    return InMemoryDeadLetterQueue()


def not_an_object_store() -> str:
    return "a bucket name is not an object store"


def _environment(tmp_path: Path, **overrides: str) -> dict[str, str]:
    _OBJECT_STORE_ROOT.append(tmp_path / "objects")
    settings = {
        "BACKTEST_DATABASE_URL": "postgresql+psycopg://user:pw@localhost:5432/idea2strategy",
        "BACKTEST_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/000000000000/backtest",
        "BACKTEST_API_HOST": "127.0.0.1",
        "BACKTEST_API_PORT": "8080",
        "BACKTEST_AUTHENTICATOR": "test_api_wiring:authenticator",
        "BACKTEST_OBJECT_STORE": "test_api_wiring:object_store",
        "BACKTEST_OWNER_DIRECTORY": "test_api_wiring:owners",
        "BACKTEST_COMPILED_PLAN_SOURCE": "test_api_wiring:plans",
        "BACKTEST_DATASET_MANIFEST_SOURCE": "test_api_wiring:manifests",
        "BACKTEST_EXECUTION_POLICY_CATALOG": "test_api_wiring:policies",
        "BACKTEST_DEAD_LETTER_SINK": "test_api_wiring:dead_letters",
        # boto3 refuses to build an SQS client without a region; not a backtest setting.
        "AWS_DEFAULT_REGION": "us-east-1",
    }
    settings.update(overrides)
    return settings


# ---------------------------------------------------------------------------
# Fail fast
# ---------------------------------------------------------------------------


def test_every_required_setting_is_reported_at_once_when_the_environment_is_empty() -> None:
    with pytest.raises(WiringError) as raised:
        build_api_runtime({})

    message = str(raised.value)
    for name in API_REQUIRED_ENV:
        assert name in message, f"{name} is required but was not named in the failure"


@pytest.mark.parametrize("name", API_REQUIRED_ENV)
def test_no_required_setting_has_a_silent_default(tmp_path: Path, name: str) -> None:
    """Dropping any one of them must stop the process, not pick something."""

    environment = _environment(tmp_path)
    del environment[name]

    with pytest.raises(WiringError, match=name):
        build_api_runtime(environment)


@pytest.mark.parametrize("port", ["", "http", "0", "70000", "8080.0"])
def test_a_port_that_is_not_a_tcp_port_is_refused(tmp_path: Path, port: str) -> None:
    with pytest.raises(WiringError, match="BACKTEST_API_PORT"):
        build_api_runtime(_environment(tmp_path, BACKTEST_API_PORT=port))


def test_a_factory_target_that_does_not_resolve_names_the_setting(tmp_path: Path) -> None:
    with pytest.raises(WiringError, match="BACKTEST_OWNER_DIRECTORY"):
        build_api_runtime(
            _environment(tmp_path, BACKTEST_OWNER_DIRECTORY="test_api_wiring:no_such_factory")
        )
    with pytest.raises(WiringError, match="BACKTEST_AUTHENTICATOR"):
        build_api_runtime(
            _environment(tmp_path, BACKTEST_AUTHENTICATOR="backtest_engine.api")
        )


def test_an_object_store_factory_that_returns_something_else_is_refused(tmp_path: Path) -> None:
    """A string bucket name satisfies `str` and nothing the read model needs."""

    with pytest.raises(WiringError, match="BACKTEST_OBJECT_STORE"):
        build_api_runtime(
            _environment(tmp_path, BACKTEST_OBJECT_STORE="test_api_wiring:not_an_object_store")
        )


# ---------------------------------------------------------------------------
# What a complete environment produces
# ---------------------------------------------------------------------------


def test_a_complete_environment_builds_an_app_with_the_result_read_model(
    tmp_path: Path,
) -> None:
    """The gap this closes: `create_app` now receives a real query service.

    Asserted through the routing table and the service's own store type rather than a
    return value: a `create_app(lifecycle, authenticator)` with no third argument
    produces an app whose two D29 routes answer 503 forever.
    """

    runtime = build_api_runtime(_environment(tmp_path))

    assert runtime.host == "127.0.0.1"
    assert runtime.port == 8080
    assert runtime.object_store.bucket_name == "api-wiring-bucket"
    paths = {route.path for route in runtime.app.routes}  # type: ignore[attr-defined]
    assert f"{API_PREFIX}/backtests/{{run_id}}/monthly-trades" in paths
    assert f"{API_PREFIX}/backtests/{{run_id}}/inputs" in paths

    service = _injected_result_service(runtime.app)
    assert isinstance(service, BacktestResultQueryService)
    assert isinstance(service._store, DurableBacktestResultQueryStore)


def test_build_result_query_service_binds_the_persistence_and_bucket_it_is_given(
    tmp_path: Path,
) -> None:
    runtime = build_api_runtime(_environment(tmp_path))

    service = build_result_query_service(runtime.persistence, runtime.object_store)

    store = service._store
    assert isinstance(store, DurableBacktestResultQueryStore)
    assert store._persistence is runtime.persistence
    assert store._store is runtime.object_store


def test_building_the_runtime_opens_no_connection(tmp_path: Path) -> None:
    """The database URL points nowhere reachable, and building still succeeds.

    Construction and verification are separate steps so a configuration error is
    reported as one, and a database that is merely down is reported as the other.
    Nothing here connects, so no timeout is involved: `create_engine` is lazy and
    `build_api_runtime` never asks it for a connection.
    """

    runtime = build_api_runtime(
        _environment(
            tmp_path,
            BACKTEST_DATABASE_URL="postgresql+psycopg://nobody:nobody@203.0.113.1:5432/nothing",
        )
    )

    assert runtime.persistence.engine.url.host == "203.0.113.1"
    assert runtime.persistence.engine.pool.checkedout() == 0


def test_the_console_script_entry_point_is_runnable(monkeypatch: pytest.MonkeyPatch) -> None:
    """`backtest-api` used to raise `NotImplementedError` unconditionally.

    It now fails only for the reason a misconfigured deployment should fail for, and
    with the list of settings it needs. Serving is not exercised here - that is
    uvicorn's - but everything up to the bind is.
    """

    from backtest_engine import api

    monkeypatch.setattr(api.os, "environ", {}, raising=True)

    with pytest.raises(WiringError, match="BACKTEST_DATABASE_URL"):
        api.run()


def test_the_entry_point_verifies_the_schema_before_it_serves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Order matters: drift must stop start-up, not every request afterwards.

    The runtime applies no DDL, so there is nothing to repair; serving anyway means a
    process that passes its health check and 500s on every query route. Both halves are
    asserted - `verify_schema` is reached, and `uvicorn.run` is not.
    """

    from backtest_engine import api
    from backtest_engine.persistence import BacktestPersistence
    from backtest_engine.persistence.errors import SchemaDriftError

    served: list[Any] = []
    verified: list[bool] = []

    def drifted(self: BacktestPersistence) -> None:
        verified.append(True)
        raise SchemaDriftError("backtest.runs.result_hash: column is missing")

    monkeypatch.setattr(api.os, "environ", _environment(tmp_path), raising=True)
    monkeypatch.setattr(BacktestPersistence, "verify_schema", drifted)
    import uvicorn

    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: served.append(kwargs))

    with pytest.raises(SchemaDriftError, match="column is missing"):
        api.run()

    assert verified == [True]
    assert served == [], "the process must not start serving against an unverified schema"


def test_the_entry_point_serves_the_wired_app_on_the_configured_address(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And when the schema does verify, it serves - the app, the host and the port."""

    from backtest_engine import api
    from backtest_engine.persistence import BacktestPersistence

    served: list[tuple[Any, dict[str, Any]]] = []
    monkeypatch.setattr(api.os, "environ", _environment(tmp_path), raising=True)
    monkeypatch.setattr(BacktestPersistence, "verify_schema", lambda self: None)
    import uvicorn

    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: served.append((app, kwargs)))

    api.run()

    assert len(served) == 1
    app, options = served[0]
    assert options == {"host": "127.0.0.1", "port": 8080}
    assert isinstance(_injected_result_service(app), BacktestResultQueryService)


def _injected_result_service(app: Any) -> Any:
    """Pull the `BacktestResultQueryService` back out of the closure `create_app` built.

    `create_app` closes over its arguments rather than storing them, so this reads the
    route handler's own closure. Reaching for it is deliberate: the alternative is
    asserting on an HTTP response, and this test must distinguish "no service" (503)
    from "a service that has nothing to serve", which look the same from outside.
    """

    for route in app.routes:
        if getattr(route, "path", "").endswith("/monthly-trades"):
            closure = route.endpoint.__closure__ or ()
            for cell in closure:
                if isinstance(cell.cell_contents, BacktestResultQueryService):
                    return cell.cell_contents
    raise AssertionError("the monthly-trades route closes over no result query service")
