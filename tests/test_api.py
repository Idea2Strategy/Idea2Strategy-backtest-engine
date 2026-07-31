from backtest_engine.api import create_app


def test_health_route_is_registered() -> None:
    routes = {route.path for route in create_app().routes}

    assert "/health" in routes
