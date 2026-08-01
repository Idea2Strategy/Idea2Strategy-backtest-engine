from datetime import datetime, timezone
from decimal import Decimal

import pytest

from backtest_engine.execution_policy import (
    ExecutionPolicy,
    ExecutionPolicyCatalog,
    ExecutionPolicyUnavailable,
)


def _policy(quarter: str, version: str, start_year: int) -> ExecutionPolicy:
    return ExecutionPolicy(
        version=version,
        release_quarter=quarter,
        period_start=datetime(start_year, 1, 2, 14, 30, tzinfo=timezone.utc),
        period_end=datetime(start_year + 3, 1, 1, 21, 0, tzinfo=timezone.utc),
        fee_rate=Decimal("0.002"),
        slippage_rate=Decimal("0.0005"),
        timezone="America/New_York",
        session_calendar="XNYS",
        timestamp_unit="us",
        price_arrow_type="double",
        volume_arrow_type="int64",
        market_data_schema_version="market-bars-v2",
        calculation_model_version="backtest-calculation-v1",
    )


def test_same_et_release_quarter_selects_the_same_published_policy() -> None:
    q1 = _policy("2024-Q1", "official-backtest-policy-2024-q1-v1", 2021)
    catalog = ExecutionPolicyCatalog([q1])

    early_release = datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc)
    late_release = datetime(2024, 4, 1, 3, 59, tzinfo=timezone.utc)

    assert catalog.select(early_release) is q1
    assert catalog.select(late_release) is q1


def test_et_quarter_boundary_selects_the_next_published_policy() -> None:
    q1 = _policy("2024-Q1", "official-backtest-policy-2024-q1-v1", 2021)
    q2 = _policy("2024-Q2", "official-backtest-policy-2024-q2-v1", 2021)
    catalog = ExecutionPolicyCatalog([q1, q2])

    before_boundary = datetime(2024, 4, 1, 3, 59, tzinfo=timezone.utc)
    at_boundary = datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc)

    assert catalog.select(before_boundary) is q1
    assert catalog.select(at_boundary) is q2


def test_catalog_rejects_mid_quarter_policy_replacement() -> None:
    original = _policy("2024-Q1", "official-backtest-policy-2024-q1-v1", 2021)
    replacement = _policy("2024-Q1", "official-backtest-policy-2024-q1-v2", 2020)

    with pytest.raises(ValueError, match="2024-Q1"):
        ExecutionPolicyCatalog([original, replacement])


def test_catalog_rejects_unpublished_quarter_and_naive_release_time() -> None:
    catalog = ExecutionPolicyCatalog(
        [_policy("2024-Q1", "official-backtest-policy-2024-q1-v1", 2021)]
    )

    with pytest.raises(ExecutionPolicyUnavailable, match="2024-Q2"):
        catalog.select(datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc))

    with pytest.raises(ValueError, match="timezone-aware"):
        catalog.select(datetime(2024, 1, 1, 0, 0))
