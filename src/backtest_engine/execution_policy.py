"""Pinned execution-policy fixtures for reproducible official backtests."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from types import MappingProxyType
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")


class ExecutionPolicyUnavailable(LookupError):
    """Raised when no official policy was published for a release quarter."""


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    version: str
    release_quarter: str
    period_start: datetime
    period_end: datetime
    fee_rate: Decimal
    slippage_rate: Decimal
    timezone: str
    session_calendar: str
    timestamp_unit: str
    price_arrow_type: str
    volume_arrow_type: str
    market_data_schema_version: str
    calculation_model_version: str

    def __post_init__(self) -> None:
        if (
            self.period_start.tzinfo != timezone.utc
            or self.period_end.tzinfo != timezone.utc
        ):
            raise ValueError("execution policy period must use UTC")
        if self.period_start >= self.period_end:
            raise ValueError("execution policy period must be increasing")
        if self.fee_rate < 0 or self.slippage_rate < 0:
            raise ValueError("execution policy rates must not be negative")


def _release_quarter(strategy_released_at: datetime) -> str:
    if (
        strategy_released_at.tzinfo is None
        or strategy_released_at.utcoffset() is None
    ):
        raise ValueError("strategy_released_at must be timezone-aware")
    released_at_et = strategy_released_at.astimezone(ET)
    quarter = ((released_at_et.month - 1) // 3) + 1
    return f"{released_at_et.year}-Q{quarter}"


class ExecutionPolicyCatalog:
    """Selects an immutable pre-published policy by the release's ET quarter."""

    def __init__(self, policies: Iterable[ExecutionPolicy]) -> None:
        by_quarter: dict[str, ExecutionPolicy] = {}
        for policy in policies:
            if policy.release_quarter in by_quarter:
                raise ValueError(
                    f"execution policy already published for {policy.release_quarter}"
                )
            by_quarter[policy.release_quarter] = policy
        self._by_quarter = MappingProxyType(by_quarter)

    def select(self, strategy_released_at: datetime) -> ExecutionPolicy:
        quarter = _release_quarter(strategy_released_at)
        try:
            return self._by_quarter[quarter]
        except KeyError as exc:
            raise ExecutionPolicyUnavailable(
                f"execution policy is not published for {quarter}"
            ) from exc


D17_EXECUTION_POLICY_FIXTURE = ExecutionPolicy(
    version="official-backtest-policy-v1",
    release_quarter="2024-Q1",
    period_start=datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
    period_end=datetime(2024, 1, 2, 15, 30, tzinfo=timezone.utc),
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
