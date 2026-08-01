"""Pinned execution-policy fixtures for reproducible official backtests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal


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
