"""Pinned execution policies for reproducible official backtests.

A published policy carries every column ``backtest.runs`` pins for a run
(``db/schema.dbml``): ``market_rules_version``, ``accounting_rules_version``,
``precision_rules_version``, ``fee_policy_id``, ``slippage_rate_bps`` and
``buying_power_buffer_policy_id``. ``precision_rules_version`` is bound to
:mod:`backtest_engine.money`, the single quantization point.

Two different quarters live in this module and must not be conflated:

``release_quarter``
    The ET calendar quarter of strategy releases this policy governs. It is the
    catalog selection key.
``period_start`` / ``period_end``
    The pinned evaluation window. It is independent of ``release_quarter``:
    Development may publish a shorter immutable evaluation period when that is
    the complete locked dataset available for the release.

Order horizons
--------------
``good_till_cancelled_horizon`` and ``max_order_horizon`` are required fields
with no defaults. Before the rebuild the matching engine carried a hardcoded
90-day GTC expiry, which meant every run silently inherited a policy nobody had
published. A GTC order's lifetime is a property of the run's policy, so it is
pinned here and the engine reads it; there is no fallback if a policy omits it,
because a policy cannot omit it.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import MappingProxyType
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .money import PRECISION_RULES_VERSION


ET = ZoneInfo("America/New_York")
BASIS_POINTS = Decimal(10_000)

__all__ = [
    "BASIS_POINTS",
    "D17_EXECUTION_POLICY_FIXTURE",
    "ET",
    "ExecutionPolicy",
    "ExecutionPolicyCatalog",
    "ExecutionPolicyUnavailable",
    "et_quarter_start",
    "release_quarter_of",
]


class ExecutionPolicyUnavailable(LookupError):
    """Raised when no official policy was published for a release quarter."""


def et_quarter_start(year: int, quarter: int) -> datetime:
    """UTC instant at which the given ET calendar quarter begins."""
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"quarter must be 1-4, got {quarter}")
    month = 3 * (quarter - 1) + 1
    return datetime(year, month, 1, tzinfo=ET).astimezone(timezone.utc)


def _quarter_index(instant: datetime) -> int | None:
    """Absolute quarter number if ``instant`` is exactly an ET quarter start."""
    local = instant.astimezone(ET)
    if local.month not in (1, 4, 7, 10):
        return None
    if (local.day, local.hour, local.minute, local.second, local.microsecond) != (
        1,
        0,
        0,
        0,
        0,
    ):
        return None
    return local.year * 4 + (local.month - 1) // 3


def release_quarter_of(moment: datetime) -> str:
    """``YYYY-QN`` label of the ET calendar quarter containing ``moment``."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("strategy_released_at must be timezone-aware")
    local = moment.astimezone(ET)
    return f"{local.year}-Q{((local.month - 1) // 3) + 1}"


@dataclass(frozen=True, slots=True)
class ExecutionPolicy:
    version: str
    release_quarter: str
    period_start: datetime
    period_end: datetime
    fee_rate: Decimal
    slippage_rate_bps: int
    timezone: str
    session_calendar: str
    timestamp_unit: str
    price_arrow_type: str
    volume_arrow_type: str
    market_data_schema_version: str
    calculation_model_version: str
    market_rules_version: str
    accounting_rules_version: str
    fee_policy_id: str
    buying_power_buffer_policy_id: str
    #: How long a GTC order stays live. Canonical basis: `db/schema.dbml`
    #: `trading.orders` check `order_expiry_within_ninety_days`.
    good_till_cancelled_horizon: timedelta
    #: The longest expiry any order of any time-in-force may carry.
    max_order_horizon: timedelta
    precision_rules_version: str = PRECISION_RULES_VERSION

    def __post_init__(self) -> None:
        if not self.version:
            raise ValueError("execution policy version must not be empty")
        if (
            self.period_start.tzinfo != timezone.utc
            or self.period_end.tzinfo != timezone.utc
        ):
            raise ValueError("execution policy period must use UTC")
        if self.period_start >= self.period_end:
            raise ValueError("execution policy period must be increasing")
        try:
            policy_zone = ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError(f"execution policy timezone is invalid: {self.timezone!r}") from exc
        for field_name in ("period_start", "period_end"):
            local = getattr(self, field_name).astimezone(policy_zone)
            if (local.hour, local.minute, local.second, local.microsecond) != (0, 0, 0, 0):
                raise ValueError(
                    "execution policy period must use local calendar-day boundaries: "
                    f"{field_name}={local.isoformat()}"
                )

        if self.fee_rate < 0:
            raise ValueError("execution policy rates must not be negative")
        if self.slippage_rate_bps < 0:
            raise ValueError("execution policy rates must not be negative")
        if not isinstance(self.slippage_rate_bps, int) or isinstance(
            self.slippage_rate_bps, bool
        ):
            raise ValueError("slippage_rate_bps must be an integer basis-point count")

        for field_name in ("good_till_cancelled_horizon", "max_order_horizon"):
            horizon = getattr(self, field_name)
            if not isinstance(horizon, timedelta) or horizon <= timedelta(0):
                raise ValueError(
                    f"{field_name} must be a positive horizon, got {horizon!r}"
                )
        if self.good_till_cancelled_horizon > self.max_order_horizon:
            raise ValueError(
                "good_till_cancelled_horizon must not exceed max_order_horizon"
            )

        if self.precision_rules_version != PRECISION_RULES_VERSION:
            raise ValueError(
                "this build implements only "
                f"{PRECISION_RULES_VERSION}, not {self.precision_rules_version}"
            )
        for field_name in ("market_rules_version", "accounting_rules_version"):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must not be empty")
        for field_name in ("fee_policy_id", "buying_power_buffer_policy_id"):
            value = getattr(self, field_name)
            try:
                uuid.UUID(str(value))
            except (ValueError, AttributeError) as exc:
                raise ValueError(f"{field_name} must be a UUID, got {value!r}") from exc

    @property
    def quarter_count(self) -> int:
        """Number of whole ET quarters, for quarter-aligned policy fixtures."""
        start_index = _quarter_index(self.period_start)
        end_index = _quarter_index(self.period_end)
        if start_index is None or end_index is None:
            raise ValueError("execution policy period is not aligned to ET quarter boundaries")
        return end_index - start_index

    @property
    def slippage_rate(self) -> Decimal:
        """The basis-point column read back as an exact decimal rate."""
        return Decimal(self.slippage_rate_bps) / BASIS_POINTS

    def run_columns(self) -> dict[str, object]:
        """The ``backtest.runs`` policy columns this policy pins."""
        return {
            "market_rules_version": self.market_rules_version,
            "accounting_rules_version": self.accounting_rules_version,
            "precision_rules_version": self.precision_rules_version,
            "fee_policy_id": self.fee_policy_id,
            "slippage_rate_bps": self.slippage_rate_bps,
            "buying_power_buffer_policy_id": self.buying_power_buffer_policy_id,
        }


class ExecutionPolicyCatalog:
    """Selects an immutable pre-published policy by release quarter or version."""

    def __init__(self, policies: Iterable[ExecutionPolicy]) -> None:
        by_quarter: dict[str, ExecutionPolicy] = {}
        by_version: dict[str, ExecutionPolicy] = {}
        for policy in policies:
            if policy.release_quarter in by_quarter:
                raise ValueError(
                    f"execution policy already published for {policy.release_quarter}"
                )
            if policy.version in by_version:
                raise ValueError(
                    f"execution policy version is not unique: {policy.version}"
                )
            by_quarter[policy.release_quarter] = policy
            by_version[policy.version] = policy
        self._by_quarter = MappingProxyType(by_quarter)
        self._by_version = MappingProxyType(by_version)

    @property
    def versions(self) -> tuple[str, ...]:
        return tuple(sorted(self._by_version))

    def select(self, strategy_released_at: datetime) -> ExecutionPolicy:
        quarter = release_quarter_of(strategy_released_at)
        try:
            return self._by_quarter[quarter]
        except KeyError as exc:
            raise ExecutionPolicyUnavailable(
                f"execution policy is not published for {quarter}"
            ) from exc

    def get(self, version: str) -> ExecutionPolicy:
        """Resolve a requested ``execution_policy_version`` with no substitution."""
        try:
            return self._by_version[version]
        except KeyError as exc:
            raise ExecutionPolicyUnavailable(
                f"execution policy version is not published: {version}"
            ) from exc


D17_EXECUTION_POLICY_FIXTURE = ExecutionPolicy(
    version="official-backtest-policy-v1",
    release_quarter="2024-Q1",
    period_start=et_quarter_start(2024, 1),
    period_end=et_quarter_start(2024, 2),
    fee_rate=Decimal("0.002"),
    slippage_rate_bps=5,
    timezone="America/New_York",
    session_calendar="XNYS",
    timestamp_unit="us",
    price_arrow_type="double",
    volume_arrow_type="int64",
    market_data_schema_version="market-bars-v2",
    calculation_model_version="backtest-calculation-v1",
    market_rules_version="market:1.0.0",
    accounting_rules_version="accounting:1.0.0",
    fee_policy_id="00000000-0000-4000-8000-000000000001",
    buying_power_buffer_policy_id="00000000-0000-4000-8000-000000000001",
    # db/schema.dbml trading.orders check `order_expiry_within_ninety_days`:
    # the canonical model refuses any live order expiry beyond 90 days after
    # acceptance, so a replayed GTC order may not outlive that boundary either.
    good_till_cancelled_horizon=timedelta(days=90),
    max_order_horizon=timedelta(days=90),
    precision_rules_version=PRECISION_RULES_VERSION,
)
