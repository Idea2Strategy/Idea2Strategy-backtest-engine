from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from backtest_engine.execution_policy import (
    D17_EXECUTION_POLICY_FIXTURE,
    ExecutionPolicy,
    ExecutionPolicyCatalog,
    ExecutionPolicyUnavailable,
    et_quarter_start,
)
from backtest_engine.money import PRECISION_RULES_VERSION


FEE_POLICY_ID = "00000000-0000-4000-8000-000000000001"
BUFFER_POLICY_ID = "00000000-0000-4000-8000-000000000002"


def _policy(
    quarter: str,
    version: str,
    period_start: datetime,
    period_end: datetime,
    **overrides: object,
) -> ExecutionPolicy:
    fields: dict[str, object] = {
        "version": version,
        "release_quarter": quarter,
        "period_start": period_start,
        "period_end": period_end,
        "fee_rate": Decimal("0.002"),
        "slippage_rate_bps": 5,
        "timezone": "America/New_York",
        "session_calendar": "XNYS",
        "timestamp_unit": "us",
        "price_arrow_type": "double",
        "volume_arrow_type": "int64",
        "market_data_schema_version": "market-bars-v2",
        "calculation_model_version": "backtest-calculation-v1",
        "market_rules_version": "market:1.0.0",
        "accounting_rules_version": "accounting:1.0.0",
        "precision_rules_version": PRECISION_RULES_VERSION,
        "fee_policy_id": FEE_POLICY_ID,
        "buying_power_buffer_policy_id": BUFFER_POLICY_ID,
        "good_till_cancelled_horizon": timedelta(days=90),
        "max_order_horizon": timedelta(days=90),
    }
    fields.update(overrides)
    return ExecutionPolicy(**fields)  # type: ignore[arg-type]


def _three_year_policy(quarter: str, version: str) -> ExecutionPolicy:
    return _policy(
        quarter,
        version,
        et_quarter_start(2021, 1),
        et_quarter_start(2024, 1),
    )


# --------------------------------------------------------------------------
# Order horizons: the former hidden default (execution_model.py:440 pre-rebuild
# hardcoded a 90-day GTC expiry inside the matching engine).
# --------------------------------------------------------------------------


def test_order_horizons_are_required_policy_fields_with_no_default() -> None:
    """A run's GTC expiry is pinned by its policy, never assumed by the engine."""

    fields = ExecutionPolicy.__dataclass_fields__
    for name in ("good_till_cancelled_horizon", "max_order_horizon"):
        assert fields[name].default is dataclasses.MISSING, name
        assert fields[name].default_factory is dataclasses.MISSING, name

    with pytest.raises(TypeError, match="good_till_cancelled_horizon"):
        incomplete = {
            key: value
            for key, value in dataclasses.asdict(D17_EXECUTION_POLICY_FIXTURE).items()
            if key != "good_till_cancelled_horizon"
        }
        ExecutionPolicy(**incomplete)  # type: ignore[arg-type]


def test_the_published_fixture_pins_the_canonical_ninety_day_horizons() -> None:
    # db/schema.dbml trading.orders check `order_expiry_within_ninety_days`.
    assert D17_EXECUTION_POLICY_FIXTURE.good_till_cancelled_horizon == timedelta(days=90)
    assert D17_EXECUTION_POLICY_FIXTURE.max_order_horizon == timedelta(days=90)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"good_till_cancelled_horizon": timedelta(0)}, "positive horizon"),
        ({"good_till_cancelled_horizon": timedelta(seconds=-1)}, "positive horizon"),
        ({"max_order_horizon": timedelta(0)}, "positive horizon"),
        ({"good_till_cancelled_horizon": 90}, "positive horizon"),
        (
            {
                "good_till_cancelled_horizon": timedelta(days=120),
                "max_order_horizon": timedelta(days=90),
            },
            "must not exceed",
        ),
    ],
)
def test_unusable_order_horizons_are_refused(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            et_quarter_start(2024, 1),
            et_quarter_start(2024, 2),
            **overrides,
        )


def test_a_policy_may_pin_a_horizon_shorter_than_the_canonical_maximum() -> None:
    policy = _policy(
        "2024-Q1",
        "short-horizon-policy",
        et_quarter_start(2024, 1),
        et_quarter_start(2024, 2),
        good_till_cancelled_horizon=timedelta(days=7),
    )

    assert policy.good_till_cancelled_horizon == timedelta(days=7)
    assert policy.max_order_horizon == timedelta(days=90)


# --------------------------------------------------------------------------
# ET quarter derivation (kept: it was already correct)
# --------------------------------------------------------------------------


def test_et_quarter_start_pins_the_utc_instant_across_dst() -> None:
    # America/New_York is UTC-5 in January and UTC-4 in April/July.
    assert et_quarter_start(2024, 1) == datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc)
    assert et_quarter_start(2024, 2) == datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc)
    assert et_quarter_start(2024, 3) == datetime(2024, 7, 1, 4, 0, tzinfo=timezone.utc)
    assert et_quarter_start(2024, 4) == datetime(2024, 10, 1, 4, 0, tzinfo=timezone.utc)
    assert et_quarter_start(2025, 1) == datetime(2025, 1, 1, 5, 0, tzinfo=timezone.utc)


def test_same_et_release_quarter_selects_the_same_published_policy() -> None:
    q1 = _three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")
    catalog = ExecutionPolicyCatalog([q1])

    early_release = datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc)
    late_release = datetime(2024, 4, 1, 3, 59, tzinfo=timezone.utc)

    assert catalog.select(early_release) is q1
    assert catalog.select(late_release) is q1


def test_et_quarter_boundary_selects_the_next_published_policy() -> None:
    q1 = _three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")
    q2 = _three_year_policy("2024-Q2", "official-backtest-policy-2024-q2-v1")
    catalog = ExecutionPolicyCatalog([q1, q2])

    before_boundary = datetime(2024, 4, 1, 3, 59, tzinfo=timezone.utc)
    at_boundary = datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc)

    assert catalog.select(before_boundary) is q1
    assert catalog.select(at_boundary) is q2


def test_catalog_rejects_mid_quarter_policy_replacement() -> None:
    original = _three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")
    replacement = _policy(
        "2024-Q1",
        "official-backtest-policy-2024-q1-v2",
        et_quarter_start(2020, 1),
        et_quarter_start(2024, 1),
    )

    with pytest.raises(ValueError, match="2024-Q1"):
        ExecutionPolicyCatalog([original, replacement])


def test_catalog_rejects_unpublished_quarter_and_naive_release_time() -> None:
    catalog = ExecutionPolicyCatalog(
        [_three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")]
    )

    with pytest.raises(ExecutionPolicyUnavailable, match="2024-Q2"):
        catalog.select(datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc))

    with pytest.raises(ValueError, match="timezone-aware"):
        catalog.select(datetime(2024, 1, 1, 0, 0))


# --------------------------------------------------------------------------
# The period must actually be a quarterly period
# --------------------------------------------------------------------------


def test_policy_rejects_an_intraday_period_presented_as_a_quarter() -> None:
    # The pre-rebuild D17 fixture declared 2024-Q1 with a 60-minute period.
    with pytest.raises(ValueError, match="local calendar-day boundaries"):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc),
            datetime(2024, 1, 2, 15, 30, tzinfo=timezone.utc),
        )


def test_policy_accepts_a_complete_et_calendar_month() -> None:
    policy = _policy(
        "2024-Q1",
        "official-backtest-policy-v1",
        et_quarter_start(2024, 1),
        datetime(2024, 2, 1, 5, 0, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError, match="not aligned to ET quarter"):
        _ = policy.quarter_count


def test_policy_accepts_the_fixed_maximum_local_data_window() -> None:
    policy = _policy(
        "2026-Q3",
        "official-backtest-policy-max-range-v1",
        datetime(2016, 1, 1, 5, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 30, 4, 0, tzinfo=timezone.utc),
    )

    assert policy.period_start == datetime(2016, 1, 1, 5, 0, tzinfo=timezone.utc)
    assert policy.period_end == datetime(2026, 7, 30, 4, 0, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="not aligned to ET quarter"):
        _ = policy.quarter_count


def test_policy_rejects_a_reversed_period() -> None:
    with pytest.raises(ValueError, match="increasing"):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            et_quarter_start(2024, 2),
            et_quarter_start(2024, 1),
        )


def test_policy_accepts_exactly_one_calendar_quarter() -> None:
    policy = _policy(
        "2024-Q1",
        "official-backtest-policy-v1",
        et_quarter_start(2024, 1),
        et_quarter_start(2024, 2),
    )

    assert policy.quarter_count == 1


def test_policy_reports_the_number_of_evaluated_quarters() -> None:
    assert _three_year_policy("2024-Q1", "v1").quarter_count == 12


# --------------------------------------------------------------------------
# Canonical backtest.runs columns
# --------------------------------------------------------------------------


def test_slippage_is_stored_in_basis_points_and_read_as_a_rate() -> None:
    policy = _policy(
        "2024-Q1",
        "official-backtest-policy-v1",
        et_quarter_start(2024, 1),
        et_quarter_start(2024, 2),
    )

    assert policy.slippage_rate_bps == 5
    assert policy.slippage_rate == Decimal("0.0005")


def test_slippage_rate_conversion_is_exact_for_other_basis_point_values() -> None:
    policy = _policy(
        "2024-Q1",
        "official-backtest-policy-v1",
        et_quarter_start(2024, 1),
        et_quarter_start(2024, 2),
        slippage_rate_bps=125,
    )

    assert policy.slippage_rate == Decimal("0.0125")


def test_policy_rejects_negative_rates() -> None:
    with pytest.raises(ValueError, match="must not be negative"):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            et_quarter_start(2024, 1),
            et_quarter_start(2024, 2),
            slippage_rate_bps=-1,
        )


def test_policy_rejects_a_precision_rule_set_this_build_does_not_implement() -> None:
    with pytest.raises(ValueError, match="precision:2.0.0"):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            et_quarter_start(2024, 1),
            et_quarter_start(2024, 2),
            precision_rules_version="precision:2.0.0",
        )


def test_policy_exposes_the_canonical_run_columns() -> None:
    policy = _policy(
        "2024-Q1",
        "official-backtest-policy-v1",
        et_quarter_start(2024, 1),
        et_quarter_start(2024, 2),
    )

    assert policy.run_columns() == {
        "market_rules_version": "market:1.0.0",
        "accounting_rules_version": "accounting:1.0.0",
        "precision_rules_version": "precision:1.0.0",
        "fee_policy_id": FEE_POLICY_ID,
        "slippage_rate_bps": 5,
        "buying_power_buffer_policy_id": BUFFER_POLICY_ID,
    }


def test_policy_rejects_a_non_uuid_fee_policy_id() -> None:
    with pytest.raises(ValueError, match="fee_policy_id"):
        _policy(
            "2024-Q1",
            "official-backtest-policy-v1",
            et_quarter_start(2024, 1),
            et_quarter_start(2024, 2),
            fee_policy_id="fee-policy-1",
        )


# --------------------------------------------------------------------------
# Version lookup (the API a later stage wires into lifecycle.accept)
# --------------------------------------------------------------------------


def test_catalog_resolves_a_requested_policy_version() -> None:
    q1 = _three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")
    q2 = _three_year_policy("2024-Q2", "official-backtest-policy-2024-q2-v1")
    catalog = ExecutionPolicyCatalog([q1, q2])

    assert catalog.get("official-backtest-policy-2024-q2-v1") is q2
    assert catalog.versions == (
        "official-backtest-policy-2024-q1-v1",
        "official-backtest-policy-2024-q2-v1",
    )


def test_catalog_rejects_an_unpublished_policy_version_without_substitution() -> None:
    catalog = ExecutionPolicyCatalog(
        [_three_year_policy("2024-Q1", "official-backtest-policy-2024-q1-v1")]
    )

    with pytest.raises(ExecutionPolicyUnavailable, match="official-backtest-policy-v9"):
        catalog.get("official-backtest-policy-v9")


def test_catalog_rejects_two_policies_sharing_one_version_identifier() -> None:
    q1 = _three_year_policy("2024-Q1", "official-backtest-policy-shared")
    q2 = _three_year_policy("2024-Q2", "official-backtest-policy-shared")

    with pytest.raises(ValueError, match="official-backtest-policy-shared"):
        ExecutionPolicyCatalog([q1, q2])


# --------------------------------------------------------------------------
# The published fixture must satisfy the invariants above
# --------------------------------------------------------------------------


def test_published_fixture_period_is_the_full_2024_q1_calendar_quarter() -> None:
    policy = D17_EXECUTION_POLICY_FIXTURE

    assert policy.quarter_count == 1
    assert policy.period_start == datetime(2024, 1, 1, 5, 0, tzinfo=timezone.utc)
    assert policy.period_end == datetime(2024, 4, 1, 4, 0, tzinfo=timezone.utc)
    assert ExecutionPolicyCatalog([policy]).select(
        datetime(2024, 2, 14, 12, 0, tzinfo=timezone.utc)
    ) is policy


def test_published_fixture_binds_precision_to_the_money_module() -> None:
    assert (
        D17_EXECUTION_POLICY_FIXTURE.precision_rules_version == PRECISION_RULES_VERSION
    )
    assert D17_EXECUTION_POLICY_FIXTURE.slippage_rate == Decimal("0.0005")
