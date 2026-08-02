"""The terminal ``EMIT_ORDER_CANDIDATE`` element (card D22).

``LOAD_FEATURE`` and ``COMPARE`` decide *whether* an instrument qualifies;
``EMIT_ORDER_CANDIDATE`` decides *what* is handed to the execution layer for the
instruments that qualified. It is the only element that produces an artefact
outside the evaluation scratchpad, so it is the only place where the fields
:mod:`backtest_engine.execution_model` needs are assembled.

Why the step is re-read here rather than trusted from the loader
----------------------------------------------------------------
``side``, ``orderType`` and ``allocation`` are *step arguments*, not runtime
configuration. Reading them from the step at emission time means a plan that
says ``SELL`` can never emit a ``BUY``, whatever the caller passes, and a build
that does not implement ``LIMIT`` refuses rather than downgrading to ``MARKET``.

What a candidate is not
-----------------------
A candidate is not a fill and not an order. It carries no quantity: quantity
depends on cash, budget caps and price at fill time, which belong to the
execution model. It carries the *inputs* that decision needs:

``allocation``
    The exact ``Fraction`` share of the buy budget this instrument was assigned
    by equal allocation. ``None`` for a sell, whose size comes from the held
    position rather than from a budget.
``reference_price``
    The close of the last bar completed at the decision instant, already
    quantized under ``precision:1.0.0``. It is the look-ahead-safe price the
    execution model starts from; it is not a fill price.
``eligible_at`` / ``session_closes_at``
    The window in which the execution model may act. A candidate decided at or
    after the session close is rejected here rather than silently expiring.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from fractions import Fraction
from uuid import UUID

from backtest_engine.elements.core import (
    ElementCompatibilityError,
    ElementEvaluationError,
    PlanLoadFailure,
    PlanStep,
)
from backtest_engine.money import is_quantized_money


__all__ = [
    "SUPPORTED_ALLOCATION_MODES",
    "SUPPORTED_ORDER_TYPES",
    "SUPPORTED_SIDES",
    "TERMINAL_OPERATION",
    "OrderCandidate",
    "emit_order_candidate",
]


TERMINAL_OPERATION = "EMIT_ORDER_CANDIDATE"

SUPPORTED_SIDES: tuple[str, ...] = ("BUY", "SELL")
SUPPORTED_ORDER_TYPES: tuple[str, ...] = ("MARKET",)
SUPPORTED_ALLOCATION_MODES: tuple[str, ...] = ("EQUAL",)

BUDGET_CAP_BPS_MIN = 1
BUDGET_CAP_BPS_MAX = 10000
"""``basic-compiled-plan.v1`` bounds ``budgetCapBps`` to 1..10000 (0.01%..100%)."""


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ElementEvaluationError(f"{label} must be a non-empty string")
    return value


def _instant(value: object, label: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ElementEvaluationError(f"{label} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _reject_argument(detail: str) -> ElementCompatibilityError:
    return ElementCompatibilityError(
        PlanLoadFailure.UNSUPPORTED_ELEMENT_ARGUMENT, detail
    )


@dataclass(frozen=True, slots=True)
class OrderCandidate:
    """One instrument's surviving decision, ready for the execution model."""

    evaluation_id: str
    instrument_id: str
    partition_key: str
    flow_id: str
    side: str
    order_type: str
    allocation: Fraction | None
    reference_price: Decimal
    decided_at: datetime
    eligible_at: datetime
    session_date_et: date
    session_closes_at: datetime
    budget_cap_bps: int

    def __post_init__(self) -> None:
        _text(self.evaluation_id, "evaluation_id")
        _text(self.partition_key, "partition_key")
        _text(self.flow_id, "flow_id")
        try:
            object.__setattr__(
                self, "instrument_id", str(UUID(_text(self.instrument_id, "instrument_id")))
            )
        except ValueError as exc:
            raise ElementEvaluationError("instrument_id must be a UUID") from exc

        if self.side not in SUPPORTED_SIDES:
            raise ElementEvaluationError(
                f"side must be one of {', '.join(SUPPORTED_SIDES)}, got {self.side!r}"
            )
        if self.order_type not in SUPPORTED_ORDER_TYPES:
            raise ElementEvaluationError(
                "order_type must be one of "
                f"{', '.join(SUPPORTED_ORDER_TYPES)}, got {self.order_type!r}"
            )

        if self.allocation is not None:
            if not isinstance(self.allocation, Fraction):
                raise ElementEvaluationError(
                    "allocation must be an exact Fraction, not a float"
                )
            if not Fraction(0) < self.allocation <= Fraction(1):
                raise ElementEvaluationError(
                    f"allocation must lie in (0, 1], got {self.allocation}"
                )

        if not is_quantized_money(self.reference_price):
            raise ElementEvaluationError(
                "reference_price must be quantized under precision:1.0.0, got "
                f"{self.reference_price!r}"
            )
        if self.reference_price <= 0:
            raise ElementEvaluationError(
                f"reference_price must be positive, got {self.reference_price}"
            )

        decided_at = _instant(self.decided_at, "decided_at")
        eligible_at = _instant(self.eligible_at, "eligible_at")
        session_closes_at = _instant(self.session_closes_at, "session_closes_at")
        if eligible_at < decided_at:
            raise ElementEvaluationError(
                "eligible_at must not precede decided_at: "
                f"{eligible_at.isoformat()} < {decided_at.isoformat()}"
            )
        if decided_at >= session_closes_at:
            raise ElementEvaluationError(
                "decided_at must precede session_closes_at: "
                f"{decided_at.isoformat()} >= {session_closes_at.isoformat()}"
            )
        object.__setattr__(self, "decided_at", decided_at)
        object.__setattr__(self, "eligible_at", eligible_at)
        object.__setattr__(self, "session_closes_at", session_closes_at)

        if not isinstance(self.session_date_et, date) or isinstance(
            self.session_date_et, datetime
        ):
            raise ElementEvaluationError("session_date_et must be a date, not a datetime")

        if (
            not isinstance(self.budget_cap_bps, int)
            or isinstance(self.budget_cap_bps, bool)
            or not BUDGET_CAP_BPS_MIN <= self.budget_cap_bps <= BUDGET_CAP_BPS_MAX
        ):
            raise ElementEvaluationError(
                f"budget_cap_bps must be an integer in "
                f"[{BUDGET_CAP_BPS_MIN}, {BUDGET_CAP_BPS_MAX}], "
                f"got {self.budget_cap_bps!r}"
            )


def emit_order_candidate(
    step: PlanStep,
    *,
    evaluation_id: str,
    instrument_id: str,
    partition_key: str,
    flow_id: str,
    budget_cap_bps: int,
    allocation: Fraction | None,
    reference_price: Decimal,
    decided_at: datetime,
    eligible_at: datetime,
    session_date_et: date,
    session_closes_at: datetime,
) -> OrderCandidate:
    """Realise ``step`` for one instrument that passed every condition step.

    ``step`` must be the plan's terminal ``EMIT_ORDER_CANDIDATE``; ``side``,
    ``orderType`` and ``allocation`` are read from it, never from the caller.
    """
    if step.operation != TERMINAL_OPERATION:
        raise ElementEvaluationError(
            f"only a {TERMINAL_OPERATION} step can emit an order candidate, "
            f"got {step.operation!r}"
        )

    allocation_mode = step.argument("allocation")
    order_type = step.argument("orderType")
    side = step.argument("side")

    if allocation_mode not in SUPPORTED_ALLOCATION_MODES:
        raise _reject_argument(
            f"{TERMINAL_OPERATION} argument allocation={allocation_mode!r} is not "
            "one of " + ", ".join(SUPPORTED_ALLOCATION_MODES)
        )
    if order_type not in SUPPORTED_ORDER_TYPES:
        raise _reject_argument(
            f"{TERMINAL_OPERATION} argument orderType={order_type!r} is not one of "
            + ", ".join(SUPPORTED_ORDER_TYPES)
        )
    if side not in SUPPORTED_SIDES:
        raise _reject_argument(
            f"{TERMINAL_OPERATION} argument side={side!r} is not one of "
            + ", ".join(SUPPORTED_SIDES)
        )

    if side == "BUY" and allocation is None:
        raise ElementEvaluationError(
            f"a {allocation_mode}-allocated BUY candidate requires an allocation "
            "share; emitting one without a share would invent a position size"
        )
    if side == "SELL" and allocation is not None:
        raise ElementEvaluationError(
            "a SELL candidate must not carry a buy allocation share: its size "
            "comes from the held position, not from the buy budget"
        )

    return OrderCandidate(
        evaluation_id=evaluation_id,
        instrument_id=instrument_id,
        partition_key=partition_key,
        flow_id=flow_id,
        side=side,
        order_type=order_type,
        allocation=allocation,
        reference_price=reference_price,
        decided_at=decided_at,
        eligible_at=eligible_at,
        session_date_et=session_date_et,
        session_closes_at=session_closes_at,
        budget_cap_bps=budget_cap_bps,
    )
