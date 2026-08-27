"""The `/api/v1` HTTP surface for the backtest engine (D28, D29).

Eight owner-scoped query endpoints, one acceptance endpoint for B's
`OFFICIAL_BACKTEST_REQUESTED`, and one result-ingestion endpoint for the worker.

Two read models sit behind those queries and they are not interchangeable:

* `lifecycle.BacktestLifecycleService` is the **write** model - the `backtest.runs`
  row, its attempts, and the summary rows a completed run wrote. It answers the run
  list, the run itself, attempts, performance, monthly summaries and detail manifests.
* `result_query.BacktestResultQueryService` is the **result** read model. It is the
  only thing that can answer `monthly-trades`, because trade rows live in the ET
  Monday week Parquet parts rather than in any `backtest.*` table, and the ET
  week -> ET month join is its responsibility. It also answers `inputs`, which
  reports the pinned reproducibility boundary and, for an `UNAVAILABLE` run, the
  requirements that were missing.

Authentication and authorisation are separate concerns here, and the difference is
visible in the status codes:

* **401** - the request carried no usable credential. The `Authorization` header was
  absent, malformed, or names a token this deployment does not know.
* **403** - the credential is valid but the run belongs to a different account.

Returning 404 for a foreign run would hide the distinction and is a defensible choice
in some products, but it also hides genuine authorisation bugs from the client and from
this test suite, so the boundary is explicit.

**409, not 404, for "the run has no result yet".** The five evidence routes -
`performance`, `monthly-summaries`, `detail-manifests`, `monthly-trades` and the model
half of `inputs` - all answer `409 Conflict` with
`reasonCode: BACKTEST_RESULT_NOT_READY` while the run has not reached `COMPLETED`.
They previously disagreed: `performance` answered 404, `monthly-trades` answered 409
through the result read model, and the two list routes answered `200 {"items": []}`.
Three answers to one question is three branches in a UI that only has two things to do
- give up, or poll - and the empty list was the worst of them, because a finished run
that traded nothing looks exactly the same.

404 keeps one meaning across the whole surface: *there is no such run, or it is not
yours*. 409 means *the run is yours and this evidence does not exist yet, so come
back*. The status is repeated in the body so a client does not have to re-read the run
to know which. Emptiness is never a reason for 409: a `COMPLETED` run with no detail
objects returns `200 {"items": []}`, which is the true answer.

**The two result-read-model routes deliberately answer 404 instead.** They serve a
run's evidence rather than its metadata, so confirming that a run id exists and that
another account finished it is a disclosure in itself. `result_query` fails closed the
same way - `QueryNotFound` covers "no such run" and "not yours" alike - and the route
keeps that semantic rather than re-deriving ownership from the write model.

There is no default authenticator and no default gateway: `create_app` requires both.
A backtest API that silently falls back to an in-memory store, or to "any token works",
is precisely the class of empty implementation this rebuild exists to remove. The
result read model is optional only because a deployment may serve acceptance and
ingestion without it; when it is absent the routes answer **503** naming what is
missing, never 404 and never an empty list.

Result ingestion is written for at-least-once delivery:

* Replaying a byte-identical event returns the first outcome with `applied: false`.
* Replaying a *different* event under the same idempotency key is `409`.
* An `If-Match` that no longer matches the run is `412`, and the response body carries
  the run's current state so a worker whose previous response was lost can reconcile
  rather than guess.
* A structurally invalid event is poison; it is dead-lettered rather than retried.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from .contracts import ContractValidationError
from .lifecycle import (
    BacktestLifecycleService,
    BacktestRun,
    BacktestRunNotFound,
    IdempotencyConflict,
    InvalidStatusTransition,
    NotRunOwner,
    PreconditionFailed,
    RequestNotSatisfiable,
)
from .monthly_judgment import EtMonth
from .persistence.rows import (
    DetailManifestRow,
    MonthlyJudgmentSummaryRow,
    PerformanceSummaryRow,
    RunAttemptRow,
    RunRow,
    RunStatus,
)
from .result_query import (
    BacktestOverview,
    BacktestResultQueryService,
    InputModelView,
    PerformanceSeriesView,
    QueryIntegrityError,
    QueryNotFound,
    QueryNotReady,
    QueryValidationError,
    TradeDetailView,
)
from .result_snapshot import PositionAfter


__all__ = [
    "API_PREFIX",
    "Authenticator",
    "Principal",
    "StaticTokenAuthenticator",
    "create_app",
    "run",
]


API_PREFIX = "/api/v1"


@dataclass(frozen=True, slots=True)
class Principal:
    """An authenticated caller."""

    account_id: UUID
    scopes: frozenset[str] = frozenset()

    def has(self, scope: str) -> bool:
        return scope in self.scopes


class Authenticator(Protocol):
    """Resolves a bearer token to a principal, or `None` if it is not valid."""

    def authenticate(self, token: str) -> Principal | None: ...


@dataclass(frozen=True, slots=True)
class StaticTokenAuthenticator:
    """Explicit token to principal map.

    Suitable for tests and for a deployment that injects tokens from a secret store.
    There is deliberately no wildcard and no "development mode" bypass.
    """

    principals: Mapping[str, Principal]

    def authenticate(self, token: str) -> Principal | None:
        return self.principals.get(token)


#: Scope required to publish worker results. Query endpoints need only ownership.
RESULT_INGEST_SCOPE = "backtest:results:write"


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------


def _run_payload(run: BacktestRun) -> dict[str, Any]:
    row: RunRow = run.run
    return {
        "backtestRunId": str(row.id),
        "botId": str(row.bot_id),
        "ownerAccountId": None if row.owner_account_id is None else str(row.owner_account_id),
        "status": row.status.value,
        "configurationHash": row.configuration_hash,
        "evaluationStart": row.evaluation_start.isoformat(),
        "evaluationEnd": row.evaluation_end.isoformat(),
        "initialCashAmount": f"{row.initial_cash_amount:.8f}",
        "marketRulesVersion": row.market_rules_version,
        "accountingRulesVersion": row.accounting_rules_version,
        "precisionRulesVersion": row.precision_rules_version,
        "feePolicyId": str(row.fee_policy_id),
        "slippageRateBps": row.slippage_rate_bps,
        "buyingPowerBufferPolicyId": str(row.buying_power_buffer_policy_id),
        "idempotencyKey": row.idempotency_key,
        "queuedAt": _iso(row.queued_at),
        "startedAt": _iso(row.started_at),
        "completedAt": _iso(row.completed_at),
        "failureCode": row.failure_code,
        "resultHash": row.result_hash,
        "cancellationRequestedAt": _iso(row.cancellation_requested_at),
        "cancellationReasonCode": row.cancellation_reason_code,
        "cancelledAt": _iso(row.cancelled_at),
        "deletionRequestedAt": _iso(row.deletion_requested_at),
        "deletedAt": _iso(row.deleted_at),
        "attemptCount": len(run.attempts),
    }


def _attempt_payload(row: RunAttemptRow) -> dict[str, Any]:
    return {
        "attemptId": str(row.id),
        "backtestRunId": str(row.run_id),
        "attemptNumber": row.attempt_number,
        "workerExecutionKey": row.worker_execution_key,
        "status": row.status.value,
        "startedAt": _iso(row.started_at),
        "completedAt": _iso(row.completed_at),
        "failureCode": row.failure_code,
    }


def _performance_payload(row: PerformanceSummaryRow) -> dict[str, Any]:
    return {
        "backtestRunId": str(row.run_id),
        "metricCatalogVersion": row.metric_catalog_version,
        "metricsDocument": dict(row.metrics_document),
        "calculationRulesVersion": row.calculation_rules_version,
        "sourceSetHash": row.source_set_hash,
        "inputHash": row.input_hash,
        "resultHash": row.result_hash,
        "calculatedAt": _iso(row.calculated_at),
    }


def _monthly_payload(row: MonthlyJudgmentSummaryRow) -> dict[str, Any]:
    return {
        "monthlySummaryId": str(row.id),
        "backtestRunId": str(row.run_id),
        "etYearMonth": row.et_year_month,
        "evaluationCount": row.evaluation_count,
        "activeBranchCount": row.active_branch_count,
        "tradeEventCount": row.trade_event_count,
        "dataGapCount": row.data_gap_count,
        "triggeredCount": row.triggered_count,
        "rejectedCount": row.rejected_count,
        "summaryDocument": dict(row.summary_document),
        "summaryHash": row.summary_hash,
    }


def _manifest_payload(row: DetailManifestRow) -> dict[str, Any]:
    return {
        "manifestId": str(row.id),
        "backtestRunId": str(row.run_id),
        "objectId": str(row.object_id),
        "recordType": row.record_type,
        "weekStartDate": row.week_start_date.isoformat(),
        "periodStart": _iso(row.period_start),
        "periodEnd": _iso(row.period_end),
        "partNumber": row.part_number,
        "rowCount": row.row_count,
        "schemaVersion": row.schema_version,
        "sourceSetHash": row.source_set_hash,
        "supersedesManifestId": str(row.supersedes_manifest_id) if row.supersedes_manifest_id else None,
        "detailHash": row.detail_hash,
        "createdAt": _iso(row.created_at),
    }


def _trade_payload(view: TradeDetailView) -> dict[str, Any]:
    """One `TradeDetailView` row. Every amount is `numeric(24,8)` text, never a float.

    JSON has one number type and it is binary floating point, so an amount serialised
    as a number is a rounding decision taken by whichever runtime parses it. The
    quantised eight-place string is the same form `_run_payload` uses for cash.
    """
    return {
        "recordId": view.record_id,
        "occurredAt": _iso(view.occurred_at),
        "kind": view.kind,
        "orderId": view.order_id,
        "instrumentId": view.instrument_id,
        "orderStatus": view.order_status,
        "cashAfter": _amount(view.cash_after),
        "positionsAfter": [_position_payload(item) for item in view.positions_after],
        "reasonCode": view.reason_code,
        "fillId": view.fill_id,
        "quantity": _amount(view.quantity),
        "basePrice": _amount(view.base_price),
        "price": _amount(view.price),
        "grossAmount": _amount(view.gross_amount),
        "slippageAmount": _amount(view.slippage_amount),
        "fee": _amount(view.fee),
        "costBasis": _amount(view.cost_basis),
        "realizedPnl": _amount(view.realized_pnl),
    }


def _performance_series_payload(view: PerformanceSeriesView) -> dict[str, Any]:
    return {
        "backtestRunId": view.run_id,
        "points": [
            {"occurredAt": _iso(point.occurred_at), "equity": _amount(point.equity)}
            for point in view.points
        ],
        "resultHash": view.result_hash,
        "sourceSetHash": view.source_set_hash,
    }


def _position_payload(position: PositionAfter) -> dict[str, Any]:
    return {
        "instrumentId": position.instrument_id,
        "quantity": _amount(position.quantity),
        "costBasis": _amount(position.cost_basis),
    }


def _input_model_payload(view: InputModelView, overview: BacktestOverview) -> dict[str, Any]:
    """Card D29's "입력 데이터·모델과 unavailable 이유", in one response.

    The two halves are one answer: an `UNAVAILABLE` run has the pinned inputs but no
    model versions, and the only thing worth reporting about it is what was missing.
    The model versions stay `null` in that case rather than being filled with a
    plausible-looking default.
    """
    market_bars = tuple(item for item in view.datasets if item.purpose_code == "MARKET_BARS")
    if not market_bars:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="canonical input bundle must contain at least one MARKET_BARS dataset",
        )
    primary = market_bars[0]
    return {
        "backtestRunId": view.run_id,
        "botId": view.bot_id,
        "status": overview.status,
        "strategySnapshotHash": view.strategy_snapshot_hash,
        "compiledPlanChecksum": view.compiled_plan_checksum,
        # Kept for client compatibility as a deterministic representative; the
        # complete immutable input set is always returned in datasets.
        "datasetManifestId": primary.dataset_manifest_id,
        "datasetHash": primary.locked_dataset_hash,
        "datasets": [
            {
                "datasetManifestId": item.dataset_manifest_id,
                "purposeCode": item.purpose_code,
                "lockedDatasetHash": item.locked_dataset_hash,
            }
            for item in view.datasets
        ],
        "featureMaterializations": [
            {
                "featureMaterializationId": item.feature_materialization_id,
                "lockedResultHash": item.locked_result_hash,
            }
            for item in view.feature_materializations
        ],
        "inputBundleFingerprint": view.input_bundle_fingerprint,
        "inputContractVersion": view.input_contract_version,
        "executionPolicyVersion": view.execution_policy_version,
        "precisionRulesVersion": view.precision_rules_version,
        "calculationModelVersion": view.calculation_model_version,
        "costModelVersion": view.cost_model_version,
        "executionModelVersion": view.execution_model_version,
        "reasonCode": overview.reason_code,
        "missingRequirements": list(overview.missing_requirements),
    }


def _amount(value: Decimal | None) -> str | None:
    return None if value is None else f"{value:.8f}"


def _iso(value: Any) -> str | None:
    return None if value is None else value.isoformat().replace("+00:00", "Z")


#: `et_month` accepts exactly the `backtest.monthly_judgment_summaries.et_year_month`
#: form. Year 0000 is excluded because `EtMonth` requires a positive year and a
#: rejected value must be rejected by the parameter contract, not by an exception
#: raised deeper in the read model.
_ET_MONTH_PATTERN = re.compile(r"(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])")


def _parse_et_month(value: str) -> EtMonth:
    if _ET_MONTH_PATTERN.fullmatch(value) is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": (
                    "et_month must be a single ET calendar month written YYYY-MM, "
                    f"got {value!r}"
                ),
                "reasonCode": "ET_MONTH_MALFORMED",
                "parameter": "et_month",
            },
        )
    year, _, month = value.partition("-")
    return EtMonth(int(year), int(month))


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


def create_app(
    lifecycle: BacktestLifecycleService,
    authenticator: Authenticator,
    results: BacktestResultQueryService | None = None,
    *,
    allow_test_provider_creation: bool = False,
) -> FastAPI:
    """Build the `/api/v1` application.

    `lifecycle` and `authenticator` are required. See the module docstring for why
    there is no default for either, and why `results` may be omitted but is never
    substituted.
    """
    if lifecycle is None:
        raise ValueError("a BacktestLifecycleService is required")
    if authenticator is None:
        raise ValueError("an Authenticator is required")

    app = FastAPI(title="Idea2Strategy Backtest API", version="1.0.0")

    def current_principal(authorization: str | None = Header(default=None)) -> Principal:
        if not authorization:
            raise _unauthenticated("an Authorization header is required")
        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise _unauthenticated("only 'Authorization: Bearer <token>' is accepted")
        principal = authenticator.authenticate(token.strip())
        if principal is None:
            raise _unauthenticated("the bearer token is not recognised")
        return principal

    Auth = Depends(current_principal)

    @app.get("/health", tags=["operations"])
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "backtest-api"}

    # -- acceptance -------------------------------------------------------

    @app.post(f"{API_PREFIX}/backtests", status_code=status.HTTP_202_ACCEPTED, tags=["backtests"])
    def accept_backtest(
        body: dict[str, Any],
        response: Response,
        principal: Principal = Auth,
    ) -> dict[str, Any]:
        """Test-only compatibility path for B's `OFFICIAL_BACKTEST_REQUESTED`.

        The body is `{"request": <strategy-bot.v1 message>, "compiledPlan": <plan>}`.
        Production always returns 405 because Backend owns Run creation. In explicitly
        enabled test fixtures the plan is optional: when omitted it is resolved through the configured
        `CompiledPlanSource`, and the request is rejected if neither supplies it.
        """
        if not allow_test_provider_creation:
            raise HTTPException(
                status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                detail=(
                    "Run identity and immutable input pins are created by the Backend "
                    "provider transaction; the Backtest API is consumer-only"
                ),
            )
        request_document = body.get("request", body)
        compiled_plan = body.get("compiledPlan")
        try:
            accepted = lifecycle.accept(request_document, compiled_plan=compiled_plan)
        except RequestNotSatisfiable as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": str(exc),
                    "reasonCode": exc.reason_code,
                    "missingRequirements": list(exc.missing),
                },
            ) from exc
        except ContractValidationError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

        run = accepted.run
        if run.owner_account_id != principal.account_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="the authenticated account does not own the bot this request names",
            )
        response.headers["ETag"] = run.etag
        response.headers["Location"] = f"{API_PREFIX}/backtests/{run.backtest_run_id}"
        return {
            "run": _run_payload(run),
            "created": accepted.created,
            "dispatched": accepted.dispatched,
        }

    # -- queries ----------------------------------------------------------

    @app.get(f"{API_PREFIX}/backtests", tags=["backtests"])
    def list_backtests(
        limit: int = 50,
        offset: int = 0,
        principal: Principal = Auth,
    ) -> dict[str, Any]:
        """Query 1/8: the authenticated owner's runs. Never another account's."""
        if not 1 <= limit <= 200:
            raise HTTPException(status_code=422, detail="limit must be between 1 and 200")
        if offset < 0:
            raise HTTPException(status_code=422, detail="offset must not be negative")
        runs = lifecycle.list_runs(principal.account_id, limit=limit, offset=offset)
        return {
            "items": [_run_payload(run) for run in _with_attempt_counts(lifecycle, runs)],
            "limit": limit,
            "offset": offset,
        }

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}", tags=["backtests"])
    def get_backtest(run_id: UUID, response: Response, principal: Principal = Auth) -> dict[str, Any]:
        """Query 2/8: one run."""
        run = _owned(lifecycle, run_id, principal)
        response.headers["ETag"] = run.etag
        return _run_payload(run)

    @app.post(
        f"{API_PREFIX}/backtests/{{run_id}}/cancellation",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["backtests"],
    )
    def cancel_backtest(
        run_id: UUID,
        body: dict[str, Any] | None = None,
        principal: Principal = Auth,
    ) -> dict[str, Any]:
        """Owner cancellation: queued is immediate, running stops at the next heartbeat."""
        reason = str((body or {}).get("reasonCode", "USER_CANCELLED")).strip()
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", reason):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="reasonCode must be an uppercase token of at most 80 characters",
            )
        try:
            run = lifecycle.request_cancellation(
                run_id,
                owner_account_id=principal.account_id,
                reason_code=reason,
            )
        except BacktestRunNotFound as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except NotRunOwner as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        except InvalidStatusTransition as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        return {"run": _run_payload(run), "cancellationRequested": True}

    @app.delete(
        f"{API_PREFIX}/backtests/{{run_id}}",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["backtests"],
    )
    def delete_backtest(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Owner delete with evidence retention and cooperative running cancellation."""
        try:
            run = lifecycle.request_deletion(
                run_id,
                owner_account_id=principal.account_id,
            )
        except BacktestRunNotFound as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except NotRunOwner as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        return {
            "run": _run_payload(run),
            "deletionRequested": True,
            "deleted": run.run.deleted_at is not None,
        }

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/attempts", tags=["backtests"])
    def get_attempts(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Query 3/8: the durable attempt history behind a run."""
        _owned(lifecycle, run_id, principal)
        attempts = lifecycle.attempts_of(run_id, owner_account_id=principal.account_id)
        return {"items": [_attempt_payload(row) for row in attempts]}

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/performance", tags=["backtests"])
    def get_performance(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Query 4/8: the immutable performance summary, once the run completed."""
        _require_completed(_owned(lifecycle, run_id, principal))
        summary = lifecycle.performance_of(run_id, owner_account_id=principal.account_id)
        if summary is None:
            # COMPLETED with no summary row is not "not ready"; it is a run whose
            # publish wrote the status without the evidence.
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"backtest run {run_id} is COMPLETED but has no performance summary",
            )
        return _performance_payload(summary)

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/monthly-summaries", tags=["backtests"])
    def get_monthly(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Query 5/8: per-ET-month judgment summaries with all six canonical counters."""
        _require_completed(_owned(lifecycle, run_id, principal))
        summaries = lifecycle.monthly_of(run_id, owner_account_id=principal.account_id)
        return {"items": [_monthly_payload(row) for row in summaries]}

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/performance-series", tags=["backtests"])
    def get_performance_series(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Official mark-to-market equity points reconstructed from immutable Parquet."""
        service = _result_query(results)
        try:
            view = service.performance_series(str(principal.account_id), str(run_id))
        except QueryNotFound as exc:
            # Acceptance and immutable-result publication are separate transactions.
            # Before publication the result projection legitimately has no row, so use
            # the write model only to distinguish "owned and pending" from not found.
            try:
                pending = lifecycle.get(run_id, owner_account_id=principal.account_id)
            except (BacktestRunNotFound, NotRunOwner):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="backtest not found") from exc
            _require_completed(pending)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"backtest run {run_id} is COMPLETED but has no performance series",
            ) from exc
        except QueryNotReady as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"message": str(exc), "reasonCode": RESULT_NOT_READY_REASON},
            ) from exc
        except QueryValidationError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
        except QueryIntegrityError as exc:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
        return _performance_series_payload(view)

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/detail-manifests", tags=["backtests"])
    def get_manifests(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Query 6/8: detail object manifests - ET Monday weeks plus `partNumber`.

        These are the evidence *objects*. The rows inside them are `monthly-trades`.
        """
        _require_completed(_owned(lifecycle, run_id, principal))
        manifests = lifecycle.manifests_of(run_id, owner_account_id=principal.account_id)
        return {"items": [_manifest_payload(row) for row in manifests]}

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/monthly-trades", tags=["backtests"])
    def get_monthly_trades(
        run_id: UUID, et_month: str, principal: Principal = Auth
    ) -> dict[str, Any]:
        """Query 7/8: one ET month of trade detail (D29 "월별 거래 상세").

        `et_month` is required and is never defaulted: a month picked on the caller's
        behalf would silently answer a question nobody asked. The read model reads
        every ET Monday week part that *overlaps* the month and places each row by the
        ET month of its own instant, then cross-checks the result against the month's
        judgment summary, so a lost or tampered part fails closed rather than
        returning a short list.
        """
        service = _result_query(results)
        month = _parse_et_month(et_month)
        with _query_errors():
            trades = service.monthly_trades(
                str(principal.account_id), str(run_id), month
            )
        return {
            "backtestRunId": str(run_id),
            "etMonth": month.key,
            "items": [_trade_payload(view) for view in trades],
        }

    @app.get(f"{API_PREFIX}/backtests/{{run_id}}/inputs", tags=["backtests"])
    def get_inputs(run_id: UUID, principal: Principal = Auth) -> dict[str, Any]:
        """Query 8/8: pinned inputs, model versions and the unavailable reason (D29).

        Available at every status. An `UNAVAILABLE` run reports null model versions
        and the requirements it could not resolve; `GET /{run_id}` carries the
        `failureCode` but not the requirement list, so this is the only place the UI
        can say *what* was missing.
        """
        service = _result_query(results)
        with _query_errors():
            owner = str(principal.account_id)
            return _input_model_payload(
                service.inputs_and_models(owner, str(run_id)),
                service.overview(owner, str(run_id)),
            )

    # -- result ingestion --------------------------------------------------

    @app.post(f"{API_PREFIX}/backtests/{{run_id}}/results", tags=["backtests"])
    def ingest_result(
        run_id: UUID,
        event: dict[str, Any],
        request: Request,
        response: Response,
        if_match: str | None = Header(default=None, alias="If-Match"),
        principal: Principal = Auth,
    ) -> Any:
        """Apply a `backtest.v1` result event published by a worker."""
        if not principal.has(RESULT_INGEST_SCOPE):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"the {RESULT_INGEST_SCOPE} scope is required to publish results",
            )
        if str(event.get("backtestRunId")) != str(run_id):
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="the event's backtestRunId does not match the path",
            )

        delivery_attempt = _delivery_attempt(request)
        try:
            outcome = lifecycle.ingest_result(
                event, if_match=if_match, delivery_attempt=delivery_attempt
            )
        except BacktestRunNotFound as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except PreconditionFailed as exc:
            return JSONResponse(
                status_code=status.HTTP_412_PRECONDITION_FAILED,
                headers={"ETag": exc.current.etag},
                content={
                    "detail": str(exc),
                    "current": _run_payload(exc.current),
                },
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except InvalidStatusTransition as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ContractValidationError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

        response.headers["ETag"] = outcome.run.etag
        return {"run": _run_payload(outcome.run), "applied": outcome.applied}

    return app


def _owned(lifecycle: BacktestLifecycleService, run_id: UUID, principal: Principal) -> BacktestRun:
    try:
        return lifecycle.get(run_id, owner_account_id=principal.account_id)
    except BacktestRunNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except NotRunOwner as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


def _with_attempt_counts(
    lifecycle: BacktestLifecycleService, runs: tuple[BacktestRun, ...]
) -> list[BacktestRun]:
    """Fill in the attempt history `lifecycle.list_runs` does not load.

    `list_runs` builds each aggregate as `BacktestRun(run=row)`, leaving `attempts` at
    its default `()`, while `get` goes through `_load`. Serialising both with
    `_run_payload` therefore reported `attemptCount: 0` for every listed run and the
    true count for the same run fetched individually - one number, two answers.

    The rows are already owner-scoped by `list_by_owner`, so the count is read here
    from the same gateway rather than published wrong. This is one extra read per
    listed run, bounded by `limit`; the fix that removes it is a one-line change in
    `lifecycle.list_runs` (`self._load(row)`), which this card does not own.
    """
    return [
        replace(run, attempts=lifecycle.gateway.attempts(run.backtest_run_id))
        for run in runs
    ]


#: The reason code every "your run exists, it just has no result yet" answer carries.
#: One token for the UI to branch on, on every evidence route.
RESULT_NOT_READY_REASON = "BACKTEST_RESULT_NOT_READY"


def _require_completed(run: BacktestRun) -> BacktestRun:
    """409 unless the run reached `COMPLETED`. See `_query_errors` for the rule.

    Applied to the three write-model evidence routes so they agree with the two
    result-read-model ones. Note what this is *not*: a check that the answer is
    non-empty. A `COMPLETED` run that published no detail objects legitimately returns
    an empty list, and turning that into a 409 would make "finished and traded nothing"
    indistinguishable from "still running" all over again.
    """
    if run.status is not RunStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": (
                    f"backtest run {run.backtest_run_id} is {run.status.value}; its result "
                    "evidence exists only once the run is COMPLETED"
                ),
                "reasonCode": RESULT_NOT_READY_REASON,
                "status": run.status.value,
            },
        )
    return run


def _result_query(results: BacktestResultQueryService | None) -> BacktestResultQueryService:
    if results is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "this deployment was built without a BacktestResultQueryService, so "
                "result read models are not served; pass one to create_app"
            ),
        )
    return results


@contextmanager
def _query_errors() -> Iterator[None]:
    """Translate the result read model's failures into the surface's status codes.

    * **404** for `QueryNotFound`, which covers "no such run" and "not yours" alike.
    * **409** for `QueryNotReady`: the run exists and is yours, but it has not
      completed, so there is no immutable evidence to serve. Distinct from 404 because
      the caller should retry later rather than conclude the run does not exist.
    * **422** for `QueryValidationError`, a malformed identity or projection input.
    * **500** for `QueryIntegrityError`. The read model refuses to serve evidence whose
      identities disagree; that is a data fault in published artifacts, not a bad
      request, and it must not be reported as an empty month.
    """
    try:
        yield
    except QueryNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except QueryNotReady as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": str(exc), "reasonCode": RESULT_NOT_READY_REASON},
        ) from exc
    except QueryValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
    except QueryIntegrityError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
        ) from exc


def _unauthenticated(detail: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"},
    )


def _delivery_attempt(request: Request) -> int:
    """SQS surfaces its redelivery count; a direct caller may state it explicitly."""
    raw = request.headers.get("X-Delivery-Attempt")
    if raw is None:
        return 1
    try:
        parsed = int(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Delivery-Attempt must be an integer",
        ) from exc
    if parsed < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-Delivery-Attempt starts at 1",
        )
    return parsed


def run() -> None:  # pragma: no cover - process entry point
    """Entry point for `backtest-api`. Mirrors `worker.run`.

    Three steps, in this order and no other:

    1. **build** the whole application from the environment. Every required setting is
       listed in `wiring.API_REQUIRED_ENV` and none of them has a default, so a missing
       one is a `WiringError` naming all of them at once - not a process that starts
       and serves an empty store.
    2. **verify** the live schema. The runtime applies no DDL, so drift is fatal rather
       than repairable, and finding out at start-up beats finding out per request.
    3. **serve**.

    The import is local because `wiring` imports `create_app` from this module; making
    it a module-level import would be a cycle.
    """
    from .wiring import build_api_runtime

    runtime = build_api_runtime(os.environ)
    runtime.verify()

    import uvicorn

    uvicorn.run(runtime.app, host=runtime.host, port=runtime.port)
