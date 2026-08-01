from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, status

from .contracts import ContractValidationError
from .lifecycle import (
    BacktestLifecycleService,
    BacktestRun,
    BacktestRunNotFound,
    IdempotencyConflict,
    InMemoryBacktestJobQueue,
    InMemoryBacktestRunStore,
)


def _response(run: BacktestRun) -> dict[str, Any]:
    return {
        "backtest_run_id": run.backtest_run_id,
        "idempotency_key": run.idempotency_key,
        "status": run.status,
        "version": run.version,
        "status_result": run.status_result,
    }


def create_app(lifecycle: BacktestLifecycleService | None = None) -> FastAPI:
    app = FastAPI(title="Idea2Strategy Backtest API", version="0.1.0")
    service = lifecycle or BacktestLifecycleService(
        InMemoryBacktestRunStore(),
        InMemoryBacktestJobQueue(),
    )

    @app.get("/health", tags=["operations"])
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "backtest-api"}

    @app.post(
        "/backtests",
        status_code=status.HTTP_202_ACCEPTED,
        tags=["backtests"],
    )
    def accept_backtest(request: dict[str, Any]) -> dict[str, Any]:
        try:
            return _response(service.accept(request))
        except ContractValidationError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    @app.get("/backtests/{backtest_run_id}", tags=["backtests"])
    def get_backtest(backtest_run_id: str) -> dict[str, Any]:
        try:
            return _response(service.get(backtest_run_id))
        except BacktestRunNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("backtest_engine.api:app", host="0.0.0.0", port=8082)
