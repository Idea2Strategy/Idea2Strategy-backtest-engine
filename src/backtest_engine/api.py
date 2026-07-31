from fastapi import FastAPI


def create_app() -> FastAPI:
    app = FastAPI(title="Idea2Strategy Backtest API", version="0.1.0")

    @app.get("/health", tags=["operations"])
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "backtest-api"}

    return app


app = create_app()


def run() -> None:
    import uvicorn

    uvicorn.run("backtest_engine.api:app", host="0.0.0.0", port=8082)
