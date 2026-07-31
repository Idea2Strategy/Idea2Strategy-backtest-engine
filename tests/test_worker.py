from backtest_engine.worker import BacktestWorker


def test_worker_can_be_stopped_before_run() -> None:
    worker = BacktestWorker()

    worker.request_stop()
    worker.run()
