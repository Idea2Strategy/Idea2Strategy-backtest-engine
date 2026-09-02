"""Subprocess worker killed by Task 5 tests at deterministic production seams."""

from __future__ import annotations

import json
import os
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

import boto3

from backtest_engine.attempt_coordinator import ResourceSample
from backtest_engine.persistence import BacktestPersistence, create_backtest_engine
from backtest_engine.wiring import DurableResultPublisher, PersistenceExecutionKeyStore
from backtest_engine.worker import BacktestWorker, WorkerConfig
from d_integration_stack import ScriptedMonitor, build_stack


class _ReplayGateMonitor:
    def __init__(self, checkpoint: Any) -> None:
        self._checkpoint = checkpoint
        self._delegate = ScriptedMonitor(ResourceSample(timedelta(seconds=1), 64 * 1024 * 1024))

    def sample(self) -> ResourceSample:
        self._checkpoint()
        return self._delegate.sample()


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise ValueError("missing Task 5 subprocess setting")
    return value


def main() -> int:
    marker = Path(_required("TASK5_CHECKPOINT_MARKER"))
    error = Path(_required("TASK5_ERROR_MARKER"))
    checkpoint_name = _required("TASK5_CHECKPOINT")
    run_id = _required("TASK5_RUN_ID")

    def checkpoint() -> None:
        marker.write_text(
            json.dumps(
                {"checkpoint": checkpoint_name, "run_id": run_id},
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        threading.Event().wait(300)

    engine = None
    try:
        engine = create_backtest_engine(_required("TASK5_DATABASE_URL"))
        persistence = BacktestPersistence(engine)
        sqs = boto3.client(
            "sqs",
            endpoint_url=_required("TASK5_SQS_ENDPOINT"),
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        s3 = boto3.client(
            "s3",
            endpoint_url=_required("TASK5_S3_ENDPOINT"),
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        monitor: Any = _ReplayGateMonitor(checkpoint) if checkpoint_name == "replay" else ScriptedMonitor()
        stack = build_stack(
            persistence=persistence,
            sqs_client=sqs,
            s3_client=s3,
            queues=(
                _required("TASK5_MAIN_QUEUE"),
                _required("TASK5_DLQ"),
            ),
            bucket=_required("TASK5_BUCKET"),
            root=Path(_required("TASK5_MARKET_ROOT")),
            monitor=monitor,
            request=json.loads(_required("TASK5_REQUEST_JSON")),
        )

        if checkpoint_name == "binding":
            original_bind = stack.handler.bind

            def gated_bind(*args: Any, **kwargs: Any) -> Any:
                binding = original_bind(*args, **kwargs)
                checkpoint()
                return binding

            stack.handler.bind = gated_bind  # type: ignore[method-assign]
        elif checkpoint_name == "upload":
            original_put = stack.store.put

            def gated_put(*args: Any, **kwargs: Any) -> Any:
                receipt = original_put(*args, **kwargs)
                checkpoint()
                return receipt

            stack.store.put = gated_put  # type: ignore[method-assign]
        elif checkpoint_name == "publication":
            original_write = DurableResultPublisher._write

            def gated_write(self: DurableResultPublisher, *args: Any, **kwargs: Any) -> None:
                original_write(self, *args, **kwargs)
                checkpoint()

            DurableResultPublisher._write = gated_write  # type: ignore[method-assign]

        worker = BacktestWorker(
            client=sqs,
            config=WorkerConfig(
                queue_url=_required("TASK5_MAIN_QUEUE"),
                dead_letter_queue_url=_required("TASK5_DLQ"),
                worker_id=f"task5-{checkpoint_name}-worker",
                max_receive_count=3,
                visibility_timeout=timedelta(seconds=3),
                wait_time=timedelta(0),
                max_messages=1,
                heartbeat_interval=timedelta(seconds=1),
            ),
            handler=stack.handler,
            store=PersistenceExecutionKeyStore(persistence),
        )
        worker.poll_once()
        return 0
    except BaseException as exc:
        error.write_text(type(exc).__name__ + "\n", encoding="utf-8")
        return 70
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
