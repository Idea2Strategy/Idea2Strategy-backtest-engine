import signal
import threading


class BacktestWorker:
    """Lifecycle shell for the durable SQS consumer.

    Domain execution is intentionally absent until the versioned job contract
    and input bundle are implemented.
    """

    def __init__(self) -> None:
        self._stop = threading.Event()

    def request_stop(self, *_: object) -> None:
        self._stop.set()

    def run(self) -> None:
        self._stop.wait()


def run() -> None:
    worker = BacktestWorker()
    signal.signal(signal.SIGINT, worker.request_stop)
    signal.signal(signal.SIGTERM, worker.request_stop)
    worker.run()
