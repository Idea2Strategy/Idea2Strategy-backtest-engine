"""Bounded child workload for Task 5 production resource-monitor evidence.

The parent controls this helper through private marker files. It burns a small, finite amount of
CPU or commits a small bytearray, then stays alive so ``ProcessResourceMonitor``
can observe the real process counters before teardown.
"""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path


def main() -> int:
    ready_path = Path(sys.argv[1])
    command_path = Path(sys.argv[2])
    exit_path = Path(sys.argv[3])
    ready_path.write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 30
    waiter = threading.Event()
    while not command_path.is_file():
        if exit_path.is_file():
            return 0
        if time.monotonic() >= deadline:
            return 4
        waiter.wait(0.01)
    command = command_path.read_text(encoding="utf-8").strip()
    held_memory: bytearray | None = None
    if command == "cpu":
        deadline = time.monotonic() + 0.50
        value = 1
        while time.monotonic() < deadline:
            value = (value * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        if value < 0:  # pragma: no cover - keeps the loop result live
            raise AssertionError(value)
    elif command == "memory":
        held_memory = bytearray(32 * 1024 * 1024)
        for offset in range(0, len(held_memory), 4096):
            held_memory[offset] = 1
    else:
        return 2
    ready_path.write_text("workload-complete", encoding="utf-8")
    deadline = time.monotonic() + 30
    while not exit_path.is_file():
        if time.monotonic() >= deadline:
            return 5
        waiter.wait(0.01)
    if held_memory is not None and held_memory[0] != 1:  # pragma: no cover
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
