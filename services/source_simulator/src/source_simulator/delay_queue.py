"""A small heap-based scheduler for "run this callable at time T", used to
model `LABEL_DELAY_SECONDS`: label events are generated at the same time
as their feature event but dispatched later, the way a real labeling
process (human review, a downstream system, settlement) would actually
arrive after the fact.
"""

from __future__ import annotations

import heapq
import itertools
import threading
import time
from collections.abc import Callable


class DelayedDispatcher:
    def __init__(self) -> None:
        self._heap: list[tuple[float, int, Callable[[], None]]] = []
        self._counter = itertools.count()
        self._lock = threading.Lock()
        self._wakeup = threading.Condition(self._lock)
        self._stop = False
        self._thread = threading.Thread(target=self._run, daemon=True, name="delayed-dispatcher")
        self._thread.start()

    def schedule(self, delay_seconds: float, callback: Callable[[], None]) -> None:
        run_at = time.monotonic() + max(delay_seconds, 0.0)
        with self._wakeup:
            heapq.heappush(self._heap, (run_at, next(self._counter), callback))
            self._wakeup.notify()

    def _run(self) -> None:
        with self._wakeup:
            while not self._stop:
                if not self._heap:
                    self._wakeup.wait()
                    continue
                run_at, _, callback = self._heap[0]
                remaining = run_at - time.monotonic()
                if remaining > 0:
                    self._wakeup.wait(timeout=remaining)
                    continue
                heapq.heappop(self._heap)
                self._wakeup.release()
                try:
                    callback()
                finally:
                    self._wakeup.acquire()

    def close(self) -> None:
        with self._wakeup:
            self._stop = True
            self._wakeup.notify_all()
        self._thread.join(timeout=2.0)
