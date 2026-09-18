import threading
import time

from source_simulator.delay_queue import DelayedDispatcher


def test_delayed_dispatcher_runs_callback_after_delay() -> None:
    dispatcher = DelayedDispatcher()
    fired = threading.Event()
    fired_at: list[float] = []
    start = time.monotonic()

    def callback() -> None:
        fired_at.append(time.monotonic() - start)
        fired.set()

    dispatcher.schedule(0.05, callback)

    assert fired.wait(timeout=1.0)
    assert fired_at[0] >= 0.04
    dispatcher.close()


def test_delayed_dispatcher_runs_in_deadline_order() -> None:
    dispatcher = DelayedDispatcher()
    order: list[str] = []
    done = threading.Event()

    def mark_second() -> None:
        order.append("second")
        done.set()

    def mark_first() -> None:
        order.append("first")

    dispatcher.schedule(0.08, mark_second)
    dispatcher.schedule(0.02, mark_first)

    assert done.wait(timeout=1.0)
    assert order == ["first", "second"]
    dispatcher.close()
