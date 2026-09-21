"""Loop/probe behaviour without a broker. Regression for the Milestone 3
incident where a rejected offset commit killed the consume loop and
/health kept returning 200 (RUNBOOKS.md)."""

from __future__ import annotations

from typing import Any

from confluent_kafka import KafkaException, TopicPartition
from stream_ingestor.config import Settings
from stream_ingestor.consumer import StreamIngestor


class FakeConsumer:
    def __init__(self) -> None:
        self.commit_error: Exception | None = None
        self.commits: list[Any] = []
        self.on_assign: Any = None
        self.on_revoke: Any = None

    def subscribe(self, _topics: list[str], on_assign: Any, on_revoke: Any, on_lost: Any) -> None:
        self.on_assign, self.on_revoke = on_assign, on_revoke

    def commit(self, offsets: Any, asynchronous: bool) -> None:
        if self.commit_error:
            raise self.commit_error
        self.commits.append(offsets)

    def close(self) -> None:
        pass


def _ingestor() -> tuple[StreamIngestor, FakeConsumer]:
    consumer = FakeConsumer()
    ingestor = StreamIngestor(Settings(), dlq=None, consumer=consumer)  # type: ignore[arg-type]
    return ingestor, consumer


def _partitions() -> list[TopicPartition]:
    return [TopicPartition("higgs.features.v1", 0)]


def test_not_ready_before_assignment_and_after_revoke() -> None:
    ingestor, consumer = _ingestor()
    assert ingestor.liveness() == (True, "ok")
    assert ingestor.readiness() == (False, "no partition assignment")
    consumer.on_assign(consumer, _partitions())
    assert ingestor.readiness() == (True, "ready")
    consumer.on_revoke(consumer, _partitions())
    assert not ingestor.ready


def test_rejected_commit_does_not_kill_the_loop_but_fails_readiness(monkeypatch: Any) -> None:
    ingestor, consumer = _ingestor()
    consumer.on_assign(consumer, _partitions())
    monkeypatch.setattr(ingestor, "_write_with_retry", lambda *_a: None)
    consumer.commit_error = KafkaException("Commit failed: Broker: Unknown member")

    class Msg:
        def topic(self) -> str:
            return "higgs.features.v1"

        def partition(self) -> int:
            return 0

        def offset(self) -> int:
            return 7

        def value(self) -> bytes:
            return b"{}"

    monkeypatch.setattr(ingestor, "_validate", lambda *_a: (None, "x"))
    monkeypatch.setattr(ingestor, "dlq", type("Dlq", (), {"send": lambda *_a, **_k: None})())

    ingestor._process_batch([Msg()])  # type: ignore[list-item]  # must not raise

    assert ingestor.liveness()[0]
    ok, reason = ingestor.readiness()
    assert not ok and "Unknown member" in reason
    assert ingestor.commit_failures == 1

    consumer.commit_error = None
    ingestor._process_batch([Msg()])  # type: ignore[list-item]
    assert ingestor.readiness() == (True, "ready")
    assert len(consumer.commits) == 1


def test_loop_crash_fails_liveness(monkeypatch: Any) -> None:
    ingestor, _ = _ingestor()

    def boom() -> list:
        raise RuntimeError("db exhausted retries")

    monkeypatch.setattr(ingestor, "_poll_batch", boom)
    ingestor._run()

    live, reason = ingestor.liveness()
    assert not live and "db exhausted retries" in reason
    assert not ingestor.ready


def test_stalled_loop_fails_liveness_even_though_the_thread_is_alive(monkeypatch: Any) -> None:
    import threading
    import time as _time

    ingestor, consumer = _ingestor()
    consumer.on_assign(consumer, _partitions())
    ingestor.settings.stall_timeout_seconds = 0.2
    ingestor._thread = threading.Thread(target=lambda: _time.sleep(5), daemon=True)
    ingestor._thread.start()  # alive, but never touches last_progress_at
    assert ingestor.liveness() == (True, "ok")
    _time.sleep(0.3)
    live, reason = ingestor.liveness()
    assert not live and "stalled" in reason
    assert not ingestor.ready
    ingestor.last_progress_at = _time.monotonic()  # progress resumes
    assert ingestor.liveness()[0]
