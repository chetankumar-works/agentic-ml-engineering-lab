"""The readiness/liveness state machine, without Kafka. These exist
because of the Milestone 3 producer-wedge incident: the loop thread died,
/health kept returning 200. See RUNBOOKS.md."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from amel_common.schemas import HIGGS_FEATURE_NAMES
from source_simulator.config import Settings

from source_simulator import simulator as sim


class FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []
        self.poll_thread_alive = True
        self.fail_with: Exception | None = None

    def publish(self, topic: str, key: str, value: bytes) -> None:
        if self.fail_with:
            raise self.fail_with
        self.published.append((topic, key))

    def close(self) -> None:
        pass


@dataclass
class FakeKafkaError:
    text: str
    is_fatal: bool = False

    def fatal(self) -> bool:
        return self.is_fatal

    def __str__(self) -> str:
        return self.text


def _runner(**overrides: Any) -> sim.SimulatorRunner:
    settings = Settings(
        readiness_error_window_seconds=0.2, data_dir=Path("/nonexistent"), **overrides
    )
    return sim.SimulatorRunner(settings, FakeProducer())  # type: ignore[arg-type]


def test_not_ready_until_dataset_available_but_live() -> None:
    runner = _runner()
    assert runner.liveness() == (True, "ok")
    ok, reason = runner.readiness()
    assert not ok and "dataset" in reason


def test_delivery_error_fails_readiness_and_a_later_success_clears_it() -> None:
    runner = _runner()
    runner.ready_event.set()
    assert runner.readiness()[0]

    runner.record_delivery("higgs.features.v1", FakeKafkaError("Local: Message timed out"))
    ok, reason = runner.readiness()
    assert not ok and "Message timed out" in reason
    assert runner.liveness()[0], "a transient delivery error must not fail liveness"

    runner.record_delivery("higgs.features.v1", None)
    assert runner.readiness() == (True, "ready")
    assert runner.health.snapshot()["delivery_failures"] == 1


def test_delivery_error_expires_after_the_window_with_no_new_errors() -> None:
    runner = _runner()
    runner.ready_event.set()
    runner.record_delivery("higgs.features.v1", FakeKafkaError("Broker: Not enough replicas"))
    assert not runner.readiness()[0]
    time.sleep(0.25)
    assert runner.readiness()[0]


def test_fatal_delivery_error_fails_liveness_too() -> None:
    runner = _runner()
    runner.ready_event.set()
    runner.record_delivery(
        "higgs.features.v1", FakeKafkaError("Local: Inconsistent state", is_fatal=True)
    )
    live, reason = runner.liveness()
    assert not live and "fatal" in reason
    assert not runner.readiness()[0]


def test_loop_crash_is_recorded_and_fails_liveness(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner()

    def boom(*_a: Any, **_k: Any) -> Any:
        raise RuntimeError("dataset exploded")

    monkeypatch.setattr(sim, "ensure_dataset", boom)
    runner._run()  # synchronously, on this thread

    live, reason = runner.liveness()
    assert not live and "dataset exploded" in reason
    assert not runner.readiness()[0]


def test_loop_thread_exiting_unexpectedly_fails_liveness() -> None:
    runner = _runner()
    runner._thread = threading.Thread(target=lambda: None)
    runner._thread.start()
    runner._thread.join()
    live, reason = runner.liveness()
    assert not live and "exited unexpectedly" in reason


def test_publish_failure_is_counted_and_loop_survives(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(events_per_second=1e9)
    producer: FakeProducer = runner.producer  # type: ignore[assignment]
    producer.fail_with = BufferError("Local: Queue full")
    monkeypatch.setattr(sim.time, "sleep", lambda _s: None)
    monkeypatch.setattr(sim, "ensure_dataset", lambda *_a: "/dev/null")

    @dataclass
    class Row:
        features: dict[str, float]
        target: int

    def two_rows(*_a: Any, **_k: Any) -> Any:
        for _ in range(2):
            yield Row(features=dict.fromkeys(HIGGS_FEATURE_NAMES, 0.5), target=1)
        runner.stop_event.set()

    monkeypatch.setattr(sim, "stream_rows", two_rows)
    runner._run()

    assert runner.counters.snapshot()["events_dropped"] == 2
    assert runner.health.loop_error is None
    assert runner.liveness()[0], "a queue-full error is transient, not fatal"


def test_start_index_offsets_entity_ids(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _runner(events_per_second=1e9, higgs_start_index=884_910, label_delay_seconds=0)
    producer: FakeProducer = runner.producer  # type: ignore[assignment]
    monkeypatch.setattr(sim.time, "sleep", lambda _s: None)
    monkeypatch.setattr(sim, "ensure_dataset", lambda *_a: "/dev/null")

    @dataclass
    class Row:
        features: dict[str, float]
        target: int

    def two_rows(*_a: Any, **_k: Any) -> Any:
        for _ in range(2):
            yield Row(features=dict.fromkeys(HIGGS_FEATURE_NAMES, 0.5), target=1)
        runner.stop_event.set()

    monkeypatch.setattr(sim, "stream_rows", two_rows)
    runner._run()

    feature_keys = [key for topic, key in producer.published if topic.startswith("higgs.features")]
    assert feature_keys == ["higgs-000884910", "higgs-000884911"]
