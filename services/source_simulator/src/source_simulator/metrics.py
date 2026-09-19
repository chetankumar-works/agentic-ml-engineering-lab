from __future__ import annotations

from prometheus_client import Counter, Gauge

EVENTS_PUBLISHED = Counter(
    "amel_simulator_events_published_total",
    "Events published by the source simulator",
    ["event_type", "outcome"],
)
DELIVERY_ERRORS = Counter(
    "amel_simulator_kafka_delivery_errors_total",
    "Kafka delivery errors reported by the producer",
    ["topic"],
)
EVENTS_DROPPED = Counter(
    "amel_simulator_events_dropped_total",
    "Events the simulator could not hand to the producer (local queue full / fatal producer)",
    ["event_type"],
)
READY = Gauge(
    "amel_simulator_ready",
    "1 if /ready would return 200 right now, else 0",
)
PAUSED = Gauge(
    "amel_simulator_paused",
    "1 if the simulator is currently paused, else 0",
)
CURRENT_RATE = Gauge(
    "amel_simulator_current_events_per_second",
    "Effective target event rate right now (accounts for burst mode)",
)
