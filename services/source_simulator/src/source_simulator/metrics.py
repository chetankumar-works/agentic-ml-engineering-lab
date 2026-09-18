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
PAUSED = Gauge(
    "amel_simulator_paused",
    "1 if the simulator is currently paused, else 0",
)
CURRENT_RATE = Gauge(
    "amel_simulator_current_events_per_second",
    "Effective target event rate right now (accounts for burst mode)",
)
