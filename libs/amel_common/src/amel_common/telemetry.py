"""OpenTelemetry for every first-party AMEL service (Milestone 6).

One call — `configure_telemetry(service_name)` — gives a service:
- a TracerProvider exporting spans over OTLP/HTTP to the collector
  (`OTEL_EXPORTER_OTLP_ENDPOINT`, default http://otel-collector:4318),
- a LoggerProvider so stdlib log records (and therefore structlog, see
  `amel_common.logging`) are shipped to the collector → Loki with the
  active trace/span ids attached,
- W3C trace-context propagation (traceparent) — over HTTP via the
  FastAPI/httpx instrumentors and over Kafka via message headers with
  the confluent-kafka instrumentor,
- resource attributes (service.name, service.version, deployment env).

Why here and not per service: the value of tracing is that every hop
speaks the same propagation format and reports to the same place; a
service that configures it differently breaks the chain silently.

Telemetry is *off* unless `OTEL_ENABLED=true` (the Compose stack sets
it) so unit tests and ad-hoc scripts never try to reach a collector.
"""

from __future__ import annotations

import os
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource

_configured: dict[str, Any] = {}


def enabled() -> bool:
    return os.environ.get("OTEL_ENABLED", "false").lower() in ("1", "true", "yes")


def configure_telemetry(service_name: str, version: str = "0.1.0") -> bool:
    """Idempotent. Returns True if telemetry was configured (enabled)."""
    if _configured.get("done"):
        return bool(_configured.get("enabled"))
    _configured["done"] = True
    if not enabled():
        _configured["enabled"] = False
        return False

    from opentelemetry._logs import set_logger_provider
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4318")
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": version,
            "deployment.environment": os.environ.get("ENVIRONMENT", "local"),
        }
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
    )
    trace.set_tracer_provider(tracer_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(OTLPLogExporter(endpoint=f"{endpoint}/v1/logs"))
    )
    set_logger_provider(logger_provider)

    _configured.update(
        {"enabled": True, "tracer_provider": tracer_provider, "logger_provider": logger_provider}
    )
    return True


def otel_logging_handler() -> Any | None:
    """A stdlib logging handler that ships records via OTLP, or None when
    telemetry is disabled. Attached by `amel_common.logging`."""
    provider = _configured.get("logger_provider")
    if provider is None:
        return None
    from opentelemetry.sdk._logs import LoggingHandler

    return LoggingHandler(logger_provider=provider)


def current_trace_ids() -> tuple[str | None, str | None]:
    """(trace_id, span_id) as hex, or (None, None) outside a span."""
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None, None
    return format(ctx.trace_id, "032x"), format(ctx.span_id, "016x")


def instrument_fastapi(app: Any) -> None:
    if not enabled():
        return
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

    FastAPIInstrumentor.instrument_app(app, excluded_urls="health,ready,metrics")


def instrument_sqlalchemy(engine: Any) -> None:
    if not enabled():
        return
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    SQLAlchemyInstrumentor().instrument(engine=engine, enable_commenter=False)


def instrument_httpx() -> None:
    if not enabled():
        return
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    HTTPXClientInstrumentor().instrument()


def instrument_kafka_producer(producer: Any) -> Any:
    """Wrap a confluent_kafka Producer so produce() injects traceparent
    headers. Returns the (possibly wrapped) producer."""
    if not enabled():
        return producer
    from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

    return ConfluentKafkaInstrumentor().instrument_producer(producer)


def instrument_kafka_consumer(consumer: Any) -> Any:
    """Wrap a confluent_kafka Consumer so poll() extracts traceparent from
    headers and opens a consumer span per message."""
    if not enabled():
        return consumer
    from opentelemetry.instrumentation.confluent_kafka import ConfluentKafkaInstrumentor

    return ConfluentKafkaInstrumentor().instrument_consumer(consumer)


def shutdown() -> None:
    for key in ("tracer_provider", "logger_provider"):
        provider = _configured.get(key)
        if provider is not None:
            provider.shutdown()
