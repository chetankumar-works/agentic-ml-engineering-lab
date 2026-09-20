"""Structured JSON logging, shared so every service's logs are uniformly
machine-parseable and correlate on trace ids.

Milestone 6: log records now flow through stdlib `logging` (structlog
renders, stdlib emits) so that one extra handler — OpenTelemetry's —
ships every line to the collector → Loki with `trace_id`/`span_id`
attached from the active span. stdout still gets the same JSON line, so
`docker logs` is unchanged.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
from typing import Any

import structlog

from amel_common import telemetry


def _add_trace_ids(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    trace_id, span_id = telemetry.current_trace_ids()
    if trace_id:
        event_dict.setdefault("trace_id", trace_id)
        event_dict["span_id"] = span_id
    return event_dict


def configure_logging(service: str, level: str = "info") -> None:
    """Configure stdlib logging + structlog so every log line is one JSON
    object with a consistent shape: timestamp, level, service, event,
    trace/span ids when inside a span, and whatever contextvars are
    bound at call time.
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    shared: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        _add_trace_ids,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]
    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
    )
    root = logging.getLogger()
    root.handlers.clear()
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(formatter)
    root.addHandler(stdout)
    otel = telemetry.otel_logging_handler()
    if otel is not None:
        otel.setFormatter(formatter)
        root.addHandler(otel)
    root.setLevel(log_level)
    # Third-party chatter that would otherwise be shipped at INFO.
    for noisy in ("uvicorn.access", "httpx", "httpcore", "botocore", "urllib3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(**initial_values: object) -> structlog.typing.FilteringBoundLogger:
    return structlog.get_logger(**initial_values)
