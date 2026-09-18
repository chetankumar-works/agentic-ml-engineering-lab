"""Structured JSON logging, shared so every service's logs are uniformly
machine-parseable and correlate on `trace_id` (see ARCHITECTURE.md's
observability cross-cutting concern — this is the log side of that; a
real OpenTelemetry SDK integration lands in Milestone 6).
"""

from __future__ import annotations

import logging
import sys

import structlog


def configure_logging(service: str, level: str = "info") -> None:
    """Configure stdlib logging + structlog so every log line is one JSON
    object with a consistent shape: timestamp, level, service, event, and
    whatever contextvars (e.g. trace_id) are bound at call time.
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(**initial_values: object) -> structlog.typing.FilteringBoundLogger:
    return structlog.get_logger(**initial_values)
