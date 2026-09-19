"""Prediction persistence (`ml.predictions`). Written before the Kafka
event so the table is the system of record."""

from __future__ import annotations

import time

from amel_common.logging import get_logger
from amel_db.models import Prediction
from amel_db.session import get_engine, session_scope
from sqlalchemy import text

logger = get_logger(component="prediction_store")


class PredictionStore:
    def __init__(self) -> None:
        self.write_failures = 0
        self.last_error: str | None = None
        self.last_error_at: float | None = None

    def ping(self) -> bool:
        try:
            with get_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as exc:  # noqa: BLE001 — probe
            self.last_error = repr(exc)
            self.last_error_at = time.monotonic()
            return False

    def save(self, row: Prediction) -> bool:
        try:
            with session_scope() as session:
                session.add(row)
            return True
        except Exception as exc:  # noqa: BLE001 — counted + surfaced via /ready
            self.write_failures += 1
            self.last_error = repr(exc)
            self.last_error_at = time.monotonic()
            logger.error(
                "prediction_persist_failed", error=repr(exc), prediction_id=row.prediction_id
            )
            return False
