"""Engine/session helpers. `DATABASE_URL` is read at call time (not
import time) so tests can point it wherever they need without import-order
tricks.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from amel_common import telemetry

_engine: Engine | None = None
_session_factory: sessionmaker[Session] | None = None


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set — copy .env.example to .env and fill it in, "
            "or export it directly (see infra/docker-compose.yml for local defaults)."
        )
    return url


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(get_database_url(), pool_pre_ping=True)
        telemetry.instrument_sqlalchemy(_engine)  # no-op unless OTEL_ENABLED
    return _engine


def get_session_factory() -> sessionmaker[Session]:
    global _session_factory
    if _session_factory is None:
        _session_factory = sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _session_factory


@contextmanager
def session_scope() -> Iterator[Session]:
    """One transaction per `with` block: commits on clean exit, rolls
    back and re-raises on any exception. This is the only place a
    transaction boundary should be decided — callers should not call
    `session.commit()` themselves.
    """
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
