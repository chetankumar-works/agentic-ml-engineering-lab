from datetime import datetime

from amel_lake.watermark import EPOCH, _advance_watermark_stmt, utcnow_naive


def test_epoch_is_naive() -> None:
    assert EPOCH.tzinfo is None


def test_utcnow_naive_returns_naive_datetime() -> None:
    assert utcnow_naive().tzinfo is None


def test_advance_watermark_upsert_is_monotonic_via_greatest() -> None:
    """Guards against the regression found during Milestone 2's
    acceptance run: concurrent DAG run retries could commit out of
    upper_bound order, and an unconditional overwrite would let an
    older value clobber a newer one. `GREATEST` in the ON CONFLICT
    clause is what prevents that — assert it's actually there rather
    than trusting the SQL construction silently.
    """
    stmt = _advance_watermark_stmt("higgs_features", datetime(2026, 1, 1))
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    assert "greatest" in compiled.lower()
    assert "on conflict" in compiled.lower()


def test_advance_watermark_upsert_refreshes_updated_at() -> None:
    """`onupdate=func.now()` on the model only fires through the ORM
    unit-of-work, not this raw Core `ON CONFLICT DO UPDATE` — without
    setting it explicitly, `updated_at` would silently go stale forever
    after the first insert (observed during the acceptance run).
    """
    stmt = _advance_watermark_stmt("higgs_features", datetime(2026, 1, 1))
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": False}))
    assert "updated_at" in compiled.lower()
