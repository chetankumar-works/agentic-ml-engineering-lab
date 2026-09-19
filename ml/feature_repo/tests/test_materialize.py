import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from materialize import windows

T0 = datetime(2026, 9, 18, 20, 0, tzinfo=UTC)


def _t(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


def test_no_boundaries_is_one_window() -> None:
    assert windows(_t(0), _t(10), []) == [(_t(0), _t(10))]


def test_boundaries_split_the_range_contiguously() -> None:
    result = windows(_t(0), _t(10), [_t(3), _t(7)])
    assert result == [(_t(0), _t(3)), (_t(3), _t(7)), (_t(7), _t(10))]
    # contiguous: each window starts where the previous ended
    assert all(a[1] == b[0] for a, b in zip(result, result[1:], strict=False))


def test_boundaries_outside_or_on_the_edges_are_ignored() -> None:
    assert windows(_t(0), _t(10), [_t(-1), _t(0), _t(10), _t(11)]) == [(_t(0), _t(10))]


def test_empty_range_yields_nothing() -> None:
    assert windows(_t(5), _t(5), [_t(5)]) == []
    assert windows(_t(6), _t(5), []) == []
