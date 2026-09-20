"""The synthetic archive must be readable by the exact code path that
reads the real UCI archive."""

from __future__ import annotations

import sys
from pathlib import Path

from amel_common.schemas import HIGGS_FEATURE_NAMES
from source_simulator.dataset import stream_rows

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
from make_synthetic_higgs import synthetic_rows, write_archive  # noqa: E402


def test_synthetic_archive_streams_like_the_real_one(tmp_path: Path) -> None:
    out = tmp_path / "higgs.zip"
    write_archive(synthetic_rows(50, seed=1), out)
    rows = list(stream_rows(out, max_rows=None))
    assert len(rows) == 50
    assert set(rows[0].features) == set(HIGGS_FEATURE_NAMES)
    assert {r.target for r in rows} == {0, 1}
