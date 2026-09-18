import csv
import gzip
import io
import zipfile
from pathlib import Path

from amel_common.schemas import HIGGS_FEATURE_NAMES
from source_simulator.dataset import stream_rows


def _csv_text(rows: list[list[float]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue()


def _make_higgs_zip(tmp_path: Path, rows: list[list[float]]) -> Path:
    """Plain `HIGGS.csv` member — kept in case UCI ever ships it uncompressed."""
    zip_path = tmp_path / "higgs.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("HIGGS.csv", _csv_text(rows))
    return zip_path


def _make_higgs_zip_with_gz_member(tmp_path: Path, rows: list[list[float]]) -> Path:
    """Matches the real UCI archive layout: a zip containing `HIGGS.csv.gz`
    (the CSV is gzip-compressed *inside* the zip, not stored plain).
    """
    zip_path = tmp_path / "higgs.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("HIGGS.csv.gz", gzip.compress(_csv_text(rows).encode("utf-8")))
    return zip_path


def test_stream_rows_parses_target_and_named_features(tmp_path: Path) -> None:
    row = [1.0] + [float(i) for i in range(28)]
    zip_path = _make_higgs_zip(tmp_path, [row])

    rows = list(stream_rows(zip_path, max_rows=None))

    assert len(rows) == 1
    assert rows[0].target == 1
    assert set(rows[0].features) == set(HIGGS_FEATURE_NAMES)
    assert rows[0].features["lepton_pt"] == 0.0


def test_stream_rows_respects_max_rows(tmp_path: Path) -> None:
    rows_data = [[0.0] + [float(i)] * 28 for i in range(10)]
    zip_path = _make_higgs_zip(tmp_path, rows_data)

    rows = list(stream_rows(zip_path, max_rows=3))

    assert len(rows) == 3


def test_stream_rows_cycles_when_requested(tmp_path: Path) -> None:
    rows_data = [[0.0] + [1.0] * 28, [1.0] + [2.0] * 28]
    zip_path = _make_higgs_zip(tmp_path, rows_data)

    gen = stream_rows(zip_path, max_rows=None, cycle=True)
    first_pass = [next(gen).target for _ in range(2)]
    second_pass = [next(gen).target for _ in range(2)]

    assert first_pass == [0, 1]
    assert second_pass == [0, 1]


def test_stream_rows_reads_gzip_compressed_csv_member(tmp_path: Path) -> None:
    row = [1.0] + [float(i) for i in range(28)]
    zip_path = _make_higgs_zip_with_gz_member(tmp_path, [row])

    rows = list(stream_rows(zip_path, max_rows=None))

    assert len(rows) == 1
    assert rows[0].target == 1
    assert set(rows[0].features) == set(HIGGS_FEATURE_NAMES)
