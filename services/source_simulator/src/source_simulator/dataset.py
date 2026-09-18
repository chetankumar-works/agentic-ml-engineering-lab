"""Fetch and stream the UCI HIGGS dataset without ever loading it fully
into memory. The archive ships as a ~2.8GB zip containing a single
member — as of this writing `HIGGS.csv.gz`, i.e. the CSV is itself
gzip-compressed *inside* the zip, so reading it is an unzip wrapped
around a gunzip — with 11M rows and no header: column 0 is the class
label (1=signal, 0=background), columns 1-28 are the 28 features in the
order `amel_common.schemas.HIGGS_FEATURE_NAMES` expects. A plain `.csv`
member (no inner gzip) is also supported in case UCI changes the
packaging again.
"""

from __future__ import annotations

import csv
import gzip
import io
import itertools
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import requests
from amel_common.logging import get_logger
from amel_common.schemas import HIGGS_FEATURE_NAMES

logger = get_logger(component="dataset")

# archive.ics.uci.edu returns 403 to requests with no browser-like
# User-Agent — not an auth mechanism, just a bot filter.
_DOWNLOAD_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AMEL-source-simulator/0.1)"}


@dataclass(frozen=True)
class HiggsRow:
    target: int
    features: dict[str, float]


def ensure_dataset(data_dir: Path, url: str) -> Path:
    """Download the dataset zip if not already present. Downloads to a
    `.part` file first so a crash mid-download is never mistaken for a
    complete, usable file on the next start.
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    dest = data_dir / "higgs.zip"
    if dest.exists():
        logger.info("dataset_already_present", path=str(dest))
        return dest

    part = data_dir / "higgs.zip.part"
    logger.info("dataset_download_started", url=url, dest=str(dest))
    with requests.get(url, headers=_DOWNLOAD_HEADERS, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        bytes_written = 0
        with part.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                bytes_written += len(chunk)
    part.rename(dest)
    logger.info("dataset_download_complete", path=str(dest), bytes=bytes_written)
    return dest


def _find_csv_member(zf: zipfile.ZipFile) -> str:
    for name in zf.namelist():
        if name.lower().endswith((".csv", ".csv.gz")):
            return name
    raise FileNotFoundError("no .csv or .csv.gz member found inside the HIGGS zip archive")


def stream_rows(zip_path: Path, max_rows: int | None, *, cycle: bool = False) -> Iterator[HiggsRow]:
    """Yield rows from the dataset one at a time. With `cycle=True`,
    restarts from the beginning once `max_rows` (or the file) is
    exhausted, so a long-running simulator never just stops — this models
    the fact that a real event source doesn't go quiet just because a
    fixed replay buffer ran out.
    """
    while True:
        yielded = 0
        with zipfile.ZipFile(zip_path) as zf:
            member = _find_csv_member(zf)
            with zf.open(member) as raw:
                decompressed = gzip.GzipFile(fileobj=raw) if member.lower().endswith(".gz") else raw
                with io.TextIOWrapper(decompressed, encoding="utf-8") as text:
                    reader = csv.reader(text)
                    rows = reader if max_rows is None else itertools.islice(reader, max_rows)
                    for row in rows:
                        target = int(float(row[0]))
                        values = (float(v) for v in row[1:29])
                        yield HiggsRow(
                            target=target,
                            features=dict(zip(HIGGS_FEATURE_NAMES, values, strict=True)),
                        )
                        yielded += 1
        if not cycle:
            return
        logger.info("dataset_exhausted_restarting", rows_yielded=yielded)
