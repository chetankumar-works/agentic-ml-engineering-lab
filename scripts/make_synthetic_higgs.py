#!/usr/bin/env python3
"""Write a small, synthetic stand-in for the UCI HIGGS archive.

The real archive is ~2.8 GB from a server that stalls and rejects
resumes (MISTAKES.md, Milestone 1) — unusable in CI and slow for a
clean-checkout check. This produces `higgs.zip` with the same packaging
(`HIGGS.csv.gz` inside a zip, no header, label + 28 float columns) so
`source_simulator.dataset.stream_rows` reads it unchanged. The label is
a noisy function of a few features so a decision tree can learn
something; it is NOT physics.

Usage:
    uv run python scripts/make_synthetic_higgs.py --rows 20000 --out data/raw/higgs.zip
"""

from __future__ import annotations

import argparse
import gzip
import io
import sys
import zipfile
from pathlib import Path

import numpy as np

N_FEATURES = 28


def synthetic_rows(n: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, N_FEATURES)).astype(np.float32)
    x[:, [0, 3, 5, 9, 13, 17]] = np.abs(x[:, [0, 3, 5, 9, 13, 17]])  # pt/energy-like: positive
    score = 0.9 * x[:, 0] - 0.7 * x[:, 25] + 0.5 * x[:, 21] + rng.normal(scale=0.8, size=n)
    y = (score > np.median(score)).astype(np.float32)
    return np.column_stack([y, x])


def write_archive(rows: np.ndarray, out: Path) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    csv_bytes = io.StringIO()
    np.savetxt(csv_bytes, rows, delimiter=",", fmt="%.6g")
    gz = gzip.compress(csv_bytes.getvalue().encode())
    tmp = out.with_suffix(".zip.part")
    with zipfile.ZipFile(tmp, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("HIGGS.csv.gz", gz)
    tmp.rename(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=20_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("data/raw/higgs.zip"))
    args = parser.parse_args(argv)
    if args.out.exists():
        print(f"{args.out} already exists — not overwriting (delete it to regenerate)")
        return 0
    write_archive(synthetic_rows(args.rows, args.seed), args.out)
    print(f"wrote {args.out} ({args.out.stat().st_size:,} bytes, {args.rows:,} rows, synthetic)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
