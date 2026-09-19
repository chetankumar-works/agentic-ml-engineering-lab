"""What was this model trained on, and from which code? Both answers are
logged with every run so a registered version can be traced back."""

from __future__ import annotations

import hashlib
import subprocess
from dataclasses import asdict, dataclass

import pandas as pd


@dataclass(frozen=True)
class DatasetVersion:
    """A content fingerprint of the training frame plus the human-readable
    facts behind it. Two runs with the same fingerprint trained on
    byte-identical (entity, timestamp, target) rows — regardless of when
    they ran or how many rows curated has since gained."""

    fingerprint: str
    n_rows: int
    n_entities: int
    min_event_timestamp: str
    max_event_timestamp: str
    max_entity_id: str
    positive_fraction: float
    feature_view: str
    feature_view_version: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def dataset_version(
    df: pd.DataFrame, feature_view: str, feature_view_version: str
) -> DatasetVersion:
    keyed = df[["entity_id", "event_timestamp", "target"]].sort_values(
        ["entity_id", "event_timestamp"]
    )
    # hash_pandas_object is deterministic across processes for these dtypes;
    # summing keeps it order-independent, the sort above makes it explicit.
    row_hashes = pd.util.hash_pandas_object(keyed, index=False).to_numpy()
    digest = hashlib.sha256(row_hashes.tobytes()).hexdigest()[:16]
    return DatasetVersion(
        fingerprint=digest,
        n_rows=int(len(df)),
        n_entities=int(df["entity_id"].nunique()),
        min_event_timestamp=str(df["event_timestamp"].min()),
        max_event_timestamp=str(df["event_timestamp"].max()),
        max_entity_id=str(df["entity_id"].max()),
        positive_fraction=float(df["target"].mean()) if len(df) else 0.0,
        feature_view=feature_view,
        feature_view_version=feature_view_version,
    )


def git_sha(explicit: str | None = None) -> str | None:
    """`TRAINING_GIT_SHA` when set (Docker/CI have no .git); else ask git."""
    if explicit:
        return explicit
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, timeout=5
        )
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None
