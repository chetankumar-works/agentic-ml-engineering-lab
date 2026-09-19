"""Training data through Feast's *offline* path — the point-in-time
correct one. The entity dataframe is "which entities, as of when, with
what label"; Feast joins the features that were true at each timestamp
(LEARNING_LOG.md, Milestone 3). Inference uses the *online* path with
the same feature definitions — that shared definition is what prevents
train/serve skew."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from amel_common.schemas import HIGGS_FEATURE_NAMES
from amel_db.session import session_scope
from feast import FeatureStore
from sqlalchemy import text

from amel_training.config import TrainingConfig

FEATURE_COLUMNS: list[str] = list(HIGGS_FEATURE_NAMES)
TARGET_COLUMN = "target"


def load_entity_df(max_rows: int, as_of: datetime | None = None) -> pd.DataFrame:
    """Labelled entities up to `as_of`, newest first (the most recent
    `max_rows` when capped), with the label timestamp as the as-of time
    for the point-in-time join."""
    sql = "SELECT entity_id, label_timestamp AS event_timestamp, target FROM curated.higgs_labels"
    params: dict[str, object] = {}
    if as_of is not None:
        sql += " WHERE label_timestamp <= :as_of"
        params["as_of"] = as_of
    sql += " ORDER BY label_timestamp DESC"
    if max_rows > 0:
        sql += " LIMIT :n"
        params["n"] = max_rows
    with session_scope() as session:
        rows = session.execute(text(sql), params).mappings().all()
    if not rows:
        raise RuntimeError("curated.higgs_labels is empty — run the Milestone 2 pipeline first")
    return pd.DataFrame(rows)


def load_training_frame(cfg: TrainingConfig, store: FeatureStore | None = None) -> pd.DataFrame:
    store = store or FeatureStore(repo_path=str(cfg.feature_repo_path))
    entity_df = load_entity_df(cfg.max_rows, cfg.as_of)
    refs = [f"{cfg.feature_view}:{name}" for name in FEATURE_COLUMNS]
    df = store.get_historical_features(entity_df=entity_df, features=refs).to_df()
    before = len(df)
    df = df.dropna(subset=FEATURE_COLUMNS)
    dropped = before - len(df)
    if dropped:
        # A label whose feature event never made it to curated (DLQ'd as
        # malformed, or not yet curated) has no point-in-time match.
        print(f"dropped {dropped} labelled rows with no point-in-time feature match")
    df[TARGET_COLUMN] = df[TARGET_COLUMN].astype("int8")
    return df.reset_index(drop=True)


def feature_view_version(store: FeatureStore, name: str) -> str:
    return str(store.get_feature_view(name).tags.get("version", "unversioned"))
