from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from sklearn.model_selection import train_test_split

from amel_training.dataset import FEATURE_COLUMNS, TARGET_COLUMN


@dataclass(frozen=True)
class Splits:
    X_train: pd.DataFrame
    y_train: pd.Series
    X_val: pd.DataFrame
    y_val: pd.Series
    X_test: pd.DataFrame
    y_test: pd.Series

    def sizes(self) -> dict[str, int]:
        return {"train": len(self.X_train), "validation": len(self.X_val), "test": len(self.X_test)}


def split_frame(
    df: pd.DataFrame, *, seed: int, validation_fraction: float, test_fraction: float
) -> Splits:
    """Stratified, seeded three-way split. The test split is carved out
    first and never touched again until final evaluation; validation is
    carved from what remains. Same seed + same frame → identical splits,
    which is what makes a run reproducible."""
    if not 0 < validation_fraction + test_fraction < 1:
        raise ValueError("validation_fraction + test_fraction must be in (0, 1)")
    X = df[FEATURE_COLUMNS].astype("float32")
    y = df[TARGET_COLUMN]
    X_rest, X_test, y_rest, y_test = train_test_split(
        X, y, test_size=test_fraction, random_state=seed, stratify=y
    )
    val_share_of_rest = validation_fraction / (1.0 - test_fraction)
    X_train, X_val, y_train, y_val = train_test_split(
        X_rest, y_rest, test_size=val_share_of_rest, random_state=seed, stratify=y_rest
    )
    return Splits(X_train, y_train, X_val, y_val, X_test, y_test)
