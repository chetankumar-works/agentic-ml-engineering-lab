from __future__ import annotations

from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)

matplotlib.use("Agg")  # headless: containers have no display
import matplotlib.pyplot as plt  # noqa: E402


def compute_metrics(
    y_true: np.ndarray, y_pred: np.ndarray, y_proba: np.ndarray
) -> dict[str, float]:
    return {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, y_proba)),
        "log_loss": float(log_loss(y_true, y_proba, labels=[0, 1])),
    }


def confusion(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, int]:
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)}


def plot_confusion(cm: dict[str, int], path: Path, title: str) -> Path:
    grid = np.array([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]])
    fig, ax = plt.subplots(figsize=(4, 4))
    ax.imshow(grid, cmap="Blues")
    for (i, j), v in np.ndenumerate(grid):
        ax.text(j, i, f"{v:,}", ha="center", va="center", color="black")
    ax.set_xticks([0, 1], ["pred 0", "pred 1"])
    ax.set_yticks([0, 1], ["true 0", "true 1"])
    ax.set_title(title)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path


def feature_importances(model_importances: np.ndarray, names: list[str]) -> pd.DataFrame:
    df = pd.DataFrame({"feature": names, "importance": model_importances.astype(float)})
    return df.sort_values("importance", ascending=False).reset_index(drop=True)


def plot_importances(df: pd.DataFrame, path: Path, top: int = 15) -> Path:
    head = df.head(top).iloc[::-1]
    fig, ax = plt.subplots(figsize=(6, 5))
    ax.barh(head["feature"], head["importance"])
    ax.set_title(f"Top {len(head)} feature importances")
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return path
