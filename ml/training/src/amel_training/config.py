from __future__ import annotations

from datetime import datetime
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class TrainingConfig(BaseSettings):
    """Everything that can change a training run's outcome lives here and
    is logged to MLflow as parameters — so a run can be reproduced from
    its logged params alone. Environment variables use the `TRAINING_`
    prefix (`TRAINING_MAX_ROWS=200000`, `TRAINING_MAX_DEPTH=8`, ...).
    """

    model_config = SettingsConfigDict(env_prefix="TRAINING_", case_sensitive=False)

    # --- data ---
    feature_repo_path: Path = Path("/app/ml/feature_repo")
    feature_view: str = "higgs_features"
    # Most-recent N labelled entities. 200k keeps a dev run under a minute
    # and well under 1 GB; set to 0 for every labelled row (full-scale).
    max_rows: int = 200_000
    # Only labels with label_timestamp <= as_of. Curated data grows every
    # 5 minutes, so "the most recent N rows" is a moving target; pinning
    # as_of makes the dataset — and therefore the run — reproducible.
    # Unset = now (logged, so a later run can pin it).
    as_of: datetime | None = None

    # --- split ---
    random_seed: int = 42
    validation_fraction: float = 0.15
    test_fraction: float = 0.15

    # --- model (sklearn.tree.DecisionTreeClassifier) ---
    criterion: str = "gini"
    max_depth: int | None = 8
    min_samples_leaf: int = 200
    # Kept deliberately simple: the project is about engineering, not
    # model sophistication (AMEL_KICKOFF_PROMPT.md, DATASET).

    # --- tracking / registry ---
    mlflow_tracking_uri: str = "http://mlflow:5000"
    experiment_name: str = "higgs_decision_tree"
    registered_model_name: str = "higgs_decision_tree"
    candidate_alias: str = "candidate"
    champion_alias: str = "champion"

    # --- promotion criteria (evaluated by `promote`, logged with each decision) ---
    # A candidate must clear an absolute floor AND not be worse than the
    # current champion by more than `promotion_max_regression` on the
    # held-out *test* split. Thresholds are config, not code.
    promotion_min_test_accuracy: float = 0.66
    promotion_min_test_roc_auc: float = 0.70
    promotion_max_regression: float = 0.005

    git_sha: str | None = Field(default=None, description="Set by CI/Docker; else read from git")

    @field_validator("as_of", "git_sha", mode="before")
    @classmethod
    def _blank_is_none(cls, v: object) -> object:
        # Compose passes `${TRAINING_AS_OF:-}` through as "" when unset.
        return None if isinstance(v, str) and not v.strip() else v

    def tree_params(self) -> dict[str, object]:
        return {
            "criterion": self.criterion,
            "max_depth": self.max_depth,
            "min_samples_leaf": self.min_samples_leaf,
            "random_state": self.random_seed,
        }
