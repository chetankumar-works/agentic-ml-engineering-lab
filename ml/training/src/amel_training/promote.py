"""Explicit, audited candidate → champion promotion.

The decision is a pure function of (candidate metrics, champion metrics,
criteria) so it is unit-testable and its inputs can be logged verbatim.
Applying it does two things atomically-enough for a single operator:
flips the MLflow alias, and writes an `ml.model_promotions` row with the
full context. Nothing in the serving path (Milestone 5) ever refers to
a version number — only to the `champion` alias."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass

import mlflow
from amel_db.models import ModelPromotion
from amel_db.session import session_scope
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from amel_training.config import TrainingConfig


@dataclass(frozen=True)
class PromotionCriteria:
    min_test_accuracy: float
    min_test_roc_auc: float
    max_regression: float

    @classmethod
    def from_config(cls, cfg: TrainingConfig) -> PromotionCriteria:
        return cls(
            min_test_accuracy=cfg.promotion_min_test_accuracy,
            min_test_roc_auc=cfg.promotion_min_test_roc_auc,
            max_regression=cfg.promotion_max_regression,
        )


@dataclass(frozen=True)
class PromotionDecision:
    approved: bool
    reasons: list[str]
    candidate_metrics: dict[str, float]
    champion_metrics: dict[str, float] | None
    criteria: PromotionCriteria


def evaluate_promotion(
    candidate: dict[str, float],
    champion: dict[str, float] | None,
    criteria: PromotionCriteria,
) -> PromotionDecision:
    """Absolute floors first, then relative-to-champion. Every failed
    check is a reason; an approval lists what it cleared."""
    reasons: list[str] = []
    ok = True
    acc, auc = candidate.get("test_accuracy"), candidate.get("test_roc_auc")
    if acc is None or auc is None:
        return PromotionDecision(
            False,
            ["candidate is missing test_accuracy/test_roc_auc"],
            candidate,
            champion,
            criteria,
        )
    if acc < criteria.min_test_accuracy:
        ok = False
        reasons.append(f"test_accuracy {acc:.4f} < floor {criteria.min_test_accuracy}")
    else:
        reasons.append(f"test_accuracy {acc:.4f} >= floor {criteria.min_test_accuracy}")
    if auc < criteria.min_test_roc_auc:
        ok = False
        reasons.append(f"test_roc_auc {auc:.4f} < floor {criteria.min_test_roc_auc}")
    else:
        reasons.append(f"test_roc_auc {auc:.4f} >= floor {criteria.min_test_roc_auc}")
    if champion is None:
        reasons.append("no current champion — first promotion")
    else:
        champ_acc = champion.get("test_accuracy", 0.0)
        if acc < champ_acc - criteria.max_regression:
            ok = False
            reasons.append(
                f"test_accuracy {acc:.4f} regresses champion {champ_acc:.4f} "
                f"by more than {criteria.max_regression}"
            )
        else:
            reasons.append(f"test_accuracy {acc:.4f} vs champion {champ_acc:.4f} within tolerance")
    return PromotionDecision(ok, reasons, candidate, champion, criteria)


def _metrics_for_version(
    client: MlflowClient, name: str, version: str
) -> tuple[str | None, dict[str, float]]:
    mv = client.get_model_version(name, version)
    if mv.run_id is None:
        return None, {}
    run = client.get_run(mv.run_id)
    return mv.run_id, {k: float(v) for k, v in run.data.metrics.items()}


def promote(
    cfg: TrainingConfig,
    *,
    version: str | None,
    decided_by: str,
    force: bool = False,
) -> PromotionDecision:
    """Promote `version` (default: whatever holds the candidate alias) to
    champion if the criteria pass — or if `force` is set, in which case
    the audit row says so. Returns the decision either way."""
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = MlflowClient()
    name = cfg.registered_model_name
    if version is None:
        version = client.get_model_version_by_alias(name, cfg.candidate_alias).version
    cand_run_id, cand_metrics = _metrics_for_version(client, name, version)

    previous: str | None = None
    champ_metrics: dict[str, float] | None = None
    try:
        previous = client.get_model_version_by_alias(name, cfg.champion_alias).version
        _, champ_metrics = _metrics_for_version(client, name, previous)
    except MlflowException:
        pass  # no champion yet

    decision = evaluate_promotion(cand_metrics, champ_metrics, PromotionCriteria.from_config(cfg))
    if not decision.approved and not force:
        return decision

    reason = "; ".join(decision.reasons) + (
        "; FORCED by operator" if force and not decision.approved else ""
    )
    client.set_registered_model_alias(name, cfg.champion_alias, version)
    client.set_model_version_tag(name, version, "promoted_by", decided_by)
    client.set_model_version_tag(name, version, "promotion_reason", reason)
    if previous is not None and previous != version:
        client.set_model_version_tag(name, previous, "superseded_by", version)
    with session_scope() as session:
        session.add(
            ModelPromotion(
                model_name=name,
                version=version,
                run_id=cand_run_id,
                from_alias=cfg.candidate_alias,
                to_alias=cfg.champion_alias,
                previous_champion_version=previous,
                decided_by=decided_by,
                reason=reason,
                criteria=asdict(decision.criteria),
                candidate_metrics=cand_metrics,
                champion_metrics=champ_metrics,
            )
        )
    return decision


def describe(cfg: TrainingConfig) -> dict[str, object]:
    """What the registry says right now — for humans and for the CLI."""
    mlflow.set_tracking_uri(cfg.mlflow_tracking_uri)
    client = MlflowClient()
    aliases: dict[str, object] = {}
    for alias in (cfg.candidate_alias, cfg.champion_alias):
        try:
            mv = client.get_model_version_by_alias(cfg.registered_model_name, alias)
            aliases[alias] = {"version": mv.version, "run_id": mv.run_id, "tags": dict(mv.tags)}
        except MlflowException:
            aliases[alias] = None
    return {"model": cfg.registered_model_name, "aliases": aliases}


def to_json(obj: object) -> str:
    return json.dumps(obj, indent=2, default=str)
