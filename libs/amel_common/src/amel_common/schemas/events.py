"""Versioned event schemas shared by every producer/consumer of the HIGGS
event streams. Nothing internal to AMEL should pass a raw, unvalidated
dict between services when these models apply — see ARCHITECTURE.md's
"Schema evolution" cross-cutting concern.

The 28 HIGGS feature names below follow the ordering used in the original
UCI HIGGS dataset / Baldi, Sadowski & Whiteson (2014): 21 low-level
kinematic features measured by particle detectors, followed by 7
high-level features derived by physicists specifically to help
discriminate signal from background. The dataset ships with no header
row, so this tuple is the authoritative name-to-column mapping used
everywhere a HIGGS row becomes a `FeatureEvent`.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator

FEATURE_SCHEMA_VERSION = "1.0.0"
LABEL_SCHEMA_VERSION = "1.0.0"
PREDICTION_SCHEMA_VERSION = "1.0.0"

HIGGS_FEATURE_NAMES: tuple[str, ...] = (
    "lepton_pt",
    "lepton_eta",
    "lepton_phi",
    "missing_energy_magnitude",
    "missing_energy_phi",
    "jet1_pt",
    "jet1_eta",
    "jet1_phi",
    "jet1_b_tag",
    "jet2_pt",
    "jet2_eta",
    "jet2_phi",
    "jet2_b_tag",
    "jet3_pt",
    "jet3_eta",
    "jet3_phi",
    "jet3_b_tag",
    "jet4_pt",
    "jet4_eta",
    "jet4_phi",
    "jet4_b_tag",
    "m_jj",
    "m_jjj",
    "m_lv",
    "m_jlv",
    "m_bb",
    "m_wbb",
    "m_wwbb",
)
_EXPECTED_FEATURE_SET = frozenset(HIGGS_FEATURE_NAMES)


class FeatureEvent(BaseModel):
    """One HIGGS detector reading, published to `higgs.features.v1`."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    entity_id: str
    event_timestamp: datetime
    schema_version: str = FEATURE_SCHEMA_VERSION
    source: str = "source_simulator"
    features: dict[str, float]
    trace_id: str

    @field_validator("features")
    @classmethod
    def features_match_higgs_schema(cls, value: dict[str, float]) -> dict[str, float]:
        got = frozenset(value.keys())
        if got != _EXPECTED_FEATURE_SET:
            missing = _EXPECTED_FEATURE_SET - got
            unexpected = got - _EXPECTED_FEATURE_SET
            raise ValueError(
                f"features must be exactly the 28 HIGGS fields; "
                f"missing={sorted(missing)} unexpected={sorted(unexpected)}"
            )
        return value


class LabelEvent(BaseModel):
    """The ground-truth label for a HIGGS event, published separately to
    `higgs.labels.v1` (often delayed — see LABEL_DELAY_SECONDS) to model
    the realistic case where labels arrive after the features that
    produced them.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: str
    entity_id: str
    target: Annotated[int, Field(ge=0, le=1)]
    label_timestamp: datetime
    schema_version: str = LABEL_SCHEMA_VERSION


class PredictionEvent(BaseModel):
    """One served prediction, published to `predictions.v1` by
    `apps/inference_api` (Milestone 5) after it has been persisted to
    `ml.predictions`. Downstream consumers (platform events, FinOps,
    monitoring) get exactly what the caller got — same ids, same model
    version — never a re-derived value.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    prediction_id: str
    entity_id: str | None
    source: str  # "raw" | "entity"
    model_name: str
    model_version: str
    model_run_id: str | None
    prediction: Annotated[int, Field(ge=0, le=1)]
    probability: Annotated[float, Field(ge=0.0, le=1.0)]
    predicted_at: datetime
    schema_version: str = PREDICTION_SCHEMA_VERSION
    trace_id: str
