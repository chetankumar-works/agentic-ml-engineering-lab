"""Feast entity and feature view definitions for the HIGGS feature
repository. Applied via `feast apply` (run from this directory — see
`infra/feast/Dockerfile`, the `feast-apply` Compose service).

Only one entity and one feature view exist today (Milestone 3's scope);
this file grows as later milestones add more curated data worth serving
as features.
"""

from __future__ import annotations

from datetime import timedelta

from amel_common.schemas import HIGGS_FEATURE_NAMES
from feast import Entity, FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)
from feast.types import Float32
from feast.value_type import ValueType

entity = Entity(
    name="entity_id",
    join_keys=["entity_id"],
    value_type=ValueType.STRING,
    description=(
        "One HIGGS detector event, shared by its feature event and its "
        "(possibly later-arriving) label event."
    ),
)

higgs_features_source = PostgreSQLSource(
    name="higgs_features_source",
    table="curated.higgs_features_flat",
    timestamp_field="event_timestamp",
    description=(
        "Flattened view over curated.higgs_features — one typed float "
        "column per HIGGS feature instead of a nested JSON blob. See "
        "amel_db migration 0003_curated_higgs_features_flat_view."
    ),
)

# TTL is generous (10 years) because HIGGS event_timestamps are assigned
# by the simulator at publish time, not meaningful physical event dates —
# a short, "real" TTL (e.g. a few days) would be the right choice for
# genuinely time-sensitive production features, but would make this
# demo's historical retrieval flaky depending on how long ago the
# simulator happened to run.
higgs_feature_view = FeatureView(
    name="higgs_features",
    entities=[entity],
    ttl=timedelta(days=3650),
    schema=[Field(name=name, dtype=Float32) for name in HIGGS_FEATURE_NAMES],
    online=True,
    source=higgs_features_source,
    description="The 28 HIGGS detector features — offline from curated Postgres, online via Redis.",
    # Feature versioning: bump when the *meaning* of a field changes (a
    # renamed feature, a changed unit, a new derivation) so training
    # metadata (Milestone 4) can record exactly which definition it used
    # and so an old model is never served features with new semantics.
    # Adding a field is additive and does not require a bump. Feast also
    # keeps a full history of every `feast apply` in the SQL registry's
    # feature_view_version_history table.
    tags={"version": "1", "schema_version": "1"},
)
