import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from amel_common.schemas import HIGGS_FEATURE_NAMES
from definitions import entity, higgs_feature_view, higgs_features_source


def test_entity_join_key_matches_curated_schema() -> None:
    assert entity.join_key == "entity_id"


def test_feature_view_has_exactly_the_higgs_features() -> None:
    field_names = {f.name for f in higgs_feature_view.schema}
    assert field_names == set(HIGGS_FEATURE_NAMES)


def test_feature_view_is_online_enabled() -> None:
    assert higgs_feature_view.online is True


def test_source_points_at_the_flattened_view() -> None:
    assert higgs_features_source.get_table_query_string() == "curated.higgs_features_flat"
    assert higgs_features_source.timestamp_field == "event_timestamp"


def test_feature_view_carries_a_version_tag() -> None:
    assert higgs_feature_view.tags["version"] == "1"
