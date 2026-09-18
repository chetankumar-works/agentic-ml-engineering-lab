from amel_db.models import Base, HiggsFeatureEvent, HiggsLabelEvent


def test_landing_tables_registered_on_shared_metadata() -> None:
    table_names = {t.key for t in Base.metadata.tables.values()}
    assert "landing.higgs_feature_events" in table_names
    assert "landing.higgs_label_events" in table_names


def test_event_id_is_the_primary_key_on_both_tables() -> None:
    for model in (HiggsFeatureEvent, HiggsLabelEvent):
        pk_columns = [c.name for c in model.__table__.primary_key]
        assert pk_columns == ["event_id"]
