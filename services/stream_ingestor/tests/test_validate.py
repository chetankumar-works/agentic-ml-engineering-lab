from datetime import UTC, datetime

from amel_common.schemas import HIGGS_FEATURE_NAMES, FeatureEvent, LabelEvent
from stream_ingestor.config import Settings
from stream_ingestor.consumer import StreamIngestor
from stream_ingestor.dlq import DlqProducer


def _make_ingestor() -> StreamIngestor:
    # Constructing confluent_kafka.Consumer/Producer only builds local
    # client config; it doesn't block on a broker connection, so this is
    # safe to do without a live Kafka broker in a unit test.
    settings = Settings(kafka_bootstrap_servers="localhost:9092")
    dlq = DlqProducer(settings.kafka_bootstrap_servers)
    return StreamIngestor(settings, dlq)


def _valid_feature_json() -> bytes:
    event = FeatureEvent(
        event_id="feat-1",
        entity_id="higgs-1",
        event_timestamp=datetime.now(UTC),
        features=dict.fromkeys(HIGGS_FEATURE_NAMES, 1.0),
        trace_id="trace-1",
    )
    return event.model_dump_json().encode("utf-8")


def _valid_label_json() -> bytes:
    event = LabelEvent(
        event_id="label-1",
        entity_id="higgs-1",
        target=1,
        label_timestamp=datetime.now(UTC),
    )
    return event.model_dump_json().encode("utf-8")


def test_validate_accepts_well_formed_feature_event() -> None:
    ingestor = _make_ingestor()
    parsed, reason = ingestor._validate(ingestor.settings.features_topic, _valid_feature_json())
    assert isinstance(parsed, FeatureEvent)
    assert reason is None


def test_validate_accepts_well_formed_label_event() -> None:
    ingestor = _make_ingestor()
    parsed, reason = ingestor._validate(ingestor.settings.labels_topic, _valid_label_json())
    assert isinstance(parsed, LabelEvent)
    assert reason is None


def test_validate_rejects_invalid_json() -> None:
    ingestor = _make_ingestor()
    parsed, reason = ingestor._validate(ingestor.settings.features_topic, b"{not json")
    assert parsed is None
    assert reason is not None
    assert "invalid_json" in reason


def test_validate_rejects_schema_mismatch() -> None:
    ingestor = _make_ingestor()
    parsed, reason = ingestor._validate(ingestor.settings.features_topic, b'{"entity_id": "x"}')
    assert parsed is None
    assert reason is not None
    assert "schema_validation_failed" in reason


def test_validate_rejects_empty_value() -> None:
    ingestor = _make_ingestor()
    parsed, reason = ingestor._validate(ingestor.settings.features_topic, None)
    assert parsed is None
    assert reason == "empty_message_value"
