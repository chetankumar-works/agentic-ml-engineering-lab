import pytest
from stream_ingestor.config import Settings


def test_dlq_topic_for_features() -> None:
    settings = Settings()
    assert settings.dlq_topic_for(settings.features_topic) == settings.features_dlq_topic


def test_dlq_topic_for_labels() -> None:
    settings = Settings()
    assert settings.dlq_topic_for(settings.labels_topic) == settings.labels_dlq_topic


def test_dlq_topic_for_unknown_topic_raises() -> None:
    settings = Settings()
    with pytest.raises(ValueError, match="no DLQ topic configured"):
        settings.dlq_topic_for("some.other.topic")
