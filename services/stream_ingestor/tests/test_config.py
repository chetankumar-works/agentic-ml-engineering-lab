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


def test_consumer_config_defaults_match_librdkafka() -> None:
    cfg = Settings().consumer_config()
    assert cfg["queued.max.messages.kbytes"] == 65536
    assert cfg["fetch.max.bytes"] == 52428800
    assert cfg["enable.auto.commit"] is False


def test_consumer_config_prefetch_bounds_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_QUEUED_MAX_MESSAGES_KBYTES", "16384")
    monkeypatch.setenv("KAFKA_FETCH_MAX_BYTES", "8388608")
    cfg = Settings().consumer_config()
    assert cfg["queued.max.messages.kbytes"] == 16384
    assert cfg["fetch.max.bytes"] == 8388608
