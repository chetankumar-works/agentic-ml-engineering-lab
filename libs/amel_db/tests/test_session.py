import pytest
from amel_db.session import pool_settings


def test_pool_settings_default_to_sqlalchemy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DB_POOL_SIZE", raising=False)
    monkeypatch.delenv("DB_MAX_OVERFLOW", raising=False)
    assert pool_settings() == {"pool_size": 5, "max_overflow": 10}


def test_pool_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_POOL_SIZE", "2")
    monkeypatch.setenv("DB_MAX_OVERFLOW", "0")
    assert pool_settings() == {"pool_size": 2, "max_overflow": 0}
