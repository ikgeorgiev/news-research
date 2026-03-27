from __future__ import annotations

from app.config import Settings


def test_settings_loads_ingest_cooldown_env(monkeypatch):
    monkeypatch.delenv("INGEST_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv("INGEST_COOLDOWN_SECONDS", "7")

    settings = Settings(_env_file=None)

    assert settings.ingest_cooldown_seconds == 7.0
