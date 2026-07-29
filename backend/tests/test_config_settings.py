from __future__ import annotations

from pathlib import Path

import pytest

from app.config import Settings


def test_settings_loads_ingest_cooldown_env(monkeypatch):
    monkeypatch.delenv("INGEST_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv("INGEST_COOLDOWN_SECONDS", "7")

    settings = Settings(_env_file=None)

    assert settings.ingest_cooldown_seconds == 7.0


def test_example_env_preserves_safe_feed_retry_and_local_ticker_defaults(
    monkeypatch: pytest.MonkeyPatch,
):
    for variable_name in (
        "FEED_FETCH_MAX_ATTEMPTS",
        "FEED_FETCH_BACKOFF_SECONDS",
        "FEED_FETCH_BACKOFF_JITTER_SECONDS",
        "TICKERS_CSV_PATH",
    ):
        monkeypatch.delenv(variable_name, raising=False)

    repo_root = Path(__file__).resolve().parents[2]
    settings = Settings(_env_file=repo_root / ".env.example")

    assert settings.feed_fetch_max_attempts == 1
    assert settings.feed_fetch_backoff_seconds == 0.25
    assert settings.feed_fetch_backoff_jitter_seconds == 0.3
    assert Path(settings.tickers_csv_path) == repo_root / "data" / "cef_tickers.csv"
