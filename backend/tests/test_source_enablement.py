from __future__ import annotations

import threading
import time
from pathlib import Path
from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.ingestion import run_ingestion_cycle
from app.models import Source
from app.sources import SourceFeed, seed_sources


def _settings(**overrides):
    defaults = {
        "tickers_csv_path": "data/cef_tickers.csv",
        "source_enable_yahoo": False,
        "source_enable_prn": False,
        "source_enable_gn": False,
        "source_enable_bw": True,
        "yahoo_chunk_size": 40,
        "request_timeout_seconds": 5,
        "feed_fetch_max_attempts": 1,
        "feed_fetch_backoff_seconds": 0.0,
        "feed_fetch_backoff_jitter_seconds": 0.0,
        "feed_failure_backoff_base_seconds": 30.0,
        "feed_failure_backoff_max_seconds": 600.0,
        "raw_feed_retention_days": 30,
        "raw_feed_prune_batch_size": 5000,
        "raw_feed_prune_max_batches": 1,
        "raw_feed_prune_interval_seconds": 3600,
        "ingestion_stale_run_timeout_seconds": 3600,
        "ingestion_max_workers": 1,
        "ingestion_enable_conditional_get": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_seed_sources_preserves_existing_enabled_state(db_session):
    db = db_session
    existing = Source(
        code="businesswire",
        name="Old Name",
        base_url="https://old.example.com",
        enabled=False,
    )
    db.add(existing)
    db.commit()

    seed_sources(
        db,
        [
            SourceFeed(
                code="businesswire",
                name="Business Wire",
                base_url="https://feed.businesswire.com",
                feed_urls=["https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg=="],
            )
        ],
    )

    row = db.scalar(select(Source).where(Source.code == "businesswire"))
    assert row is not None
    assert row.enabled is False
    assert row.name == "Business Wire"
    assert row.base_url == "https://feed.businesswire.com"


def test_run_ingestion_cycle_skips_sources_disabled_in_db(db_session, monkeypatch):
    db = db_session
    db.add(
        Source(
            code="businesswire",
            name="Business Wire",
            base_url="https://feed.businesswire.com",
            enabled=False,
        )
    )
    db.commit()

    calls = {"ingest_feed": 0}

    def fake_load_tickers(_db: Session, _path: str):
        return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

    def fake_ingest_feed(*_args, **_kwargs):
        calls["ingest_feed"] += 1
        return {
            "source": "businesswire",
            "feed_url": "https://example.com/feed.xml",
            "status": "success",
            "items_seen": 0,
            "items_inserted": 0,
            "error": None,
        }

    monkeypatch.setattr("app.ingestion.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.ingestion.ingest_feed", fake_ingest_feed)

    result = run_ingestion_cycle(db, _settings())

    assert result["total_feeds"] == 0
    assert calls["ingest_feed"] == 0


def test_run_ingestion_cycle_parallel_mode_serializes_per_source(monkeypatch):
    db_path = Path(__file__).resolve().parent / ".tmp_ingestion_parallel.db"
    if db_path.exists():
        db_path.unlink()

    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    db = session_factory()

    settings = _settings(
        source_enable_bw=True,
        source_enable_prn=True,
        ingestion_max_workers=4,
    )

    feed_plan = [
        SourceFeed(
            code="businesswire",
            name="Business Wire",
            base_url="https://feed.businesswire.com",
            feed_urls=["https://example.com/bw/1", "https://example.com/bw/2", "https://example.com/bw/3"],
        ),
        SourceFeed(
            code="prnewswire",
            name="PR Newswire",
            base_url="https://www.prnewswire.com",
            feed_urls=["https://example.com/prn/1"],
        ),
    ]

    lock = threading.Lock()
    active_by_source: dict[str, int] = {}
    max_active_by_source: dict[str, int] = {}
    global_active = 0
    max_global_active = 0

    def fake_build_source_feeds(_settings, _db):
        return feed_plan

    def fake_load_tickers(_db: Session, _path: str):
        return {"loaded": 0, "created": 0, "updated": 0, "unchanged": 0}

    def fake_ingest_feed(_db: Session, *, source: Source, feed_url: str, **_kwargs):
        nonlocal global_active, max_global_active
        with lock:
            source_active = active_by_source.get(source.code, 0) + 1
            active_by_source[source.code] = source_active
            max_active_by_source[source.code] = max(max_active_by_source.get(source.code, 0), source_active)
            global_active += 1
            max_global_active = max(max_global_active, global_active)

        time.sleep(0.03)

        with lock:
            active_by_source[source.code] = max(active_by_source.get(source.code, 1) - 1, 0)
            global_active = max(global_active - 1, 0)

        return {
            "source": source.code,
            "feed_url": feed_url,
            "status": "success",
            "items_seen": 0,
            "items_inserted": 0,
            "error": None,
        }

    monkeypatch.setattr("app.ingestion.build_source_feeds", fake_build_source_feeds)
    monkeypatch.setattr("app.ingestion.load_tickers_from_csv", fake_load_tickers)
    monkeypatch.setattr("app.ingestion.ingest_feed", fake_ingest_feed)
    monkeypatch.setattr("app.ingestion._should_run_raw_feed_prune", lambda _interval: False)

    # Force the parallel branch in run_ingestion_cycle for this test DB.
    bind = db.get_bind()
    assert bind is not None
    monkeypatch.setattr(bind.dialect, "name", "postgresql", raising=False)

    try:
        result = run_ingestion_cycle(db, settings)

        assert result["total_feeds"] == 4
        assert max_active_by_source.get("businesswire", 0) == 1
        assert max_global_active >= 2
    finally:
        db.close()
        engine.dispose()
        if db_path.exists():
            db_path.unlink()
