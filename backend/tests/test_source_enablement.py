from __future__ import annotations

from types import SimpleNamespace

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.ingestion import run_ingestion_cycle
from app.models import Source
from app.sources import SourceFeed, seed_sources


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


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
        "raw_feed_retention_days": 30,
        "raw_feed_prune_batch_size": 5000,
        "raw_feed_prune_max_batches": 1,
        "ingestion_stale_run_timeout_seconds": 3600,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_seed_sources_preserves_existing_enabled_state():
    db = _make_db_session()
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
    db.close()


def test_run_ingestion_cycle_skips_sources_disabled_in_db(monkeypatch):
    db = _make_db_session()
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
    db.close()
