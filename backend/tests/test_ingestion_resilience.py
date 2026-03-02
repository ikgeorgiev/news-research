from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.database import Base
from app.ingestion import (
    _fetch_feed_with_retries,
    ingest_feed,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
)
from app.models import IngestionRun, RawFeedItem, Source


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


def _seed_source(db: Session) -> Source:
    source = Source(
        code="businesswire",
        name="Business Wire",
        base_url="https://www.businesswire.com",
        enabled=True,
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    return source


def test_reconcile_stale_ingestion_runs_marks_only_stale_running_rows():
    db = _make_db_session()
    source = _seed_source(db)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    stale_run = IngestionRun(
        source_id=source.id,
        feed_url="https://example.com/stale",
        status="running",
        started_at=now - timedelta(hours=2),
        finished_at=None,
    )
    fresh_run = IngestionRun(
        source_id=source.id,
        feed_url="https://example.com/fresh",
        status="running",
        started_at=now - timedelta(minutes=5),
        finished_at=None,
    )
    done_run = IngestionRun(
        source_id=source.id,
        feed_url="https://example.com/done",
        status="success",
        started_at=now - timedelta(hours=3),
        finished_at=now - timedelta(hours=2),
    )
    db.add_all([stale_run, fresh_run, done_run])
    db.commit()

    fixed = reconcile_stale_ingestion_runs(db, stale_after_seconds=3600, now=now)
    stale_after = db.scalar(select(IngestionRun).where(IngestionRun.id == stale_run.id))
    fresh_after = db.scalar(select(IngestionRun).where(IngestionRun.id == fresh_run.id))

    assert fixed == 1
    assert stale_after is not None and stale_after.status == "failed"
    assert stale_after.finished_at is not None
    assert stale_after.finished_at.replace(tzinfo=timezone.utc) == now
    assert stale_after.error_text is not None
    assert fresh_after is not None and fresh_after.status == "running"
    db.close()


def test_fetch_feed_with_retries_succeeds_after_transient_failures(monkeypatch):
    attempts = {"count": 0}

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise requests.Timeout("temporary timeout")
        return FakeResponse()

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)
    monkeypatch.setattr("app.ingestion.time.sleep", lambda _seconds: None)

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=3,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert attempts["count"] == 3
    assert response.content == b"<rss />"


def test_ingest_feed_dedupes_raw_feed_rows(monkeypatch):
    db = _make_db_session()
    source = _seed_source(db)
    feed_entry = {
        "id": "guid-1",
        "guid": "guid-1",
        "title": "Fund update",
        "link": "https://example.com/story",
        "summary": "Summary text",
        "published": "2026-01-01T00:00:00Z",
    }

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[feed_entry]),
    )

    first = ingest_feed(
        db,
        source=source,
        feed_url="https://example.com/feed.xml",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )
    second = ingest_feed(
        db,
        source=source,
        feed_url="https://example.com/feed.xml",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    raw_rows = db.scalars(select(RawFeedItem).where(RawFeedItem.source_id == source.id)).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(raw_rows) == 1
    db.close()


def test_prune_raw_feed_items_respects_retention_window():
    db = _make_db_session()
    source = _seed_source(db)
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)

    old_row = RawFeedItem(
        source_id=source.id,
        article_id=None,
        feed_url="https://example.com/feed.xml",
        raw_guid="old-guid",
        raw_title="Old",
        raw_link="https://example.com/old",
        raw_pub_date=now - timedelta(days=40),
        raw_payload_json={},
        fetched_at=now - timedelta(days=40),
    )
    fresh_row = RawFeedItem(
        source_id=source.id,
        article_id=None,
        feed_url="https://example.com/feed.xml",
        raw_guid="fresh-guid",
        raw_title="Fresh",
        raw_link="https://example.com/fresh",
        raw_pub_date=now - timedelta(days=2),
        raw_payload_json={},
        fetched_at=now - timedelta(days=2),
    )
    db.add_all([old_row, fresh_row])
    db.commit()

    deleted = prune_raw_feed_items(
        db,
        retention_days=30,
        batch_size=100,
        max_batches=2,
        now=now,
    )
    remaining_guids = {
        row.raw_guid for row in db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    }

    assert deleted == 1
    assert remaining_guids == {"fresh-guid"}
    db.close()
