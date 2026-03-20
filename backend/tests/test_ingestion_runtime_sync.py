from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.article_ingest import ingest_feed
from app.article_maintenance import (
    _upsert_article_tickers,
    dedupe_articles_by_title,
    dedupe_businesswire_url_variants,
    purge_token_only_articles,
    remap_source_articles,
    revalidate_stale_article_tickers,
)
from app.config import Settings
from app.feed_runtime import (
    _fetch_feed_with_retries,
    _get_feed_conditional_headers,
    _load_tickers_from_csv_if_changed,
    _update_feed_http_cache,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
)
from app.models import (
    Article,
    ArticleTicker,
    FeedPollState,
    IngestionRun,
    RawFeedItem,
    Source,
    Ticker,
)
from app.constants import EXTRACTION_VERSION
from app.utils import sha256_str
from tests.helpers import (
    FakeRssResponse,
    call_ingest,
    seed_article,
    seed_article_with_raw,
    seed_source,
    seed_ticker,
)

def test_reconcile_stale_ingestion_runs_marks_only_stale_running_rows(db_session):
    db = db_session
    source = seed_source(db)
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

def test_load_tickers_from_csv_if_changed_retries_after_transient_loader_failure(
    db_session, monkeypatch
):
    db = db_session
    seed_ticker(db, symbol="GOF")

    csv_path = Path(__file__).with_name(".tmp") / "tickers-retry.csv"
    csv_path.parent.mkdir(exist_ok=True)
    csv_path.write_text(
        "ticker,fund_name,sponsor,active\nGOF,Guggenheim,Guggenheim,true\n",
        encoding="utf-8",
    )

    call_count = {"count": 0}

    def fake_loader(_db: Session, _path: str):
        call_count["count"] += 1
        if call_count["count"] == 1:
            raise RuntimeError("transient loader failure")
        return {"loaded": 1, "created": 0, "updated": 0, "unchanged": 1}

    monkeypatch.setattr("app.feed_runtime.load_tickers_from_csv", fake_loader)
    monkeypatch.setattr("app.feed_runtime._tickers_csv_mtime_cache", {})

    try:
        with pytest.raises(RuntimeError, match="transient loader failure"):
            _load_tickers_from_csv_if_changed(db, str(csv_path))

        second = _load_tickers_from_csv_if_changed(db, str(csv_path))

        assert call_count["count"] == 2
        assert second == {"loaded": 1, "created": 0, "updated": 0, "unchanged": 1}
    finally:
        if csv_path.exists():
            csv_path.unlink()

def test_prune_raw_feed_items_respects_retention_window(db_session):
    db = db_session
    source = seed_source(db)
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
        row.raw_guid
        for row in db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    }

    assert deleted == 1
    assert remaining_guids == {"fresh-guid"}
