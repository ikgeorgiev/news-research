from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from queue import Queue
import time

import pytest
from sqlalchemy import event, func, select, update
from sqlalchemy.orm import Session

from app.feed_runtime import (
    _load_tickers_from_csv_if_changed,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
)
from app.models import IngestionRun, RawFeedItem
from tests.helpers import seed_article, seed_source, seed_ticker


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
    attached_article = seed_article(
        db,
        canonical_url="https://example.com/attached",
        title="Attached",
        summary="Attached summary",
    )
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
    attached_old_row = RawFeedItem(
        source_id=source.id,
        article_id=attached_article.id,
        feed_url="https://example.com/feed.xml",
        raw_guid="attached-old-guid",
        raw_title="Attached Old",
        raw_link="https://example.com/attached",
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
    db.add_all([old_row, attached_old_row, fresh_row])
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
    attached_after = db.scalar(
        select(RawFeedItem).where(RawFeedItem.id == attached_old_row.id)
    )

    assert deleted == 1
    assert remaining_guids == {"attached-old-guid", "fresh-guid"}
    assert attached_after is not None


def test_raw_prune_rechecks_rows_changed_by_ingestion_before_the_delete(db_session):
    db = db_session
    now = datetime(2026, 3, 1, tzinfo=timezone.utc)
    source = seed_source(db)
    article_id = seed_article(db, slug="recovered-article").id
    for guid in ("attached", "refreshed", "expired"):
        db.add(RawFeedItem(source_id=source.id, feed_url="https://example.com/feed",
                           raw_guid=guid, fetched_at=now - timedelta(days=40)))
    db.commit()

    # Simulate a competing ingestion transaction winning the race immediately
    # before the pruning DELETE executes, independent of database lock timing.
    @event.listens_for(db, "do_orm_execute")
    def recover_rows(operation):
        if operation.is_delete:
            conn = db.connection()
            conn.execute(update(RawFeedItem).where(RawFeedItem.raw_guid == "attached").values(article_id=article_id))
            conn.execute(update(RawFeedItem).where(RawFeedItem.raw_guid == "refreshed").values(fetched_at=now))

    deleted = prune_raw_feed_items(db, retention_days=30, batch_size=100, max_batches=1, now=now)

    assert deleted == 1
    assert set(db.scalars(select(RawFeedItem.raw_guid))) == {"attached", "refreshed"}


def test_postgres_prune_retains_row_attached_while_waiting_for_lock(disposable_postgres_engine):
    engine = disposable_postgres_engine
    now = datetime(2026, 3, 1, tzinfo=timezone.utc)
    pids = Queue()

    def prune():
        with Session(engine) as db:
            pids.put(db.scalar(select(func.pg_backend_pid())))
            return prune_raw_feed_items(db, retention_days=30, batch_size=100, max_batches=1, now=now)

    with Session(engine) as writer:
        source_id = seed_source(writer).id
        article_id = seed_article(writer, slug="resolved").id
        row = RawFeedItem(source_id=source_id, feed_url="https://example.com/feed", raw_guid="resolved",
                          fetched_at=now - timedelta(days=40))
        writer.add(row)
        writer.commit()
        row_id = row.id
        writer.execute(update(RawFeedItem).where(RawFeedItem.id == row_id).values(article_id=article_id))
        writer_pid = writer.scalar(select(func.pg_backend_pid()))
        with ThreadPoolExecutor(max_workers=1) as executor:
            deleted = executor.submit(prune)
            try:
                pruner_pid = pids.get(timeout=3)
                deadline = time.monotonic() + 3
                while writer_pid not in writer.scalar(select(func.pg_blocking_pids(pruner_pid))):
                    assert time.monotonic() < deadline, "Pruning did not wait for the uncommitted attachment"
                    time.sleep(0.01)
            finally:
                writer.commit()
            assert deleted.result(timeout=3) == 0
        assert writer.get(RawFeedItem, row_id).article_id == article_id
