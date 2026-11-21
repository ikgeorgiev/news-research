from __future__ import annotations

import json
import logging
import os
import threading
import time
from uuid import uuid4

import pytest
import httpx
from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from app import article_ingest
from app.models import ArticleTicker, FeedPollState, RawFeedItem, Source, Ticker
from app.sources import POLICY_VALIDATED_MAPPING_REQUIRED
from app.sources import SourcePageConfig
from tests.helpers import call_ingest, run_concurrent_ingests, seed_source, seed_ticker
from tests.test_ingest_concurrency import _configure_yahoo_race_io
from tests.test_article_enrichment import case


DATABASE_URL = os.getenv("DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not DATABASE_URL or not DATABASE_URL.startswith("postgresql+psycopg://"),
    reason="Postgres-only transaction and advisory-lock coverage",
)


@pytest.fixture()
def ingestion_case():
    engine = create_engine(
        DATABASE_URL, connect_args={"options": "-c statement_timeout=8000"}
    )
    factory = sessionmaker(bind=engine, autoflush=False)
    token = uuid4().hex[:12]
    with factory() as db:
        source = seed_source(db, code=f"ci-latency-{token}")
        ticker = db.scalar(select(Ticker).where(Ticker.symbol == "UTF"))
        if ticker is None:
            ticker = seed_ticker(db, symbol="UTF")
        source_id, ticker_id = source.id, ticker.id
    entries = [
        {
            "id": f"{token}-{index}",
            "title": f"Research {token} release {index} NYSE: UTF",
            "link": f"https://example.com/{token}/{index}",
            "published": "2026-10-26T10:00:00Z",
            "summary": "Distribution update",
        }
        for index in range(2)
    ]
    try:
        overrides = {"known_symbols": {"UTF"}, "symbol_to_id": {"UTF": ticker_id}}
        yield factory, source_id, overrides, entries
    finally:
        engine.dispose()


def _raw_count(factory, source_id):
    with factory() as observer:
        return observer.scalar(
            select(func.count()).select_from(RawFeedItem)
            .where(RawFeedItem.source_id == source_id)
        )


@pytest.mark.parametrize("failure", [False, True])
def test_pending_enrichment_recovers_on_304_and_retains_failed_work(monkeypatch, ingestion_case, failure):
    from types import SimpleNamespace
    from tests.test_pending_enrichment import force_due
    factory, source_id, overrides, entries = ingestion_case
    for index, entry in enumerate(entries):
        entry["title"] = f"Pending distribution {entry['id']} item {index}"
    url = entries[0]["link"] + "/feed"
    phase = {"recover": False, "fail_next_commit": False}
    with factory() as db:
        source = db.get(Source, source_id)
        code = source.code
    monkeypatch.setitem(article_ingest.PAGE_FETCH_CONFIGS, code, SourcePageConfig(code, "example.com", "bw_table"))
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries", lambda **_: httpx.Response(
        304 if phase["recover"] else 200, content=b"feed", headers={"ETag": '"pending"'}, request=httpx.Request("GET", url),
    ))
    monkeypatch.setattr(article_ingest.feedparser, "parse", lambda _: SimpleNamespace(feed={}, entries=entries))
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", lambda *_a, **_k: {"UTF": ("exchange", .88)} if phase["recover"] else None)
    notifications = []
    monkeypatch.setattr(article_ingest, "notify_new_articles", lambda _db, count: notifications.append(count))
    kwargs = {**overrides, "persistence_policy_override": POLICY_VALIDATED_MAPPING_REQUIRED}
    with factory() as db:
        result = call_ingest(db, db.get(Source, source_id), url, **kwargs)
        assert result["items_inserted"] == 0
        force_due(db)
    phase["recover"] = True
    original_process = article_ingest._process_single_entry
    def process(db, **args):
        result = original_process(db, **args)
        if failure and args["prepared"].raw_guid == entries[1]["id"] and result.created_article:
            phase["fail_next_commit"] = True
        return result
    monkeypatch.setattr(article_ingest, "_process_single_entry", process)
    with factory() as db:
        original_commit = db.commit
        def commit():
            if phase["fail_next_commit"]:
                phase["fail_next_commit"] = False
                raise SQLAlchemyError("synthetic pending-entry commit failure")
            original_commit()
        monkeypatch.setattr(db, "commit", commit)
        result = call_ingest(db, db.get(Source, source_id), url, **kwargs)
        assert result["items_inserted"] == (1 if failure else 2), result
        assert result["status"] == ("failed" if failure else "success"), result
    with factory() as observer:
        raws = observer.scalars(select(RawFeedItem).where(RawFeedItem.source_id == source_id).order_by(RawFeedItem.raw_guid)).all()
        assert len(raws) == 2
        assert sum(r.article_id is not None for r in raws) == (1 if failure else 2)
        if failure:
            assert "_page_enrichment" in raws[1].raw_payload_json
            state = observer.scalar(select(FeedPollState).where(FeedPollState.feed_url == url))
            state.backoff_until = None
            observer.commit()
    monkeypatch.setattr(article_ingest, "_process_single_entry", original_process)
    with factory() as db:
        retry = call_ingest(db, db.get(Source, source_id), url, **kwargs)
        assert retry["items_inserted"] == (1 if failure else 0), retry
        assert call_ingest(db, db.get(Source, source_id), url, **kwargs)["items_inserted"] == 0
    assert notifications == [1, 1]


def test_overlapping_pending_retries_do_not_duplicate_articles(monkeypatch, ingestion_case):
    from types import SimpleNamespace
    from tests.test_pending_enrichment import force_due
    factory, source_id, overrides, entries = ingestion_case
    for entry in entries:
        entry["title"] = f"Pending announcement {entry['id']}"
    url = entries[0]["link"] + "/feed"
    phase = {"recover": False}
    with factory() as db:
        code = db.get(Source, source_id).code
    monkeypatch.setitem(article_ingest.PAGE_FETCH_CONFIGS, code, SourcePageConfig(code, "example.com", "bw_table"))
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries", lambda **_: httpx.Response(
        304 if phase["recover"] else 200, content=b"feed", request=httpx.Request("GET", url),
    ))
    monkeypatch.setattr(article_ingest.feedparser, "parse", lambda _: SimpleNamespace(feed={}, entries=entries))
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", lambda *_a, **_k: {"UTF": ("exchange", .88)} if phase["recover"] else None)
    notifications = []
    monkeypatch.setattr(article_ingest, "notify_new_articles", lambda _db, count: notifications.append(count))
    kwargs = {**overrides, "persistence_policy_override": POLICY_VALIDATED_MAPPING_REQUIRED}
    with factory() as db:
        assert call_ingest(db, db.get(Source, source_id), url, **kwargs)["items_inserted"] == 0
        force_due(db)
    phase["recover"] = True
    barrier = threading.Barrier(2)
    original_due = article_ingest.due_enrichment_entries
    def due(*args):
        pending = original_due(*args)
        barrier.wait(timeout=5)  # Both workers select the same pending rows.
        return pending
    monkeypatch.setattr(article_ingest, "due_enrichment_entries", due)
    results = run_concurrent_ingests(factory, [
        {"source_id": source_id, "feed_url": url, "overrides": kwargs},
        {"source_id": source_id, "feed_url": url, "overrides": kwargs},
    ])
    assert all(r["status"] == "success" and r["error"] is None for r in results), results
    assert sum(r["items_inserted"] for r in results) == 2
    assert notifications == [1, 1]
    with factory() as db:
        raws = db.scalars(select(RawFeedItem).where(RawFeedItem.source_id == source_id)).all()
        assert len(raws) == 2 and len({r.article_id for r in raws}) == 2
        assert all(r.article_id is not None and "_page_enrichment" not in r.raw_payload_json for r in raws)


def test_reversed_overlapping_feeds_do_not_deadlock(monkeypatch, ingestion_case):
    factory, source_id, overrides, entries = ingestion_case
    urls = [entry["link"] + "/feed" for entry in entries]
    _configure_yahoo_race_io(monkeypatch, {urls[0]: entries, urls[1]: entries[::-1]})
    original_process = article_ingest._process_single_entry
    first_entry_barrier = threading.Barrier(2)
    local = threading.local()

    def process(db, **kwargs):
        result = original_process(db, **kwargs)
        if not getattr(local, "processed_first", False):
            local.processed_first = True
            # Each transaction holds a different article before either asks for
            # the other. A whole-feed transaction deadlocks here on PostgreSQL.
            first_entry_barrier.wait(timeout=5)
        return result

    monkeypatch.setattr(article_ingest, "_process_single_entry", process)
    monkeypatch.setattr(article_ingest, "notify_new_articles", lambda *_: None)
    jobs = [
        {"source_id": source_id, "feed_url": url, "overrides": overrides}
        for url in urls
    ]
    results = run_concurrent_ingests(factory, jobs)
    assert all(result["error"] is None for result in results), results
    assert all(result["status"] == "success" for result in results), results
    assert sum(result["items_inserted"] for result in results) == 2
    with factory() as db:
        rows = db.scalars(
            select(RawFeedItem).where(RawFeedItem.source_id == source_id)
        ).all()
        assert len(rows) == 2
        assert len({row.article_id for row in rows}) == 2
        for row in rows:
            all_feeds = {row.feed_url, *row.raw_payload_json.get("_alt_feed_urls", [])}
            assert all_feeds == set(urls)


def test_article_is_visible_and_notified_before_next_entry(monkeypatch, ingestion_case):
    factory, source_id, overrides, entries = ingestion_case
    url = entries[0]["link"] + "/feed"
    _configure_yahoo_race_io(monkeypatch, {url: entries})
    original_process = article_ingest._process_single_entry
    visible_before_second = []
    notifications = []

    def process(db, **kwargs):
        if kwargs["prepared"].raw_guid == entries[1]["id"]:
            visible_before_second.append(
                (_raw_count(factory, source_id), list(notifications))
            )
        return original_process(db, **kwargs)

    def notify(db, count):
        notifications.append((count, _raw_count(factory, source_id)))

    monkeypatch.setattr(article_ingest, "_process_single_entry", process)
    monkeypatch.setattr(article_ingest, "notify_new_articles", notify)
    with factory() as db:
        result = call_ingest(db, db.get(Source, source_id), url, **overrides)
    assert visible_before_second == [(1, [(1, 1)])]
    assert notifications == [(1, 1), (1, 2)]
    assert result["items_inserted"] == 2


@pytest.mark.parametrize("page_validates_ticker", [True, False])
def test_feed_match_is_visible_before_earlier_page_lookup(
    monkeypatch, ingestion_case, page_validates_ticker, caplog
):
    caplog.set_level(logging.INFO, logger="app.article_ingest")
    metric_samples = []
    monkeypatch.setattr(article_ingest, "observe_article_commit", metric_samples.append)
    factory, source_id, overrides, entries = ingestion_case
    entries[0]["title"] = entries[0]["title"].replace(" NYSE: UTF", "")
    url = entries[0]["link"] + "/feed"
    _configure_yahoo_race_io(monkeypatch, {url: entries})
    with factory() as db:
        code = db.get(Source, source_id).code
    monkeypatch.setitem(
        article_ingest.PAGE_FETCH_CONFIGS, code,
        article_ingest.PAGE_FETCH_CONFIGS["prnewswire"],
    )
    notifications = []
    visible_at_lookup = []

    def fallback(*args, **kwargs):
        # Independent connection: a flush or uncommitted insert is insufficient.
        visible_at_lookup.append((_raw_count(factory, source_id), list(notifications)))
        time.sleep(0.01)
        return {"UTF": ("exchange", 0.99)} if page_validates_ticker else {}

    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    monkeypatch.setattr(
        article_ingest, "notify_new_articles", lambda db, count: notifications.append(count)
    )
    overrides["persistence_policy_override"] = POLICY_VALIDATED_MAPPING_REQUIRED
    with factory() as db:
        result = call_ingest(db, db.get(Source, source_id), url, **overrides)
        assert visible_at_lookup == [(1, [1])]
        expected = 2 if page_validates_ticker else 1
        assert result["status"] == "success"
        assert result["items_inserted"] == expected
        rows = db.scalars(select(RawFeedItem).where(RawFeedItem.source_id == source_id)).all()
        attached = [row for row in rows if row.article_id is not None]
        assert len(attached) == expected
        assert db.scalar(select(func.count()).select_from(ArticleTicker).where(
            ArticleTicker.article_id.in_([row.article_id for row in attached])
        )) == expected
        traces = [json.loads(record.getMessage().removeprefix("article_latency "))
                  for record in caplog.records if record.getMessage().startswith("article_latency ")]
        assert len(traces) == expected
        assert traces[0]["article_url"] == entries[1]["link"]
        assert traces[0]["used_page_fetch"] is False
        assert traces[0]["page_lookup_ms"] == 0
        if page_validates_ticker:
            assert traces[1]["used_page_fetch"] is True
            assert 8 <= traces[1]["page_lookup_ms"] <= traces[1]["processing_ms"]
        retry = call_ingest(db, db.get(Source, source_id), url, **overrides)
        assert retry["items_inserted"] == 0
        assert _raw_count(factory, source_id) == 2
        assert len(metric_samples) == expected, "Duplicate replay must not inflate commit metrics"


@pytest.mark.parametrize("fault", ["commit", "entry_database"])
def test_failed_later_entry_preserves_earlier_article_and_retries_cache(
    monkeypatch, ingestion_case, fault, caplog
):
    caplog.set_level(logging.INFO, logger="app.article_ingest")
    metric_samples = []
    monkeypatch.setattr(article_ingest, "observe_article_commit", metric_samples.append)
    factory, source_id, overrides, entries = ingestion_case
    url = entries[0]["link"] + "/feed"
    _configure_yahoo_race_io(monkeypatch, {url: entries})
    original_fetch = article_ingest._fetch_feed_with_retries
    original_process = article_ingest._process_single_entry
    second_entered = False
    failed_once = False
    notifications = []

    def fetch(**kwargs):
        response = original_fetch(**kwargs)
        response.headers = {"ETag": '"new"'}
        return response

    def process(db, **kwargs):
        nonlocal second_entered, failed_once
        second_entered |= kwargs["prepared"].raw_guid == entries[1]["id"]
        if fault == "entry_database" and second_entered and not failed_once:
            failed_once = True
            raise SQLAlchemyError("injected database failure")
        return original_process(db, **kwargs)

    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries", fetch)
    monkeypatch.setattr(article_ingest, "_process_single_entry", process)
    monkeypatch.setattr(
        article_ingest, "notify_new_articles", lambda db, count: notifications.append(count)
    )
    with factory() as db:
        db.add(FeedPollState(feed_url=url, etag='"old"'))
        db.commit()
        original_commit = db.commit

        def commit():
            nonlocal failed_once
            if fault == "commit" and second_entered and not failed_once:
                failed_once = True
                raise RuntimeError("injected commit failure")
            original_commit()

        monkeypatch.setattr(db, "commit", commit)
        result = call_ingest(db, db.get(Source, source_id), url, **overrides)
        assert result["status"] == "failed"
        assert result["items_inserted"] == 1
        assert _raw_count(factory, source_id) == 1
        assert notifications == [1]
        traces = [json.loads(record.getMessage().removeprefix("article_latency "))
                  for record in caplog.records if record.getMessage().startswith("article_latency ")]
        assert [trace["article_url"] for trace in traces] == [entries[0]["link"]]
        assert [sample["article_url"] for sample in metric_samples] == [entries[0]["link"]]
        trace = traces[0]
        assert trace["committed_at"] >= trace["feed_received_at"]
        assert trace["receipt_to_commit_ms"] == pytest.approx(
            trace["wait_before_processing_ms"] + trace["processing_ms"] + trace["commit_ms"],
            abs=0.003,
        )
        assert trace["notification_ok"] is True
        cache = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == url))
        assert cache.etag == '"old"'
        cache.backoff_until = None
        db.commit()
        retry = call_ingest(db, db.get(Source, source_id), url, **overrides)
        assert retry["status"] == "success"
        assert retry["items_inserted"] == 1
        assert _raw_count(factory, source_id) == 2
        assert notifications == [1, 1]
        traces = [json.loads(record.getMessage().removeprefix("article_latency "))
                  for record in caplog.records if record.getMessage().startswith("article_latency ")]
        assert [trace["article_url"] for trace in traces] == [entry["link"] for entry in entries]
        assert [sample["article_url"] for sample in metric_samples] == [entry["link"] for entry in entries]
        db.refresh(cache)
        assert cache.etag == '"new"'


@pytest.mark.parametrize("budget", [2, 3])
def test_current_feed_gets_capacity_during_pending_backlog_postgres(monkeypatch, case, budget):
    # Reuse the same behavior assertions against independent PostgreSQL state.
    # The fixture creates and drops its own isolated schema in the test DB.
    from tests.test_pending_enrichment import test_old_retries_leave_lookup_capacity_for_new_feed_item
    with case.factory() as db:
        test_old_retries_leave_lookup_capacity_for_new_feed_item(db, monkeypatch, budget)
