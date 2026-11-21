from datetime import datetime, timedelta, timezone

import httpx
import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app import article_ingest
from app.models import Article, RawFeedItem
from tests.helpers import call_ingest, seed_source, seed_ticker

FEED = "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg=="


def configure(monkeypatch, entries, statuses):
    from types import SimpleNamespace
    responses = iter(statuses)
    monkeypatch.setattr(article_ingest, "_fetch_feed_with_retries", lambda **_: httpx.Response(
        next(responses), content=b"feed", headers={"ETag": '"unchanged"'}, request=httpx.Request("GET", FEED),
    ))
    monkeypatch.setattr(article_ingest.feedparser, "parse", lambda _: SimpleNamespace(feed={}, entries=list(entries)))
    monkeypatch.setattr(article_ingest, "notify_new_articles", lambda *_: None)
    monkeypatch.setattr(article_ingest, "_source_page_fetch_requires_network", lambda *_: True)


def entries(count=1):
    return [{"id": f"pending-{i}", "title": f"Distribution announcement number {i}",
             "link": f"https://www.businesswire.com/news/home/pending-{i}/en/Distribution",
             "published": "2026-10-26T17:00:00Z", "summary": "Details on article page."} for i in range(count)]


def force_due(db):
    for row in db.scalars(select(RawFeedItem)).all():
        payload = dict(row.raw_payload_json)
        if "_page_enrichment" in payload:
            retry = dict(payload["_page_enrichment"])
            retry["next_retry_at"] = (datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()
            payload["_page_enrichment"] = retry
            row.raw_payload_json = payload
    db.commit()


def ingest(db, source, ticker):
    return call_ingest(db, source, FEED, persistence_policy_override="validated_mapping_required",
                       known_symbols={"UTF"}, symbol_to_id={"UTF": ticker.id})


@pytest.mark.parametrize("next_status", [304, 200])
def test_pending_survives_session_restart_and_feed_rollover(db_session, monkeypatch, next_status):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    feed_entries = entries()
    configure(monkeypatch, feed_entries, [200, next_status, 304])
    outcomes = iter([None, {"UTF": ("bw_table", .84)}])
    calls = []
    def fallback(*args, **kwargs):
        calls.append(args[2])
        return next(outcomes)
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    assert ingest(db, source, ticker)["items_inserted"] == 0
    source_id, ticker_id, bind = source.id, ticker.id, db.get_bind()
    force_due(db)
    feed_entries.clear()  # Item has rolled off; only durable pending data remains.
    db.close()
    with Session(bind) as restarted:
        from app.models import Source, Ticker
        source, ticker = restarted.get(Source, source_id), restarted.get(Ticker, ticker_id)
        result = ingest(restarted, source, ticker)
        assert result["status"] == "success", result
        assert result["items_inserted"] == 1
        assert ingest(restarted, source, ticker)["items_inserted"] == 0
        assert restarted.scalar(select(func.count()).select_from(Article)) == 1
        raw = restarted.scalar(select(RawFeedItem))
        assert raw.article_id is not None
        assert "_page_enrichment" not in raw.raw_payload_json
    assert len(calls) == 2


def test_pending_backoff_applies_to_unchanged_200_responses(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    configure(monkeypatch, entries(), [200, 200, 304])
    calls = []
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", lambda *a, **k: calls.append(a[2]))
    for _ in range(3):
        assert ingest(db, source, ticker)["status"] == "success"
    assert len(calls) == 1
    retry = db.scalar(select(RawFeedItem)).raw_payload_json["_page_enrichment"]
    assert retry["attempts"] == 1


def test_successfully_parsed_unmapped_page_is_not_queued(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    configure(monkeypatch, entries(), [200, 304])
    calls = []
    def fallback(*args, **kwargs):
        calls.append(args[2])
        return {}
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    ingest(db, source, ticker)
    force_due(db)
    ingest(db, source, ticker)
    assert len(calls) == 1
    assert "_page_enrichment" not in db.scalar(select(RawFeedItem)).raw_payload_json


def test_page_budget_defers_and_recovers_tail_on_304(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    configure(monkeypatch, entries(4), [200, 304, 304])
    calls = []
    def fallback(*args, **kwargs):
        calls.append(args[2])
        return {"UTF": ("bw_table", .84)}
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    first = ingest(db, source, ticker)
    assert first["items_inserted"] == 3
    assert len(calls) == 3
    assert ingest(db, source, ticker)["items_inserted"] == 1
    assert len(calls) == 4
    assert ingest(db, source, ticker)["items_inserted"] == 0
    assert db.scalar(select(func.count()).select_from(Article)) == 4


def test_due_pending_item_is_not_starved_by_fresh_entries(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    feed_entries = entries(4)
    configure(monkeypatch, feed_entries, [200, 200])
    calls = []
    def fallback(*args, **kwargs):
        calls.append(args[2])
        return None if len(calls) <= 3 else {"UTF": ("bw_table", .84)}
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    ingest(db, source, ticker)
    pending_tail = feed_entries[-1]["link"]
    feed_entries[:] = [{**e, "id": e["id"] + "-new", "link": e["link"] + "-new",
                       "title": e["title"] + " new"} for e in entries(3)]
    result = ingest(db, source, ticker)
    assert result["status"] == "success", result
    assert calls[3] == pending_tail
    assert len(calls) == 6
    assert db.scalar(select(RawFeedItem).where(RawFeedItem.raw_link == pending_tail)).article_id is not None


@pytest.mark.parametrize("budget", [2, 3])
def test_old_retries_leave_lookup_capacity_for_new_feed_item(db_session, monkeypatch, budget):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    feed_entries = entries(3)
    old_urls = {entry["link"] for entry in feed_entries}
    configure(monkeypatch, feed_entries, [200, 200, 304])
    calls = []

    def fallback(*args, **kwargs):
        calls.append(args[2])
        return None if args[2] in old_urls else {"UTF": ("bw_table", .84)}

    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    assert ingest(db, source, ticker)["items_inserted"] == 0
    force_due(db)
    fresh = {**entries(1)[0], "id": "fresh-fund", "title": "Fresh distribution announcement",
             "link": "https://www.businesswire.com/news/home/fresh-fund/en/Distribution"}
    feed_entries[:] = [fresh]
    calls.clear()
    result = call_ingest(db, source, FEED, persistence_policy_override="validated_mapping_required",
                         known_symbols={"UTF"}, symbol_to_id={"UTF": ticker.id},
                         businesswire_source_page_max_fetches_per_feed=budget)
    assert result["status"] == "success", result
    assert fresh["link"] in calls, "Old retries exhausted the entire page budget"
    assert len(calls) == budget
    assert sum(url in old_urls for url in calls) == budget - 1
    assert result["items_inserted"] == 1
    old_rows = db.scalars(select(RawFeedItem).where(RawFeedItem.raw_link.in_(old_urls))).all()
    assert len(old_rows) == 3
    assert all("_page_enrichment" in row.raw_payload_json for row in old_rows)
    assert sorted(row.raw_payload_json["_page_enrichment"]["attempts"] for row in old_rows) == sorted([2] * (budget - 1) + [1] * (4 - budget))
    # A 304 with no competing current-feed items still permits all three retries.
    force_due(db)
    calls.clear()
    assert ingest(db, source, ticker)["status"] == "success"
    assert len(calls) == 3
    assert set(calls) == old_urls


def test_new_feed_evidence_can_resolve_before_retry_backoff(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    feed_entries = entries()
    configure(monkeypatch, feed_entries, [200, 200])
    calls = []
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", lambda *a, **k: calls.append(a[2]))
    ingest(db, source, ticker)
    feed_entries[0]["title"] += " NYSE: UTF"
    assert ingest(db, source, ticker)["items_inserted"] == 1
    assert len(calls) == 1
    assert "_page_enrichment" not in db.scalar(select(RawFeedItem)).raw_payload_json


def test_general_feed_keeps_enrichment_for_all_persisted_articles(db_session, monkeypatch):
    db = db_session
    source, ticker = seed_source(db), seed_ticker(db, symbol="UTF")
    configure(monkeypatch, entries(4), [200])
    calls = []
    def fallback(*args, **kwargs):
        calls.append(args[2])
        return {"UTF": ("bw_table", .84)}
    monkeypatch.setattr(article_ingest, "_extract_source_fallback_tickers", fallback)
    result = call_ingest(db, source, "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
                         known_symbols={"UTF"}, symbol_to_id={"UTF": ticker.id})
    assert result["items_inserted"] == 4
    assert len(calls) == 4
