from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import httpx
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

def test_fetch_feed_with_retries_succeeds_after_transient_failures(monkeypatch):
    attempts = {"count": 0}

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise httpx.TimeoutException("temporary timeout")
        return FakeRssResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr("app.feed_runtime.time.sleep", lambda _seconds: None)

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=3,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert attempts["count"] == 3
    assert response.content == b"<rss />"

def test_fetch_feed_with_retries_honors_retry_after_for_429(monkeypatch):
    attempts = {"count": 0}
    slept: list[float] = []

    class RateLimitedResponse:
        status_code = 429
        headers = httpx.Headers({"Retry-After": "2"})
        content = b""
        request = httpx.Request("GET", "https://example.com/feed.xml")

        def raise_for_status(self) -> None:
            raise httpx.HTTPStatusError(
                "429",
                request=self.request,
                response=httpx.Response(429, headers=self.headers, request=self.request),
            )

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return RateLimitedResponse()
        return FakeRssResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.feed_runtime.time.sleep", lambda seconds: slept.append(float(seconds))
    )

    response = _fetch_feed_with_retries(
        feed_url="https://example.com/feed.xml",
        timeout_seconds=5,
        max_attempts=2,
        backoff_seconds=0.01,
        backoff_jitter_seconds=0.0,
    )

    assert attempts["count"] == 2
    assert slept == [2.0]
    assert response.content == b"<rss />"

def test_update_feed_http_cache_reads_requests_case_insensitive_headers(db_session):
    db = db_session
    feed_url = "https://example.com/feed-with-conditional-cache.xml"
    state = FeedPollState(feed_url=feed_url)
    db.add(state)
    db.commit()

    class FakeResponse:
        headers = httpx.Headers(
            {
                "etag": '"abc123"',
                "last-modified": "Tue, 03 Mar 2026 10:00:00 GMT",
            }
        )

    _update_feed_http_cache(state, FakeResponse())  # type: ignore[arg-type]
    db.commit()
    headers = _get_feed_conditional_headers(state)

    assert headers["If-None-Match"] == '"abc123"'
    assert headers["If-Modified-Since"] == "Tue, 03 Mar 2026 10:00:00 GMT"

def test_ingest_feed_persists_conditional_headers_across_runs(db_session, monkeypatch):
    db = db_session
    source = seed_source(db)
    sent_headers: list[dict[str, str]] = []
    attempts = {"count": 0}

    class FirstResponse:
        status_code = 200
        headers = httpx.Headers(
            {
                "etag": '"persisted-etag"',
                "last-modified": "Tue, 03 Mar 2026 10:00:00 GMT",
            }
        )
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    class NotModifiedResponse:
        status_code = 304
        headers = httpx.Headers({})
        content = b""

        def raise_for_status(self) -> None:
            return None

    def fake_get(*_args, **kwargs):
        attempts["count"] += 1
        sent_headers.append(dict(kwargs.get("headers") or {}))
        if attempts["count"] == 1:
            return FirstResponse()
        return NotModifiedResponse()

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=fake_get),
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[]),
    )

    first = call_ingest(db, source, "https://example.com/conditional.xml")
    second = call_ingest(db, source, "https://example.com/conditional.xml")

    state = db.scalar(
        select(FeedPollState).where(
            FeedPollState.feed_url == "https://example.com/conditional.xml"
        )
    )

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(sent_headers) == 2
    assert sent_headers[1].get("If-None-Match") == '"persisted-etag"'
    assert sent_headers[1].get("If-Modified-Since") == "Tue, 03 Mar 2026 10:00:00 GMT"
    assert state is not None and state.etag == '"persisted-etag"'

def test_ingest_feed_dedupes_raw_feed_rows(db_session, stub_feed_io):
    db = db_session
    source = seed_source(db)
    feed_entry = {
        "id": "guid-1",
        "guid": "guid-1",
        "title": "Fund update",
        "link": "https://example.com/story",
        "summary": "Summary text",
        "published": "2026-01-01T00:00:00Z",
    }
    stub_feed_io([feed_entry])

    first = call_ingest(db, source, "https://example.com/feed.xml")
    second = call_ingest(db, source, "https://example.com/feed.xml")

    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.source_id == source.id)
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(raw_rows) == 1

def test_ingest_feed_dedupes_businesswire_story_across_yahoo_and_bw(
    db_session, stub_feed_io
):
    db = db_session
    yahoo = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    businesswire = seed_source(db)

    yahoo_entry = {
        "id": "y-guid-1",
        "guid": "y-guid-1",
        "title": "ACME distribution update NYSE: UTF",
        "link": "https://www.businesswire.com/news/home/20260301000001/en?feedref=JjAwJuNHiystnCoBq_hl-XxV8f8yqXw8M0Q",
        "summary": "Yahoo copy",
        "published": "2026-03-01T00:00:00Z",
    }
    bw_entry = {
        "id": "bw-guid-1",
        "guid": "bw-guid-1",
        "title": "ACME distribution update NYSE: UTF",
        "link": "https://www.businesswire.com/news/home/20260301000001/en",
        "summary": "Business Wire copy",
        "published": "2026-03-01T00:00:00Z",
    }

    stub_feed_io([yahoo_entry], "Yahoo Finance")
    yahoo_run = call_ingest(
        db, yahoo,
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
        known_symbols={"UTF"},
    )

    stub_feed_io([bw_entry])
    bw_run = call_ingest(
        db, businesswire,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
        known_symbols={"UTF"},
    )

    articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    raw_rows = db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    assert yahoo_run["status"] == "success"
    assert bw_run["status"] == "success"
    assert len(articles) == 1
    assert (
        articles[0].canonical_url
        == "https://www.businesswire.com/news/home/20260301000001/en"
    )
    raw_links_by_source = {row.source_id: row.raw_link for row in raw_rows}
    assert raw_links_by_source.get(yahoo.id) == (
        "https://www.businesswire.com/news/home/20260301000001/en"
        "?feedref=JjAwJuNHiystnCoBq_hl-XxV8f8yqXw8M0Q"
    )
    if businesswire.id in raw_links_by_source:
        assert (
            raw_links_by_source[businesswire.id]
            == "https://www.businesswire.com/news/home/20260301000001/en"
        )

def test_ingest_feed_mirrored_bw_url_does_not_prune_existing_tickers(
    db_session, stub_feed_io
):
    db = db_session
    yahoo = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
    article = seed_article(
        db,
        canonical_url=canonical_url,
        title="ACME distribution update",
        summary="Original summary",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    ticker = seed_ticker(db, symbol="UTF")
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=ticker.id,
            match_type="exchange",
            confidence=0.88,
        )
    )
    db.commit()

    yahoo_entry = {
        "id": "y-guid-1",
        "guid": "y-guid-1",
        "title": "ACME distribution update",
        "link": canonical_url + "?tsrc=rss",
        "summary": "Yahoo mirror with no ticker text",
        "published": "2026-03-01T00:00:00Z",
    }
    stub_feed_io([yahoo_entry], "Yahoo Finance")

    result = call_ingest(
        db, yahoo,
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF,GOF",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    tickers_after = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    assert result["status"] == "success"
    assert len(tickers_after) == 1
    assert tickers_after[0].ticker_id == ticker.id

def test_ingest_feed_mirrored_bw_url_does_not_overwrite_canonical_metadata(
    db_session, stub_feed_io
):
    db = db_session
    yahoo = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
    article = seed_article(
        db,
        canonical_url=canonical_url,
        title="Canonical BW headline",
        summary="Canonical Business Wire summary with richer details.",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )

    mirror_entry = {
        "id": "y-guid-2",
        "guid": "y-guid-2",
        "title": "Mirrored Yahoo title",
        "link": canonical_url + "?tsrc=rss",
        "summary": "Short mirror summary",
        "published": "2026-03-01T00:10:00Z",
    }
    stub_feed_io([mirror_entry], "Yahoo Finance")

    result = call_ingest(
        db, yahoo,
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF,GOF",
        known_symbols={"UTF"},
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    assert result["status"] == "success"
    assert article_after is not None
    assert article_after.title == "Canonical BW headline"
    assert (
        article_after.summary == "Canonical Business Wire summary with richer details."
    )
    assert article_after.source_name == "Business Wire"
    assert article_after.provider_name == "Business Wire"
    assert article_after.published_at is not None
    assert article_after.published_at.replace(tzinfo=timezone.utc) == published

def test_ingest_feed_non_bw_query_url_still_allows_exact_update_and_prune(
    db_session, stub_feed_io
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    url_with_query = "https://www.prnewswire.com/news-releases/acme-update.html?id=123"
    article = seed_article(
        db,
        canonical_url=url_with_query,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    old_ticker = seed_ticker(db, symbol="UTF")
    new_ticker = seed_ticker(db, symbol="GOF")
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=old_ticker.id,
            match_type="exchange",
            confidence=0.88,
        )
    )
    db.commit()

    entry = {
        "id": "prn-guid-1",
        "guid": "prn-guid-1",
        "title": "NYSE: GOF distribution update",
        "link": url_with_query,
        "summary": "New short summary",
        "published": "2026-03-01T00:10:00Z",
    }
    stub_feed_io([entry], "PR Newswire")

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF", "GOF"},
        symbol_to_id={"UTF": old_ticker.id, "GOF": new_ticker.id},
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows_after = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    assert result["status"] == "success"
    assert article_after is not None
    assert article_after.title == "NYSE: GOF distribution update"
    assert article_after.summary == "New short summary"
    assert len(ticker_rows_after) == 1
    assert ticker_rows_after[0].ticker_id == new_ticker.id

def test_ingest_feed_exact_url_transient_miss_keeps_existing_tickers(
    db_session, stub_feed_io, monkeypatch
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = "https://www.prnewswire.com/news-releases/acme-update.html?id=123"
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(db, symbol="UTF")
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=ticker.id,
            match_type="exchange",
            confidence=0.88,
        )
    )
    db.commit()

    entry = {
        "id": "prn-guid-transient-miss",
        "guid": "prn-guid-transient-miss",
        "title": "New title without symbol",
        "link": article_url,
        "summary": "New short summary",
        "published": "2026-03-01T00:10:00Z",
    }
    stub_feed_io([entry], "PR Newswire")
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows_after = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    assert result["status"] == "success"
    assert article_after is not None
    assert article_after.title == "New title without symbol"
    assert article_after.summary == "New short summary"
    assert len(ticker_rows_after) == 1
    assert ticker_rows_after[0].ticker_id == ticker.id

def test_ingest_feed_exact_url_sub_threshold_hits_still_refresh_existing_article(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = "https://www.prnewswire.com/news-releases/acme-update.html?id=456"
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(db, symbol="UTF")
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=ticker.id,
            match_type="exchange",
            confidence=0.88,
        )
    )
    db.commit()

    entry = {
        "id": "prn-guid-subthreshold-refresh",
        "guid": "prn-guid-subthreshold-refresh",
        "title": "New title mentioning UTF only",
        "link": article_url,
        "summary": "Refreshed summary text",
        "published": "2026-03-01T00:15:00Z",
    }
    stub_feed_io([entry], "PR Newswire")
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows_after = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    raw_rows_after = db.scalars(
        select(RawFeedItem).where(RawFeedItem.article_id == article.id)
    ).all()

    assert result["status"] == "success"
    assert article_after is not None
    assert article_after.title == "New title mentioning UTF only"
    assert article_after.summary == "Refreshed summary text"
    assert len(ticker_rows_after) == 1
    assert ticker_rows_after[0].ticker_id == ticker.id
    assert len(raw_rows_after) == 1
    assert raw_rows_after[0].raw_guid == "prn-guid-subthreshold-refresh"

def test_ingest_feed_rejected_strict_source_entry_persists_detached_raw_row(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )

    entry = {
        "id": "prn-guid-detached-raw",
        "guid": "prn-guid-detached-raw",
        "title": "Envestnet appoints new CGO to lead growth strategy",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700001.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }
    stub_feed_io([entry], "PR Newswire")
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
    )

    articles = db.scalars(select(Article)).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-detached-raw")
    ).all()

    assert result["status"] == "success"
    assert result["items_inserted"] == 0
    assert articles == []
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id is None

def test_ingest_feed_detached_raw_row_does_not_block_later_valid_ingest(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )

    bad_entry = {
        "id": "prn-guid-retry-valid",
        "guid": "prn-guid-retry-valid",
        "title": "Envestnet appoints new CGO to lead growth strategy",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700002.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }
    good_entry = {
        "id": "prn-guid-retry-valid",
        "guid": "prn-guid-retry-valid",
        "title": "Calamos Global Total Return Fund CGO declares distribution",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700002.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }

    stub_feed_io([bad_entry], "PR Newswire")
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    first = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    stub_feed_io([good_entry], "PR Newswire")

    second = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    articles = db.scalars(select(Article)).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-retry-valid")
    ).all()

    assert first["items_inserted"] == 0
    assert second["items_inserted"] == 1
    assert len(articles) == 1
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == articles[0].id

def test_ingest_feed_detached_raw_row_does_not_block_later_valid_copy_in_same_poll(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )

    bad_entry = {
        "id": "prn-guid-same-poll",
        "guid": "prn-guid-same-poll",
        "title": "Envestnet appoints new CGO to lead growth strategy",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700003.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }
    good_entry = {
        "id": "prn-guid-same-poll",
        "guid": "prn-guid-same-poll",
        "title": "Calamos Global Total Return Fund CGO declares distribution",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700003.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }

    stub_feed_io([bad_entry, good_entry], "PR Newswire")
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    articles = db.scalars(select(Article)).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-same-poll")
    ).all()

    assert result["status"] == "success"
    assert result["items_inserted"] == 1
    assert len(articles) == 1
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == articles[0].id

def test_ingest_feed_rechecks_attached_raw_row_when_prefetch_is_stale(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, 0, 15, tzinfo=timezone.utc)
    title = "Calamos Global Total Return Fund CGO declares distribution"
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url="https://www.prnewswire.com/news-releases/calamos-302700004.html",
            title=title,
            published_at=published,
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="validated_token",
        confidence=0.68,
        raw_guid="prn-guid-stale-prefetch",
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        extraction_version=EXTRACTION_VERSION,
        raw_title=title,
        raw_payload_json={"title": title, "summary": ""},
    )

    entry = {
        "id": "prn-guid-stale-prefetch",
        "guid": "prn-guid-stale-prefetch",
        "title": article.title,
        "link": article.canonical_url,
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }
    stub_feed_io([entry], "PR Newswire")
    monkeypatch.setattr(
        "app.article_ingest._prefetch_recorded_raw_keys",
        lambda *_args, **_kwargs: (set(), set()),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = call_ingest(
        db, source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
    )

    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-stale-prefetch")
    ).all()

    assert result["status"] == "success"
    assert result["items_inserted"] == 0
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == article.id

def test_ingest_feed_keeps_distinct_businesswire_same_headline_stories(
    db_session, stub_feed_io
):
    db = db_session
    source = seed_source(db)
    entries = [
        {
            "id": "guid-1",
            "guid": "guid-1",
            "title": "Fund update",
            "link": "https://www.businesswire.com/news/home/20260301000001/en",
            "summary": "Story one",
            "published": "2026-03-01T00:00:00Z",
        },
        {
            "id": "guid-2",
            "guid": "guid-2",
            "title": "Fund update",
            "link": "https://www.businesswire.com/news/home/20260301000002/en",
            "summary": "Story two",
            "published": "2026-03-01T00:05:00Z",
        },
    ]
    stub_feed_io(entries)

    result = call_ingest(
        db, source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
    )

    articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    assert result["status"] == "success"
    assert len(articles) == 2

def test_ingest_feed_skips_when_feed_is_in_failure_backoff(db_session, monkeypatch):
    db = db_session
    source = seed_source(db)
    calls = {"count": 0}

    def failing_get(*_args, **_kwargs):
        calls["count"] += 1
        raise httpx.TimeoutException("upstream unavailable")

    monkeypatch.setattr(
        "app.http_client.get_http_client",
        lambda: SimpleNamespace(get=failing_get),
    )

    first = call_ingest(
        db, source, "https://example.com/backoff.xml",
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )
    second = call_ingest(
        db, source, "https://example.com/backoff.xml",
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )

    state = db.scalar(
        select(FeedPollState).where(
            FeedPollState.feed_url == "https://example.com/backoff.xml"
        )
    )

    assert first["status"] == "failed"
    assert second["status"] == "skipped_backoff"
    assert calls["count"] == 1
    assert state is not None
    assert state.failure_count == 1
    assert state.backoff_until is not None
