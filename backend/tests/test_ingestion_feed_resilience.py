from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from app.raw_feed_items import _preserve_alt_feed_url
from app.feed_runtime import (
    _load_tickers_from_csv_if_changed,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
)
from app.article_ingest import EntryResult
from app.models import (
    Article,
    ArticleTicker,
    IngestionRun,
    RawFeedItem,
    Source,
    Ticker,
)
from app.constants import EXTRACTION_VERSION
from app.utils import parse_datetime, sha256_str
from tests.helpers import (
    call_ingest,
    seed_article,
    seed_article_with_raw,
    seed_source,
    seed_ticker,
)

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


def test_ingest_feed_dedupes_undated_guidless_raw_rows(db_session, stub_feed_io):
    db = db_session
    source = seed_source(db)
    feed_entry = {
        "title": "Fund update without metadata",
        "link": "https://example.com/undated-story",
        "summary": "Summary text",
    }
    stub_feed_io([feed_entry])

    first = call_ingest(db, source, "https://example.com/feed.xml")
    second = call_ingest(db, source, "https://example.com/feed.xml")

    raw_rows = db.scalars(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(raw_rows) == 1
    assert raw_rows[0].raw_guid is None
    assert raw_rows[0].raw_pub_date is None


def test_ingest_feed_refreshes_undated_guidless_exact_url_rows(db_session, stub_feed_io):
    db = db_session
    source = seed_source(db)
    ticker = seed_ticker(db, symbol="UTF")
    first_entry = {
        "title": "ACME distribution update NYSE: UTF",
        "link": "https://example.com/undated-story",
        "summary": "Original summary",
    }
    updated_entry = {
        "title": "UPDATED headline NYSE: UTF",
        "link": "https://example.com/undated-story",
        "summary": "Updated summary",
    }

    stub_feed_io([first_entry])
    first = call_ingest(
        db,
        source,
        "https://example.com/feed.xml",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )
    first_row = db.scalar(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
        .limit(1)
    )

    stub_feed_io([updated_entry])
    second = call_ingest(
        db,
        source,
        "https://example.com/feed.xml",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    article = db.scalar(select(Article).where(Article.canonical_url == updated_entry["link"]))
    raw_rows = db.scalars(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert article is not None
    assert article.title == updated_entry["title"]
    assert article.summary == updated_entry["summary"]
    assert len(raw_rows) == 1
    assert raw_rows[0].raw_pub_date is None
    assert first_row is not None
    assert raw_rows[0].id == first_row.id


def test_ingest_feed_matches_legacy_undated_guidless_rows(db_session, stub_feed_io):
    db = db_session
    source = seed_source(db)
    ticker = seed_ticker(db, symbol="UTF")
    legacy_article = seed_article(
        db,
        canonical_url="https://example.com/undated-story",
        title="Legacy undated item NYSE: UTF",
        summary="Legacy summary",
    )
    legacy_row = RawFeedItem(
        source_id=source.id,
        article_id=legacy_article.id,
        feed_url="https://example.com/feed.xml",
        raw_guid=None,
        raw_title=legacy_article.title,
        raw_link=legacy_article.canonical_url,
        raw_pub_date=legacy_article.published_at,
        raw_payload_json={"title": legacy_article.title, "link": legacy_article.canonical_url},
    )
    db.add(legacy_row)
    db.commit()

    updated_entry = {
        "title": "Legacy undated item updated NYSE: UTF",
        "link": legacy_article.canonical_url,
        "summary": "Updated summary",
    }
    stub_feed_io([updated_entry])

    result = call_ingest(
        db,
        source,
        "https://example.com/feed.xml",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    article = db.scalar(select(Article).where(Article.id == legacy_article.id))
    raw_rows = db.scalars(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
    ).all()
    assert result["status"] == "success"
    assert article is not None
    assert article.title == updated_entry["title"]
    assert len(raw_rows) == 1
    assert raw_rows[0].id == legacy_row.id
    assert raw_rows[0].raw_pub_date is None


def test_ingest_feed_same_guid_different_feed_reuses_single_raw_row(
    db_session, stub_feed_io
):
    db = db_session
    source = seed_source(db)
    feed_entry = {
        "id": "guid-shared",
        "guid": "guid-shared",
        "title": "Fund update",
        "link": "https://example.com/shared-story",
        "summary": "Summary text",
        "published": "2026-01-01T00:00:00Z",
    }
    first_feed_url = "https://example.com/feed-a.xml"
    second_feed_url = "https://example.com/feed-b.xml"

    stub_feed_io([feed_entry])
    first = call_ingest(db, source, first_feed_url)
    first_row = db.scalar(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
        .limit(1)
    )

    stub_feed_io([feed_entry])
    second = call_ingest(db, source, second_feed_url)

    raw_rows = db.scalars(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first_row is not None
    assert len(raw_rows) == 1
    assert raw_rows[0].id == first_row.id
    assert raw_rows[0].feed_url == first_feed_url
    assert raw_rows[0].raw_guid == "guid-shared"
    alt = (raw_rows[0].raw_payload_json or {}).get("_alt_feed_urls") or []
    assert second_feed_url in alt


def test_preserve_alt_feed_url_refreshes_fetched_at_for_prune(db_session):
    db = db_session
    source = seed_source(db)
    article = seed_article(
        db,
        canonical_url="https://example.com/shared-story",
        title="Fund update",
        summary="Summary text",
    )
    stale_time = datetime.now(timezone.utc) - timedelta(days=120)
    row = RawFeedItem(
        source_id=source.id,
        article_id=article.id,
        feed_url="https://example.com/feed-a.xml",
        raw_guid="guid-shared",
        raw_title="Fund update",
        raw_link="https://example.com/shared-story",
        raw_pub_date=parse_datetime("2026-01-01T00:00:00Z"),
        raw_payload_json={"title": "Fund update", "link": "https://example.com/shared-story"},
        fetched_at=stale_time,
    )
    db.add(row)
    db.commit()

    prepared = SimpleNamespace(
        raw_guid="guid-shared",
        raw_link="https://example.com/shared-story",
        raw_pub_date=parse_datetime("2026-01-01T00:00:00Z"),
    )

    _preserve_alt_feed_url(
        db,
        source_id=source.id,
        prepared=prepared,
        feed_url="https://example.com/feed-b.xml",
    )
    db.commit()

    refreshed = db.scalar(select(RawFeedItem).where(RawFeedItem.id == row.id))
    assert refreshed is not None
    assert refreshed.fetched_at is not None
    refreshed_at = refreshed.fetched_at
    if refreshed_at.tzinfo is None:
        refreshed_at = refreshed_at.replace(tzinfo=timezone.utc)
    assert refreshed_at > stale_time

    deleted = prune_raw_feed_items(
        db,
        retention_days=30,
        batch_size=100,
        max_batches=10,
    )
    remaining = db.scalar(select(RawFeedItem).where(RawFeedItem.id == row.id))
    alt = (remaining.raw_payload_json or {}).get("_alt_feed_urls") if remaining else []

    assert deleted == 0
    assert remaining is not None
    assert "https://example.com/feed-b.xml" in (alt or [])


def test_ingest_feed_same_link_and_pubdate_different_guid_reuses_single_raw_row(
    db_session, stub_feed_io
):
    db = db_session
    source = seed_source(db)
    feed_url = "https://example.com/feed.xml"
    first_entry = {
        "id": "guid-a",
        "guid": "guid-a",
        "title": "Fund update",
        "link": "https://example.com/shared-story",
        "summary": "Summary text",
        "published": "2026-01-01T00:00:00Z",
    }
    second_entry = {
        "id": "guid-b",
        "guid": "guid-b",
        "title": "Fund update",
        "link": "https://example.com/shared-story",
        "summary": "Updated summary text",
        "published": "2026-01-01T00:00:00Z",
    }

    stub_feed_io([first_entry])
    first = call_ingest(db, source, feed_url)
    first_row = db.scalar(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
        .limit(1)
    )

    stub_feed_io([second_entry])
    second = call_ingest(db, source, feed_url)

    raw_rows = db.scalars(
        select(RawFeedItem)
        .where(RawFeedItem.source_id == source.id)
        .order_by(RawFeedItem.id.asc())
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first_row is not None
    assert len(raw_rows) == 1
    assert raw_rows[0].id == first_row.id
    assert raw_rows[0].raw_guid == "guid-a"
    assert raw_rows[0].raw_payload_json["summary"] == "Summary text"


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


def test_ingest_feed_businesswire_finance_persists_detached_raw_row(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(db)

    entry = {
        "id": "bw-finance-detached",
        "guid": "bw-finance-detached",
        "title": "Redfin expands brokerage footprint",
        "link": "https://www.businesswire.com/news/home/20260301000011/en/Redfin-Expands-Brokerage-Footprint",
        "summary": "No fund ticker appears in the release copy.",
        "published": "2026-03-01T00:15:00Z",
    }
    stub_feed_io([entry], "Business Wire Professional Services: Finance News")
    monkeypatch.setattr(
        "app.article_ingest._extract_source_fallback_tickers",
        lambda *_args, **_kwargs: {},
    )

    result = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
    )

    articles = db.scalars(select(Article)).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "bw-finance-detached")
    ).all()

    assert result["status"] == "success"
    assert result["items_inserted"] == 0
    assert articles == []
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id is None
    assert raw_rows[0].raw_payload_json["source"] == "Business Wire"


def test_ingest_feed_businesswire_finance_keeps_businesswire_source_name(
    db_session,
    stub_feed_io,
):
    db = db_session
    source = seed_source(db)
    ticker = seed_ticker(db, symbol="UTF")

    entry = {
        "id": "bw-finance-source-name",
        "guid": "bw-finance-source-name",
        "title": "ACME distribution update NYSE: UTF",
        "link": "https://www.businesswire.com/news/home/20260301000012/en/ACME-Distribution-Update",
        "summary": "Finance feed title should not replace the provider label.",
        "published": "2026-03-01T00:20:00Z",
    }
    stub_feed_io([entry], "Business Wire Professional Services: Finance News")

    result = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
    )

    article = db.scalar(select(Article).where(Article.canonical_url == entry["link"]))

    assert result["status"] == "success"
    assert result["items_inserted"] == 1
    assert article is not None
    assert article.source_name == "Business Wire"
    assert article.provider_name == "Business Wire"


def test_ingest_feed_businesswire_finance_exact_url_transient_miss_refreshes_article(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(db)
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = (
        "https://www.businesswire.com/news/home/20260301000014/en/"
        "Acme-Distribution-Update"
    )
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Old finance title",
        summary="Old finance summary",
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

    entry = {
        "id": "bw-finance-refresh-existing",
        "guid": "bw-finance-refresh-existing",
        "title": "Updated finance title without symbol",
        "link": article_url,
        "summary": "Updated finance summary",
        "published": "2026-03-01T00:25:00Z",
    }
    stub_feed_io([entry], "Business Wire Professional Services: Finance News")
    monkeypatch.setattr(
        "app.article_ingest._extract_source_fallback_tickers",
        lambda *_args, **_kwargs: {},
    )

    result = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
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
    assert result["items_inserted"] == 0
    assert article_after is not None
    assert article_after.title == "Updated finance title without symbol"
    assert article_after.summary == "Updated finance summary"
    assert len(ticker_rows_after) == 1
    assert ticker_rows_after[0].ticker_id == ticker.id
    assert len(raw_rows_after) == 1
    assert raw_rows_after[0].raw_guid == "bw-finance-refresh-existing"


def test_ingest_feed_businesswire_finance_exact_url_prunes_missing_tickers(
    db_session,
    stub_feed_io,
):
    db = db_session
    source = seed_source(db)
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = (
        "https://www.businesswire.com/news/home/20260301000015/en/"
        "Acme-Distribution-Update"
    )
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Old finance title",
        summary="Old finance summary",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
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
        "id": "bw-finance-prune-existing",
        "guid": "bw-finance-prune-existing",
        "title": "NYSE: GOF distribution update",
        "link": article_url,
        "summary": "Updated finance summary",
        "published": "2026-03-01T00:30:00Z",
    }
    stub_feed_io([entry], "Business Wire Professional Services: Finance News")

    result = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
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
    assert article_after.summary == "Updated finance summary"
    assert len(ticker_rows_after) == 1
    assert ticker_rows_after[0].ticker_id == new_ticker.id


def test_ingest_feed_businesswire_finance_repeated_detached_raw_skips_repeat_fallback(
    db_session,
    stub_feed_io,
    monkeypatch,
):
    db = db_session
    source = seed_source(db)
    entry = {
        "id": "bw-finance-repeat",
        "guid": "bw-finance-repeat",
        "title": "Redfin expands brokerage footprint",
        "link": "https://www.businesswire.com/news/home/20260301000013/en/Redfin-Expands-Brokerage-Footprint",
        "summary": "No fund ticker appears in the release copy.",
        "published": "2026-03-01T00:15:00Z",
    }
    stub_feed_io([entry], "Business Wire Professional Services: Finance News")

    fallback_calls = {"count": 0}

    def fake_fallback(*_args, **_kwargs):
        fallback_calls["count"] += 1
        return {}

    monkeypatch.setattr(
        "app.article_ingest._extract_source_fallback_tickers",
        fake_fallback,
    )

    first = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
    )
    second = call_ingest(
        db,
        source,
        "https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGFNTXg==",
        persistence_policy_override="validated_mapping_required",
        article_source_name="Business Wire",
    )

    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "bw-finance-repeat")
    ).all()

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert first["items_inserted"] == 0
    assert second["items_inserted"] == 0
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id is None
    assert fallback_calls["count"] == 1

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


def test_ingest_feed_globenewswire_cap_does_not_apply_to_prnewswire(
    db_session, stub_feed_io, monkeypatch
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    entries = [
        {
            "id": f"guid-{idx}",
            "guid": f"guid-{idx}",
            "title": f"PRN story {idx}",
            "link": f"https://www.prnewswire.com/news-releases/story-{idx}.html",
            "summary": "Summary text",
            "published": "2026-01-01T00:00:00Z",
        }
        for idx in range(4)
    ]
    stub_feed_io(entries, "PR Newswire")

    seen_page_configs: list[str | None] = []

    def fake_process_single_entry(*_args, page_config, **_kwargs):
        seen_page_configs.append(None if page_config is None else page_config.source_code)
        return EntryResult()

    monkeypatch.setattr("app.article_ingest._process_single_entry", fake_process_single_entry)

    result = call_ingest(
        db,
        source,
        "https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
        globenewswire_source_page_max_fetches_per_feed=0,
    )

    assert result["status"] == "success"
    assert seen_page_configs == ["prnewswire", "prnewswire", "prnewswire", "prnewswire"]


def test_ingest_feed_globenewswire_budget_ignores_cached_or_skipped_fallbacks(
    db_session, stub_feed_io, monkeypatch
):
    db = db_session
    source = seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    entries = [
        {
            "id": f"guid-{idx}",
            "guid": f"guid-{idx}",
            "title": f"GN story {idx}",
            "link": f"https://www.globenewswire.com/news-release/2026/03/story-{idx}",
            "summary": "Summary text",
            "published": "2026-01-01T00:00:00Z",
        }
        for idx in range(4)
    ]
    stub_feed_io(entries, "GlobeNewswire")

    page_configs: list[str | None] = []
    network_sequence = iter([True, False, True, True])

    def fake_requires_network(_url, _config):
        return next(network_sequence)

    def fake_process_single_entry(*_args, page_config, **_kwargs):
        page_configs.append(None if page_config is None else page_config.source_code)
        return EntryResult(used_page_fetch=page_config is not None)

    monkeypatch.setattr(
        "app.article_ingest._source_page_fetch_requires_network",
        fake_requires_network,
    )
    monkeypatch.setattr("app.article_ingest._process_single_entry", fake_process_single_entry)

    result = call_ingest(
        db,
        source,
        "https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        globenewswire_source_page_max_fetches_per_feed=2,
    )

    assert result["status"] == "success"
    assert page_configs == ["globenewswire", "globenewswire", "globenewswire", None]

