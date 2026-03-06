from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import requests
from requests.structures import CaseInsensitiveDict
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

from app.config import Settings
from app.database import Base
from app.ingestion import (
    _fetch_feed_with_retries,
    _get_feed_conditional_headers,
    _update_feed_http_cache,
    dedupe_articles_by_title,
    dedupe_businesswire_url_variants,
    ingest_feed,
    purge_token_only_articles,
    prune_raw_feed_items,
    reconcile_stale_ingestion_runs,
    remap_source_articles,
)
from app.models import Article, ArticleTicker, FeedPollState, IngestionRun, RawFeedItem, Source, Ticker
from app.utils import sha256_str


def _make_db_session() -> Session:
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(autoflush=False, autocommit=False, bind=engine)
    return session_factory()


def _seed_source(
    db: Session,
    *,
    code: str = "businesswire",
    name: str = "Business Wire",
    base_url: str = "https://www.businesswire.com",
) -> Source:
    source = Source(
        code=code,
        name=name,
        base_url=base_url,
        enabled=True,
    )
    db.add(source)
    db.commit()
    db.refresh(source)
    return source


def _seed_article(
    db: Session,
    *,
    canonical_url: str,
    title: str,
    summary: str,
    published_at: datetime,
    source_name: str,
    provider_name: str,
) -> Article:
    article = Article(
        canonical_url=canonical_url,
        canonical_url_hash=sha256_str(canonical_url),
        title=title,
        summary=summary,
        published_at=published_at,
        source_name=source_name,
        provider_name=provider_name,
        content_hash=sha256_str(f"content:{canonical_url}"),
        title_normalized_hash=sha256_str(f"title:{title.lower()}"),
        cluster_key=sha256_str(f"cluster:{title.lower()}"),
        created_at=published_at,
        updated_at=published_at,
    )
    db.add(article)
    db.commit()
    db.refresh(article)
    return article


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


def test_fetch_feed_with_retries_honors_retry_after_for_429(monkeypatch):
    attempts = {"count": 0}
    slept: list[float] = []

    class RateLimitedResponse:
        status_code = 429
        headers = {"Retry-After": "2"}
        content = b""

        def raise_for_status(self) -> None:
            raise requests.HTTPError("429", response=self)  # type: ignore[arg-type]

    class OkResponse:
        status_code = 200
        headers = {}
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    def fake_get(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return RateLimitedResponse()
        return OkResponse()

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)
    monkeypatch.setattr("app.ingestion.time.sleep", lambda seconds: slept.append(float(seconds)))

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


def test_update_feed_http_cache_reads_requests_case_insensitive_headers():
    db = _make_db_session()
    feed_url = "https://example.com/feed-with-conditional-cache.xml"
    state = FeedPollState(feed_url=feed_url)
    db.add(state)
    db.commit()

    class FakeResponse:
        headers = CaseInsensitiveDict(
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
    db.close()


def test_ingest_feed_persists_conditional_headers_across_runs(monkeypatch):
    db = _make_db_session()
    source = _seed_source(db)
    sent_headers: list[dict[str, str]] = []
    attempts = {"count": 0}

    class FirstResponse:
        status_code = 200
        headers = CaseInsensitiveDict(
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
        headers = CaseInsensitiveDict({})
        content = b""

        def raise_for_status(self) -> None:
            return None

    def fake_get(*_args, **kwargs):
        attempts["count"] += 1
        sent_headers.append(dict(kwargs.get("headers") or {}))
        if attempts["count"] == 1:
            return FirstResponse()
        return NotModifiedResponse()

    monkeypatch.setattr("app.ingestion.requests.get", fake_get)
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[]),
    )

    first = ingest_feed(
        db,
        source=source,
        feed_url="https://example.com/conditional.xml",
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
        feed_url="https://example.com/conditional.xml",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    state = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == "https://example.com/conditional.xml"))

    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(sent_headers) == 2
    assert sent_headers[1].get("If-None-Match") == '"persisted-etag"'
    assert sent_headers[1].get("If-Modified-Since") == "Tue, 03 Mar 2026 10:00:00 GMT"
    assert state is not None and state.etag == '"persisted-etag"'
    db.close()


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


def test_ingest_feed_dedupes_businesswire_story_across_yahoo_and_bw(monkeypatch):
    db = _make_db_session()
    yahoo = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    businesswire = _seed_source(db)

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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Yahoo Finance"}, entries=[yahoo_entry]),
    )
    yahoo_run = ingest_feed(
        db,
        source=yahoo,
        feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
        known_symbols={"UTF"},
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=[bw_entry]),
    )
    bw_run = ingest_feed(
        db,
        source=businesswire,
        feed_url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
        known_symbols={"UTF"},
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    raw_rows = db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    assert yahoo_run["status"] == "success"
    assert bw_run["status"] == "success"
    assert len(articles) == 1
    assert articles[0].canonical_url == "https://www.businesswire.com/news/home/20260301000001/en"
    raw_links_by_source = {row.source_id: row.raw_link for row in raw_rows}
    assert raw_links_by_source.get(yahoo.id) == (
        "https://www.businesswire.com/news/home/20260301000001/en"
        "?feedref=JjAwJuNHiystnCoBq_hl-XxV8f8yqXw8M0Q"
    )
    if businesswire.id in raw_links_by_source:
        assert raw_links_by_source[businesswire.id] == "https://www.businesswire.com/news/home/20260301000001/en"
    db.close()


def test_ingest_feed_mirrored_bw_url_does_not_prune_existing_tickers(monkeypatch):
    db = _make_db_session()
    yahoo = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
    article = _seed_article(
        db,
        canonical_url=canonical_url,
        title="ACME distribution update",
        summary="Original summary",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    ticker = Ticker(symbol="UTF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Yahoo Finance"}, entries=[yahoo_entry]),
    )

    result = ingest_feed(
        db,
        source=yahoo,
        # Multi-symbol feed URL disables context-symbol auto-hit.
        feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF,GOF",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    tickers_after = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    assert result["status"] == "success"
    assert len(tickers_after) == 1
    assert tickers_after[0].ticker_id == ticker.id
    db.close()


def test_ingest_feed_mirrored_bw_url_does_not_overwrite_canonical_metadata(monkeypatch):
    db = _make_db_session()
    yahoo = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
    article = _seed_article(
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Yahoo Finance"}, entries=[mirror_entry]),
    )

    result = ingest_feed(
        db,
        source=yahoo,
        feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF,GOF",
        known_symbols={"UTF"},
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    assert result["status"] == "success"
    assert article_after is not None
    assert article_after.title == "Canonical BW headline"
    assert article_after.summary == "Canonical Business Wire summary with richer details."
    assert article_after.source_name == "Business Wire"
    assert article_after.provider_name == "Business Wire"
    assert article_after.published_at is not None
    assert article_after.published_at.replace(tzinfo=timezone.utc) == published
    db.close()


def test_ingest_feed_non_bw_query_url_still_allows_exact_update_and_prune(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    url_with_query = "https://www.prnewswire.com/news-releases/acme-update.html?id=123"
    article = _seed_article(
        db,
        canonical_url=url_with_query,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    old_ticker = Ticker(symbol="UTF", active=True)
    new_ticker = Ticker(symbol="GOF", active=True)
    db.add_all([old_ticker, new_ticker])
    db.commit()
    db.refresh(old_ticker)
    db.refresh(new_ticker)
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "PR Newswire"}, entries=[entry]),
    )

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF", "GOF"},
        symbol_to_id={"UTF": old_ticker.id, "GOF": new_ticker.id},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    db.close()


def test_ingest_feed_exact_url_transient_miss_keeps_existing_tickers(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = "https://www.prnewswire.com/news-releases/acme-update.html?id=123"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="UTF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "PR Newswire"}, entries=[entry]),
    )
    monkeypatch.setattr("app.ingestion._fetch_source_page_html", lambda *_args, **_kwargs: "")

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    db.close()


def test_ingest_feed_exact_url_sub_threshold_hits_still_refresh_existing_article(
    monkeypatch,
):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = "https://www.prnewswire.com/news-releases/acme-update.html?id=456"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Old title",
        summary="Old summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="UTF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "PR Newswire"}, entries=[entry]),
    )
    monkeypatch.setattr("app.ingestion._fetch_source_page_html", lambda *_args, **_kwargs: "")

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"UTF"},
        symbol_to_id={"UTF": ticker.id},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    db.close()


def test_ingest_feed_keeps_distinct_businesswire_same_headline_stories(monkeypatch):
    db = _make_db_session()
    source = _seed_source(db)
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr("app.ingestion.requests.get", lambda *_args, **_kwargs: FakeResponse())
    monkeypatch.setattr(
        "app.ingestion.feedparser.parse",
        lambda _content: SimpleNamespace(feed={"title": "Business Wire"}, entries=entries),
    )

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    assert result["status"] == "success"
    assert len(articles) == 2
    db.close()


def test_dedupe_businesswire_url_variants_merges_historical_rows():
    db = _make_db_session()
    yahoo = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    businesswire = _seed_source(db)
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    query_url = (
        "https://www.businesswire.com/news/home/20260301000001/en"
        "?feedref=JjAwJuNHiystnCoBq_hl-XxV8f8yqXw8M0Q"
    )
    clean_url = "https://www.businesswire.com/news/home/20260301000001/en"
    yahoo_article = _seed_article(
        db,
        canonical_url=query_url,
        title="ACME distribution update",
        summary="Short Yahoo summary",
        published_at=published,
        source_name="Yahoo Finance",
        provider_name="Yahoo Finance",
    )
    businesswire_article = _seed_article(
        db,
        canonical_url=clean_url,
        title="ACME distribution update",
        summary="Business Wire summary has more context than Yahoo copy",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    ticker = Ticker(symbol="UTF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add_all([
        ArticleTicker(
            article_id=yahoo_article.id,
            ticker_id=ticker.id,
            match_type="token",
            confidence=0.62,
        ),
        ArticleTicker(
            article_id=businesswire_article.id,
            ticker_id=ticker.id,
            match_type="exchange",
            confidence=0.88,
        ),
        RawFeedItem(
            source_id=yahoo.id,
            article_id=yahoo_article.id,
            feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
            raw_guid="y-guid-1",
            raw_link=query_url,
            raw_pub_date=published,
            raw_payload_json={},
        ),
        RawFeedItem(
            source_id=businesswire.id,
            article_id=businesswire_article.id,
            feed_url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
            raw_guid="bw-guid-1",
            raw_link=clean_url,
            raw_pub_date=published,
            raw_payload_json={},
        ),
    ])
    db.commit()

    result = dedupe_businesswire_url_variants(db)

    remaining_articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    assert result["duplicate_groups"] == 1
    assert result["merged_articles"] == 1
    assert result["raw_items_relinked"] == 1
    assert result["ticker_rows_deleted"] == 1
    assert len(remaining_articles) == 1
    winner = remaining_articles[0]
    assert winner.canonical_url == clean_url

    raw_article_ids = {
        row.article_id
        for row in db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    }
    assert raw_article_ids == {winner.id}
    winner_tickers = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == winner.id)
    ).all()
    assert len(winner_tickers) == 1
    assert winner_tickers[0].confidence == 0.88
    assert winner_tickers[0].match_type == "exchange"
    db.close()


def test_dedupe_businesswire_url_variants_skips_distinct_story_ids():
    db = _make_db_session()
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    _seed_article(
        db,
        canonical_url="https://www.businesswire.com/news/home/20260301000001/en",
        title="Fund update",
        summary="Story one",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    _seed_article(
        db,
        canonical_url="https://www.businesswire.com/news/home/20260301000002/en",
        title="Fund update",
        summary="Story two",
        published_at=published + timedelta(minutes=5),
        source_name="Business Wire",
        provider_name="Business Wire",
    )

    result = dedupe_businesswire_url_variants(db)

    articles = db.scalars(select(Article).order_by(Article.id.asc())).all()
    assert result["scanned_articles"] == 2
    assert result["duplicate_groups"] == 0
    assert result["merged_articles"] == 0
    assert len(articles) == 2
    db.close()


def test_ingest_feed_skips_when_feed_is_in_failure_backoff(monkeypatch):
    db = _make_db_session()
    source = _seed_source(db)
    calls = {"count": 0}

    def failing_get(*_args, **_kwargs):
        calls["count"] += 1
        raise requests.Timeout("upstream unavailable")

    monkeypatch.setattr("app.ingestion.requests.get", failing_get)

    first = ingest_feed(
        db,
        source=source,
        feed_url="https://example.com/backoff.xml",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )
    second = ingest_feed(
        db,
        source=source,
        feed_url="https://example.com/backoff.xml",
        known_symbols=set(),
        symbol_to_id={},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
        failure_backoff_base_seconds=30.0,
        failure_backoff_max_seconds=600.0,
    )

    state = db.scalar(select(FeedPollState).where(FeedPollState.feed_url == "https://example.com/backoff.xml"))

    assert first["status"] == "failed"
    assert second["status"] == "skipped_backoff"
    assert calls["count"] == 1
    assert state is not None
    assert state.failure_count == 1
    assert state.backoff_until is not None
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


def test_purge_token_only_articles_rechecks_source_fallback_before_deleting(
    monkeypatch,
):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article_url = (
        "https://www.prnewswire.com/news-releases/"
        "envestnet-accelerates-adaptive-wealthtech-innovation-301000000.html"
    )
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=ticker.id,
                match_type="token",
                confidence=0.62,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-1",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <table><tr><td>CGO</td></tr></table>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 1
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0
    db.close()


def test_purge_token_only_articles_skips_unknown_provenance_articles():
    db = _make_db_session()
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://example.com/legacy-pmo-story",
        title="Mace Consult Launches as Standalone PMO Company",
        summary="",
        published_at=published,
        source_name="Legacy Mirror",
        provider_name="Legacy Mirror",
    )
    ticker = Ticker(
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
        active=True,
    )
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=ticker.id,
            match_type="token",
            confidence=0.62,
        )
    )
    db.commit()

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    remaining_article = db.scalar(select(Article).where(Article.id == article.id))
    remaining_tickers = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["scanned_articles"] == 0
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0
    assert remaining_article is not None
    assert len(remaining_tickers) == 1
    db.close()


def test_remap_source_articles_skips_sub_threshold_non_bw_hits(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = (
        "https://www.prnewswire.com/news-releases/"
        "envestnet-accelerates-adaptive-wealthtech-innovation-301000001.html"
    )
    _seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    db.add(
        Ticker(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
            active=True,
        )
    )
    db.commit()
    article = db.scalar(select(Article).where(Article.canonical_url == article_url))
    assert article is not None
    db.add(
        RawFeedItem(
            source_id=source.id,
            article_id=article.id,
            feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
            raw_guid="prn-guid-remap-1",
            raw_link=article_url,
            raw_pub_date=published,
            raw_payload_json={},
        )
    )
    db.commit()

    fetch_calls = {"count": 0}

    def fake_fetch(_url, _timeout, _config):
        fetch_calls["count"] += 1
        return ""

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=True,
    )

    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert fetch_calls["count"] == 1
    assert result["processed"] == 1
    assert result["articles_with_hits"] == 0
    assert result["remapped_articles"] == 0
    assert ticker_rows == []
    db.close()


def test_remap_source_articles_uses_fallback_after_low_confidence_tokens(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = (
        "https://www.prnewswire.com/news-releases/"
        "envestnet-accelerates-adaptive-wealthtech-innovation-301000002.html"
    )
    _seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    article = db.scalar(select(Article).where(Article.canonical_url == article_url))
    assert article is not None
    db.add(
        RawFeedItem(
            source_id=source.id,
            article_id=article.id,
            feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
            raw_guid="prn-guid-remap-2",
            raw_link=article_url,
            raw_pub_date=published,
            raw_payload_json={},
        )
    )
    db.commit()

    def fake_fetch(_url, _timeout, _config):
        return """
        <html><body>
          <table><tr><td>CGO</td></tr></table>
        </body></html>
        """

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=True,
    )

    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["processed"] == 1
    assert result["articles_with_hits"] == 1
    assert result["remapped_articles"] == 1
    assert len(ticker_rows) == 1
    assert ticker_rows[0].ticker_id == ticker.id
    assert ticker_rows[0].match_type == "prn_table"
    assert ticker_rows[0].confidence == 0.84
    db.close()


def test_purge_token_only_articles_limit_applies_to_distinct_articles(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published_recent = datetime(2026, 3, 3, tzinfo=timezone.utc)
    published_older = datetime(2026, 3, 2, tzinfo=timezone.utc)

    cgo = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    pmo = Ticker(
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
        active=True,
    )
    ft = Ticker(
        symbol="FT",
        fund_name="Franklin Universal Trust",
        sponsor="Franklin",
        active=True,
    )
    db.add_all([cgo, pmo, ft])
    db.commit()
    db.refresh(cgo)
    db.refresh(pmo)
    db.refresh(ft)

    recent_article = _seed_article(
        db,
        canonical_url="https://example.com/recent-false-positive",
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published_recent,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    older_article = _seed_article(
        db,
        canonical_url="https://example.com/older-false-positive",
        title="Mace Consult Launches as Standalone PMO Company",
        summary="",
        published_at=published_older,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )

    db.add_all(
        [
            ArticleTicker(
                article_id=recent_article.id,
                ticker_id=cgo.id,
                match_type="token",
                confidence=0.62,
            ),
            ArticleTicker(
                article_id=recent_article.id,
                ticker_id=ft.id,
                match_type="token",
                confidence=0.62,
            ),
            ArticleTicker(
                article_id=older_article.id,
                ticker_id=pmo.id,
                match_type="token",
                confidence=0.62,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=recent_article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-purge-limit-1",
                raw_link=recent_article.canonical_url,
                raw_pub_date=published_recent,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=older_article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-purge-limit-2",
                raw_link=older_article.canonical_url,
                raw_pub_date=published_older,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", lambda *_args: "")

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=2,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 2
    assert result["purged_articles"] == 2
    assert result["deleted_article_tickers"] == 3
    assert result["deleted_raw_feed_items"] == 2
    db.close()


def test_purge_token_only_articles_pages_past_recent_valid_rows(monkeypatch):
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published_recent = datetime(2026, 3, 4, tzinfo=timezone.utc)
    published_mid = datetime(2026, 3, 3, tzinfo=timezone.utc)
    published_older = datetime(2026, 3, 2, tzinfo=timezone.utc)

    cgo = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    ft = Ticker(
        symbol="FT",
        fund_name="Franklin Universal Trust",
        sponsor="Franklin",
        active=True,
    )
    pmo = Ticker(
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
        active=True,
    )
    db.add_all([cgo, ft, pmo])
    db.commit()
    db.refresh(cgo)
    db.refresh(ft)
    db.refresh(pmo)

    recent_valid = _seed_article(
        db,
        canonical_url="https://example.com/recent-valid-cgo",
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published_recent,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    mid_valid = _seed_article(
        db,
        canonical_url="https://example.com/mid-valid-ft",
        title="Sokin Appoints Former FT Partners VP Tom Steer as CFO",
        summary="",
        published_at=published_mid,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    older_false_positive = _seed_article(
        db,
        canonical_url="https://example.com/older-false-positive-pmo",
        title="Mace Consult Launches as Standalone PMO Company",
        summary="",
        published_at=published_older,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )

    db.add_all(
        [
            ArticleTicker(
                article_id=recent_valid.id,
                ticker_id=cgo.id,
                match_type="token",
                confidence=0.62,
            ),
            ArticleTicker(
                article_id=mid_valid.id,
                ticker_id=ft.id,
                match_type="token",
                confidence=0.62,
            ),
            ArticleTicker(
                article_id=older_false_positive.id,
                ticker_id=pmo.id,
                match_type="token",
                confidence=0.62,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=recent_valid.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-page-1",
                raw_link=recent_valid.canonical_url,
                raw_pub_date=published_recent,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=mid_valid.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-page-2",
                raw_link=mid_valid.canonical_url,
                raw_pub_date=published_mid,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=older_false_positive.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-page-3",
                raw_link=older_false_positive.canonical_url,
                raw_pub_date=published_older,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    def fake_fetch(url, _timeout, _config):
        if url == recent_valid.canonical_url:
            return "<html><body><table><tr><td>CGO</td></tr></table></body></html>"
        if url == mid_valid.canonical_url:
            return "<html><body><table><tr><td>FT</td></tr></table></body></html>"
        return ""

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", fake_fetch)

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=1,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 3
    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 1
    db.close()


def test_purge_token_only_articles_dry_run_false_deletes_rows(monkeypatch):
    """Verify the real DELETE path actually removes articles, tickers, and raw items."""
    db = _make_db_session()
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://example.com/purge-real-delete",
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=ticker.id,
                match_type="token",
                confidence=0.62,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-real-delete",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr("app.ingestion._fetch_source_page_html", lambda *_args: "")

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 1

    # Verify rows are actually gone from the database.
    assert db.scalar(select(Article).where(Article.id == article.id)) is None
    assert (
        db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id == article.id)
        ).all()
        == []
    )
    assert (
        db.scalars(
            select(RawFeedItem).where(RawFeedItem.article_id == article.id)
        ).all()
        == []
    )
    db.close()


def test_dedupe_articles_by_title_merges_duplicates():
    """Basic coverage: two non-BW articles with the same normalized title get merged."""
    db = _make_db_session()
    prn_source = _seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    gnw_source = _seed_source(db, code="globenewswire", name="GlobeNewswire", base_url="https://rss.globenewswire.com")

    published = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    title = "Acme Corp Declares Quarterly Distribution"

    article_prn = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/acme-distribution.html",
        title=title,
        summary="Short PRN summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    article_gnw = _seed_article(
        db,
        canonical_url="https://www.globenewswire.com/news-release/acme-distribution",
        title=title,
        summary="Longer GlobeNewswire summary with more detail",
        published_at=published + timedelta(minutes=5),
        source_name="GlobeNewswire",
        provider_name="GlobeNewswire",
    )

    ticker = Ticker(symbol="ACME", fund_name="Acme Fund", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)

    db.add_all(
        [
            ArticleTicker(article_id=article_prn.id, ticker_id=ticker.id, match_type="exchange", confidence=0.88),
            ArticleTicker(article_id=article_gnw.id, ticker_id=ticker.id, match_type="paren", confidence=0.75),
            RawFeedItem(
                source_id=prn_source.id,
                article_id=article_prn.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-dedupe-1",
                raw_link=article_prn.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=gnw_source.id,
                article_id=article_gnw.id,
                feed_url="https://rss.globenewswire.com/en/RssFeed/subjectcode/12",
                raw_guid="gnw-dedupe-1",
                raw_link=article_gnw.canonical_url,
                raw_pub_date=published + timedelta(minutes=5),
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    result = dedupe_articles_by_title(db, window_hours=48)

    assert result["duplicate_groups"] >= 1
    assert result["merged_articles"] == 1

    # The GNW article has longer summary + later publish time, so should be the winner.
    winner = db.scalar(select(Article).where(Article.id == article_gnw.id))
    loser = db.scalar(select(Article).where(Article.id == article_prn.id))
    assert winner is not None
    assert loser is None

    # The winner should have the longer summary.
    assert winner.summary == "Longer GlobeNewswire summary with more detail"

    # Ticker should still be attached.
    remaining_tickers = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == winner.id)
    ).all()
    assert len(remaining_tickers) >= 1

    # Raw feed items should be re-linked to the winner.
    raw_items = db.scalars(
        select(RawFeedItem).where(RawFeedItem.article_id == winner.id)
    ).all()
    assert len(raw_items) == 2
    db.close()
