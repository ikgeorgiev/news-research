from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from requests.structures import CaseInsensitiveDict
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


def test_reconcile_stale_ingestion_runs_marks_only_stale_running_rows(db_session):
    db = db_session
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

    monkeypatch.setattr("app.feed_runtime.requests.get", fake_get)
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

    monkeypatch.setattr("app.feed_runtime.requests.get", fake_get)
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


def test_ingest_feed_persists_conditional_headers_across_runs(db_session, monkeypatch):
    db = db_session
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

    monkeypatch.setattr("app.ticker_extraction.requests.get", fake_get)
    monkeypatch.setattr("app.feed_runtime.requests.get", fake_get)
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
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


def test_ingest_feed_dedupes_raw_feed_rows(db_session, monkeypatch):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Business Wire"}, entries=[feed_entry]
        ),
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

    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.source_id == source.id)
    ).all()
    assert first["status"] == "success"
    assert second["status"] == "success"
    assert len(raw_rows) == 1


def test_load_tickers_from_csv_if_changed_retries_after_transient_loader_failure(
    db_session, monkeypatch
):
    db = db_session
    db.add(Ticker(symbol="GOF", active=True))
    db.commit()

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


def test_ingest_feed_dedupes_businesswire_story_across_yahoo_and_bw(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Yahoo Finance"}, entries=[yahoo_entry]
        ),
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
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Business Wire"}, entries=[bw_entry]
        ),
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
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Yahoo Finance"}, entries=[yahoo_entry]
        ),
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


def test_ingest_feed_mirrored_bw_url_does_not_overwrite_canonical_metadata(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Yahoo Finance"}, entries=[mirror_entry]
        ),
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
    assert (
        article_after.summary == "Canonical Business Wire summary with richer details."
    )
    assert article_after.source_name == "Business Wire"
    assert article_after.provider_name == "Business Wire"
    assert article_after.published_at is not None
    assert article_after.published_at.replace(tzinfo=timezone.utc) == published


def test_ingest_feed_non_bw_query_url_still_allows_exact_update_and_prune(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[entry]
        ),
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


def test_ingest_feed_exact_url_transient_miss_keeps_existing_tickers(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[entry]
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

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


def test_ingest_feed_exact_url_sub_threshold_hits_still_refresh_existing_article(
    db_session,
    monkeypatch,
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[entry]
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

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


def test_ingest_feed_rejected_strict_source_entry_persists_detached_raw_row(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
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

    entry = {
        "id": "prn-guid-detached-raw",
        "guid": "prn-guid-detached-raw",
        "title": "Envestnet appoints new CGO to lead growth strategy",
        "link": "https://www.prnewswire.com/news-releases/envestnet-growth-302700001.html",
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[entry]
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[bad_entry]
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    first = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[good_entry]
        ),
    )

    second = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
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

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"},
            entries=[bad_entry, good_entry],
        ),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
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
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
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

    published = datetime(2026, 3, 1, 0, 15, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302700004.html",
        title="Calamos Global Total Return Fund CGO declares distribution",
        summary="",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=ticker.id,
                match_type="validated_token",
                confidence=0.68,
                extraction_version=EXTRACTION_VERSION,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
                raw_guid="prn-guid-stale-prefetch",
                raw_link=article.canonical_url,
                raw_title=article.title,
                raw_pub_date=published,
                raw_payload_json={"title": article.title, "summary": ""},
            ),
        ]
    )
    db.commit()

    entry = {
        "id": "prn-guid-stale-prefetch",
        "guid": "prn-guid-stale-prefetch",
        "title": article.title,
        "link": article.canonical_url,
        "summary": "",
        "published": "2026-03-01T00:15:00Z",
    }

    class FakeResponse:
        content = b"<rss />"

        def raise_for_status(self) -> None:
            return None

    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "PR Newswire"}, entries=[entry]
        ),
    )
    monkeypatch.setattr(
        "app.article_ingest._prefetch_recorded_raw_keys",
        lambda *_args, **_kwargs: (set(), set()),
    )
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = ingest_feed(
        db,
        source=source,
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/dividends-list.rss",
        known_symbols={"CGO"},
        symbol_to_id={"CGO": ticker.id},
        symbol_keywords={"CGO": frozenset({"calamos", "calamos global"})},
        timeout_seconds=5,
        fetch_max_attempts=1,
        fetch_backoff_seconds=0.0,
        fetch_backoff_jitter_seconds=0.0,
    )

    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-stale-prefetch")
    ).all()

    assert result["status"] == "success"
    assert result["items_inserted"] == 0
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == article.id


def test_ingest_feed_keeps_distinct_businesswire_same_headline_stories(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.feed_runtime.requests.get", lambda *_args, **_kwargs: FakeResponse()
    )
    monkeypatch.setattr(
        "app.article_ingest.feedparser.parse",
        lambda _content: SimpleNamespace(
            feed={"title": "Business Wire"}, entries=entries
        ),
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


def test_dedupe_businesswire_url_variants_merges_historical_rows(db_session):
    db = db_session
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
    db.add_all(
        [
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
        ]
    )
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


def test_dedupe_businesswire_url_variants_skips_distinct_story_ids(db_session):
    db = db_session
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


def test_ingest_feed_skips_when_feed_is_in_failure_backoff(db_session, monkeypatch):
    db = db_session
    source = _seed_source(db)
    calls = {"count": 0}

    def failing_get(*_args, **_kwargs):
        calls["count"] += 1
        raise requests.Timeout("upstream unavailable")

    monkeypatch.setattr("app.feed_runtime.requests.get", failing_get)

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


def test_prune_raw_feed_items_respects_retention_window(db_session):
    db = db_session
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
        row.raw_guid
        for row in db.scalars(select(RawFeedItem).order_by(RawFeedItem.id.asc())).all()
    }

    assert deleted == 1
    assert remaining_guids == {"fresh-guid"}


def test_purge_token_only_articles_does_not_trust_table_only_fallback(
    db_session,
    monkeypatch,
):
    db = db_session
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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 1
    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 1


def test_purge_token_only_articles_skips_unknown_provenance_articles(db_session):
    db = db_session
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


def test_purge_token_only_articles_skips_high_confidence_verified_rows(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://example.com/high-confidence-prn",
        title="NYSE: CGO distribution update",
        summary="",
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
                match_type="exchange",
                confidence=0.88,
                extraction_version=EXTRACTION_VERSION,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-high-confidence",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("verified entry hits should skip fallback fetch")
        ),
    )

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    # Purge stays scoped to low-confidence candidates, so verified exchange hits
    # are not revalidated at all.
    assert result["scanned_articles"] == 0
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0


def test_purge_token_only_articles_skips_high_confidence_page_derived_paren_rows(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url=(
            "https://www.globenewswire.com/en/news-release/2026/03/10/3253311/0/en/"
            "Artiva-Biotherapeutics-Reports-Full-Year-2025-Financial-Results-and-Recent-Business-Highlights.html"
        ),
        title="Distribution update",
        summary="",
        published_at=published,
        source_name="GlobeNewswire",
        provider_name="GlobeNewswire",
    )
    ticker = Ticker(
        symbol="RA",
        fund_name="Brookfield Real Assets Income Fund Inc.",
        sponsor="Brookfield Public Securities Group LLC",
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
                match_type="paren",
                confidence=0.75,
                extraction_version=EXTRACTION_VERSION,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url=(
                    "https://rss.globenewswire.com/en/RssFeed/subjectcode/"
                    "13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/"
                    "Earnings%20Releases%20And%20Operating%20Results"
                ),
                raw_guid="gnw-guid-page-derived-paren",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(
                "high-confidence page-derived rows should not be revalidated destructively"
            )
        ),
    )

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 0
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0


def test_purge_token_only_articles_skips_high_confidence_page_derived_validated_token_rows(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
        title="Distribution update",
        summary="",
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
                match_type="validated_token",
                confidence=0.68,
                extraction_version=EXTRACTION_VERSION,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-page-derived-validated-token",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(
                "high-confidence page-derived rows should not be revalidated destructively"
            )
        ),
    )

    result = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 0
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0


def test_purge_token_only_articles_prunes_token_rows_when_verified_rows_already_exist(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url=(
            "https://www.globenewswire.com/en/news-release/2026/03/10/3253311/0/en/"
            "Artiva-Biotherapeutics-Reports-Full-Year-2025-Financial-Results-and-Recent-Business-Highlights.html"
        ),
        title="Distribution update",
        summary="",
        published_at=published,
        source_name="GlobeNewswire",
        provider_name="GlobeNewswire",
    )
    verified_ticker = Ticker(
        symbol="RA",
        fund_name="Brookfield Real Assets Income Fund Inc.",
        sponsor="Brookfield Public Securities Group LLC",
        active=True,
    )
    token_ticker = Ticker(
        symbol="ARTV",
        fund_name="Artiva Biotherapeutics, Inc.",
        sponsor="Artiva",
        active=True,
    )
    db.add_all([verified_ticker, token_ticker])
    db.commit()
    db.refresh(verified_ticker)
    db.refresh(token_ticker)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=verified_ticker.id,
                match_type="paren",
                confidence=0.75,
            ),
            ArticleTicker(
                article_id=article.id,
                ticker_id=token_ticker.id,
                match_type="token",
                confidence=0.2,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url=(
                    "https://rss.globenewswire.com/en/RssFeed/subjectcode/"
                    "13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/"
                    "Earnings%20Releases%20And%20Operating%20Results"
                ),
                raw_guid="gnw-guid-mixed-page-derived-token",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError(
                "mixed verified articles should prune token rows without refetching"
            )
        ),
    )

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    remaining_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    remaining_raw = db.scalars(
        select(RawFeedItem).where(RawFeedItem.article_id == article.id)
    ).all()

    assert result["scanned_articles"] == 1
    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 0
    assert len(remaining_rows) == 1
    assert remaining_rows[0].ticker_id == verified_ticker.id
    assert len(remaining_raw) == 1


def test_purge_token_only_articles_prunes_stale_rows_after_partial_revalidation(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
        title="Calamos Global Total Return Fund CGO declares monthly distribution",
        summary="Legacy PMO mention should be removed",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    verified_ticker = Ticker(
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
        active=True,
    )
    stale_ticker = Ticker(
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
        active=True,
    )
    db.add_all([verified_ticker, stale_ticker])
    db.commit()
    db.refresh(verified_ticker)
    db.refresh(stale_ticker)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=verified_ticker.id,
                match_type="token",
                confidence=0.62,
            ),
            ArticleTicker(
                article_id=article.id,
                ticker_id=stale_ticker.id,
                match_type="token",
                confidence=0.62,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-partial-revalidation",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args: "")

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    remaining_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["scanned_articles"] == 1
    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 0
    assert len(remaining_rows) == 1


def test_purge_token_only_articles_rechecks_stale_high_confidence_rows(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 12, 14, 50, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url=(
            "https://www.prnewswire.com/news-releases/"
            "ecovadis-and-watershed-partner-to-close-the-scope-3-data-gap-302712475.html"
        ),
        title="EcoVadis and Watershed partner to close the Scope 3 data gap",
        summary=(
            "Combined with the launch of EcoVadis PCF Calculator, the partnership "
            "with Watershed is a cornerstone of EcoVadis' mission."
        ),
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="PCF",
        fund_name="High Income Securities",
        sponsor="Bulldog Investors LLP",
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
                match_type="paren",
                confidence=0.70,
                extraction_version=EXTRACTION_VERSION - 1,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
                raw_guid="prn-guid-stale-high-confidence-pcf",
                raw_link=article.canonical_url,
                raw_title=article.title,
                raw_pub_date=published,
                raw_payload_json={"summary": article.summary, "title": article.title},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (
            "<html><body><article><p>"
            "EcoVadis and Watershed partner to close the Scope 3 data gap."
            "</p></article></body></html>"
        ),
    )

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    assert result["scanned_articles"] == 1
    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    remaining_article = db.scalar(select(Article).where(Article.id == article.id))
    remaining_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    detached_raw = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-stale-high-confidence-pcf")
    ).all()

    assert remaining_article is None
    assert remaining_rows == []
    assert detached_raw and all(row.article_id is None for row in detached_raw)


def test_purge_token_only_articles_restamps_unchanged_stale_verified_rows(
    db_session,
    monkeypatch,
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 12, 15, 10, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302712476.html",
        title="Calamos Global Total Return Fund CGO declares monthly distribution",
        summary="",
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
                match_type="validated_token",
                confidence=0.68,
                extraction_version=EXTRACTION_VERSION - 1,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-stale-unchanged-cgo",
                raw_link=article.canonical_url,
                raw_title=article.title,
                raw_pub_date=published,
                raw_payload_json={"summary": article.summary, "title": article.title},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("verified entry hits should avoid fallback fetch")
        ),
    )

    first = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )
    refreshed_row = db.scalar(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    )
    second = purge_token_only_articles(
        db,
        dry_run=True,
        limit=10,
        timeout_seconds=5,
    )

    assert first["scanned_articles"] == 1
    assert first["purged_articles"] == 0
    assert first["deleted_article_tickers"] == 0
    assert refreshed_row is not None
    assert refreshed_row.extraction_version == EXTRACTION_VERSION
    assert second["scanned_articles"] == 0
    assert second["purged_articles"] == 0


def test_purge_token_only_articles_preserves_stale_high_confidence_on_fetch_miss(
    db_session,
    monkeypatch,
):
    """A stale exchange:0.88 row whose symbol only appeared on the source page
    must survive a transient page-fetch failure during purge."""
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 11, 21, 0, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/thornburg-awards-302711484.html",
        title="With Intelligence Honors Thornburg with Two 2026 Mutual Fund & ETF Awards",
        summary="Thornburg takes top accolades in multi-asset mutual fund of the year.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="TBLD",
        fund_name="Thornburg Income Builder Opp Trust",
        sponsor="Thornburg Investment Management Inc",
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
                match_type="exchange",
                confidence=0.88,
                extraction_version=EXTRACTION_VERSION - 1,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
                raw_guid="prn-guid-tbld-stale-page-miss",
                raw_link=article.canonical_url,
                raw_title=article.title,
                raw_pub_date=published,
                raw_payload_json={"title": article.title, "summary": article.summary},
            ),
        ]
    )
    db.commit()

    # Source page returns None — simulates timeout / 404.
    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html",
        lambda *_args, **_kwargs: None,
    )

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    remaining_article = db.scalar(select(Article).where(Article.id == article.id))
    remaining_row = db.scalar(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    )

    assert result["purged_articles"] == 0
    assert remaining_article is not None
    assert remaining_row is not None
    assert remaining_row.match_type == "exchange"
    assert remaining_row.confidence == 0.88
    assert remaining_row.extraction_version == EXTRACTION_VERSION

    # Second run should not re-select this article.
    second = purge_token_only_articles(
        db, dry_run=True, limit=10, timeout_seconds=5,
    )
    assert second["scanned_articles"] == 0


def test_remap_source_articles_unmapped_miss_is_non_destructive(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=True,
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-remap-1")
    ).all()

    assert fetch_calls["count"] == 1
    assert result["processed"] == 1
    assert result["articles_with_hits"] == 0
    assert result["remapped_articles"] == 0
    assert article_after is not None
    assert ticker_rows == []
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == article.id


def test_remap_source_articles_full_remap_miss_is_non_destructive(
    db_session, monkeypatch
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/legacy-remap-miss"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="No explicit fund keywords remain in stored text.",
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
                match_type="prn_table",
                confidence=0.84,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-miss",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=False,
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-remap-miss")
    ).all()

    assert result["processed"] == 1
    assert result["articles_with_hits"] == 0
    assert result["remapped_articles"] == 0
    assert article_after is not None
    assert len(ticker_rows) == 1
    assert ticker_rows[0].ticker_id == ticker.id
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id == article.id


def test_remap_source_articles_full_remap_partial_hits_stay_additive(
    db_session, monkeypatch
):
    db = db_session
    source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/legacy-remap-partial"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbols remain in stored text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    gof = Ticker(
        symbol="GOF",
        fund_name="Guggenheim Strategic Opportunities Fund",
        sponsor="Guggenheim",
        active=True,
    )
    pdi = Ticker(
        symbol="PDI",
        fund_name="PIMCO Dynamic Income Fund",
        sponsor="PIMCO",
        active=True,
    )
    db.add_all([gof, pdi])
    db.commit()
    db.refresh(gof)
    db.refresh(pdi)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=gof.id,
                match_type="prn_table",
                confidence=0.84,
            ),
            ArticleTicker(
                article_id=article.id,
                ticker_id=pdi.id,
                match_type="prn_table",
                confidence=0.84,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-partial",
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
          <p>Guggenheim Strategic Opportunities Fund monthly update.</p>
          <table><tr><td>GOF</td></tr></table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=False,
    )

    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["processed"] == 1
    assert result["articles_with_hits"] == 1
    assert result["remapped_articles"] == 0
    assert {row.ticker_id for row in ticker_rows} == {gof.id, pdi.id}


def test_remap_source_articles_uses_verified_fallback_after_low_confidence_tokens(
    db_session, monkeypatch
):
    db = db_session
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
          <p>Calamos Global Total Return Fund distribution notice.</p>
          <table><tr><td>CGO</td></tr></table>
        </body></html>
        """

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

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


def test_remap_source_articles_ignores_other_source_verification_but_stays_non_destructive(
    db_session,
):
    db = db_session
    prn_source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    yahoo_source = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/mixed-source-article"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbol in text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="GOF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add(
        ArticleTicker(
            article_id=article.id,
            ticker_id=ticker.id,
            match_type="context",
            confidence=0.93,
        )
    )
    db.add_all(
        [
            RawFeedItem(
                source_id=prn_source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-mixed",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=yahoo_source.id,
                article_id=article.id,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF",
                raw_guid="y-guid-remap-mixed",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    result = remap_source_articles(
        db,
        Settings(request_timeout_seconds=5),
        source_code="prnewswire",
        limit=10,
        only_unmapped=False,
    )

    article_after = db.scalar(select(Article).where(Article.id == article.id))
    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["processed"] == 1
    assert result["articles_with_hits"] == 0
    assert result["remapped_articles"] == 0
    assert article_after is not None
    assert len(ticker_rows) == 1
    assert ticker_rows[0].ticker_id == ticker.id


def test_remap_source_articles_only_adds_from_requested_source_when_unmapped(
    db_session,
):
    db = db_session
    prn_source = _seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    yahoo_source = _seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/mixed-source-unmapped-article"
    article = _seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbol in text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="GOF", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add_all(
        [
            RawFeedItem(
                source_id=prn_source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-mixed-unmapped",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=yahoo_source.id,
                article_id=article.id,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF",
                raw_guid="y-guid-remap-mixed-unmapped",
                raw_link=article_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

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
    assert result["articles_with_hits"] == 0
    assert result["remapped_articles"] == 0
    assert ticker_rows == []


def test_purge_token_only_articles_limit_applies_to_distinct_articles(
    db_session, monkeypatch
):
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args: ""
    )

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


def test_purge_token_only_articles_pages_past_recent_valid_rows(
    db_session, monkeypatch
):
    db = db_session
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
            return "<html><body><p>Calamos Global Total Return Fund</p><table><tr><td>CGO</td></tr></table></body></html>"
        if url == mid_valid.canonical_url:
            return "<html><body><p>Franklin Universal Trust</p><table><tr><td>FT</td></tr></table></body></html>"
        return ""

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", fake_fetch)

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


def test_purge_token_only_articles_dry_run_false_detaches_raw_rows(
    db_session, monkeypatch
):
    """Verify the real purge path removes the article and detaches raw rows."""
    db = db_session
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

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args: ""
    )

    result = purge_token_only_articles(
        db,
        dry_run=False,
        limit=10,
        timeout_seconds=5,
    )

    assert result["purged_articles"] == 1
    assert result["deleted_article_tickers"] == 1
    assert result["deleted_raw_feed_items"] == 1

    assert db.scalar(select(Article).where(Article.id == article.id)) is None
    assert (
        db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id == article.id)
        ).all()
        == []
    )
    raw_rows = db.scalars(
        select(RawFeedItem).where(RawFeedItem.raw_guid == "prn-guid-real-delete")
    ).all()
    assert len(raw_rows) == 1
    assert raw_rows[0].article_id is None


def test_dedupe_articles_by_title_merges_duplicates(db_session):
    """Basic coverage: two non-BW articles with the same normalized title get merged."""
    db = db_session
    prn_source = _seed_source(
        db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com"
    )
    gnw_source = _seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )

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
            ArticleTicker(
                article_id=article_prn.id,
                ticker_id=ticker.id,
                match_type="exchange",
                confidence=0.88,
            ),
            ArticleTicker(
                article_id=article_gnw.id,
                ticker_id=ticker.id,
                match_type="paren",
                confidence=0.75,
            ),
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



def test_upsert_article_tickers_force_update_lowers_confidence(db_session):
    db = db_session
    article = _seed_article(
        db,
        canonical_url="https://example.com/revalidate-upsert",
        title="Article",
        summary="",
        published_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="CGO", fund_name="Calamos Global Total Return Fund", sponsor="Calamos", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    row = ArticleTicker(
        article_id=article.id,
        ticker_id=ticker.id,
        match_type="exchange",
        confidence=0.88,
        extraction_version=1,
    )
    db.add(row)
    db.commit()

    _upsert_article_tickers(
        db,
        article.id,
        {"CGO": ("token", 0.62)},
        {"CGO": ticker.id},
        force_update=True,
    )
    db.commit()
    db.refresh(row)

    assert row.match_type == "token"
    assert row.confidence == 0.62
    assert row.extraction_version == EXTRACTION_VERSION


def test_upsert_stamps_extraction_version_on_all_rows(db_session):
    db = db_session
    article = _seed_article(
        db,
        canonical_url="https://example.com/revalidate-stamp",
        title="Article",
        summary="",
        published_at=datetime(2026, 3, 11, 1, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="FFA", fund_name="First Trust Enhanced Equity Income", sponsor="First Trust Advisors L.P.", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    row = ArticleTicker(
        article_id=article.id,
        ticker_id=ticker.id,
        match_type="validated_token",
        confidence=0.68,
        extraction_version=1,
    )
    db.add(row)
    db.commit()

    _upsert_article_tickers(
        db,
        article.id,
        {"FFA": ("validated_token", 0.68)},
        {"FFA": ticker.id},
        force_update=True,
    )
    db.commit()
    db.refresh(row)

    assert row.extraction_version == EXTRACTION_VERSION


def test_revalidation_processes_stale_rows(db_session, monkeypatch):
    db = db_session
    source = _seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/first-trust-302701173.html",
        title="First Trust declares quarterly distribution for FFA",
        summary="",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(
        symbol="FFA",
        fund_name="First Trust Enhanced Equity Income",
        sponsor="First Trust Advisors L.P.",
        validation_keywords="first trust",
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
                extraction_version=1,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-revalidation",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: None)

    result = revalidate_stale_article_tickers(db, limit=10, timeout_seconds=5)
    row = db.scalar(select(ArticleTicker).where(ArticleTicker.article_id == article.id))

    assert result["scanned"] == 1
    assert result["revalidated"] == 1
    assert result["purged"] == 0
    assert row is not None
    assert row.match_type == "validated_token"
    assert row.confidence == 0.68
    assert row.extraction_version == EXTRACTION_VERSION


def test_revalidation_updates_version_after_processing(db_session, monkeypatch):
    db = db_session
    source = _seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    published = datetime(2026, 3, 10, 20, 10, tzinfo=timezone.utc)
    article = _seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
        title="Calamos Global Total Return Fund CGO declares distribution",
        summary="",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = Ticker(symbol="CGO", fund_name="Calamos Global Total Return Fund", sponsor="Calamos", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)
    db.add_all(
        [
            ArticleTicker(
                article_id=article.id,
                ticker_id=ticker.id,
                match_type="validated_token",
                confidence=0.68,
                extraction_version=1,
            ),
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-revalidation-version",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: None)

    result = revalidate_stale_article_tickers(db, limit=10, timeout_seconds=5)
    row = db.scalar(select(ArticleTicker).where(ArticleTicker.article_id == article.id))

    assert result["unchanged"] == 1
    assert row is not None
    assert row.extraction_version == EXTRACTION_VERSION


def test_revalidation_respects_limit(db_session, monkeypatch):
    db = db_session
    source = _seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    ticker = Ticker(
        symbol="FFA",
        fund_name="First Trust Enhanced Equity Income",
        sponsor="First Trust Advisors L.P.",
        validation_keywords="first trust",
        active=True,
    )
    db.add(ticker)
    db.commit()
    db.refresh(ticker)

    for idx in range(3):
        published = datetime(2026, 3, 10, 21, idx, tzinfo=timezone.utc)
        article = _seed_article(
            db,
            canonical_url=f"https://example.com/revalidation-limit/{idx}",
            title=f"First Trust update {idx} for FFA",
            summary="",
            published_at=published,
            source_name="PR Newswire",
            provider_name="PR Newswire",
        )
        db.add(
            ArticleTicker(
                article_id=article.id,
                ticker_id=ticker.id,
                match_type="token",
                confidence=0.62,
                extraction_version=1,
            )
        )
        db.add(
            RawFeedItem(
                source_id=source.id,
                article_id=article.id,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid=f"prn-guid-revalidation-limit-{idx}",
                raw_link=article.canonical_url,
                raw_pub_date=published,
                raw_payload_json={},
            )
        )
        db.commit()

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: None)

    result = revalidate_stale_article_tickers(db, limit=2, timeout_seconds=5)
    stale_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.extraction_version < EXTRACTION_VERSION)
    ).all()

    assert result["scanned"] == 2
    assert len(stale_rows) == 1
