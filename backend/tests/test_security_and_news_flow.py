from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest
from fastapi import HTTPException
from fastapi.routing import APIRoute
from sqlalchemy import event, func, select
from sqlalchemy.orm import Session

from app.main import (
    app,
    count_news,
    EPOCH_UTC,
    _first_seen_at_for_response,
    _published_at_for_response,
    get_news_item,
    health,
    list_news,
    list_news_ids,
    mark_news_alerts_sent,
    require_admin_api_key,
    settings,
)
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.schemas import MarkAlertsSentRequest
from app.ticker_loader import load_tickers_from_csv
from app.utils import sha256_str


def _seed_article(
    db: Session,
    *,
    slug: str,
    published_at: datetime,
    created_at: datetime | None = None,
    canonical_url: str | None = None,
    title: str | None = None,
) -> Article:
    url = canonical_url or f"https://example.com/{slug}"
    article = Article(
        canonical_url=url,
        canonical_url_hash=sha256_str(url),
        title=title or f"Title {slug}",
        summary=f"Summary {slug}",
        published_at=published_at,
        source_name="Test Source",
        provider_name="Test Provider",
        content_hash=sha256_str(f"content-{slug}"),
        title_normalized_hash=sha256_str(f"title-{slug}"),
        cluster_key=sha256_str(f"cluster-{slug}"),
        created_at=created_at or published_at,
        updated_at=created_at or published_at,
    )
    db.add(article)
    db.commit()
    db.refresh(article)
    return article


def test_require_admin_api_key_rejects_when_not_configured(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", None)
    with pytest.raises(HTTPException) as exc:
        require_admin_api_key("anything")
    assert exc.value.status_code == 503


def test_require_admin_api_key_rejects_invalid_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", "expected-key")
    with pytest.raises(HTTPException) as exc:
        require_admin_api_key("wrong-key")
    assert exc.value.status_code == 401


def test_require_admin_api_key_accepts_valid_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(settings, "admin_api_key", "expected-key")
    require_admin_api_key("expected-key")


def test_mark_news_alerts_sent_route_requires_admin_api_key():
    route = next(
        route
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.path == f"{settings.api_prefix}/news/alerts/sent"
        and "POST" in route.methods
    )

    dependency_calls = {dependency.call for dependency in route.dependant.dependencies}
    assert require_admin_api_key in dependency_calls


def test_health_returns_degraded_when_db_check_raises(monkeypatch: pytest.MonkeyPatch):
    def _raise() -> bool:
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("app.main.db_health_check", _raise)
    response = health()
    assert response["status"] == "degraded"


def test_count_news_treats_empty_ticker_query_like_default_mapped_only(db_session: Session):
    db = db_session
    ticker = Ticker(symbol="AAA", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)

    mapped_article = _seed_article(
        db,
        slug="mapped",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    unmapped_article = _seed_article(
        db,
        slug="unmapped",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    db.add(ArticleTicker(article_id=mapped_article.id, ticker_id=ticker.id))
    db.commit()

    response = count_news(
        ticker=" , ",
        include_unmapped=False,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        db=db,
    )
    assert response.total == 1
    assert unmapped_article.id != mapped_article.id


def test_list_news_keeps_empty_ticker_input_on_mapped_only_path(db_session: Session):
    db = db_session
    ticker = Ticker(symbol="AAA", active=True)
    db.add(ticker)
    db.commit()
    db.refresh(ticker)

    mapped_article = _seed_article(
        db,
        slug="mapped-empty-ticker",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    _unmapped_article = _seed_article(
        db,
        slug="unmapped-empty-ticker",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    db.add(ArticleTicker(article_id=mapped_article.id, ticker_id=ticker.id))
    db.commit()

    response = list_news(
        ticker=" , ",
        source=None,
        provider=None,
        q=None,
        include_unmapped=True,
        include_unmapped_from_provider="Business Wire",
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )

    assert [item.id for item in response.items] == [mapped_article.id]


def test_inactive_only_ticker_mapping_is_treated_as_unmapped(db_session: Session):
    db = db_session
    active_ticker = Ticker(symbol="GOF", active=True)
    inactive_ticker = Ticker(symbol="AAA", active=False)
    db.add_all([active_ticker, inactive_ticker])
    db.commit()
    db.refresh(active_ticker)
    db.refresh(inactive_ticker)

    active_article = _seed_article(
        db,
        slug="active-mapped",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    inactive_only_article = _seed_article(
        db,
        slug="inactive-only-mapped",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    db.add_all(
        [
            ArticleTicker(article_id=active_article.id, ticker_id=active_ticker.id),
            ArticleTicker(article_id=inactive_only_article.id, ticker_id=inactive_ticker.id),
        ]
    )
    db.commit()

    mapped_count = count_news(
        include_unmapped=False,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        db=db,
    )
    mapped_news = list_news(
        include_unmapped=False,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )
    mapped_ids = list_news_ids(
        include_unmapped=False,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )
    all_news = list_news(
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )

    assert mapped_count.total == 1
    assert [item.id for item in mapped_news.items] == [active_article.id]
    assert mapped_ids.ids == [active_article.id]
    assert [item.id for item in all_news.items] == [active_article.id, inactive_only_article.id]
    assert next(item for item in all_news.items if item.id == inactive_only_article.id).tickers == []


def test_ticker_filter_excludes_inactive_only_mappings(db_session: Session):
    db = db_session
    inactive_ticker = Ticker(symbol="AAA", active=False)
    db.add(inactive_ticker)
    db.commit()
    db.refresh(inactive_ticker)

    article = _seed_article(
        db,
        slug="inactive-filter",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    db.add(ArticleTicker(article_id=article.id, ticker_id=inactive_ticker.id))
    db.commit()

    response = list_news(
        ticker="AAA",
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )

    assert response.items == []


def test_list_news_provider_filter_prefers_canonical_source_over_latest_raw(db_session: Session):
    db = db_session
    businesswire = Source(
        code="businesswire",
        name="Business Wire",
        base_url="https://feed.businesswire.com",
        enabled=True,
    )
    yahoo = Source(
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
        enabled=True,
    )
    db.add_all([businesswire, yahoo])
    db.commit()
    db.refresh(businesswire)
    db.refresh(yahoo)

    canonical_url = "https://www.businesswire.com/news/home/20260301000001/en"
    yahoo_variant_url = canonical_url + "?feedref=abc123"
    article = _seed_article(
        db,
        slug="bw-provider",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        canonical_url=canonical_url,
    )

    db.add(
        RawFeedItem(
            source_id=businesswire.id,
            article_id=article.id,
            feed_url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
            raw_guid="bw-1",
            raw_link=canonical_url,
            raw_pub_date=datetime(2025, 1, 3, tzinfo=timezone.utc),
            raw_payload_json={},
        )
    )
    db.commit()

    # Add a newer mirrored Yahoo raw row that should NOT override canonical provider attribution.
    db.add(
        RawFeedItem(
            source_id=yahoo.id,
            article_id=article.id,
            feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
            raw_guid="y-1",
            raw_link=yahoo_variant_url,
            raw_pub_date=datetime(2025, 1, 3, tzinfo=timezone.utc),
            raw_payload_json={},
        )
    )
    db.commit()

    bw_response = list_news(
        ticker=None,
        source=None,
        provider="Business Wire",
        q=None,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )
    assert [item.id for item in bw_response.items] == [article.id]
    assert bw_response.items[0].provider == "Business Wire"
    assert bw_response.items[0].first_seen_at is not None
    assert bw_response.items[0].alert_sent_at is None

    yahoo_response = list_news(
        ticker=None,
        source=None,
        provider="Yahoo Finance",
        q=None,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )
    assert yahoo_response.items == []


def test_list_news_uses_single_statement_for_enriched_rows(db_session: Session):
    db = db_session
    businesswire = Source(
        code="businesswire",
        name="Business Wire",
        base_url="https://feed.businesswire.com",
        enabled=True,
    )
    yahoo = Source(
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
        enabled=True,
    )
    gof = Ticker(symbol="GOF", active=True)
    utf = Ticker(symbol="UTF", active=True)
    db.add_all([businesswire, yahoo, gof, utf])
    db.commit()
    db.refresh(businesswire)
    db.refresh(yahoo)
    db.refresh(gof)
    db.refresh(utf)

    article = _seed_article(
        db,
        slug="single-statement",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        canonical_url="https://www.businesswire.com/news/home/20260301000001/en",
    )
    db.add_all(
        [
            ArticleTicker(article_id=article.id, ticker_id=gof.id),
            ArticleTicker(article_id=article.id, ticker_id=utf.id),
            RawFeedItem(
                source_id=businesswire.id,
                article_id=article.id,
                feed_url="https://feed.businesswire.com/rss/home/",
                raw_guid="bw-single-1",
                raw_link=article.canonical_url,
                raw_pub_date=article.published_at,
                raw_payload_json={},
            ),
            RawFeedItem(
                source_id=yahoo.id,
                article_id=article.id,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
                raw_guid="y-single-1",
                raw_link=article.canonical_url + "?mirror=1",
                raw_pub_date=article.published_at,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    statements: list[str] = []

    @event.listens_for(db.get_bind(), "before_cursor_execute")
    def capture_sql(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement)

    try:
            response = list_news(
                include_unmapped=True,
                include_unmapped_from_provider=None,
                include_global_summary=False,
                from_=None,
                to=None,
                limit=10,
                cursor=None,
                db=db,
        )
    finally:
        event.remove(db.get_bind(), "before_cursor_execute", capture_sql)

    assert len(statements) == 1
    assert [item.id for item in response.items] == [article.id]
    assert response.items[0].provider == "Business Wire"
    assert response.items[0].tickers == ["GOF", "UTF"]


def test_get_news_item_uses_single_statement_for_enriched_row(db_session: Session):
    db = db_session
    businesswire = Source(
        code="businesswire",
        name="Business Wire",
        base_url="https://feed.businesswire.com",
        enabled=True,
    )
    gof = Ticker(symbol="GOF", active=True)
    db.add_all([businesswire, gof])
    db.commit()
    db.refresh(businesswire)
    db.refresh(gof)

    article = _seed_article(
        db,
        slug="detail-single-statement",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        canonical_url="https://www.businesswire.com/news/home/20260301000002/en",
    )
    db.add_all(
        [
            ArticleTicker(article_id=article.id, ticker_id=gof.id),
            RawFeedItem(
                source_id=businesswire.id,
                article_id=article.id,
                feed_url="https://feed.businesswire.com/rss/home/",
                raw_guid="bw-single-detail",
                raw_link=article.canonical_url,
                raw_pub_date=article.published_at,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()
    article_id = article.id

    statements: list[str] = []

    @event.listens_for(db.get_bind(), "before_cursor_execute")
    def capture_sql(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement)

    try:
        result = get_news_item(article_id, db=db)
    finally:
        event.remove(db.get_bind(), "before_cursor_execute", capture_sql)

    assert len(statements) == 1
    assert result.id == article.id
    assert result.provider == "Business Wire"
    assert result.tickers == ["GOF"]


def test_list_news_with_global_summary_uses_two_statements_and_keeps_global_scope(db_session: Session):
    db = db_session
    businesswire = Source(
        code="businesswire",
        name="Business Wire",
        base_url="https://feed.businesswire.com",
        enabled=True,
    )
    gof = Ticker(symbol="GOF", active=True)
    db.add_all([businesswire, gof])
    db.commit()
    db.refresh(businesswire)
    db.refresh(gof)

    mapped_article = _seed_article(
        db,
        slug="global-summary-mapped",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    bw_general_article = _seed_article(
        db,
        slug="global-summary-general",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    db.add_all(
        [
            ArticleTicker(article_id=mapped_article.id, ticker_id=gof.id),
            RawFeedItem(
                source_id=businesswire.id,
                article_id=bw_general_article.id,
                feed_url="https://feed.businesswire.com/rss/home/",
                raw_guid="bw-global-general",
                raw_link=bw_general_article.canonical_url,
                raw_pub_date=bw_general_article.published_at,
                raw_payload_json={},
            ),
        ]
    )
    db.commit()

    statements: list[str] = []

    @event.listens_for(db.get_bind(), "before_cursor_execute")
    def capture_sql(
        _conn,
        _cursor,
        statement,
        _parameters,
        _context,
        _executemany,
    ):
        statements.append(statement)

    try:
        response = list_news(
            ticker="GOF",
            include_unmapped=False,
            include_unmapped_from_provider=None,
            include_global_summary=True,
            from_=None,
            to=None,
            limit=10,
            cursor=None,
            db=db,
        )
    finally:
        event.remove(db.get_bind(), "before_cursor_execute", capture_sql)

    assert len(statements) == 2
    assert [item.id for item in response.items] == [mapped_article.id]
    assert response.global_summary is not None
    assert response.global_summary.total == 2
    assert response.global_summary.tracked_ids == [mapped_article.id, bw_general_article.id]
    assert response.global_summary.tracked_limit == 100


def test_list_news_cursor_paginates_consistently(db_session: Session):
    db = db_session
    newest = _seed_article(
        db,
        slug="newest",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    middle = _seed_article(
        db,
        slug="middle",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    oldest = _seed_article(
        db,
        slug="oldest",
        published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    page_one = list_news(
        limit=1,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=None,
        db=db,
    )
    assert [item.id for item in page_one.items] == [newest.id]
    assert page_one.next_cursor is not None

    page_two = list_news(
        limit=2,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=page_one.next_cursor,
        db=db,
    )
    assert [item.id for item in page_two.items] == [middle.id, oldest.id]


def test_timestamp_fallback_helpers_return_epoch_for_fully_legacy_rows():
    row = SimpleNamespace(
        published_at=None,
        created_at=None,
        first_seen_at=None,
    )

    assert _published_at_for_response(row) == EPOCH_UTC
    assert _first_seen_at_for_response(row) == EPOCH_UTC


def test_list_news_ids_supports_cursor_pagination(db_session: Session):
    db = db_session
    first = _seed_article(
        db,
        slug="first",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )
    second = _seed_article(
        db,
        slug="second",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
    )
    third = _seed_article(
        db,
        slug="third",
        published_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    page_one = list_news_ids(
        limit=2,
        cursor=None,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        db=db,
    )
    assert page_one.ids == [first.id, second.id]
    assert page_one.next_cursor is not None

    page_two = list_news_ids(
        limit=2,
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        cursor=page_one.next_cursor,
        db=db,
    )
    assert page_two.ids == [third.id]
    assert page_two.next_cursor is None


def test_ticker_loader_accepts_case_insensitive_header(db_session: Session):
    db = db_session
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temp_dir / f"tickers-{uuid.uuid4().hex}.csv"
    csv_path.write_text("Ticker,fund_name,sponsor,active\nAAA,Fund A,Sponsor A,true\n", encoding="utf-8")

    try:
        stats = load_tickers_from_csv(db, str(csv_path))
        ticker = db.scalar(select(Ticker).where(Ticker.symbol == "AAA"))

        assert stats["loaded"] == 1
        assert stats["created"] == 1
        assert ticker is not None
        assert ticker.fund_name == "Fund A"
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_ticker_loader_handles_more_than_sqlite_parameter_limit(db_session: Session):
    db = db_session
    temp_dir = Path("backend/tests/.tmp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    csv_path = temp_dir / f"tickers-bulk-{uuid.uuid4().hex}.csv"
    rows = ["ticker,fund_name,sponsor,active"]
    rows.extend(
        f"T{index:04d},Fund {index},Sponsor {index},true"
        for index in range(1005)
    )
    csv_path.write_text("\n".join(rows) + "\n", encoding="utf-8")

    try:
        stats = load_tickers_from_csv(db, str(csv_path))
        total = db.scalar(select(func.count()).select_from(Ticker)) or 0

        assert stats["loaded"] == 1005
        assert stats["created"] == 1005
        assert total == 1005
    finally:
        if csv_path.exists():
            csv_path.unlink()


def test_mark_news_alerts_sent_sets_first_timestamp_once(db_session: Session):
    db = db_session
    article = _seed_article(
        db,
        slug="alert-mark",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
    )

    first = mark_news_alerts_sent(
        MarkAlertsSentRequest(article_ids=[article.id, article.id, 999999]),
        db=db,
    )
    row = db.scalar(select(Article).where(Article.id == article.id))
    assert row is not None
    assert first.requested == 2
    assert first.marked == 1
    assert row.first_alert_sent_at is not None

    second = mark_news_alerts_sent(
        MarkAlertsSentRequest(article_ids=[article.id]),
        db=db,
    )
    assert second.requested == 1
    assert second.marked == 0


def test_mark_news_alerts_sent_chunks_large_updates(db_session: Session):
    db = db_session
    published_at = datetime(2025, 1, 3, tzinfo=timezone.utc)
    articles: list[Article] = []
    for index in range(1005):
        url = f"https://example.com/bulk-alert-{index}"
        articles.append(
            Article(
                canonical_url=url,
                canonical_url_hash=sha256_str(url),
                title=f"Bulk Alert {index}",
                summary=f"Summary {index}",
                published_at=published_at,
                source_name="Test Source",
                provider_name="Test Provider",
                content_hash=sha256_str(f"bulk-content-{index}"),
                title_normalized_hash=sha256_str(f"bulk-title-{index}"),
                cluster_key=sha256_str(f"bulk-cluster-{index}"),
                created_at=published_at,
                updated_at=published_at,
            )
        )
    db.add_all(articles)
    db.commit()

    response = mark_news_alerts_sent(
        MarkAlertsSentRequest(article_ids=[article.id for article in articles]),
        db=db,
    )
    marked_total = db.scalar(
        select(func.count())
        .select_from(Article)
        .where(Article.first_alert_sent_at.is_not(None))
    ) or 0

    assert response.requested == 1005
    assert response.marked == 1005
    assert marked_total == 1005


def test_list_news_search_treats_wildcard_chars_as_literals(db_session: Session):
    db = db_session
    literal = _seed_article(
        db,
        slug="literal-wildcard",
        published_at=datetime(2025, 1, 3, tzinfo=timezone.utc),
        title="Yield 100%_covered",
    )
    _other = _seed_article(
        db,
        slug="plain-text",
        published_at=datetime(2025, 1, 2, tzinfo=timezone.utc),
        title="Yield 100 basis points covered",
    )

    response = list_news(
        ticker=None,
        source=None,
        provider=None,
        q="%_",
        include_unmapped=True,
        include_unmapped_from_provider=None,
        from_=None,
        to=None,
        limit=10,
        cursor=None,
        db=db,
    )

    assert [item.id for item in response.items] == [literal.id]
