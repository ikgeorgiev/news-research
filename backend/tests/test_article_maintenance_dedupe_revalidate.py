from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select

from app.article_maintenance import (
    _upsert_article_tickers,
    dedupe_articles_by_title,
    dedupe_businesswire_url_variants,
    revalidate_stale_article_tickers,
)
from app.constants import EXTRACTION_VERSION
from app.models import Article, ArticleTicker, RawFeedItem
from tests.helpers import (
    build_article_ticker,
    build_raw_feed_item,
    seed_article,
    seed_article_with_raw,
    seed_source,
    seed_ticker,
)


def test_dedupe_businesswire_url_variants_merges_historical_rows(db_session):
    db = db_session
    yahoo = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    businesswire = seed_source(db)
    published = datetime(2026, 3, 1, tzinfo=timezone.utc)
    query_url = (
        "https://www.businesswire.com/news/home/20260301000001/en"
        "?feedref=JjAwJuNHiystnCoBq_hl-XxV8f8yqXw8M0Q"
    )
    clean_url = "https://www.businesswire.com/news/home/20260301000001/en"
    yahoo_article = seed_article(
        db,
        canonical_url=query_url,
        title="ACME distribution update",
        summary="Short Yahoo summary",
        published_at=published,
        source_name="Yahoo Finance",
        provider_name="Yahoo Finance",
    )
    businesswire_article = seed_article(
        db,
        canonical_url=clean_url,
        title="ACME distribution update",
        summary="Business Wire summary has more context than Yahoo copy",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    ticker = seed_ticker(db, symbol="UTF")
    db.add_all(
        [
            build_article_ticker(
                article=yahoo_article,
                ticker=ticker,
                match_type="token",
                confidence=0.62,
            ),
            build_article_ticker(
                article=businesswire_article,
                ticker=ticker,
                match_type="exchange",
                confidence=0.88,
            ),
            build_raw_feed_item(
                source=yahoo,
                article=yahoo_article,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=UTF",
                raw_guid="y-guid-1",
                raw_link=query_url,
                raw_pub_date=published,
            ),
            build_raw_feed_item(
                source=businesswire,
                article=businesswire_article,
                feed_url="https://feed.businesswire.com/rss/home/?rss=G1QFDERJXkJeGVtYXg==",
                raw_guid="bw-guid-1",
                raw_link=clean_url,
                raw_pub_date=published,
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
    seed_article(
        db,
        canonical_url="https://www.businesswire.com/news/home/20260301000001/en",
        title="Fund update",
        summary="Story one",
        published_at=published,
        source_name="Business Wire",
        provider_name="Business Wire",
    )
    seed_article(
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


def test_dedupe_articles_by_title_merges_duplicates(db_session):
    db = db_session
    prn_source = seed_source(
        db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com"
    )
    gnw_source = seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    published = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    title = "Acme Corp Declares Quarterly Distribution"
    article_prn = seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/acme-distribution.html",
        title=title,
        summary="Short PRN summary",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    article_gnw = seed_article(
        db,
        canonical_url="https://www.globenewswire.com/news-release/acme-distribution",
        title=title,
        summary="Longer GlobeNewswire summary with more detail",
        published_at=published + timedelta(minutes=5),
        source_name="GlobeNewswire",
        provider_name="GlobeNewswire",
    )
    ticker = seed_ticker(db, symbol="ACME", fund_name="Acme Fund")
    db.add_all(
        [
            build_article_ticker(
                article=article_prn,
                ticker=ticker,
                match_type="exchange",
                confidence=0.88,
            ),
            build_article_ticker(
                article=article_gnw,
                ticker=ticker,
                match_type="paren",
                confidence=0.75,
            ),
            build_raw_feed_item(
                source=prn_source,
                article=article_prn,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-dedupe-1",
                raw_link=article_prn.canonical_url,
                raw_pub_date=published,
            ),
            build_raw_feed_item(
                source=gnw_source,
                article=article_gnw,
                feed_url="https://rss.globenewswire.com/en/RssFeed/subjectcode/12",
                raw_guid="gnw-dedupe-1",
                raw_link=article_gnw.canonical_url,
                raw_pub_date=published + timedelta(minutes=5),
            ),
        ]
    )
    db.commit()

    result = dedupe_articles_by_title(db, window_hours=48)

    assert result["duplicate_groups"] >= 1
    assert result["merged_articles"] == 1
    winner = db.scalar(select(Article).where(Article.id == article_gnw.id))
    loser = db.scalar(select(Article).where(Article.id == article_prn.id))
    assert winner is not None
    assert loser is None
    assert winner.summary == "Longer GlobeNewswire summary with more detail"
    remaining_tickers = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == winner.id)
    ).all()
    assert len(remaining_tickers) >= 1
    raw_items = db.scalars(
        select(RawFeedItem).where(RawFeedItem.article_id == winner.id)
    ).all()
    assert len(raw_items) == 2


def test_upsert_article_tickers_force_update_lowers_confidence(db_session):
    db = db_session
    article = seed_article(
        db,
        canonical_url="https://example.com/revalidate-upsert",
        title="Article",
        published_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    row = build_article_ticker(
        article=article,
        ticker=ticker,
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
    article = seed_article(
        db,
        canonical_url="https://example.com/revalidate-stamp",
        title="Article",
        published_at=datetime(2026, 3, 11, 1, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(
        db,
        symbol="FFA",
        fund_name="First Trust Enhanced Equity Income",
        sponsor="First Trust Advisors L.P.",
    )
    row = build_article_ticker(
        article=article,
        ticker=ticker,
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
    source = seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    article, _ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="FFA",
            fund_name="First Trust Enhanced Equity Income",
            sponsor="First Trust Advisors L.P.",
            validation_keywords="first trust",
        ),
        article_kwargs=dict(
            canonical_url="https://www.prnewswire.com/news-releases/first-trust-302701173.html",
            title="First Trust declares quarterly distribution for FFA",
            published_at=datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="token",
        confidence=0.62,
        raw_guid="prn-guid-revalidation",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        extraction_version=1,
    )

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
    source = seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    article, _ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
            title="Calamos Global Total Return Fund CGO declares distribution",
            published_at=datetime(2026, 3, 10, 20, 10, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="validated_token",
        confidence=0.68,
        raw_guid="prn-guid-revalidation-version",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        extraction_version=1,
    )

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: None)

    result = revalidate_stale_article_tickers(db, limit=10, timeout_seconds=5)
    row = db.scalar(select(ArticleTicker).where(ArticleTicker.article_id == article.id))

    assert result["unchanged"] == 1
    assert row is not None
    assert row.extraction_version == EXTRACTION_VERSION


def test_revalidation_respects_limit(db_session, monkeypatch):
    db = db_session
    source = seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    ticker = seed_ticker(
        db,
        symbol="FFA",
        fund_name="First Trust Enhanced Equity Income",
        sponsor="First Trust Advisors L.P.",
        validation_keywords="first trust",
    )

    for idx in range(3):
        published = datetime(2026, 3, 10, 21, idx, tzinfo=timezone.utc)
        article = seed_article(
            db,
            canonical_url=f"https://example.com/revalidation-limit/{idx}",
            title=f"First Trust update {idx} for FFA",
            published_at=published,
            source_name="PR Newswire",
            provider_name="PR Newswire",
        )
        db.add_all(
            [
                build_article_ticker(
                    article=article,
                    ticker=ticker,
                    match_type="token",
                    confidence=0.62,
                    extraction_version=1,
                ),
                build_raw_feed_item(
                    source=source,
                    article=article,
                    feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                    raw_guid=f"prn-guid-revalidation-limit-{idx}",
                    raw_link=article.canonical_url,
                    raw_pub_date=published,
                ),
            ]
        )
        db.commit()

    monkeypatch.setattr("app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: None)

    result = revalidate_stale_article_tickers(db, limit=2, timeout_seconds=5)
    stale_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.extraction_version < EXTRACTION_VERSION)
    ).all()

    assert result["scanned"] == 2
    assert len(stale_rows) == 1


def test_revalidation_uses_globenewswire_specific_page_timeout(db_session, monkeypatch):
    db = db_session
    source = seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    article, _ticker = seed_article_with_raw(
        db,
        source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url="https://www.globenewswire.com/news-release/2026/03/example",
            title="Acme announces quarterly distribution",
            published_at=datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc),
            source_name="GlobeNewswire",
            provider_name="GlobeNewswire",
        ),
        match_type="token",
        confidence=0.62,
        raw_guid="gn-guid-revalidation-timeout",
        feed_url="https://www.globenewswire.com/RssFeed/orgclass/1/feedTitle/GlobeNewswire%20-%20News%20about%20Public%20Companies",
        extraction_version=1,
    )

    seen_timeouts: list[int] = []

    def fake_fallback(
        _title,
        _summary,
        _link,
        _feed_url,
        _known_symbols,
        timeout_seconds,
        _config,
        *,
        symbol_keywords=None,
    ):
        seen_timeouts.append(timeout_seconds)
        return {}

    monkeypatch.setattr(
        "app.article_maintenance._common._extract_source_fallback_tickers",
        fake_fallback,
    )

    result = revalidate_stale_article_tickers(
        db,
        limit=10,
        timeout_seconds=20,
        globenewswire_source_page_timeout_seconds=5,
    )
    row = db.scalar(select(ArticleTicker).where(ArticleTicker.article_id == article.id))

    assert result["scanned"] == 1
    assert result["unchanged"] == 1
    assert row is not None
    assert row.extraction_version == 1
    assert seen_timeouts == [5]
