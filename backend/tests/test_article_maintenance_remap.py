from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import select

from app.article_maintenance import remap_source_articles
from app.config import Settings
from app.models import Article, ArticleTicker, RawFeedItem
from tests.helpers import (
    build_article_ticker,
    build_raw_feed_item,
    seed_article,
    seed_article_with_raw,
    seed_source,
    seed_ticker,
)


def _remap_settings() -> Settings:
    return Settings(request_timeout_seconds=5)


def test_remap_source_articles_unmapped_miss_is_non_destructive(
    db_session, monkeypatch
):
    db = db_session
    source = seed_source(
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
    seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    article = db.scalar(select(Article).where(Article.canonical_url == article_url))
    assert article is not None
    db.add(
        build_raw_feed_item(
            source=source,
            article=article,
            feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
            raw_guid="prn-guid-remap-1",
            raw_link=article_url,
            raw_pub_date=published,
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
        _remap_settings(),
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/legacy-remap-miss"
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url=article_url,
            title="Envestnet Accelerates Adaptive WealthTech Innovation",
            summary="No explicit fund keywords remain in stored text.",
            published_at=published,
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="prn_table",
        confidence=0.84,
        raw_guid="prn-guid-remap-miss",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
    )

    monkeypatch.setattr(
        "app.ticker_extraction._fetch_source_page_html", lambda *_args, **_kwargs: ""
    )

    result = remap_source_articles(
        db,
        _remap_settings(),
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/legacy-remap-partial"
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbols remain in stored text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    gof = seed_ticker(
        db,
        symbol="GOF",
        fund_name="Guggenheim Strategic Opportunities Fund",
        sponsor="Guggenheim",
    )
    pdi = seed_ticker(
        db,
        symbol="PDI",
        fund_name="PIMCO Dynamic Income Fund",
        sponsor="PIMCO",
    )
    db.add_all(
        [
            build_article_ticker(
                article=article,
                ticker=gof,
                match_type="prn_table",
                confidence=0.84,
            ),
            build_article_ticker(
                article=article,
                ticker=pdi,
                match_type="prn_table",
                confidence=0.84,
            ),
            build_raw_feed_item(
                source=source,
                article=article,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-partial",
                raw_link=article_url,
                raw_pub_date=published,
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
        _remap_settings(),
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


def test_remap_source_articles_full_remap_updates_already_mapped_article_when_requested(
    db_session, monkeypatch
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = (
        "https://www.prnewswire.com/news-releases/"
        "calamos-distribution-update-301000003.html"
    )
    article, old_ticker = seed_article_with_raw(
        db,
        source,
        ticker_kwargs=dict(
            symbol="PDI",
            fund_name="PIMCO Dynamic Income Fund",
            sponsor="PIMCO",
        ),
        article_kwargs=dict(
            canonical_url=article_url,
            title="Distribution update without explicit ticker text",
            summary="Stored copy without a ticker symbol.",
            published_at=published,
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="context",
        confidence=0.91,
        raw_guid="prn-guid-remap-existing",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
    )
    new_ticker = seed_ticker(
        db,
        symbol="GOF",
        fund_name="Guggenheim Strategic Opportunities Fund",
        sponsor="Guggenheim",
    )

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
        _remap_settings(),
        source_code="prnewswire",
        limit=10,
        only_unmapped=False,
    )

    ticker_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id == article.id)
    ).all()

    assert result["processed"] == 1
    assert result["articles_with_hits"] == 1
    assert result["remapped_articles"] == 1
    assert {row.ticker_id for row in ticker_rows} == {old_ticker.id, new_ticker.id}


def test_remap_source_articles_uses_verified_fallback_after_low_confidence_tokens(
    db_session, monkeypatch
):
    db = db_session
    source = seed_source(
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
    seed_article(
        db,
        canonical_url=article_url,
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    article = db.scalar(select(Article).where(Article.canonical_url == article_url))
    assert article is not None
    db.add(
        build_raw_feed_item(
            source=source,
            article=article,
            feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
            raw_guid="prn-guid-remap-2",
            raw_link=article_url,
            raw_pub_date=published,
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
        _remap_settings(),
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
    prn_source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    yahoo_source = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/mixed-source-article"
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbol in text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(db, symbol="GOF")
    db.add_all(
        [
            build_article_ticker(
                article=article,
                ticker=ticker,
                match_type="context",
                confidence=0.93,
            ),
            build_raw_feed_item(
                source=prn_source,
                article=article,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-mixed",
                raw_link=article_url,
                raw_pub_date=published,
            ),
            build_raw_feed_item(
                source=yahoo_source,
                article=article,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF",
                raw_guid="y-guid-remap-mixed",
                raw_link=article_url,
                raw_pub_date=published,
            ),
        ]
    )
    db.commit()

    result = remap_source_articles(
        db,
        _remap_settings(),
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
    prn_source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    yahoo_source = seed_source(
        db,
        code="yahoo",
        name="Yahoo Finance",
        base_url="https://feeds.finance.yahoo.com",
    )
    published = datetime(2026, 3, 2, tzinfo=timezone.utc)
    article_url = "https://example.com/mixed-source-unmapped-article"
    article = seed_article(
        db,
        canonical_url=article_url,
        title="Monthly portfolio commentary",
        summary="No explicit symbol in text.",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    seed_ticker(db, symbol="GOF")
    db.add_all(
        [
            build_raw_feed_item(
                source=prn_source,
                article=article,
                feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
                raw_guid="prn-guid-remap-mixed-unmapped",
                raw_link=article_url,
                raw_pub_date=published,
            ),
            build_raw_feed_item(
                source=yahoo_source,
                article=article,
                feed_url="https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF",
                raw_guid="y-guid-remap-mixed-unmapped",
                raw_link=article_url,
                raw_pub_date=published,
            ),
        ]
    )
    db.commit()

    result = remap_source_articles(
        db,
        _remap_settings(),
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
