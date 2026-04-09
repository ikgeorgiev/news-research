from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select

from app.article_maintenance import (
    purge_token_only_articles,
)
from app.constants import EXTRACTION_VERSION
from app.models import (
    Article,
    ArticleTicker,
    RawFeedItem,
)
from tests.helpers import (
    seed_article,
    seed_article_with_raw,
    seed_source,
    seed_ticker,
)

def test_purge_token_only_articles_does_not_trust_table_only_fallback(
    db_session,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    article_url = (
        "https://www.prnewswire.com/news-releases/"
        "envestnet-accelerates-adaptive-wealthtech-innovation-301000000.html"
    )
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
            summary="New CGO appointment announced",
            published_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="token",
        confidence=0.62,
        raw_guid="prn-guid-1",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
    )

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
    article = seed_article(
        db,
        canonical_url="https://example.com/legacy-pmo-story",
        title="Mace Consult Launches as Standalone PMO Company",
        published_at=published,
        source_name="Legacy Mirror",
        provider_name="Legacy Mirror",
    )
    ticker = seed_ticker(
        db,
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
    )
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

@pytest.mark.parametrize(
    "source_kwargs, article_kwargs, ticker_kwargs, match_type, confidence, raw_guid, feed_url",
    [
        pytest.param(
            dict(code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com"),
            dict(
                canonical_url="https://example.com/high-confidence-prn",
                title="NYSE: CGO distribution update",
                published_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
                source_name="PR Newswire",
                provider_name="PR Newswire",
            ),
            dict(symbol="CGO", fund_name="Calamos Global Total Return Fund", sponsor="Calamos"),
            "exchange",
            0.88,
            "prn-guid-high-confidence",
            "https://www.prnewswire.com/rss/news-releases-list.rss",
            id="exchange_verified",
        ),
        pytest.param(
            dict(code="globenewswire", name="GlobeNewswire", base_url="https://rss.globenewswire.com"),
            dict(
                canonical_url=(
                    "https://www.globenewswire.com/en/news-release/2026/03/10/3253311/0/en/"
                    "Artiva-Biotherapeutics-Reports-Full-Year-2025-Financial-Results-and-Recent-Business-Highlights.html"
                ),
                title="Distribution update",
                published_at=datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc),
                source_name="GlobeNewswire",
                provider_name="GlobeNewswire",
            ),
            dict(
                symbol="RA",
                fund_name="Brookfield Real Assets Income Fund Inc.",
                sponsor="Brookfield Public Securities Group LLC",
            ),
            "paren",
            0.75,
            "gnw-guid-page-derived-paren",
            (
                "https://rss.globenewswire.com/en/RssFeed/subjectcode/"
                "13-Earnings%20Releases%20And%20Operating%20Results/feedTitle/"
                "Earnings%20Releases%20And%20Operating%20Results"
            ),
            id="page_derived_paren",
        ),
        pytest.param(
            dict(code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com"),
            dict(
                canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
                title="Distribution update",
                published_at=datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc),
                source_name="PR Newswire",
                provider_name="PR Newswire",
            ),
            dict(symbol="CGO", fund_name="Calamos Global Total Return Fund", sponsor="Calamos"),
            "paren",
            0.75,
            "prn-guid-page-derived-paren",
            "https://www.prnewswire.com/rss/news-releases-list.rss",
            id="page_derived_paren",
        ),
    ],
)
def test_purge_token_only_articles_skips_high_confidence_current_version_rows(
    db_session,
    monkeypatch,
    source_kwargs,
    article_kwargs,
    ticker_kwargs,
    match_type,
    confidence,
    raw_guid,
    feed_url,
):
    db = db_session
    source = seed_source(db, **source_kwargs)
    seed_article_with_raw(
        db, source,
        ticker_kwargs=ticker_kwargs,
        article_kwargs=article_kwargs,
        match_type=match_type,
        confidence=confidence,
        raw_guid=raw_guid,
        feed_url=feed_url,
        extraction_version=EXTRACTION_VERSION,
    )

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

    assert result["scanned_articles"] == 0
    assert result["purged_articles"] == 0
    assert result["deleted_article_tickers"] == 0
    assert result["deleted_raw_feed_items"] == 0

def test_purge_token_only_articles_prunes_token_rows_when_verified_rows_already_exist(
    db_session,
    monkeypatch,
):
    db = db_session
    source = seed_source(
        db,
        code="globenewswire",
        name="GlobeNewswire",
        base_url="https://rss.globenewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = seed_article(
        db,
        canonical_url=(
            "https://www.globenewswire.com/en/news-release/2026/03/10/3253311/0/en/"
            "Artiva-Biotherapeutics-Reports-Full-Year-2025-Financial-Results-and-Recent-Business-Highlights.html"
        ),
        title="Distribution update",
        published_at=published,
        source_name="GlobeNewswire",
        provider_name="GlobeNewswire",
    )
    verified_ticker = seed_ticker(
        db,
        symbol="RA",
        fund_name="Brookfield Real Assets Income Fund Inc.",
        sponsor="Brookfield Public Securities Group LLC",
    )
    token_ticker = seed_ticker(
        db,
        symbol="ARTV",
        fund_name="Artiva Biotherapeutics, Inc.",
        sponsor="Artiva",
    )
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published = datetime(2026, 3, 10, 20, 5, tzinfo=timezone.utc)
    article = seed_article(
        db,
        canonical_url="https://www.prnewswire.com/news-releases/calamos-302701173.html",
        title="Calamos Global Total Return Fund CGO declares monthly distribution",
        summary="Legacy PMO mention should be removed",
        published_at=published,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    verified_ticker = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    stale_ticker = seed_ticker(
        db,
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
    )
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    title = "EcoVadis and Watershed partner to close the Scope 3 data gap"
    summary = (
        "Combined with the launch of EcoVadis PCF Calculator, the partnership "
        "with Watershed is a cornerstone of EcoVadis' mission."
    )
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="PCF",
            fund_name="High Income Securities",
            sponsor="Bulldog Investors LLP",
        ),
        article_kwargs=dict(
            canonical_url=(
                "https://www.prnewswire.com/news-releases/"
                "ecovadis-and-watershed-partner-to-close-the-scope-3-data-gap-302712475.html"
            ),
            title=title,
            summary=summary,
            published_at=datetime(2026, 3, 12, 14, 50, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="paren",
        confidence=0.70,
        raw_guid="prn-guid-stale-high-confidence-pcf",
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
        extraction_version=EXTRACTION_VERSION - 1,
        raw_title=title,
        raw_payload_json={"summary": summary, "title": title},
    )

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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    title = "Calamos Global Total Return Fund CGO declares monthly distribution"
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url="https://www.prnewswire.com/news-releases/calamos-302712476.html",
            title=title,
            published_at=datetime(2026, 3, 12, 15, 10, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="validated_token",
        confidence=0.68,
        raw_guid="prn-guid-stale-unchanged-cgo",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
        extraction_version=EXTRACTION_VERSION - 1,
        raw_title=title,
        raw_payload_json={"summary": "", "title": title},
    )

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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    title = "With Intelligence Honors Thornburg with Two 2026 Mutual Fund & ETF Awards"
    summary = "Thornburg takes top accolades in multi-asset mutual fund of the year."
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="TBLD",
            fund_name="Thornburg Income Builder Opp Trust",
            sponsor="Thornburg Investment Management Inc",
        ),
        article_kwargs=dict(
            canonical_url="https://www.prnewswire.com/news-releases/thornburg-awards-302711484.html",
            title=title,
            summary=summary,
            published_at=datetime(2026, 3, 11, 21, 0, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="exchange",
        confidence=0.88,
        raw_guid="prn-guid-tbld-stale-page-miss",
        feed_url="https://www.prnewswire.com/rss/financial-services-latest-news/financial-services-latest-news-list.rss",
        extraction_version=EXTRACTION_VERSION - 1,
        raw_title=title,
        raw_payload_json={"title": title, "summary": summary},
    )

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

def test_purge_token_only_articles_limit_applies_to_distinct_articles(
    db_session, monkeypatch
):
    db = db_session
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published_recent = datetime(2026, 3, 3, tzinfo=timezone.utc)
    published_older = datetime(2026, 3, 2, tzinfo=timezone.utc)

    cgo = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    pmo = seed_ticker(
        db,
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
    )
    ft = seed_ticker(
        db,
        symbol="FT",
        fund_name="Franklin Universal Trust",
        sponsor="Franklin",
    )

    recent_article = seed_article(
        db,
        canonical_url="https://example.com/recent-false-positive",
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published_recent,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    older_article = seed_article(
        db,
        canonical_url="https://example.com/older-false-positive",
        title="Mace Consult Launches as Standalone PMO Company",
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    published_recent = datetime(2026, 3, 4, tzinfo=timezone.utc)
    published_mid = datetime(2026, 3, 3, tzinfo=timezone.utc)
    published_older = datetime(2026, 3, 2, tzinfo=timezone.utc)

    cgo = seed_ticker(
        db,
        symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
    ft = seed_ticker(
        db,
        symbol="FT",
        fund_name="Franklin Universal Trust",
        sponsor="Franklin",
    )
    pmo = seed_ticker(
        db,
        symbol="PMO",
        fund_name="Putnam Municipal Opportunities Trust",
        sponsor="Putnam",
    )

    recent_valid = seed_article(
        db,
        canonical_url="https://example.com/recent-valid-cgo",
        title="Envestnet Accelerates Adaptive WealthTech Innovation",
        summary="New CGO appointment announced",
        published_at=published_recent,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    mid_valid = seed_article(
        db,
        canonical_url="https://example.com/mid-valid-ft",
        title="Sokin Appoints Former FT Partners VP Tom Steer as CFO",
        published_at=published_mid,
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    older_false_positive = seed_article(
        db,
        canonical_url="https://example.com/older-false-positive-pmo",
        title="Mace Consult Launches as Standalone PMO Company",
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
    source = seed_source(
        db,
        code="prnewswire",
        name="PR Newswire",
        base_url="https://www.prnewswire.com",
    )
    article, ticker = seed_article_with_raw(
        db, source,
        ticker_kwargs=dict(
            symbol="CGO",
            fund_name="Calamos Global Total Return Fund",
            sponsor="Calamos",
        ),
        article_kwargs=dict(
            canonical_url="https://example.com/purge-real-delete",
            title="Envestnet Accelerates Adaptive WealthTech Innovation",
            summary="New CGO appointment announced",
            published_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
            source_name="PR Newswire",
            provider_name="PR Newswire",
        ),
        match_type="token",
        confidence=0.62,
        raw_guid="prn-guid-real-delete",
        feed_url="https://www.prnewswire.com/rss/news-releases-list.rss",
    )

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

