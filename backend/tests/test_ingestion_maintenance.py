from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
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
            "validated_token",
            0.68,
            "prn-guid-page-derived-validated-token",
            "https://www.prnewswire.com/rss/news-releases-list.rss",
            id="page_derived_validated_token",
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

def test_dedupe_articles_by_title_merges_duplicates(db_session):
    """Basic coverage: two non-BW articles with the same normalized title get merged."""
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
    article = seed_article(
        db,
        canonical_url="https://example.com/revalidate-upsert",
        title="Article",
        published_at=datetime(2026, 3, 11, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(
        db, symbol="CGO",
        fund_name="Calamos Global Total Return Fund",
        sponsor="Calamos",
    )
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
    article = seed_article(
        db,
        canonical_url="https://example.com/revalidate-stamp",
        title="Article",
        published_at=datetime(2026, 3, 11, 1, tzinfo=timezone.utc),
        source_name="PR Newswire",
        provider_name="PR Newswire",
    )
    ticker = seed_ticker(
        db, symbol="FFA",
        fund_name="First Trust Enhanced Equity Income",
        sponsor="First Trust Advisors L.P.",
    )
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
    source = seed_source(db, code="prnewswire", name="PR Newswire", base_url="https://www.prnewswire.com")
    article, ticker = seed_article_with_raw(
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
    article, ticker = seed_article_with_raw(
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
