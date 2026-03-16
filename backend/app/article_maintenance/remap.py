from __future__ import annotations

from typing import TypedDict

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.sources import PAGE_FETCH_CONFIGS
from app.ticker_extraction import _build_symbol_keywords

from app.article_maintenance._common import (
    _apply_revalidation,
    _has_general_allowed_raw_provenance,
    _reextract_purge_article_tickers,
)


class SourceRemapStats(TypedDict):
    source_code: str
    processed: int
    articles_with_hits: int
    remapped_articles: int
    only_unmapped: bool


def remap_source_articles(
    db: Session,
    settings: Settings,
    *,
    source_code: str,
    limit: int = 500,
    only_unmapped: bool = True,
) -> SourceRemapStats:
    ticker_rows = db.execute(
        select(
            Ticker.id,
            Ticker.symbol,
            Ticker.fund_name,
            Ticker.sponsor,
            Ticker.validation_keywords,
        ).where(
            Ticker.active.is_(True)
        )
    ).all()
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    known_symbols = set(symbol_to_id.keys())
    symbol_keywords = _build_symbol_keywords(ticker_rows)

    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
    source_exists = (
        select(1)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                Source.code == source_code,
            )
        )
        .correlate(Article)
        .exists()
    )

    query = select(Article).where(source_exists)
    if only_unmapped:
        query = query.where(~mapped_exists)

    rows = db.scalars(
        query.order_by(
            Article.published_at.desc().nullslast(), Article.id.desc()
        ).limit(limit)
    ).all()
    article_ids = [row.id for row in rows]
    ticker_rows_by_article: dict[int, dict[int, ArticleTicker]] = {
        article_id: {} for article_id in article_ids
    }
    raw_contexts_by_article: dict[int, list[tuple[str, str | None, str | None]]] = {
        article_id: [] for article_id in article_ids
    }
    if article_ids:
        existing_rows = db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
        ).all()
        for ticker_row in existing_rows:
            ticker_rows_by_article.setdefault(ticker_row.article_id, {})[
                ticker_row.ticker_id
            ] = ticker_row
        raw_context_rows = db.execute(
            select(
                RawFeedItem.article_id,
                Source.code,
                RawFeedItem.raw_link,
                RawFeedItem.feed_url,
            )
            .join(Source, Source.id == RawFeedItem.source_id)
            .where(
                and_(
                    RawFeedItem.article_id.in_(article_ids),
                    Source.code == source_code,
                )
            )
            .order_by(RawFeedItem.article_id.asc(), RawFeedItem.id.desc())
        ).all()
        for article_id, raw_source_code, raw_link, feed_url in raw_context_rows:
            if article_id is None:
                continue
            raw_contexts_by_article.setdefault(article_id, []).append(
                (raw_source_code, raw_link, feed_url)
            )

    processed = 0
    articles_with_hits = 0
    remapped_articles = 0

    for row in rows:
        processed += 1
        raw_contexts = raw_contexts_by_article.get(row.id, [])
        verified_hits = _reextract_purge_article_tickers(
            row,
            raw_contexts,
            known_symbols,
            settings.request_timeout_seconds,
            symbol_keywords=symbol_keywords,
        )
        if not verified_hits:
            continue
        has_general_provenance = _has_general_allowed_raw_provenance(raw_contexts)
        existing_for_article = ticker_rows_by_article.setdefault(row.id, {})
        outcome = _apply_revalidation(
            db,
            row,
            verified_hits,
            has_general_provenance,
            symbol_to_id,
            existing_rows=existing_for_article,
            prune_verified_hits=False,
        )
        if outcome.had_verified_hits:
            articles_with_hits += 1
        if outcome.changed_mappings:
            remapped_articles += 1

    db.commit()

    return {
        "source_code": source_code,
        "processed": processed,
        "articles_with_hits": articles_with_hits,
        "remapped_articles": remapped_articles,
        "only_unmapped": only_unmapped,
    }
