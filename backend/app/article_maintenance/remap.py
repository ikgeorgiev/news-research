from __future__ import annotations

from typing import TypedDict

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.query_utils import any_ticker_mapped_exists
from app.article_maintenance._common import (
    _apply_revalidation,
    _has_general_allowed_raw_provenance,
    _reextract_purge_article_tickers,
    load_article_maintenance_context,
)
from app.config import Settings
from app.models import Article, RawFeedItem, Source
from app.ticker_context import load_ticker_context


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
    globenewswire_source_page_timeout_seconds: int | None = None,
) -> SourceRemapStats:
    ticker_context = load_ticker_context(db)

    mapped_exists = any_ticker_mapped_exists()
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
    raw_contexts_by_article, ticker_rows_by_article = (
        load_article_maintenance_context(db, article_ids, source_code=source_code)
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
            ticker_context.known_symbols,
            settings.request_timeout_seconds,
            globenewswire_source_page_timeout_seconds=globenewswire_source_page_timeout_seconds,
            symbol_keywords=ticker_context.symbol_keywords,
        )
        if not verified_hits:
            continue
        has_general_provenance = _has_general_allowed_raw_provenance(raw_contexts)
        existing_for_article = ticker_rows_by_article.get(row.id)
        outcome = _apply_revalidation(
            db,
            row,
            verified_hits,
            has_general_provenance,
            ticker_context.symbol_to_id,
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
