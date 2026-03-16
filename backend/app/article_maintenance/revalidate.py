from __future__ import annotations

from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.ticker_extraction import EXTRACTION_VERSION, _build_symbol_keywords

from app.article_maintenance._common import (
    _apply_revalidation,
    _has_general_allowed_raw_provenance,
    _reextract_purge_article_tickers,
)


class RevalidationStats(TypedDict):
    scanned: int
    revalidated: int
    purged: int
    unchanged: int


def revalidate_stale_article_tickers(
    db: Session,
    *,
    limit: int = 200,
    timeout_seconds: int = 20,
) -> RevalidationStats:
    ticker_rows = db.execute(
        select(
            Ticker.id,
            Ticker.symbol,
            Ticker.fund_name,
            Ticker.sponsor,
            Ticker.validation_keywords,
        ).where(Ticker.active.is_(True))
    ).all()
    if not ticker_rows:
        return {"scanned": 0, "revalidated": 0, "purged": 0, "unchanged": 0}

    article_rows = db.execute(
        select(Article.id, Article.published_at)
        .join(ArticleTicker, ArticleTicker.article_id == Article.id)
        .where(ArticleTicker.extraction_version < EXTRACTION_VERSION)
        .group_by(Article.id, Article.published_at)
        .order_by(Article.published_at.desc().nullslast(), Article.id.desc())
        .limit(limit)
    ).all()
    article_ids = [article_id for article_id, _ in article_rows]
    if not article_ids:
        return {"scanned": 0, "revalidated": 0, "purged": 0, "unchanged": 0}

    articles = db.scalars(select(Article).where(Article.id.in_(article_ids))).all()
    articles_by_id = {article.id: article for article in articles}

    raw_contexts_by_article: dict[int, list[tuple[str, str | None, str | None]]] = {}
    raw_context_rows = db.execute(
        select(
            RawFeedItem.article_id,
            Source.code,
            RawFeedItem.raw_link,
            RawFeedItem.feed_url,
        )
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(RawFeedItem.article_id.in_(article_ids))
        .order_by(RawFeedItem.article_id.asc(), RawFeedItem.id.desc())
    ).all()
    for article_id, source_code, raw_link, feed_url in raw_context_rows:
        raw_contexts_by_article.setdefault(article_id, []).append(
            (source_code, raw_link, feed_url)
        )

    existing_rows_by_article: dict[int, dict[int, ArticleTicker]] = {}
    at_rows = db.scalars(
        select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
    ).all()
    for at_row in at_rows:
        existing_rows_by_article.setdefault(at_row.article_id, {})[at_row.ticker_id] = at_row

    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    known_symbols = frozenset(symbol_to_id.keys())
    symbol_keywords = _build_symbol_keywords(ticker_rows)

    scanned = 0
    revalidated = 0
    purged = 0
    unchanged = 0

    for article_id in article_ids:
        article = articles_by_id.get(article_id)
        if article is None:
            continue
        scanned += 1

        raw_contexts = raw_contexts_by_article.get(article_id, [])
        if not raw_contexts:
            # No raw feed items (pruned by retention policy). Stamp version
            # so these rows don't stall the revalidation queue permanently.
            for row in (existing_rows_by_article.get(article_id) or {}).values():
                row.extraction_version = EXTRACTION_VERSION
            unchanged += 1
            continue

        verified_hits = _reextract_purge_article_tickers(
            article,
            raw_contexts,
            known_symbols,
            timeout_seconds,
            symbol_keywords=symbol_keywords,
        )

        if not verified_hits:
            # Revalidation must never delete articles — that's the purge
            # function's job.  Don't stamp the version either: a transient
            # fetch failure should leave the article eligible for retry on
            # the next cycle rather than permanently marking it current.
            unchanged += 1
            continue

        outcome = _apply_revalidation(
            db,
            article,
            verified_hits,
            _has_general_allowed_raw_provenance(raw_contexts),
            symbol_to_id,
            existing_rows=existing_rows_by_article.get(article_id),
            prune_verified_hits=False,
            force_update=True,
        )
        # Stamp version on ALL rows for this article — including ones
        # not in verified_hits — so they stop being reselected while
        # keeping their mapping data intact (no prune).
        for row in (existing_rows_by_article.get(article_id) or {}).values():
            row.extraction_version = EXTRACTION_VERSION
        if outcome.action == "kept":
            if outcome.changed_mappings:
                revalidated += 1
            else:
                unchanged += 1
        else:
            purged += 1

    db.commit()
    return {
        "scanned": scanned,
        "revalidated": revalidated,
        "purged": purged,
        "unchanged": unchanged,
    }
