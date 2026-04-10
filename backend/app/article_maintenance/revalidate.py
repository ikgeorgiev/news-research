from __future__ import annotations

from typing import TypedDict

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker
from app.ticker_context import load_ticker_context
from app.constants import EXTRACTION_VERSION

from app.article_maintenance._common import (
    _apply_revalidation,
    _has_general_allowed_raw_provenance,
    _reextract_purge_article_tickers,
    load_article_maintenance_context,
    stamp_article_ticker_version,
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
    globenewswire_source_page_timeout_seconds: int | None = None,
) -> RevalidationStats:
    ticker_context = load_ticker_context(db)
    if not ticker_context.symbol_to_id:
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

    raw_contexts_by_article, existing_rows_by_article = (
        load_article_maintenance_context(db, article_ids)
    )

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
        existing_for_article = existing_rows_by_article.get(article_id)
        if not raw_contexts:
            # No raw feed items (pruned by retention policy). Stamp version
            # so these rows don't stall the revalidation queue permanently.
            stamp_article_ticker_version(existing_for_article)
            unchanged += 1
            continue

        verified_hits = _reextract_purge_article_tickers(
            article,
            raw_contexts,
            ticker_context.known_symbols,
            timeout_seconds,
            globenewswire_source_page_timeout_seconds=globenewswire_source_page_timeout_seconds,
            symbol_keywords=ticker_context.symbol_keywords,
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
            ticker_context.symbol_to_id,
            existing_rows=existing_for_article,
            prune_verified_hits=False,
            force_update=True,
        )
        # Stamp version on ALL rows for this article — including ones
        # not in verified_hits — so they stop being reselected while
        # keeping their mapping data intact (no prune).
        stamp_article_ticker_version(existing_for_article)
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
