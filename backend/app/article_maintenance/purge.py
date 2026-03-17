from __future__ import annotations

from datetime import datetime
from typing import TypedDict

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.article_maintenance._common import (
    _apply_revalidation,
    _has_general_allowed_raw_provenance,
    _reextract_purge_article_tickers,
    load_raw_contexts,
)
from app.models import Article, ArticleTicker, RawFeedItem, Source
from app.query_utils import any_ticker_mapped_exists
from app.ticker_context import load_ticker_context
from app.constants import EXTRACTION_VERSION, MIN_PERSIST_CONFIDENCE, NO_KEYWORDS_CONFIDENCE
from app.utils import GENERAL_SOURCE_CODE


class PurgeFalsePositiveStats(TypedDict):
    scanned_articles: int
    purged_articles: int
    deleted_article_tickers: int
    deleted_raw_feed_items: int


def purge_token_only_articles(
    db: Session,
    *,
    dry_run: bool = True,
    limit: int = 2000,
    timeout_seconds: int = 20,
) -> PurgeFalsePositiveStats:
    ticker_context = load_ticker_context(db)

    mapped_exists = any_ticker_mapped_exists()
    plain_token_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(
            and_(
                ArticleTicker.article_id == Article.id,
                ArticleTicker.match_type == "token",
            )
        )
        .correlate(Article)
        .exists()
    )
    stale_version_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(
            and_(
                ArticleTicker.article_id == Article.id,
                ArticleTicker.extraction_version < EXTRACTION_VERSION,
            )
        )
        .correlate(Article)
        .exists()
    )
    low_confidence_exists = (
        select(func.max(ArticleTicker.confidence))
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .scalar_subquery()
    )
    has_any_raw = (
        select(1)
        .select_from(RawFeedItem)
        .where(RawFeedItem.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
    bw_source_id = db.scalar(
        select(Source.id).where(Source.code == GENERAL_SOURCE_CODE)
    )
    bw_exists = (
        select(1)
        .select_from(RawFeedItem)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                RawFeedItem.source_id == bw_source_id,
            )
        )
        .correlate(Article)
        .exists()
        if bw_source_id is not None
        else None
    )

    scanned = 0
    purged = 0
    deleted_at = 0
    deleted_rfi = 0
    seen_ids: set[int] = set()

    batch_size = min(max(limit, 200), 1000)
    cursor_published_at: datetime | None = None
    cursor_id: int | None = None

    while purged < limit:
        query = (
            select(Article)
            .where(mapped_exists)
            .where(has_any_raw)
            .where(
                or_(
                    low_confidence_exists < MIN_PERSIST_CONFIDENCE,
                    plain_token_exists,
                    stale_version_exists,
                )
            )
        )
        if bw_exists is not None:
            query = query.where(~bw_exists)
        if cursor_id is not None:
            if cursor_published_at is None:
                query = query.where(
                    and_(
                        Article.published_at.is_(None),
                        Article.id < cursor_id,
                    )
                )
            else:
                query = query.where(
                    or_(
                        Article.published_at.is_(None),
                        Article.published_at < cursor_published_at,
                        and_(
                            Article.published_at == cursor_published_at,
                            Article.id < cursor_id,
                        ),
                    )
                )
        articles = db.scalars(
            query.order_by(
                Article.published_at.desc().nullslast(), Article.id.desc()
            ).limit(batch_size)
        ).all()
        if not articles:
            break

        article_ids = [article.id for article in articles]
        raw_contexts_by_article = load_raw_contexts(db, article_ids)

        existing_rows_by_article: dict[int, dict[int, ArticleTicker]] = {}
        if article_ids:
            at_rows = db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
            ).all()
            for at_row in at_rows:
                existing_rows_by_article.setdefault(at_row.article_id, {})[
                    at_row.ticker_id
                ] = at_row

        for article in articles:
            if article.id in seen_ids:
                continue
            seen_ids.add(article.id)
            scanned += 1

            raw_contexts = raw_contexts_by_article.get(article.id, [])
            if not raw_contexts:
                continue

            has_general_provenance = _has_general_allowed_raw_provenance(raw_contexts)
            existing_for_article = existing_rows_by_article.get(article.id)
            has_stale_existing_rows = any(
                (row.extraction_version or 0) < EXTRACTION_VERSION
                for row in (existing_for_article or {}).values()
            )
            preserved_existing_hits = {
                ticker_context.id_to_symbol[ticker_id]: (row.match_type, row.confidence)
                for ticker_id, row in (existing_for_article or {}).items()
                if ticker_id in ticker_context.id_to_symbol
                and row.confidence >= NO_KEYWORDS_CONFIDENCE
                and row.match_type != "token"
            }
            # When the only rows outside preserved_existing_hits are token
            # rows, we can prune them without a page fetch.
            non_preserved_non_token = any(
                ticker_context.id_to_symbol.get(ticker_id)
                and ticker_context.id_to_symbol[ticker_id] not in preserved_existing_hits
                and row.match_type != "token"
                for ticker_id, row in (existing_for_article or {}).items()
            )
            if preserved_existing_hits and not non_preserved_non_token and len(
                preserved_existing_hits
            ) < len(existing_for_article or {}):
                outcome = _apply_revalidation(
                    db,
                    article,
                    preserved_existing_hits,
                    has_general_provenance,
                    ticker_context.symbol_to_id,
                    existing_rows=existing_for_article,
                    dry_run=dry_run,
                    stamp_retained_version=has_stale_existing_rows,
                )
                if outcome.changed_mappings:
                    purged += 1
                    deleted_at += outcome.deleted_article_tickers
                continue

            existing_symbols = {
                ticker_context.id_to_symbol[ticker_id]
                for ticker_id, row in (existing_for_article or {}).items()
                if ticker_id in ticker_context.id_to_symbol
                and row.match_type != "token"
            }
            fetch_status: list[str] = []
            verified_hits = _reextract_purge_article_tickers(
                article,
                raw_contexts,
                ticker_context.known_symbols,
                timeout_seconds,
                symbol_keywords=ticker_context.symbol_keywords,
                stop_when_existing_symbols_verified=existing_symbols,
                page_fetch_status=fetch_status,
            )
            if verified_hits:
                outcome = _apply_revalidation(
                    db,
                    article,
                    verified_hits,
                    has_general_provenance,
                    ticker_context.symbol_to_id,
                    existing_rows=existing_for_article,
                    dry_run=dry_run,
                    stamp_retained_version=has_stale_existing_rows,
                )
                if outcome.changed_mappings:
                    purged += 1
                    deleted_at += outcome.deleted_article_tickers
                continue

            page_fetch_failed = fetch_status and fetch_status[0] == "failed"
            if preserved_existing_hits and page_fetch_failed:
                # Re-extraction returned nothing and the page fetch failed.
                # The article had high-confidence non-token mappings so this
                # is likely a transient miss — stamp only the preserved rows
                # so they leave the stale queue.  Non-preserved rows stay
                # stale so they are retried on the next cycle.
                if has_stale_existing_rows and not dry_run:
                    preserved_ticker_ids = {
                        ticker_id
                        for ticker_id, row in (existing_for_article or {}).items()
                        if ticker_id in ticker_context.id_to_symbol
                        and ticker_context.id_to_symbol[ticker_id] in preserved_existing_hits
                    }
                    for ticker_id, row in (existing_for_article or {}).items():
                        if ticker_id in preserved_ticker_ids:
                            row.extraction_version = EXTRACTION_VERSION
                continue

            outcome = _apply_revalidation(
                db,
                article,
                {},
                has_general_provenance,
                ticker_context.symbol_to_id,
                existing_rows=existing_for_article,
                dry_run=dry_run,
            )
            deleted_at += outcome.deleted_article_tickers
            deleted_rfi += outcome.deleted_raw_feed_items
            purged += 1
            if purged >= limit:
                break

        last_article = articles[-1]
        cursor_published_at = last_article.published_at
        cursor_id = last_article.id

    if not dry_run:
        db.commit()

    return {
        "scanned_articles": scanned,
        "purged_articles": purged,
        "deleted_article_tickers": deleted_at,
        "deleted_raw_feed_items": deleted_rfi,
    }
