from __future__ import annotations

from typing import TypedDict

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.constants import TITLE_DEDUP_WINDOW_HOURS
from app.models import Article, ArticleTicker, RawFeedItem, Source
from app.ticker_extraction import _canonical_businesswire_article_url, _is_businesswire_article_url
from app.utils import GENERAL_SOURCE_CODE, canonicalize_url, sha256_str, to_utc


class DedupeStats(TypedDict):
    scanned_articles: int
    duplicate_groups: int
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


class _MergeStats(TypedDict):
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


def _merge_duplicates_into_winner(
    db: Session,
    *,
    winner: Article,
    duplicates: list[Article],
    ticker_rows_by_article: dict[int, list[ArticleTicker]],
    raw_rows: list[RawFeedItem],
) -> _MergeStats:
    winner_tickers: dict[int, ArticleTicker] = {
        row.ticker_id: row for row in ticker_rows_by_article.get(winner.id, [])
    }
    duplicate_ids = {row.id for row in duplicates}
    raw_items_relinked = 0
    ticker_rows_relinked = 0
    ticker_rows_updated = 0
    ticker_rows_deleted = 0
    merged_articles = 0

    for raw_row in raw_rows:
        if raw_row.article_id not in duplicate_ids:
            continue
        raw_row.article_id = winner.id
        raw_items_relinked += 1

    for duplicate in duplicates:
        duplicate_summary = duplicate.summary or ""
        winner_summary = winner.summary or ""
        if duplicate_summary and (
            not winner_summary or len(duplicate_summary) > len(winner_summary)
        ):
            winner.summary = duplicate.summary
        if duplicate.published_at and (
            winner.published_at is None
            or duplicate.published_at > winner.published_at
        ):
            winner.published_at = duplicate.published_at
        duplicate_first_seen = duplicate.first_seen_at or duplicate.created_at
        winner_first_seen = winner.first_seen_at or winner.created_at
        if duplicate_first_seen and (
            winner_first_seen is None
            or to_utc(duplicate_first_seen) < to_utc(winner_first_seen)
        ):
            winner.first_seen_at = duplicate_first_seen
        if duplicate.first_alert_sent_at and (
            winner.first_alert_sent_at is None
            or to_utc(duplicate.first_alert_sent_at)
            < to_utc(winner.first_alert_sent_at)
        ):
            winner.first_alert_sent_at = duplicate.first_alert_sent_at

        for ticker_row in ticker_rows_by_article.get(duplicate.id, []):
            existing = winner_tickers.get(ticker_row.ticker_id)
            if existing is None:
                ticker_row.article_id = winner.id
                winner_tickers[ticker_row.ticker_id] = ticker_row
                ticker_rows_relinked += 1
                continue

            if ticker_row.confidence > existing.confidence:
                existing.confidence = ticker_row.confidence
                existing.match_type = ticker_row.match_type
                ticker_rows_updated += 1
            db.delete(ticker_row)
            ticker_rows_deleted += 1

        db.flush()
        db.delete(duplicate)
        merged_articles += 1

    return {
        "merged_articles": merged_articles,
        "raw_items_relinked": raw_items_relinked,
        "ticker_rows_relinked": ticker_rows_relinked,
        "ticker_rows_updated": ticker_rows_updated,
        "ticker_rows_deleted": ticker_rows_deleted,
    }


def dedupe_businesswire_url_variants(db: Session) -> DedupeStats:
    grouped: dict[str, list[Article]] = {}
    scanned_articles = 0
    batch_size = 500
    cursor_id = 0

    while True:
        batch = db.scalars(
            select(Article)
            .where(
                Article.canonical_url.ilike("%businesswire.com%"),
                Article.id > cursor_id,
            )
            .order_by(Article.id.asc())
            .limit(batch_size)
        ).all()
        if not batch:
            break
        cursor_id = batch[-1].id
        for article in batch:
            canonical_candidate = canonicalize_url(article.canonical_url)
            if not _is_businesswire_article_url(canonical_candidate):
                continue
            scanned_articles += 1
            normalized = _canonical_businesswire_article_url(canonical_candidate)
            grouped.setdefault(normalized, []).append(article)

    duplicate_groups = 0
    merged_articles = 0
    raw_items_relinked = 0
    ticker_rows_relinked = 0
    ticker_rows_updated = 0
    ticker_rows_deleted = 0

    for normalized_url, articles in grouped.items():
        if len(articles) <= 1:
            continue

        duplicate_groups += 1
        article_ids = [row.id for row in articles]
        ticker_rows = db.scalars(
            select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
        ).all()
        ticker_rows_by_article: dict[int, list[ArticleTicker]] = {}
        for ticker_row in ticker_rows:
            ticker_rows_by_article.setdefault(ticker_row.article_id, []).append(
                ticker_row
            )

        raw_rows = db.scalars(
            select(RawFeedItem).where(RawFeedItem.article_id.in_(article_ids))
        ).all()

        def _winner_rank(row: Article) -> tuple[int, int, int, float, int]:
            cleaned_url = canonicalize_url(row.canonical_url)
            has_normalized_url = int(cleaned_url == normalized_url)
            ticker_count = len(ticker_rows_by_article.get(row.id, []))
            summary_len = len(row.summary or "")
            published = row.published_at or row.created_at
            published_rank = to_utc(published).timestamp()
            return has_normalized_url, ticker_count, summary_len, published_rank, row.id

        winner = max(articles, key=_winner_rank)
        duplicates = [row for row in articles if row.id != winner.id]
        if not duplicates:
            continue

        winner_cleaned_url = canonicalize_url(winner.canonical_url)
        if winner_cleaned_url != normalized_url:
            winner.canonical_url = normalized_url
            winner.canonical_url_hash = sha256_str(normalized_url)

        merge_stats = _merge_duplicates_into_winner(
            db,
            winner=winner,
            duplicates=duplicates,
            ticker_rows_by_article=ticker_rows_by_article,
            raw_rows=raw_rows,
        )
        merged_articles += merge_stats["merged_articles"]
        raw_items_relinked += merge_stats["raw_items_relinked"]
        ticker_rows_relinked += merge_stats["ticker_rows_relinked"]
        ticker_rows_updated += merge_stats["ticker_rows_updated"]
        ticker_rows_deleted += merge_stats["ticker_rows_deleted"]

    db.commit()
    return {
        "scanned_articles": scanned_articles,
        "duplicate_groups": duplicate_groups,
        "merged_articles": merged_articles,
        "raw_items_relinked": raw_items_relinked,
        "ticker_rows_relinked": ticker_rows_relinked,
        "ticker_rows_updated": ticker_rows_updated,
        "ticker_rows_deleted": ticker_rows_deleted,
    }


def dedupe_articles_by_title(
    db: Session,
    *,
    window_hours: int = TITLE_DEDUP_WINDOW_HOURS,
) -> DedupeStats:
    """Merge articles that share the same normalized title within a time window."""
    # Only load title hashes that have more than one article (potential dupes).
    dupe_hashes = db.execute(
        select(Article.title_normalized_hash)
        .group_by(Article.title_normalized_hash)
        .having(func.count(Article.id) > 1)
    ).scalars().all()

    grouped: dict[str, list[Article]] = {}
    scanned_articles = 0
    batch_size = 500
    for offset in range(0, len(dupe_hashes), batch_size):
        hash_batch = dupe_hashes[offset : offset + batch_size]
        batch_articles = db.scalars(
            select(Article)
            .where(Article.title_normalized_hash.in_(hash_batch))
            .order_by(Article.id.asc())
        ).all()
        scanned_articles += len(batch_articles)
        for article in batch_articles:
            grouped.setdefault(article.title_normalized_hash, []).append(article)
    duplicate_groups = 0
    merged_articles = 0
    raw_items_relinked = 0
    ticker_rows_relinked = 0
    ticker_rows_updated = 0
    ticker_rows_deleted = 0

    for articles in grouped.values():
        if len(articles) <= 1:
            continue

        sorted_articles = sorted(
            articles,
            key=lambda a: to_utc(a.published_at or a.created_at),
        )
        clusters: list[list[Article]] = []
        for article in sorted_articles:
            pub = to_utc(article.published_at or article.created_at)
            placed = False
            for cluster in clusters:
                earliest = to_utc(cluster[0].published_at or cluster[0].created_at)
                if (pub - earliest).total_seconds() <= window_hours * 3600:
                    cluster.append(article)
                    placed = True
                    break
            if not placed:
                clusters.append([article])

        for cluster in clusters:
            if len(cluster) <= 1:
                continue

            article_ids = [row.id for row in cluster]
            cluster_raw_rows = db.scalars(
                select(RawFeedItem).where(RawFeedItem.article_id.in_(article_ids))
            ).all()
            raw_by_article: dict[int, set[int]] = {}
            for raw_row in cluster_raw_rows:
                raw_by_article.setdefault(raw_row.article_id, set()).add(
                    raw_row.source_id
                )
            bw_source_id = db.scalar(
                select(Source.id).where(Source.code == GENERAL_SOURCE_CODE)
            )
            has_bw_risk = False
            for aid in article_ids:
                source_ids = raw_by_article.get(aid, set())
                if not source_ids or bw_source_id in source_ids:
                    has_bw_risk = True
                    break
            if has_bw_risk:
                continue

            duplicate_groups += 1
            ticker_rows = db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id.in_(article_ids))
            ).all()
            ticker_rows_by_article: dict[int, list[ArticleTicker]] = {}
            for ticker_row in ticker_rows:
                ticker_rows_by_article.setdefault(ticker_row.article_id, []).append(
                    ticker_row
                )

            raw_rows = cluster_raw_rows

            def _rank(row: Article) -> tuple[int, int, float, int]:
                ticker_count = len(ticker_rows_by_article.get(row.id, []))
                summary_len = len(row.summary or "")
                published = row.published_at or row.created_at
                return ticker_count, summary_len, to_utc(published).timestamp(), row.id

            winner = max(cluster, key=_rank)
            duplicates = [row for row in cluster if row.id != winner.id]
            if not duplicates:
                continue

            merge_stats = _merge_duplicates_into_winner(
                db,
                winner=winner,
                duplicates=duplicates,
                ticker_rows_by_article=ticker_rows_by_article,
                raw_rows=raw_rows,
            )
            merged_articles += merge_stats["merged_articles"]
            raw_items_relinked += merge_stats["raw_items_relinked"]
            ticker_rows_relinked += merge_stats["ticker_rows_relinked"]
            ticker_rows_updated += merge_stats["ticker_rows_updated"]
            ticker_rows_deleted += merge_stats["ticker_rows_deleted"]

    db.commit()
    return {
        "scanned_articles": scanned_articles,
        "duplicate_groups": duplicate_groups,
        "merged_articles": merged_articles,
        "raw_items_relinked": raw_items_relinked,
        "ticker_rows_relinked": ticker_rows_relinked,
        "ticker_rows_updated": ticker_rows_updated,
        "ticker_rows_deleted": ticker_rows_deleted,
    }
