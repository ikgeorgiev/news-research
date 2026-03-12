from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TypedDict

from sqlalchemy import and_, delete, desc, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.sources import PAGE_FETCH_CONFIGS, POLICY_GENERAL_ALLOWED, get_source_policy
from app.ticker_extraction import (
    EXTRACTION_VERSION,
    MIN_PERSIST_CONFIDENCE,
    _build_symbol_keywords,
    _canonical_businesswire_article_url,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _is_businesswire_article_url,
    _merge_ticker_hits,
    _verified_ticker_hits,
)
from app.utils import GENERAL_SOURCE_CODE, canonicalize_url, sha256_str, to_utc


class SourceRemapStats(TypedDict):
    source_code: str
    processed: int
    articles_with_hits: int
    remapped_articles: int
    only_unmapped: bool


class DedupeStats(TypedDict):
    scanned_articles: int
    duplicate_groups: int
    merged_articles: int
    raw_items_relinked: int
    ticker_rows_relinked: int
    ticker_rows_updated: int
    ticker_rows_deleted: int


class PurgeFalsePositiveStats(TypedDict):
    scanned_articles: int
    purged_articles: int
    deleted_article_tickers: int
    deleted_raw_feed_items: int


class RevalidationStats(TypedDict):
    scanned: int
    revalidated: int
    purged: int
    unchanged: int


def _has_general_allowed_raw_provenance(
    raw_contexts: list[tuple[str, str | None, str | None]],
) -> bool:
    return any(
        get_source_policy(source_code) == POLICY_GENERAL_ALLOWED
        for source_code, _, _ in raw_contexts
    )


def _upsert_article_tickers(
    db: Session,
    article_id: int,
    ticker_hits: dict[str, tuple[str, float]],
    symbol_to_id: dict[str, int],
    *,
    existing_rows: dict[int, ArticleTicker] | None = None,
    prune_missing: bool = False,
    force_update: bool = False,
) -> None:
    if not ticker_hits and not prune_missing:
        return

    if existing_rows is None:
        existing = {
            row.ticker_id: row
            for row in db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id == article_id)
            ).all()
        }
    else:
        existing = existing_rows

    resolved_ticker_ids: set[int] = set()
    for symbol, (match_type, confidence) in ticker_hits.items():
        ticker_id = symbol_to_id.get(symbol)
        if ticker_id is None:
            continue
        resolved_ticker_ids.add(ticker_id)

        row = existing.get(ticker_id)
        if row is None:
            candidate = ArticleTicker(
                article_id=article_id,
                ticker_id=ticker_id,
                match_type=match_type,
                confidence=confidence,
                extraction_version=EXTRACTION_VERSION,
            )
            try:
                with db.begin_nested():
                    db.add(candidate)
                    db.flush()
                row = candidate
                existing[ticker_id] = row
            except IntegrityError:
                row = db.scalar(
                    select(ArticleTicker).where(
                        and_(
                            ArticleTicker.article_id == article_id,
                            ArticleTicker.ticker_id == ticker_id,
                        )
                    )
                )
                if row is None:
                    raise

        updated = False
        if force_update or confidence > row.confidence:
            row.confidence = confidence
            row.match_type = match_type
            updated = True
        if updated or force_update:
            row.extraction_version = EXTRACTION_VERSION

    if prune_missing:
        for ticker_id, row in list(existing.items()):
            if ticker_id in resolved_ticker_ids:
                continue
            db.delete(row)
            existing.pop(ticker_id, None)


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


def dedupe_businesswire_url_variants(db: Session) -> DedupeStats:
    candidate_rows = db.scalars(
        select(Article)
        .where(Article.canonical_url.ilike("%businesswire.com%"))
        .order_by(Article.id.asc())
    ).all()

    grouped: dict[str, list[Article]] = {}
    scanned_articles = 0
    for article in candidate_rows:
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

        winner_tickers: dict[int, ArticleTicker] = {
            row.ticker_id: row for row in ticker_rows_by_article.get(winner.id, [])
        }

        duplicate_ids = {row.id for row in duplicates}
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


def dedupe_articles_by_title(db: Session, *, window_hours: int = 48) -> DedupeStats:
    """Merge articles that share the same normalized title within a time window."""
    all_articles = db.scalars(select(Article).order_by(Article.id.asc())).all()

    grouped: dict[str, list[Article]] = {}
    for article in all_articles:
        grouped.setdefault(article.title_normalized_hash, []).append(article)

    scanned_articles = len(all_articles)
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

            winner_tickers: dict[int, ArticleTicker] = {
                row.ticker_id: row for row in ticker_rows_by_article.get(winner.id, [])
            }

            duplicate_ids = {row.id for row in duplicates}
            for raw_row in raw_rows:
                if raw_row.article_id not in duplicate_ids:
                    continue
                raw_row.article_id = winner.id
                raw_items_relinked += 1

            for duplicate in duplicates:
                dup_summary = duplicate.summary or ""
                win_summary = winner.summary or ""
                if dup_summary and (
                    not win_summary or len(dup_summary) > len(win_summary)
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


def _reextract_purge_article_tickers(
    article: Article,
    raw_contexts: list[tuple[str, str | None, str | None]],
    known_symbols: set[str],
    timeout_seconds: int,
    *,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
    stop_when_existing_symbols_verified: set[str] | None = None,
) -> dict[str, tuple[str, float]]:
    hits: dict[str, tuple[str, float]] = {}
    required_symbols = {
        symbol.upper()
        for symbol in (stop_when_existing_symbols_verified or set())
        if symbol
    }
    for source_code, raw_link, feed_url in raw_contexts:
        entry_hits = _extract_entry_tickers(
            article.title,
            article.summary or "",
            raw_link or article.canonical_url,
            feed_url or "",
            known_symbols,
            symbol_keywords=symbol_keywords,
        )
        _merge_ticker_hits(hits, _verified_ticker_hits(source_code, entry_hits))
        if required_symbols and required_symbols.issubset(hits.keys()):
            return hits
        config = PAGE_FETCH_CONFIGS.get(source_code)
        if config is None:
            continue
        fallback_hits = _extract_source_fallback_tickers(
            article.title,
            article.summary or "",
            raw_link or article.canonical_url,
            feed_url or "",
            known_symbols,
            timeout_seconds,
            config,
            symbol_keywords=symbol_keywords,
        )
        _merge_ticker_hits(hits, _verified_ticker_hits(source_code, fallback_hits))
        if required_symbols and required_symbols.issubset(hits.keys()):
            return hits

    return hits


@dataclass
class _RevalidationOutcome:
    action: str
    had_verified_hits: bool
    changed_mappings: bool
    deleted_article_tickers: int
    deleted_raw_feed_items: int


def _apply_revalidation(
    db: Session,
    article: Article,
    verified_hits: dict[str, tuple[str, float]],
    has_general_provenance: bool,
    symbol_to_id: dict[str, int],
    *,
    existing_rows: dict[int, ArticleTicker] | None = None,
    prune_verified_hits: bool = True,
    dry_run: bool = False,
    force_update: bool = False,
) -> _RevalidationOutcome:
    if existing_rows is None:
        existing_rows = {
            row.ticker_id: row
            for row in db.scalars(
                select(ArticleTicker).where(ArticleTicker.article_id == article.id)
            ).all()
        }
    existing_ids = set(existing_rows.keys())

    if verified_hits:
        verified_ids = {symbol_to_id[s] for s in verified_hits if s in symbol_to_id}
        if prune_verified_hits:
            changed = verified_ids != existing_ids
            removed_count = len(existing_ids - verified_ids)
        else:
            changed = bool(verified_ids - existing_ids)
            removed_count = 0

        if force_update:
            for symbol, (match_type, confidence) in verified_hits.items():
                ticker_id = symbol_to_id.get(symbol)
                if ticker_id is None:
                    continue
                existing_row = existing_rows.get(ticker_id)
                if existing_row is None:
                    changed = True
                    continue
                if existing_row.match_type != match_type:
                    changed = True
                    continue
                if existing_row.confidence != confidence:
                    changed = True

        if not dry_run:
            _upsert_article_tickers(
                db,
                article.id,
                verified_hits,
                symbol_to_id,
                existing_rows=existing_rows,
                prune_missing=prune_verified_hits,
                force_update=force_update,
            )
        return _RevalidationOutcome("kept", True, changed, removed_count, 0)

    at_count = len(existing_ids)

    if has_general_provenance:
        if not dry_run:
            _upsert_article_tickers(
                db,
                article.id,
                {},
                symbol_to_id,
                existing_rows=existing_rows,
                prune_missing=True,
            )
        return _RevalidationOutcome("pruned", False, bool(existing_ids), at_count, 0)

    if not dry_run:
        rfi_count = db.execute(
            update(RawFeedItem)
            .where(RawFeedItem.article_id == article.id)
            .values(article_id=None)
        ).rowcount
        db.execute(delete(ArticleTicker).where(ArticleTicker.article_id == article.id))
        db.execute(delete(Article).where(Article.id == article.id))
    else:
        rfi_count = (
            db.scalar(
                select(func.count())
                .select_from(RawFeedItem)
                .where(RawFeedItem.article_id == article.id)
            )
            or 0
        )

    return _RevalidationOutcome(
        "deleted", False, bool(existing_ids), at_count, rfi_count
    )


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


def purge_token_only_articles(
    db: Session,
    *,
    dry_run: bool = True,
    limit: int = 2000,
    timeout_seconds: int = 20,
) -> PurgeFalsePositiveStats:
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
    symbol_keywords = _build_symbol_keywords(ticker_rows)
    symbol_to_id = {row[1].upper(): row[0] for row in ticker_rows}
    id_to_symbol = {row[0]: row[1].upper() for row in ticker_rows}
    known_symbols = frozenset(symbol_to_id.keys())

    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .where(ArticleTicker.article_id == Article.id)
        .correlate(Article)
        .exists()
    )
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
        raw_contexts_by_article: dict[int, list[tuple[str, str | None, str | None]]] = (
            {}
        )
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
            preserved_existing_hits = {
                id_to_symbol[ticker_id]: (row.match_type, row.confidence)
                for ticker_id, row in (existing_for_article or {}).items()
                if ticker_id in id_to_symbol
                and row.confidence >= MIN_PERSIST_CONFIDENCE
                and row.match_type != "token"
            }
            if preserved_existing_hits and len(preserved_existing_hits) < len(
                existing_for_article or {}
            ):
                outcome = _apply_revalidation(
                    db,
                    article,
                    preserved_existing_hits,
                    has_general_provenance,
                    symbol_to_id,
                    existing_rows=existing_for_article,
                    dry_run=dry_run,
                )
                if outcome.changed_mappings:
                    purged += 1
                    deleted_at += outcome.deleted_article_tickers
                continue

            existing_symbols = {
                id_to_symbol[ticker_id]
                for ticker_id in (existing_for_article or {}).keys()
                if ticker_id in id_to_symbol
            }
            verified_hits = _reextract_purge_article_tickers(
                article,
                raw_contexts,
                known_symbols,
                timeout_seconds,
                symbol_keywords=symbol_keywords,
                stop_when_existing_symbols_verified=existing_symbols,
            )
            if verified_hits:
                outcome = _apply_revalidation(
                    db,
                    article,
                    verified_hits,
                    has_general_provenance,
                    symbol_to_id,
                    existing_rows=existing_for_article,
                    dry_run=dry_run,
                )
                if outcome.changed_mappings:
                    purged += 1
                    deleted_at += outcome.deleted_article_tickers
                continue

            outcome = _apply_revalidation(
                db,
                article,
                {},
                has_general_provenance,
                symbol_to_id,
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

