from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker, RawFeedItem, Source
from app.sources import PAGE_FETCH_CONFIGS, POLICY_GENERAL_ALLOWED, get_source_policy
from app.constants import EXTRACTION_VERSION, MIN_PERSIST_CONFIDENCE, NO_KEYWORDS_CONFIDENCE
from app.ticker_extraction import (
    _build_symbol_keywords,
    _extract_entry_tickers,
    _extract_source_fallback_tickers,
    _merge_ticker_hits,
    _verified_ticker_hits,
)


type RawContext = tuple[str, str | None, str | None]


def _has_general_allowed_raw_provenance(
    raw_contexts: list[RawContext],
) -> bool:
    return any(
        get_source_policy(source_code) == POLICY_GENERAL_ALLOWED
        for source_code, _, _ in raw_contexts
    )


def load_raw_contexts(
    db: Session,
    article_ids: Sequence[int],
    *,
    source_code: str | None = None,
) -> dict[int, list[RawContext]]:
    if not article_ids:
        return {}

    query = (
        select(
            RawFeedItem.article_id,
            Source.code,
            RawFeedItem.raw_link,
            RawFeedItem.feed_url,
            RawFeedItem.raw_payload_json,
        )
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(RawFeedItem.article_id.in_(article_ids))
    )
    if source_code is not None:
        query = query.where(Source.code == source_code)

    raw_contexts_by_article: dict[int, list[RawContext]] = {}
    for article_id, raw_source_code, raw_link, feed_url, payload in db.execute(
        query.order_by(RawFeedItem.article_id.asc(), RawFeedItem.id.desc())
    ).all():
        if article_id is None:
            continue
        contexts = raw_contexts_by_article.setdefault(article_id, [])
        contexts.append((raw_source_code, raw_link, feed_url))
        seen = {feed_url}
        for alt_url in (payload or {}).get("_alt_feed_urls") or []:
            if alt_url not in seen:
                seen.add(alt_url)
                contexts.append((raw_source_code, raw_link, alt_url))
    return raw_contexts_by_article


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


def _reextract_purge_article_tickers(
    article: Article,
    raw_contexts: list[RawContext],
    known_symbols: set[str],
    timeout_seconds: int,
    *,
    symbol_keywords: dict[str, frozenset[str]] | None = None,
    stop_when_existing_symbols_verified: set[str] | None = None,
    page_fetch_status: list[str] | None = None,
) -> dict[str, tuple[str, float]]:
    """Re-extract tickers from raw contexts.

    If *page_fetch_status* is provided (a mutable list), the function appends
    ``"ok"`` when at least one page fetch succeeded, or ``"failed"`` when all
    attempted page fetches failed.  When no page fetch was attempted the list
    is left unchanged.
    """
    hits: dict[str, tuple[str, float]] = {}
    required_symbols = {
        symbol.upper()
        for symbol in (stop_when_existing_symbols_verified or set())
        if symbol
    }
    any_fetch_attempted = False
    any_fetch_succeeded = False
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
            if page_fetch_status is not None and any_fetch_attempted:
                page_fetch_status.append("ok" if any_fetch_succeeded else "failed")
            return hits
        config = PAGE_FETCH_CONFIGS.get(source_code)
        if config is None:
            continue
        any_fetch_attempted = True
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
        if fallback_hits is not None:
            any_fetch_succeeded = True
            _merge_ticker_hits(hits, _verified_ticker_hits(source_code, fallback_hits))
        if required_symbols and required_symbols.issubset(hits.keys()):
            if page_fetch_status is not None:
                page_fetch_status.append("ok" if any_fetch_succeeded else "failed")
            return hits

    if page_fetch_status is not None and any_fetch_attempted:
        page_fetch_status.append("ok" if any_fetch_succeeded else "failed")
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
    stamp_retained_version: bool = False,
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
            if stamp_retained_version:
                for symbol in verified_hits:
                    ticker_id = symbol_to_id.get(symbol)
                    if ticker_id is None:
                        continue
                    retained_row = existing_rows.get(ticker_id)
                    if retained_row is not None:
                        retained_row.extraction_version = EXTRACTION_VERSION
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
