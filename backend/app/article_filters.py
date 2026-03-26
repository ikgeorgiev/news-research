from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.query_utils import active_ticker_mapped_exists, contains_literal_pattern

GENERAL_UNMAPPED_PROVIDER = "Business Wire"


def _normalized_ticker_symbols(
    *,
    ticker: str | None = None,
    tickers: Sequence[str] | None = None,
) -> list[str]:
    raw_values: list[str]
    if tickers is not None:
        raw_values = [str(value) for value in tickers]
    elif ticker:
        raw_values = ticker.split(",")
    else:
        raw_values = []

    normalized: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        symbol = value.strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def find_source_by_name(db: Session, source_name: str) -> Source | None:
    normalized = source_name.strip()
    if not normalized:
        return None
    return db.scalar(select(Source).where(func.lower(Source.name) == normalized.lower()))


def source_id_by_name_subquery(source_name: str):
    normalized = source_name.strip()
    return select(Source.id).where(func.lower(Source.name) == normalized.lower())


def article_has_any_raw_provenance(
    *,
    article_id_col=Article.id,
):
    return (
        select(1)
        .select_from(RawFeedItem)
        .where(RawFeedItem.article_id == article_id_col)
        .exists()
    )


def article_provider_name_matches(
    provider_name: str,
    *,
    provider_name_col=Article.provider_name,
):
    return provider_name_col.ilike(
        contains_literal_pattern(provider_name),
        escape="\\",
    )


def article_source_exists(
    *,
    article_id_col=Article.id,
    source_ids,
):
    return (
        select(1)
        .select_from(RawFeedItem)
        .where(
            and_(
                RawFeedItem.article_id == article_id_col,
                RawFeedItem.source_id.in_(source_ids),
            )
        )
        .exists()
    )


def article_canonical_source_id(
    *,
    article_id_col=Article.id,
    canonical_url_col=Article.canonical_url,
):
    return (
        select(RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == article_id_col,
                RawFeedItem.raw_link == canonical_url_col,
            )
        )
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )


def article_latest_source_id(
    *,
    article_id_col=Article.id,
):
    return (
        select(RawFeedItem.source_id)
        .where(RawFeedItem.article_id == article_id_col)
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )


def article_resolved_provider_name(
    *,
    article_id_col=Article.id,
    canonical_url_col=Article.canonical_url,
    fallback_provider_name_col=Article.provider_name,
):
    canonical_provider = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == article_id_col,
                RawFeedItem.raw_link == canonical_url_col,
            )
        )
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    latest_provider = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(RawFeedItem.article_id == article_id_col)
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return func.coalesce(
        canonical_provider,
        latest_provider,
        fallback_provider_name_col,
    )


def mapped_or_provider_unmapped_condition(
    provider_name: str,
    *,
    source_ids=None,
    article_id_col=Article.id,
    provider_name_col=Article.provider_name,
):
    mapped_exists = active_ticker_mapped_exists()
    if source_ids is None:
        return mapped_exists

    has_any_raw = article_has_any_raw_provenance(article_id_col=article_id_col)
    include_provider_exists = article_source_exists(
        article_id_col=article_id_col,
        source_ids=source_ids,
    )
    rawless_provider_match = article_provider_name_matches(
        provider_name,
        provider_name_col=provider_name_col,
    )
    return or_(
        mapped_exists,
        and_(~mapped_exists, include_provider_exists),
        and_(~mapped_exists, ~has_any_raw, rawless_provider_match),
    )


def provider_filter_condition(
    provider_name: str,
    *,
    source_ids=None,
    article_id_col=Article.id,
    canonical_url_col=Article.canonical_url,
    provider_name_col=Article.provider_name,
):
    if source_ids is None:
        return article_provider_name_matches(
            provider_name,
            provider_name_col=provider_name_col,
        )

    has_raw_provenance = article_has_any_raw_provenance(article_id_col=article_id_col)
    canonical_source_id = article_canonical_source_id(
        article_id_col=article_id_col,
        canonical_url_col=canonical_url_col,
    )
    latest_source_id = article_latest_source_id(article_id_col=article_id_col)
    provider_name_match = article_provider_name_matches(
        provider_name,
        provider_name_col=provider_name_col,
    )
    return or_(
        func.coalesce(canonical_source_id, latest_source_id).in_(source_ids),
        and_(~has_raw_provenance, provider_name_match),
    )


def build_article_query(
    db: Session,
    *,
    ticker: str | None = None,
    tickers: Sequence[str] | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = False,
    include_unmapped_from_provider: str | None = None,
    from_: datetime | None = None,
    to: datetime | None = None,
):
    query = select(Article)
    publish_sort_key = func.coalesce(
        Article.published_at,
        Article.created_at,
        Article.first_seen_at,
    )
    ticker_symbols = _normalized_ticker_symbols(ticker=ticker, tickers=tickers)
    ticker_filter_supplied = ticker is not None or tickers is not None

    mapped_exists = active_ticker_mapped_exists()

    if ticker_symbols:
        ticker_match_exists = (
            select(1)
            .select_from(ArticleTicker)
            .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
            .where(
                and_(
                    ArticleTicker.article_id == Article.id,
                    Ticker.symbol.in_(ticker_symbols),
                    Ticker.active.is_(True),
                )
            )
            .correlate(Article)
            .exists()
        )
        query = query.where(ticker_match_exists)
    elif ticker_filter_supplied:
        query = query.where(mapped_exists)
    elif include_unmapped:
        pass  # Show all articles (including unmapped) — no filter needed
    elif include_unmapped_from_provider:
        include_name = include_unmapped_from_provider.strip()
        if include_name:
            query = query.where(
                mapped_or_provider_unmapped_condition(
                    include_name,
                    source_ids=source_id_by_name_subquery(include_name),
                )
            )
        else:
            query = query.where(mapped_exists)
    else:
        query = query.where(mapped_exists)

    if source:
        source_text = source.strip()
        if source_text:
            query = query.where(
                Article.source_name.ilike(
                    contains_literal_pattern(source_text),
                    escape="\\",
                )
            )

    if provider:
        provider_text = provider.strip()
        if provider_text:
            has_source = find_source_by_name(db, provider_text) is not None
            query = query.where(
                provider_filter_condition(
                    provider_text,
                    source_ids=source_id_by_name_subquery(provider_text) if has_source else None,
                )
            )

    if q:
        q_text = q.strip()
        if q_text:
            title_match = Article.title.ilike(
                contains_literal_pattern(q_text),
                escape="\\",
            )
            ticker_q_match = (
                select(1)
                .select_from(ArticleTicker)
                .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
                .where(
                    and_(
                        ArticleTicker.article_id == Article.id,
                        Ticker.symbol.ilike(
                            contains_literal_pattern(q_text.upper()),
                            escape="\\",
                        ),
                        Ticker.active.is_(True),
                    )
                )
                .correlate(Article)
                .exists()
            )
            query = query.where(or_(title_match, ticker_q_match))

    if from_:
        query = query.where(publish_sort_key >= from_)

    if to:
        query = query.where(publish_sort_key <= to)

    return query
