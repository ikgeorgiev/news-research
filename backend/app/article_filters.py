from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.query_utils import active_ticker_mapped_exists, contains_literal_pattern


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
        include_source_row = db.scalar(
            select(Source).where(func.lower(Source.name) == include_name.lower())
        )
        if include_source_row is None:
            query = query.where(mapped_exists)
        else:
            include_provider_exists = (
                select(1)
                .select_from(RawFeedItem)
                .where(
                    and_(
                        RawFeedItem.article_id == Article.id,
                        RawFeedItem.source_id == include_source_row.id,
                    )
                )
                .correlate(Article)
                .exists()
            )
            has_any_raw = (
                select(1)
                .select_from(RawFeedItem)
                .where(RawFeedItem.article_id == Article.id)
                .correlate(Article)
                .exists()
            )
            rawless_provider_match = Article.provider_name.ilike(
                contains_literal_pattern(include_name),
                escape="\\",
            )
            query = query.where(
                or_(
                    mapped_exists,
                    and_(~mapped_exists, include_provider_exists),
                    and_(~mapped_exists, ~has_any_raw, rawless_provider_match),
                )
            )
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
            source_row = db.scalar(
                select(Source).where(func.lower(Source.name) == provider_text.lower())
            )
            if source_row is not None:
                has_raw_provenance = (
                    select(1)
                    .select_from(RawFeedItem)
                    .where(RawFeedItem.article_id == Article.id)
                    .correlate(Article)
                    .exists()
                )
                canonical_source_id = (
                    select(RawFeedItem.source_id)
                    .where(
                        and_(
                            RawFeedItem.article_id == Article.id,
                            RawFeedItem.raw_link == Article.canonical_url,
                        )
                    )
                    .order_by(RawFeedItem.id.desc())
                    .limit(1)
                    .correlate(Article)
                    .scalar_subquery()
                )
                latest_source_id = (
                    select(RawFeedItem.source_id)
                    .where(RawFeedItem.article_id == Article.id)
                    .order_by(RawFeedItem.id.desc())
                    .limit(1)
                    .correlate(Article)
                    .scalar_subquery()
                )
                provider_name_match = Article.provider_name.ilike(
                    contains_literal_pattern(provider_text),
                    escape="\\",
                )
                query = query.where(
                    or_(
                        func.coalesce(canonical_source_id, latest_source_id)
                        == source_row.id,
                        and_(~has_raw_provenance, provider_name_match),
                    )
                )
            else:
                query = query.where(
                    Article.provider_name.ilike(
                        contains_literal_pattern(provider_text),
                        escape="\\",
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
