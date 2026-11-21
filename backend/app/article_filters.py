from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import Session

from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.query_utils import active_ticker_mapped_exists, contains_literal_pattern
from app.utils import to_utc

GENERAL_UNMAPPED_PROVIDER = "Business Wire"
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def article_sort_timestamp():
    return func.coalesce(Article.published_at, Article.created_at, Article.first_seen_at, EPOCH_UTC)


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


def article_resolved_provider_name(
    *,
    article_id_col=Article.id,
    canonical_url_col=Article.canonical_url,
    fallback_provider_name_col=Article.provider_name,
):
    provenance = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(RawFeedItem.article_id == article_id_col)
    )
    latest = provenance.order_by(RawFeedItem.id.desc()).limit(1)
    canonical = latest.where(RawFeedItem.raw_link == canonical_url_col)
    # A tracking parameter can prevent an exact URL match. Prefer the recorded
    # provider when provenance supports it before considering a later mirror.
    recorded = (
        provenance.where(func.lower(Source.name) == func.lower(fallback_provider_name_col))
        .order_by(RawFeedItem.id.asc())
        .limit(1)
    )
    return func.coalesce(
        canonical.scalar_subquery(),
        recorded.scalar_subquery(),
        latest.scalar_subquery(),
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
        include_provider_exists,
        and_(~has_any_raw, rawless_provider_match),
    )


def provider_filter_condition(provider_name: str, *, registered: bool):
    resolved = article_resolved_provider_name()
    if not registered:
        return article_provider_name_matches(provider_name, provider_name_col=resolved)

    # A registered provider selects that exact source. Historical articles
    # with no raw provenance can still carry a longer, free-form label.
    return or_(
        func.lower(resolved) == provider_name.lower(),
        and_(~article_has_any_raw_provenance(), article_provider_name_matches(provider_name)),
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
                provider_filter_condition(provider_text, registered=has_source)
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
        query = query.where(article_sort_timestamp() >= to_utc(from_))

    if to:
        query = query.where(article_sort_timestamp() <= to_utc(to))

    return query
