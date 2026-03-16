from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.orm import Session

from app.article_filters import build_article_query
from app.database import get_db
from app.deps import require_admin_api_key
from app.models import Article, ArticleTicker, RawFeedItem, Source, Ticker
from app.query_utils import iter_chunks, prefix_literal_pattern
from app.schemas import (
    MarkAlertsSentRequest,
    MarkAlertsSentResponse,
    NewsGlobalSummary,
    NewsIdsResponse,
    NewsItem,
    NewsListResponse,
    TickerItem,
    TickerListResponse,
)
from app.utils import clean_summary_text, decode_cursor, encode_cursor

news_router = APIRouter()

MAX_NEWS_IDS_PAGE_SIZE = 5000
GLOBAL_NEWS_TRACKED_LIMIT = 100
EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _article_sort_key_expr():
    return func.coalesce(Article.published_at, Article.created_at, Article.first_seen_at)


def _coalesce_row_timestamp(*values: datetime | None) -> datetime:
    for value in values:
        if value is not None:
            return value
    return EPOCH_UTC


def _published_at_for_response(row: object) -> datetime:
    """Protect API serialization/cursoring from legacy rows with missing timestamps."""
    return _coalesce_row_timestamp(
        getattr(row, "published_at", None),
        getattr(row, "created_at", None),
        getattr(row, "first_seen_at", None),
    )


def _first_seen_at_for_response(row: object) -> datetime:
    """Protect serialization for legacy rows created before first_seen_at existed."""
    return _coalesce_row_timestamp(
        getattr(row, "first_seen_at", None),
        getattr(row, "created_at", None),
        getattr(row, "published_at", None),
    )


def _parse_aggregated_symbols(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = [item for item in value.split(",") if item]
    elif isinstance(value, (list, tuple)):
        raw_values = [str(item) for item in value if item]
    else:
        raw_values = [str(value)]
    unique = sorted({item.strip() for item in raw_values if item and item.strip()})
    return unique


def _apply_news_cursor(query, *, cursor: str | None = None):
    sort_key = _article_sort_key_expr()
    cursor_payload = decode_cursor(cursor) if cursor else None
    if cursor_payload:
        cursor_published, cursor_id = cursor_payload
        query = query.where(
            or_(
                sort_key < cursor_published,
                and_(sort_key == cursor_published, Article.id < cursor_id),
            )
        )
    return query, sort_key


def _build_news_page_subquery(
    db: Session,
    *,
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = False,
    include_unmapped_from_provider: str | None = None,
    from_: datetime | None = None,
    to: datetime | None = None,
    cursor: str | None = None,
    limit: int | None = None,
    article_id: int | None = None,
):
    query = build_article_query(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
    )
    query, sort_key = _apply_news_cursor(query, cursor=cursor)
    if article_id is not None:
        query = query.where(Article.id == article_id)
    query = query.with_only_columns(
        Article.id.label("id"),
        Article.title.label("title"),
        Article.canonical_url.label("url"),
        Article.source_name.label("source"),
        Article.provider_name.label("provider_name"),
        Article.summary.label("summary"),
        Article.published_at.label("published_at"),
        Article.created_at.label("created_at"),
        Article.first_seen_at.label("first_seen_at"),
        Article.first_alert_sent_at.label("alert_sent_at"),
        Article.cluster_key.label("dedupe_group"),
        sort_key.label("sort_ts"),
    ).order_by(sort_key.desc(), Article.id.desc())
    if limit is not None:
        query = query.limit(limit)
    return query.subquery("news_page")


def _serialize_news_item(row: object) -> NewsItem:
    return NewsItem(
        id=row.id,
        title=row.title,
        url=row.url,
        source=row.source,
        provider=row.provider,
        summary=clean_summary_text(row.summary),
        published_at=_published_at_for_response(row),
        first_seen_at=_first_seen_at_for_response(row),
        alert_sent_at=row.alert_sent_at,
        tickers=_parse_aggregated_symbols(row.tickers),
        dedupe_group=row.dedupe_group,
    )


def _build_ticker_agg_subquery(db: Session, news_page):
    ticker_rows = (
        select(
            news_page.c.id.label("article_id"),
            Ticker.symbol.label("symbol"),
        )
        .select_from(news_page)
        .join(ArticleTicker, ArticleTicker.article_id == news_page.c.id)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(Ticker.active.is_(True))
        .distinct()
        .subquery("ticker_rows")
    )
    dialect_name = db.get_bind().dialect.name if db.get_bind() is not None else ""
    if dialect_name == "postgresql":
        ticker_agg = func.array_agg(ticker_rows.c.symbol).label("tickers")
    else:
        ticker_agg = func.group_concat(ticker_rows.c.symbol, ",").label("tickers")
    return (
        select(
            ticker_rows.c.article_id,
            ticker_agg,
        )
        .group_by(ticker_rows.c.article_id)
        .subquery("ticker_agg")
    )


def _build_enriched_news_select(db: Session, news_page):
    ticker_agg = _build_ticker_agg_subquery(db, news_page)
    canonical_provider = (
        select(Source.name)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == news_page.c.id,
                RawFeedItem.raw_link == news_page.c.url,
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
        .where(RawFeedItem.article_id == news_page.c.id)
        .order_by(RawFeedItem.id.desc())
        .limit(1)
        .scalar_subquery()
    )
    return (
        select(
            news_page.c.id,
            news_page.c.title,
            news_page.c.url,
            news_page.c.source,
            news_page.c.provider_name,
            news_page.c.summary,
            news_page.c.published_at,
            news_page.c.created_at,
            news_page.c.first_seen_at,
            news_page.c.alert_sent_at,
            news_page.c.dedupe_group,
            news_page.c.sort_ts,
            func.coalesce(canonical_provider, latest_provider, news_page.c.provider_name).label("provider"),
            ticker_agg.c.tickers.label("tickers"),
        )
        .select_from(news_page.outerjoin(ticker_agg, ticker_agg.c.article_id == news_page.c.id))
        .order_by(news_page.c.sort_ts.desc(), news_page.c.id.desc())
    )


def _build_global_summary(db: Session) -> NewsGlobalSummary | None:
    sort_key = _article_sort_key_expr()
    mapped_exists = (
        select(1)
        .select_from(ArticleTicker)
        .join(Ticker, Ticker.id == ArticleTicker.ticker_id)
        .where(
            and_(
                ArticleTicker.article_id == Article.id,
                Ticker.active.is_(True),
            )
        )
        .correlate(Article)
        .exists()
    )
    include_provider_exists = (
        select(1)
        .select_from(RawFeedItem)
        .join(Source, Source.id == RawFeedItem.source_id)
        .where(
            and_(
                RawFeedItem.article_id == Article.id,
                func.lower(Source.name) == "business wire",
            )
        )
        .correlate(Article)
        .exists()
    )
    base_query = select(Article).where(
        or_(
            mapped_exists,
            and_(~mapped_exists, include_provider_exists),
        )
    )
    summary_page = (
        base_query.with_only_columns(
            Article.id.label("id"),
            func.count().over().label("total"),
        )
        .order_by(sort_key.desc(), Article.id.desc())
        .limit(GLOBAL_NEWS_TRACKED_LIMIT)
        .subquery("global_summary_page")
    )
    dialect_name = db.get_bind().dialect.name if db.get_bind() is not None else ""
    if dialect_name == "postgresql":
        tracked_ids_agg = func.array_agg(summary_page.c.id).label("tracked_ids")
    else:
        tracked_ids_agg = func.group_concat(summary_page.c.id, ",").label("tracked_ids")
    row = db.execute(
        select(
            func.max(summary_page.c.total).label("total"),
            tracked_ids_agg,
        ).select_from(summary_page)
    ).one()
    tracked_ids_value = row.tracked_ids
    if tracked_ids_value is None:
        tracked_ids: list[int] = []
    elif isinstance(tracked_ids_value, str):
        tracked_ids = [int(value) for value in tracked_ids_value.split(",") if value]
    else:
        tracked_ids = [int(value) for value in tracked_ids_value if value is not None]
    total = int(row.total or 0)
    return NewsGlobalSummary(
        total=total,
        tracked_ids=tracked_ids,
        tracked_limit=GLOBAL_NEWS_TRACKED_LIMIT,
    )


@news_router.get("/tickers", response_model=TickerListResponse)
def list_tickers(
    q: str | None = Query(default=None, description="Prefix match on ticker symbol"),
    limit: int = Query(default=5000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
):
    query = select(Ticker).where(Ticker.active.is_(True)).order_by(Ticker.symbol.asc())
    if q:
        query = query.where(
            Ticker.symbol.ilike(
                prefix_literal_pattern(q.strip().upper()),
                escape="\\",
            )
        )

    total = db.scalar(select(func.count()).select_from(query.order_by(None).subquery())) or 0
    rows = db.scalars(query.limit(limit).offset(offset)).all()
    return TickerListResponse(
        items=[
            TickerItem(
                symbol=row.symbol,
                fund_name=row.fund_name,
                sponsor=row.sponsor,
                active=row.active,
            )
            for row in rows
        ],
        total=total,
    )


@news_router.get("/news/ids", response_model=NewsIdsResponse)
def list_news_ids(
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = Query(
        default=False,
        description="Include articles not mapped to any active ticker",
    ),
    include_unmapped_from_provider: str | None = Query(
        default=None,
        description="Include articles not mapped to any active ticker only from this provider while keeping active-ticker-mapped articles from all providers",
    ),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=MAX_NEWS_IDS_PAGE_SIZE, ge=1, le=MAX_NEWS_IDS_PAGE_SIZE),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    query = build_article_query(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
    )
    query, sort_key = _apply_news_cursor(query, cursor=cursor)

    id_query = (
        query.with_only_columns(Article.id, sort_key.label("sort_ts"))
        .order_by(sort_key.desc(), Article.id.desc())
        .limit(limit + 1)
    )
    rows = db.execute(id_query).all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    ids = [article_id for article_id, _sort_ts in rows]
    next_cursor = None
    if has_more and rows:
        last_id, last_sort_ts = rows[-1]
        next_cursor = encode_cursor(last_sort_ts or EPOCH_UTC, last_id)
    return NewsIdsResponse(ids=ids, next_cursor=next_cursor)


@news_router.get("/news", response_model=NewsListResponse)
def list_news(
    ticker: str | None = None,
    source: str | None = None,
    provider: str | None = None,
    q: str | None = None,
    include_unmapped: bool = Query(
        default=False,
        description="Include articles not mapped to any active ticker",
    ),
    include_unmapped_from_provider: str | None = Query(
        default=None,
        description="Include articles not mapped to any active ticker only from this provider while keeping active-ticker-mapped articles from all providers",
    ),
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = None,
    limit: int = Query(default=50, ge=1, le=100),
    include_global_summary: bool = Query(default=False),
    cursor: str | None = None,
    db: Session = Depends(get_db),
):
    news_page = _build_news_page_subquery(
        db,
        ticker=ticker,
        source=source,
        provider=provider,
        q=q,
        include_unmapped=include_unmapped,
        include_unmapped_from_provider=include_unmapped_from_provider,
        from_=from_,
        to=to,
        cursor=cursor,
        limit=limit + 1,
    )
    rows = db.execute(_build_enriched_news_select(db, news_page)).all()
    has_more = len(rows) > limit
    rows = rows[:limit]

    items = [_serialize_news_item(row) for row in rows]

    next_cursor = None
    if has_more and rows:
        last = rows[-1]
        next_cursor = encode_cursor(_published_at_for_response(last), last.id)

    global_summary = _build_global_summary(db) if include_global_summary else None

    return NewsListResponse(
        items=items,
        next_cursor=next_cursor,
        meta={
            "count": len(items),
            "limit": limit,
            "sort": "latest",
        },
        global_summary=global_summary,
    )


@news_router.get("/news/{article_id}", response_model=NewsItem)
def get_news_item(article_id: int, db: Session = Depends(get_db)):
    news_page = _build_news_page_subquery(db, article_id=article_id)
    row = db.execute(_build_enriched_news_select(db, news_page)).first()
    if row is None:
        raise HTTPException(status_code=404, detail="Article not found")
    return _serialize_news_item(row)


@news_router.post(
    "/news/alerts/sent",
    response_model=MarkAlertsSentResponse,
    dependencies=[Depends(require_admin_api_key)],
)
def mark_news_alerts_sent(payload: MarkAlertsSentRequest, db: Session = Depends(get_db)):
    unique_ids = sorted({int(article_id) for article_id in payload.article_ids if int(article_id) > 0})
    if not unique_ids:
        return MarkAlertsSentResponse(requested=0, marked=0, first_alert_sent_at=None)

    sent_at = datetime.now(timezone.utc)
    updated = 0
    for chunk in iter_chunks(unique_ids):
        updated += db.execute(
            update(Article)
            .where(
                Article.id.in_(chunk),
                Article.first_alert_sent_at.is_(None),
            )
            .values(first_alert_sent_at=sent_at)
        ).rowcount or 0
    db.commit()
    return MarkAlertsSentResponse(
        requested=len(unique_ids),
        marked=int(updated),
        first_alert_sent_at=sent_at if updated else None,
    )
